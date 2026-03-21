from __future__ import annotations

import hmac
import ipaddress
import json
import logging
import re
import secrets
import asyncio
from dataclasses import dataclass
from hashlib import sha256
from contextlib import suppress
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from .accounts import AccountProfile, list_profiles
from .account_identity import (
    decode_jwt_claims,
    extract_access_token,
    extract_account_identity,
    extract_email,
    extract_id_token,
)
from .account_usage_store import (
    delete_account_data,
    ensure_account,
    get_account,
    initialize_usage_store,
    list_usage_rollovers,
    merge_account_data,
    migrate_account_ids,
    rename_account_data,
    record_account_usage,
    reconcile_due_accounts,
    sync_account_rate_limit_percentages,
    sync_account_usage_snapshot,
)
from .auth_store import (
    AuthStoreError,
    persist_and_save_label,
    persist_current_auth,
    save_current_auth_under_label,
)
from .codex_cli import (
    CodexCLIError,
    cancel_login,
    derive_label,
    get_login_status,
    read_rate_limits_via_app_server,
    read_current_auth,
    relay_callback_to_login,
    start_login,
    wait_for_auth_update,
)
from .codex_switch import (
    CodexSwitchError,
    current_label,
    list_labels,
    switch_label,
)
from .config import settings
from .login_sessions import (
    cancel_login_session,
    create_login_session,
    get_latest_session,
    get_login_session,
    mark_relay_callback,
    session_state,
    to_public_session,
    validate_relay_token,
)

app = FastAPI(title="Codex Auth Manager", version="0.2.0")
logger = logging.getLogger(__name__)
_RECONCILE_INTERVAL_SECONDS = 600
_LIVE_REFRESH_CONCURRENCY = 4
_USAGE_STALE_SECONDS = 1800
_reconcile_task: asyncio.Task[None] | None = None
LABEL_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
_FRONTEND_DIST = Path(__file__).resolve().parents[1] / "frontend" / "dist"
_LAST_REFRESH_STATUS_BY_KEY: dict[str, dict[str, Any]] = {}
_LAST_REFRESH_COMPLETED_AT: str | None = None

if (_FRONTEND_DIST / "assets").exists():
    app.mount("/assets", StaticFiles(directory=str(_FRONTEND_DIST / "assets")), name="frontend-assets")


@dataclass
class PersistCurrentAuthResult:
    persisted: bool
    skipped: bool
    reason: str
    label: str | None
    account_key: str | None
    email: str | None
    matched_existing_profile: bool
    created_new_profile: bool
    up_to_date: bool
    codex_switch: dict[str, Any] | None


@app.on_event("startup")
async def on_startup() -> None:
    initialize_usage_store()
    _migrate_usage_keys_from_labels()
    global _reconcile_task
    if _reconcile_task is None:
        _reconcile_task = asyncio.create_task(_periodic_reconcile_usage_windows())


@app.on_event("shutdown")
async def on_shutdown() -> None:
    global _reconcile_task
    if _reconcile_task is not None:
        _reconcile_task.cancel()
        with suppress(asyncio.CancelledError):
            await _reconcile_task
        _reconcile_task = None


async def _periodic_reconcile_usage_windows() -> None:
    while True:
        await asyncio.sleep(_RECONCILE_INTERVAL_SECONDS)
        try:
            refreshed = reconcile_due_accounts(now=datetime.now(timezone.utc))
            if refreshed:
                logger.info("usage reconciliation refreshed %s account window(s)", refreshed)
        except Exception:
            logger.exception("usage reconciliation failed")


@app.middleware("http")
async def web_login_guard(request: Request, call_next):
    if not _web_login_enabled():
        return await call_next(request)
    if _is_login_exempt_path(request.url.path):
        return await call_next(request)
    if _is_internal_request(request):
        return await call_next(request)
    if _has_valid_internal_api_token(request):
        return await call_next(request)
    if _has_valid_web_session(request):
        return await call_next(request)
    return JSONResponse({"detail": "Login required"}, status_code=401)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/login")
async def login_page() -> JSONResponse:
    return JSONResponse(
        {"detail": "Backend UI login is disabled. Use the React frontend and API authentication."},
        status_code=410,
    )


@app.post("/login")
async def login_submit() -> JSONResponse:
    return JSONResponse(
        {"detail": "Backend UI login is disabled. Use the React frontend and API authentication."},
        status_code=410,
    )


@app.post("/logout")
async def logout() -> JSONResponse:
    return JSONResponse(
        {"detail": "Backend UI logout is disabled. Use the React frontend session flow."},
        status_code=410,
    )


@app.get("/")
async def index() -> JSONResponse:
    return JSONResponse(
        {
            "service": "codex-auth-manager-api",
            "ui": "Use the React frontend service for UI.",
            "health": "/health",
            "docs": "/docs",
        }
    )


@app.get("/ui")
async def ui() -> JSONResponse:
    return JSONResponse(
        {"detail": "Backend UI is disabled. Use the React frontend service."},
        status_code=410,
    )


@app.get("/ui/accounts/{label}")
async def ui_account_usage(label: str) -> JSONResponse:
    return JSONResponse(
        {"detail": "Backend UI is disabled. Use the React frontend service."},
        status_code=410,
    )


@app.get("/ui/stats")
async def ui_usage_stats() -> JSONResponse:
    return JSONResponse(
        {"detail": "Backend UI is disabled. Use the React frontend service."},
        status_code=410,
    )


@app.get("/oauth/callback")
async def oauth_callback(request: Request) -> JSONResponse:
    received = dict(request.query_params)
    stored_at = _store_callback(received)
    return JSONResponse(
        {
            "received": received,
            "stored_at": str(stored_at),
            "next": "POST /auth/exchange with code + code_verifier (optional)",
        }
    )


@app.post("/oauth/callback")
async def oauth_callback_post(request: Request, payload: dict[str, Any]) -> JSONResponse:
    stored_at = _store_callback(payload)

    label = payload.get("label")
    auth_json = payload.get("auth_json")

    if label and auth_json:
        _require_internal_auth(request)
        _persist_auth_and_save(str(label), auth_json)
        return JSONResponse(
            {
                "stored_at": str(stored_at),
                "saved_label": str(label),
                "message": "Auth saved and codex-switch profile updated.",
            }
        )

    return JSONResponse(
        {
            "stored_at": str(stored_at),
            "message": "Callback captured. To save, POST /auth/save.",
        }
    )


@app.get("/auth/callback")
async def auth_callback(request: Request) -> JSONResponse:
    return await oauth_callback(request)


@app.post("/auth/callback")
async def auth_callback_post(request: Request, payload: dict[str, Any]) -> JSONResponse:
    return await oauth_callback_post(request, payload)


@app.post("/auth/login/start")
async def auth_login_start(request: Request) -> JSONResponse:
    _require_internal_auth(request)
    return _start_login_response()


@app.post("/auth/login/start-relay")
async def auth_login_start_relay() -> JSONResponse:
    # Extension-facing start endpoint; session/relay token still gates callback relay.
    return _start_login_response()


def _start_login_response() -> JSONResponse:
    try:
        result = start_login()
    except CodexCLIError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    auth_url = result.browser_url
    session = create_login_session(
        auth_url=auth_url, ttl_seconds=settings.login_session_ttl_seconds
    )

    return JSONResponse(
        {
            "status": "started",
            "auth_path": result.auth_path,
            "pid": result.pid,
            "started_at": result.started_at,
            "browser_url": result.browser_url,
            "auth_url": auth_url,
            "session_id": session.session_id,
            "relay_token": session.relay_token,
            "session_expires_at": session.expires_at.isoformat(),
            "instructions": result.instructions,
            "output_excerpt": result.output_excerpt,
            "session": to_public_session(session, include_relay_token=True),
        }
    )


@app.get("/auth/login/status")
async def auth_login_status(wait_seconds: int = 0, session_id: str | None = None) -> JSONResponse:
    if wait_seconds > 0:
        wait_for_auth_update(timeout_seconds=min(wait_seconds, 120))

    session = get_login_session(session_id) if session_id else get_latest_session()
    if session_id and session is None:
        raise HTTPException(status_code=404, detail="Login session not found or expired")
    result = get_login_status()
    state, state_error = session_state(
        session,
        auth_updated=result.auth_updated,
        cli_failed=result.status == "failed",
        cli_error=result.error,
        cli_status=result.status,
    )
    callback_received = bool(session is not None and session.callback_received_at is not None)
    relay_stage = "not_received"
    if callback_received and result.auth_updated:
        relay_stage = "relayed_and_auth_updated"
    elif callback_received and not result.auth_updated:
        relay_stage = "relayed_waiting_for_auth_update"
    elif session is not None and session.provider_error:
        relay_stage = "provider_error"

    auto_persist: dict[str, Any] = {"attempted": False}
    if result.auth_updated:
        auto_persist["attempted"] = True
        try:
            current_auth = read_current_auth()
            if callback_received:
                auth_validation = _validate_relay_finalized_auth(
                    current_auth,
                    started_at_iso=result.started_at,
                )
                auto_persist["auth_validation"] = auth_validation
                if not auth_validation.get("ok"):
                    auto_persist.update(
                        {
                            "status": "error",
                            "reason": "auth_not_fresh",
                            "error": auth_validation.get("message"),
                        }
                    )
                    raise ValueError("Relay auth payload is stale or expired")

            persist_result = _persist_current_auth_to_profile(
                desired_label=None,
                create_if_missing=True,
                auth_json=current_auth,
            )
            auto_persist.update(
                {
                    "status": "persisted" if persist_result.persisted else "skipped",
                    "reason": persist_result.reason,
                    "label": persist_result.label,
                    "account_key": persist_result.account_key,
                    "email": persist_result.email,
                    "matched_existing_profile": persist_result.matched_existing_profile,
                    "created_new_profile": persist_result.created_new_profile,
                    "up_to_date": persist_result.up_to_date,
                    "codex_switch": persist_result.codex_switch,
                }
            )
        except ValueError as exc:
            if auto_persist.get("status") != "error":
                auto_persist.update(
                    {
                        "status": "error",
                        "reason": "persist_failed",
                        "error": str(exc),
                    }
                )
        except (CodexCLIError, AuthStoreError, CodexSwitchError) as exc:
            logger.warning("auto persist after auth update failed: %s", exc)
            auto_persist.update(
                {
                    "status": "error",
                    "reason": "persist_failed",
                    "error": str(exc),
                }
            )

    next_action: str | None
    if callback_received and not result.auth_updated:
        next_action = (
            "Relay callback captured. Waiting for auth finalization before any profile persistence."
        )
    elif result.auth_updated and auto_persist.get("status") == "persisted":
        if auto_persist.get("created_new_profile"):
            next_action = "Auth finalized and saved as a new profile automatically."
        else:
            next_action = "Auth finalized and matching saved profile was updated automatically."
    elif result.auth_updated and auto_persist.get("status") == "skipped":
        if auto_persist.get("reason") == "up_to_date":
            next_action = "Auth finalized. Saved profile already had the latest auth."
        elif auto_persist.get("reason") == "no_matching_profile":
            next_action = (
                "Auth finalized but no existing profile matched. Use POST /auth/import-current "
                "to intentionally create/import a new saved profile."
            )
        else:
            next_action = None
    elif result.auth_updated and auto_persist.get("status") == "error":
        if auto_persist.get("reason") == "auth_not_fresh":
            next_action = (
                "Relay callback was received, but finalized auth is stale/expired. "
                "Retry Add Account and complete a fresh login for the intended user."
            )
        else:
            next_action = (
                "Auth finalized but automatic profile persistence failed. "
                "Check auto_persist.error or run POST /auth/import-current."
            )
    else:
        next_action = None

    return JSONResponse(
        {
            "status": state,
            "session_id": session.session_id if session else None,
            "auth": {
                "exists": result.auth_exists,
                "updated": result.auth_updated,
                "path": result.auth_path,
            },
            "callback_received": callback_received,
            "session": to_public_session(session) if session else None,
            "started_at": result.started_at,
            "completed_at": result.completed_at,
            "pid": result.pid,
            "browser_url": result.browser_url,
            "error": state_error or result.error,
            "raw_cli_status": result.status,
            "relay": {
                "stage": relay_stage,
                "callback_received": callback_received,
                "callback_received_at": (
                    session.callback_received_at.isoformat()
                    if session and session.callback_received_at
                    else None
                ),
                "provider_error": session.provider_error if session else None,
                "provider_error_description": (
                    session.provider_error_description if session else None
                ),
                "auth_updated": result.auth_updated,
                "cli_status": result.status,
                "handoff_supported": True,
                "finalization_supported": False,
                "next_action": (
                    next_action
                ),
            },
            "auto_persist": auto_persist,
        }
    )


@app.post("/auth/login/cancel")
async def auth_login_cancel(request: Request, payload: dict[str, Any] | None = None) -> JSONResponse:
    _require_internal_auth(request)
    session_id = str((payload or {}).get("session_id", "")).strip() or None
    session_canceled = cancel_login_session(session_id)
    process_canceled = cancel_login()
    return JSONResponse(
        {
            "status": "canceled",
            "session_canceled": session_canceled,
            "process_canceled": process_canceled,
        }
    )


@app.post("/auth/relay-callback")
async def auth_relay_callback(payload: dict[str, Any]) -> JSONResponse:
    session_id = str(payload.get("session_id", "")).strip()
    relay_token = str(payload.get("relay_token", "")).strip()
    code = payload.get("code")
    state = payload.get("state")
    error = payload.get("error")
    error_description = payload.get("error_description")
    full_url = str(payload.get("full_url", "")).strip()

    if not session_id or not relay_token:
        logger.warning("relay-callback rejected: missing session_id or relay_token")
        raise HTTPException(
            status_code=400, detail="session_id and relay_token are required"
        )
    if not full_url:
        logger.warning("relay-callback rejected: missing full_url for session_id=%s", session_id)
        raise HTTPException(status_code=400, detail="full_url is required")
    if not code and not error:
        logger.warning(
            "relay-callback rejected: missing code/error for session_id=%s",
            session_id,
        )
        raise HTTPException(
            status_code=400, detail="code or error must be present in callback payload"
        )

    session = get_login_session(session_id)
    if session is None:
        logger.warning("relay-callback rejected: session not found or expired session_id=%s", session_id)
        raise HTTPException(status_code=404, detail="Login session not found or expired")
    if not validate_relay_token(session, relay_token):
        logger.warning("relay-callback rejected: invalid relay token session_id=%s", session_id)
        raise HTTPException(status_code=403, detail="Invalid relay token")
    expected_state = _expected_state_from_auth_url(session.auth_url)
    if expected_state and state and str(state).strip() != expected_state:
        logger.warning(
            "relay-callback rejected: state mismatch session_id=%s expected=%s got=%s",
            session_id,
            expected_state,
            state,
        )
        raise HTTPException(
            status_code=409,
            detail=(
                "Callback state does not match active login session. "
                "Start Add Account again and use that session's callback URL."
            ),
        )

    callback_payload = {
        "code": code,
        "state": state,
        "error": error,
        "error_description": error_description,
        "full_url": full_url,
        "relayed_at": datetime.now(timezone.utc).isoformat(),
    }

    updated = mark_relay_callback(
        session_id,
        callback_payload,
        provider_error=(str(error) if error else None),
        provider_error_description=(
            str(error_description) if error_description else None
        ),
    )
    if updated is None:
        logger.warning("relay-callback rejected: session already consumed session_id=%s", session_id)
        raise HTTPException(
            status_code=409,
            detail="Session has already consumed a different callback or has expired",
        )

    _store_callback({"type": "relay_callback", "session_id": session_id, "payload": callback_payload})
    handoff = relay_callback_to_login(callback_payload)
    logger.info(
        "relay-callback accepted session_id=%s provider_error=%s handoff_supported=%s",
        session_id,
        bool(error),
        bool(handoff.get("supported")),
    )

    return JSONResponse(
        {
            "status": "callback_received",
            "session": to_public_session(updated),
            "handoff": handoff,
        }
    )


@app.post("/auth/import-current")
async def import_current_auth(request: Request, payload: dict[str, Any] | None = None) -> JSONResponse:
    _require_internal_auth(request)
    desired_label = (payload or {}).get("label") if payload else None

    try:
        persist_result = _persist_current_auth_to_profile(
            desired_label=(str(desired_label) if desired_label is not None else None),
            create_if_missing=True,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except CodexCLIError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except (AuthStoreError, CodexSwitchError) as exc:
        raise _to_switch_http_error(exc) from exc

    return JSONResponse(
        {
            "status": "imported",
            "label": persist_result.label,
            "account_key": persist_result.account_key,
            "email": persist_result.email,
            "matched_existing_profile": persist_result.matched_existing_profile,
            "saved": persist_result.persisted or persist_result.up_to_date,
            "created_new_profile": persist_result.created_new_profile,
            "up_to_date": persist_result.up_to_date,
            "codex_switch": persist_result.codex_switch,
        }
    )


@app.post("/auth/import-json")
async def import_auth_json(request: Request, payload: dict[str, Any] | None = None) -> JSONResponse:
    _require_internal_auth(request)
    raw_auth = (payload or {}).get("auth_json") if payload else None
    desired_label = (payload or {}).get("label") if payload else None

    if not isinstance(raw_auth, dict):
        raise HTTPException(status_code=400, detail="auth_json object is required")

    try:
        persist_result = _persist_current_auth_to_profile(
            desired_label=(str(desired_label) if desired_label is not None else None),
            create_if_missing=True,
            auth_json=raw_auth,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except (AuthStoreError, CodexSwitchError) as exc:
        raise _to_switch_http_error(exc) from exc

    return JSONResponse(
        {
            "status": "imported",
            "label": persist_result.label,
            "account_key": persist_result.account_key,
            "email": persist_result.email,
            "matched_existing_profile": persist_result.matched_existing_profile,
            "saved": persist_result.persisted or persist_result.up_to_date,
            "created_new_profile": persist_result.created_new_profile,
            "up_to_date": persist_result.up_to_date,
            "codex_switch": persist_result.codex_switch,
        }
    )


@app.post("/auth/switch")
async def auth_switch(request: Request, payload: dict[str, Any]) -> JSONResponse:
    _require_internal_auth(request)
    label = str(payload.get("label", "")).strip()
    if not label:
        raise HTTPException(status_code=400, detail="label is required")

    profile = _profile_for_label(label)
    switched_account_key = profile.account_key if profile is not None else None

    try:
        result = switch_label(label)
        now_current = _resolve_current_label(read_current_auth(), list_profiles())
        if profile is not None:
            _touch_account_usage(profile=profile)
    except CodexSwitchError as exc:
        raise _to_switch_http_error(exc) from exc
    except CodexCLIError:
        now_current = None

    return JSONResponse(
        {
            "status": "switched",
            "label": label,
            "account_key": switched_account_key,
            "current_label": now_current or label,
            "codex_switch": {
                "command": result.command,
                "exit_code": result.returncode,
                "stdout": result.stdout,
            },
        }
    )


@app.post("/auth/delete")
async def auth_delete(request: Request, payload: dict[str, Any]) -> JSONResponse:
    _require_internal_auth(request)
    label = str(payload.get("label", "")).strip()
    if not label:
        raise HTTPException(status_code=400, detail="label is required")

    profile_path = settings.profiles_dir() / f"{label}.json"
    if not profile_path.exists():
        raise HTTPException(status_code=404, detail="Label not found")

    profile = _profile_for_label(label)
    usage_key = profile.account_key if profile else label

    try:
        profile_path.unlink()
        delete_account_data(usage_key)
    except OSError as exc:
        raise HTTPException(status_code=500, detail=f"Unable to delete profile: {exc}") from exc

    return JSONResponse({"status": "deleted", "label": label})


@app.post("/auth/rename")
async def auth_rename(request: Request, payload: dict[str, Any]) -> JSONResponse:
    _require_internal_auth(request)
    old_label = str(payload.get("old_label", "")).strip()
    new_label = str(payload.get("new_label", "")).strip()
    if not old_label or not new_label:
        raise HTTPException(status_code=400, detail="old_label and new_label are required")
    if old_label == new_label:
        return JSONResponse({"status": "unchanged", "label": old_label})
    if not LABEL_RE.match(new_label):
        raise HTTPException(
            status_code=400,
            detail="new_label must be 1-64 chars and contain only letters, numbers, dot, underscore, or dash",
        )

    profiles_dir = settings.profiles_dir()
    source = profiles_dir / f"{old_label}.json"
    target = profiles_dir / f"{new_label}.json"
    if not source.exists():
        raise HTTPException(status_code=404, detail="old_label not found")
    merged_duplicate = False
    if target.exists():
        try:
            source_auth = json.loads(source.read_text())
            target_auth = json.loads(target.read_text())
        except (OSError, ValueError):
            source_auth = None
            target_auth = None

        source_email = extract_email(source_auth) if isinstance(source_auth, dict) else None
        target_email = extract_email(target_auth) if isinstance(target_auth, dict) else None
        source_token = extract_access_token(source_auth)
        target_token = extract_access_token(target_auth)
        same_identity = False
        if source_email and target_email and source_email.strip().lower() == target_email.strip().lower():
            same_identity = True
        if source_token and target_token and source_token == target_token:
            same_identity = True
        if not same_identity:
            raise HTTPException(status_code=409, detail="new_label already exists")
        try:
            target.unlink()
            merged_duplicate = True
        except OSError as exc:
            raise HTTPException(status_code=500, detail=f"Unable to replace duplicate profile: {exc}") from exc
        with suppress(Exception):
            delete_account_data(new_label)

    try:
        source.rename(target)
    except OSError as exc:
        raise HTTPException(status_code=500, detail=f"Unable to rename profile: {exc}") from exc

    with suppress(Exception):
        rename_account_data(old_label, new_label)
    with suppress(Exception):
        renamed_profile = _profile_for_label(new_label)
        if renamed_profile is not None:
            merge_account_data(old_label, renamed_profile.account_key)

    now_current = None
    was_current = False
    try:
        was_current = current_label() == old_label
    except CodexSwitchError:
        was_current = False
    if was_current:
        try:
            switch_label(new_label)
            now_current = new_label
        except CodexSwitchError:
            now_current = None

    return JSONResponse(
        {
            "status": "renamed",
            "old_label": old_label,
            "label": new_label,
            "current_label": now_current,
            "merged_duplicate": merged_duplicate,
        }
    )


@app.get("/auth/current")
async def auth_current() -> JSONResponse:
    auth_path = settings.codex_auth_file()
    meta = _auth_file_metadata(auth_path)

    if not meta["exists"]:
        return JSONResponse(
            {
                "auth": meta,
                "email": None,
                "current_label": None,
                "status": "missing",
            }
        )

    try:
        auth_json = read_current_auth()
        email = extract_email(auth_json)
        identity = extract_account_identity(auth_json)
        current = _resolve_current_label(auth_json, list_profiles())
    except CodexCLIError as exc:
        return JSONResponse(
            {
                "auth": meta,
                "email": None,
                "current_label": None,
                "status": "invalid",
                "error": str(exc),
            }
        )

    return JSONResponse(
        {
            "auth": meta,
            "email": email,
            "account_key": identity.account_key,
            "current_label": current,
            "current_display_label": _display_label(current, email),
            "status": "ok",
        }
    )


@app.get("/auth/rate-limits")
async def auth_rate_limits(request: Request) -> JSONResponse:
    _require_internal_auth(request)
    try:
        result = read_rate_limits_via_app_server()
    except CodexCLIError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    profiles = list_profiles()
    current_label = None
    current_email = None
    try:
        auth_json = read_current_auth()
        current_label = _resolve_current_label(auth_json, profiles)
        if current_label:
            current_profile = next((p for p in profiles if p.label == current_label), None)
            current_email = current_profile.email if current_profile else None
            if current_profile is not None:
                _touch_account_usage(profile=current_profile)
            snapshot = _extract_limit_snapshot(result.rate_limits)
            sync_account_usage_snapshot(
                current_profile.account_key if current_profile else current_label,
                usage_limit=snapshot["usage_limit"],
                usage_used=snapshot["usage_used"],
                rate_limit_window_type=snapshot["window_type"],
                rate_limit_refresh_at=snapshot["refresh_at"],
                provider_account_id=_account_provider_id(result.account),
                name=_account_name(result.account, current_email),
                now=datetime.now(timezone.utc),
            )
    except CodexCLIError:
        current_label = None

    return JSONResponse(
        {
            "source": "codex_app_server",
            "account": result.account,
            "rate_limits": result.rate_limits,
            "notifications": result.notifications,
            "current_label": current_label,
        }
    )


@app.get("/auth/export")
async def auth_export(request: Request, label: str) -> JSONResponse:
    _require_internal_auth(request)
    profile = _profile_for_label(label)
    if profile is None:
        raise HTTPException(status_code=404, detail="Label not found")

    return JSONResponse(
        {
            "label": profile.label,
            "email": profile.email,
            "auth_json": profile.auth,
        }
    )


@app.post("/auth/save")
async def save_auth(request: Request, payload: dict[str, Any]) -> JSONResponse:
    _require_internal_auth(request)
    label = payload.get("label")
    auth_json = payload.get("auth_json")

    if not label or not auth_json:
        raise HTTPException(status_code=400, detail="label and auth_json are required")

    _persist_auth_and_save(str(label), auth_json)
    return JSONResponse({"saved_label": str(label), "message": "Auth saved."})


@app.post("/auth/exchange")
async def exchange_code(request: Request, payload: dict[str, Any]) -> JSONResponse:
    _require_internal_auth(request)
    code = payload.get("code")
    code_verifier = payload.get("code_verifier")
    label = payload.get("label")
    redirect_uri = payload.get("redirect_uri") or settings.openai_redirect_uri

    if not code or not code_verifier:
        raise HTTPException(
            status_code=400, detail="code and code_verifier are required"
        )

    token_response = await _exchange_code_for_token(str(code), str(code_verifier), redirect_uri)

    stored_at = _store_callback(
        {
            "type": "token_response",
            "received_at": datetime.now(timezone.utc).isoformat(),
            "token_response": token_response,
        }
    )

    if label:
        _persist_auth_and_save(str(label), token_response)
        return JSONResponse(
            {
                "stored_at": str(stored_at),
                "saved_label": str(label),
                "token_response": token_response,
            }
        )

    return JSONResponse({"stored_at": str(stored_at), "token_response": token_response})


@app.get("/api/accounts")
async def api_accounts(request: Request) -> JSONResponse:
    _require_internal_auth(request)
    snapshot = _build_cached_accounts_snapshot()
    return JSONResponse(snapshot)


@app.get("/api/accounts/cached")
async def api_accounts_cached(request: Request) -> JSONResponse:
    _require_internal_auth(request)
    snapshot = _build_cached_accounts_snapshot()
    return JSONResponse(snapshot)


@app.get("/api/accounts/stream")
async def api_accounts_stream(request: Request) -> StreamingResponse:
    _require_internal_auth_or_query(request)
    profiles = _dedupe_profiles(list_profiles())
    snapshot = _build_cached_accounts_snapshot(profiles=profiles)
    event_headers = {
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    }

    async def _event_gen():
        yield _sse_event(
            "snapshot",
            {
                "accounts": snapshot["accounts"],
                "current_label": snapshot["current_label"],
                "aggregate": snapshot["aggregate"],
                "pending_labels": [p.label for p in profiles],
            },
        )
        if not profiles:
            yield _sse_event("complete", {"completed": 0, "failed": 0})
            return

        latest_by_label = {item["label"]: dict(item) for item in snapshot["accounts"]}
        baseline_auth = _safe_read_current_auth()
        session_limits_by_label = _fetch_session_limits_for_profiles(profiles, baseline_auth)
        completed = 0
        failed = 0

        for profile in profiles:
            rate_info = session_limits_by_label.get(profile.label) or {"error": "No rate-limit data returned"}
            try:
                if isinstance(rate_info, dict) and isinstance(rate_info.get("error"), str):
                    error_msg = str(rate_info.get("error"))
                    _mark_refresh_status(profile.account_key, ok=False, error=error_msg)
                    usage = _usage_tracking_payload(profile.account_key)
                    account_payload = _account_payload(profile, snapshot["current_label"], usage_tracking=usage)
                    account_payload["rate_limits"] = {"error": error_msg}
                    ok = False
                else:
                    if isinstance(rate_info, dict):
                        _sync_profile_usage_from_session_limits(profile, rate_info)
                    usage = _usage_tracking_payload(profile.account_key)
                    account_payload = _account_payload(profile, snapshot["current_label"], usage_tracking=usage)
                    if isinstance(rate_info, dict):
                        account_payload["rate_limits"] = rate_info
                    _mark_refresh_status(profile.account_key, ok=True, error=None)
                    ok = True
            except Exception as exc:
                _mark_refresh_status(profile.account_key, ok=False, error=str(exc))
                usage = _usage_tracking_payload(profile.account_key)
                account_payload = _account_payload(profile, snapshot["current_label"], usage_tracking=usage)
                account_payload["rate_limits"] = {"error": str(exc)}
                ok = False

            latest_by_label[profile.label] = account_payload
            completed += 1
            if not ok:
                failed += 1
                rate_obj = account_payload.get("rate_limits")
                error_msg = None
                if isinstance(rate_obj, dict):
                    maybe_error = rate_obj.get("error")
                    if isinstance(maybe_error, str):
                        error_msg = maybe_error
                yield _sse_event(
                    "error",
                    {
                        "label": profile.label,
                        "account_key": account_payload.get("account_key"),
                        "message": error_msg,
                    },
                )
            aggregate = _compute_aggregate(list(latest_by_label.values()))
            yield _sse_event("account_update", {"account": account_payload, "ok": ok})
            yield _sse_event("aggregate_update", aggregate)

        _mark_refresh_completed()
        yield _sse_event("complete", {"completed": completed, "failed": failed})

    return StreamingResponse(_event_gen(), media_type="text/event-stream", headers=event_headers)


@app.get("/api/accounts/{label}/usage-history")
async def api_account_usage_history(request: Request, label: str) -> JSONResponse:
    _require_internal_auth(request)
    profile = _profile_for_label(label)
    if profile is None:
        raise HTTPException(status_code=404, detail="Label not found")

    # Try to refresh usage snapshot from live rate-limit probe for this profile.
    if profile.access_token:
        async with httpx.AsyncClient(timeout=10) as client:
            rate_info = await _fetch_rate_limits(client, profile.access_token)
        if isinstance(rate_info, dict):
            _sync_profile_usage_from_probe(profile, rate_info)

    _touch_account_usage(profile=profile)
    usage = _usage_tracking_payload(profile.account_key)
    rollovers = list_usage_rollovers(profile.account_key)
    summary = _rollover_summary(rollovers, usage)

    return JSONResponse(
        {
            "label": profile.label,
            "account_key": profile.account_key,
            "display_label": _display_label(profile.label, profile.email),
            "email": profile.email,
            "usage_tracking": usage,
            "rollovers": rollovers,
            "summary": summary,
        }
    )


@app.get("/api/usage/stats")
async def api_usage_stats(request: Request) -> JSONResponse:
    _require_internal_auth(request)
    profiles = _dedupe_profiles(list_profiles())
    _touch_profiles_usage(profiles)

    per_account: list[dict[str, Any]] = []
    all_rollovers: list[dict[str, Any]] = []
    totals = {
        "accounts": len(profiles),
        "lifetime_used": 0,
        "active_window_used": 0,
        "active_window_limit": 0,
        "total_wasted": 0,
        "rollover_windows": 0,
    }

    for profile in profiles:
        usage = _usage_tracking_payload(profile.account_key)
        rollovers = list_usage_rollovers(profile.account_key)
        summary = _rollover_summary(rollovers, usage)
        all_rollovers.extend(rollovers)

        totals["lifetime_used"] += int((usage or {}).get("lifetime_used") or 0)
        totals["active_window_used"] += int((usage or {}).get("usage_in_window") or 0)
        totals["active_window_limit"] += int((usage or {}).get("usage_limit") or 0)
        totals["total_wasted"] += int(summary["total_wasted"])
        totals["rollover_windows"] += int(summary["window_count"])

        per_account.append(
            {
                "label": profile.label,
                "account_key": profile.account_key,
                "display_label": _display_label(profile.label, profile.email),
                "email": profile.email,
                "usage_tracking": usage,
                "summary": summary,
            }
        )

    per_account.sort(
        key=lambda item: (
            int((item.get("usage_tracking") or {}).get("lifetime_used") or 0),
            int((item.get("summary") or {}).get("total_wasted") or 0),
        ),
        reverse=True,
    )

    return JSONResponse(
        {
            "totals": totals,
            "per_account": per_account,
            "daily_rollover_trend": _daily_rollover_trend(all_rollovers),
        }
    )


@app.get("/api/usage/aggregate")
async def api_usage_aggregate(request: Request) -> JSONResponse:
    _require_internal_auth(request)
    snapshot = _build_cached_accounts_snapshot()
    return JSONResponse({"aggregate": snapshot["aggregate"], "current_label": snapshot["current_label"]})


@app.get("/api/public-stats")
async def api_public_stats() -> JSONResponse:
    profiles = _dedupe_profiles(list_profiles())
    auth_meta = _auth_file_metadata(settings.codex_auth_file())
    login = get_login_status()

    profiles_with_tokens = sum(1 for p in profiles if bool(p.access_token))
    profiles_with_email = sum(1 for p in profiles if bool(p.email))

    return JSONResponse(
        {
            "accounts_managed": len(profiles),
            "profiles_with_tokens": profiles_with_tokens,
            "profiles_with_email": profiles_with_email,
            "auth_file": {
                "exists": auth_meta["exists"],
                "modified_at": auth_meta["modified_at"],
            },
            "login_status": login.status,
        }
    )


@app.get("/internal/auths")
async def internal_auths(request: Request, label: str | None = None) -> JSONResponse:
    _require_internal_auth(request)
    profiles = list_profiles()

    if label:
        profile = _profile_for_label(label)
        if profile is None:
            raise HTTPException(status_code=404, detail="Label not found")
        return JSONResponse({"label": profile.label, "auth_json": profile.auth})

    return JSONResponse(
        {
            "accounts": [
                {"label": profile.label, "auth_json": profile.auth}
                for profile in profiles
            ]
        }
    )


def _persist_auth_and_save(label: str, auth_json: Any) -> None:
    try:
        persist_and_save_label(label, auth_json)
    except (AuthStoreError, CodexSwitchError) as exc:
        raise _to_switch_http_error(exc) from exc


def _to_switch_http_error(exc: Exception) -> HTTPException:
    if isinstance(exc, CodexSwitchError):
        return HTTPException(
            status_code=500,
            detail={
                "message": str(exc),
                "command": exc.command,
                "exit_code": exc.exit_code,
                "stderr": exc.stderr or None,
            },
        )
    return HTTPException(status_code=500, detail=str(exc))


def _migrate_usage_keys_from_labels() -> None:
    profiles = list_profiles()
    if not profiles:
        return
    id_map: dict[str, str] = {}
    for profile in profiles:
        if not profile.account_key:
            continue
        id_map[profile.label] = profile.account_key
    if not id_map:
        return
    migrated = migrate_account_ids(id_map)
    if migrated:
        logger.info("migrated %s legacy usage account id(s) from labels to canonical keys", migrated)


def _spa_or_legacy_index() -> HTMLResponse:
    index_path = _FRONTEND_DIST / "index.html"
    if index_path.exists():
        return HTMLResponse(index_path.read_text(encoding="utf-8"))
    return HTMLResponse(
        "<html><body><h1>Frontend build not found</h1>"
        "<p>Run <code>cd frontend && npm run build</code> and restart the service.</p>"
        "</body></html>",
        status_code=503,
    )


def _store_callback(payload: Any) -> Path:
    callback_dir = settings.callback_dir()
    callback_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    filename = f"callback-{timestamp}.json"
    path = callback_dir / filename

    try:
        path.write_text(json.dumps(payload, indent=2, sort_keys=True))
    except OSError as exc:
        raise HTTPException(status_code=500, detail=f"Unable to store callback: {exc}") from exc

    return path


def _expected_state_from_auth_url(auth_url: str | None) -> str | None:
    if not auth_url:
        return None
    try:
        parsed = urlparse(auth_url)
        states = parse_qs(parsed.query).get("state", [])
    except Exception:
        return None
    if not states:
        return None
    state = str(states[0]).strip()
    return state or None


async def _exchange_code_for_token(
    code: str, code_verifier: str, redirect_uri: str | None
) -> dict[str, Any]:
    if not settings.openai_token_url or not settings.openai_client_id:
        raise HTTPException(
            status_code=400,
            detail="OPENAI_TOKEN_URL and OPENAI_CLIENT_ID must be configured",
        )

    data: dict[str, str] = {
        "grant_type": "authorization_code",
        "client_id": settings.openai_client_id,
        "code": code,
        "code_verifier": code_verifier,
    }

    if redirect_uri:
        data["redirect_uri"] = redirect_uri

    if settings.openai_client_secret:
        data["client_secret"] = settings.openai_client_secret

    async with httpx.AsyncClient(timeout=20) as client:
        response = await client.post(
            settings.openai_token_url,
            data=data,
            headers={"Accept": "application/json"},
        )

    if response.status_code >= 400:
        raise HTTPException(
            status_code=response.status_code,
            detail=response.text.strip() or "Token exchange failed",
        )

    try:
        return response.json()
    except ValueError as exc:
        raise HTTPException(status_code=500, detail="Invalid token response") from exc


def _require_internal_auth(request: Request) -> None:
    configured_token = (settings.internal_api_token or "").strip()
    if not configured_token:
        raise HTTPException(
            status_code=503,
            detail="API key is required for this action, but INTERNAL_API_TOKEN is not configured on the server.",
        )

    if _has_valid_internal_api_token(request):
        return
    if request.headers.get("authorization", "").lower().startswith("bearer "):
        raise HTTPException(status_code=403, detail="Invalid API key")
    if request.headers.get("x-api-key", "").strip():
        raise HTTPException(status_code=403, detail="Invalid API key")

    raise HTTPException(
        status_code=401,
        detail="API key required. Provide Authorization: Bearer <token> or X-API-Key header.",
    )


def _has_valid_internal_api_token(request: Request) -> bool:
    configured_token = (settings.internal_api_token or "").strip()
    if not configured_token:
        return False

    x_api_key = request.headers.get("x-api-key", "").strip()
    if x_api_key:
        return secrets.compare_digest(x_api_key, configured_token)

    auth_header = request.headers.get("authorization", "")
    if auth_header.lower().startswith("bearer "):
        token = auth_header.split(" ", 1)[1].strip()
        return secrets.compare_digest(token, configured_token)
    return False


def _require_internal_auth_or_query(request: Request) -> None:
    if _has_valid_internal_api_token(request):
        return
    configured_token = (settings.internal_api_token or "").strip()
    if not configured_token:
        return
    query_token = str(
        request.query_params.get("api_key")
        or request.query_params.get("token")
        or ""
    ).strip()
    if query_token and secrets.compare_digest(query_token, configured_token):
        return
    raise HTTPException(status_code=401, detail="API key required for stream endpoint.")


def _build_cached_accounts_snapshot(
    *,
    profiles: list[AccountProfile] | None = None,
) -> dict[str, Any]:
    profs = _dedupe_profiles(profiles if profiles is not None else list_profiles())
    current = None
    try:
        current = _resolve_current_label(read_current_auth(), profs)
    except CodexCLIError:
        current = None

    accounts = [
        _account_payload(profile, current, usage_tracking=_usage_tracking_payload(profile.account_key))
        for profile in profs
    ]
    aggregate = _compute_aggregate(accounts)
    return {"accounts": accounts, "current_label": current, "aggregate": aggregate}


def _account_payload(
    profile: AccountProfile,
    current_label_name: str | None,
    *,
    usage_tracking: dict[str, Any] | None,
) -> dict[str, Any]:
    refresh_status = _refresh_status_payload(profile.account_key, usage_tracking)
    usage_limit = int((usage_tracking or {}).get("usage_limit") or 0)
    usage_used = int((usage_tracking or {}).get("usage_in_window") or 0)
    cached_primary_percent = (usage_tracking or {}).get("primary_used_percent")
    cached_secondary_percent = (usage_tracking or {}).get("secondary_used_percent")
    percent = (
        float(cached_primary_percent)
        if isinstance(cached_primary_percent, (int, float))
        else (round((usage_used / usage_limit) * 100, 1) if usage_limit > 0 else 0.0)
    )
    primary_reset = (usage_tracking or {}).get("primary_resets_at") or (usage_tracking or {}).get("rate_limit_refresh_at")
    secondary_reset = (usage_tracking or {}).get("secondary_resets_at")
    rate_limits = {
        "primary": {
            "limit": usage_limit,
            "used": usage_used,
            "remaining": max(usage_limit - usage_used, 0),
            "percent": percent,
            "resetsAt": primary_reset,
        },
        "secondary": (
            {
                "limit": None,
                "used": None,
                "remaining": None,
                "percent": float(cached_secondary_percent),
                "resetsAt": secondary_reset,
            }
            if isinstance(cached_secondary_percent, (int, float))
            else None
        ),
    }
    return {
        "label": profile.label,
        "account_key": profile.account_key,
        "display_label": _display_label(profile.label, profile.email),
        "email": profile.email,
        "is_current": profile.label == current_label_name,
        "rate_limits": rate_limits,
        "usage_tracking": usage_tracking,
        "refresh_status": refresh_status,
    }


def _compute_aggregate(accounts: list[dict[str, Any]]) -> dict[str, Any]:
    total_used = 0
    total_limit = 0
    lifetime_used = 0
    total_wasted = 0
    stale_count = 0
    failed_count = 0
    last_refresh: str | None = None

    for account in accounts:
        usage = account.get("usage_tracking") or {}
        total_used += int(usage.get("usage_in_window") or 0)
        total_limit += int(usage.get("usage_limit") or 0)
        lifetime_used += int(usage.get("lifetime_used") or 0)
        refresh_status = account.get("refresh_status") or {}
        if refresh_status.get("is_stale"):
            stale_count += 1
        if refresh_status.get("state") == "failed":
            failed_count += 1
        last_success_at = refresh_status.get("last_success_at")
        if isinstance(last_success_at, str):
            if last_refresh is None or last_success_at > last_refresh:
                last_refresh = last_success_at
        rollovers = list_usage_rollovers(str(account.get("account_key") or ""))
        total_wasted += sum(int(item.get("usage_wasted") or 0) for item in rollovers)

    remaining = max(total_limit - total_used, 0)
    utilization = round((total_used / total_limit) * 100, 2) if total_limit > 0 else 0.0
    return {
        "accounts": len(accounts),
        "total_current_window_used": total_used,
        "total_current_window_limit": total_limit,
        "total_remaining": remaining,
        "aggregate_utilization_percent": utilization,
        "lifetime_total_used": lifetime_used,
        "total_wasted": total_wasted,
        "stale_accounts": stale_count,
        "failed_accounts": failed_count,
        "last_refresh_time": _LAST_REFRESH_COMPLETED_AT or last_refresh,
    }


def _sse_event(event_name: str, payload: dict[str, Any]) -> str:
    return f"event: {event_name}\ndata: {json.dumps(payload, separators=(',', ':'))}\n\n"


def _refresh_status_payload(
    account_key: str,
    usage_tracking: dict[str, Any] | None,
) -> dict[str, Any]:
    status = dict(_LAST_REFRESH_STATUS_BY_KEY.get(account_key) or {})
    status.setdefault("state", "idle")
    status.setdefault("last_attempt_at", None)
    status.setdefault("last_success_at", None)
    status.setdefault("last_error", None)

    updated_at = (
        (usage_tracking or {}).get("last_usage_sync_at")
        or (usage_tracking or {}).get("updated_at")
    )
    is_stale = True
    if isinstance(updated_at, str):
        with suppress(ValueError):
            dt = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            age = (datetime.now(timezone.utc) - dt).total_seconds()
            is_stale = age > _USAGE_STALE_SECONDS
    status["is_stale"] = is_stale
    return status


def _mark_refresh_status(account_key: str, *, ok: bool, error: str | None) -> None:
    now_iso = datetime.now(timezone.utc).isoformat()
    state = "ok" if ok else "failed"
    entry = _LAST_REFRESH_STATUS_BY_KEY.setdefault(account_key, {})
    entry["state"] = state
    entry["last_attempt_at"] = now_iso
    if ok:
        entry["last_success_at"] = now_iso
        entry["last_error"] = None
    else:
        entry["last_error"] = error


def _mark_refresh_completed() -> None:
    global _LAST_REFRESH_COMPLETED_AT
    _LAST_REFRESH_COMPLETED_AT = datetime.now(timezone.utc).isoformat()


def _web_login_enabled() -> bool:
    return bool(
        (settings.web_login_username or "").strip()
        and (settings.web_login_password or "").strip()
        and (settings.web_login_session_secret or "").strip()
    )


def _is_login_exempt_path(path: str) -> bool:
    if path in {"/", "/health", "/login", "/ui"}:
        return True
    if path.startswith("/ui/"):
        return True
    if path.startswith("/oauth/callback") or path.startswith("/auth/callback"):
        return True
    if path.startswith("/docs") or path.startswith("/openapi.json"):
        return True
    return False


def _trusted_proxy_hosts() -> set[str]:
    raw = settings.trusted_proxy_ips or ""
    return {value.strip() for value in raw.split(",") if value.strip()}


def _parse_networks(value: str) -> list[ipaddress._BaseNetwork]:
    networks: list[ipaddress._BaseNetwork] = []
    for chunk in value.split(","):
        text = chunk.strip()
        if not text:
            continue
        try:
            networks.append(ipaddress.ip_network(text, strict=False))
        except ValueError:
            continue
    return networks


def _resolve_client_ip(request: Request) -> str | None:
    direct = request.client.host if request.client else None
    if not direct:
        return None

    trusted = _trusted_proxy_hosts()
    if direct not in trusted:
        return direct

    forwarded = request.headers.get("x-forwarded-for", "")
    if not forwarded:
        return direct
    first = forwarded.split(",")[0].strip()
    return first or direct


def _is_internal_request(request: Request) -> bool:
    ip_text = _resolve_client_ip(request)
    if not ip_text:
        return False
    try:
        ip_value = ipaddress.ip_address(ip_text)
    except ValueError:
        return False

    for network in _parse_networks(settings.internal_network_cidrs):
        if ip_value in network:
            return True
    return False


def _web_session_sign(payload: str) -> str:
    secret = (settings.web_login_session_secret or "").encode("utf-8")
    return hmac.new(secret, payload.encode("utf-8"), sha256).hexdigest()


def _build_web_session_token() -> str:
    now = int(datetime.now(timezone.utc).timestamp())
    expires = now + max(settings.web_login_session_ttl_seconds, 60)
    nonce = secrets.token_hex(8)
    payload = f"{expires}.{nonce}"
    sig = _web_session_sign(payload)
    return f"{payload}.{sig}"


def _has_valid_web_session(request: Request) -> bool:
    token = request.cookies.get(settings.web_login_cookie_name, "")
    if not token:
        return False
    parts = token.split(".")
    if len(parts) != 3:
        return False
    expires_text, nonce, sig = parts
    payload = f"{expires_text}.{nonce}"
    expected = _web_session_sign(payload)
    if not hmac.compare_digest(sig, expected):
        return False
    try:
        expires = int(expires_text)
    except ValueError:
        return False
    now = int(datetime.now(timezone.utc).timestamp())
    return expires > now


def _verify_web_credentials(username: str, password: str) -> bool:
    expected_user = (settings.web_login_username or "").strip()
    expected_pass = settings.web_login_password or ""
    return secrets.compare_digest(username, expected_user) and secrets.compare_digest(
        password, expected_pass
    )


def _safe_next_path(next_path: str) -> str:
    candidate = (next_path or "/").strip()
    if not candidate.startswith("/"):
        return "/"
    if candidate.startswith("//"):
        return "/"
    return candidate


def _set_web_session_cookie(request: Request, response: RedirectResponse) -> None:
    secure = request.headers.get("x-forwarded-proto", request.url.scheme) == "https"
    response.set_cookie(
        key=settings.web_login_cookie_name,
        value=_build_web_session_token(),
        httponly=True,
        secure=secure,
        samesite="lax",
        max_age=max(settings.web_login_session_ttl_seconds, 60),
        path="/",
    )


async def _fetch_rate_limits(
    client: httpx.AsyncClient, token: str
) -> dict[str, Any]:
    headers = {"Authorization": f"Bearer {token}"}
    if settings.openai_organization:
        headers["OpenAI-Organization"] = settings.openai_organization
    if settings.openai_project:
        headers["OpenAI-Project"] = settings.openai_project

    try:
        response = await client.get(settings.rate_limit_probe_url, headers=headers)
    except httpx.RequestError as exc:
        return {"error": str(exc)}

    rate_headers = {
        key.lower(): value
        for key, value in response.headers.items()
        if key.lower().startswith("x-ratelimit-")
    }

    requests_remaining = _parse_int(rate_headers.get("x-ratelimit-remaining-requests"))
    requests_limit = _parse_int(rate_headers.get("x-ratelimit-limit-requests"))
    tokens_remaining = _parse_int(rate_headers.get("x-ratelimit-remaining-tokens"))
    tokens_limit = _parse_int(rate_headers.get("x-ratelimit-limit-tokens"))

    return {
        "status": response.status_code,
        "requests": _format_limit(
            requests_remaining,
            requests_limit,
            rate_headers.get("x-ratelimit-reset-requests"),
        ),
        "tokens": _format_limit(
            tokens_remaining,
            tokens_limit,
            rate_headers.get("x-ratelimit-reset-tokens"),
        ),
        "raw_headers": rate_headers,
        "error": response.text.strip() if response.status_code >= 400 else None,
    }


def _has_limit_data(rate_info: dict[str, Any]) -> bool:
    if not isinstance(rate_info, dict):
        return False
    return bool(rate_info.get("requests") or rate_info.get("tokens"))


def _safe_read_current_auth() -> dict[str, Any] | None:
    try:
        return read_current_auth()
    except CodexCLIError:
        return None


def _normalize_session_limit_payload(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {"raw": payload}

    limits = payload.get("rateLimits")
    if isinstance(limits, dict):
        payload = limits

    primary = payload.get("primary")
    secondary = payload.get("secondary")
    return {
        "primary": primary if isinstance(primary, dict) else None,
        "secondary": secondary if isinstance(secondary, dict) else None,
        "raw": payload,
    }


def _fetch_session_limits_for_profiles(
    profiles: list[AccountProfile], baseline_auth: dict[str, Any] | None
) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    if not profiles:
        return out

    try:
        for profile in profiles:
            try:
                switch_label(profile.label)
                result = read_rate_limits_via_app_server()
                out[profile.label] = _normalize_session_limit_payload(result.rate_limits)
            except (CodexSwitchError, CodexCLIError) as exc:
                out[profile.label] = {"error": str(exc)}
    finally:
        if baseline_auth is not None:
            try:
                persist_current_auth(baseline_auth)
            except AuthStoreError:
                pass

    return out


def _format_limit(
    remaining: int | None, limit: int | None, reset: str | None
) -> dict[str, Any] | None:
    if remaining is None and limit is None and reset is None:
        return None
    percent = None
    if remaining is not None and limit:
        percent = round((remaining / limit) * 100, 1)
    return {
        "remaining": remaining,
        "limit": limit,
        "percent": percent,
        "reset": reset,
    }


def _parse_int(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _touch_profiles_usage(profiles: list[AccountProfile]) -> None:
    now = datetime.now(timezone.utc)
    for profile in profiles:
        _touch_account_usage(profile=profile, now=now)


def _touch_account_usage(
    *,
    profile: AccountProfile,
    now: datetime | None = None,
) -> None:
    moment = now or datetime.now(timezone.utc)
    try:
        ensure_account(
            profile.account_key,
            moment,
            provider_account_id=profile.provider_account_id,
            name=profile.name or profile.email,
            rate_limit_window_type=None,
            usage_limit=None,
        )
        # Activity-driven refresh path; delta may be 0 when no concrete usage delta is known.
        record_account_usage(profile.account_key, 0, now=moment)
    except Exception:
        logger.exception("Unable to touch usage account for key=%s", profile.account_key)


def _sync_profile_usage_from_probe(profile: AccountProfile, rate_info: dict[str, Any]) -> None:
    requests_data = rate_info.get("requests")
    if not isinstance(requests_data, dict):
        return
    limit = requests_data.get("limit")
    remaining = requests_data.get("remaining")
    reset = requests_data.get("reset")
    if not isinstance(limit, int) or not isinstance(remaining, int):
        return
    used = max(limit - remaining, 0)
    refresh_at = _parse_probe_reset_to_iso(reset)
    try:
        sync_account_usage_snapshot(
            profile.account_key,
            usage_limit=limit,
            usage_used=used,
            rate_limit_window_type="daily",
            rate_limit_refresh_at=refresh_at,
            provider_account_id=profile.provider_account_id,
            name=profile.name or profile.email,
            now=datetime.now(timezone.utc),
        )
    except Exception:
        logger.exception("Unable to sync probe usage for key=%s", profile.account_key)


def _sync_profile_usage_from_session_limits(profile: AccountProfile, rate_info: dict[str, Any]) -> None:
    snapshot = _extract_limit_snapshot(rate_info)
    primary = rate_info.get("primary") if isinstance(rate_info, dict) and isinstance(rate_info.get("primary"), dict) else None
    secondary = rate_info.get("secondary") if isinstance(rate_info, dict) and isinstance(rate_info.get("secondary"), dict) else None
    primary_percent = _find_float(primary, {"used_percent", "usedpercent", "percent", "usedpct"})
    secondary_percent = _find_float(secondary, {"used_percent", "usedpercent", "percent", "usedpct"})
    primary_resets_at = _find_datetime_any(primary, {"resets_at", "resetsat", "reset_at", "resetat", "next_reset_at", "nextresetat", "refresh_at", "refreshat"})
    secondary_resets_at = _find_datetime_any(secondary, {"resets_at", "resetsat", "reset_at", "resetat", "next_reset_at", "nextresetat", "refresh_at", "refreshat"})

    if (
        primary_percent is not None
        or secondary_percent is not None
        or primary_resets_at is not None
        or secondary_resets_at is not None
    ):
        try:
            sync_account_rate_limit_percentages(
                profile.account_key,
                primary_used_percent=primary_percent,
                primary_resets_at=primary_resets_at,
                secondary_used_percent=secondary_percent,
                secondary_resets_at=secondary_resets_at,
                provider_account_id=profile.provider_account_id,
                name=profile.name or profile.email,
                now=datetime.now(timezone.utc),
            )
        except Exception:
            logger.exception("Unable to sync percentage limits for key=%s", profile.account_key)

    if (
        snapshot["usage_limit"] is None
        and snapshot["usage_used"] is None
        and snapshot["refresh_at"] is None
    ):
        return
    try:
        sync_account_usage_snapshot(
            profile.account_key,
            usage_limit=snapshot["usage_limit"],
            usage_used=snapshot["usage_used"],
            rate_limit_window_type=snapshot["window_type"],
            rate_limit_refresh_at=snapshot["refresh_at"],
            provider_account_id=profile.provider_account_id,
            name=profile.name or profile.email,
            now=datetime.now(timezone.utc),
        )
    except Exception:
        logger.exception("Unable to sync session usage for key=%s", profile.account_key)


def _parse_probe_reset_to_iso(reset: Any) -> str | None:
    if not isinstance(reset, str):
        return None
    raw = reset.strip()
    if not raw:
        return None
    try:
        # Common x-ratelimit reset format is relative durations (e.g. "15s", "1m").
        suffix = raw[-1].lower()
        value = int(raw[:-1])
        multiplier = {"s": 1, "m": 60, "h": 3600}.get(suffix)
        if multiplier is None:
            return None
        dt = datetime.now(timezone.utc).timestamp() + (value * multiplier)
        return datetime.fromtimestamp(dt, tz=timezone.utc).isoformat()
    except (ValueError, OverflowError):
        return None


def _extract_limit_snapshot(rate_limits: Any) -> dict[str, Any]:
    payload = rate_limits if isinstance(rate_limits, dict) else {}

    primary = payload.get("primary") if isinstance(payload.get("primary"), dict) else None
    secondary = payload.get("secondary") if isinstance(payload.get("secondary"), dict) else None
    candidate = primary or secondary or payload

    usage_limit = _find_int(candidate, {"limit", "max", "quota", "total"})
    usage_used = _find_int(candidate, {"used", "consumed"})
    if usage_used is None:
        remaining = _find_int(candidate, {"remaining", "left"})
        if usage_limit is not None and remaining is not None:
            usage_used = max(usage_limit - remaining, 0)
    refresh_at = _find_iso_datetime(
        candidate, {"refresh_at", "reset_at", "next_reset_at", "resets_at"}
    )
    window_type = _find_str(candidate, {"window_type", "window", "period"}) or "daily"

    return {
        "usage_limit": usage_limit,
        "usage_used": usage_used,
        "refresh_at": refresh_at,
        "window_type": window_type,
    }


def _find_int(payload: Any, keys: set[str]) -> int | None:
    if isinstance(payload, dict):
        for key, value in payload.items():
            normalized = key.lower().replace("-", "_")
            if normalized in keys and isinstance(value, int):
                return value
            found = _find_int(value, keys)
            if found is not None:
                return found
    elif isinstance(payload, list):
        for item in payload:
            found = _find_int(item, keys)
            if found is not None:
                return found
    return None


def _find_float(payload: Any, keys: set[str]) -> float | None:
    if isinstance(payload, dict):
        for key, value in payload.items():
            normalized = key.lower().replace("-", "_")
            if normalized in keys and isinstance(value, (int, float)):
                return float(value)
            found = _find_float(value, keys)
            if found is not None:
                return found
    elif isinstance(payload, list):
        for item in payload:
            found = _find_float(item, keys)
            if found is not None:
                return found
    return None


def _find_str(payload: Any, keys: set[str]) -> str | None:
    if isinstance(payload, dict):
        for key, value in payload.items():
            normalized = key.lower().replace("-", "_")
            if normalized in keys and isinstance(value, str) and value.strip():
                return value.strip()
            found = _find_str(value, keys)
            if found:
                return found
    elif isinstance(payload, list):
        for item in payload:
            found = _find_str(item, keys)
            if found:
                return found
    return None


def _find_iso_datetime(payload: Any, keys: set[str]) -> str | None:
    value = _find_str(payload, keys)
    if not value:
        return None
    candidate = value.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(candidate)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.isoformat()


def _find_datetime_any(payload: Any, keys: set[str]) -> str | None:
    if isinstance(payload, dict):
        for key, value in payload.items():
            normalized = key.lower().replace("-", "_")
            if normalized in keys:
                if isinstance(value, (int, float)) and int(value) > 0:
                    dt = datetime.fromtimestamp(int(value), tz=timezone.utc)
                    return dt.isoformat()
                if isinstance(value, str):
                    candidate = value.strip()
                    if not candidate:
                        continue
                    if candidate.isdigit():
                        dt = datetime.fromtimestamp(int(candidate), tz=timezone.utc)
                        return dt.isoformat()
                    candidate = candidate.replace("Z", "+00:00")
                    try:
                        dt = datetime.fromisoformat(candidate)
                    except ValueError:
                        continue
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    else:
                        dt = dt.astimezone(timezone.utc)
                    return dt.isoformat()
            found = _find_datetime_any(value, keys)
            if found:
                return found
    elif isinstance(payload, list):
        for item in payload:
            found = _find_datetime_any(item, keys)
            if found:
                return found
    return None


def _account_provider_id(account_payload: dict[str, Any] | None) -> str | None:
    if not isinstance(account_payload, dict):
        return None
    for key in ["id", "account_id", "accountId", "user_id", "userId"]:
        value = account_payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _account_name(account_payload: dict[str, Any] | None, fallback_email: str | None) -> str | None:
    if isinstance(account_payload, dict):
        for key in ["name", "display_name", "displayName", "email"]:
            value = account_payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return fallback_email


def _usage_tracking_payload(account_key: str) -> dict[str, Any] | None:
    state = get_account(account_key)
    if state is None:
        return None
    return {
        "rate_limit_window_type": state.rate_limit_window_type,
        "usage_limit": state.usage_limit,
        "usage_in_window": state.usage_in_window,
        "lifetime_used": state.lifetime_used,
        "rate_limit_refresh_at": state.rate_limit_refresh_at,
        "rate_limit_last_refreshed_at": state.rate_limit_last_refreshed_at,
        "primary_used_percent": state.primary_used_percent,
        "primary_resets_at": state.primary_resets_at,
        "secondary_used_percent": state.secondary_used_percent,
        "secondary_resets_at": state.secondary_resets_at,
        "last_usage_sync_at": state.last_usage_sync_at,
        "created_at": state.created_at,
        "updated_at": state.updated_at,
    }


def _persist_current_auth_to_profile(
    *,
    desired_label: str | None,
    create_if_missing: bool,
    auth_json: dict[str, Any] | None = None,
) -> PersistCurrentAuthResult:
    resolved_auth = auth_json if isinstance(auth_json, dict) else read_current_auth()
    identity = extract_account_identity(resolved_auth)
    email = identity.email
    existing = set(list_labels())
    profiles = _dedupe_profiles(list_profiles())
    matched_profile = _find_matching_profile(profiles, resolved_auth, identity)

    if desired_label is not None:
        label = desired_label.strip()
        if not label:
            raise ValueError("label cannot be empty")
        created_new_profile = matched_profile is None
    elif matched_profile is not None:
        label = matched_profile.label
        created_new_profile = False
    elif create_if_missing:
        label = derive_label(email or "account", existing_labels=existing)
        created_new_profile = True
    else:
        return PersistCurrentAuthResult(
            persisted=False,
            skipped=True,
            reason="no_matching_profile",
            label=None,
            account_key=identity.account_key,
            email=email,
            matched_existing_profile=False,
            created_new_profile=False,
            up_to_date=False,
            codex_switch=None,
        )

    current_for_label = next((p for p in profiles if p.label == label), None)
    if current_for_label is not None and current_for_label.auth == resolved_auth:
        _touch_account_usage(profile=current_for_label)
        return PersistCurrentAuthResult(
            persisted=False,
            skipped=True,
            reason="up_to_date",
            label=label,
            account_key=current_for_label.account_key,
            email=email,
            matched_existing_profile=matched_profile is not None,
            created_new_profile=False,
            up_to_date=True,
            codex_switch=None,
        )

    persist_current_auth(resolved_auth)
    switch_save = save_current_auth_under_label(label)
    profile_for_usage = matched_profile or AccountProfile(
        label=label,
        path=settings.profiles_dir() / f"{label}.json",
        auth=resolved_auth,
        account_key=identity.account_key,
        subject=identity.subject,
        user_id=identity.user_id,
        provider_account_id=identity.account_id,
        name=identity.name,
        access_token=extract_access_token(resolved_auth),
        email=email,
    )
    _touch_account_usage(profile=profile_for_usage)
    return PersistCurrentAuthResult(
        persisted=True,
        skipped=False,
        reason="persisted",
        label=label,
        account_key=profile_for_usage.account_key,
        email=email,
        matched_existing_profile=matched_profile is not None,
        created_new_profile=created_new_profile,
        up_to_date=False,
        codex_switch={
            "command": switch_save.command,
            "exit_code": switch_save.returncode,
            "stdout": switch_save.stdout,
        },
    )


def _find_matching_profile(
    profiles: list[AccountProfile],
    auth_json: dict[str, Any],
    identity: Any,
) -> AccountProfile | None:
    incoming_email = (identity.email or "").strip().lower() if getattr(identity, "email", None) else ""
    incoming_key = getattr(identity, "account_key", None)
    incoming_token = extract_access_token(auth_json)
    for profile in profiles:
        if incoming_key and profile.account_key == incoming_key:
            return profile
        profile_email = (profile.email or "").strip().lower()
        if incoming_email and profile_email and incoming_email == profile_email:
            return profile
        if incoming_token and profile.access_token and incoming_token == profile.access_token:
            return profile
    return None


def _dedupe_profiles(profiles: list[AccountProfile]) -> list[AccountProfile]:
    deduped: dict[str, AccountProfile] = {}
    try:
        current_label_name = current_label()
    except CodexSwitchError:
        current_label_name = None
    for profile in profiles:
        key = _profile_identity_key(profile)
        if key not in deduped:
            deduped[key] = profile
            continue
        current = deduped[key]
        # Prefer current active profile, otherwise keep latest by file mtime.
        if current_label_name == profile.label:
            deduped[key] = profile
            continue
        try:
            if profile.path.stat().st_mtime > current.path.stat().st_mtime:
                deduped[key] = profile
        except OSError:
            pass
    return sorted(deduped.values(), key=lambda p: p.label)


def _profile_identity_key(profile: AccountProfile) -> str:
    if profile.account_key and profile.account_key != "unknown":
        return f"account:{profile.account_key}"
    email = (profile.email or "").strip().lower()
    if email:
        return f"email:{email}"
    token = (profile.access_token or "").strip()
    if token:
        return f"token:{token}"
    return f"label:{profile.label}"


def _rollover_summary(rollovers: list[dict[str, Any]], usage: dict[str, Any] | None) -> dict[str, Any]:
    total_wasted = sum(int(item.get("usage_wasted") or 0) for item in rollovers)
    total_used_completed = sum(int(item.get("usage_used") or 0) for item in rollovers)
    total_limit_completed = sum(int(item.get("usage_limit") or 0) for item in rollovers)
    window_count = len(rollovers)
    avg_completed_utilization_percent = None
    if total_limit_completed > 0:
        avg_completed_utilization_percent = round(
            (total_used_completed / total_limit_completed) * 100, 2
        )

    current_limit = int((usage or {}).get("usage_limit") or 0)
    current_used = int((usage or {}).get("usage_in_window") or 0)
    current_wasted_if_rollover_now = max(current_limit - current_used, 0)

    return {
        "window_count": window_count,
        "total_wasted": total_wasted,
        "total_used_completed": total_used_completed,
        "total_limit_completed": total_limit_completed,
        "avg_completed_utilization_percent": avg_completed_utilization_percent,
        "current_wasted_if_rollover_now": current_wasted_if_rollover_now,
    }


def _daily_rollover_trend(rollovers: list[dict[str, Any]]) -> list[dict[str, Any]]:
    buckets: dict[str, dict[str, int]] = {}
    for item in rollovers:
        ended_at = str(item.get("window_ended_at") or "")
        day = ended_at[:10]
        if len(day) != 10:
            continue
        bucket = buckets.setdefault(
            day,
            {"usage_used": 0, "usage_wasted": 0, "usage_limit": 0, "windows": 0},
        )
        bucket["usage_used"] += int(item.get("usage_used") or 0)
        bucket["usage_wasted"] += int(item.get("usage_wasted") or 0)
        bucket["usage_limit"] += int(item.get("usage_limit") or 0)
        bucket["windows"] += 1

    points: list[dict[str, Any]] = []
    for day in sorted(buckets.keys())[-60:]:
        bucket = buckets[day]
        utilization = None
        if bucket["usage_limit"] > 0:
            utilization = round((bucket["usage_used"] / bucket["usage_limit"]) * 100, 2)
        points.append(
            {
                "day": day,
                "usage_used": bucket["usage_used"],
                "usage_wasted": bucket["usage_wasted"],
                "usage_limit": bucket["usage_limit"],
                "windows": bucket["windows"],
                "utilization_percent": utilization,
            }
        )
    return points


def _profile_for_label(label: str) -> AccountProfile | None:
    wanted = label.strip()
    for profile in list_profiles():
        if profile.label == wanted:
            return profile
    return None


def _resolve_current_label(
    current_auth: dict[str, Any], profiles: list[AccountProfile]
) -> str | None:
    try:
        label = current_label()
        if label:
            return label
    except CodexSwitchError:
        pass

    current_identity = extract_account_identity(current_auth)
    token = extract_access_token(current_auth)
    for profile in profiles:
        if current_identity.account_key and profile.account_key == current_identity.account_key:
            return profile.label
        if profile.auth == current_auth:
            return profile.label
        if token and profile.access_token and profile.access_token == token:
            return profile.label
    return None


def _display_label(label: str | None, email: str | None) -> str | None:
    if not label:
        return None
    # For legacy auto-generated UUID labels, prefer email local-part as display.
    if re.fullmatch(
        r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}",
        label,
    ):
        if not email:
            return label
        local = email.split("@", 1)[0].strip().lower()
        return local or label
    return label


def _validate_relay_finalized_auth(
    auth_json: dict[str, Any],
    *,
    started_at_iso: str | None,
) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    access_token = extract_access_token(auth_json)
    if not access_token:
        return {"ok": False, "reason": "missing_access_token", "message": "No access token found in finalized auth."}

    access_claims = decode_jwt_claims(access_token)
    if not access_claims:
        return {"ok": False, "reason": "invalid_access_token", "message": "Unable to decode finalized access token."}

    exp = access_claims.get("exp")
    iat = access_claims.get("iat")
    exp_dt = None
    if isinstance(exp, int):
        exp_dt = datetime.fromtimestamp(exp, tz=timezone.utc)
        if exp_dt <= now:
            return {
                "ok": False,
                "reason": "access_token_expired",
                "message": "Finalized access token is already expired.",
                "access_exp_at": exp_dt.isoformat(),
            }

    started_dt = None
    if started_at_iso:
        with suppress(ValueError):
            started_dt = datetime.fromisoformat(started_at_iso.replace("Z", "+00:00"))
            if started_dt.tzinfo is None:
                started_dt = started_dt.replace(tzinfo=timezone.utc)
            else:
                started_dt = started_dt.astimezone(timezone.utc)
    iat_dt = None
    if isinstance(iat, int):
        iat_dt = datetime.fromtimestamp(iat, tz=timezone.utc)
    # If relay was just completed but token was minted long before this login session,
    # treat it as stale and avoid persisting a false "refreshed" auth.
    if started_dt and iat_dt and iat_dt < (started_dt.replace(microsecond=0)):
        return {
            "ok": False,
            "reason": "stale_access_token",
            "message": "Finalized access token predates the current login session.",
            "access_iat": iat_dt.isoformat(),
            "login_started_at": started_dt.isoformat(),
        }

    id_token = extract_id_token(auth_json)
    email = None
    if id_token:
        id_claims = decode_jwt_claims(id_token)
        if id_claims:
            maybe_email = id_claims.get("email")
            if isinstance(maybe_email, str) and maybe_email.strip():
                email = maybe_email.strip().lower()

    result: dict[str, Any] = {"ok": True, "reason": "fresh_access_token"}
    if exp_dt:
        result["access_exp_at"] = exp_dt.isoformat()
    if iat_dt:
        result["access_iat"] = iat_dt.isoformat()
    if email:
        result["email"] = email
    return result


def _auth_file_metadata(path: Path) -> dict[str, Any]:
    exists = path.exists()
    stat = path.stat() if exists else None
    modified_at = (
        datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat() if stat else None
    )
    return {
        "path": str(path),
        "exists": exists,
        "size_bytes": stat.st_size if stat else None,
        "modified_at": modified_at,
    }


def _render_login(next_path: str) -> str:
    safe_next = _safe_next_path(next_path)
    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Auth Manager | Login</title>
    <link rel="preconnect" href="https://fonts.googleapis.com" />
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
    <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@400;600;700&family=Inter:wght@400;500&display=swap" rel="stylesheet" />
    <style>
      :root {{
        --bg: #030712;
        --glass: rgba(17, 24, 39, 0.7);
        --glass-border: rgba(255, 255, 255, 0.08);
        --text: #f8fafc;
        --muted: #94a3b8;
        --accent: #10b981;
        --accent-glow: rgba(16, 185, 129, 0.4);
      }}
      * {{ box-sizing: border-box; }}
      body {{
        margin: 0;
        min-height: 100vh;
        display: flex;
        align-items: center;
        justify-content: center;
        font-family: 'Inter', sans-serif;
        background-color: var(--bg);
        background-image: 
          radial-gradient(at 0% 0%, rgba(16, 185, 129, 0.15) 0px, transparent 50%),
          radial-gradient(at 100% 0%, rgba(59, 130, 246, 0.15) 0px, transparent 50%),
          radial-gradient(at 100% 100%, rgba(16, 185, 129, 0.1) 0px, transparent 50%),
          radial-gradient(at 0% 100%, rgba(59, 130, 246, 0.1) 0px, transparent 50%);
        color: var(--text);
      }}
      .login-container {{
        width: min(400px, 90vw);
        padding: 40px;
        background: var(--glass);
        backdrop-filter: blur(20px);
        -webkit-backdrop-filter: blur(20px);
        border: 1px solid var(--glass-border);
        border-radius: 24px;
        box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.5);
        animation: fadeIn 0.6s ease-out;
      }}
      @keyframes fadeIn {{
        from {{ opacity: 0; transform: translateY(10px); }}
        to {{ opacity: 1; transform: translateY(0); }}
      }}
      h1 {{ 
        margin: 0 0 8px; 
        font-family: 'Outfit', sans-serif;
        font-size: 28px; 
        font-weight: 700;
        background: linear-gradient(135deg, #fff 0%, #94a3b8 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
      }}
      p {{ 
        margin: 0 0 32px; 
        color: var(--muted); 
        font-size: 14px; 
        line-height: 1.6;
      }}
      .form-group {{ margin-bottom: 20px; }}
      label {{ 
        display: block; 
        margin-bottom: 8px; 
        font-size: 13px; 
        font-weight: 500;
        color: var(--muted); 
      }}
      input {{
        width: 100%;
        height: 48px;
        border-radius: 12px;
        border: 1px solid var(--glass-border);
        background: rgba(0, 0, 0, 0.2);
        color: var(--text);
        padding: 0 16px;
        font-size: 15px;
        transition: all 0.2s ease;
        outline: none;
      }}
      input:focus {{
        border-color: var(--accent);
        background: rgba(0, 0, 0, 0.3);
        box-shadow: 0 0 0 4px var(--accent-glow);
      }}
      button {{
        margin-top: 12px;
        width: 100%;
        height: 50px;
        border: none;
        border-radius: 14px;
        background: var(--accent);
        color: #064e3b;
        font-family: 'Outfit', sans-serif;
        font-weight: 700;
        font-size: 16px;
        cursor: pointer;
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
        box-shadow: 0 10px 15px -3px var(--accent-glow);
      }}
      button:hover {{
        transform: translateY(-2px);
        box-shadow: 0 15px 20px -5px var(--accent-glow);
        filter: brightness(1.1);
      }}
      button:active {{ transform: translateY(0); }}
      .footer {{
        margin-top: 24px;
        text-align: center;
        font-size: 12px;
        color: var(--muted);
      }}
    </style>
  </head>
  <body>
    <div class="login-container">
      <form method="post" action="/login">
        <h1>Welcome Back</h1>
        <p>Sign in to manage your auth profiles. Access is restricted to trusted networks.</p>
        <input type="hidden" name="next" value="{safe_next}" />
        <div class="form-group">
          <label for="username">Username</label>
          <input id="username" name="username" type="text" autocomplete="username" placeholder="Enter username" required />
        </div>
        <div class="form-group">
          <label for="password">Password</label>
          <input id="password" name="password" type="password" autocomplete="current-password" placeholder="••••••••" required />
        </div>
        <button type="submit">Sign In</button>
        <div class="footer">
          Auth Manager &copy; 2026
        </div>
      </form>
    </div>
  </body>
</html>"""


def _render_account_usage_page(label: str) -> str:
    template = """<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Account Usage History</title>
    <link rel="preconnect" href="https://fonts.googleapis.com" />
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
    <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@500;600;700&family=Inter:wght@400;500;600&display=swap" rel="stylesheet" />
    <style>
      *,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
      :root{
        --bg:#0A0A0A;--surface:#171717;--surface-hi:#202020;--border:#2B2B2B;
        --text:#F1F5F9;--dim:#94A3B8;--green:#10B981;--amber:#F59E0B;--red:#EF4444;--blue:#38BDF8;
      }
      body{font-family:'Inter',system-ui,sans-serif;color:var(--text);background:var(--bg);min-height:100vh}
      .wrap{max-width:1100px;margin:0 auto;padding:24px 18px 52px}
      .top{display:flex;justify-content:space-between;gap:10px;align-items:center;flex-wrap:wrap;margin-bottom:18px}
      .title{font:700 1.55rem 'Outfit',sans-serif;letter-spacing:-.02em}
      .sub{color:var(--dim);font-size:.9rem;margin-top:4px}
      .actions{display:flex;gap:8px;flex-wrap:wrap}
      .btn,.token-input{
        height:38px;border:1px solid var(--border);border-radius:8px;background:var(--surface);color:var(--text);
      }
      .btn{padding:0 14px;font-weight:600;cursor:pointer;text-decoration:none;display:inline-flex;align-items:center}
      .btn:hover{background:var(--surface-hi)}
      .token-input{padding:0 12px;width:300px}
      .grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px;margin-bottom:12px}
      .card{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:14px}
      .k{font-size:.73rem;text-transform:uppercase;letter-spacing:.06em;color:var(--dim);margin-bottom:6px}
      .v{font:700 1.1rem 'Outfit',sans-serif}
      .panel{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:14px;margin-top:12px}
      .panel h2{font:600 1rem 'Outfit',sans-serif;margin-bottom:10px}
      #status{margin:10px 0;color:var(--amber);font-size:.88rem}
      #utilChart,#wasteChart{width:100%;height:210px;background:#0f172a;border:1px solid #1e293b;border-radius:10px;padding:6px}
      table{width:100%;border-collapse:collapse}
      th,td{padding:10px 8px;border-bottom:1px solid var(--border);text-align:left;font-size:.86rem}
      th{color:var(--dim);text-transform:uppercase;font-size:.7rem;letter-spacing:.06em}
      @media(max-width:940px){.grid{grid-template-columns:repeat(2,minmax(0,1fr))}}
      @media(max-width:640px){.grid{grid-template-columns:1fr}.token-input{width:100%}}
    </style>
  </head>
  <body>
    <div class="wrap">
      <div class="top">
        <div>
          <div class="title" id="pageTitle">Account Usage History</div>
          <div class="sub" id="pageSub">Usage windows, rollover wastage, and sync history.</div>
        </div>
        <div class="actions">
          <input id="tokenInput" class="token-input" type="password" placeholder="Bearer token" />
          <button id="applyToken" class="btn" type="button">Apply Token</button>
          <a class="btn" href="/ui/stats">Overall Stats</a>
          <a class="btn" href="/ui">Back</a>
        </div>
      </div>
      <div id="status"></div>

      <div class="grid">
        <div class="card"><div class="k">Current Window</div><div class="v" id="currentWindow">--</div></div>
        <div class="card"><div class="k">Lifetime Used</div><div class="v" id="lifetimeUsed">--</div></div>
        <div class="card"><div class="k">Total Wasted</div><div class="v" id="totalWasted">--</div></div>
        <div class="card"><div class="k">Completed Windows</div><div class="v" id="windowCount">--</div></div>
      </div>

      <div class="panel">
        <h2>Window Utilization Trend</h2>
        <svg id="utilChart" viewBox="0 0 1000 220" preserveAspectRatio="none"></svg>
      </div>
      <div class="panel">
        <h2>Rollover Wastage Trend</h2>
        <svg id="wasteChart" viewBox="0 0 1000 220" preserveAspectRatio="none"></svg>
      </div>
      <div class="panel">
        <h2>Rollover History</h2>
        <div style="overflow:auto">
          <table>
            <thead><tr><th>Window Ended</th><th>Used</th><th>Limit</th><th>Wasted</th><th>Utilization</th><th>Rolled Over</th></tr></thead>
            <tbody id="historyBody"></tbody>
          </table>
        </div>
      </div>
    </div>

    <script>
      const ACCOUNT_LABEL = __ACCOUNT_LABEL__;
      const $ = id => document.getElementById(id);
      const statusEl = $("status");
      const tokenInput = $("tokenInput");
      const getToken = () => (localStorage.getItem("internalToken") || "").trim();
      tokenInput.value = getToken();

      function setStatus(msg) { statusEl.textContent = msg || ""; }
      function authHeaders() {
        const token = getToken();
        return token ? { Authorization: "Bearer " + token } : {};
      }

      function pct(used, limit) {
        if (!limit) return null;
        return Math.max(0, Math.min(100, (used / limit) * 100));
      }

      function drawBars(svg, points, valueKey, color) {
        const w = 1000, h = 220, pad = 26;
        if (!points.length) { svg.innerHTML = ""; return; }
        const maxVal = Math.max(...points.map(p => Number(p[valueKey] || 0)), 1);
        const bw = (w - (pad * 2)) / points.length;
        const bars = points.map((p, i) => {
          const v = Number(p[valueKey] || 0);
          const bh = ((h - (pad * 2)) * v) / maxVal;
          const x = pad + (i * bw) + 2;
          const y = h - pad - bh;
          return `<rect x="${x.toFixed(2)}" y="${y.toFixed(2)}" width="${Math.max(2, bw - 4).toFixed(2)}" height="${bh.toFixed(2)}" fill="${color}" opacity="0.86"></rect>`;
        }).join("");
        svg.innerHTML = `<line x1="${pad}" y1="${h-pad}" x2="${w-pad}" y2="${h-pad}" stroke="#334155" stroke-width="1"></line>${bars}`;
      }

      function renderHistoryRows(rollovers, usage) {
        if (!rollovers.length) {
          const used = Number((usage || {}).usage_in_window || 0);
          const limit = Number((usage || {}).usage_limit || 0);
          const utilization = limit > 0 ? ((used / limit) * 100) : null;
          const nextReset = (usage || {}).rate_limit_refresh_at || "--";
          const currentWaste = Math.max(limit - used, 0);
          return `<tr>
            <td>Active window (current)</td>
            <td>${used.toLocaleString()}</td>
            <td>${limit.toLocaleString()}</td>
            <td>${currentWaste.toLocaleString()}</td>
            <td>${utilization === null ? "--" : utilization.toFixed(1) + "%"}</td>
            <td>${nextReset}</td>
          </tr>`;
        }
        return rollovers.slice().reverse().map(r => {
          const used = Number(r.usage_used || 0);
          const limit = Number(r.usage_limit || 0);
          const utilization = pct(used, limit);
          return `<tr>
            <td>${r.window_ended_at || "--"}</td>
            <td>${used.toLocaleString()}</td>
            <td>${limit.toLocaleString()}</td>
            <td>${Number(r.usage_wasted || 0).toLocaleString()}</td>
            <td>${utilization === null ? "--" : utilization.toFixed(1) + "%"}</td>
            <td>${r.rolled_over_at || "--"}</td>
          </tr>`;
        }).join("");
      }

      function drawHistoryCharts(rollovers, usage) {
        if (rollovers.length) {
          drawBars($("utilChart"), rollovers, "usage_used", "#10B981");
          drawBars($("wasteChart"), rollovers, "usage_wasted", "#EF4444");
          return;
        }
        const used = Number((usage || {}).usage_in_window || 0);
        const limit = Number((usage || {}).usage_limit || 0);
        const wasted = Math.max(limit - used, 0);
        const synthetic = [{ usage_used: used, usage_wasted: wasted }];
        drawBars($("utilChart"), synthetic, "usage_used", "#10B981");
        drawBars($("wasteChart"), synthetic, "usage_wasted", "#EF4444");
      }

      async function loadUsageHistory() {
        setStatus("");
        try {
          const res = await fetch(`/api/accounts/${encodeURIComponent(ACCOUNT_LABEL)}/usage-history`, {
            headers: { "Content-Type": "application/json", ...authHeaders() },
          });
          if (!res.ok) {
            const text = await res.text();
            throw new Error(text || "Failed to load history");
          }
          const data = await res.json();
          $("pageTitle").textContent = (data.display_label || data.label || ACCOUNT_LABEL) + " usage history";
          $("pageSub").textContent = data.email ? `Account: ${data.email}` : `Label: ${data.label}`;
          const usage = data.usage_tracking || {};
          const summary = data.summary || {};
          const rollovers = data.rollovers || [];

          const currentUsed = Number(usage.usage_in_window || 0);
          const currentLimit = Number(usage.usage_limit || 0);
          const currentPct = pct(currentUsed, currentLimit);
          $("currentWindow").textContent = `${currentUsed.toLocaleString()} / ${currentLimit.toLocaleString()} (${currentPct === null ? "--" : currentPct.toFixed(1) + "%"})`;
          $("lifetimeUsed").textContent = Number(usage.lifetime_used || 0).toLocaleString();
          $("totalWasted").textContent = Number(summary.total_wasted || 0).toLocaleString();
          $("windowCount").textContent = Number(summary.window_count || 0).toLocaleString();

          drawHistoryCharts(rollovers, usage);
          $("historyBody").innerHTML = renderHistoryRows(rollovers, usage);
        } catch (err) {
          setStatus(err.message || "Failed to load usage history.");
        }
      }

      $("applyToken").addEventListener("click", () => {
        localStorage.setItem("internalToken", tokenInput.value.trim());
        loadUsageHistory();
      });

      loadUsageHistory();
    </script>
  </body>
</html>"""
    return template.replace("__ACCOUNT_LABEL__", json.dumps(label))


def _render_usage_stats_page() -> str:
    return """<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Usage Analytics</title>
    <link rel="preconnect" href="https://fonts.googleapis.com" />
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
    <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@500;600;700&family=Inter:wght@400;500;600&display=swap" rel="stylesheet" />
    <style>
      *,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
      :root{
        --bg:#0A0A0A;--surface:#171717;--surface-hi:#202020;--border:#2B2B2B;
        --text:#E5E7EB;--dim:#94A3B8;--green:#10B981;--red:#EF4444;--amber:#F59E0B;--blue:#38BDF8;
      }
      body{font-family:'Inter',system-ui,sans-serif;color:var(--text);background:var(--bg);min-height:100vh}
      .wrap{max-width:1200px;margin:0 auto;padding:24px 18px 56px}
      .top{display:flex;justify-content:space-between;gap:10px;align-items:center;flex-wrap:wrap;margin-bottom:16px}
      .title{font:700 1.6rem 'Outfit',sans-serif}
      .sub{color:var(--dim);font-size:.9rem;margin-top:4px}
      .actions{display:flex;gap:8px;flex-wrap:wrap}
      .btn,.token-input{
        height:38px;border:1px solid var(--border);border-radius:8px;background:var(--surface);color:var(--text);
      }
      .btn{padding:0 14px;font-weight:600;text-decoration:none;display:inline-flex;align-items:center;cursor:pointer}
      .btn:hover{background:var(--surface-hi)}
      .token-input{padding:0 12px;width:280px}
      .grid{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:10px;margin-bottom:12px}
      .card,.panel{background:var(--surface);border:1px solid var(--border);border-radius:12px}
      .card{padding:14px}
      .k{font-size:.72rem;text-transform:uppercase;letter-spacing:.06em;color:var(--dim);margin-bottom:6px}
      .v{font:700 1.08rem 'Outfit',sans-serif}
      .panel{padding:14px;margin-top:12px}
      .panel h2{font:600 1rem 'Outfit',sans-serif;margin-bottom:10px}
      #chartTitle{margin-bottom:8px}
      #chartArea{width:100%;height:260px;background:#0f172a;border:1px solid #1e293b;border-radius:10px;padding:6px}
      #status{margin:8px 0;color:var(--amber);font-size:.88rem}
      .graph-pills{display:flex;flex-wrap:wrap;gap:8px;margin-bottom:10px}
      .graph-pill{
        padding:6px 10px;border:1px solid var(--border);border-radius:999px;background:var(--surface-hi);
        color:var(--dim);font-size:.78rem;cursor:pointer;
      }
      .graph-pill.active{border-color:var(--blue);color:#e2e8f0;background:#0f172a}
      .account-list{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}
      .acct{padding:10px;border:1px solid var(--border);border-radius:10px;background:#111827}
      .acct a{color:#93C5FD;text-decoration:none}
      .meter{margin-top:8px;height:8px;background:#0b1220;border-radius:99px;overflow:hidden;border:1px solid #1f2937}
      .meter span{display:block;height:100%;background:var(--green)}
      @media(max-width:1024px){.grid{grid-template-columns:repeat(2,minmax(0,1fr))}.account-list{grid-template-columns:1fr}}
      @media(max-width:640px){.grid{grid-template-columns:1fr}.token-input{width:100%}}
    </style>
  </head>
  <body>
    <div class="wrap">
      <div class="top">
        <div>
          <div class="title">Overall Usage Analytics</div>
          <div class="sub">Cross-account totals, live usage percentages, rollover wastage, and trends.</div>
        </div>
        <div class="actions">
          <input id="tokenInput" class="token-input" type="password" placeholder="Bearer token" />
          <button id="applyToken" class="btn" type="button">Apply Token</button>
          <a class="btn" href="/ui">Back</a>
        </div>
      </div>
      <div id="status"></div>
      <div class="grid">
        <div class="card"><div class="k">Accounts</div><div class="v" id="totalAccounts">--</div></div>
        <div class="card"><div class="k">Lifetime Used</div><div class="v" id="lifetimeUsed">--</div></div>
        <div class="card"><div class="k">Current Window Used</div><div class="v" id="windowUsed">--</div></div>
        <div class="card"><div class="k">Current Window Limit</div><div class="v" id="windowLimit">--</div></div>
        <div class="card"><div class="k">Total Wasted</div><div class="v" id="totalWasted">--</div></div>
      </div>

      <div class="panel">
        <h2 id="chartTitle">Graph</h2>
        <div class="graph-pills" id="graphPills"></div>
        <svg id="chartArea" viewBox="0 0 1000 260" preserveAspectRatio="none"></svg>
      </div>
      <div class="panel">
        <h2>Per-Account Usage Overview</h2>
        <div class="account-list" id="accountList"></div>
      </div>
    </div>

    <script>
      const $ = id => document.getElementById(id);
      const tokenInput = $("tokenInput");
      const statusEl = $("status");
      const chartArea = $("chartArea");
      const chartTitleEl = $("chartTitle");
      const graphPillsEl = $("graphPills");
      const getToken = () => (localStorage.getItem("internalToken") || "").trim();
      tokenInput.value = getToken();
      function setStatus(msg) { statusEl.textContent = msg || ""; }
      function authHeaders() {
        const token = getToken();
        return token ? { Authorization: "Bearer " + token } : {};
      }

      const GRAPH_OPTIONS = [
        { key: "rollover_used_wasted", label: "Rollover Used vs Wasted" },
        { key: "rollover_utilization", label: "Rollover Utilization %" },
        { key: "live_5hr", label: "Live 5hr %" },
        { key: "live_7d", label: "Live 7d %" },
        { key: "lifetime_by_account", label: "Lifetime Used by Account" },
        { key: "wasted_by_account", label: "Total Wasted by Account" },
      ];
      let selectedGraph = "rollover_used_wasted";
      let statsData = null;
      let accountsData = null;

      function drawNoData(svg, message) {
        svg.innerHTML = `<rect x="0" y="0" width="1000" height="260" fill="#0f172a"></rect>
          <text x="500" y="132" text-anchor="middle" fill="#94A3B8" font-size="18">${message}</text>`;
      }

      function drawBars(svg, points, series) {
        const w = 1000, h = 260, pad = 28;
        if (!points.length) {
          drawNoData(svg, "No data available for this graph yet");
          return;
        }
        const maxY = Math.max(
          1,
          ...points.map(p => Math.max(...series.map(s => Number(p[s.key] || 0))))
        );
        const fullWidth = (w - (pad * 2)) / points.length;
        const eachWidth = Math.max(2, (fullWidth / series.length) - 3);
        const baseLine = `<line x1="${pad}" y1="${h-pad}" x2="${w-pad}" y2="${h-pad}" stroke="#334155" stroke-width="1"></line>`;
        const bars = series.map((s, si) => points.map((p, i) => {
          const v = Number(p[s.key] || 0);
          const bh = ((h - (pad * 2)) * v) / maxY;
          const x = pad + (i * fullWidth) + (si * (eachWidth + 2)) + 2;
          const y = h - pad - bh;
          return `<rect x="${x.toFixed(2)}" y="${y.toFixed(2)}" width="${eachWidth.toFixed(2)}" height="${bh.toFixed(2)}" fill="${s.color}" opacity="0.88"></rect>`;
        }).join("")).join("");
        svg.innerHTML = baseLine + bars;
      }

      function buildGraphPoints() {
        const trend = (statsData && statsData.daily_rollover_trend) || [];
        const perAccount = (statsData && statsData.per_account) || [];
        const liveAccounts = (accountsData && accountsData.accounts) || [];

        if (selectedGraph === "rollover_used_wasted") {
          chartTitleEl.textContent = "Daily Rollover Trend (Used vs Wasted)";
          return {
            points: trend,
            series: [
              { key: "usage_used", color: "#10B981" },
              { key: "usage_wasted", color: "#EF4444" },
            ],
          };
        }
        if (selectedGraph === "rollover_utilization") {
          chartTitleEl.textContent = "Daily Rollover Utilization %";
          return {
            points: trend.map(p => ({ value: Number(p.utilization_percent || 0) })),
            series: [{ key: "value", color: "#38BDF8" }],
          };
        }
        if (selectedGraph === "live_5hr") {
          chartTitleEl.textContent = "Live 5hr Usage % by Account";
          return {
            points: liveAccounts.map(a => ({
              value: Number(a.rate_limits?.primary?.usedPercent ?? a.rate_limits?.requests?.usedPercent ?? 0),
            })),
            series: [{ key: "value", color: "#10B981" }],
          };
        }
        if (selectedGraph === "live_7d") {
          chartTitleEl.textContent = "Live 7d Usage % by Account";
          return {
            points: liveAccounts.map(a => ({
              value: Number(a.rate_limits?.secondary?.usedPercent ?? a.rate_limits?.tokens?.usedPercent ?? 0),
            })),
            series: [{ key: "value", color: "#F59E0B" }],
          };
        }
        if (selectedGraph === "lifetime_by_account") {
          chartTitleEl.textContent = "Lifetime Used by Account";
          return {
            points: perAccount.map(a => ({ value: Number(a.usage_tracking?.lifetime_used || 0) })),
            series: [{ key: "value", color: "#38BDF8" }],
          };
        }
        chartTitleEl.textContent = "Total Wasted by Account";
        return {
          points: perAccount.map(a => ({ value: Number(a.summary?.total_wasted || 0) })),
          series: [{ key: "value", color: "#EF4444" }],
        };
      }

      function renderGraphPills() {
        graphPillsEl.innerHTML = GRAPH_OPTIONS.map(opt =>
          `<button type="button" class="graph-pill ${opt.key === selectedGraph ? "active" : ""}" data-graph="${opt.key}">${opt.label}</button>`
        ).join("");
      }

      function renderSelectedGraph() {
        renderGraphPills();
        if (!statsData) {
          drawNoData(chartArea, "Load stats to view graphs");
          return;
        }
        const built = buildGraphPoints();
        drawBars(chartArea, built.points, built.series);
      }

      function accountTile(item) {
        const usage = item.usage_tracking || {};
        const summary = item.summary || {};
        const used = Number(usage.usage_in_window || 0);
        const limit = Number(usage.usage_limit || 0);
        const pct = limit > 0 ? Math.max(0, Math.min(100, (used / limit) * 100)) : 0;
        return `<div class="acct">
          <div style="display:flex;justify-content:space-between;gap:10px;align-items:flex-start">
            <div>
              <div style="font:600 .98rem 'Outfit',sans-serif">${item.display_label || item.label}</div>
              <div style="color:#94A3B8;font-size:.82rem">${item.email || "—"}</div>
            </div>
            <a href="/ui/accounts/${encodeURIComponent(item.label)}">View</a>
          </div>
          <div style="margin-top:8px;font-size:.84rem">Window: ${used.toLocaleString()} / ${limit.toLocaleString()}</div>
          <div class="meter"><span style="width:${pct.toFixed(2)}%"></span></div>
          <div style="margin-top:8px;font-size:.82rem;color:#94A3B8">Lifetime used: ${Number(usage.lifetime_used || 0).toLocaleString()} | Total wasted: ${Number(summary.total_wasted || 0).toLocaleString()}</div>
        </div>`;
      }

      async function loadStats() {
        setStatus("");
        try {
          const [statsRes, accountsRes] = await Promise.all([
            fetch("/api/usage/stats", { headers: { "Content-Type": "application/json", ...authHeaders() } }),
            fetch("/api/accounts", { headers: { "Content-Type": "application/json", ...authHeaders() } }),
          ]);
          if (!statsRes.ok) {
            const text = await statsRes.text();
            throw new Error(text || "Failed to load usage stats");
          }
          statsData = await statsRes.json();
          accountsData = accountsRes.ok ? await accountsRes.json() : { accounts: [] };

          const totals = statsData.totals || {};
          $("totalAccounts").textContent = Number(totals.accounts || 0).toLocaleString();
          $("lifetimeUsed").textContent = Number(totals.lifetime_used || 0).toLocaleString();
          $("windowUsed").textContent = Number(totals.active_window_used || 0).toLocaleString();
          $("windowLimit").textContent = Number(totals.active_window_limit || 0).toLocaleString();
          $("totalWasted").textContent = Number(totals.total_wasted || 0).toLocaleString();

          const accounts = statsData.per_account || [];
          $("accountList").innerHTML = accounts.length
            ? accounts.map(accountTile).join("")
            : '<div style="color:#94A3B8">No accounts found.</div>';

          renderSelectedGraph();
        } catch (err) {
          setStatus(err.message || "Failed to load usage stats.");
          statsData = null;
          accountsData = null;
          renderSelectedGraph();
        }
      }

      graphPillsEl.addEventListener("click", (e) => {
        const btn = e.target.closest("[data-graph]");
        if (!btn) return;
        selectedGraph = btn.getAttribute("data-graph") || selectedGraph;
        renderSelectedGraph();
      });

      $("applyToken").addEventListener("click", () => {
        localStorage.setItem("internalToken", tokenInput.value.trim());
        loadStats();
      });

      renderSelectedGraph();
      loadStats();
    </script>
  </body>
</html>"""


def _render_index() -> str:
    return """<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Codex Auth Manager</title>
    <link rel="preconnect" href="https://fonts.googleapis.com" />
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
    <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@400;500;600;700&family=Inter:wght@400;500;600&display=swap" rel="stylesheet" />
    <style>
      *,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
      :root{
        --bg:#0A0A0A;--surface:#171717;--surface-hi:#262626;
        --border:#262626;--border-hi:#404040;
        --text:#EDEDED;--dim:#A3A3A3;--green:#10B981;--blue:#38BDF8;
        --amber:#F59E0B;--red:#EF4444;--radius:10px;
      }
      html{font-size:15px}
      body{
        font-family:'Inter',system-ui,sans-serif;color:var(--text);min-height:100vh;
        background:var(--bg);
      }

      /* ── NAV BAR ─────────────────────────── */
      .navbar{
        position:sticky;top:0;z-index:50;
        display:flex;align-items:center;justify-content:space-between;
        padding:16px 32px;gap:16px;flex-wrap:wrap;
        background:rgba(10,10,10,.85);backdrop-filter:blur(14px);
        border-bottom:1px solid var(--border);
      }
      .nav-brand{
        font-family:'Outfit',sans-serif;font-weight:700;font-size:1.35rem;
        letter-spacing:-.02em;color:var(--text);display:flex;align-items:center;gap:10px;
      }
      .nav-brand .dot{width:8px;height:8px;border-radius:50%;background:var(--green);
        box-shadow:0 0 6px rgba(16,185,129,.4);}
      .nav-actions{display:flex;gap:10px;flex-wrap:wrap}

      /* ── BUTTONS ──────────────────────────── */
      .btn{
        height:38px;padding:0 18px;border-radius:6px;border:1px solid var(--border);
        background:var(--surface);color:var(--text);font-family:'Inter',sans-serif;
        font-weight:500;font-size:.85rem;cursor:pointer;
        display:inline-flex;align-items:center;gap:7px;
        transition:background .2s,border-color .2s,transform .15s;
      }
      .btn:hover{background:var(--surface-hi);border-color:var(--border-hi);}
      .btn-primary{background:var(--text);color:var(--bg);border:none;font-weight:600;}
      .btn-primary:hover{background:#FFFFFF;}
      .btn-icon{width:38px;padding:0;justify-content:center}
      .btn-sm{height:34px;padding:0 14px;font-size:.8rem;border-radius:6px}

      /* ── LAYOUT ───────────────────────────── */
      .page{max-width:1140px;margin:0 auto;padding:32px 24px 60px}
      .two-col{display:grid;grid-template-columns:320px 1fr;gap:24px;align-items:start}

      /* ── SIDEBAR ──────────────────────────── */
      .sidebar{display:flex;flex-direction:column;gap:16px;position:sticky;top:88px}
      .panel{
        background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);
        padding:20px;
      }
      .panel-title{
        font-family:'Outfit',sans-serif;font-size:.75rem;font-weight:600;
        text-transform:uppercase;letter-spacing:.08em;color:var(--dim);margin-bottom:14px;
      }
      .kv-list{display:flex;flex-direction:column;gap:14px}
      .kv-item .kv-label{font-size:.7rem;font-weight:600;text-transform:uppercase;
        letter-spacing:.06em;color:var(--dim);margin-bottom:3px}
      .kv-item .kv-value{font-family:'Outfit',sans-serif;font-size:1.15rem;font-weight:700;
        color:var(--text);word-break:break-word}
      .kv-item .kv-value.small{font-size:.85rem;font-weight:500}

      /* status dot */
      .status-dot{display:inline-block;width:7px;height:7px;border-radius:50%;margin-right:5px}
      .status-dot.green{background:var(--green);box-shadow:0 0 5px var(--green)}
      .status-dot.amber{background:var(--amber);box-shadow:0 0 5px var(--amber)}
      .status-dot.red{background:var(--red);box-shadow:0 0 5px var(--red)}

      .token-bar{
        display:flex;align-items:center;gap:10px;padding:12px 16px;
        background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);
        margin-bottom:24px;
      }
      .token-bar input{
        flex:1;height:38px;background:var(--bg);border:1px solid var(--border);
        border-radius:8px;padding:0 14px;color:var(--text);font-size:.85rem;outline:none;
        font-family:'Inter',sans-serif;transition:border-color .2s;
      }
      .token-bar input:focus{border-color:var(--blue)}
      .token-bar input::placeholder{color:var(--dim)}

      /* ── TOAST / STATUS ───────────────────── */
      #statusNote{
        padding:12px 18px;border-radius:10px;font-size:.85rem;display:none;margin-bottom:16px;
      }
      #statusNote.show{display:flex;align-items:center;gap:8px}
      #statusNote.warn{background:rgba(251,191,36,.08);color:var(--amber);
        border:1px solid rgba(251,191,36,.18)}

      /* ── ACCOUNT TABLE ────────────────────── */
      .accounts-table{width:100%;border-collapse:separate;border-spacing:0}
      .accounts-table col:nth-child(1){width:28%}
      .accounts-table col:nth-child(2){width:34%}
      .accounts-table col:nth-child(3){width:22%}
      .accounts-table col:nth-child(4){width:16%}
      .accounts-table th{
        font-size:.7rem;font-weight:600;text-transform:uppercase;letter-spacing:.07em;
        color:var(--dim);text-align:left;padding:10px 14px;
        border-bottom:1px solid var(--border);
      }
      .accounts-table td{
        padding:14px;border-bottom:1px solid var(--border);vertical-align:middle;
        font-size:.88rem;
      }
      .accounts-table tr:last-child td{border-bottom:none}
      .accounts-table tr:hover td{background:rgba(148,163,184,.04)}

      .acct-name{font-family:'Outfit',sans-serif;font-weight:600;font-size:.95rem}
      .acct-email{color:var(--dim);font-size:.8rem;margin-top:2px}
      .acct-label{
        color:var(--dim);font-size:.74rem;margin-top:3px;
        font-family:ui-monospace,SFMono-Regular,Menlo,monospace;
        letter-spacing:.01em;
      }
      .profile-link{
        display:block;text-decoration:none;color:inherit;border-radius:8px;padding:2px 0;
      }
      .profile-link:hover .acct-name{text-decoration:underline}
      .pill{
        display:inline-flex;align-items:center;gap:5px;
        padding:4px 10px;border-radius:4px;font-size:.72rem;font-weight:600;
        border:1px solid var(--border);color:var(--dim);background:transparent;
      }
      .pill.active{color:var(--green);border-color:rgba(16,185,129,.35);
        background:rgba(16,185,129,.08)}

      /* progress bar for limits */
      .limit-bar-wrap{display:flex;flex-direction:column;gap:6px}
      .limit-bar-row{display:flex;align-items:center;gap:10px;min-height:24px}
      .limit-bar-label{font-size:.72rem;font-weight:600;text-transform:uppercase;
        letter-spacing:.05em;color:var(--dim);min-width:50px}
      .limit-bar-track{flex:0 0 140px;min-width:140px;width:140px;height:6px;border-radius:99px;background:var(--bg);
        overflow:hidden;border:1px solid var(--border)}
      .limit-bar-fill{height:100%;border-radius:99px;transition:width .6s ease}
      .limit-bar-fill.ok{background:var(--green)}
      .limit-bar-fill.warn{background:var(--amber)}
      .limit-bar-fill.danger{background:var(--red)}
      .limit-bar-text{font-size:.78rem;font-weight:600;min-width:52px;text-align:right;
        font-family:'Outfit',sans-serif}
      .reset-wrap{display:flex;flex-direction:column;gap:6px}
      .reset-row{display:flex;align-items:center;min-height:24px}
      .reset-value{
        font-size:.82rem;color:var(--text);font-weight:500;white-space:nowrap
      }

      .acct-actions{display:flex;gap:6px}
      .acct-actions a{text-decoration:none}
      .actions-menu{position:relative;display:inline-block}
      .actions-menu .btn{min-width:96px;justify-content:space-between}
      .actions-panel{
        position:absolute;right:0;top:calc(100% + 6px);z-index:25;display:none;min-width:170px;
        background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:6px;
        box-shadow:0 14px 28px rgba(0,0,0,.35);
      }
      .actions-menu.open .actions-panel{display:block}
      .menu-item{
        width:100%;height:34px;display:flex;align-items:center;padding:0 10px;border:0;background:transparent;
        color:var(--text);font-size:.82rem;border-radius:8px;cursor:pointer;text-align:left;
      }
      .menu-item:hover{background:var(--surface-hi)}
      .menu-item.danger{color:#fca5a5}

      /* empty state */
      .empty-state{
        text-align:center;padding:48px 20px;color:var(--dim);
      }
      .empty-state .empty-icon{font-size:2.5rem;margin-bottom:12px;opacity:.4}
      .empty-state p{font-size:.9rem;line-height:1.5}

      .callback-modal{
        position:fixed;inset:0;background:rgba(2,6,23,.78);display:none;align-items:center;
        justify-content:center;padding:20px;z-index:90;
      }
      .callback-modal.open{display:flex}
      .callback-card{
        width:min(760px,95vw);background:var(--surface);border:1px solid var(--border);
        border-radius:14px;padding:18px;
      }
      .callback-title{font-family:'Outfit',sans-serif;font-size:1rem;font-weight:600;margin-bottom:8px}
      .callback-sub{color:var(--dim);font-size:.84rem;line-height:1.4;margin-bottom:10px}
      .callback-row{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-bottom:10px}
      .callback-row .btn{height:34px}
      #callbackUrlInput{
        width:100%;height:88px;border:1px solid var(--border);background:var(--bg);color:var(--text);
        border-radius:10px;padding:10px 12px;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;
        font-size:.82rem;resize:vertical;
      }
      #callbackHint{font-size:.8rem;color:var(--dim);min-height:20px}
      #importLabelInput,#importJsonInput{
        width:100%;border:1px solid var(--border);background:var(--bg);color:var(--text);
        border-radius:10px;padding:10px 12px;font-family:'Inter',sans-serif;font-size:.84rem;
      }
      #importLabelInput{height:38px;padding:0 12px}
      #importJsonInput{
        height:150px;resize:vertical;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;
      }
      #importFileInput{
        width:100%;padding:8px;border:1px dashed var(--border);border-radius:10px;background:var(--bg);
        color:var(--dim);font-size:.8rem;
      }
      #importHint{font-size:.8rem;color:var(--dim);min-height:20px}
      #editProfileInput{
        width:100%;height:38px;border:1px solid var(--border);background:var(--bg);color:var(--text);
        border-radius:10px;padding:0 12px;font-family:'Inter',sans-serif;font-size:.85rem;
      }
      #editProfileHint{font-size:.8rem;color:var(--dim);min-height:20px}

      /* ── RESPONSIVE ───────────────────────── */
      @media(max-width:860px){
        .two-col{grid-template-columns:1fr}
        .sidebar{position:static;flex-direction:row;flex-wrap:wrap}
        .sidebar .panel{flex:1;min-width:240px}
        .navbar{padding:14px 16px}
        .page{padding:20px 14px 48px}
      }
      @media(max-width:600px){
        .limit-bar-track{flex:0 0 110px;min-width:110px;width:110px}
        .accounts-table thead{display:none}
        .accounts-table,
        .accounts-table tbody,
        .accounts-table tr,
        .accounts-table td{display:block;width:100%}
        .accounts-table tr{
          background:var(--surface);border:1px solid var(--border);
          border-radius:var(--radius);padding:16px;margin-bottom:12px;
        }
        .accounts-table td{border:none;padding:4px 0}
        .accounts-table td:before{
          content:attr(data-label);display:block;
          font-size:.7rem;font-weight:600;text-transform:uppercase;
          color:var(--dim);margin-bottom:2px;
        }
      }
    </style>
  </head>
  <body>

    <!-- NAV -->
    <nav class="navbar">
      <div class="nav-brand"><span class="dot"></span> Auth Manager</div>
      <div class="nav-actions">
        <button id="addAccount" class="btn btn-primary" type="button">
          <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>
          Add Account
        </button>
        <button id="importCurrent" class="btn" type="button">Import Current</button>
        <a href="/ui/stats" class="btn" style="text-decoration:none">Overall Stats</a>
        <button id="refreshAll" class="btn btn-icon" type="button" title="Refresh">
          <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round"><path d="M23 4v6h-6"/><path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10"/></svg>
        </button>
      </div>
    </nav>

    <div class="page">
      <!-- TOKEN BAR -->
      <div class="token-bar">
        <input id="tokenInput" type="password" placeholder="Enter your API bearer token to unlock account management..." />
        <button id="tokenSave" class="btn btn-primary btn-sm" type="button">Apply</button>
        <button id="tokenClear" class="btn btn-sm" type="button">Clear</button>
      </div>

      <div id="statusNote"></div>

      <div id="callbackModal" class="callback-modal" role="dialog" aria-modal="true" aria-labelledby="callbackModalTitle">
        <div class="callback-card">
          <div id="callbackModalTitle" class="callback-title">Complete Add Account</div>
          <div class="callback-sub">Paste the full localhost callback URL from browser redirect to relay it into this login session.</div>
          <div class="callback-row">
            <button id="openAuthUrl" class="btn btn-sm" type="button">Open Auth URL</button>
            <span id="callbackSessionMeta" style="font-size:.78rem;color:var(--dim)"></span>
          </div>
          <textarea id="callbackUrlInput" placeholder="http://127.0.0.1:1455/auth/callback?code=...&state=..."></textarea>
          <div id="callbackHint"></div>
          <div class="callback-row" style="justify-content:flex-end;margin-top:8px">
            <button id="cancelCallback" class="btn btn-sm" type="button">Cancel</button>
            <button id="submitCallback" class="btn btn-primary btn-sm" type="button">Submit Callback Link</button>
          </div>
        </div>
      </div>
      <div id="editProfileModal" class="callback-modal" role="dialog" aria-modal="true" aria-labelledby="editProfileTitle">
        <div class="callback-card" style="width:min(520px,95vw)">
          <div id="editProfileTitle" class="callback-title">Edit Profile Name</div>
          <div class="callback-sub">Change the saved profile label used by switch/export operations.</div>
          <input id="editProfileInput" type="text" maxlength="64" autocomplete="off" />
          <div id="editProfileHint"></div>
          <div class="callback-row" style="justify-content:flex-end;margin-top:8px">
            <button id="cancelEditProfile" class="btn btn-sm" type="button">Cancel</button>
            <button id="saveEditProfile" class="btn btn-primary btn-sm" type="button">Save</button>
          </div>
        </div>
      </div>
      <div id="importModal" class="callback-modal" role="dialog" aria-modal="true" aria-labelledby="importModalTitle">
        <div class="callback-card" style="width:min(760px,95vw)">
          <div id="importModalTitle" class="callback-title">Import Auth Credentials</div>
          <div class="callback-sub">Import current local auth, or paste/upload auth JSON from another machine.</div>
          <div class="callback-row" style="align-items:flex-start">
            <div style="flex:1;min-width:220px">
              <div class="callback-sub" style="margin:0 0 6px 0">Optional profile label</div>
              <input id="importLabelInput" type="text" maxlength="64" placeholder="Leave blank for auto label" />
            </div>
          </div>
          <div class="callback-sub" style="margin:8px 0 6px 0">Paste auth JSON</div>
          <textarea id="importJsonInput" placeholder='{"auth_mode":"chatgpt","tokens":{"access_token":"..."}}'></textarea>
          <div class="callback-sub" style="margin:10px 0 6px 0">Or upload auth JSON file</div>
          <input id="importFileInput" type="file" accept="application/json,.json" />
          <div id="importHint"></div>
          <div class="callback-row" style="justify-content:flex-end;margin-top:8px">
            <button id="cancelImport" class="btn btn-sm" type="button">Cancel</button>
            <button id="importCurrentModalBtn" class="btn btn-sm" type="button">Import Current</button>
            <button id="importJsonModalBtn" class="btn btn-primary btn-sm" type="button">Import JSON</button>
          </div>
        </div>
      </div>

      <!-- TWO COLUMN LAYOUT -->
      <div class="two-col">

        <!-- LEFT: SIDEBAR -->
        <aside class="sidebar">
          <div class="panel">
            <div class="panel-title">System Overview</div>
            <div class="kv-list">
              <div class="kv-item">
                <div class="kv-label">Accounts Managed</div>
                <div class="kv-value" id="accountsManaged">--</div>
              </div>
              <div class="kv-item">
                <div class="kv-label">Profiles with Token</div>
                <div class="kv-value" id="profilesWithTokens">--</div>
              </div>
              <div class="kv-item">
                <div class="kv-label">Auth File Updated</div>
                <div class="kv-value small" id="authUpdatedAt">--</div>
              </div>
            </div>
          </div>

          <div class="panel">
            <div class="panel-title">Aggregated Usage</div>
            <div class="kv-list">
              <div class="kv-item">
                <div class="kv-label">Cluster 5hr Usage</div>
                <div class="kv-value small" id="agg5Hr">
                  <div class="limit-bar-track"><div class="limit-bar-fill" style="width:0%"></div></div>
                </div>
              </div>
              <div class="kv-item">
                <div class="kv-label">Cluster 7d Usage</div>
                <div class="kv-value small" id="aggWeekly">
                  <div class="limit-bar-track"><div class="limit-bar-fill" style="width:0%"></div></div>
                </div>
              </div>
              <div class="kv-item">
                <div class="kv-label">Recommended Profile</div>
                <div class="kv-value small" id="recProfile">--</div>
              </div>
            </div>
          </div>
        </aside>

        <!-- RIGHT: MAIN -->
        <main>
          <div class="panel" style="padding:0;overflow:visible">
            <div style="padding:16px 20px;border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between">
              <div class="panel-title" style="margin:0">Saved Profiles</div>
              <span class="pill" id="accountCount">0 accounts</span>
            </div>
            <div id="accounts"></div>
          </div>
        </main>
      </div>
    </div>

    <script>
      const $ = id => document.getElementById(id);
      const accountsEl = $("accounts");
      const statusNoteEl = $("statusNote");
      const tokenInput = $("tokenInput");
      const accountsManagedEl = $("accountsManaged");
      const profilesWithTokensEl = $("profilesWithTokens");
      const authUpdatedAtEl = $("authUpdatedAt");
      const accountCountEl = $("accountCount");
      const callbackModalEl = $("callbackModal");
      const callbackUrlInputEl = $("callbackUrlInput");
      const callbackHintEl = $("callbackHint");
      const callbackSessionMetaEl = $("callbackSessionMeta");
      const editProfileModalEl = $("editProfileModal");
      const editProfileInputEl = $("editProfileInput");
      const editProfileHintEl = $("editProfileHint");
      const importModalEl = $("importModal");
      const importLabelInputEl = $("importLabelInput");
      const importJsonInputEl = $("importJsonInput");
      const importFileInputEl = $("importFileInput");
      const importHintEl = $("importHint");
      let pendingRelay = null;
      let pendingRenameLabel = null;

      const getToken = () => (localStorage.getItem("internalToken") || "").trim();

      function setStatus(text, warn = false) {
        if (!warn || !text) {
          statusNoteEl.textContent = "";
          statusNoteEl.className = "";
          return;
        }
        statusNoteEl.textContent = text;
        statusNoteEl.className = "show warn";
      }

      function authHeaders() {
        const t = getToken();
        return t ? { Authorization: "Bearer " + t } : {};
      }

      async function apiFetch(url, opts = {}) {
        return fetch(url, {
          ...opts,
          headers: { "Content-Type": "application/json", ...authHeaders(), ...(opts.headers || {}) },
        });
      }

      async function readError(res, fallback) {
        const raw = await res.text();
        try {
          const d = JSON.parse(raw);
          if (typeof d.detail === "string") return d.detail;
          if (d.detail?.message) return d.detail.message;
          if (typeof d.message === "string") return d.message;
        } catch (_) {}
        return raw || fallback;
      }

      /* ── human-readable helpers ─────────── */
      function humanDate(isoStr) {
        if (!isoStr || isoStr === "--") return "--";
        try {
          const d = new Date(isoStr);
          if (isNaN(d)) return isoStr;
          const now = new Date();
          const diffMs = now - d;
          const diffMin = Math.floor(diffMs / 60000);
          if (diffMin < 1) return "Just now";
          if (diffMin < 60) return diffMin + " min ago";
          const diffHr = Math.floor(diffMin / 60);
          if (diffHr < 24) return diffHr + "h ago";
          return d.toLocaleDateString("en-US", { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" });
        } catch (_) { return isoStr; }
      }

      function formatResetValue(data) {
        if (!data || typeof data !== "object") return null;
        const raw = data.resetsAt ?? data.resetAt ?? data.nextResetAt ?? data.reset;
        if (raw === undefined || raw === null || raw === "") return null;

        let d = null;
        if (typeof raw === "number" && Number.isFinite(raw)) {
          d = new Date(raw * 1000);
        } else if (typeof raw === "string") {
          const trimmed = raw.trim();
          if (!trimmed) return null;
          if (/^\\d+$/.test(trimmed)) d = new Date(Number(trimmed) * 1000);
          else d = new Date(trimmed);
        }

        if (!d || Number.isNaN(d.getTime())) return String(raw);
        return d.toLocaleString("en-US", {
          month: "short",
          day: "numeric",
          hour: "2-digit",
          minute: "2-digit",
        });
      }

      function parseResetDate(data) {
        if (!data || typeof data !== "object") return null;
        const raw = data.resetsAt ?? data.resetAt ?? data.nextResetAt ?? data.reset;
        if (raw === undefined || raw === null || raw === "") return null;
        let d = null;
        if (typeof raw === "number" && Number.isFinite(raw)) {
          d = new Date(raw * 1000);
        } else if (typeof raw === "string") {
          const trimmed = raw.trim();
          if (!trimmed) return null;
          if (/^\d+$/.test(trimmed)) d = new Date(Number(trimmed) * 1000);
          else d = new Date(trimmed);
        }
        if (!d || Number.isNaN(d.getTime())) return null;
        return d;
      }

      function formatRefreshCountdown(msRemaining) {
        if (!Number.isFinite(msRemaining)) return "refresh --";
        if (msRemaining <= 60000) return "refresh due";
        const totalMin = Math.floor(msRemaining / 60000);
        const days = Math.floor(totalMin / (60 * 24));
        const hours = Math.floor((totalMin % (60 * 24)) / 60);
        const mins = totalMin % 60;
        if (days > 0) return `refresh ${days}d ${hours}h`;
        if (hours > 0) return `refresh ${hours}h ${mins}m`;
        return `refresh ${mins}m`;
      }

      function refreshBadge(primary, secondary) {
        const now = Date.now();
        const resets = [parseResetDate(primary), parseResetDate(secondary)]
          .filter((d) => d instanceof Date)
          .map((d) => d.getTime());
        if (!resets.length) return "";

        const soonest = Math.min(...resets);
        const msRemaining = soonest - now;
        const minMs = 60 * 1000;
        const maxMs = 7 * 24 * 60 * 60 * 1000;
        const clamped = Math.max(minMs, Math.min(maxMs, msRemaining));
        const ratio = (clamped - minMs) / (maxMs - minMs);
        const hue = Math.round(ratio * 120);
        const color = `hsl(${hue}, 82%, 55%)`;
        const bg = `hsla(${hue}, 82%, 55%, 0.14)`;
        const label = formatRefreshCountdown(msRemaining);
        return `<span class="pill" style="margin-left:8px;color:${color};border-color:${bg};background:${bg}">${label}</span>`;
      }

      /* ── render helpers ─────────────────── */
      function limitBar(label, data) {
        if (!data) return "";
        const pct = data.percent ?? data.usedPercent ?? null;
        const fillPct = pct !== null ? Math.min(100, Math.max(0, pct)) : 0;
        const cls = fillPct > 85 ? "danger" : fillPct > 60 ? "warn" : "ok";
        const pctText = pct !== null ? fillPct + "%" : "--";
        return `<div class="limit-bar-row">
          <span class="limit-bar-label">${label}</span>
          <div class="limit-bar-track"><div class="limit-bar-fill ${cls}" style="width:${fillPct}%"></div></div>
          <span class="limit-bar-text">${pctText}</span>
        </div>`;
      }

      function resetRows(primary, secondary) {
        const p = formatResetValue(primary) || "--";
        const s = formatResetValue(secondary) || "--";
        return `<div class="reset-wrap">
          <div class="reset-row"><span class="reset-value">${p}</span></div>
          <div class="reset-row"><span class="reset-value">${s}</span></div>
        </div>`;
      }

      function renderCard(account) {
        const isActive = account.is_current;
        const displayLabel = account.display_label || account.label;
        const email = account.email || "—";
        const rate = account.rate_limits || {};
        const prim = rate.requests || rate.primary || null;
        const sec = rate.tokens || rate.secondary || null;
        const rateError = typeof rate.error === "string" ? rate.error : "";
        const limitsHtml = (prim || sec)
          ? `<div class="limit-bar-wrap">${limitBar("5hr", prim)}${limitBar("7d", sec)}</div>`
          : (
            rateError
              ? `<span style="color:var(--amber);font-size:.8rem">${rateError.includes("token_expired") ? "Auth token expired. Re-login/import this account." : "Rate limit read failed."}</span>`
              : '<span style="color:var(--dim);font-size:.8rem">No limit data</span>'
          );
        const resetHtml = resetRows(prim, sec);
        const activeBadge = isActive ? refreshBadge(prim, sec) : "";

        return `<tr>
          <td data-label="Profile"><a class="profile-link" href="/ui/accounts/${encodeURIComponent(account.label)}"><div class="acct-name">${displayLabel} ${activeBadge}</div><div class="acct-email">${email}</div><div class="acct-label">Profile label: ${account.label}</div></a></td>
          <td data-label="Rate Limits">${limitsHtml}</td>
          <td data-label="Rate Limit Reset">${resetHtml}</td>
          <td data-label="Actions"><div class="acct-actions">
            <div class="actions-menu" data-menu-root>
              <button class="btn btn-sm" type="button" data-action="menu" data-label="${account.label}">Actions
                <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="6 9 12 15 18 9"/></svg>
              </button>
              <div class="actions-panel">
                <button class="menu-item" type="button" data-action="switch" data-label="${account.label}">${isActive ? "Switch (Current)" : "Switch"}</button>
                <button class="menu-item" type="button" data-action="edit" data-label="${account.label}" data-display="${displayLabel}">Change profile label</button>
                <button class="menu-item" type="button" data-action="export" data-label="${account.label}">Export</button>
                <button class="menu-item danger" type="button" data-action="delete" data-label="${account.label}">Delete</button>
              </div>
            </div>
          </div></td>
        </tr>`;
      }

      function wrapTable(rows) {
        return `<table class="accounts-table">
          <colgroup><col><col><col><col></colgroup>
          <thead><tr><th>Profile</th><th>Rate Limits</th><th>Rate Limit Reset</th><th style="width:260px">Actions</th></tr></thead>
          <tbody>${rows}</tbody>
        </table>`;
      }

      function emptyState(msg) {
        return `<div class="empty-state"><div class="empty-icon">📂</div><p>${msg}</p></div>`;
      }

      /* ── data loaders ───────────────────── */
      async function loadPublicStats() {
        try {
          const res = await fetch("/api/public-stats");
          const data = await res.json();
          accountsManagedEl.textContent = (data.accounts_managed ?? "--").toString();
          profilesWithTokensEl.textContent = (data.profiles_with_tokens ?? "--").toString();
          authUpdatedAtEl.textContent = humanDate(data.auth_file?.modified_at);
        } catch (_) {
          accountsManagedEl.textContent = "--";
          profilesWithTokensEl.textContent = "--";
          authUpdatedAtEl.textContent = "--";
        }
      }

      async function loadAccounts() {
        tokenInput.value = getToken();
        try {
          const res = await apiFetch("/api/accounts", { method: "GET" });
          if (res.status === 401 || res.status === 403) {
            accountsEl.innerHTML = emptyState("Enter your bearer token above to manage accounts.");
            accountCountEl.textContent = "locked";
            $("agg5Hr").innerHTML = "--";
            $("aggWeekly").innerHTML = "--";
            $("recProfile").innerHTML = "--";
            return;
          }
          const data = await res.json();
          const accounts = data.accounts || [];
          accountCountEl.textContent = accounts.length + " account" + (accounts.length !== 1 ? "s" : "");
          if (!accounts.length) {
            accountsEl.innerHTML = emptyState("No saved profiles yet.<br>Click <strong>Add Account</strong> or <strong>Import Current</strong> to get started.");
            $("agg5Hr").innerHTML = "--";
            $("aggWeekly").innerHTML = "--";
            $("recProfile").innerHTML = "--";
            return;
          }

          let sum5Hr = 0, sumWeekly = 0;
          let rec = accounts[0];
          let minUsed = 999;
          
          for (const a of accounts) {
            let p = a.rate_limits?.primary?.usedPercent ?? a.rate_limits?.requests?.usedPercent ?? 0;
            let s = a.rate_limits?.secondary?.usedPercent ?? a.rate_limits?.tokens?.usedPercent ?? 0;
            sum5Hr += p;
            sumWeekly += s;
            if ((p + s) < minUsed) { minUsed = p + s; rec = a; }
          }
          
          let avg5Hr = Math.round(sum5Hr / accounts.length);
          let avgWeekly = Math.round(sumWeekly / accounts.length);
          
          $("agg5Hr").innerHTML = limitBar("5hr", {percent: avg5Hr}) + `<div style="font-size:0.75rem;margin-top:6px;color:var(--dim)">${100 - avg5Hr}% remaining across cluster</div>`;
          $("aggWeekly").innerHTML = limitBar("7d", {percent: avgWeekly}) + `<div style="font-size:0.75rem;margin-top:6px;color:var(--dim)">${100 - avgWeekly}% remaining across cluster</div>`;
          $("recProfile").innerHTML = `<span class="pill active" style="cursor:pointer" onclick="document.querySelector('[data-action=\\'switch\\'][data-label=\\'${rec.label}\\']').click()">Switch to <b>${rec.display_label || rec.label}</b></span>`;

          accountsEl.innerHTML = wrapTable(accounts.map(renderCard).join(""));
        } catch (_) {
          accountsEl.innerHTML = emptyState("Failed to load accounts.");
          $("agg5Hr").innerHTML = "--";
          $("aggWeekly").innerHTML = "--";
          $("recProfile").innerHTML = "--";
        }
      }

      async function startLogin() {
        const res = await apiFetch("/auth/login/start", { method: "POST", body: "{}" });
        if (!res.ok) throw new Error(await readError(res, "Unable to start login"));
        const data = await res.json();
        pendingRelay = {
          sessionId: data.session_id,
          relayToken: data.relay_token,
          authUrl: data.auth_url || data.browser_url || null,
        };
        openCallbackModal();
        setStatus(data.instructions || "Login started. Paste callback link to complete relay.");
        await loadPublicStats();
      }

      function openCallbackModal() {
        if (!pendingRelay) return;
        callbackSessionMetaEl.textContent = pendingRelay.sessionId ? `session: ${pendingRelay.sessionId}` : "";
        callbackUrlInputEl.value = "";
        callbackHintEl.textContent = "";
        callbackModalEl.classList.add("open");
      }

      function closeCallbackModal() {
        callbackModalEl.classList.remove("open");
      }

      async function cancelAddAccountFlow() {
        const sid = pendingRelay?.sessionId || null;
        try {
          await apiFetch("/auth/login/cancel", {
            method: "POST",
            body: JSON.stringify({ session_id: sid }),
          });
        } catch (_) {
          // Best effort cancel.
        }
        pendingRelay = null;
        closeCallbackModal();
      }

      function parseCallbackUrl(fullUrl) {
        let parsed;
        try {
          parsed = new URL(fullUrl);
        } catch (_) {
          throw new Error("Callback URL is invalid.");
        }
        const code = parsed.searchParams.get("code");
        const state = parsed.searchParams.get("state");
        const error = parsed.searchParams.get("error");
        const error_description = parsed.searchParams.get("error_description");
        if (!code && !error) {
          throw new Error("Callback URL must include code or error.");
        }
        return { code, state, error, error_description };
      }

      async function submitCallbackLink() {
        if (!pendingRelay?.sessionId || !pendingRelay?.relayToken) {
          throw new Error("No active login session. Click Add Account first.");
        }
        const fullUrl = callbackUrlInputEl.value.trim();
        if (!fullUrl) throw new Error("Paste the callback URL first.");
        const parsed = parseCallbackUrl(fullUrl);

        const payload = {
          session_id: pendingRelay.sessionId,
          relay_token: pendingRelay.relayToken,
          full_url: fullUrl,
          ...parsed,
        };
        const res = await apiFetch("/auth/relay-callback", {
          method: "POST",
          body: JSON.stringify(payload),
        });
        if (!res.ok) throw new Error(await readError(res, "Relay callback failed"));
        closeCallbackModal();
        callbackHintEl.textContent = "";
        setStatus("Callback relayed. Waiting for auth completion...");

        const ready = await waitForLoginCompletion(pendingRelay.sessionId, 25);
        if (!ready) {
          setStatus("Callback relayed. Auth is still processing. Click Import Current in a few seconds.", true);
          return;
        }

        const importRes = await apiFetch("/auth/import-current", {
          method: "POST",
          body: JSON.stringify({}),
        });
        if (!importRes.ok) {
          throw new Error(await readError(importRes, "Auth completed, but import failed"));
        }
        const importData = await importRes.json();
        pendingRelay = null;
        setStatus("Account imported as '" + importData.label + "'");
      }

      async function waitForLoginCompletion(sessionId, timeoutSeconds = 25) {
        const deadline = Date.now() + timeoutSeconds * 1000;
        while (Date.now() < deadline) {
          const res = await fetch("/auth/login/status?session_id=" + encodeURIComponent(sessionId));
          if (!res.ok) return false;
          const data = await res.json();
          if (data?.auth?.updated || data?.status === "complete") return true;
          if (data?.status === "failed") {
            throw new Error(data?.error || "Login failed before auth import");
          }
          await new Promise(r => setTimeout(r, 1000));
        }
        return false;
      }

      async function importCurrent() {
        const lbl = importLabelInputEl.value.trim();
        const body = lbl ? { label: lbl } : {};
        const res = await apiFetch("/auth/import-current", { method: "POST", body: JSON.stringify(body) });
        if (!res.ok) throw new Error(await readError(res, "Import failed"));
        const data = await res.json();
        setStatus("Imported current auth as '" + data.label + "'");
      }

      function openImportModal() {
        importHintEl.textContent = "";
        importJsonInputEl.value = "";
        importFileInputEl.value = "";
        importLabelInputEl.value = "";
        importModalEl.classList.add("open");
      }

      function closeImportModal() {
        importModalEl.classList.remove("open");
      }

      async function parseImportedAuthJson() {
        const pasted = importJsonInputEl.value.trim();
        if (pasted) {
          try { return JSON.parse(pasted); }
          catch (_) { throw new Error("Pasted JSON is invalid."); }
        }
        const file = importFileInputEl.files && importFileInputEl.files[0];
        if (file) {
          const txt = await file.text();
          try { return JSON.parse(txt); }
          catch (_) { throw new Error("Uploaded file is not valid JSON."); }
        }
        throw new Error("Paste JSON or upload a JSON file first.");
      }

      async function importProvidedJson() {
        const authJson = await parseImportedAuthJson();
        const lbl = importLabelInputEl.value.trim();
        const body = { auth_json: authJson };
        if (lbl) body.label = lbl;
        const res = await apiFetch("/auth/import-json", { method: "POST", body: JSON.stringify(body) });
        if (!res.ok) throw new Error(await readError(res, "Import JSON failed"));
        const data = await res.json();
        setStatus("Imported credentials as '" + data.label + "'");
      }

      async function switchProfile(label) {
        const res = await apiFetch("/auth/switch", { method: "POST", body: JSON.stringify({ label }) });
        if (!res.ok) throw new Error(await readError(res, "Switch failed"));
        setStatus("Switched to '" + label + "'");
      }

      async function exportProfile(label) {
        const res = await apiFetch("/auth/export?label=" + encodeURIComponent(label), { method: "GET" });
        if (!res.ok) throw new Error(await readError(res, "Export failed"));
        const data = await res.json();
        const blob = new Blob([JSON.stringify(data.auth_json, null, 2)], { type: "application/json" });
        const a = Object.assign(document.createElement("a"), { href: URL.createObjectURL(blob), download: label + "-auth.json" });
        document.body.appendChild(a); a.click(); a.remove(); URL.revokeObjectURL(a.href);
        setStatus("Exported '" + label + "'");
      }

      async function deleteProfile(label) {
        const confirmed = window.confirm(`Delete profile "${label}" from this system? This removes saved auth + usage history.`);
        if (!confirmed) return;
        const res = await apiFetch("/auth/delete", { method: "POST", body: JSON.stringify({ label }) });
        if (!res.ok) throw new Error(await readError(res, "Delete failed"));
        setStatus("Deleted '" + label + "'");
      }

      function openEditProfileModal(label) {
        pendingRenameLabel = label;
        editProfileInputEl.value = label;
        editProfileHintEl.textContent = "";
        editProfileModalEl.classList.add("open");
        setTimeout(() => editProfileInputEl.focus(), 0);
      }

      function closeEditProfileModal() {
        editProfileModalEl.classList.remove("open");
        pendingRenameLabel = null;
      }

      async function renameProfile(oldLabel, newLabel) {
        const res = await apiFetch("/auth/rename", {
          method: "POST",
          body: JSON.stringify({ old_label: oldLabel, new_label: newLabel }),
        });
        if (!res.ok) throw new Error(await readError(res, "Rename failed"));
        return await res.json();
      }

      const refreshAll = () => Promise.all([loadPublicStats(), loadAccounts()]);

      $("tokenSave").addEventListener("click", async () => { localStorage.setItem("internalToken", tokenInput.value.trim()); await refreshAll(); });
      $("tokenClear").addEventListener("click", async () => { localStorage.removeItem("internalToken"); tokenInput.value = ""; await refreshAll(); });
      $("addAccount").addEventListener("click", async () => { try { await startLogin(); } catch (e) { setStatus(e.message, true); } });
      $("importCurrent").addEventListener("click", () => { openImportModal(); });
      $("refreshAll").addEventListener("click", refreshAll);
      accountsEl.addEventListener("click", async e => {
        const t = e.target.closest("[data-action]"); if (!t) return;
        const action = t.dataset.action, label = t.dataset.label; if (!action || !label) return;
        if (action === "menu") {
          const root = t.closest("[data-menu-root]");
          if (!root) return;
          document.querySelectorAll("[data-menu-root].open").forEach(el => { if (el !== root) el.classList.remove("open"); });
          root.classList.toggle("open");
          return;
        }
        document.querySelectorAll("[data-menu-root].open").forEach(el => el.classList.remove("open"));
        try {
          if (action === "switch") { await switchProfile(label); await refreshAll(); }
          else if (action === "edit") { openEditProfileModal(label); }
          else if (action === "export") { await exportProfile(label); }
          else if (action === "delete") { await deleteProfile(label); await refreshAll(); }
        } catch (err) { setStatus(err.message, true); }
      });
      document.addEventListener("click", e => {
        if (!(e.target instanceof Element)) return;
        if (!e.target.closest("[data-menu-root]")) {
          document.querySelectorAll("[data-menu-root].open").forEach(el => el.classList.remove("open"));
        }
      });
      $("openAuthUrl").addEventListener("click", () => {
        if (!pendingRelay?.authUrl) {
          callbackHintEl.textContent = "No auth URL was returned by the server.";
          return;
        }
        window.open(pendingRelay.authUrl, "_blank", "noopener,noreferrer");
      });
      $("submitCallback").addEventListener("click", async () => {
        try {
          callbackHintEl.textContent = "";
          await submitCallbackLink();
          await refreshAll();
        } catch (err) {
          callbackHintEl.textContent = err.message || "Unable to submit callback link.";
        }
      });
      $("cancelCallback").addEventListener("click", () => {
        callbackHintEl.textContent = "";
        cancelAddAccountFlow();
      });
      callbackModalEl.addEventListener("click", e => {
        if (e.target === callbackModalEl) {
          callbackHintEl.textContent = "";
          cancelAddAccountFlow();
        }
      });
      $("cancelEditProfile").addEventListener("click", () => {
        editProfileHintEl.textContent = "";
        closeEditProfileModal();
      });
      $("saveEditProfile").addEventListener("click", async () => {
        const oldLabel = pendingRenameLabel;
        const nextLabel = editProfileInputEl.value.trim();
        if (!oldLabel) return;
        if (!nextLabel) {
          editProfileHintEl.textContent = "Profile name cannot be empty.";
          return;
        }
        try {
          const result = await renameProfile(oldLabel, nextLabel);
          closeEditProfileModal();
          setStatus(result.status === "unchanged" ? "Profile name unchanged." : `Renamed '${oldLabel}' to '${result.label}'.`);
          await refreshAll();
        } catch (err) {
          editProfileHintEl.textContent = err.message || "Unable to rename profile.";
        }
      });
      editProfileModalEl.addEventListener("click", e => {
        if (e.target === editProfileModalEl) {
          editProfileHintEl.textContent = "";
          closeEditProfileModal();
        }
      });
      $("cancelImport").addEventListener("click", () => {
        importHintEl.textContent = "";
        closeImportModal();
      });
      $("importCurrentModalBtn").addEventListener("click", async () => {
        try {
          importHintEl.textContent = "";
          await importCurrent();
          closeImportModal();
          await refreshAll();
        } catch (err) {
          importHintEl.textContent = err.message || "Import current failed.";
        }
      });
      $("importJsonModalBtn").addEventListener("click", async () => {
        try {
          importHintEl.textContent = "";
          await importProvidedJson();
          closeImportModal();
          await refreshAll();
        } catch (err) {
          importHintEl.textContent = err.message || "Import JSON failed.";
        }
      });
      importModalEl.addEventListener("click", e => {
        if (e.target === importModalEl) {
          importHintEl.textContent = "";
          closeImportModal();
        }
      });
      refreshAll();
      setInterval(() => { loadPublicStats(); if(getToken()) loadAccounts(); }, 15000);
    </script>
  </body>
</html>"""
