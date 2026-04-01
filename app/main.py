from __future__ import annotations

import base64
import hmac
import ipaddress
import json
import os
import hashlib
import logging
import re
import secrets
import asyncio
from dataclasses import dataclass
from hashlib import sha256
from contextlib import suppress
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles

from .accounts import AccountProfile, list_profiles
from .account_identity import (
    decode_jwt_claims,
    extract_access_token,
    extract_account_identity,
    extract_email,
    extract_id_token,
    extract_refresh_token,
)
from .account_usage_store import (
    delete_account_data,
    ensure_account,
    delete_saved_profile,
    get_active_auth_json,
    get_active_auth_updated_at,
    get_saved_profile,
    get_account,
    get_active_profile_label,
    get_runtime_settings,
    initialize_usage_store,
    list_openclaw_usage_by_credential,
    list_absolute_usage_snapshots,
    list_usage_rollovers,
    list_usage_snapshots,
    migrate_legacy_local_state,
    merge_account_data,
    migrate_account_ids,
    reconcile_legacy_account_aliases,
    rename_saved_profile,
    rename_account_data,
    import_openclaw_usage_export,
    record_account_usage,
    record_absolute_usage_snapshot,
    record_percentage_snapshot,
    reconcile_due_accounts,
    set_active_auth_json,
    set_active_profile_label,
    sync_account_rate_limit_percentages,
    sync_account_usage_snapshot,
    touch_profile_last_used,
    update_saved_profile_reauth_status,
    update_runtime_settings,
    upsert_saved_profile,
)
from .lease_broker_store import (
    ROTATION_REASONS,
    acquire_broker_lease,
    get_broker_lease,
    get_broker_lease_status,
    initialize_lease_broker_store,
    is_credential_assignable,
    list_broker_lease_telemetry,
    list_broker_leases,
    list_active_broker_leases_by_credential,
    list_broker_credentials,
    mark_broker_credential_exhausted,
    set_broker_credential_assignment_disabled,
    materialize_broker_lease,
    record_broker_lease_telemetry,
    reconcile_broker_leases,
    release_broker_lease,
    renew_broker_lease,
    rotate_broker_lease,
    sync_broker_credential,
)
from .auth_store import (
    AuthStoreError,
    AuthStoreSwitchError,
    persist_and_save_label,
    get_current_auth_label,
    list_auth_labels,
    persist_current_auth,
    save_current_auth_under_label,
    switch_active_auth_to_label,
)
from .codex_cli import (
    CodexCLIError,
    cancel_login,
    derive_label,
    get_login_status,
    read_rate_limits_for_auth_async,
    read_rate_limits_via_app_server_async,
    read_current_auth,
    relay_callback_to_login_async,
    start_login,
    wait_for_auth_update,
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
from .oauth_flow import build_auth_payload

APP_VERSION = "1.2.5"
GITHUB_REPO = "HalSysFin/Codex-Auth-Manager"
_VERSION_CHECK_CACHE: dict[str, Any] = {
    "checked_at": None,
    "payload": None,
}
_VERSION_CHECK_CACHE_TTL_SECONDS = 3600

app = FastAPI(title="Codex Auth Manager", version=APP_VERSION)
logger = logging.getLogger(__name__)
_LEASE_RECONCILE_INTERVAL_SECONDS = 15
_AUTH_KEEPALIVE_INTERVAL_SECONDS = 300
_AUTH_REFRESH_LEEWAY_SECONDS = 10 * 60
_LIVE_REFRESH_CONCURRENCY = 4
_USAGE_STALE_SECONDS = 1800
_reconcile_task: asyncio.Task[None] | None = None
_lease_reconcile_task: asyncio.Task[None] | None = None
_auth_keepalive_task: asyncio.Task[None] | None = None
_rate_limit_sync_task: asyncio.Task[None] | None = None
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


def _runtime_settings_payload() -> dict[str, Any]:
    return get_runtime_settings()


def _usage_snapshot_interval_seconds() -> int:
    runtime = _runtime_settings_payload()
    return max(int(runtime.get("analytics_snapshot_interval_seconds") or 600), 60)


def _audit_broker_event(event: str, **fields: Any) -> None:
    payload = {"event": event}
    for key, value in fields.items():
        if value is None:
            continue
        payload[key] = value
    logger.info("broker_audit %s", json.dumps(payload, separators=(",", ":"), sort_keys=True))


def _broker_health_score_for_profile(
    *,
    utilization_pct: float | None,
    refresh_status: dict[str, Any] | None,
) -> float:
    score = 100.0
    if utilization_pct is not None:
        score -= max(0.0, min(utilization_pct, 100.0)) * 0.6
    if refresh_status and refresh_status.get("is_stale"):
        score -= 15.0
    if refresh_status and refresh_status.get("state") == "failed":
        score -= 30.0
    if refresh_status and refresh_status.get("reauth_required"):
        score = min(score, 5.0)
    return round(max(0.0, min(score, 100.0)), 2)


def _refresh_error_requires_reauth(error: str | None) -> bool:
    text = str(error or "").strip().lower()
    if not text:
        return False
    markers = (
        "token_expired",
        "refresh_token_reused",
        "sign in again",
        "signing in again",
        "please try signing in again",
        "please log out and sign in again",
        "provided authentication token is expired",
        "refresh token was already used",
        "access token could not be refreshed",
    )
    return any(marker in text for marker in markers)


def _access_token_expired(auth_json: dict[str, Any] | None) -> bool:
    if not isinstance(auth_json, dict):
        return False
    access_token = extract_access_token(auth_json)
    if not access_token:
        return False
    claims = decode_jwt_claims(access_token)
    if not claims:
        return False
    exp = claims.get("exp")
    if not isinstance(exp, (int, float)):
        return False
    return datetime.fromtimestamp(float(exp), tz=timezone.utc) <= datetime.now(timezone.utc)


def _access_token_expiry_ts(auth_json: dict[str, Any] | None) -> int | None:
    if not isinstance(auth_json, dict):
        return None
    access_token = extract_access_token(auth_json)
    if not access_token:
        return None
    claims = decode_jwt_claims(access_token)
    if not claims:
        return None
    exp = claims.get("exp")
    if not isinstance(exp, (int, float)):
        return None
    return int(exp)


def _access_token_expiry_payload(auth_json: dict[str, Any] | None) -> dict[str, Any]:
    exp_ts = _access_token_expiry_ts(auth_json)
    now_ts = int(datetime.now(timezone.utc).timestamp())
    if exp_ts is None:
        return {
            "access_token_expires_at": None,
            "access_token_expires_in_seconds": None,
            "access_token_expired": False,
        }
    expires_at = datetime.fromtimestamp(exp_ts, tz=timezone.utc).isoformat()
    return {
        "access_token_expires_at": expires_at,
        "access_token_expires_in_seconds": exp_ts - now_ts,
        "access_token_expired": exp_ts <= now_ts,
    }


def _decoded_token_payload(auth_json: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(auth_json, dict):
        return {
            "access": None,
            "id": None,
            "refresh": None,
        }

    access_token = extract_access_token(auth_json)
    id_token = extract_id_token(auth_json)
    refresh_token = extract_refresh_token(auth_json)

    def _token_info(token: str | None) -> dict[str, Any] | None:
        if not isinstance(token, str) or not token.strip():
            return None
        claims = decode_jwt_claims(token)
        exp = claims.get("exp") if isinstance(claims, dict) else None
        exp_iso = None
        if isinstance(exp, (int, float)) and exp > 0:
            exp_iso = datetime.fromtimestamp(int(exp), tz=timezone.utc).isoformat()
        return {
            "is_jwt": bool(isinstance(claims, dict)),
            "expires_at": exp_iso,
            "claims": claims if isinstance(claims, dict) else None,
        }

    return {
        "access": _token_info(access_token),
        "id": _token_info(id_token),
        "refresh": _token_info(refresh_token),
    }


def _saved_profile_token_metadata(profile: AccountProfile) -> dict[str, Any]:
    return {
        "access_token_expires_at": profile.access_token_expires_at,
        "id_token_expires_at": profile.id_token_expires_at,
        "refresh_token_expires_at": profile.refresh_token_expires_at,
        "last_refresh_at": profile.last_refresh_at,
        "refresh_token_present": profile.refresh_token_present,
        "reauth_required": profile.reauth_required,
        "reauth_reason": profile.reauth_reason,
    }


def _profile_reauth_requirement(
    profile: AccountProfile,
    refresh_status: dict[str, Any] | None,
) -> tuple[bool, str | None]:
    auth_json = profile.auth if isinstance(profile.auth, dict) else {}
    if not _access_token_expired(auth_json):
        return False, None
    status = refresh_status or {}
    if status.get("reauth_required"):
        return True, str(status.get("last_error") or "Reauthentication required")
    if profile.reauth_required:
        return True, profile.reauth_reason or "Reauthentication required"
    has_refresh = bool(extract_refresh_token(auth_json))
    if not has_refresh:
        return True, "Access token expired and no refresh token is available."
    if not _refresh_keepalive_supported():
        return True, "Access token expired and automatic refresh is not configured on the manager."
    return False, None


def _sync_broker_credentials_from_profiles() -> list[dict[str, Any]]:
    profiles = _dedupe_profiles(list_profiles())
    profile_by_account_key = {profile.account_key: profile for profile in profiles if profile.account_key}
    _touch_profiles_usage(profiles)
    cached = _build_cached_accounts_snapshot(profiles=profiles)
    synced: list[dict[str, Any]] = []
    for account in cached.get("accounts", []):
        if not isinstance(account, dict):
            continue
        account_key = str(account.get("account_key") or "").strip()
        if not account_key:
            continue
        usage = account.get("usage_tracking") or {}
        refresh_status = account.get("refresh_status") or {}
        utilization_pct = usage.get("secondary_used_percent")
        usage_limit = usage.get("usage_limit")
        usage_in_window = usage.get("usage_in_window")
        quota_remaining = None
        if isinstance(usage_limit, (int, float)) and usage_limit > 0 and isinstance(usage_in_window, (int, float)):
            quota_remaining = max(int(usage_limit) - int(usage_in_window), 0)
        metadata = {
            "label": account.get("label"),
            "display_label": account.get("display_label"),
            "email": account.get("email"),
            "account_type": account.get("account_type"),
        }
        if refresh_status.get("reauth_required"):
            metadata["reauth_required"] = True
            metadata["reauth_reason"] = refresh_status.get("last_error")
        profile = profile_by_account_key.get(account_key)
        if profile and profile.auth_updated_at:
            metadata["auth_updated_at"] = profile.auth_updated_at
        synced.append(
            sync_broker_credential(
                credential_id=account_key,
                label=str(account.get("display_label") or account.get("label") or account.get("account_key") or ""),
                utilization_pct=float(utilization_pct) if isinstance(utilization_pct, (int, float)) else None,
                quota_remaining=quota_remaining,
                health_score=_broker_health_score_for_profile(
                    utilization_pct=float(utilization_pct) if isinstance(utilization_pct, (int, float)) else None,
                    refresh_status=refresh_status,
                ),
                weekly_reset_at=(
                    usage.get("secondary_resets_at")
                    or usage.get("rate_limit_refresh_at")
                ),
                last_telemetry_at=usage.get("last_usage_sync_at") or usage.get("updated_at"),
                metadata=metadata,
            )
        )
    return synced


def _leased_profile_payload_for_credential(
    credential_id: str,
    *,
    label_hint: str | None = None,
) -> dict[str, Any] | None:
    if label_hint:
        saved = get_saved_profile(label_hint)
        if saved and str(saved.get("account_key") or "") == credential_id:
            return saved
    for profile in _dedupe_profiles(list_profiles()):
        if profile.account_key == credential_id:
            return {
                "label": profile.label,
                "account_key": profile.account_key,
                "email": profile.email,
                "name": getattr(profile, "name", None),
                "provider_account_id": None,
                "auth_updated_at": getattr(profile, "auth_updated_at", None),
                "auth_json": profile.auth,
            }
    return None


@app.on_event("startup")
async def on_startup() -> None:
    initialize_usage_store()
    initialize_lease_broker_store()
    try:
        migration_result = migrate_legacy_local_state(
            sqlite_usage_path=settings.usage_db_file(),
            profiles_dir=settings.profiles_dir(),
        )
        if any(v > 0 for v in migration_result.values()):
            logger.info("legacy local state migrated to primary DB: %s", migration_result)
    except Exception as exc:
        logger.warning("legacy migration skipped/failed: %s", exc)
    _migrate_usage_keys_from_labels()
    try:
        reconciled = reconcile_legacy_account_aliases()
        if reconciled:
            logger.info("reconciled legacy account aliases into canonical account keys: %s", reconciled)
    except Exception as exc:
        logger.warning("legacy account alias reconciliation failed: %s", exc)
    try:
        # Avoid blocking startup on Codex CLI refreshes; let the periodic task handle it.
        if settings.openai_token_url and settings.openai_client_id:
            refreshed = await _refresh_saved_auths_if_needed()
            if refreshed:
                logger.info("auth keepalive refreshed %s saved auth(s) on startup", refreshed)
                _sync_broker_credentials_from_profiles()
    except Exception as exc:
        logger.warning("startup auth keepalive refresh failed: %s", exc)
    global _reconcile_task
    global _lease_reconcile_task
    global _auth_keepalive_task
    global _rate_limit_sync_task
    if _reconcile_task is None:
        _reconcile_task = asyncio.create_task(_periodic_reconcile_usage_windows())
    if _lease_reconcile_task is None:
        _lease_reconcile_task = asyncio.create_task(_periodic_reconcile_broker_leases())
    if settings.auth_keepalive_in_app and _auth_keepalive_task is None:
        _auth_keepalive_task = asyncio.create_task(_periodic_refresh_saved_auths())
    if _rate_limit_sync_task is None:
        _rate_limit_sync_task = asyncio.create_task(_periodic_refresh_profile_rate_limits())


@app.on_event("shutdown")
async def on_shutdown() -> None:
    global _reconcile_task
    global _lease_reconcile_task
    global _auth_keepalive_task
    global _rate_limit_sync_task
    if _reconcile_task is not None:
        _reconcile_task.cancel()
        with suppress(asyncio.CancelledError):
            await _reconcile_task
        _reconcile_task = None
    if _lease_reconcile_task is not None:
        _lease_reconcile_task.cancel()
        with suppress(asyncio.CancelledError):
            await _lease_reconcile_task
        _lease_reconcile_task = None
    if _auth_keepalive_task is not None:
        _auth_keepalive_task.cancel()
        with suppress(asyncio.CancelledError):
            await _auth_keepalive_task
        _auth_keepalive_task = None
    if _rate_limit_sync_task is not None:
        _rate_limit_sync_task.cancel()
        with suppress(asyncio.CancelledError):
            await _rate_limit_sync_task
        _rate_limit_sync_task = None


async def _periodic_reconcile_usage_windows() -> None:
    while True:
        await asyncio.sleep(_usage_snapshot_interval_seconds())
        try:
            refreshed = reconcile_due_accounts(now=datetime.now(timezone.utc))
            if refreshed:
                logger.info("usage reconciliation refreshed %s account window(s)", refreshed)
            _capture_periodic_usage_snapshots(now=datetime.now(timezone.utc))
        except Exception:
            logger.exception("usage reconciliation failed")


async def _periodic_reconcile_broker_leases() -> None:
    while True:
        await asyncio.sleep(_LEASE_RECONCILE_INTERVAL_SECONDS)
        try:
            expired = reconcile_broker_leases(now=datetime.now(timezone.utc))
            if expired:
                logger.info("lease reconciliation reclaimed %s stale/expired lease(s)", expired)
        except Exception:
            logger.exception("lease reconciliation failed")


async def _periodic_refresh_saved_auths() -> None:
    while True:
        await asyncio.sleep(_AUTH_KEEPALIVE_INTERVAL_SECONDS)
        try:
            refreshed = await _refresh_saved_auths_if_needed()
            if refreshed:
                logger.info("auth keepalive refreshed %s saved auth(s)", refreshed)
                _sync_broker_credentials_from_profiles()
        except Exception:
            logger.exception("auth keepalive failed")


async def _periodic_refresh_profile_rate_limits() -> None:
    while True:
        await asyncio.sleep(max(int(settings.rate_limit_sync_interval_seconds), 60))
        try:
            refreshed, failed = await _refresh_all_profile_rate_limits()
            if refreshed or failed:
                logger.info(
                    "profile rate-limit sync completed refreshed=%s failed=%s",
                    refreshed,
                    failed,
                )
        except Exception:
            logger.exception("profile rate-limit sync failed")


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


@app.get("/api/settings")
async def api_settings() -> JSONResponse:
    return JSONResponse({"runtime": _runtime_settings_payload()})


@app.post("/api/settings")
async def api_settings_update(request: Request, payload: dict[str, Any]) -> JSONResponse:
    _require_internal_auth(request)
    updated = update_runtime_settings(payload if isinstance(payload, dict) else {})
    return JSONResponse({"runtime": updated})


@app.get("/login")
async def login_page(next: str = "/") -> HTMLResponse:
    return HTMLResponse(_render_login(next))


@app.post("/login")
async def login_submit(request: Request) -> JSONResponse:
    if not _web_login_enabled():
        raise HTTPException(status_code=503, detail="Web login is not configured")

    content_type = request.headers.get("content-type", "")
    username = ""
    password = ""
    next_path = "/"

    if "application/json" in content_type:
        payload = await request.json()
        if isinstance(payload, dict):
            username = str(payload.get("username", "")).strip()
            password = str(payload.get("password", ""))
            next_path = str(payload.get("next", "/")) or "/"
    else:
        raw = (await request.body()).decode("utf-8", errors="replace")
        parsed = parse_qs(raw, keep_blank_values=True)
        username = (parsed.get("username", [""])[0] or "").strip()
        password = parsed.get("password", [""])[0] or ""
        next_path = parsed.get("next", ["/"])[0] or "/"

    if not _verify_web_credentials(username, password):
        raise HTTPException(status_code=401, detail="Invalid username or password")

    response = JSONResponse({"ok": True, "next": _safe_next_path(next_path)})
    _set_web_session_cookie(request, response)
    return response


@app.post("/logout")
async def logout(request: Request) -> JSONResponse:
    response = JSONResponse({"ok": True})
    response.delete_cookie(settings.web_login_cookie_name, path="/")
    return response


@app.get("/")
async def index() -> HTMLResponse:
    return _spa_or_legacy_index()


@app.get("/ui")
async def ui() -> HTMLResponse:
    return _spa_or_legacy_index()


@app.get("/v1")
async def v1_legacy_ui() -> RedirectResponse:
    return RedirectResponse(url="/ui", status_code=307)


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
                "message": "Auth saved and profile updated in DB.",
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
        except (CodexCLIError, AuthStoreError, AuthStoreSwitchError) as exc:
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
    full_url = str(payload.get("full_url", "")).strip()
    parsed_callback = _parse_callback_payload(full_url)
    code = payload.get("code") or parsed_callback.get("code")
    state = payload.get("state") or parsed_callback.get("state")
    error = payload.get("error") or parsed_callback.get("error")
    error_description = payload.get("error_description") or parsed_callback.get(
        "error_description"
    )

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
    state_matches_session = not (
        expected_state and state and str(state).strip() != expected_state
    )
    if not state_matches_session:
        logger.warning(
            "relay-callback accepting mismatched state for pasted/manual callback "
            "session_id=%s expected=%s got=%s",
            session_id,
            expected_state,
            state,
        )

    callback_payload = {
        "code": code,
        "state": state,
        "error": error,
        "error_description": error_description,
        "full_url": full_url,
        "state_matches_session": state_matches_session,
        "expected_state": expected_state,
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
    handoff = await relay_callback_to_login_async(callback_payload)
    logger.info(
        "relay-callback accepted session_id=%s provider_error=%s handoff_supported=%s",
        session_id,
        bool(error),
        bool(handoff.get("supported")),
    )

    auto_persist: dict[str, Any] = {"attempted": False}
    if bool(handoff.get("completed")) and not error:
        auto_persist["attempted"] = True
        try:
            current_auth = read_current_auth()
            auth_validation = _validate_relay_finalized_auth(
                current_auth,
                started_at_iso=updated.created_at.isoformat() if updated.created_at else None,
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

            desired_label = payload.get("label")
            persist_result = _persist_current_auth_to_profile(
                desired_label=(str(desired_label) if desired_label is not None else None),
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
                    "saved": persist_result.persisted or persist_result.up_to_date,
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
        except (CodexCLIError, AuthStoreError, AuthStoreSwitchError) as exc:
            logger.warning("auto persist during relay callback failed: %s", exc)
            auto_persist.update(
                {
                    "status": "error",
                    "reason": "persist_failed",
                    "error": str(exc),
                }
            )

    return JSONResponse(
        {
            "status": "callback_received",
            "session": to_public_session(updated),
            "handoff": handoff,
            "auto_persist": auto_persist,
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
    except (AuthStoreError, AuthStoreSwitchError) as exc:
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
    except (AuthStoreError, AuthStoreSwitchError) as exc:
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
        result = switch_active_auth_to_label(label)
        now_current = _resolve_current_label(read_current_auth(), list_profiles())
        if profile is not None:
            _touch_account_usage(profile=profile)
            with suppress(Exception):
                _persist_active_auth_db_copy(profile.label)
    except AuthStoreSwitchError as exc:
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

    profile = _profile_for_label(label)
    if profile is None:
        raise HTTPException(status_code=404, detail="Label not found")
    usage_key = profile.account_key if profile else label

    try:
        deleted = delete_saved_profile(label)
        if not deleted:
            raise HTTPException(status_code=404, detail="Label not found")
        delete_account_data(usage_key)
        if (get_active_profile_label() or "").strip() == label:
            set_active_profile_label(None)
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

    source_profile = get_saved_profile(old_label)
    if source_profile is None:
        raise HTTPException(status_code=404, detail="old_label not found")
    merged_duplicate = False
    target_profile = get_saved_profile(new_label)
    if target_profile is not None:
        source_auth = source_profile.get("auth_json")
        target_auth = target_profile.get("auth_json")
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
        with suppress(Exception):
            delete_saved_profile(new_label)
        with suppress(Exception):
            delete_account_data(new_label)
        merged_duplicate = True

    try:
        renamed = rename_saved_profile(old_label, new_label)
        if not renamed:
            raise HTTPException(status_code=404, detail="old_label not found")
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    with suppress(Exception):
        rename_account_data(old_label, new_label)
    with suppress(Exception):
        renamed_profile = _profile_for_label(new_label)
        if renamed_profile is not None:
            merge_account_data(old_label, renamed_profile.account_key)

    now_current = None
    was_current = False
    try:
        was_current = (get_active_profile_label() or get_current_auth_label()) == old_label
    except AuthStoreSwitchError:
        was_current = False
    if was_current:
        set_active_profile_label(new_label)
        now_current = new_label

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
    meta = _auth_file_metadata()

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

    expiry_payload = _access_token_expiry_payload(auth_json)

    return JSONResponse(
        {
            "auth": meta,
            "email": email,
            "account_key": identity.account_key,
            "current_label": current,
            "current_display_label": _display_label(current, email),
            "status": "ok",
            "last_refresh": auth_json.get("last_refresh"),
            "has_refresh_token": bool(extract_refresh_token(auth_json)),
            "decoded_tokens": _decoded_token_payload(auth_json),
            **expiry_payload,
        }
    )


@app.get("/auth/rate-limits")
async def auth_rate_limits(request: Request) -> JSONResponse:
    _require_internal_auth(request)
    try:
        active_auth = read_current_auth()
        if (
            isinstance(active_auth, dict)
            and extract_refresh_token(active_auth)
            and _refresh_keepalive_supported()
        ):
            # Ensure we attempt a refresh through the CLI once the access token is dead.
            if _auth_access_token_expiring_soon(active_auth, leeway_seconds=0):
                await _refresh_active_auth_if_needed(active_auth)
    except CodexCLIError:
        pass
    try:
        result = await read_rate_limits_via_app_server_async()
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
            if current_profile is not None:
                with suppress(Exception):
                    _persist_active_auth_db_copy(current_profile.label)
    except CodexCLIError:
        current_label = None

    return JSONResponse(
        {
            "source": "openai_api",
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
    auth_payload = build_auth_payload(token_response)

    stored_at = _store_callback(
        {
            "type": "token_response",
            "received_at": datetime.now(timezone.utc).isoformat(),
            "token_response": token_response,
            "auth_json": auth_payload,
        }
    )

    if label:
        _persist_auth_and_save(str(label), auth_payload)
        return JSONResponse(
            {
                "stored_at": str(stored_at),
                "saved_label": str(label),
                "auth_json": auth_payload,
                "token_response": token_response,
            }
        )

    return JSONResponse(
        {"stored_at": str(stored_at), "auth_json": auth_payload, "token_response": token_response}
    )


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


@app.get("/api/accounts/next-available")
async def api_next_available_account(
    request: Request,
    include_auth: bool = False,
) -> JSONResponse:
    _require_internal_auth(request)
    snapshot = _build_cached_accounts_snapshot()
    accounts = snapshot.get("accounts") if isinstance(snapshot, dict) else []
    if not isinstance(accounts, list) or not accounts:
        return JSONResponse({"recommended": None, "candidates": [], "reason": "no_accounts"})

    ranked = sorted(
        (_rank_account_for_weekly_availability(account) for account in accounts if isinstance(account, dict)),
        key=lambda item: (
            float(item.get("score") or -1.0),
            float(item.get("weekly_remaining_percent") or -1.0),
            -1.0 * float(item.get("seconds_to_weekly_reset") or 9_999_999_999),
        ),
        reverse=True,
    )
    if not ranked:
        return JSONResponse({"recommended": None, "candidates": [], "reason": "no_rankable_accounts"})

    best = dict(ranked[0])
    label = str(best.get("label") or "")
    if include_auth and label:
        saved = get_saved_profile(label)
        if saved and isinstance(saved.get("auth_json"), dict):
            best["auth_json"] = saved.get("auth_json")

    return JSONResponse(
        {
            "recommended": best,
            "candidates": ranked,
            "evaluated": len(ranked),
            "current_label": snapshot.get("current_label"),
            "selection_basis": "weekly_remaining_usage_then_next_weekly_refresh",
        }
    )


@app.post("/api/leases/acquire")
async def api_lease_acquire(request: Request, payload: dict[str, Any]) -> JSONResponse:
    _require_internal_auth(request)
    machine_id = str(payload.get("machine_id") or "").strip()
    agent_id = str(payload.get("agent_id") or "").strip()
    if not machine_id or not agent_id:
        raise HTTPException(status_code=400, detail="machine_id and agent_id are required")
    _sync_broker_credentials_from_profiles()
    result = acquire_broker_lease(
        machine_id=machine_id,
        agent_id=agent_id,
        requested_ttl_seconds=payload.get("requested_ttl_seconds"),
        reason=payload.get("reason"),
    )
    _audit_broker_event(
        "lease_acquired" if result["status"] == "ok" else "lease_acquire_denied",
        lease_id=(result.get("lease") or {}).get("id"),
        credential_id=(result.get("lease") or {}).get("credential_id"),
        machine_id=machine_id,
        agent_id=agent_id,
        decision=result["status"],
        reason=result.get("reason") or payload.get("reason"),
    )
    return JSONResponse(result, status_code=200 if result["status"] == "ok" else 409)


@app.post("/api/leases/{lease_id}/renew")
async def api_lease_renew(request: Request, lease_id: str, payload: dict[str, Any]) -> JSONResponse:
    _require_internal_auth(request)
    machine_id = str(payload.get("machine_id") or "").strip()
    agent_id = str(payload.get("agent_id") or "").strip()
    if not machine_id or not agent_id:
        raise HTTPException(status_code=400, detail="machine_id and agent_id are required")
    _sync_broker_credentials_from_profiles()
    result = renew_broker_lease(
        lease_id=lease_id,
        machine_id=machine_id,
        agent_id=agent_id,
    )
    _audit_broker_event(
        "lease_renewed" if result["status"] == "ok" else "lease_renew_denied",
        lease_id=lease_id,
        credential_id=((result.get("lease") or {}).get("credential_id")),
        machine_id=machine_id,
        agent_id=agent_id,
        decision=result["status"],
        reason=result.get("reason"),
    )
    return JSONResponse(result, status_code=200 if result["status"] == "ok" else 409)


@app.post("/api/leases/{lease_id}/release")
async def api_lease_release(request: Request, lease_id: str, payload: dict[str, Any]) -> JSONResponse:
    _require_internal_auth(request)
    machine_id = str(payload.get("machine_id") or "").strip()
    agent_id = str(payload.get("agent_id") or "").strip()
    if not machine_id or not agent_id:
        raise HTTPException(status_code=400, detail="machine_id and agent_id are required")
    result = release_broker_lease(
        lease_id=lease_id,
        machine_id=machine_id,
        agent_id=agent_id,
        reason=payload.get("reason"),
    )
    _audit_broker_event(
        "lease_released" if result["status"] == "ok" else "lease_release_denied",
        lease_id=lease_id,
        credential_id=((result.get("lease") or {}).get("credential_id")),
        machine_id=machine_id,
        agent_id=agent_id,
        decision=result["status"],
        reason=result.get("reason") or payload.get("reason"),
    )
    return JSONResponse(result, status_code=200 if result["status"] == "ok" else 409)


@app.post("/api/leases/{lease_id}/telemetry")
async def api_lease_telemetry(request: Request, lease_id: str, payload: dict[str, Any]) -> JSONResponse:
    _require_internal_auth(request)
    _sync_broker_credentials_from_profiles()
    machine_id = str(payload.get("machine_id") or "").strip()
    agent_id = str(payload.get("agent_id") or "").strip()
    captured_at = str(payload.get("captured_at") or "").strip()
    if not machine_id or not agent_id or not captured_at:
        raise HTTPException(status_code=400, detail="machine_id, agent_id, and captured_at are required")
    result = record_broker_lease_telemetry(
        lease_id=lease_id,
        machine_id=machine_id,
        agent_id=agent_id,
        captured_at=captured_at,
        requests_count=payload.get("requests_count"),
        tokens_in=payload.get("tokens_in"),
        tokens_out=payload.get("tokens_out"),
        utilization_pct=payload.get("utilization_pct"),
        quota_remaining=payload.get("quota_remaining"),
        rate_limit_remaining=payload.get("rate_limit_remaining"),
        status=payload.get("status"),
        last_success_at=payload.get("last_success_at"),
        last_error_at=payload.get("last_error_at"),
        error_rate_1h=payload.get("error_rate_1h"),
        metadata=payload.get("metadata") if isinstance(payload.get("metadata"), dict) else None,
    )
    event_name = "lease_telemetry_ingested" if result["status"] == "ok" else "lease_telemetry_denied"
    result_lease = result.get("lease") or {}
    result_credential = result.get("credential") or {}
    if result["status"] == "ok" and result_credential.get("state") == "exhausted":
        _audit_broker_event(
            "credential_marked_exhausted",
            lease_id=lease_id,
            credential_id=result_credential.get("id"),
            machine_id=machine_id,
            agent_id=agent_id,
            decision="ok",
            reason="credential_exhausted",
        )
    _audit_broker_event(
        event_name,
        lease_id=lease_id,
        credential_id=result_lease.get("credential_id"),
        machine_id=machine_id,
        agent_id=agent_id,
        decision=result["status"],
        reason=result.get("reason") or payload.get("status"),
    )
    return JSONResponse(result, status_code=200 if result["status"] == "ok" else 409)


@app.post("/api/leases/{lease_id}/materialize")
async def api_lease_materialize(request: Request, lease_id: str, payload: dict[str, Any]) -> JSONResponse:
    _require_internal_auth(request)
    _sync_broker_credentials_from_profiles()
    machine_id = str(payload.get("machine_id") or "").strip()
    agent_id = str(payload.get("agent_id") or "").strip()
    if not machine_id or not agent_id:
        raise HTTPException(status_code=400, detail="machine_id and agent_id are required")
    result = materialize_broker_lease(
        lease_id=lease_id,
        machine_id=machine_id,
        agent_id=agent_id,
    )
    if result["status"] != "ok":
        _audit_broker_event(
            "lease_materialize_denied",
            lease_id=lease_id,
            machine_id=machine_id,
            agent_id=agent_id,
            decision=result["status"],
            reason=result.get("reason"),
        )
        return JSONResponse(result, status_code=409)

    lease = result["lease"] or {}
    lease_metadata = lease.get("metadata") if isinstance(lease.get("metadata"), dict) else {}
    profile = _leased_profile_payload_for_credential(
        str(lease.get("credential_id") or ""),
        label_hint=str(lease_metadata.get("label") or ""),
    )
    if profile is None:
        _audit_broker_event(
            "lease_materialize_denied",
            lease_id=lease_id,
            credential_id=lease.get("credential_id"),
            machine_id=machine_id,
            agent_id=agent_id,
            decision="denied",
            reason="credential_material_not_found",
        )
        return JSONResponse(
            {"status": "denied", "reason": "credential_material_not_found", "lease": lease},
            status_code=404,
        )

    material = {
        "label": profile.get("label"),
        "account_key": profile.get("account_key"),
        "email": profile.get("email"),
        "name": profile.get("name"),
        "provider_account_id": profile.get("provider_account_id"),
        "auth_json": profile.get("auth_json"),
        "openclaw": _build_openclaw_material_for_auth(
            profile.get("auth_json") if isinstance(profile.get("auth_json"), dict) else {},
            email=profile.get("email") if isinstance(profile.get("email"), str) else None,
            name=profile.get("name") if isinstance(profile.get("name"), str) else None,
        ),
    }
    _audit_broker_event(
        "lease_materialized",
        lease_id=lease_id,
        credential_id=lease.get("credential_id"),
        machine_id=machine_id,
        agent_id=agent_id,
        decision="ok",
        reason="credential_material_delivered",
    )
    return JSONResponse({"status": "ok", "reason": None, "lease": lease, "credential_material": material})


@app.post("/api/leases/{lease_id}/reconcile-auth")
async def api_lease_reconcile_auth(request: Request, lease_id: str, payload: dict[str, Any]) -> JSONResponse:
    _require_internal_auth(request)
    _sync_broker_credentials_from_profiles()
    machine_id = str(payload.get("machine_id") or "").strip()
    agent_id = str(payload.get("agent_id") or "").strip()
    incoming_auth = payload.get("auth_json")
    if not machine_id or not agent_id:
        raise HTTPException(status_code=400, detail="machine_id and agent_id are required")
    if not isinstance(incoming_auth, dict):
        raise HTTPException(status_code=400, detail="auth_json object is required")

    lease_status = get_broker_lease_status(lease_id)
    if lease_status is None:
        raise HTTPException(status_code=404, detail="Lease not found")
    if (
        str(lease_status.get("machine_id") or "") != machine_id
        or str(lease_status.get("agent_id") or "") != agent_id
    ):
        raise HTTPException(status_code=409, detail="Lease not found or not owned")

    profile = _leased_profile_payload_for_credential(
        str(lease_status.get("credential_id") or ""),
        label_hint="",
    )
    if profile is None:
        raise HTTPException(status_code=404, detail="credential_material_not_found")

    current_auth = profile.get("auth_json") if isinstance(profile.get("auth_json"), dict) else None
    comparison = _compare_auth_freshness(current_auth, incoming_auth)
    current_label = str(profile.get("label") or "").strip() or None
    current_auth_updated_at = str(lease_status.get("credential_auth_updated_at") or "") or None

    if comparison > 0:
        persist_result = _persist_current_auth_to_profile(
            desired_label=current_label,
            create_if_missing=False,
            auth_json=incoming_auth,
        )
        if current_label and get_active_profile_label() == current_label:
            set_active_auth_json(incoming_auth)
        _sync_broker_credentials_from_profiles()
        refreshed = get_broker_lease_status(lease_id) or lease_status
        return JSONResponse(
            {
                "status": "ok",
                "decision": "client_updated_manager",
                "reason": persist_result.reason,
                "profile_label": persist_result.label,
                "credential_auth_updated_at": refreshed.get("credential_auth_updated_at"),
                "auth_json": None,
            }
        )

    if comparison < 0 and isinstance(current_auth, dict):
        refreshed = get_broker_lease_status(lease_id) or lease_status
        return JSONResponse(
            {
                "status": "ok",
                "decision": "manager_updated_client",
                "reason": "manager_auth_newer",
                "profile_label": current_label,
                "credential_auth_updated_at": refreshed.get("credential_auth_updated_at") or current_auth_updated_at,
                "auth_json": current_auth,
            }
        )

    return JSONResponse(
        {
            "status": "ok",
            "decision": "in_sync",
            "reason": "auth_in_sync",
            "profile_label": current_label,
            "credential_auth_updated_at": current_auth_updated_at,
            "auth_json": None,
        }
    )


@app.post("/api/leases/rotate")
async def api_lease_rotate(request: Request, payload: dict[str, Any]) -> JSONResponse:
    _require_internal_auth(request)
    lease_id = str(payload.get("lease_id") or "").strip()
    machine_id = str(payload.get("machine_id") or "").strip()
    agent_id = str(payload.get("agent_id") or "").strip()
    reason = str(payload.get("reason") or "").strip()
    if not lease_id or not machine_id or not agent_id or not reason:
        raise HTTPException(status_code=400, detail="lease_id, machine_id, agent_id, and reason are required")
    if reason not in ROTATION_REASONS:
        raise HTTPException(status_code=400, detail="invalid rotation reason")
    _sync_broker_credentials_from_profiles()
    result = rotate_broker_lease(
        lease_id=lease_id,
        machine_id=machine_id,
        agent_id=agent_id,
        reason=reason,
    )
    _audit_broker_event(
        "rotation_approved" if result["status"] == "ok" else "rotation_denied",
        lease_id=lease_id,
        credential_id=((result.get("lease") or {}).get("credential_id")),
        machine_id=machine_id,
        agent_id=agent_id,
        decision=result["status"],
        reason=result.get("reason") or reason,
    )
    return JSONResponse(result, status_code=200 if result["status"] == "ok" else 409)


@app.get("/api/leases/{lease_id}")
async def api_lease_status(request: Request, lease_id: str) -> JSONResponse:
    _require_internal_auth(request)
    _sync_broker_credentials_from_profiles()
    status = get_broker_lease_status(lease_id)
    if status is None:
        raise HTTPException(status_code=404, detail="Lease not found")
    return JSONResponse(status)


@app.post("/api/admin/credentials/{credential_id}/mark-exhausted")
async def api_admin_mark_credential_exhausted(
    request: Request,
    credential_id: str,
    payload: dict[str, Any] | None = None,
) -> JSONResponse:
    _require_internal_auth(request)
    _sync_broker_credentials_from_profiles()
    result = mark_broker_credential_exhausted(
        credential_id,
        reason=str((payload or {}).get("reason") or "admin_marked_exhausted"),
    )
    if result is None:
        raise HTTPException(status_code=404, detail="Credential not found")
    _audit_broker_event(
        "credential_marked_exhausted",
        credential_id=credential_id,
        decision="ok",
        reason=str((payload or {}).get("reason") or "admin_marked_exhausted"),
    )
    return JSONResponse({"status": "ok", "credential": result})


@app.post("/api/admin/credentials/{credential_id}/disable-assignment")
async def api_admin_disable_credential_assignment(
    request: Request,
    credential_id: str,
    payload: dict[str, Any] | None = None,
) -> JSONResponse:
    _require_internal_auth(request)
    _sync_broker_credentials_from_profiles()
    result = set_broker_credential_assignment_disabled(
        credential_id,
        disabled=True,
    )
    if result is None:
        raise HTTPException(status_code=404, detail="Credential not found")
    _audit_broker_event(
        "credential_assignment_disabled",
        credential_id=credential_id,
        decision="ok",
        reason=str((payload or {}).get("reason") or "admin_assignment_disabled"),
    )
    return JSONResponse({"status": "ok", "credential": result})


@app.post("/api/admin/credentials/{credential_id}/enable-assignment")
async def api_admin_enable_credential_assignment(
    request: Request,
    credential_id: str,
    payload: dict[str, Any] | None = None,
) -> JSONResponse:
    _require_internal_auth(request)
    _sync_broker_credentials_from_profiles()
    result = set_broker_credential_assignment_disabled(
        credential_id,
        disabled=False,
    )
    if result is None:
        raise HTTPException(status_code=404, detail="Credential not found")
    _audit_broker_event(
        "credential_assignment_enabled",
        credential_id=credential_id,
        decision="ok",
        reason=str((payload or {}).get("reason") or "admin_assignment_enabled"),
    )
    return JSONResponse({"status": "ok", "credential": result})


@app.get("/api/admin/leases/overview")
async def api_admin_leases_overview(request: Request) -> JSONResponse:
    _require_internal_auth(request)
    return JSONResponse(_lease_overview_payload())


@app.get("/api/admin/leases/stream")
async def api_admin_leases_stream(request: Request) -> StreamingResponse:
    _require_internal_auth_or_query(request)
    event_headers = {
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    }

    async def _event_gen():
        payload = _lease_overview_payload()
        payload_hash = hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        yield _sse_event("snapshot", payload)
        while True:
            if await request.is_disconnected():
                return
            await asyncio.sleep(10)
            latest = _lease_overview_payload()
            latest_hash = hashlib.sha256(
                json.dumps(latest, sort_keys=True, separators=(",", ":")).encode("utf-8")
            ).hexdigest()
            if latest_hash != payload_hash:
                payload_hash = latest_hash
                yield _sse_event("snapshot", latest)
            else:
                yield ": keepalive\n\n"

    return StreamingResponse(_event_gen(), media_type="text/event-stream", headers=event_headers)


def _lease_overview_payload() -> dict[str, Any]:
    _sync_broker_credentials_from_profiles()
    credentials = list_broker_credentials()
    active_leases = list_broker_leases(active_only=True)
    credential_by_id = {str(c.get("id") or ""): c for c in credentials}

    machines: dict[str, dict[str, Any]] = {}
    lease_payloads: list[dict[str, Any]] = []
    for lease in active_leases:
        machine_id = str(lease.get("machine_id") or "").strip() or "unknown"
        agent_id = str(lease.get("agent_id") or "").strip() or "unknown"
        credential_id = str(lease.get("credential_id") or "").strip()
        cred = credential_by_id.get(credential_id) or {}
        item = {
            "lease_id": lease.get("id"),
            "state": lease.get("state"),
            "machine_id": machine_id,
            "agent_id": agent_id,
            "credential_id": credential_id,
            "credential_label": cred.get("label") or credential_id,
            "credential_state": cred.get("state"),
            "latest_utilization_pct": lease.get("latest_utilization_pct"),
            "latest_quota_remaining": lease.get("latest_quota_remaining"),
            "issued_at": lease.get("issued_at"),
            "expires_at": lease.get("expires_at"),
            "last_seen_at": lease.get("last_seen_at"),
            "seconds_since_seen": lease.get("seconds_since_seen"),
            "is_stale": lease.get("is_stale"),
            "updated_at": lease.get("updated_at"),
            "reason": lease.get("reason"),
        }
        lease_payloads.append(item)
        machine = machines.setdefault(
            machine_id,
            {"machine_id": machine_id, "agent_ids": set(), "active_leases": []},
        )
        machine["agent_ids"].add(agent_id)
        machine["active_leases"].append(item)

    machine_list = []
    for machine in machines.values():
        active = machine["active_leases"]
        active.sort(key=lambda row: str(row.get("updated_at") or ""), reverse=True)
        machine_list.append(
            {
                "machine_id": machine["machine_id"],
                "agent_ids": sorted(machine["agent_ids"]),
                "active_lease_count": len(active),
                "is_stale": all(bool(lease.get("is_stale")) for lease in active) if active else False,
                "active_leases": active,
            }
        )
    machine_list.sort(key=lambda row: row["active_lease_count"], reverse=True)

    return {
        "connected_machines": machine_list,
        "active_leases": lease_payloads,
        "credentials": credentials,
        "summary": {
            "machine_count": len(machine_list),
            "active_lease_count": len(lease_payloads),
            "credential_count": len(credentials),
        },
    }


@app.post("/api/admin/leases/{lease_id}/release")
async def api_admin_release_lease(
    request: Request,
    lease_id: str,
    payload: dict[str, Any] | None = None,
) -> JSONResponse:
    _require_internal_auth(request)
    lease = get_broker_lease(lease_id)
    if lease is None:
        raise HTTPException(status_code=404, detail="Lease not found")
    result = release_broker_lease(
        lease_id=lease_id,
        machine_id=str(lease.get("machine_id") or ""),
        agent_id=str(lease.get("agent_id") or ""),
        reason=str((payload or {}).get("reason") or "admin_released_lease"),
    )
    status_code = 200 if result.get("status") == "ok" else 409
    _audit_broker_event(
        "admin_lease_released" if status_code == 200 else "admin_lease_release_denied",
        lease_id=lease_id,
        credential_id=str((lease or {}).get("credential_id") or ""),
        decision=result.get("status"),
        reason=result.get("reason") or str((payload or {}).get("reason") or "admin_released_lease"),
    )
    return JSONResponse(result, status_code=status_code)


@app.post("/api/openclaw/usage/import")
async def api_openclaw_usage_import(request: Request, payload: dict[str, Any]) -> JSONResponse:
    _require_internal_auth(request)
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="payload object is required")
    export_data = payload.get("export_json")
    if not isinstance(export_data, dict):
        export_data = payload if all(key in payload for key in ("totals", "daily")) else None
    if not isinstance(export_data, dict):
        raise HTTPException(status_code=400, detail="export_json object is required")

    result = import_openclaw_usage_export(
        export_data=export_data,
        machine_id=str(payload.get("machine_id") or "").strip() or None,
        agent_id=str(payload.get("agent_id") or "").strip() or None,
        lease_id=str(payload.get("lease_id") or "").strip() or None,
        credential_id=str(payload.get("credential_id") or "").strip() or None,
        source_name=str(payload.get("source_name") or "").strip() or None,
    )
    return JSONResponse(result)


@app.post("/api/admin/leases/{lease_id}/rotate")
async def api_admin_rotate_lease(
    request: Request,
    lease_id: str,
    payload: dict[str, Any] | None = None,
) -> JSONResponse:
    _require_internal_auth(request)
    lease = get_broker_lease(lease_id)
    if lease is None:
        raise HTTPException(status_code=404, detail="Lease not found")
    _sync_broker_credentials_from_profiles()
    reason = str((payload or {}).get("reason") or "admin_requested_rotation")
    if reason not in ROTATION_REASONS:
        reason = "admin_requested_rotation"
    result = rotate_broker_lease(
        lease_id=lease_id,
        machine_id=str(lease.get("machine_id") or ""),
        agent_id=str(lease.get("agent_id") or ""),
        reason=reason,
    )
    status_code = 200 if result.get("status") == "ok" else 409
    _audit_broker_event(
        "admin_rotation_approved" if status_code == 200 else "admin_rotation_denied",
        lease_id=lease_id,
        credential_id=str((lease or {}).get("credential_id") or ""),
        decision=result.get("status"),
        reason=result.get("reason") or reason,
    )
    return JSONResponse(result, status_code=status_code)


@app.get("/api/admin/machines/{machine_id}/detail")
async def api_admin_machine_detail(
    request: Request,
    machine_id: str,
    lease_limit: int = 200,
    telemetry_limit_per_lease: int = 30,
) -> JSONResponse:
    _require_internal_auth(request)
    _sync_broker_credentials_from_profiles()
    normalized_machine_id = machine_id.strip()
    if not normalized_machine_id:
        raise HTTPException(status_code=400, detail="machine_id is required")
    capped_lease_limit = max(1, min(int(lease_limit), 1000))
    capped_telemetry_limit = max(1, min(int(telemetry_limit_per_lease), 500))

    all_leases = list_broker_leases(active_only=False, limit=capped_lease_limit)
    machine_leases = [row for row in all_leases if str(row.get("machine_id") or "") == normalized_machine_id]
    machine_leases.sort(key=lambda row: str(row.get("updated_at") or ""), reverse=True)

    leases_payload: list[dict[str, Any]] = []
    all_telemetry: list[dict[str, Any]] = []
    for lease in machine_leases:
        lease_id = str(lease.get("id") or "")
        telemetry_rows = list_broker_lease_telemetry(lease_id)
        if capped_telemetry_limit and len(telemetry_rows) > capped_telemetry_limit:
            telemetry_rows = telemetry_rows[-capped_telemetry_limit:]
        telemetry_rows.sort(key=lambda row: str(row.get("captured_at") or ""))
        lease_payload = {
            "lease_id": lease_id,
            "state": lease.get("state"),
            "machine_id": lease.get("machine_id"),
            "agent_id": lease.get("agent_id"),
            "credential_id": lease.get("credential_id"),
            "issued_at": lease.get("issued_at"),
            "expires_at": lease.get("expires_at"),
            "updated_at": lease.get("updated_at"),
            "reason": lease.get("reason"),
            "latest_utilization_pct": lease.get("latest_utilization_pct"),
            "latest_quota_remaining": lease.get("latest_quota_remaining"),
            "telemetry_count": len(telemetry_rows),
            "telemetry": telemetry_rows,
        }
        leases_payload.append(lease_payload)
        for row in telemetry_rows:
            merged = dict(row)
            merged["lease_id"] = lease_id
            all_telemetry.append(merged)

    all_telemetry.sort(key=lambda row: str(row.get("captured_at") or ""))
    return JSONResponse(
        {
            "machine_id": normalized_machine_id,
            "summary": {
                "lease_count": len(leases_payload),
                "active_lease_count": len(
                    [row for row in leases_payload if str(row.get("state") or "") in {"active", "rotation_required"}]
                ),
                "agent_count": len({str(row.get("agent_id") or "") for row in leases_payload if row.get("agent_id")}),
                "telemetry_points": len(all_telemetry),
            },
            "leases": leases_payload,
            "telemetry": all_telemetry,
        }
    )


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
        completed = 0
        failed = 0

        semaphore = asyncio.Semaphore(max(int(settings.live_refresh_concurrency), 1))

        async def _run_profile(profile: AccountProfile) -> tuple[str, dict[str, Any], bool]:
            async with semaphore:
                account_payload, ok = await _refresh_profile_session_limits(
                    profile,
                    current_label_name=snapshot["current_label"],
                    timeout_seconds=float(settings.live_rate_limit_worker_timeout_seconds),
                )
                return profile.label, account_payload, ok

        tasks = [asyncio.create_task(_run_profile(profile)) for profile in profiles]
        try:
            for task in asyncio.as_completed(tasks):
                label, account_payload, ok = await task
                latest_by_label[label] = account_payload
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
                        "account_error",
                        {
                            "label": label,
                            "account_key": account_payload.get("account_key"),
                            "message": error_msg,
                        },
                    )
                aggregate = _compute_aggregate(list(latest_by_label.values()))
                yield _sse_event("account_update", {"account": account_payload, "ok": ok})
                yield _sse_event("aggregate_update", aggregate)
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()

        _mark_refresh_completed()
        yield _sse_event("complete", {"completed": completed, "failed": failed})

    return StreamingResponse(_event_gen(), media_type="text/event-stream", headers=event_headers)


@app.get("/api/accounts/{label}/usage-history")
async def api_account_usage_history(request: Request, label: str) -> JSONResponse:
    _require_internal_auth(request)
    profile = _profile_for_label(label)
    if profile is None:
        raise HTTPException(status_code=404, detail="Label not found")

    # Refresh usage snapshot using the same per-profile worker path as stream refresh.
    rate_info = await _fetch_session_limits_for_profile(profile)
    if isinstance(rate_info, dict) and not isinstance(rate_info.get("error"), str):
        _sync_profile_usage_from_session_limits(profile, rate_info)

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
            "account_type": _infer_account_type(profile),
            "usage_tracking": usage,
            "rollovers": rollovers,
            "summary": summary,
        }
    )


def _rollover_summary(rollovers: list[dict[str, Any]], usage: dict[str, Any] | None) -> dict[str, Any]:
    weekly_rollovers = [r for r in rollovers if _is_weekly_rollover(r)]
    total_wasted = sum(float(item.get("usage_wasted") or 0.0) for item in weekly_rollovers)
    total_used_completed = sum(float(item.get("usage_used") or 0.0) for item in weekly_rollovers)
    window_count = len(weekly_rollovers)
    
    avg_efficiency = None
    if (total_used_completed + total_wasted) > 0:
        avg_efficiency = round((total_used_completed / (total_used_completed + total_wasted)) * 100, 2)

    current_weekly_used = float((usage or {}).get("secondary_used_percent") or 0.0)
    current_weekly_remaining = max(100.0 - current_weekly_used, 0.0)

    return {
        "total_wasted_units": total_wasted,
        "total_used_units": total_used_completed,
        "window_count": window_count,
        "avg_efficiency_pct": avg_efficiency,
        "current_weekly_used": current_weekly_used,
        "current_weekly_remaining": current_weekly_remaining,
    }


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


@app.get("/api/usage/history")
async def api_usage_history(request: Request, range: str = "30d") -> JSONResponse:
    _require_internal_auth(request)
    selected_range, since_dt = _parse_history_range(range)
    range_meta = _history_range_metadata(selected_range)
    analytics_tz = _analytics_tzinfo()

    profiles = _dedupe_profiles(list_profiles())
    _touch_profiles_usage(profiles)
    cached = _build_cached_accounts_snapshot(profiles=profiles)
    account_label_by_key = {p.account_key: p.label for p in profiles}
    account_display_by_key = {
        p.account_key: _display_label(p.label, p.email) or p.label for p in profiles
    }
    account_email_by_key = {p.account_key: p.email for p in profiles}

    snapshots = list_absolute_usage_snapshots(account_id=None)
    per_account_daily = _compute_daily_consumption_per_account(
        snapshots=snapshots,
        since_dt=since_dt,
        tz=analytics_tz,
    )
    cluster_daily: dict[str, int] = {}
    for _, day_map in per_account_daily.items():
        for day, consumed in day_map.items():
            cluster_daily[day] = cluster_daily.get(day, 0) + int(consumed)
    daily_series = [{"day": day, "consumed": cluster_daily[day]} for day in sorted(cluster_daily.keys())]

    running = 0
    cumulative_series: list[dict[str, Any]] = []
    for item in daily_series:
        running += int(item["consumed"])
        cumulative_series.append(
            {
                "day": item["day"],
                "cumulative": running,
                "consumed": int(item["consumed"]),
            }
        )

    rollovers_all: list[dict[str, Any]] = []
    for profile in profiles:
        rollovers_all.extend(list_usage_rollovers(profile.account_key))
    rollovers_filtered = _filter_rollovers_by_range(rollovers_all, since_dt)
    weekly_rollovers = [row for row in rollovers_filtered if _is_weekly_rollover(row)]
    wasted_bars = _group_rollover_metric_by_day(weekly_rollovers, metric="usage_wasted")
    daily_used_bars = _group_rollover_metric_by_day(weekly_rollovers, metric="usage_used")

    consumed_by_account = {
        account_key: sum(day_map.values()) for account_key, day_map in per_account_daily.items()
    }
    top_accounts = sorted(consumed_by_account.items(), key=lambda item: item[1], reverse=True)[:10]

    stale_accounts = []
    failed_accounts = []
    for account in cached["accounts"]:
        status = account.get("refresh_status") or {}
        if status.get("is_stale"):
            stale_accounts.append(
                {
                    "account_key": account.get("account_key"),
                    "label": account.get("label"),
                    "display_label": account.get("display_label"),
                    "email": account.get("email"),
                    "last_success_at": status.get("last_success_at"),
                    "last_error": status.get("last_error"),
                }
            )
        if status.get("state") == "failed":
            failed_accounts.append(
                {
                    "account_key": account.get("account_key"),
                    "label": account.get("label"),
                    "display_label": account.get("display_label"),
                    "email": account.get("email"),
                    "last_attempt_at": status.get("last_attempt_at"),
                    "last_error": status.get("last_error"),
                }
            )

    recent_rollovers = sorted(
        weekly_rollovers,
        key=lambda row: str(row.get("rolled_over_at") or row.get("window_ended_at") or ""),
        reverse=True,
    )[:20]
    for row in recent_rollovers:
        key = str(row.get("account_id") or "")
        row["label"] = account_label_by_key.get(key, key)
        row["display_label"] = account_display_by_key.get(key, key)
        row["email"] = account_email_by_key.get(key)

    consumed_total = sum(int(item["consumed"]) for item in daily_series)
    selected_days = _selected_day_count(selected_range, daily_series)
    avg_daily = round(consumed_total / selected_days, 2) if selected_days > 0 else 0.0
    wasted_total = sum(int(item.get("usage_wasted") or 0) for item in weekly_rollovers)

    # Fallback analytics path when absolute usage counters are unavailable:
    # use stored weekly utilization percentage snapshots so stats are still informative.
    usage_snapshots = list_usage_snapshots(
        account_id=None,
        hours=_history_hours_for_range(selected_range),
    )
    modeled_per_account_daily = _compute_modeled_consumption_per_account(
        snapshots=usage_snapshots,
        since_dt=since_dt,
        tz=analytics_tz,
        metric="secondary_used_percent",
    )
    daily_weekly_utilization = _compute_daily_utilization_series(
        snapshots=usage_snapshots,
        since_dt=since_dt,
        metric="secondary_used_percent",
    )
    hourly_weekly_utilization = _compute_hourly_utilization_series(
        snapshots=usage_snapshots,
        since_dt=since_dt,
        metric="secondary_used_percent",
    )
    fallback_series = (
        hourly_weekly_utilization if selected_range == "1d" else daily_weekly_utilization
    )
    avg_weekly_utilization_in_range = (
        round(
            sum(float(item.get("value") or 0) for item in fallback_series)
            / max(len(fallback_series), 1),
            2,
        )
        if fallback_series
        else None
    )
    live_weekly_percents = [
        float((account.get("usage_tracking") or {}).get("secondary_used_percent"))
        for account in cached["accounts"]
        if (account.get("usage_tracking") or {}).get("secondary_used_percent") is not None
    ]
    weekly_utilization_now = (
        round(sum(live_weekly_percents) / max(len(live_weekly_percents), 1), 2)
        if live_weekly_percents
        else None
    )
    aggregate_summary = cached["aggregate"]
    current_total_used = int(aggregate_summary["total_current_window_used"] or 0)
    current_total_limit = int(aggregate_summary["total_current_window_limit"] or 0)
    current_total_remaining = int(aggregate_summary["total_remaining"] or 0)
    active_account_count = len(cached["accounts"])
    active_account_keys = {
        str(account.get("account_key") or "").strip()
        for account in cached["accounts"]
        if str(account.get("account_key") or "").strip()
    }
    normalized_pool_limit = active_account_count * 100 if active_account_count > 0 else 0
    normalized_pool_used = (
        round(sum(live_weekly_percents), 2)
        if live_weekly_percents
        else (round(float(weekly_utilization_now or 0) * active_account_count, 2) if active_account_count > 0 else 0.0)
    )
    normalized_pool_remaining = (
        round(max(normalized_pool_limit - normalized_pool_used, 0.0), 2)
        if normalized_pool_limit > 0
        else 0.0
    )
    absolute_account_keys = {
        str(row.get("account_id") or "").strip()
        for row in snapshots
        if str(row.get("account_id") or "").strip()
        and any(int(row.get(field) or 0) > 0 for field in ("usage_in_window", "usage_limit", "lifetime_used"))
    }
    absolute_snapshot_available = bool(active_account_keys & absolute_account_keys)
    absolute_pool_usage_available = bool(active_account_keys) and absolute_account_keys.issuperset(
        active_account_keys
    )
    absolute_usage_available = bool(
        absolute_pool_usage_available
        and (
            current_total_limit > 0
            or current_total_used > 0
            or current_total_remaining > 0
            or absolute_snapshot_available
        )
    )
    fallback_mode = (not absolute_usage_available) and bool(fallback_series)
    if fallback_mode:
        cluster_modeled_daily: dict[str, float] = {}
        for _, day_map in modeled_per_account_daily.items():
            for day, consumed in day_map.items():
                cluster_modeled_daily[day] = cluster_modeled_daily.get(day, 0.0) + float(consumed)
        daily_series = [
            {"day": day, "consumed": round(cluster_modeled_daily[day], 2)}
            for day in sorted(cluster_modeled_daily.keys())
        ]
        running_modeled = 0.0
        cumulative_series = []
        for item in daily_series:
            running_modeled += float(item["consumed"])
            cumulative_series.append(
                {
                    "day": item["day"],
                    "cumulative": round(running_modeled, 2),
                    "consumed": float(item["consumed"]),
                }
            )
        consumed_total = round(sum(float(item["consumed"]) for item in daily_series), 2) if daily_series else 0.0
        avg_daily = round(consumed_total / selected_days, 2) if selected_days > 0 else 0.0
        modeled_current_values = [
            float((account.get("usage_tracking") or {}).get("secondary_used_percent"))
            for account in cached["accounts"]
            if (account.get("usage_tracking") or {}).get("secondary_used_percent") is not None
        ]
        current_total_used_value = round(sum(modeled_current_values), 2) if modeled_current_values else None
        current_total_limit_value = float(len(modeled_current_values) * 100) if modeled_current_values else None
        current_total_remaining_value = (
            round(current_total_limit_value - current_total_used_value, 2)
            if current_total_limit_value is not None and current_total_used_value is not None
            else None
        )
        consumed_by_account = {
            account_key: round(sum(float(v) for v in day_map.values()), 2)
            for account_key, day_map in modeled_per_account_daily.items()
        }
        top_accounts = sorted(consumed_by_account.items(), key=lambda item: item[1], reverse=True)[:10]
        top_accounts_payload = [
            {
                "account_key": account_key,
                "label": account_label_by_key.get(account_key, account_key),
                "display_label": account_display_by_key.get(account_key, account_key),
                "email": account_email_by_key.get(account_key),
                "consumed": float(consumed),
            }
            for account_key, consumed in top_accounts
        ]
        top_accounts_available = True
    else:
        current_total_used_value = normalized_pool_used
        current_total_limit_value = normalized_pool_limit
        current_total_remaining_value = normalized_pool_remaining
        top_accounts_payload = [
            {
                "account_key": account_key,
                "label": account_label_by_key.get(account_key, account_key),
                "display_label": account_display_by_key.get(account_key, account_key),
                "email": account_email_by_key.get(account_key),
                "consumed": int(consumed),
            }
            for account_key, consumed in top_accounts
        ]
        top_accounts_available = True
    if fallback_mode:
        coverage_start = None
        coverage_end = None
        if fallback_series:
            first_point = fallback_series[0]
            last_point = fallback_series[-1]
            coverage_start = str(first_point.get("day") or first_point.get("t") or "")
            coverage_end = str(last_point.get("day") or last_point.get("t") or "")
    else:
        coverage_start = cumulative_series[0]["day"] if cumulative_series else None
        coverage_end = cumulative_series[-1]["day"] if cumulative_series else None

    return JSONResponse(
        {
            "range": selected_range,
            "range_metadata": range_meta,
            "summary": {
                "absolute_usage_available": absolute_usage_available,
                "total_consumed_in_range": consumed_total,
                "average_daily_consumption": avg_daily,
                "current_total_used": current_total_used_value,
                "current_total_limit": current_total_limit_value,
                "current_total_remaining": current_total_remaining_value,
                "total_wasted": wasted_total,
                "stale_account_count": len(stale_accounts),
                "failed_account_count": len(failed_accounts),
                "last_refresh_time": aggregate_summary["last_refresh_time"],
                "fallback_mode": fallback_mode,
                "fallback_reason": (
                    "absolute_usage_unavailable_using_weekly_utilization"
                    if fallback_mode
                    else None
                ),
                "last_refresh_label": (
                    "Last snapshot refresh" if fallback_mode else "Last absolute usage refresh"
                ),
                "modeled_usage_basis": (
                    "normalized_100_units_from_utilization_snapshots"
                    if fallback_mode
                    else None
                ),
                "weekly_utilization_now": weekly_utilization_now,
                "average_weekly_utilization_in_range": avg_weekly_utilization_in_range,
            },
            "series": {
                "cumulative_usage": cumulative_series,
                "daily_usage": daily_series,
                "daily_rollover_wasted": wasted_bars,
                "daily_rollover_used": daily_used_bars,
                "daily_weekly_utilization": daily_weekly_utilization,
                "hourly_weekly_utilization": hourly_weekly_utilization,
            },
            "sections": {
                "top_consuming_accounts": top_accounts_payload,
                "top_consuming_accounts_available": top_accounts_available,
                "stale_accounts": stale_accounts,
                "failed_accounts": failed_accounts,
                "recent_rollovers": recent_rollovers,
            },
            "freshness": {
                "coverage_start": coverage_start,
                "coverage_end": coverage_end,
                "snapshot_points": len(usage_snapshots) if fallback_mode else len(snapshots),
                "daily_points": len(fallback_series) if fallback_mode else len(daily_series),
                "is_sparse": (len(fallback_series) if fallback_mode else len(daily_series)) < 3,
            },
        }
    )


@app.get("/api/openclaw/usage/by-credential")
async def api_openclaw_usage_by_credential(request: Request, range: str = "30d") -> JSONResponse:
    _require_internal_auth(request)
    selected_range, since_dt = _parse_history_range(range)
    since_date = since_dt.date().isoformat() if since_dt is not None else None

    rows = list_openclaw_usage_by_credential(since_date=since_date)
    broker_credentials = {
        str(row.get("id") or ""): row for row in list_broker_credentials()
    }
    profiles = _dedupe_profiles(list_profiles())
    profile_by_account_key = {p.account_key: p for p in profiles}

    payload_rows: list[dict[str, Any]] = []
    for row in rows:
        credential_id = str(row.get("credential_id") or "").strip()
        broker_row = broker_credentials.get(credential_id) or {}
        profile = profile_by_account_key.get(credential_id)
        label = (
            str(broker_row.get("label") or "").strip()
            or (profile.label if profile else "")
            or credential_id
        )
        display_label = _display_label(label, profile.email if profile else None) if label else credential_id
        payload_rows.append(
            {
                "credential_id": credential_id,
                "lease_id": row.get("lease_id"),
                "label": label,
                "display_label": display_label,
                "email": profile.email if profile else None,
                "input_tokens": int(row.get("input_tokens") or 0),
                "output_tokens": int(row.get("output_tokens") or 0),
                "cache_read_tokens": int(row.get("cache_read_tokens") or 0),
                "cache_write_tokens": int(row.get("cache_write_tokens") or 0),
                "total_tokens": int(row.get("total_tokens") or 0),
                "total_cost": row.get("total_cost"),
                "day_count": int(row.get("day_count") or 0),
                "machine_count": int(row.get("machine_count") or 0),
                "agent_count": int(row.get("agent_count") or 0),
                "last_updated_at": row.get("last_updated_at"),
            }
        )

    totals = {
        "input_tokens": sum(int(row["input_tokens"]) for row in payload_rows),
        "output_tokens": sum(int(row["output_tokens"]) for row in payload_rows),
        "cache_read_tokens": sum(int(row["cache_read_tokens"]) for row in payload_rows),
        "cache_write_tokens": sum(int(row["cache_write_tokens"]) for row in payload_rows),
        "total_tokens": sum(int(row["total_tokens"]) for row in payload_rows),
        "total_cost": sum(float(row["total_cost"] or 0) for row in payload_rows) if payload_rows else 0,
        "credential_count": len(payload_rows),
    }

    return JSONResponse(
        {
            "range": selected_range,
            "range_metadata": _history_range_metadata(selected_range),
            "totals": totals,
            "rows": payload_rows,
        }
    )


@app.get("/api/app/version")
async def api_app_version(request: Request) -> JSONResponse:
    _require_internal_auth(request)
    now = datetime.now(timezone.utc)
    checked_at = _VERSION_CHECK_CACHE.get("checked_at")
    cached_payload = _VERSION_CHECK_CACHE.get("payload")
    if (
        isinstance(checked_at, datetime)
        and cached_payload is not None
        and (now - checked_at).total_seconds() < _VERSION_CHECK_CACHE_TTL_SECONDS
    ):
        return JSONResponse(cached_payload)

    latest_tag: str | None = None
    latest_name: str | None = None
    latest_url: str | None = None
    update_available = False
    error: str | None = None

    try:
        async with asyncio.timeout(3.0):
            async with httpx.AsyncClient(timeout=3.0, follow_redirects=True, trust_env=False) as client:
                response = await client.get(
                    f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest",
                    headers={
                        "Accept": "application/vnd.github+json",
                        "X-GitHub-Api-Version": "2022-11-28",
                        "User-Agent": "codex-auth-manager-version-check",
                    },
                )
                response.raise_for_status()
                payload = response.json()
        latest_tag = str(payload.get("tag_name") or "").strip() or None
        latest_name = str(payload.get("name") or "").strip() or None
        latest_url = str(payload.get("html_url") or "").strip() or None
        if latest_tag:
            current_key = APP_VERSION.lstrip("vV")
            latest_key = latest_tag.lstrip("vV")
            update_available = latest_key != current_key
    except Exception as exc:
        logger.warning("version_check_failed %s", exc)
        error = str(exc)

    response_payload = {
        "current_version": APP_VERSION,
        "repo": GITHUB_REPO,
        "latest_version": latest_tag,
        "latest_name": latest_name,
        "latest_url": latest_url,
        "update_available": update_available,
        "error": error,
    }
    _VERSION_CHECK_CACHE["checked_at"] = now
    _VERSION_CHECK_CACHE["payload"] = response_payload
    return JSONResponse(response_payload)


@app.get("/api/accounts/{label}/history")
async def api_account_history(request: Request, label: str, range: str = "30d") -> JSONResponse:
    _require_internal_auth(request)
    profile = _profile_for_label(label)
    if profile is None:
        raise HTTPException(status_code=404, detail="Label not found")

    selected_range, since_dt = _parse_history_range(range)
    range_meta = _history_range_metadata(selected_range)
    analytics_tz = _analytics_tzinfo()
    _touch_account_usage(profile=profile)
    usage = _usage_tracking_payload(profile.account_key) or {}
    rollovers_all = list_usage_rollovers(profile.account_key)
    rollovers_filtered = _filter_rollovers_by_range(rollovers_all, since_dt)
    weekly_rollovers = [row for row in rollovers_filtered if _is_weekly_rollover(row)]
    absolute_snapshots = list_absolute_usage_snapshots(account_id=profile.account_key)
    per_account_daily = _compute_daily_consumption_per_account(
        snapshots=absolute_snapshots,
        since_dt=since_dt,
        tz=analytics_tz,
    )
    day_map = per_account_daily.get(profile.account_key, {})
    daily_series = [{"day": day, "consumed": day_map[day]} for day in sorted(day_map.keys())]

    running = 0
    cumulative_series: list[dict[str, Any]] = []
    for item in daily_series:
        running += int(item["consumed"])
        cumulative_series.append(
            {"day": item["day"], "cumulative": running, "consumed": int(item["consumed"])}
        )

    status = _refresh_status_payload(profile.account_key, usage)
    current_used = int(usage.get("usage_in_window") or 0)
    current_limit = int(usage.get("usage_limit") or 0)
    remaining = max(current_limit - current_used, 0)
    utilization = round((current_used / current_limit) * 100, 2) if current_limit > 0 else None
    wasted_total = sum(int(item.get("usage_wasted") or 0) for item in weekly_rollovers)
    selected_days = _selected_day_count(selected_range, daily_series)
    total_consumed_in_range = sum(int(item["consumed"]) for item in daily_series)
    avg_daily = round((total_consumed_in_range / selected_days), 2) if selected_days > 0 else 0.0

    usage_snapshots = list_usage_snapshots(
        account_id=profile.account_key,
        hours=_history_hours_for_range(selected_range),
    )
    modeled_day_map = _compute_modeled_consumption_per_account(
        snapshots=usage_snapshots,
        since_dt=since_dt,
        tz=analytics_tz,
        metric="secondary_used_percent",
    ).get(profile.account_key, {})
    daily_weekly_utilization = _compute_daily_utilization_series(
        snapshots=usage_snapshots,
        since_dt=since_dt,
        metric="secondary_used_percent",
    )
    hourly_weekly_utilization = _compute_hourly_utilization_series(
        snapshots=usage_snapshots,
        since_dt=since_dt,
        metric="secondary_used_percent",
    )
    fallback_series = (
        hourly_weekly_utilization if selected_range == "1d" else daily_weekly_utilization
    )
    absolute_snapshot_available = any(
        any(int(row.get(field) or 0) > 0 for field in ("usage_in_window", "usage_limit", "lifetime_used"))
        for row in absolute_snapshots
    )
    absolute_usage_available = bool(
        current_limit > 0
        or current_used > 0
        or remaining > 0
        or absolute_snapshot_available
    )
    fallback_mode = (not absolute_usage_available) and bool(fallback_series)
    stale_account_count = 1 if status.get("is_stale") else 0
    failed_account_count = 1 if status.get("state") == "failed" else 0
    if fallback_mode:
        daily_series = [
            {"day": day, "consumed": round(float(modeled_day_map[day]), 2)}
            for day in sorted(modeled_day_map.keys())
        ]
        running_modeled = 0.0
        cumulative_series = []
        for item in daily_series:
            running_modeled += float(item["consumed"])
            cumulative_series.append(
                {
                    "day": item["day"],
                    "cumulative": round(running_modeled, 2),
                    "consumed": float(item["consumed"]),
                }
            )
        total_consumed_in_range = (
            round(sum(float(item["consumed"]) for item in daily_series), 2) if daily_series else 0.0
        )
        avg_daily = round((total_consumed_in_range / selected_days), 2) if selected_days > 0 else 0.0
        current_used_value = _current_modeled_usage_from_percent(usage.get("secondary_used_percent"))
        current_limit_value = 100.0 if current_used_value is not None else None
        remaining_value = (
            round(max(current_limit_value - current_used_value, 0.0), 2)
            if current_limit_value is not None and current_used_value is not None
            else None
        )
        utilization_value = current_used_value
        coverage_start = str(fallback_series[0].get("day") or fallback_series[0].get("t") or "") if fallback_series else None
        coverage_end = str(fallback_series[-1].get("day") or fallback_series[-1].get("t") or "") if fallback_series else None
        last_refresh_time = _latest_captured_at(usage_snapshots) or status.get("last_success_at") or usage.get("last_usage_sync_at") or usage.get("updated_at")
        last_refresh_label = "Last snapshot refresh"
    else:
        current_used_value = current_used
        current_limit_value = current_limit
        remaining_value = remaining
        utilization_value = utilization
        coverage_start = cumulative_series[0]["day"] if cumulative_series else None
        coverage_end = cumulative_series[-1]["day"] if cumulative_series else None
        last_refresh_time = _latest_captured_at(absolute_snapshots) or usage.get("last_usage_sync_at") or usage.get("updated_at")
        last_refresh_label = "Last absolute usage refresh"

    completed_windows = sorted(
        weekly_rollovers,
        key=lambda row: str(row.get("window_ended_at") or ""),
        reverse=True,
    )
    for row in completed_windows:
        used = int(row.get("usage_used") or 0)
        limit = int(row.get("usage_limit") or 0)
        row["utilization_percent"] = round((used / limit) * 100, 2) if limit > 0 else None

    sec_used = float(usage.get("secondary_used_percent") or 0.0)
    if fallback_mode:
        efficiency = None
    elif total_consumed_in_range is None:
        efficiency = None
    else:
        efficiency = (
            round((total_consumed_in_range / (total_consumed_in_range + wasted_total)) * 100, 2)
            if (total_consumed_in_range + wasted_total) > 0
            else 100.0
        )

    return JSONResponse(
        {
            "label": profile.label,
            "account_key": profile.account_key,
            "display_label": _display_label(profile.label, profile.email),
            "email": profile.email,
            "account_type": _infer_account_type(profile),
            "range": selected_range,
            "range_metadata": range_meta,
            "summary": {
                "absolute_usage_available": absolute_usage_available,
                "total_consumed_in_range": total_consumed_in_range,
                "average_daily_consumption": avg_daily,
                "current_total_used": current_used_value,
                "current_total_limit": current_limit_value,
                "current_total_remaining": remaining_value,
                "total_wasted": wasted_total,
                "stale_account_count": stale_account_count,
                "failed_account_count": failed_account_count,
                "last_refresh_time": last_refresh_time,
                "last_refresh_label": last_refresh_label,
                "fallback_mode": fallback_mode,
                "fallback_reason": (
                    "absolute_usage_unavailable_using_weekly_utilization"
                    if fallback_mode
                    else None
                ),
                "modeled_usage_basis": (
                    "normalized_100_units_from_utilization_snapshots"
                    if fallback_mode
                    else None
                ),
                "weekly_utilization_now": sec_used if usage.get("secondary_used_percent") is not None else None,
                "average_weekly_utilization_in_range": (
                    round(
                        sum(float(item.get("value") or 0) for item in fallback_series)
                        / max(len(fallback_series), 1),
                        2,
                    )
                    if fallback_series
                    else None
                ),
            },
            "current_state": {
                "absolute_usage_available": absolute_usage_available,
                "usage_in_window": current_used_value,
                "usage_limit": current_limit_value,
                "remaining": remaining_value,
                "utilization_percent": utilization_value,
                "weekly_used_units": sec_used,
                "weekly_remaining_units": max(100.0 - sec_used, 0.0),
                "next_reset": usage.get("primary_resets_at") or usage.get("rate_limit_refresh_at"),
                "lifetime_used": int(usage.get("lifetime_used") or 0) if absolute_usage_available else None,
                "last_sync": last_refresh_time,
                "refresh_status": status,
                "efficiency_pct": efficiency,
            },
            "consumption_trend": {
                "cumulative_usage": cumulative_series,
                "daily_usage": daily_series,
                "total_consumed_in_range": total_consumed_in_range,
                "average_daily_consumption": avg_daily,
                "absolute_usage_available": absolute_usage_available,
                "fallback_mode": fallback_mode,
                "modeled_usage_basis": (
                    "normalized_100_units_from_utilization_snapshots"
                    if fallback_mode
                    else None
                ),
                "daily_weekly_utilization": daily_weekly_utilization,
                "hourly_weekly_utilization": hourly_weekly_utilization,
            },
            "completed_windows": [
                {
                    "window_start": row.get("window_started_at"),
                    "window_end": row.get("window_ended_at"),
                    "used": int(row.get("usage_used") or 0),
                    "limit": int(row.get("usage_limit") or 0),
                    "wasted": int(row.get("usage_wasted") or 0),
                    "utilization_percent": row.get("utilization_percent"),
                    "rolled_over_at": row.get("rolled_over_at"),
                    "primary_percent_at_reset": row.get("primary_percent_at_reset"),
                    "secondary_percent_at_reset": row.get("secondary_percent_at_reset"),
                }
                for row in completed_windows
            ],
            "wastage_series": {
                "daily_wasted": _group_rollover_metric_by_day(weekly_rollovers, metric="usage_wasted"),
                "daily_used": _group_rollover_metric_by_day(weekly_rollovers, metric="usage_used"),
                "total_wasted": wasted_total,
            },
            "freshness": {
                "coverage_start": coverage_start,
                "coverage_end": coverage_end,
                "snapshot_points": len(usage_snapshots) if fallback_mode else len(absolute_snapshots),
                "daily_points": len(fallback_series) if fallback_mode else len(daily_series),
                "is_sparse": (len(fallback_series) if fallback_mode else len(daily_series)) < 3,
            },
        }
    )


@app.get("/api/usage/snapshots")
async def api_usage_snapshots(request: Request) -> JSONResponse:
    """Return cluster-wide hourly utilization trend from percentage snapshots."""
    _require_internal_auth(request)
    snapshots = list_usage_snapshots(account_id=None, hours=168)  # 7 days

    # Bucket by hour across all accounts
    hourly: dict[str, dict[str, Any]] = {}
    for snap in snapshots:
        ts = str(snap.get("captured_at") or "")[:13]  # YYYY-MM-DDTHH
        if len(ts) < 13:
            continue
        bucket = hourly.setdefault(ts, {"p1_sum": 0, "p2_sum": 0, "count": 0})
        bucket["p1_sum"] += float(snap.get("primary_used_percent") or 0)
        bucket["p2_sum"] += float(snap.get("secondary_used_percent") or 0)
        bucket["count"] += 1

    points = []
    for hour_key in sorted(hourly.keys()):
        b = hourly[hour_key]
        n = max(b["count"], 1)
        points.append({
            "t": hour_key + ":00:00",
            "avg_primary_pct": round(b["p1_sum"] / n, 1),
            "avg_secondary_pct": round(b["p2_sum"] / n, 1),
            "samples": b["count"],
        })

    return JSONResponse({"trend": points})


@app.get("/api/accounts/{label}/snapshots")
async def api_account_snapshots(request: Request, label: str) -> JSONResponse:
    """Return per-account hourly utilization trend from percentage snapshots."""
    _require_internal_auth(request)
    profile = _profile_for_label(label)
    if profile is None:
        raise HTTPException(status_code=404, detail="Label not found")

    snapshots = list_usage_snapshots(account_id=profile.account_key, hours=168)

    hourly: dict[str, dict[str, Any]] = {}
    for snap in snapshots:
        ts = str(snap.get("captured_at") or "")[:13]
        if len(ts) < 13:
            continue
        bucket = hourly.setdefault(ts, {"p1_sum": 0, "p2_sum": 0, "count": 0})
        bucket["p1_sum"] += float(snap.get("primary_used_percent") or 0)
        bucket["p2_sum"] += float(snap.get("secondary_used_percent") or 0)
        bucket["count"] += 1

    points = []
    for hour_key in sorted(hourly.keys()):
        b = hourly[hour_key]
        n = max(b["count"], 1)
        points.append({
            "t": hour_key + ":00:00",
            "avg_primary_pct": round(b["p1_sum"] / n, 1),
            "avg_secondary_pct": round(b["p2_sum"] / n, 1),
            "samples": b["count"],
        })

    # Also include rollovers with percentage data
    rollovers = list_usage_rollovers(profile.account_key)

    return JSONResponse({
        "label": profile.label,
        "trend": points,
        "rollovers": rollovers,
    })


@app.get("/api/public-stats")
async def api_public_stats() -> JSONResponse:
    profiles = _dedupe_profiles(list_profiles())
    auth_meta = _auth_file_metadata()
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


@app.get("/api/session/status")
async def api_session_status(request: Request) -> JSONResponse:
    return JSONResponse(
        {
            "web_login_enabled": _web_login_enabled(),
            "session_valid": _has_valid_web_session(request),
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
    except (AuthStoreError, AuthStoreSwitchError) as exc:
        raise _to_switch_http_error(exc) from exc


def _to_switch_http_error(exc: Exception) -> HTTPException:
    if isinstance(exc, AuthStoreSwitchError):
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


def _parse_callback_payload(full_url: str) -> dict[str, str]:
    if not full_url:
        return {}
    try:
        parsed = urlparse(full_url)
        params = parse_qs(parsed.query, keep_blank_values=True)
    except Exception:
        return {}
    extracted: dict[str, str] = {}
    for key in ("code", "state", "error", "error_description"):
        value = params.get(key, [None])[0]
        if value is not None:
            extracted[key] = str(value)
    return extracted


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


def _auth_access_token_expiring_soon(
    auth_json: dict[str, Any],
    *,
    leeway_seconds: int = _AUTH_REFRESH_LEEWAY_SECONDS,
) -> bool:
    claims = decode_jwt_claims(extract_access_token(auth_json))
    if not claims:
        return False
    exp = claims.get("exp")
    if not isinstance(exp, (int, float)):
        return False
    now_ts = int(datetime.now(timezone.utc).timestamp())
    return exp <= now_ts + max(int(leeway_seconds), 0)


async def _refresh_saved_auth_via_refresh_token(profile: AccountProfile) -> bool:
    auth_json = profile.auth if isinstance(profile.auth, dict) else {}
    refresh_token = extract_refresh_token(auth_json)
    if not refresh_token or not settings.openai_token_url or not settings.openai_client_id:
        return False

    data: dict[str, str] = {
        "grant_type": "refresh_token",
        "client_id": settings.openai_client_id,
        "refresh_token": refresh_token,
    }
    if settings.openai_client_secret:
        data["client_secret"] = settings.openai_client_secret

    async with httpx.AsyncClient(timeout=20) as client:
        response = await client.post(
            settings.openai_token_url,
            data=data,
            headers={"Accept": "application/json"},
        )

    if response.status_code >= 400:
        raise RuntimeError(response.text.strip() or "Refresh token exchange failed")

    try:
        token_response = response.json()
    except ValueError as exc:
        raise RuntimeError("Invalid refresh token response") from exc

    if not isinstance(token_response, dict):
        raise RuntimeError("Unexpected refresh token response format")

    next_auth = build_auth_payload(token_response, existing_auth=auth_json)
    identity = extract_account_identity(next_auth)
    upsert_saved_profile(
        label=profile.label,
        account_key=identity.account_key or profile.account_key,
        auth_json=next_auth,
        email=identity.email or profile.email,
        name=identity.name or profile.name,
        subject=identity.subject or profile.subject,
        user_id=identity.user_id or profile.user_id,
        provider_account_id=identity.account_id or profile.provider_account_id,
        reauth_required=False,
        reauth_reason=None,
    )
    return True


async def _refresh_active_auth_via_refresh_token(auth_json: dict[str, Any]) -> bool:
    refresh_token = extract_refresh_token(auth_json)
    if not refresh_token or not settings.openai_token_url or not settings.openai_client_id:
        return False

    data: dict[str, str] = {
        "grant_type": "refresh_token",
        "client_id": settings.openai_client_id,
        "refresh_token": refresh_token,
    }
    if settings.openai_client_secret:
        data["client_secret"] = settings.openai_client_secret

    async with httpx.AsyncClient(timeout=20) as client:
        response = await client.post(
            settings.openai_token_url,
            data=data,
            headers={"Accept": "application/json"},
        )

    if response.status_code >= 400:
        raise RuntimeError(response.text.strip() or "Refresh token exchange failed")

    try:
        token_response = response.json()
    except ValueError as exc:
        raise RuntimeError("Invalid refresh token response") from exc

    if not isinstance(token_response, dict):
        raise RuntimeError("Unexpected refresh token response format")

    next_auth = build_auth_payload(token_response, existing_auth=auth_json)
    old_access = extract_access_token(auth_json) or ""
    new_access = extract_access_token(next_auth) or ""
    if not new_access:
        return False
    if new_access == old_access and next_auth.get("last_refresh") == auth_json.get("last_refresh"):
        return False

    set_active_auth_json(next_auth)
    return True


def _refresh_keepalive_supported() -> bool:
    return bool(settings.openai_token_url and settings.openai_client_id)


async def _refresh_active_auth_if_needed(auth_json: dict[str, Any]) -> bool:
    if not _auth_access_token_expiring_soon(auth_json, leeway_seconds=0):
        return False
    return await _refresh_active_auth_via_refresh_token(auth_json)


async def _refresh_saved_auths_if_needed() -> int:
    refreshed = 0
    for profile in _dedupe_profiles(list_profiles()):
        auth_json = profile.auth if isinstance(profile.auth, dict) else {}
        if not auth_json or not extract_refresh_token(auth_json):
            continue
        refresh_status = _refresh_status_payload(
            profile.account_key,
            _usage_tracking_payload(profile.account_key),
        )
        if refresh_status.get("reauth_required") or profile.reauth_required:
            continue
        if not _auth_access_token_expiring_soon(auth_json):
            continue
        try:
            ok = await _refresh_saved_auth_via_refresh_token(profile)
            if ok:
                refreshed += 1
                update_saved_profile_reauth_status(
                    profile.label,
                    reauth_required=False,
                    reauth_reason=None,
                )
                if profile.account_key:
                    with suppress(Exception):
                        set_broker_credential_assignment_disabled(profile.account_key, disabled=False)
        except Exception as exc:
            logger.warning("auth keepalive refresh failed for %s: %s", profile.label, exc)
            error_text = str(exc)
            if _refresh_error_requires_reauth(error_text):
                update_saved_profile_reauth_status(
                    profile.label,
                    reauth_required=True,
                    reauth_reason=error_text,
                )
            if profile.account_key and _refresh_error_requires_reauth(error_text):
                with suppress(Exception):
                    set_broker_credential_assignment_disabled(profile.account_key, disabled=True)
    return refreshed


def _require_internal_auth(request: Request) -> None:
    if _web_login_enabled() and _has_valid_web_session(request):
        return
    configured_token = (settings.internal_api_token or "").strip()
    if not configured_token:
        raise HTTPException(
            status_code=503,
            detail="API key is required for this action, but INTERNAL_API_TOKEN is not configured on the server.",
        )

    if _has_valid_internal_api_token(request):
        return
    if request.headers.get("authorization", "").lower().startswith("bearer "):
        _log_invalid_internal_auth(request, header_type="authorization")
        raise HTTPException(status_code=403, detail="Invalid API key")
    if request.headers.get("x-api-key", "").strip():
        _log_invalid_internal_auth(request, header_type="x-api-key")
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


def _token_fingerprint(value: str) -> str | None:
    token = (value or "").strip()
    if not token:
        return None
    return hashlib.sha256(token.encode("utf-8")).hexdigest()[:12]


def _log_invalid_internal_auth(request: Request, *, header_type: str) -> None:
    presented = ""
    if header_type == "authorization":
        auth_header = request.headers.get("authorization", "")
        if auth_header.lower().startswith("bearer "):
            presented = auth_header.split(" ", 1)[1].strip()
    elif header_type == "x-api-key":
        presented = request.headers.get("x-api-key", "").strip()

    configured = (settings.internal_api_token or "").strip()
    logger.warning(
        "invalid_internal_auth path=%s header_type=%s presented_len=%s presented_fp=%s configured_len=%s configured_fp=%s client=%s",
        request.url.path,
        header_type,
        len(presented) if presented else 0,
        _token_fingerprint(presented),
        len(configured) if configured else 0,
        _token_fingerprint(configured),
        request.client.host if request.client else None,
    )


def _require_internal_auth_or_query(request: Request) -> None:
    if _web_login_enabled() and _has_valid_web_session(request):
        return
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
    active_leases_by_credential = list_active_broker_leases_by_credential()
    current = None
    try:
        current = _resolve_current_label(read_current_auth(), profs)
    except CodexCLIError:
        current = None

    accounts = [
        _account_payload(
            profile,
            current,
            usage_tracking=_usage_tracking_payload(profile.account_key),
            active_lease=active_leases_by_credential.get(profile.account_key),
        )
        for profile in profs
    ]
    aggregate = _compute_aggregate(accounts)
    return {"accounts": accounts, "current_label": current, "aggregate": aggregate}


def _infer_account_type(profile: AccountProfile) -> str:
    explicit_plan = (profile.plan_type or "").strip().lower()
    if explicit_plan == "plus":
        return "ChatGPT Plus"
    if explicit_plan == "team":
        return "ChatGPT Business"

    n = (profile.name or "").lower()
    p = (profile.provider_account_id or "").lower()
    if "business" in n or "team" in n or p.startswith("org-"):
        return "ChatGPT Business"
    return "ChatGPT Plus"


def _account_payload(
    profile: AccountProfile,
    current_label_name: str | None,
    *,
    usage_tracking: dict[str, Any] | None,
    active_lease: dict[str, Any] | None = None,
) -> dict[str, Any]:
    refresh_status = _refresh_status_payload(profile.account_key, usage_tracking)
    reauth_required, reauth_reason = _profile_reauth_requirement(profile, refresh_status)
    refresh_status["reauth_required"] = reauth_required
    if reauth_required and not refresh_status.get("last_error"):
        refresh_status["last_error"] = reauth_reason
        if refresh_status.get("state") == "idle":
            refresh_status["state"] = "failed"
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
        "account_type": _infer_account_type(profile),
        "rate_limits": rate_limits,
        "usage_tracking": usage_tracking,
        "token_metadata": _saved_profile_token_metadata(profile),
        "decoded_tokens": _decoded_token_payload(profile.auth),
        "refresh_status": refresh_status,
        "active_lease": (
            {
                "lease_id": active_lease.get("id"),
                "machine_id": active_lease.get("machine_id"),
                "agent_id": active_lease.get("agent_id"),
                "state": active_lease.get("state"),
                "issued_at": active_lease.get("issued_at"),
                "expires_at": active_lease.get("expires_at"),
            }
            if active_lease
            else None
        ),
    }


def _compute_aggregate(accounts: list[dict[str, Any]]) -> dict[str, Any]:
    total_used = 0
    total_limit = 0
    lifetime_used = 0
    total_wasted = 0
    stale_count = 0
    failed_count = 0
    last_refresh: str | None = None

    fleet_capacity_units = len(accounts) * 100
    fleet_used_units = 0.0

    for account in accounts:
        usage = account.get("usage_tracking") or {}
        total_used += int(usage.get("usage_in_window") or 0)
        total_limit += int(usage.get("usage_limit") or 0)
        lifetime_used += int(usage.get("lifetime_used") or 0)
        
        sec_limits = account.get("rate_limits", {}).get("secondary")
        if isinstance(sec_limits, dict):
            fleet_used_units += float(sec_limits.get("percent") or 0.0)
        
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
        total_wasted += sum(int(item.get("usage_wasted") or 0) for item in rollovers if _is_weekly_rollover(item))

    remaining = max(total_limit - total_used, 0)
    utilization = round((total_used / total_limit) * 100, 2) if total_limit > 0 else 0.0
    
    fleet_utilization_pct = round((fleet_used_units / fleet_capacity_units) * 100, 2) if fleet_capacity_units > 0 else 0.0
    fleet_efficiency_pct = round((fleet_used_units / (fleet_used_units + total_wasted)) * 100, 2) if (fleet_used_units + total_wasted) > 0 else 100.0

    return {
        "accounts": len(accounts),
        "fleet_capacity_units": fleet_capacity_units,
        "fleet_used_units": round(fleet_used_units, 2),
        "fleet_remaining_units": round(max(fleet_capacity_units - fleet_used_units, 0), 2),
        "fleet_utilization_pct": fleet_utilization_pct,
        "fleet_efficiency_pct": fleet_efficiency_pct,
        "total_current_window_used": total_used,
        "total_current_window_limit": total_limit,
        "total_remaining": remaining,
        "aggregate_utilization_percent": utilization,
        "lifetime_total_used": lifetime_used,
        "total_wasted_units": total_wasted,
        "stale_accounts": stale_count,
        "failed_accounts": failed_count,
        "last_refresh_time": _LAST_REFRESH_COMPLETED_AT or last_refresh,
    }


def _rank_account_for_weekly_availability(account: dict[str, Any]) -> dict[str, Any]:
    label = str(account.get("label") or "")
    weekly = _extract_weekly_metrics(account)
    remaining = weekly["remaining_percent"]
    seconds_to_reset = weekly["seconds_to_reset"]
    # Weight remaining weekly capacity highest; use reset timing as secondary factor.
    reset_bonus = 0.0
    if seconds_to_reset is not None:
        week = 7 * 24 * 60 * 60
        clamped = max(0.0, min(float(week), float(seconds_to_reset)))
        reset_bonus = (1.0 - (clamped / float(week))) * 20.0
    score = float(remaining) + reset_bonus

    return {
        "label": label,
        "account_key": account.get("account_key"),
        "display_label": account.get("display_label"),
        "email": account.get("email"),
        "is_current": bool(account.get("is_current")),
        "weekly_used_percent": round(float(weekly["used_percent"]), 2),
        "weekly_remaining_percent": round(float(remaining), 2),
        "weekly_resets_at": weekly["resets_at"],
        "seconds_to_weekly_reset": seconds_to_reset,
        "score": round(score, 4),
    }


def _extract_weekly_metrics(account: dict[str, Any]) -> dict[str, Any]:
    rate_limits = account.get("rate_limits") if isinstance(account, dict) else {}
    usage_tracking = account.get("usage_tracking") if isinstance(account, dict) else {}
    secondary = None
    if isinstance(rate_limits, dict):
        secondary = rate_limits.get("secondary") or rate_limits.get("tokens")
    used_percent = None
    resets_at = None
    if isinstance(secondary, dict):
        pct = secondary.get("percent")
        if isinstance(pct, (int, float)):
            used_percent = float(pct)
        raw_reset = (
            secondary.get("resetsAt")
            or secondary.get("resetAt")
            or secondary.get("nextResetAt")
            or secondary.get("reset")
        )
        parsed = _parse_maybe_datetime(raw_reset)
        if parsed is not None:
            resets_at = parsed.isoformat()
    if used_percent is None and isinstance(usage_tracking, dict):
        cached_pct = usage_tracking.get("secondary_used_percent")
        if isinstance(cached_pct, (int, float)):
            used_percent = float(cached_pct)
    if resets_at is None and isinstance(usage_tracking, dict):
        parsed = _parse_maybe_datetime(usage_tracking.get("secondary_resets_at"))
        if parsed is not None:
            resets_at = parsed.isoformat()

    if used_percent is None:
        used_percent = 100.0
    used_percent = max(0.0, min(100.0, float(used_percent)))
    remaining_percent = max(0.0, 100.0 - used_percent)
    seconds_to_reset = None
    parsed_reset = _parse_maybe_datetime(resets_at)
    if parsed_reset is not None:
        seconds_to_reset = max(0, int((parsed_reset - datetime.now(timezone.utc)).total_seconds()))

    return {
        "used_percent": used_percent,
        "remaining_percent": remaining_percent,
        "resets_at": resets_at,
        "seconds_to_reset": seconds_to_reset,
    }


def _parse_maybe_datetime(raw: Any) -> datetime | None:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        dt = raw
    elif isinstance(raw, (int, float)):
        if int(raw) <= 0:
            return None
        dt = datetime.fromtimestamp(int(raw), tz=timezone.utc)
    elif isinstance(raw, str):
        text = raw.strip()
        if not text:
            return None
        if text.isdigit():
            dt = datetime.fromtimestamp(int(text), tz=timezone.utc)
        else:
            try:
                dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
            except ValueError:
                return None
    else:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt


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
    status.setdefault("reauth_required", False)

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
        entry["reauth_required"] = False
    else:
        entry["last_error"] = error
        entry["reauth_required"] = _refresh_error_requires_reauth(error)


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


def _set_web_session_cookie(request: Request, response: Response) -> None:
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


async def _refresh_profile_session_limits(
    profile: AccountProfile,
    *,
    current_label_name: str | None,
    timeout_seconds: float | None = None,
) -> tuple[dict[str, Any], bool]:
    usage = _usage_tracking_payload(profile.account_key)
    refresh_status = _refresh_status_payload(profile.account_key, usage)
    reauth_required, reauth_reason = _profile_reauth_requirement(profile, refresh_status)
    if reauth_required:
        error_msg = str(reauth_reason or "Reauthentication required")
        _mark_refresh_status(profile.account_key, ok=False, error=error_msg)
        if profile.account_key:
            with suppress(Exception):
                set_broker_credential_assignment_disabled(profile.account_key, disabled=True)
        account_payload = _account_payload(
            profile,
            current_label_name,
            usage_tracking=usage,
        )
        account_payload["rate_limits"] = {"error": error_msg}
        return account_payload, False

    try:
        rate_info = await _fetch_session_limits_for_profile(
            profile,
            timeout_seconds=timeout_seconds,
        )
        account_data = None
        if isinstance(rate_info, dict):
            raw_account = rate_info.get("_account")
            account_data = raw_account if isinstance(raw_account, dict) else None
            rate_info = {key: value for key, value in rate_info.items() if key != "_account"}
        _sync_profile_usage_from_session_limits(profile, rate_info)
        usage = _usage_tracking_payload(profile.account_key)
        account_payload = _account_payload(
            profile,
            current_label_name,
            usage_tracking=usage,
        )
        account_payload["rate_limits"] = rate_info
        if account_data is not None:
            account_payload["provider_account"] = account_data
        _mark_refresh_status(profile.account_key, ok=True, error=None)
        if profile.account_key:
            with suppress(Exception):
                set_broker_credential_assignment_disabled(profile.account_key, disabled=False)
        return account_payload, True
    except CodexCLIError as exc:
        error_msg = str(exc)
        _mark_refresh_status(profile.account_key, ok=False, error=error_msg)
        if profile.account_key and _refresh_error_requires_reauth(error_msg):
            with suppress(Exception):
                set_broker_credential_assignment_disabled(profile.account_key, disabled=True)
        account_payload = _account_payload(
            profile,
            current_label_name,
            usage_tracking=usage,
        )
        account_payload["rate_limits"] = {"error": error_msg}
        return account_payload, False
    except Exception as exc:
        error_msg = str(exc)
        _mark_refresh_status(profile.account_key, ok=False, error=error_msg)
        if profile.account_key and _refresh_error_requires_reauth(error_msg):
            with suppress(Exception):
                set_broker_credential_assignment_disabled(profile.account_key, disabled=True)
        account_payload = _account_payload(
            profile,
            current_label_name,
            usage_tracking=usage,
        )
        account_payload["rate_limits"] = {"error": error_msg}
        return account_payload, False


async def _fetch_session_limits_for_profile(
    profile: AccountProfile,
    *,
    timeout_seconds: float | None = None,
) -> dict[str, Any]:
    auth_json = profile.auth if isinstance(profile.auth, dict) else {}
    if not auth_json:
        raise CodexCLIError(f"No auth payload stored for profile {profile.label}")
    result = await read_rate_limits_for_auth_async(
        auth_json,
        timeout_seconds=(
            float(timeout_seconds)
            if timeout_seconds is not None
            else float(settings.rate_limit_worker_timeout_seconds)
        ),
    )
    payload = _normalize_session_limit_payload(result.rate_limits)
    if result.account is not None:
        payload["_account"] = result.account
    return payload


async def _refresh_all_profile_rate_limits() -> tuple[int, int]:
    profiles = _dedupe_profiles(list_profiles())
    if not profiles:
        return 0, 0

    semaphore = asyncio.Semaphore(max(int(_LIVE_REFRESH_CONCURRENCY), 1))
    current_label_name = get_active_profile_label()

    async def _run(profile: AccountProfile) -> bool:
        async with semaphore:
            _, ok = await _refresh_profile_session_limits(
                profile,
                current_label_name=current_label_name,
            )
            return ok

    results = await asyncio.gather(*(_run(profile) for profile in profiles))
    refreshed = sum(1 for ok in results if ok)
    failed = sum(1 for ok in results if not ok)
    return refreshed, failed


def _touch_profiles_usage(profiles: list[AccountProfile]) -> None:
    now = datetime.now(timezone.utc)
    for profile in profiles:
        _touch_account_usage(profile=profile, now=now)


def _capture_periodic_usage_snapshots(*, now: datetime) -> None:
    profiles = _dedupe_profiles(list_profiles())
    if not profiles:
        return
    for profile in profiles:
        usage = _usage_tracking_payload(profile.account_key) or {}
        if not usage:
            continue
        try:
            record_absolute_usage_snapshot(
                profile.account_key,
                usage_in_window=usage.get("usage_in_window"),
                usage_limit=usage.get("usage_limit"),
                lifetime_used=usage.get("lifetime_used"),
                rate_limit_refresh_at=usage.get("rate_limit_refresh_at"),
                primary_used_percent=usage.get("primary_used_percent"),
                secondary_used_percent=usage.get("secondary_used_percent"),
                now=now,
            )
            record_percentage_snapshot(
                profile.account_key,
                usage.get("primary_used_percent"),
                usage.get("secondary_used_percent"),
                now=now,
            )
        except Exception:
            logger.exception("Unable to capture periodic usage snapshot for key=%s", profile.account_key)


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

        # Record a point-in-time snapshot for time-series graphing
        if primary_percent is not None or secondary_percent is not None:
            try:
                record_percentage_snapshot(
                    profile.account_key,
                    primary_used_percent=primary_percent,
                    secondary_used_percent=secondary_percent,
                    now=datetime.now(timezone.utc),
                )
            except Exception:
                logger.exception("Unable to record percentage snapshot for key=%s", profile.account_key)

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


def _extract_limit_snapshot(rate_limits: Any) -> dict[str, Any]:
    payload = rate_limits if isinstance(rate_limits, dict) else {}

    primary = payload.get("primary") if isinstance(payload.get("primary"), dict) else None
    secondary = payload.get("secondary") if isinstance(payload.get("secondary"), dict) else None
    # Weekly window should drive rollover tracking/analytics when available.
    candidate = secondary or primary or payload

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


def _openclaw_claim_text(claims: dict[str, Any] | None, *keys: str) -> str | None:
    if not isinstance(claims, dict):
        return None
    for key in keys:
        value = claims.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _openclaw_extract_email(claims: dict[str, Any] | None) -> str | None:
    if not isinstance(claims, dict):
        return None
    direct = _openclaw_claim_text(claims, "email")
    if direct:
        return direct
    profile = claims.get("https://api.openai.com/profile")
    if isinstance(profile, dict):
        return _openclaw_claim_text(profile, "email")
    return None


def _openclaw_extract_display_name(claims: dict[str, Any] | None) -> str | None:
    if not isinstance(claims, dict):
        return None
    direct = _openclaw_claim_text(claims, "name", "display_name", "preferred_username")
    if direct:
        return direct
    profile = claims.get("https://api.openai.com/profile")
    if isinstance(profile, dict):
        return _openclaw_claim_text(profile, "name", "display_name")
    return None


def _resolve_openclaw_expiry_ms(auth_json: dict[str, Any]) -> int:
    for token in (extract_access_token(auth_json), extract_id_token(auth_json)):
        claims = decode_jwt_claims(token)
        exp = claims.get("exp") if isinstance(claims, dict) else None
        if isinstance(exp, (int, float)) and exp > 0:
            exp_num = int(exp)
            return exp_num if exp_num > 1_000_000_000_000 else exp_num * 1000
    return int((datetime.now(timezone.utc) + timedelta(minutes=30)).timestamp() * 1000)


def _decode_openclaw_jwt_segment(token: str | None, index: int) -> dict[str, Any] | None:
    if not isinstance(token, str) or not token.strip():
        return None
    parts = token.split(".")
    if len(parts) <= index:
        return None
    payload = parts[index]
    padding = "=" * ((4 - (len(payload) % 4)) % 4)
    try:
        raw = json.loads(
            base64.urlsafe_b64decode(f"{payload}{padding}".encode("utf-8")).decode("utf-8")
        )
    except Exception:
        return None
    return raw if isinstance(raw, dict) else None


def _default_openclaw_model_entries() -> dict[str, dict[str, Any]]:
    models = [
        "openai-codex/gpt-5.1",
        "openai-codex/gpt-5.1-codex-mini",
        "openai-codex/gpt-5.2",
        "openai-codex/gpt-5.2-codex",
        "openai-codex/gpt-5.3-codex",
        "openai-codex/gpt-5.4",
    ]
    return {f"agents.defaults.models.{model}": {} for model in models}


def _build_openclaw_material_for_auth(
    auth_json: dict[str, Any],
    *,
    email: str | None,
    name: str | None,
    profile_id: str = "openai-codex:lease",
) -> dict[str, Any] | None:
    access_token = extract_access_token(auth_json)
    refresh_token = None
    tokens = auth_json.get("tokens")
    if isinstance(tokens, dict):
        refresh_raw = tokens.get("refresh_token")
        if isinstance(refresh_raw, str) and refresh_raw.strip():
            refresh_token = refresh_raw.strip()

    identity = extract_account_identity(auth_json)
    account_id = identity.account_id
    if not account_id and isinstance(tokens, dict):
        account_raw = tokens.get("account_id")
        if isinstance(account_raw, str) and account_raw.strip():
            account_id = account_raw.strip()
    if not access_token or not refresh_token or not account_id:
        return None

    id_claims = decode_jwt_claims(extract_id_token(auth_json))
    access_claims = decode_jwt_claims(access_token)
    resolved_email = email or _openclaw_extract_email(id_claims) or _openclaw_extract_email(access_claims)
    resolved_name = name or _openclaw_extract_display_name(id_claims) or _openclaw_extract_display_name(access_claims)
    id_token = extract_id_token(auth_json)
    expires_at_ms = _resolve_openclaw_expiry_ms(auth_json)
    openclaw_auth = {
        **_default_openclaw_model_entries(),
        "auth.order.openai-codex": [profile_id],
        f"auth.profiles.{profile_id}": {
            "provider": "openai-codex",
            "mode": "oauth",
        },
        "openai_cid_tokens": {
            profile_id: {
                "access_token": access_token,
                "refresh_token": refresh_token,
                "id_token": id_token,
                "expires_at_ms": expires_at_ms,
                "accountId": account_id,
                "provider": "openai-codex",
                "type": "oauth",
                "decoded_access_jwt": {
                    "header": _decode_openclaw_jwt_segment(access_token, 0),
                    "payload": access_claims,
                },
                **({"email": resolved_email} if resolved_email else {}),
                **({"displayName": resolved_name} if resolved_name else {}),
            }
        },
    }
    return {
        "profile_id": profile_id,
        "openclaw_auth_json": openclaw_auth,
    }


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
    existing = set(list_auth_labels())
    profiles = _dedupe_profiles(list_profiles())
    matched_profile = _find_matching_profile(profiles, resolved_auth, identity)
    incoming_key = (identity.account_key or "").strip()
    incoming_email = (email or "").strip().lower()
    preferred_profile = _find_preferred_placeholder_profile(profiles, incoming_email)

    if desired_label is not None:
        label = desired_label.strip()
        if not label:
            raise ValueError("label cannot be empty")
        created_new_profile = matched_profile is None
    elif (
        matched_profile is not None
        and incoming_key
        and matched_profile.account_key == incoming_key
        and preferred_profile is not None
        and preferred_profile.label != matched_profile.label
    ):
        # If a placeholder profile already exists for the user's obvious label (e.g. email local-part),
        # prefer replacing that placeholder label with the real reauth payload.
        label = preferred_profile.label
        created_new_profile = False
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
        if current_for_label.reauth_required and not _access_token_expired(resolved_auth):
            with suppress(Exception):
                update_saved_profile_reauth_status(
                    label,
                    reauth_required=False,
                    reauth_reason=None,
                )
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
        path=Path("db://active-auth"),
        auth=resolved_auth,
        account_key=identity.account_key,
        subject=identity.subject,
        user_id=identity.user_id,
        provider_account_id=identity.account_id,
        name=identity.name,
        plan_type=identity.plan_type,
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


def _persist_active_auth_db_copy(label: str) -> bool:
    clean = (label or "").strip()
    if not clean:
        return False
    profile = get_saved_profile(clean)
    if profile is None:
        return False
    try:
        active_auth = read_current_auth()
    except CodexCLIError:
        return False
    if not isinstance(active_auth, dict):
        return False

    stored_auth = profile.get("auth_json") if isinstance(profile.get("auth_json"), dict) else {}
    auth_changed = stored_auth != active_auth
    identity = extract_account_identity(active_auth)

    if auth_changed:
        upsert_saved_profile(
            label=clean,
            account_key=identity.account_key or str(profile.get("account_key") or clean),
            auth_json=active_auth,
            email=identity.email or profile.get("email"),
            name=identity.name or profile.get("name"),
            subject=identity.subject or profile.get("subject"),
            user_id=identity.user_id or profile.get("user_id"),
            provider_account_id=identity.account_id or profile.get("provider_account_id"),
        )
    touch_profile_last_used(clean)
    return auth_changed


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


def _auth_freshness_tuple(auth_json: dict[str, Any] | None) -> tuple[float, float, float]:
    if not isinstance(auth_json, dict):
        return (0.0, 0.0, 0.0)
    last_refresh_raw = auth_json.get("last_refresh")
    last_refresh_ts = 0.0
    if isinstance(last_refresh_raw, str) and last_refresh_raw.strip():
        with suppress(ValueError):
            last_refresh_ts = datetime.fromisoformat(
                last_refresh_raw.replace("Z", "+00:00")
            ).timestamp()
    access_claims = decode_jwt_claims(extract_access_token(auth_json)) or {}
    access_iat = float(access_claims.get("iat") or 0.0)
    access_exp = float(access_claims.get("exp") or 0.0)
    return (last_refresh_ts, access_iat, access_exp)


def _compare_auth_freshness(
    current_auth: dict[str, Any] | None,
    incoming_auth: dict[str, Any] | None,
) -> int:
    if not isinstance(current_auth, dict) and not isinstance(incoming_auth, dict):
        return 0
    if not isinstance(current_auth, dict):
        return 1
    if not isinstance(incoming_auth, dict):
        return -1
    if current_auth == incoming_auth:
        return 0
    current_tuple = _auth_freshness_tuple(current_auth)
    incoming_tuple = _auth_freshness_tuple(incoming_auth)
    if incoming_tuple > current_tuple:
        return 1
    if incoming_tuple < current_tuple:
        return -1
    current_token = extract_access_token(current_auth) or ""
    incoming_token = extract_access_token(incoming_auth) or ""
    if current_token == incoming_token:
        return 0
    current_len = len(current_token)
    incoming_len = len(incoming_token)
    if incoming_len > current_len:
        return 1
    if incoming_len < current_len:
        return -1
    return 1


def _find_preferred_placeholder_profile(
    profiles: list[AccountProfile], incoming_email: str
) -> AccountProfile | None:
    if not incoming_email or "@" not in incoming_email:
        return None
    local = incoming_email.split("@", 1)[0].strip().lower()
    if not local:
        return None
    for profile in profiles:
        if (profile.label or "").strip().lower() != local:
            continue
        if _is_placeholder_profile(profile):
            return profile
    return None


def _is_placeholder_profile(profile: AccountProfile) -> bool:
    email = (profile.email or "").strip().lower()
    account_key = (profile.account_key or "").strip().lower()
    token = (profile.access_token or "").strip()
    if email.endswith("@example.com"):
        return True
    if account_key.startswith("email:"):
        return True
    if token and "." not in token:
        return True
    return False


def _dedupe_profiles(profiles: list[AccountProfile]) -> list[AccountProfile]:
    deduped: dict[str, AccountProfile] = {}
    try:
        current_label_name = get_current_auth_label()
    except AuthStoreSwitchError:
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


def _analytics_tzinfo() -> ZoneInfo:
    try:
        return ZoneInfo(settings.analytics_timezone)
    except (ZoneInfoNotFoundError, ValueError):
        return ZoneInfo("UTC")


def _parse_history_range(value: str | None) -> tuple[str, datetime | None]:
    raw = (value or "30d").strip().lower()
    now = datetime.now(timezone.utc)
    analytics_tz = _analytics_tzinfo()
    if raw in {"1d", "1", "day", "24h", "today"}:
        local_now = now.astimezone(analytics_tz)
        local_midnight = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
        return ("1d", local_midnight.astimezone(timezone.utc))
    if raw in {"7d", "7", "week"}:
        return ("7d", now - timedelta(days=7))
    if raw in {"30d", "30", "1m", "month"}:
        return ("30d", now - timedelta(days=30))
    if raw in {"90d", "90", "3m", "quarter"}:
        return ("90d", now - timedelta(days=90))
    if raw in {"all", "*"}:
        return ("all", None)
    return ("30d", now - timedelta(days=30))


def _history_range_metadata(selected_range: str) -> dict[str, Any]:
    tz = _analytics_tzinfo()
    if selected_range == "1d":
        return {
            "label": "Today",
            "window_label": "Since midnight",
            "timezone": getattr(tz, "key", "UTC"),
            "boundary_mode": "local_day",
        }
    return {
        "label": selected_range,
        "window_label": selected_range,
        "timezone": getattr(tz, "key", "UTC"),
        "boundary_mode": "rolling",
    }


def _parse_captured_at(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _compute_daily_consumption_per_account(
    *,
    snapshots: list[dict[str, Any]],
    since_dt: datetime | None,
    tz: ZoneInfo | None = None,
) -> dict[str, dict[str, int]]:
    analytics_tz = tz or _analytics_tzinfo()
    by_account: dict[str, list[tuple[datetime, int]]] = {}
    for row in snapshots:
        account_id = str(row.get("account_id") or "").strip()
        captured_dt = _parse_captured_at(row.get("captured_at"))
        lifetime_raw = row.get("lifetime_used")
        if not account_id or captured_dt is None or lifetime_raw is None:
            continue
        try:
            lifetime = int(lifetime_raw)
        except (TypeError, ValueError):
            continue
        by_account.setdefault(account_id, []).append((captured_dt, lifetime))

    out: dict[str, dict[str, int]] = {}
    for account_id, points in by_account.items():
        points.sort(key=lambda item: item[0])
        prev_lifetime: int | None = None
        acc_day: dict[str, int] = {}
        for captured_dt, lifetime in points:
            delta = 0 if prev_lifetime is None else max(lifetime - prev_lifetime, 0)
            prev_lifetime = lifetime
            if since_dt is not None and captured_dt < since_dt:
                continue
            day = captured_dt.astimezone(analytics_tz).date().isoformat()
            if delta <= 0:
                acc_day.setdefault(day, 0)
                continue
            acc_day[day] = acc_day.get(day, 0) + delta
        out[account_id] = acc_day
    return out


def _compute_modeled_consumption_per_account(
    *,
    snapshots: list[dict[str, Any]],
    since_dt: datetime | None,
    tz: ZoneInfo | None = None,
    metric: str = "secondary_used_percent",
) -> dict[str, dict[str, float]]:
    analytics_tz = tz or _analytics_tzinfo()
    by_account: dict[str, list[tuple[datetime, float]]] = {}
    for row in snapshots:
        account_id = str(row.get("account_id") or "").strip()
        captured_dt = _parse_captured_at(row.get("captured_at"))
        raw = row.get(metric)
        if not account_id or captured_dt is None or raw is None:
            continue
        try:
            value = float(raw)
        except (TypeError, ValueError):
            continue
        if not (0.0 <= value <= 100.0):
            continue
        by_account.setdefault(account_id, []).append((captured_dt, value))

    out: dict[str, dict[str, float]] = {}
    for account_id, points in by_account.items():
        points.sort(key=lambda item: item[0])
        prev_dt: datetime | None = None
        prev_value: float | None = None
        acc_day: dict[str, float] = {}
        for captured_dt, value in points:
            if prev_value is None:
                prev_dt = captured_dt
                prev_value = value
                continue
            delta = value - prev_value
            if delta < 0:
                prev_dt = captured_dt
                prev_value = value
                continue
            if since_dt is not None and captured_dt < since_dt:
                prev_dt = captured_dt
                prev_value = value
                continue
            if since_dt is not None and prev_dt is not None and prev_dt < since_dt <= captured_dt:
                # We have a point spanning the boundary, so treat the delta after the boundary as in-range.
                day = captured_dt.astimezone(analytics_tz).date().isoformat()
                acc_day[day] = acc_day.get(day, 0.0) + delta
            elif since_dt is None or (prev_dt is not None and prev_dt >= since_dt):
                day = captured_dt.astimezone(analytics_tz).date().isoformat()
                acc_day[day] = acc_day.get(day, 0.0) + delta
            prev_dt = captured_dt
            prev_value = value
        out[account_id] = {day: round(value, 2) for day, value in acc_day.items()}
    return out


def _current_modeled_usage_from_percent(raw: Any) -> float | None:
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    if not (0.0 <= value <= 100.0):
        return None
    return round(value, 2)


def _history_hours_for_range(selected_range: str) -> int:
    if selected_range == "1d":
        return 48
    if selected_range == "7d":
        return 24 * 8
    if selected_range == "30d":
        return 24 * 31
    if selected_range == "90d":
        return 24 * 91
    # "all" can be large; cap to last year for bounded query cost.
    return 24 * 365


def _compute_daily_utilization_series(
    *,
    snapshots: list[dict[str, Any]],
    since_dt: datetime | None,
    metric: str,
) -> list[dict[str, Any]]:
    analytics_tz = _analytics_tzinfo()
    buckets: dict[str, dict[str, float]] = {}
    for row in snapshots:
        captured_dt = _parse_captured_at(row.get("captured_at"))
        if captured_dt is None:
            continue
        if since_dt is not None and captured_dt < since_dt:
            continue
        day = captured_dt.astimezone(analytics_tz).date().isoformat()
        raw = row.get(metric)
        if raw is None:
            continue
        try:
            value = float(raw)
        except (TypeError, ValueError):
            continue
        bucket = buckets.setdefault(day, {"sum": 0.0, "count": 0.0})
        bucket["sum"] += value
        bucket["count"] += 1.0

    series: list[dict[str, Any]] = []
    for day in sorted(buckets.keys()):
        b = buckets[day]
        if b["count"] <= 0:
            continue
        series.append({"day": day, "value": round(b["sum"] / b["count"], 2)})
    return series


def _compute_hourly_utilization_series(
    *,
    snapshots: list[dict[str, Any]],
    since_dt: datetime | None,
    metric: str,
) -> list[dict[str, Any]]:
    analytics_tz = _analytics_tzinfo()
    buckets: dict[str, dict[str, float]] = {}
    for row in snapshots:
        captured_dt = _parse_captured_at(row.get("captured_at"))
        if captured_dt is None:
            continue
        if since_dt is not None and captured_dt < since_dt:
            continue
        raw = row.get(metric)
        if raw is None:
            continue
        try:
            value = float(raw)
        except (TypeError, ValueError):
            continue
        local_captured_dt = captured_dt.astimezone(analytics_tz)
        local_dt = local_captured_dt.replace(
            minute=(local_captured_dt.minute // 10) * 10,
            second=0,
            microsecond=0,
        )
        hour_key = local_dt.isoformat().replace("+00:00", "Z")
        bucket = buckets.setdefault(hour_key, {"sum": 0.0, "count": 0.0})
        bucket["sum"] += value
        bucket["count"] += 1.0

    series: list[dict[str, Any]] = []
    for hour_key in sorted(buckets.keys()):
        b = buckets[hour_key]
        if b["count"] <= 0:
            continue
        series.append({"t": hour_key, "value": round(b["sum"] / b["count"], 2)})
    return series


def _filter_rollovers_by_range(
    rollovers: list[dict[str, Any]], since_dt: datetime | None
) -> list[dict[str, Any]]:
    if since_dt is None:
        return list(rollovers)
    since_iso = since_dt.isoformat()
    return [
        row
        for row in rollovers
        if str(row.get("window_ended_at") or row.get("rolled_over_at") or "") >= since_iso
    ]


def _is_weekly_rollover(row: dict[str, Any]) -> bool:
    if row.get("window_type") == "weekly":
        return True
    start_raw = str(row.get("window_started_at") or "")
    end_raw = str(row.get("window_ended_at") or "")
    if start_raw and end_raw and start_raw == end_raw:
        try:
            usage_limit = int(row.get("usage_limit") or 0)
        except (TypeError, ValueError):
            usage_limit = 0
        if usage_limit >= 100:
            return True
    if not start_raw or not end_raw:
        return False
    try:
        start_dt = datetime.fromisoformat(start_raw.replace("Z", "+00:00"))
        end_dt = datetime.fromisoformat(end_raw.replace("Z", "+00:00"))
    except ValueError:
        return False
    if start_dt.tzinfo is None:
        start_dt = start_dt.replace(tzinfo=timezone.utc)
    else:
        start_dt = start_dt.astimezone(timezone.utc)
    if end_dt.tzinfo is None:
        end_dt = end_dt.replace(tzinfo=timezone.utc)
    else:
        end_dt = end_dt.astimezone(timezone.utc)
    duration_hours = (end_dt - start_dt).total_seconds() / 3600
    return duration_hours >= 24 * 6


def _group_rollover_metric_by_day(
    rollovers: list[dict[str, Any]], *, metric: str
) -> list[dict[str, Any]]:
    buckets: dict[str, int] = {}
    for row in rollovers:
        dt = str(row.get("window_ended_at") or row.get("rolled_over_at") or "")
        day = dt[:10]
        if len(day) != 10:
            continue
        buckets[day] = buckets.get(day, 0) + int(row.get(metric) or 0)
    return [{"day": day, "value": buckets[day]} for day in sorted(buckets.keys())]


def _latest_captured_at(rows: list[dict[str, Any]]) -> str | None:
    latest: str | None = None
    for row in rows:
        captured = str(row.get("captured_at") or "").strip()
        if not captured:
            continue
        if latest is None or captured > latest:
            latest = captured
    return latest


def _selected_day_count(selected_range: str, daily_series: list[dict[str, Any]]) -> int:
    if selected_range == "1d":
        return 1
    if selected_range == "7d":
        return 7
    if selected_range == "30d":
        return 30
    if selected_range == "90d":
        return 90
    return max(len(daily_series), 1)


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
        label = get_current_auth_label()
        if label:
            return label
    except AuthStoreSwitchError:
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


def _auth_file_metadata() -> dict[str, Any]:
    active_auth = get_active_auth_json()
    updated_at = get_active_auth_updated_at()
    encoded = json.dumps(active_auth, sort_keys=True) if isinstance(active_auth, dict) else None
    return {
        "path": "db://active-auth",
        "exists": isinstance(active_auth, dict),
        "size_bytes": len(encoded.encode("utf-8")) if encoded is not None else None,
        "modified_at": updated_at,
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
