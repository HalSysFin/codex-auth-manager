from __future__ import annotations

import hmac
import ipaddress
import json
import logging
import secrets
from hashlib import sha256
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from .accounts import AccountProfile, list_profiles
from .auth_store import (
    AuthStoreError,
    persist_and_save_label,
    persist_current_auth,
    save_current_auth_under_label,
)
from .codex_cli import (
    CodexCLIError,
    derive_label,
    extract_email,
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

    if request.url.path.startswith("/api/") or request.url.path.startswith("/auth/"):
        return JSONResponse({"detail": "Login required"}, status_code=401)

    next_path = request.url.path
    if request.url.query:
        next_path = f"{next_path}?{request.url.query}"
    return RedirectResponse(url=f"/login?next={quote(next_path, safe='/?=&')}", status_code=303)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/login")
async def login_page(next: str = "/") -> HTMLResponse:
    if not _web_login_enabled():
        return RedirectResponse(url=next or "/", status_code=303)
    return HTMLResponse(_render_login(next))


@app.post("/login")
async def login_submit(request: Request) -> RedirectResponse:
    if not _web_login_enabled():
        return RedirectResponse(url="/", status_code=303)

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
        return RedirectResponse(
            url=f"/login?next={quote(next_path, safe='/?=&')}&error=1", status_code=303
        )

    response = RedirectResponse(url=_safe_next_path(next_path), status_code=303)
    _set_web_session_cookie(request, response)
    return response


@app.post("/logout")
async def logout(request: Request) -> RedirectResponse:
    response = RedirectResponse(url="/login", status_code=303)
    response.delete_cookie(settings.web_login_cookie_name, path="/")
    return response


@app.get("/")
async def index() -> HTMLResponse:
    return HTMLResponse(_render_index())


@app.get("/ui")
async def ui() -> HTMLResponse:
    return HTMLResponse(_render_index())


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
                "handoff_supported": False,
                "finalization_supported": False,
                "next_action": (
                    "Relay callback captured. Direct CLI callback handoff is not implemented; "
                    "finalize auth in CLI/manual flow, then run POST /auth/import-current."
                    if callback_received and not result.auth_updated
                    else (
                        "Run POST /auth/import-current to save this auth into codex-switch profiles."
                        if result.auth_updated
                        else None
                    )
                ),
            },
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
        auth_json = read_current_auth()
    except CodexCLIError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    email = extract_email(auth_json)
    existing = set(list_labels())

    if desired_label:
        label = str(desired_label).strip()
        if not label:
            raise HTTPException(status_code=400, detail="label cannot be empty")
    else:
        label = derive_label(email or "account", existing_labels=existing)

    try:
        # Keep current auth.json as the active source of truth, then save profile.
        persist_current_auth(auth_json)
        switch_save = save_current_auth_under_label(label)
    except (AuthStoreError, CodexSwitchError) as exc:
        raise _to_switch_http_error(exc) from exc

    return JSONResponse(
        {
            "status": "imported",
            "label": label,
            "email": email,
            "saved": True,
            "codex_switch": {
                "command": switch_save.command,
                "exit_code": switch_save.returncode,
                "stdout": switch_save.stdout,
            },
        }
    )


@app.post("/auth/switch")
async def auth_switch(request: Request, payload: dict[str, Any]) -> JSONResponse:
    _require_internal_auth(request)
    label = str(payload.get("label", "")).strip()
    if not label:
        raise HTTPException(status_code=400, detail="label is required")

    try:
        result = switch_label(label)
        now_current = _resolve_current_label(read_current_auth(), list_profiles())
    except CodexSwitchError as exc:
        raise _to_switch_http_error(exc) from exc
    except CodexCLIError:
        now_current = None

    return JSONResponse(
        {
            "status": "switched",
            "label": label,
            "current_label": now_current or label,
            "codex_switch": {
                "command": result.command,
                "exit_code": result.returncode,
                "stdout": result.stdout,
            },
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

    return JSONResponse(
        {
            "source": "codex_app_server",
            "account": result.account,
            "rate_limits": result.rate_limits,
            "notifications": result.notifications,
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
    profiles = list_profiles()

    current = None
    try:
        current = _resolve_current_label(read_current_auth(), profiles)
    except CodexCLIError:
        current = None

    probe_by_label: dict[str, dict[str, Any]] = {}
    async with httpx.AsyncClient(timeout=10) as client:
        for profile in profiles:
            if profile.access_token:
                rate_info = await _fetch_rate_limits(client, profile.access_token)
            else:
                rate_info = {"error": "No access token found"}
            probe_by_label[profile.label] = rate_info

    session_by_label = _fetch_session_limits_for_profiles(
        profiles, baseline_auth=_safe_read_current_auth()
    )

    results: list[dict[str, Any]] = []
    for profile in profiles:
        rate_info = probe_by_label.get(profile.label, {})
        if _has_limit_data(rate_info):
            final_rate_info = rate_info
        else:
            final_rate_info = session_by_label.get(profile.label, rate_info)

        results.append(
            {
                "label": profile.label,
                "display_label": _display_label(profile.label, profile.email),
                "email": profile.email,
                "is_current": profile.label == current,
                "rate_limits": final_rate_info,
            }
        )

    return JSONResponse({"accounts": results, "current_label": current})


@app.get("/api/public-stats")
async def api_public_stats() -> JSONResponse:
    profiles = list_profiles()
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


def _web_login_enabled() -> bool:
    return bool(
        (settings.web_login_username or "").strip()
        and (settings.web_login_password or "").strip()
        and (settings.web_login_session_secret or "").strip()
    )


def _is_login_exempt_path(path: str) -> bool:
    if path in {"/health", "/login"}:
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

    token = _extract_token(current_auth)
    for profile in profiles:
        if profile.auth == current_auth:
            return profile.label
        if token and profile.access_token and profile.access_token == token:
            return profile.label
    return None


def _display_label(label: str | None, email: str | None) -> str | None:
    if not label:
        return None
    if not email:
        return label
    local = email.split("@", 1)[0].strip().lower()
    if not local:
        return label
    return local


def _extract_token(payload: Any) -> str | None:
    if isinstance(payload, dict):
        for key in ["access_token", "accessToken", "token", "api_key", "apiKey"]:
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        for value in payload.values():
            found = _extract_token(value)
            if found:
                return found
    elif isinstance(payload, list):
        for item in payload:
            found = _extract_token(item)
            if found:
                return found
    return None


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
      #statusNote.ok{background:rgba(52,211,153,.08);color:var(--green);
        border:1px solid rgba(52,211,153,.18)}

      /* ── ACCOUNT TABLE ────────────────────── */
      .accounts-table{width:100%;border-collapse:separate;border-spacing:0}
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
      .pill{
        display:inline-flex;align-items:center;gap:5px;
        padding:4px 10px;border-radius:4px;font-size:.72rem;font-weight:600;
        border:1px solid var(--border);color:var(--dim);background:transparent;
      }
      .pill.active{color:var(--green);border-color:rgba(16,185,129,.35);
        background:rgba(16,185,129,.08)}

      /* progress bar for limits */
      .limit-bar-wrap{display:flex;flex-direction:column;gap:6px}
      .limit-bar-row{display:flex;align-items:center;gap:10px}
      .limit-bar-label{font-size:.72rem;font-weight:600;text-transform:uppercase;
        letter-spacing:.05em;color:var(--dim);min-width:50px}
      .limit-bar-track{flex:1;height:6px;border-radius:99px;background:var(--bg);
        overflow:hidden;border:1px solid var(--border)}
      .limit-bar-fill{height:100%;border-radius:99px;transition:width .6s ease}
      .limit-bar-fill.ok{background:var(--green)}
      .limit-bar-fill.warn{background:var(--amber)}
      .limit-bar-fill.danger{background:var(--red)}
      .limit-bar-text{font-size:.78rem;font-weight:600;min-width:52px;text-align:right;
        font-family:'Outfit',sans-serif}

      .acct-actions{display:flex;gap:6px}

      /* empty state */
      .empty-state{
        text-align:center;padding:48px 20px;color:var(--dim);
      }
      .empty-state .empty-icon{font-size:2.5rem;margin-bottom:12px;opacity:.4}
      .empty-state p{font-size:.9rem;line-height:1.5}

      /* ── RESPONSIVE ───────────────────────── */
      @media(max-width:860px){
        .two-col{grid-template-columns:1fr}
        .sidebar{position:static;flex-direction:row;flex-wrap:wrap}
        .sidebar .panel{flex:1;min-width:240px}
        .navbar{padding:14px 16px}
        .page{padding:20px 14px 48px}
      }
      @media(max-width:600px){
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
                <div class="kv-label">Login Status</div>
                <div class="kv-value" id="loginStatus"><span class="status-dot green"></span> Idle</div>
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
                <div class="kv-label">Cluster 5HR Usage</div>
                <div class="kv-value small" id="agg5Hr">
                  <div class="limit-bar-track"><div class="limit-bar-fill" style="width:0%"></div></div>
                </div>
              </div>
              <div class="kv-item">
                <div class="kv-label">Cluster Weekly Usage</div>
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
          <div class="panel" style="padding:0;overflow:hidden">
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
      const loginStatusEl = $("loginStatus");
      const authUpdatedAtEl = $("authUpdatedAt");
      const accountCountEl = $("accountCount");

      const getToken = () => (localStorage.getItem("internalToken") || "").trim();

      function setStatus(text, warn = false) {
        statusNoteEl.textContent = text || "";
        statusNoteEl.className = text ? ("show " + (warn ? "warn" : "ok")) : "";
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

      function statusDot(status) {
        const s = (status || "").toLowerCase();
        if (s === "idle" || s === "ok") return '<span class="status-dot green"></span>';
        if (s.includes("wait") || s.includes("pending")) return '<span class="status-dot amber"></span>';
        if (s.includes("error") || s.includes("fail")) return '<span class="status-dot red"></span>';
        return '<span class="status-dot green"></span>';
      }

      /* ── render helpers ─────────────────── */
      function limitBar(label, data) {
        if (!data) return "";
        const pct = data.percent ?? data.usedPercent ?? null;
        const remaining = data.remaining ?? "--";
        const limit = data.limit ?? "--";
        const fillPct = pct !== null ? Math.min(100, Math.max(0, pct)) : 0;
        const cls = fillPct > 85 ? "danger" : fillPct > 60 ? "warn" : "ok";
        const pctText = pct !== null ? fillPct + "%" : "--";
        return `<div class="limit-bar-row">
          <span class="limit-bar-label">${label}</span>
          <div class="limit-bar-track"><div class="limit-bar-fill ${cls}" style="width:${fillPct}%"></div></div>
          <span class="limit-bar-text">${pctText}</span>
        </div>`;
      }

      function renderCard(account) {
        const isActive = account.is_current;
        const displayLabel = account.display_label || account.label;
        const email = account.email || "—";
        const rate = account.rate_limits || {};
        const prim = rate.requests || rate.primary || null;
        const sec = rate.tokens || rate.secondary || null;
        const limitsHtml = (prim || sec)
          ? `<div class="limit-bar-wrap">${limitBar("Requests", prim)}${limitBar("Tokens", sec)}</div>`
          : '<span style="color:var(--dim);font-size:.8rem">No limit data</span>';

        return `<tr>
          <td data-label="Profile"><div class="acct-name">${displayLabel}</div><div class="acct-email">${email}</div></td>
          <td data-label="Status"><span class="pill ${isActive ? "active" : ""}">${isActive ? "Active" : "Saved"}</span></td>
          <td data-label="Rate Limits">${limitsHtml}</td>
          <td data-label="Actions"><div class="acct-actions">
            <button class="btn btn-sm ${isActive ? "" : "btn-primary"}" type="button" data-action="switch" data-label="${account.label}">${isActive ? "Current" : "Switch"}</button>
            <button class="btn btn-sm btn-icon" type="button" data-action="export" data-label="${account.label}" title="Export">
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>
            </button>
          </div></td>
        </tr>`;
      }

      function wrapTable(rows) {
        return `<table class="accounts-table">
          <thead><tr><th>Profile</th><th>Status</th><th>Rate Limits</th><th style="width:140px">Actions</th></tr></thead>
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
          const status = data.login_status || "idle";
          loginStatusEl.innerHTML = statusDot(status) + " " + status.charAt(0).toUpperCase() + status.slice(1);
          authUpdatedAtEl.textContent = humanDate(data.auth_file?.modified_at);
        } catch (_) {
          accountsManagedEl.textContent = "--";
          profilesWithTokensEl.textContent = "--";
          loginStatusEl.innerHTML = '<span class="status-dot amber"></span> Unknown';
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
          
          $("agg5Hr").innerHTML = limitBar("5HR", {percent: avg5Hr}) + `<div style="font-size:0.75rem;margin-top:6px;color:var(--dim)">${100 - avg5Hr}% remaining across cluster</div>`;
          $("aggWeekly").innerHTML = limitBar("WEEKLY", {percent: avgWeekly}) + `<div style="font-size:0.75rem;margin-top:6px;color:var(--dim)">${100 - avgWeekly}% remaining across cluster</div>`;
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
        setStatus(data.instructions || "Login started.");
        await loadPublicStats();
      }

      async function importCurrent() {
        const lbl = window.prompt("Optional label (leave empty for auto):", "");
        const body = lbl && lbl.trim() ? { label: lbl.trim() } : {};
        const res = await apiFetch("/auth/import-current", { method: "POST", body: JSON.stringify(body) });
        if (!res.ok) throw new Error(await readError(res, "Import failed"));
        const data = await res.json();
        setStatus("Imported current auth as '" + data.label + "'");
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

      const refreshAll = () => Promise.all([loadPublicStats(), loadAccounts()]);

      $("tokenSave").addEventListener("click", async () => { localStorage.setItem("internalToken", tokenInput.value.trim()); await refreshAll(); });
      $("tokenClear").addEventListener("click", async () => { localStorage.removeItem("internalToken"); tokenInput.value = ""; await refreshAll(); });
      $("addAccount").addEventListener("click", async () => { try { await startLogin(); } catch (e) { setStatus(e.message, true); } });
      $("importCurrent").addEventListener("click", async () => { try { await importCurrent(); await refreshAll(); } catch (e) { setStatus(e.message, true); } });
      $("refreshAll").addEventListener("click", refreshAll);
      accountsEl.addEventListener("click", async e => {
        const t = e.target.closest("[data-action]"); if (!t) return;
        const action = t.dataset.action, label = t.dataset.label; if (!action || !label) return;
        try {
          if (action === "switch") { await switchProfile(label); await refreshAll(); }
          else if (action === "export") { await exportProfile(label); }
        } catch (err) { setStatus(err.message, true); }
      });
      refreshAll();
      setInterval(() => { loadPublicStats(); if(getToken()) loadAccounts(); }, 15000);
    </script>
  </body>
</html>"""
