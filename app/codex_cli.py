from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import re
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import httpx

from .account_usage_store import get_active_auth_json, get_active_auth_updated_at, set_active_auth_json
from .config import settings
from .oauth_flow import build_auth_payload, build_oauth_authorize_url

EMAIL_KEYS = [
    "email",
    "user_email",
    "userEmail",
    "account_email",
    "primary_email",
]
ID_TOKEN_KEYS = [
    "id_token",
    "idToken",
]

NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")
DASH_RE = re.compile(r"-+")


class CodexCLIError(RuntimeError):
    pass


@dataclass
class LoginStartResult:
    started: bool
    pid: int | None
    started_at: str
    auth_path: str
    browser_url: str | None
    instructions: str
    output_excerpt: str | None = None


@dataclass
class LoginStatusResult:
    status: str
    auth_exists: bool
    auth_updated: bool
    auth_path: str
    started_at: str | None
    completed_at: str | None
    browser_url: str | None
    pid: int | None
    error: str | None = None


@dataclass
class AppServerRateLimitsResult:
    account: dict[str, Any] | None
    rate_limits: Any
    notifications: list[dict[str, Any]]


@dataclass
class _LoginState:
    started_at: datetime
    process: subprocess.Popen[str] | None
    browser_url: str | None
    output_excerpt: str | None
    oauth_state: str | None = None
    code_verifier: str | None = None
    redirect_uri: str | None = None
    auth_updated_at: str | None = None


_LOGIN_STATE: _LoginState | None = None


def read_rate_limits_via_app_server(timeout_seconds: float = 15.0) -> AppServerRateLimitsResult:
    auth_json = read_current_auth()
    return read_rate_limits_for_auth(auth_json, timeout_seconds=timeout_seconds)


async def read_rate_limits_via_app_server_async(timeout_seconds: float = 15.0) -> AppServerRateLimitsResult:
    auth_json = read_current_auth()
    return await read_rate_limits_for_auth_async(auth_json, timeout_seconds=timeout_seconds)


def read_rate_limits_for_auth(
    auth_json: dict[str, Any],
    timeout_seconds: float = 15.0,
) -> AppServerRateLimitsResult:
    try:
        return _read_rate_limits_via_worker(auth_json, timeout_seconds=timeout_seconds)
    except CodexCLIError:
        pass
    if str(settings.chatgpt_backend_rate_limits_url or "").strip():
        try:
            return _read_rate_limits_via_chatgpt_backend(
                auth_json,
                timeout_seconds=timeout_seconds,
            )
        except CodexCLIError:
            pass
    if settings.openai_rate_limits_url:
        return _read_rate_limits_via_direct_api(auth_json, timeout_seconds=timeout_seconds)
    return _read_rate_limits_via_worker(auth_json, timeout_seconds=timeout_seconds)


async def read_rate_limits_for_auth_async(
    auth_json: dict[str, Any],
    timeout_seconds: float = 15.0,
) -> AppServerRateLimitsResult:
    try:
        return await _read_rate_limits_via_worker_async(
            auth_json,
            timeout_seconds=timeout_seconds,
        )
    except CodexCLIError:
        pass
    if str(settings.chatgpt_backend_rate_limits_url or "").strip():
        try:
            return await _read_rate_limits_via_chatgpt_backend_async(
                auth_json,
                timeout_seconds=timeout_seconds,
            )
        except CodexCLIError:
            pass
    if settings.openai_rate_limits_url:
        return await _read_rate_limits_via_direct_api_async(
            auth_json,
            timeout_seconds=timeout_seconds,
        )
    return await _read_rate_limits_via_worker_async(auth_json, timeout_seconds=timeout_seconds)


def _chatgpt_backend_headers(auth_json: dict[str, Any]) -> dict[str, str]:
    access_token = _find_first_key(auth_json, ["access_token", "accessToken", "token"])
    if not access_token:
        raise CodexCLIError("No access token available for ChatGPT backend rate-limit request")
    account_id = _find_first_key(auth_json, ["account_id", "accountId"])
    if not account_id:
        raise CodexCLIError("No account id available for ChatGPT backend rate-limit request")

    originator = str(settings.openai_originator or "codex_cli_rs").strip() or "codex_cli_rs"
    return {
        "Authorization": f"Bearer {access_token}",
        "chatgpt-account-id": account_id,
        "originator": originator,
        "User-Agent": f"{originator} (linux; x86_64)",
        "OpenAI-Beta": "responses=experimental",
        "Accept": "text/event-stream",
        "Content-Type": "application/json",
    }


def _chatgpt_backend_payload() -> dict[str, Any]:
    return {
        "model": str(settings.chatgpt_backend_rate_limits_model or "gpt-5.1-codex-mini").strip() or "gpt-5.1-codex-mini",
        "instructions": str(settings.chatgpt_backend_rate_limits_instructions or "You are Codex. Be concise.").strip() or "You are Codex. Be concise.",
        "store": False,
        "stream": True,
        "input": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": str(settings.chatgpt_backend_rate_limits_prompt or "Reply with exactly OK.").strip() or "Reply with exactly OK.",
                    }
                ],
            }
        ],
    }


def _parse_bool_header(value: str | None) -> bool | None:
    raw = str(value or "").strip().lower()
    if raw in {"true", "1", "yes"}:
        return True
    if raw in {"false", "0", "no"}:
        return False
    return None


def _parse_int_header(value: str | None) -> int | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def _chatgpt_rate_limits_from_headers(headers: dict[str, str]) -> dict[str, Any]:
    primary_used = _parse_int_header(headers.get("x-codex-primary-used-percent"))
    secondary_used = _parse_int_header(headers.get("x-codex-secondary-used-percent"))
    primary_window = _parse_int_header(headers.get("x-codex-primary-window-minutes"))
    secondary_window = _parse_int_header(headers.get("x-codex-secondary-window-minutes"))
    primary_reset_at = _parse_int_header(headers.get("x-codex-primary-reset-at"))
    secondary_reset_at = _parse_int_header(headers.get("x-codex-secondary-reset-at"))
    primary_reset_after = _parse_int_header(headers.get("x-codex-primary-reset-after-seconds"))
    secondary_reset_after = _parse_int_header(headers.get("x-codex-secondary-reset-after-seconds"))

    if all(
        value is None
        for value in (
            primary_used,
            secondary_used,
            primary_window,
            secondary_window,
            primary_reset_at,
            secondary_reset_at,
        )
    ):
        raise CodexCLIError("ChatGPT backend response did not include Codex rate-limit headers")

    primary: dict[str, Any] = {
        "usedPercent": primary_used,
        "percent": primary_used,
        "windowDurationMins": primary_window,
        "resetsAt": primary_reset_at,
    }
    secondary: dict[str, Any] = {
        "usedPercent": secondary_used,
        "percent": secondary_used,
        "windowDurationMins": secondary_window,
        "resetsAt": secondary_reset_at,
    }
    if primary_reset_after is not None:
        primary["resetAfterSeconds"] = primary_reset_after
    if secondary_reset_after is not None:
        secondary["resetAfterSeconds"] = secondary_reset_after

    return {
        "limitId": headers.get("x-codex-active-limit"),
        "planType": headers.get("x-codex-plan-type"),
        "primary": primary,
        "secondary": secondary,
        "credits": {
            "hasCredits": _parse_bool_header(headers.get("x-codex-credits-has-credits")),
            "balance": headers.get("x-codex-credits-balance"),
            "unlimited": _parse_bool_header(headers.get("x-codex-credits-unlimited")),
        },
        "raw_headers": headers,
    }


def _read_rate_limits_via_chatgpt_backend(
    auth_json: dict[str, Any],
    timeout_seconds: float = 15.0,
) -> AppServerRateLimitsResult:
    url = str(settings.chatgpt_backend_rate_limits_url or "").strip()
    if not url:
        raise CodexCLIError("Missing configured ChatGPT backend rate-limit URL")
    headers = _chatgpt_backend_headers(auth_json)
    payload = _chatgpt_backend_payload()
    try:
        with httpx.Client(timeout=timeout_seconds, follow_redirects=True) as client:
            with client.stream("POST", url, headers=headers, json=payload) as response:
                if response.status_code >= 400:
                    detail = response.read().decode("utf-8", errors="replace").strip()
                    raise CodexCLIError(detail or f"ChatGPT backend request failed with HTTP {response.status_code}")
                parsed = _chatgpt_rate_limits_from_headers(dict(response.headers))
    except httpx.HTTPError as exc:
        raise CodexCLIError(f"Unable to reach ChatGPT backend rate-limit endpoint {url}: {exc}") from exc

    return AppServerRateLimitsResult(account=None, rate_limits=parsed, notifications=[])


async def _read_rate_limits_via_chatgpt_backend_async(
    auth_json: dict[str, Any],
    timeout_seconds: float = 15.0,
) -> AppServerRateLimitsResult:
    url = str(settings.chatgpt_backend_rate_limits_url or "").strip()
    if not url:
        raise CodexCLIError("Missing configured ChatGPT backend rate-limit URL")
    headers = _chatgpt_backend_headers(auth_json)
    payload = _chatgpt_backend_payload()
    try:
        async with httpx.AsyncClient(timeout=timeout_seconds, follow_redirects=True) as client:
            async with client.stream("POST", url, headers=headers, json=payload) as response:
                if response.status_code >= 400:
                    detail = (await response.aread()).decode("utf-8", errors="replace").strip()
                    raise CodexCLIError(detail or f"ChatGPT backend request failed with HTTP {response.status_code}")
                parsed = _chatgpt_rate_limits_from_headers(dict(response.headers))
    except httpx.HTTPError as exc:
        raise CodexCLIError(f"Unable to reach ChatGPT backend rate-limit endpoint {url}: {exc}") from exc

    return AppServerRateLimitsResult(account=None, rate_limits=parsed, notifications=[])


def _read_rate_limits_via_direct_api(
    auth_json: dict[str, Any],
    timeout_seconds: float = 15.0,
) -> AppServerRateLimitsResult:
    access_token = _find_first_key(auth_json, ["access_token", "accessToken", "token"])
    if not access_token:
        raise CodexCLIError("No access token available for direct rate-limit request")

    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {access_token}",
        "User-Agent": "codex-auth-manager/direct-rate-limits",
    }

    rate_limits = _http_json_request(
        settings.openai_rate_limits_url,
        headers=headers,
        timeout_seconds=timeout_seconds,
    )

    account = None
    if settings.openai_account_url:
        account_response = _http_json_request(
            settings.openai_account_url,
            headers=headers,
            timeout_seconds=timeout_seconds,
        )
        if isinstance(account_response, dict):
            account = account_response.get("account") if isinstance(account_response.get("account"), dict) else account_response

    return AppServerRateLimitsResult(
        account=account if isinstance(account, dict) else None,
        rate_limits=rate_limits,
        notifications=[],
    )


async def _read_rate_limits_via_direct_api_async(
    auth_json: dict[str, Any],
    timeout_seconds: float = 15.0,
) -> AppServerRateLimitsResult:
    access_token = _find_first_key(auth_json, ["access_token", "accessToken", "token"])
    if not access_token:
        raise CodexCLIError("No access token available for direct rate-limit request")

    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {access_token}",
        "User-Agent": "codex-auth-manager/direct-rate-limits",
    }

    async with httpx.AsyncClient(timeout=timeout_seconds) as client:
        rate_limits_task = _http_json_request_async(
            client,
            settings.openai_rate_limits_url,
            headers=headers,
        )
        account_task = (
            _http_json_request_async(client, settings.openai_account_url, headers=headers)
            if settings.openai_account_url
            else None
        )
        if account_task is not None:
            rate_limits, account_response = await asyncio.gather(rate_limits_task, account_task)
        else:
            rate_limits = await rate_limits_task
            account_response = None

    account = None
    if isinstance(account_response, dict):
        account = account_response.get("account") if isinstance(account_response.get("account"), dict) else account_response

    return AppServerRateLimitsResult(
        account=account if isinstance(account, dict) else None,
        rate_limits=rate_limits,
        notifications=[],
    )


def _read_rate_limits_via_worker(
    auth_json: dict[str, Any],
    timeout_seconds: float = 15.0,
) -> AppServerRateLimitsResult:
    payload = _worker_payload(auth_json, timeout_seconds=timeout_seconds)
    try:
        completed = subprocess.run(
            [sys.executable, "-m", "app.codex_app_server_worker"],
            input=json.dumps(payload),
            text=True,
            capture_output=True,
            timeout=max(timeout_seconds + 1.0, timeout_seconds),
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise CodexCLIError(f"Unable to start Codex rate-limit worker: {exc}") from exc
    return _parse_worker_result(
        stdout=completed.stdout,
        stderr=completed.stderr,
        returncode=completed.returncode,
    )


async def _read_rate_limits_via_worker_async(
    auth_json: dict[str, Any],
    timeout_seconds: float = 15.0,
) -> AppServerRateLimitsResult:
    payload = _worker_payload(auth_json, timeout_seconds=timeout_seconds)
    try:
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "app.codex_app_server_worker",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except OSError as exc:
        raise CodexCLIError(f"Unable to start Codex rate-limit worker: {exc}") from exc

    try:
        stdout, stderr = await asyncio.wait_for(
            process.communicate(json.dumps(payload).encode("utf-8")),
            timeout=max(timeout_seconds + 1.0, timeout_seconds),
        )
    except TimeoutError as exc:
        with contextlib.suppress(ProcessLookupError):
            process.kill()
        await process.wait()
        raise CodexCLIError("Codex rate-limit worker timed out") from exc

    return _parse_worker_result(
        stdout=stdout.decode("utf-8", errors="replace"),
        stderr=stderr.decode("utf-8", errors="replace"),
        returncode=process.returncode,
    )


def _worker_payload(auth_json: dict[str, Any], *, timeout_seconds: float) -> dict[str, Any]:
    return {
        "auth_json": auth_json,
        "timeout_seconds": timeout_seconds,
        "codex_bin": settings.codex_bin,
    }


def _parse_worker_result(
    *,
    stdout: str,
    stderr: str,
    returncode: int,
) -> AppServerRateLimitsResult:
    output = stdout.strip()
    if not output:
        message = stderr.strip() or f"worker exited with code {returncode}"
        raise CodexCLIError(f"Codex rate-limit worker produced no output: {message}")
    try:
        payload = json.loads(output)
    except ValueError as exc:
        raise CodexCLIError(f"Codex rate-limit worker returned invalid JSON: {output}") from exc
    if not isinstance(payload, dict):
        raise CodexCLIError("Codex rate-limit worker response was not a JSON object")
    if not payload.get("ok"):
        error = str(payload.get("error") or stderr.strip() or "unknown worker failure").strip()
        raise CodexCLIError(f"Codex rate-limit worker failed: {error}")
    return AppServerRateLimitsResult(
        account=payload.get("account") if isinstance(payload.get("account"), dict) else None,
        rate_limits=payload.get("rate_limits"),
        notifications=(
            payload.get("notifications")
            if isinstance(payload.get("notifications"), list)
            else []
        ),
    )


def start_login(capture_timeout_seconds: float = 1.2) -> LoginStartResult:
    global _LOGIN_STATE

    if not settings.openai_client_id or not settings.openai_redirect_uri:
        raise CodexCLIError("OPENAI_CLIENT_ID and OPENAI_REDIRECT_URI must be configured")

    flow = build_oauth_authorize_url(
        auth_base_url=settings.openai_auth_url,
        client_id=settings.openai_client_id,
        redirect_uri=settings.openai_redirect_uri,
        scope=settings.openai_scope,
        id_token_add_organizations=settings.openai_id_token_add_organizations,
        codex_cli_simplified_flow=settings.openai_codex_cli_simplified_flow,
        originator=settings.openai_originator
    )

    _LOGIN_STATE = _LoginState(
        started_at=datetime.now(timezone.utc),
        process=None,
        browser_url=flow["authorize_url"],
        output_excerpt=None,
        oauth_state=flow["state"],
        code_verifier=flow["code_verifier"],
        redirect_uri=settings.openai_redirect_uri,
        auth_updated_at=get_active_auth_updated_at(),
    )

    return LoginStartResult(
        started=True,
        pid=None,
        started_at=_LOGIN_STATE.started_at.isoformat(),
        auth_path="db://active-auth",
        browser_url=flow["authorize_url"],
        instructions=(
            "OAuth login is ready. Open the URL, finish sign-in, and relay the callback URL "
            "back to /auth/relay-callback."
        ),
        output_excerpt=None,
    )


def get_login_status() -> LoginStatusResult:
    auth_json = get_active_auth_json()
    auth_exists = isinstance(auth_json, dict)
    auth_path = "db://active-auth"

    if _LOGIN_STATE is None:
        return LoginStatusResult(
            status="idle",
            auth_exists=auth_exists,
            auth_updated=False,
            auth_path=auth_path,
            started_at=None,
            completed_at=None,
            browser_url=None,
            pid=None,
        )

    updated_at = get_active_auth_updated_at()
    updated = bool(updated_at and updated_at != _LOGIN_STATE.auth_updated_at)

    if updated:
        return LoginStatusResult(
            status="complete",
            auth_exists=True,
            auth_updated=True,
            auth_path=auth_path,
            started_at=_LOGIN_STATE.started_at.isoformat(),
            completed_at=updated_at,
            browser_url=_LOGIN_STATE.browser_url,
            pid=None,
        )

    return LoginStatusResult(
        status="pending",
        auth_exists=auth_exists,
        auth_updated=False,
        auth_path=auth_path,
        started_at=_LOGIN_STATE.started_at.isoformat(),
        completed_at=None,
        browser_url=_LOGIN_STATE.browser_url,
        pid=None,
    )


def read_current_auth() -> dict[str, Any]:
    parsed = get_active_auth_json()
    if not isinstance(parsed, dict):
        raise CodexCLIError("Active auth not found in database")
    return parsed


def wait_for_auth_update(timeout_seconds: int = 60, poll_interval_seconds: float = 1.0) -> bool:
    baseline = get_active_auth_updated_at()

    deadline = datetime.now(timezone.utc).timestamp() + timeout_seconds
    while datetime.now(timezone.utc).timestamp() < deadline:
        current = get_active_auth_updated_at()
        if current and current != baseline:
            return True
        # Keep dependencies minimal and avoid busy spinning.
        import time

        time.sleep(poll_interval_seconds)

    return False


def extract_email(auth_json: dict[str, Any]) -> str | None:
    found = _find_first_key(auth_json, EMAIL_KEYS)
    if found:
        return found

    for key in ID_TOKEN_KEYS:
        token = _find_first_key(auth_json, [key])
        if not token:
            continue
        claims = _decode_jwt_payload(token)
        if not claims:
            continue
        claim_email = _find_first_key(claims, EMAIL_KEYS)
        if claim_email:
            return claim_email

    # TODO: If email is absent in auth.json, call an identity endpoint using the
    # bearer access token to resolve the account email.
    return None


def derive_label(email: str, existing_labels: set[str] | None = None) -> str:
    existing = existing_labels or set()
    local_part = email.split("@", 1)[0].strip().lower() if email else ""

    base = NON_ALNUM_RE.sub("-", local_part)
    base = DASH_RE.sub("-", base).strip("-")
    if not base:
        base = "account"

    label = base
    counter = 2
    while label in existing:
        label = f"{base}-{counter}"
        counter += 1

    return label


def relay_callback_to_login(callback_payload: dict[str, Any]) -> dict[str, Any]:
    global _LOGIN_STATE

    if _LOGIN_STATE is not None and _LOGIN_STATE.code_verifier and _LOGIN_STATE.redirect_uri:
        error = callback_payload.get("error")
        if isinstance(error, str) and error.strip():
            return {
                "attempted": True,
                "supported": True,
                "completed": False,
                "message": str(callback_payload.get("error_description") or error).strip(),
            }

        callback_state = str(callback_payload.get("state") or "").strip()
        expected_state = str(_LOGIN_STATE.oauth_state or "").strip()
        if expected_state and callback_state and callback_state != expected_state:
            return {
                "attempted": True,
                "supported": True,
                "completed": False,
                "message": "Callback state did not match the active PKCE login session.",
            }

        code = str(callback_payload.get("code") or "").strip()
        if not code:
            return {
                "attempted": False,
                "supported": True,
                "completed": False,
                "message": "Missing authorization code in callback payload.",
            }

        try:
            token_response = _exchange_code_for_token_sync(
                code=code,
                code_verifier=str(_LOGIN_STATE.code_verifier),
                redirect_uri=str(_LOGIN_STATE.redirect_uri),
            )
            auth_json = build_auth_payload(token_response)
            set_active_auth_json(auth_json)
            return {
                "attempted": True,
                "supported": True,
                "completed": True,
                "message": "Callback exchanged and active auth updated in database.",
                "auth_path": "db://active-auth",
            }
        except Exception as exc:
            return {
                "attempted": True,
                "supported": True,
                "completed": False,
                "message": f"Unable to exchange callback code: {exc}",
            }

    full_url_raw = callback_payload.get("full_url")
    full_url = str(full_url_raw).strip() if isinstance(full_url_raw, str) else ""
    relay_url = full_url or _build_callback_url_from_payload(callback_payload)
    if not relay_url:
        return {
            "attempted": False,
            "supported": True,
            "completed": False,
            "message": "Missing callback URL for relay handoff.",
        }

    parsed = urllib.parse.urlparse(relay_url)
    host = (parsed.hostname or "").strip().lower()
    if host not in {"localhost", "127.0.0.1", "::1"}:
        return {
            "attempted": False,
            "supported": True,
            "completed": False,
            "message": "Relay handoff URL must target localhost.",
            "url": relay_url,
        }

    req = urllib.request.Request(
        relay_url,
        method="GET",
        headers={"User-Agent": "codex-auth-manager/relay"},
    )
    try:
        with urllib.request.urlopen(req, timeout=8.0) as resp:
            status = int(getattr(resp, "status", 200) or 200)
            body = _to_text(resp.read(512))
    except urllib.error.HTTPError as exc:
        body = _to_text(exc.read(512))
        return {
            "attempted": True,
            "supported": True,
            "completed": False,
            "message": f"Local callback listener returned HTTP {exc.code}.",
            "url": relay_url,
            "http_status": int(exc.code),
            "body_excerpt": body,
        }
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        return {
            "attempted": True,
            "supported": True,
            "completed": False,
            "message": f"Unable to deliver callback to local listener: {exc}",
            "url": relay_url,
        }

    success = 200 <= status < 500
    return {
        "attempted": True,
        "supported": True,
        "completed": success,
        "message": (
            "Callback delivered to local Codex listener."
            if success
            else f"Unexpected listener response HTTP {status}."
        ),
        "url": relay_url,
        "http_status": status,
        "body_excerpt": body,
    }


async def relay_callback_to_login_async(callback_payload: dict[str, Any]) -> dict[str, Any]:
    global _LOGIN_STATE

    if _LOGIN_STATE is not None and _LOGIN_STATE.code_verifier and _LOGIN_STATE.redirect_uri:
        error = callback_payload.get("error")
        if isinstance(error, str) and error.strip():
            return {
                "attempted": True,
                "supported": True,
                "completed": False,
                "message": str(callback_payload.get("error_description") or error).strip(),
            }

        callback_state = str(callback_payload.get("state") or "").strip()
        expected_state = str(_LOGIN_STATE.oauth_state or "").strip()
        if expected_state and callback_state and callback_state != expected_state:
            return {
                "attempted": True,
                "supported": True,
                "completed": False,
                "message": "Callback state did not match the active PKCE login session.",
            }

        code = str(callback_payload.get("code") or "").strip()
        if not code:
            return {
                "attempted": False,
                "supported": True,
                "completed": False,
                "message": "Missing authorization code in callback payload.",
            }

        try:
            token_response = await _exchange_code_for_token_async(
                code=code,
                code_verifier=str(_LOGIN_STATE.code_verifier),
                redirect_uri=str(_LOGIN_STATE.redirect_uri),
            )
            auth_json = build_auth_payload(token_response)
            set_active_auth_json(auth_json)
            return {
                "attempted": True,
                "supported": True,
                "completed": True,
                "message": "Callback exchanged and active auth updated in database.",
                "auth_path": "db://active-auth",
            }
        except Exception as exc:
            return {
                "attempted": True,
                "supported": True,
                "completed": False,
                "message": f"Unable to exchange callback code: {exc}",
            }

    return relay_callback_to_login(callback_payload)


def cancel_login() -> bool:
    global _LOGIN_STATE
    if _LOGIN_STATE is None:
        return False

    process = _LOGIN_STATE.process
    _LOGIN_STATE = None
    if process is None:
        return False
    try:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=2)
            return True
    except OSError:
        return False
    return False


def _exchange_code_for_token_sync(
    *,
    code: str,
    code_verifier: str,
    redirect_uri: str,
) -> dict[str, Any]:
    if not settings.openai_token_url or not settings.openai_client_id:
        raise CodexCLIError("OPENAI_TOKEN_URL and OPENAI_CLIENT_ID must be configured")

    data = {
        "grant_type": "authorization_code",
        "client_id": settings.openai_client_id,
        "code": code,
        "code_verifier": code_verifier,
        "redirect_uri": redirect_uri,
    }
    if settings.openai_client_secret:
        data["client_secret"] = settings.openai_client_secret

    body = urllib.parse.urlencode(data).encode("utf-8")
    req = urllib.request.Request(
        settings.openai_token_url,
        data=body,
        method="POST",
        headers={
            "Accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": "codex-auth-manager/native-oauth",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=20.0) as resp:
            payload = _to_text(resp.read())
    except urllib.error.HTTPError as exc:
        detail = _to_text(exc.read())
        raise CodexCLIError(detail or f"Token exchange failed with HTTP {exc.code}") from exc
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise CodexCLIError(f"Unable to reach token endpoint: {exc}") from exc

    try:
        parsed = json.loads(payload)
    except ValueError as exc:
        raise CodexCLIError("Token exchange returned invalid JSON") from exc
    if not isinstance(parsed, dict):
        raise CodexCLIError("Token exchange response was not a JSON object")
    return parsed


async def _exchange_code_for_token_async(
    *,
    code: str,
    code_verifier: str,
    redirect_uri: str,
) -> dict[str, Any]:
    if not settings.openai_token_url or not settings.openai_client_id:
        raise CodexCLIError("OPENAI_TOKEN_URL and OPENAI_CLIENT_ID must be configured")

    data = {
        "grant_type": "authorization_code",
        "client_id": settings.openai_client_id,
        "code": code,
        "code_verifier": code_verifier,
        "redirect_uri": redirect_uri,
    }
    if settings.openai_client_secret:
        data["client_secret"] = settings.openai_client_secret

    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            response = await client.post(
                settings.openai_token_url,
                data=data,
                headers={
                    "Accept": "application/json",
                    "User-Agent": "codex-auth-manager/native-oauth",
                },
            )
    except httpx.HTTPError as exc:
        raise CodexCLIError(f"Unable to reach token endpoint: {exc}") from exc

    if response.status_code >= 400:
        raise CodexCLIError(response.text.strip() or f"Token exchange failed with HTTP {response.status_code}")
    try:
        parsed = response.json()
    except ValueError as exc:
        raise CodexCLIError("Token exchange returned invalid JSON") from exc
    if not isinstance(parsed, dict):
        raise CodexCLIError("Token exchange response was not a JSON object")
    return parsed


def _http_json_request(
    url: str | None,
    *,
    headers: dict[str, str],
    timeout_seconds: float,
) -> Any:
    clean_url = str(url or "").strip()
    if not clean_url:
        raise CodexCLIError("Missing configured API URL")

    req = urllib.request.Request(clean_url, method="GET", headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
            payload = _to_text(resp.read())
    except urllib.error.HTTPError as exc:
        detail = _to_text(exc.read())
        raise CodexCLIError(detail or f"API request failed with HTTP {exc.code}") from exc
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise CodexCLIError(f"Unable to reach API endpoint {clean_url}: {exc}") from exc

    try:
        return json.loads(payload)
    except ValueError as exc:
        raise CodexCLIError(f"API endpoint {clean_url} returned invalid JSON") from exc


async def _http_json_request_async(
    client: httpx.AsyncClient,
    url: str | None,
    *,
    headers: dict[str, str],
) -> Any:
    clean_url = str(url or "").strip()
    if not clean_url:
        raise CodexCLIError("Missing configured API URL")
    try:
        response = await client.get(clean_url, headers=headers)
    except httpx.HTTPError as exc:
        raise CodexCLIError(f"Unable to reach API endpoint {clean_url}: {exc}") from exc
    if response.status_code >= 400:
        raise CodexCLIError(response.text.strip() or f"API request failed with HTTP {response.status_code}")
    try:
        return response.json()
    except ValueError as exc:
        raise CodexCLIError(f"API endpoint {clean_url} returned invalid JSON") from exc


def _find_first_key(payload: Any, keys: list[str]) -> str | None:
    if isinstance(payload, dict):
        for key in keys:
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        for value in payload.values():
            found = _find_first_key(value, keys)
            if found:
                return found
    elif isinstance(payload, list):
        for item in payload:
            found = _find_first_key(item, keys)
            if found:
                return found
    return None


def _decode_jwt_payload(token: str) -> dict[str, Any] | None:
    parts = token.split(".")
    if len(parts) < 2:
        return None
    payload = parts[1]
    padding = "=" * ((4 - (len(payload) % 4)) % 4)
    try:
        decoded = base64.urlsafe_b64decode((payload + padding).encode("ascii"))
        parsed = json.loads(decoded.decode("utf-8", errors="replace"))
    except (ValueError, OSError):
        return None
    if isinstance(parsed, dict):
        return parsed
    return None


def _to_text(value: str | bytes | None) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def _stop_process(process: subprocess.Popen[str]) -> None:
    try:
        if process.stdin is not None:
            process.stdin.close()
    except OSError:
        pass

    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=1.5)
        except subprocess.TimeoutExpired:
            process.kill()
            try:
                process.wait(timeout=1.0)
            except subprocess.TimeoutExpired:
                pass


def _build_callback_url_from_payload(callback_payload: dict[str, Any]) -> str | None:
    base = str(settings.openai_redirect_uri or "").strip()
    if not base:
        return None
    code = callback_payload.get("code")
    state = callback_payload.get("state")
    error = callback_payload.get("error")
    error_description = callback_payload.get("error_description")

    params: dict[str, str] = {}
    if isinstance(code, str) and code.strip():
        params["code"] = code.strip()
    if isinstance(state, str) and state.strip():
        params["state"] = state.strip()
    if isinstance(error, str) and error.strip():
        params["error"] = error.strip()
    if isinstance(error_description, str) and error_description.strip():
        params["error_description"] = error_description.strip()
    if not params:
        return None

    parsed = urllib.parse.urlparse(base)
    existing = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    merged = dict(existing)
    merged.update(params)
    query = urllib.parse.urlencode(merged)
    return urllib.parse.urlunparse(
        (
            parsed.scheme or "http",
            parsed.netloc,
            parsed.path or "/auth/callback",
            parsed.params,
            query,
            parsed.fragment,
        )
    )
