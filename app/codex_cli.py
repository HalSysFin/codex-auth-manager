from __future__ import annotations

import base64
import json
import re
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import settings

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

URL_RE = re.compile(r"https?://[^\s\"'>]+")
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
    before_mtime: float | None
    process: subprocess.Popen[str] | None
    browser_url: str | None
    output_excerpt: str | None


_LOGIN_STATE: _LoginState | None = None


def read_rate_limits_via_app_server(timeout_seconds: float = 15.0) -> AppServerRateLimitsResult:
    cmd = [settings.codex_cli_bin, "app-server", "--listen", "stdio://"]
    try:
        process = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
    except FileNotFoundError as exc:
        raise CodexCLIError(f"codex CLI binary not found: {settings.codex_cli_bin}") from exc
    except OSError as exc:
        raise CodexCLIError(f"Unable to start codex app-server: {exc}") from exc

    responses: dict[int, dict[str, Any]] = {}
    notifications: list[dict[str, Any]] = []
    deadline = time.monotonic() + timeout_seconds

    try:
        _rpc_send(
            process,
            {
                "id": 1,
                "method": "initialize",
                "params": {
                    "clientInfo": {
                        "name": "auth_manager",
                        "title": "Codex Auth Manager",
                        "version": "0.2.0",
                    }
                },
            },
        )
        _rpc_send(process, {"method": "initialized", "params": {}})
        _rpc_send(
            process,
            {"id": 2, "method": "account/read", "params": {"refreshToken": True}},
        )
        _rpc_send(
            process,
            {"id": 3, "method": "account/rateLimits/read", "params": {}},
        )

        while time.monotonic() < deadline:
            if 2 in responses and 3 in responses:
                break

            line = _readline_with_timeout(process, timeout_seconds=0.25)
            if line is None:
                continue
            raw = line.strip()
            if not raw:
                continue

            try:
                message = json.loads(raw)
            except ValueError:
                continue
            if not isinstance(message, dict):
                continue

            message_id = message.get("id")
            if isinstance(message_id, int):
                responses[message_id] = message
                continue

            method = message.get("method")
            if isinstance(method, str):
                notifications.append(message)

        if 2 not in responses or 3 not in responses:
            stderr_excerpt = _drain_stderr(process, max_lines=20)
            detail = (
                f"; stderr: {' | '.join(stderr_excerpt)}"
                if stderr_excerpt
                else ""
            )
            raise CodexCLIError(
                "Timed out waiting for codex app-server account/rate-limit responses"
                + detail
            )

        account_response = responses[2]
        rate_limit_response = responses[3]

        if "error" in account_response:
            raise CodexCLIError(_rpc_error_text("account/read", account_response["error"]))
        if "error" in rate_limit_response:
            raise CodexCLIError(
                _rpc_error_text("account/rateLimits/read", rate_limit_response["error"])
            )

        account_result = account_response.get("result")
        account = None
        if isinstance(account_result, dict):
            maybe_account = account_result.get("account")
            if isinstance(maybe_account, dict):
                account = maybe_account

        rate_limits = rate_limit_response.get("result")
        return AppServerRateLimitsResult(
            account=account,
            rate_limits=rate_limits,
            notifications=notifications,
        )
    finally:
        _stop_process(process)


def start_login(capture_timeout_seconds: float = 1.2) -> LoginStartResult:
    global _LOGIN_STATE

    auth_path = settings.codex_auth_file()
    before_mtime = _mtime(auth_path)

    cmd = [settings.codex_cli_bin, "login"]
    try:
        process = subprocess.Popen(
            cmd,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    except FileNotFoundError as exc:
        raise CodexCLIError(f"codex CLI binary not found: {settings.codex_cli_bin}") from exc
    except OSError as exc:
        raise CodexCLIError(f"Unable to start codex login: {exc}") from exc

    out = ""
    err = ""
    try:
        out, err = process.communicate(timeout=capture_timeout_seconds)
        out = _to_text(out)
        err = _to_text(err)
        # Process ended quickly; still valid for non-interactive setups.
    except subprocess.TimeoutExpired as exc:
        out = _to_text(exc.stdout)
        err = _to_text(exc.stderr)

    combined = "\n".join(part for part in [out, err] if part).strip()
    browser_url = _extract_first_url(combined)

    _LOGIN_STATE = _LoginState(
        started_at=datetime.now(timezone.utc),
        before_mtime=before_mtime,
        process=process,
        browser_url=browser_url,
        output_excerpt=(combined[:1000] if combined else None),
    )

    instructions = (
        "Codex login started. Complete the browser/device login flow if prompted. "
        "Then call /auth/import-current."
    )
    if process.poll() is not None and process.returncode not in (0, None):
        instructions = "codex login exited early. Check /auth/login/status for details."

    return LoginStartResult(
        started=True,
        pid=process.pid,
        started_at=_LOGIN_STATE.started_at.isoformat(),
        auth_path=str(auth_path),
        browser_url=browser_url,
        instructions=instructions,
        output_excerpt=_LOGIN_STATE.output_excerpt,
    )


def get_login_status() -> LoginStatusResult:
    auth_path = settings.codex_auth_file()
    auth_exists = auth_path.exists()

    if _LOGIN_STATE is None:
        return LoginStatusResult(
            status="idle",
            auth_exists=auth_exists,
            auth_updated=False,
            auth_path=str(auth_path),
            started_at=None,
            completed_at=None,
            browser_url=None,
            pid=None,
        )

    updated = _has_auth_updated(_LOGIN_STATE.before_mtime, auth_path)
    process = _LOGIN_STATE.process

    if updated:
        return LoginStatusResult(
            status="complete",
            auth_exists=True,
            auth_updated=True,
            auth_path=str(auth_path),
            started_at=_LOGIN_STATE.started_at.isoformat(),
            completed_at=datetime.now(timezone.utc).isoformat(),
            browser_url=_LOGIN_STATE.browser_url,
            pid=process.pid if process else None,
        )

    if process is not None:
        rc = process.poll()
        if rc is None:
            return LoginStatusResult(
                status="pending",
                auth_exists=auth_exists,
                auth_updated=False,
                auth_path=str(auth_path),
                started_at=_LOGIN_STATE.started_at.isoformat(),
                completed_at=None,
                browser_url=_LOGIN_STATE.browser_url,
                pid=process.pid,
            )
        if rc == 0:
            return LoginStatusResult(
                status="pending",
                auth_exists=auth_exists,
                auth_updated=False,
                auth_path=str(auth_path),
                started_at=_LOGIN_STATE.started_at.isoformat(),
                completed_at=None,
                browser_url=_LOGIN_STATE.browser_url,
                pid=process.pid,
                error="codex login exited but auth.json has not changed yet",
            )
        return LoginStatusResult(
            status="failed",
            auth_exists=auth_exists,
            auth_updated=False,
            auth_path=str(auth_path),
            started_at=_LOGIN_STATE.started_at.isoformat(),
            completed_at=datetime.now(timezone.utc).isoformat(),
            browser_url=_LOGIN_STATE.browser_url,
            pid=process.pid,
            error=f"codex login exited with code {rc}",
        )

    return LoginStatusResult(
        status="pending",
        auth_exists=auth_exists,
        auth_updated=False,
        auth_path=str(auth_path),
        started_at=_LOGIN_STATE.started_at.isoformat(),
        completed_at=None,
        browser_url=_LOGIN_STATE.browser_url,
        pid=None,
    )


def read_current_auth() -> dict[str, Any]:
    auth_path = settings.codex_auth_file()
    if not auth_path.exists():
        raise CodexCLIError(f"Auth file not found at {auth_path}")

    try:
        raw = auth_path.read_text()
        parsed = json.loads(raw)
    except OSError as exc:
        raise CodexCLIError(f"Unable to read auth file: {exc}") from exc
    except ValueError as exc:
        raise CodexCLIError("Auth file is not valid JSON") from exc

    if not isinstance(parsed, dict):
        raise CodexCLIError("Auth file JSON root must be an object")

    return parsed


def wait_for_auth_update(timeout_seconds: int = 60, poll_interval_seconds: float = 1.0) -> bool:
    auth_path = settings.codex_auth_file()
    baseline = _mtime(auth_path)

    deadline = datetime.now(timezone.utc).timestamp() + timeout_seconds
    while datetime.now(timezone.utc).timestamp() < deadline:
        if _has_auth_updated(baseline, auth_path):
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
    # TODO: Wire relayed callback params (code/state/error) into codex CLI once
    # the CLI supports direct callback injection in a stable way.
    _ = callback_payload
    return {
        "attempted": False,
        "supported": False,
        "completed": False,
        "message": "Direct Codex CLI callback handoff is not implemented yet.",
    }


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


def _extract_first_url(text: str) -> str | None:
    if not text:
        return None
    urls = [_clean_url(match.group(0)) for match in URL_RE.finditer(text)]
    urls = [url for url in urls if url]
    if not urls:
        return None

    # Prefer the actual authorization URL over local callback/listener URLs.
    for url in urls:
        if "auth.openai.com/oauth/authorize" in url:
            return url

    return urls[0]


def _to_text(value: str | bytes | None) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def _rpc_send(process: subprocess.Popen[str], message: dict[str, Any]) -> None:
    if process.stdin is None:
        raise CodexCLIError("codex app-server stdin is unavailable")
    try:
        process.stdin.write(json.dumps(message) + "\n")
        process.stdin.flush()
    except OSError as exc:
        raise CodexCLIError(f"Failed writing to codex app-server: {exc}") from exc


def _readline_with_timeout(
    process: subprocess.Popen[str], timeout_seconds: float
) -> str | None:
    if process.stdout is None:
        return None
    end = time.monotonic() + timeout_seconds
    while time.monotonic() < end:
        line = process.stdout.readline()
        if line:
            return line
        if process.poll() is not None:
            return None
        time.sleep(0.02)
    return None


def _rpc_error_text(method: str, error_obj: Any) -> str:
    if isinstance(error_obj, dict):
        message = error_obj.get("message")
        code = error_obj.get("code")
        if message is not None and code is not None:
            return f"{method} failed ({code}): {message}"
        if message is not None:
            return f"{method} failed: {message}"
    return f"{method} failed"


def _drain_stderr(process: subprocess.Popen[str], max_lines: int = 20) -> list[str]:
    if process.stderr is None:
        return []
    lines: list[str] = []
    try:
        for _ in range(max_lines):
            line = process.stderr.readline()
            if not line:
                break
            value = line.strip()
            if value:
                lines.append(value)
    except OSError:
        return lines
    return lines


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


def _clean_url(url: str) -> str:
    return url.rstrip(".,);]")


def _mtime(path: Path) -> float | None:
    try:
        return path.stat().st_mtime
    except OSError:
        return None


def _has_auth_updated(before_mtime: float | None, auth_path: Path) -> bool:
    if not auth_path.exists():
        return False
    if before_mtime is None:
        return True
    current_mtime = _mtime(auth_path)
    return current_mtime is not None and current_mtime > before_mtime
