from __future__ import annotations

import secrets
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from threading import Lock
from typing import Any


@dataclass
class LoginSession:
    session_id: str
    relay_token: str
    auth_url: str | None
    created_at: datetime
    expires_at: datetime
    callback_payload: dict[str, Any] | None = None
    callback_received_at: datetime | None = None
    provider_error: str | None = None
    provider_error_description: str | None = None
    relay_used: bool = False
    notes: list[str] = field(default_factory=list)


_LOCK = Lock()
_SESSIONS: dict[str, LoginSession] = {}
_LATEST_SESSION_ID: str | None = None


def create_login_session(auth_url: str | None, ttl_seconds: int) -> LoginSession:
    global _LATEST_SESSION_ID

    now = datetime.now(timezone.utc)
    session = LoginSession(
        session_id=secrets.token_urlsafe(18),
        relay_token=secrets.token_urlsafe(24),
        auth_url=auth_url,
        created_at=now,
        expires_at=now + timedelta(seconds=ttl_seconds),
    )

    with _LOCK:
        _cleanup_expired_locked(now)
        _SESSIONS[session.session_id] = session
        _LATEST_SESSION_ID = session.session_id

    return session


def get_login_session(session_id: str) -> LoginSession | None:
    with _LOCK:
        session = _SESSIONS.get(session_id)
        if not session:
            return None
        if _is_expired(session, datetime.now(timezone.utc)):
            _SESSIONS.pop(session_id, None)
            return None
        return session


def get_latest_session() -> LoginSession | None:
    with _LOCK:
        if _LATEST_SESSION_ID is None:
            return None
        session = _SESSIONS.get(_LATEST_SESSION_ID)
        if not session:
            return None
        if _is_expired(session, datetime.now(timezone.utc)):
            _SESSIONS.pop(session.session_id, None)
            return None
        return session


def cancel_login_session(session_id: str | None = None) -> bool:
    global _LATEST_SESSION_ID
    with _LOCK:
        if session_id:
            removed = _SESSIONS.pop(session_id, None)
            if _LATEST_SESSION_ID == session_id:
                _LATEST_SESSION_ID = None
            return removed is not None

        if _LATEST_SESSION_ID is None:
            return False
        removed = _SESSIONS.pop(_LATEST_SESSION_ID, None)
        _LATEST_SESSION_ID = None
        return removed is not None


def validate_relay_token(session: LoginSession, token: str) -> bool:
    return secrets.compare_digest(session.relay_token, token)


def mark_relay_callback(
    session_id: str,
    callback_payload: dict[str, Any],
    *,
    provider_error: str | None,
    provider_error_description: str | None,
) -> LoginSession | None:
    now = datetime.now(timezone.utc)
    with _LOCK:
        session = _SESSIONS.get(session_id)
        if not session:
            return None
        if _is_expired(session, now):
            _SESSIONS.pop(session_id, None)
            return None

        if session.relay_used and session.callback_payload == callback_payload:
            return session
        if session.relay_used and session.callback_payload != callback_payload:
            return None

        session.callback_payload = callback_payload
        session.callback_received_at = now
        session.provider_error = provider_error
        session.provider_error_description = provider_error_description
        session.relay_used = True
        return session


def session_state(
    session: LoginSession | None,
    *,
    auth_updated: bool,
    cli_failed: bool,
    cli_error: str | None,
    cli_status: str | None,
) -> tuple[str, str | None]:
    if session is None and cli_status == "idle":
        return "idle", None
    if session is not None and session.provider_error:
        return "failed", session.provider_error_description or session.provider_error
    if cli_failed:
        return "failed", cli_error
    if auth_updated:
        return "complete", None
    if session is not None and session.callback_received_at is not None:
        return "callback_received", None
    return "pending", None


def to_public_session(session: LoginSession, include_relay_token: bool = False) -> dict[str, Any]:
    payload = {
        "session_id": session.session_id,
        "auth_url": session.auth_url,
        "created_at": session.created_at.isoformat(),
        "expires_at": session.expires_at.isoformat(),
        "expires_in_seconds": max(
            int((session.expires_at - datetime.now(timezone.utc)).total_seconds()),
            0,
        ),
        "callback_received": session.callback_received_at is not None,
        "callback_received_at": session.callback_received_at.isoformat()
        if session.callback_received_at
        else None,
        "relay_used": session.relay_used,
        "provider_error": session.provider_error,
        "provider_error_description": session.provider_error_description,
    }
    if include_relay_token:
        payload["relay_token"] = session.relay_token
    return payload


def _is_expired(session: LoginSession, now: datetime) -> bool:
    return now >= session.expires_at


def _cleanup_expired_locked(now: datetime) -> None:
    expired = [sid for sid, session in _SESSIONS.items() if _is_expired(session, now)]
    for sid in expired:
        _SESSIONS.pop(sid, None)
