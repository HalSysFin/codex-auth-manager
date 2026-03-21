from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .account_usage_store import get_accounts_by_ids
from .config import settings


@dataclass
class AccountProfile:
    label: str
    path: Path
    auth: dict[str, Any]
    access_token: str | None = None
    email: str | None = None
    rate_limit_window_type: str | None = None
    usage_limit: int | None = None
    usage_in_window: int | None = None
    rate_limit_refresh_at: str | None = None
    rate_limit_last_refreshed_at: str | None = None
    last_usage_sync_at: str | None = None
    lifetime_used: int | None = None
    usage_created_at: str | None = None
    usage_updated_at: str | None = None


TOKEN_KEYS = [
    "access_token",
    "accessToken",
    "token",
    "api_key",
    "apiKey",
]

EMAIL_KEYS = [
    "email",
    "user_email",
    "userEmail",
]

ID_TOKEN_KEYS = [
    "id_token",
    "idToken",
]


def _load_json(path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return None


def _find_first_key(payload: Any, keys: list[str]) -> str | None:
    if isinstance(payload, dict):
        for key in keys:
            if key in payload and isinstance(payload[key], str):
                return payload[key]
        for value in payload.values():
            found = _find_first_key(value, keys)
            if found:
                return found
    elif isinstance(payload, list):
        for value in payload:
            found = _find_first_key(value, keys)
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


def _extract_email_from_jwt_claims(payload: dict[str, Any]) -> str | None:
    direct = _find_first_key(payload, EMAIL_KEYS)
    if direct:
        return direct
    profile = payload.get("https://api.openai.com/profile")
    if isinstance(profile, dict):
        prof = _find_first_key(profile, EMAIL_KEYS)
        if prof:
            return prof
    return None


def _extract_email(payload: dict[str, Any]) -> str | None:
    direct = _find_first_key(payload, EMAIL_KEYS)
    if direct:
        return direct

    for key in ID_TOKEN_KEYS:
        token = _find_first_key(payload, [key])
        if not token:
            continue
        claims = _decode_jwt_payload(token)
        if not claims:
            continue
        email = _extract_email_from_jwt_claims(claims)
        if email:
            return email
    return None


def list_profiles() -> list[AccountProfile]:
    profiles_dir = settings.profiles_dir()
    if not profiles_dir.exists():
        return []

    profiles: list[AccountProfile] = []
    for path in sorted(profiles_dir.iterdir()):
        if not path.is_file():
            continue
        auth = _load_json(path)
        if not isinstance(auth, dict):
            continue

        label = path.stem
        access_token = _find_first_key(auth, TOKEN_KEYS)
        email = _extract_email(auth)
        profiles.append(
            AccountProfile(
                label=label,
                path=path,
                auth=auth,
                access_token=access_token,
                email=email,
            )
        )

    usage_by_id: dict[str, Any] = {}
    try:
        usage_by_id = get_accounts_by_ids([profile.label for profile in profiles])
    except Exception:
        usage_by_id = {}

    for profile in profiles:
        usage = usage_by_id.get(profile.label)
        if not usage:
            continue
        profile.rate_limit_window_type = usage.rate_limit_window_type
        profile.usage_limit = usage.usage_limit
        profile.usage_in_window = usage.usage_in_window
        profile.rate_limit_refresh_at = usage.rate_limit_refresh_at
        profile.rate_limit_last_refreshed_at = usage.rate_limit_last_refreshed_at
        profile.last_usage_sync_at = usage.last_usage_sync_at
        profile.lifetime_used = usage.lifetime_used
        profile.usage_created_at = usage.created_at
        profile.usage_updated_at = usage.updated_at

    return profiles
