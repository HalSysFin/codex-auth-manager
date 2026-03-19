from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .config import settings


@dataclass
class AccountProfile:
    label: str
    path: Path
    auth: dict[str, Any]
    access_token: str | None = None
    email: str | None = None


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
        email = _find_first_key(auth, EMAIL_KEYS)
        profiles.append(
            AccountProfile(
                label=label,
                path=path,
                auth=auth,
                access_token=access_token,
                email=email,
            )
        )

    return profiles
