from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .codex_switch import CodexSwitchResult
from .codex_switch import save_label
from .config import settings


class AuthStoreError(RuntimeError):
    pass


def write_auth_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        path.write_text(json.dumps(payload, indent=2, sort_keys=True))
    except OSError as exc:
        raise AuthStoreError(f"Unable to write auth JSON to {path}: {exc}") from exc


def persist_current_auth(payload: Any) -> Path:
    auth_path = settings.codex_auth_file()
    write_auth_json(auth_path, payload)
    return auth_path


def save_current_auth_under_label(label: str) -> CodexSwitchResult:
    return save_label(label)


def persist_and_save_label(label: str, payload: Any) -> Path:
    auth_path = persist_current_auth(payload)
    save_current_auth_under_label(label)
    return auth_path
