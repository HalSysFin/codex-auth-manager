from __future__ import annotations

import json
from dataclasses import dataclass

from .account_identity import extract_account_identity
from .account_usage_store import (
    get_active_profile_label,
    get_saved_profile,
    list_saved_profiles,
    set_active_profile_label,
    touch_profile_last_used,
    upsert_saved_profile,
)
from .config import settings


@dataclass
class CodexSwitchResult:
    command: list[str]
    returncode: int
    stdout: str
    stderr: str


class CodexSwitchError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        command: list[str],
        exit_code: int | None = None,
        stdout: str = "",
        stderr: str = "",
    ) -> None:
        super().__init__(message)
        self.command = command
        self.exit_code = exit_code
        self.stdout = stdout
        self.stderr = stderr


def save_label(label: str) -> CodexSwitchResult:
    clean = (label or "").strip()
    if not clean:
        raise CodexSwitchError("label is required", command=["internal-db-save", "--label", clean])

    auth_path = settings.codex_auth_file()
    if not auth_path.exists():
        raise CodexSwitchError(
            f"Auth file not found at {auth_path}",
            command=["internal-db-save", "--label", clean],
        )

    try:
        auth_json = json.loads(auth_path.read_text())
    except Exception as exc:
        raise CodexSwitchError(
            f"Unable to read current auth JSON: {exc}",
            command=["internal-db-save", "--label", clean],
        ) from exc

    if not isinstance(auth_json, dict):
        raise CodexSwitchError(
            "Current auth JSON must be an object",
            command=["internal-db-save", "--label", clean],
        )

    identity = extract_account_identity(auth_json)
    upsert_saved_profile(
        label=clean,
        account_key=identity.account_key or clean,
        auth_json=auth_json,
        email=identity.email,
        name=identity.name,
        subject=identity.subject,
        user_id=identity.user_id,
        provider_account_id=identity.account_id,
    )
    touch_profile_last_used(clean)

    return CodexSwitchResult(
        command=["internal-db-save", "--label", clean],
        returncode=0,
        stdout=f"Saved profile '{clean}' in DB",
        stderr="",
    )


def switch_label(label: str) -> CodexSwitchResult:
    clean = (label or "").strip()
    if not clean:
        raise CodexSwitchError("label is required", command=["internal-db-switch", "--label", clean])

    saved = get_saved_profile(clean)
    if saved is None or not isinstance(saved.get("auth_json"), dict):
        raise CodexSwitchError(
            f"Profile '{clean}' not found in DB",
            command=["internal-db-switch", "--label", clean],
        )

    auth_path = settings.codex_auth_file()
    auth_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        auth_path.write_text(json.dumps(saved["auth_json"], indent=2, sort_keys=True))
    except OSError as exc:
        raise CodexSwitchError(
            f"Unable to write active auth file: {exc}",
            command=["internal-db-switch", "--label", clean],
        ) from exc

    set_active_profile_label(clean)
    touch_profile_last_used(clean)
    return CodexSwitchResult(
        command=["internal-db-switch", "--label", clean],
        returncode=0,
        stdout=f"Switched to profile '{clean}' via DB auth materialization",
        stderr="",
    )


def list_labels() -> list[str]:
    rows = list_saved_profiles()
    return [str(row["label"]) for row in rows if str(row.get("label") or "").strip()]


def current_label() -> str | None:
    return get_active_profile_label()
