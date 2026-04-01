from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .account_identity import extract_account_identity
from .account_usage_store import (
    get_active_auth_json,
    get_active_profile_label,
    get_saved_profile,
    list_saved_profiles,
    set_active_auth_json,
    set_active_profile_label,
    touch_profile_last_used,
    upsert_saved_profile,
)


class AuthStoreError(RuntimeError):
    pass


@dataclass
class AuthStoreSwitchResult:
    command: list[str]
    returncode: int
    stdout: str
    stderr: str


class AuthStoreSwitchError(AuthStoreError):
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


def write_auth_json(path: Path, payload: Any) -> None:
    if not isinstance(payload, dict):
        raise AuthStoreError("Active auth payload must be a JSON object")
    try:
        set_active_auth_json(payload)
    except Exception as exc:
        raise AuthStoreError(f"Unable to write active auth to database: {exc}") from exc


def persist_current_auth(payload: Any) -> Path:
    write_auth_json(Path("db://active-auth"), payload)
    return Path("db://active-auth")


def save_current_auth_under_label(label: str) -> AuthStoreSwitchResult:
    clean = (label or "").strip()
    if not clean:
        raise AuthStoreSwitchError(
            "label is required",
            command=["internal-db-save", "--label", clean],
        )

    auth_json = get_active_auth_json()
    if not isinstance(auth_json, dict):
        raise AuthStoreSwitchError(
            "Active auth not found in database",
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
        reauth_required=False,
        reauth_reason=None,
    )
    touch_profile_last_used(clean)

    return AuthStoreSwitchResult(
        command=["internal-db-save", "--label", clean],
        returncode=0,
        stdout=f"Saved profile '{clean}' in DB",
        stderr="",
    )


def switch_active_auth_to_label(label: str) -> AuthStoreSwitchResult:
    clean = (label or "").strip()
    if not clean:
        raise AuthStoreSwitchError(
            "label is required",
            command=["internal-db-switch", "--label", clean],
        )

    saved = get_saved_profile(clean)
    if saved is None or not isinstance(saved.get("auth_json"), dict):
        raise AuthStoreSwitchError(
            f"Profile '{clean}' not found in DB",
            command=["internal-db-switch", "--label", clean],
        )

    try:
        set_active_auth_json(saved["auth_json"])
    except Exception as exc:
        raise AuthStoreSwitchError(
            f"Unable to write active auth to database: {exc}",
            command=["internal-db-switch", "--label", clean],
        ) from exc

    set_active_profile_label(clean)
    touch_profile_last_used(clean)
    return AuthStoreSwitchResult(
        command=["internal-db-switch", "--label", clean],
        returncode=0,
        stdout=f"Switched to profile '{clean}' via DB active auth",
        stderr="",
    )


def list_auth_labels() -> list[str]:
    rows = list_saved_profiles()
    return [str(row["label"]) for row in rows if str(row.get("label") or "").strip()]


def get_current_auth_label() -> str | None:
    return get_active_profile_label()


def persist_and_save_label(label: str, payload: Any) -> Path:
    auth_path = persist_current_auth(payload)
    save_current_auth_under_label(label)
    return auth_path
