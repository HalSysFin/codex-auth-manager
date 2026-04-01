from __future__ import annotations

import base64
import hashlib
import json
import os
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlencode


def base64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def build_oauth_authorize_url(
    *,
    auth_base_url: str,
    client_id: str,
    redirect_uri: str,
    scope: str,
    originator: str,
    id_token_add_organizations: bool,
    codex_cli_simplified_flow: bool,
) -> dict[str, str]:
    state = base64url(os.urandom(32))
    code_verifier = base64url(os.urandom(32))
    code_challenge = base64url(hashlib.sha256(code_verifier.encode("ascii")).digest())

    params = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "scope": scope,
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
        "state": state,
        "originator": originator,
        "id_token_add_organizations": "true" if id_token_add_organizations else "false",
        "codex_cli_simplified_flow": "true" if codex_cli_simplified_flow else "false",
    }
    return {
        "state": state,
        "code_verifier": code_verifier,
        "code_challenge": code_challenge,
        "authorize_url": f"{auth_base_url}?{urlencode(params)}",
    }


def decode_jwt_payload(token: str | None) -> dict[str, Any] | None:
    if not isinstance(token, str) or not token.strip():
        return None
    parts = token.split(".")
    if len(parts) < 2:
        return None
    payload = parts[1]
    padding = "=" * (-len(payload) % 4)
    try:
        decoded = base64.urlsafe_b64decode(payload + padding)
        data = json.loads(decoded.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return None
    return data if isinstance(data, dict) else None


def extract_account_id(token_response: dict[str, Any], existing_auth: dict[str, Any] | None = None) -> str | None:
    direct = token_response.get("account_id")
    if isinstance(direct, str) and direct.strip():
        return direct.strip()

    if isinstance(existing_auth, dict):
        tokens = existing_auth.get("tokens")
        existing_account_id = tokens.get("account_id") if isinstance(tokens, dict) else None
        if isinstance(existing_account_id, str) and existing_account_id.strip():
            return existing_account_id.strip()

    for token_key in ("id_token", "access_token"):
        claims = decode_jwt_payload(token_response.get(token_key))
        if not isinstance(claims, dict):
            continue
        auth_claim = claims.get("https://api.openai.com/auth")
        if isinstance(auth_claim, dict):
            account_id = auth_claim.get("chatgpt_account_id")
            if isinstance(account_id, str) and account_id.strip():
                return account_id.strip()
    return None


def _token_expiry_iso(token: str | None) -> str | None:
    claims = decode_jwt_payload(token)
    if not isinstance(claims, dict):
        return None
    exp = claims.get("exp")
    if not isinstance(exp, (int, float)) or exp <= 0:
        return None
    return datetime.fromtimestamp(int(exp), tz=timezone.utc).isoformat()


def extract_auth_db_metadata(auth_json: dict[str, Any]) -> dict[str, Any]:
    tokens = auth_json.get("tokens") if isinstance(auth_json, dict) else None
    access_token = tokens.get("access_token") if isinstance(tokens, dict) else None
    id_token = tokens.get("id_token") if isinstance(tokens, dict) else None
    refresh_token = tokens.get("refresh_token") if isinstance(tokens, dict) else None
    return {
        "access_token_expires_at": _token_expiry_iso(access_token),
        "id_token_expires_at": _token_expiry_iso(id_token),
        "refresh_token_expires_at": _token_expiry_iso(refresh_token),
        "last_refresh_at": auth_json.get("last_refresh") if isinstance(auth_json.get("last_refresh"), str) else None,
        "refresh_token_present": bool(isinstance(refresh_token, str) and refresh_token.strip()),
    }


def build_auth_payload(
    token_response: dict[str, Any],
    *,
    existing_auth: dict[str, Any] | None = None,
) -> dict[str, Any]:
    access_token = token_response.get("access_token")
    refresh_token = token_response.get("refresh_token")
    id_token = token_response.get("id_token")

    existing_tokens = existing_auth.get("tokens") if isinstance(existing_auth, dict) else None
    if not isinstance(existing_tokens, dict):
        existing_tokens = {}

    access_out = access_token.strip() if isinstance(access_token, str) and access_token.strip() else existing_tokens.get("access_token")
    refresh_out = refresh_token.strip() if isinstance(refresh_token, str) and refresh_token.strip() else existing_tokens.get("refresh_token")
    id_out = id_token.strip() if isinstance(id_token, str) and id_token.strip() else existing_tokens.get("id_token")
    account_id = extract_account_id(token_response, existing_auth=existing_auth)

    required: dict[str, str | None] = {
        "access_token": access_out if isinstance(access_out, str) else None,
        "refresh_token": refresh_out if isinstance(refresh_out, str) else None,
        "id_token": id_out if isinstance(id_out, str) else None,
        "account_id": account_id,
    }
    missing = [key for key, value in required.items() if not value]
    if missing:
        raise ValueError(
            "Missing required token values: "
            + ", ".join(missing)
            + ". Inspect the token response before writing auth.json."
        )

    now_iso = datetime.now(timezone.utc).isoformat()
    payload = {
        "auth_mode": "chatgpt",
        "OPENAI_API_KEY": None,
        "tokens": {
            "access_token": required["access_token"],
            "refresh_token": required["refresh_token"],
            "id_token": required["id_token"],
            "account_id": required["account_id"],
        },
        "last_refresh": now_iso,
    }

    if isinstance(existing_auth, dict):
        for key, value in existing_auth.items():
            payload.setdefault(key, value)

    return payload
