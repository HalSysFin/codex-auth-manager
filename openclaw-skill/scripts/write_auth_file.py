#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import socket
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict


def expand_auth_path(path: str | None) -> Path:
    raw = path or os.environ.get("CODEX_AUTH_PATH") or "~/.codex/auth.json"
    return Path(os.path.expanduser(raw)).resolve()


def default_machine_id() -> str:
    return os.environ.get("AUTH_MANAGER_MACHINE_ID") or socket.gethostname()


def default_agent_id() -> str:
    return os.environ.get("AUTH_MANAGER_AGENT_ID") or "openclaw"


def validate_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("payload must be a JSON object")
    tokens = payload.get("tokens")
    if not isinstance(tokens, dict):
        raise ValueError("payload.tokens must be an object")
    required = ["id_token", "access_token", "refresh_token", "account_id"]
    missing = [k for k in required if not tokens.get(k)]
    if missing:
        raise ValueError(f"missing token fields: {', '.join(missing)}")
    payload.setdefault("auth_mode", "oauth")
    payload.setdefault("OPENAI_API_KEY", None)
    payload.setdefault("last_refresh", datetime.now(timezone.utc).isoformat())
    return payload


def atomic_write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=path.name + ".", dir=str(path.parent))
    tmp_path = Path(tmp)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2)
            fh.write("\n")
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate and atomically write Codex auth payload")
    parser.add_argument("--input", help="Path to input JSON payload. Reads stdin if omitted.")
    parser.add_argument("--output", help="Destination auth file path. Defaults to CODEX_AUTH_PATH or ~/.codex/auth.json")
    parser.add_argument("--print-defaults", action="store_true", help="Print default runtime config and exit")
    args = parser.parse_args()

    if args.print_defaults:
        print(json.dumps({
            "base_url_env": "AUTH_MANAGER_BASE_URL",
            "api_key_env": "AUTH_MANAGER_API_KEY",
            "machine_id": default_machine_id(),
            "agent_id": default_agent_id(),
            "auth_path": str(expand_auth_path(args.output)),
        }, indent=2))
        return 0

    if args.input:
        payload = json.loads(Path(args.input).read_text(encoding="utf-8"))
    else:
        payload = json.load(sys.stdin)

    payload = validate_payload(payload)
    out = expand_auth_path(args.output)
    atomic_write_json(out, payload)
    print(json.dumps({
        "ok": True,
        "auth_path": str(out),
        "machine_id": default_machine_id(),
        "agent_id": default_agent_id(),
        "last_refresh": payload["last_refresh"],
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
