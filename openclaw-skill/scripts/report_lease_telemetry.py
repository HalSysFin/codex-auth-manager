#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import socket
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib import error, request


def _env(name: str, default: str | None = None) -> str | None:
    value = os.environ.get(name)
    if value is None:
        return default
    stripped = value.strip()
    return stripped or default


def default_base_url() -> str | None:
    return _env("AUTH_MANAGER_BASE_URL")


def default_api_key() -> str | None:
    return _env("AUTH_MANAGER_API_KEY")


def default_machine_id() -> str:
    return _env("AUTH_MANAGER_MACHINE_ID", socket.gethostname()) or socket.gethostname()


def default_agent_id() -> str:
    return _env("AUTH_MANAGER_AGENT_ID", "openclaw") or "openclaw"


def default_lease_id() -> str | None:
    return _env("AUTH_MANAGER_LEASE_ID")


def _read_json_input(path: str | None) -> dict[str, Any]:
    if path:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    return json.load(sys.stdin)


def _to_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        try:
            return int(float(value.strip()))
        except ValueError:
            return None
    return None


def _to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return None
    return None


def _first_present(container: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in container and container.get(key) is not None:
            return container.get(key)
    return None


def normalize_usage_payload(raw: dict[str, Any]) -> dict[str, Any]:
    usage = raw.get("usage") if isinstance(raw.get("usage"), dict) else {}
    metrics = raw.get("metrics") if isinstance(raw.get("metrics"), dict) else {}

    tokens_in = _to_int(
        _first_present(
            raw,
            "tokens_in",
            "prompt_tokens",
            "input_tokens",
        )
    )
    if tokens_in is None:
        tokens_in = _to_int(_first_present(usage, "prompt_tokens", "input_tokens"))
    if tokens_in is None:
        tokens_in = _to_int(_first_present(metrics, "prompt_tokens", "input_tokens"))

    tokens_out = _to_int(
        _first_present(
            raw,
            "tokens_out",
            "completion_tokens",
            "output_tokens",
        )
    )
    if tokens_out is None:
        tokens_out = _to_int(_first_present(usage, "completion_tokens", "output_tokens"))
    if tokens_out is None:
        tokens_out = _to_int(_first_present(metrics, "completion_tokens", "output_tokens"))

    total_tokens = _to_int(_first_present(raw, "total_tokens"))
    if total_tokens is None:
        total_tokens = _to_int(_first_present(usage, "total_tokens"))
    if total_tokens is None:
        total_tokens = _to_int(_first_present(metrics, "total_tokens"))

    if tokens_in is None and total_tokens is not None and tokens_out is not None:
        tokens_in = max(total_tokens - tokens_out, 0)
    if tokens_out is None and total_tokens is not None and tokens_in is not None:
        tokens_out = max(total_tokens - tokens_in, 0)

    requests_count = _to_int(_first_present(raw, "requests_count", "request_count"))
    if requests_count is None:
        requests_count = _to_int(_first_present(metrics, "requests_count", "request_count"))
    if requests_count is None and (tokens_in is not None or tokens_out is not None or total_tokens is not None):
        requests_count = 1

    normalized = {
        "requests_count": requests_count,
        "tokens_in": tokens_in,
        "tokens_out": tokens_out,
        "utilization_pct": _to_float(_first_present(raw, "utilization_pct")),
        "quota_remaining": _to_int(_first_present(raw, "quota_remaining")),
        "rate_limit_remaining": _to_int(_first_present(raw, "rate_limit_remaining")),
        "status": str(_first_present(raw, "status") or "healthy"),
        "last_success_at": _first_present(raw, "last_success_at"),
        "last_error_at": _first_present(raw, "last_error_at"),
        "error_rate_1h": _to_float(_first_present(raw, "error_rate_1h")),
    }

    # Keep safe metadata for later inspection without sending full request/response bodies.
    metadata: dict[str, Any] = {}
    source = raw.get("source")
    if isinstance(source, str) and source.strip():
        metadata["source"] = source.strip()
    model = raw.get("model")
    if isinstance(model, str) and model.strip():
        metadata["model"] = model.strip()
    if total_tokens is not None:
        metadata["total_tokens"] = total_tokens
    if isinstance(raw.get("usage"), dict):
        metadata["usage_keys"] = sorted(str(key) for key in raw["usage"].keys())
    if metadata:
        normalized["metadata"] = metadata
    return normalized


def build_telemetry_body(
    *,
    normalized: dict[str, Any],
    machine_id: str,
    agent_id: str,
    captured_at: str | None = None,
) -> dict[str, Any]:
    body = {
        "machine_id": machine_id,
        "agent_id": agent_id,
        "captured_at": captured_at or datetime.now(timezone.utc).isoformat(),
        "requests_count": normalized.get("requests_count"),
        "tokens_in": normalized.get("tokens_in"),
        "tokens_out": normalized.get("tokens_out"),
        "utilization_pct": normalized.get("utilization_pct"),
        "quota_remaining": normalized.get("quota_remaining"),
        "rate_limit_remaining": normalized.get("rate_limit_remaining"),
        "status": normalized.get("status") or "healthy",
        "last_success_at": normalized.get("last_success_at"),
        "last_error_at": normalized.get("last_error_at"),
        "error_rate_1h": normalized.get("error_rate_1h"),
    }
    if isinstance(normalized.get("metadata"), dict):
        body["metadata"] = normalized["metadata"]
    return body


def post_telemetry(base_url: str, api_key: str, lease_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    url = f"{base_url.rstrip('/')}/api/leases/{lease_id}/telemetry"
    req = request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        raise SystemExit(f"telemetry post failed: HTTP {exc.code}: {raw}") from exc
    except error.URLError as exc:
        raise SystemExit(f"telemetry post failed: {exc.reason}") from exc


def main() -> int:
    parser = argparse.ArgumentParser(description="Normalize observed OpenClaw usage and send lease telemetry to Auth Manager")
    parser.add_argument("--input", help="Path to observed usage JSON. Reads stdin if omitted.")
    parser.add_argument("--lease-id", help="Lease id. Defaults to AUTH_MANAGER_LEASE_ID.")
    parser.add_argument("--base-url", help="Auth Manager base URL. Defaults to AUTH_MANAGER_BASE_URL.")
    parser.add_argument("--api-key", help="Auth Manager API key. Defaults to AUTH_MANAGER_API_KEY.")
    parser.add_argument("--machine-id", help="Machine id. Defaults to AUTH_MANAGER_MACHINE_ID or hostname.")
    parser.add_argument("--agent-id", help="Agent id. Defaults to AUTH_MANAGER_AGENT_ID or openclaw.")
    parser.add_argument("--captured-at", help="Override captured_at timestamp.")
    parser.add_argument("--dry-run", action="store_true", help="Print normalized telemetry payload instead of POSTing it.")
    parser.add_argument("--print-defaults", action="store_true", help="Print resolved defaults and exit.")
    args = parser.parse_args()

    base_url = args.base_url or default_base_url()
    api_key = args.api_key or default_api_key()
    lease_id = args.lease_id or default_lease_id()
    machine_id = args.machine_id or default_machine_id()
    agent_id = args.agent_id or default_agent_id()

    if args.print_defaults:
        print(
            json.dumps(
                {
                    "base_url": base_url,
                    "api_key_env": "AUTH_MANAGER_API_KEY",
                    "lease_id": lease_id,
                    "machine_id": machine_id,
                    "agent_id": agent_id,
                },
                indent=2,
            )
        )
        return 0

    raw = _read_json_input(args.input)
    normalized = normalize_usage_payload(raw)
    body = build_telemetry_body(
        normalized=normalized,
        machine_id=machine_id,
        agent_id=agent_id,
        captured_at=args.captured_at,
    )

    if args.dry_run:
        print(json.dumps(body, indent=2))
        return 0

    if not base_url:
        raise SystemExit("AUTH_MANAGER_BASE_URL or --base-url is required")
    if not api_key:
        raise SystemExit("AUTH_MANAGER_API_KEY or --api-key is required")
    if not lease_id:
        raise SystemExit("AUTH_MANAGER_LEASE_ID or --lease-id is required")

    result = post_telemetry(base_url=base_url, api_key=api_key, lease_id=lease_id, payload=body)
    safe = {
        "status": result.get("status"),
        "reason": result.get("reason"),
        "lease_id": lease_id,
        "machine_id": machine_id,
        "agent_id": agent_id,
        "requests_count": body.get("requests_count"),
        "tokens_in": body.get("tokens_in"),
        "tokens_out": body.get("tokens_out"),
        "utilization_pct": body.get("utilization_pct"),
        "quota_remaining": body.get("quota_remaining"),
        "captured_at": body.get("captured_at"),
    }
    print(json.dumps(safe, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
