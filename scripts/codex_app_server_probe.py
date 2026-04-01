#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import selectors
import subprocess
import sys
import time
from typing import Any


def _send(process: subprocess.Popen[str], payload: dict[str, Any]) -> None:
    if process.stdin is None:
        raise RuntimeError("codex app-server stdin is unavailable")
    process.stdin.write(json.dumps(payload) + "\n")
    process.stdin.flush()


def _read_messages(
    process: subprocess.Popen[str],
    *,
    timeout_seconds: float,
    stop_when_ids: set[int] | None = None,
) -> tuple[list[dict[str, Any]], list[str]]:
    if process.stdout is None or process.stderr is None:
        raise RuntimeError("codex app-server stdio is unavailable")

    selector = selectors.DefaultSelector()
    selector.register(process.stdout, selectors.EVENT_READ, "stdout")
    selector.register(process.stderr, selectors.EVENT_READ, "stderr")

    messages: list[dict[str, Any]] = []
    stderr_lines: list[str] = []
    pending_ids = set(stop_when_ids or set())
    deadline = time.monotonic() + timeout_seconds

    while time.monotonic() < deadline:
        if pending_ids == set():
            break
        events = selector.select(timeout=min(0.25, max(0.0, deadline - time.monotonic())))
        if not events:
            if process.poll() is not None and not pending_ids:
                break
            continue
        for key, _ in events:
            stream_name = key.data
            line = key.fileobj.readline()
            if not line:
                continue
            text = line.strip()
            if not text:
                continue
            if stream_name == "stderr":
                stderr_lines.append(text)
                continue
            try:
                payload = json.loads(text)
            except ValueError:
                stderr_lines.append(f"non-json stdout: {text}")
                continue
            if isinstance(payload, dict):
                messages.append(payload)
                message_id = payload.get("id")
                if isinstance(message_id, int) and message_id in pending_ids:
                    pending_ids.remove(message_id)

    try:
        selector.unregister(process.stdout)
        selector.unregister(process.stderr)
    except Exception:
        pass
    selector.close()
    return messages, stderr_lines


def main() -> int:
    parser = argparse.ArgumentParser(description="Probe a local codex app-server over stdio JSON-RPC")
    parser.add_argument("--timeout", type=float, default=10.0, help="Per-read timeout in seconds")
    parser.add_argument("--codex-bin", default="codex", help="Path to the codex binary")
    parser.add_argument("--account", action="store_true", help="Call account/read after initialize")
    parser.add_argument("--rate-limits", action="store_true", help="Call account/rateLimits/read after initialize")
    args = parser.parse_args()

    methods: list[tuple[int, str, dict[str, Any]]] = []
    if args.account:
        methods.append((2, "account/read", {"refreshToken": True}))
    if args.rate_limits:
        methods.append((3, "account/rateLimits/read", {}))

    process = subprocess.Popen(
        [args.codex_bin, "app-server", "--listen", "stdio://"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )

    try:
        _send(
            process,
            {
                "id": 1,
                "method": "initialize",
                "params": {
                    "clientInfo": {
                        "name": "auth_manager_probe",
                        "title": "Auth Manager Probe",
                        "version": "0.1.0",
                    }
                },
            },
        )
        init_messages, init_stderr = _read_messages(
            process,
            timeout_seconds=args.timeout,
            stop_when_ids={1},
        )
        _send(process, {"method": "initialized", "params": {}})

        rpc_messages: list[dict[str, Any]] = []
        rpc_stderr: list[str] = []
        for message_id, method, params in methods:
            _send(process, {"id": message_id, "method": method, "params": params})
        if methods:
            ids = {message_id for message_id, _, _ in methods}
            rpc_messages, rpc_stderr = _read_messages(
                process,
                timeout_seconds=args.timeout,
                stop_when_ids=ids,
            )

        output = {
            "ok": True,
            "pid": process.pid,
            "initialize": init_messages,
            "messages": rpc_messages,
            "stderr": init_stderr + rpc_stderr,
        }
        print(json.dumps(output, indent=2))
        return 0
    finally:
        try:
            if process.stdin is not None:
                process.stdin.close()
        except Exception:
            pass
        try:
            process.terminate()
            process.wait(timeout=2)
        except Exception:
            try:
                process.kill()
            except Exception:
                pass


if __name__ == "__main__":
    raise SystemExit(main())
