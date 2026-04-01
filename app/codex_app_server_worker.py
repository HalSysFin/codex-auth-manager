from __future__ import annotations

import contextlib
import json
import os
import selectors
import subprocess
import sys
import tempfile
import time
from pathlib import Path
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
    stop_when_ids: set[int],
) -> tuple[list[dict[str, Any]], list[str]]:
    if process.stdout is None or process.stderr is None:
        raise RuntimeError("codex app-server stdio is unavailable")

    selector = selectors.DefaultSelector()
    selector.register(process.stdout, selectors.EVENT_READ, "stdout")
    selector.register(process.stderr, selectors.EVENT_READ, "stderr")

    messages: list[dict[str, Any]] = []
    stderr_lines: list[str] = []
    pending_ids = set(stop_when_ids)
    deadline = time.monotonic() + timeout_seconds

    try:
        while time.monotonic() < deadline and pending_ids:
            events = selector.select(
                timeout=min(0.25, max(0.0, deadline - time.monotonic()))
            )
            if not events:
                if process.poll() is not None and pending_ids:
                    break
                continue
            for key, _ in events:
                line = key.fileobj.readline()
                if not line:
                    continue
                text = line.strip()
                if not text:
                    continue
                if key.data == "stderr":
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
                    if isinstance(message_id, int):
                        pending_ids.discard(message_id)
    finally:
        with contextlib.suppress(Exception):
            selector.unregister(process.stdout)
        with contextlib.suppress(Exception):
            selector.unregister(process.stderr)
        selector.close()

    return messages, stderr_lines


def _find_result(messages: list[dict[str, Any]], message_id: int) -> Any:
    for message in messages:
        if message.get("id") != message_id:
            continue
        if "error" in message:
            raise RuntimeError(str(message["error"]))
        return message.get("result")
    raise RuntimeError(f"Missing JSON-RPC response for id={message_id}")


def _write_auth_json(root: Path, auth_json: dict[str, Any]) -> Path:
    codex_home = root / ".codex"
    codex_home.mkdir(parents=True, exist_ok=True)
    auth_path = codex_home / "auth.json"
    auth_path.write_text(json.dumps(auth_json, indent=2) + "\n", encoding="utf-8")
    return auth_path


def _run_probe(auth_json: dict[str, Any], *, codex_bin: str, timeout_seconds: float) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="auth-manager-codex-worker-") as temp_dir:
        temp_root = Path(temp_dir)
        auth_path = _write_auth_json(temp_root, auth_json)
        env = os.environ.copy()
        env["HOME"] = temp_dir
        env["CODEX_HOME"] = str(auth_path.parent)

        process = subprocess.Popen(
            [codex_bin, "app-server", "--listen", "stdio://"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            env=env,
        )

        try:
            _send(
                process,
                {
                    "id": 1,
                    "method": "initialize",
                    "params": {
                        "clientInfo": {
                            "name": "auth_manager_worker",
                            "title": "Auth Manager Worker",
                            "version": "0.1.0",
                        }
                    },
                },
            )
            init_messages, init_stderr = _read_messages(
                process,
                timeout_seconds=timeout_seconds,
                stop_when_ids={1},
            )
            _find_result(init_messages, 1)
            _send(process, {"method": "initialized", "params": {}})

            _send(process, {"id": 2, "method": "account/read", "params": {"refreshToken": True}})
            _send(process, {"id": 3, "method": "account/rateLimits/read", "params": {}})
            rpc_messages, rpc_stderr = _read_messages(
                process,
                timeout_seconds=timeout_seconds,
                stop_when_ids={2, 3},
            )

            return {
                "ok": True,
                "account": _find_result(rpc_messages, 2),
                "rate_limits": _find_result(rpc_messages, 3),
                "notifications": [],
                "stderr": init_stderr + rpc_stderr,
            }
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
                with contextlib.suppress(Exception):
                    process.kill()


def main() -> int:
    try:
        payload = json.load(sys.stdin)
        if not isinstance(payload, dict):
            raise ValueError("Worker input must be a JSON object")
        auth_json = payload.get("auth_json")
        if not isinstance(auth_json, dict):
            raise ValueError("auth_json is required")
        codex_bin = str(payload.get("codex_bin") or "codex").strip() or "codex"
        timeout_seconds = float(payload.get("timeout_seconds") or 15.0)
        result = _run_probe(auth_json, codex_bin=codex_bin, timeout_seconds=timeout_seconds)
        print(json.dumps(result))
        return 0
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc)}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
