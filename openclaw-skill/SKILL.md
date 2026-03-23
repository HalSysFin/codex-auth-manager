---
name: openclaw-codex-lease
description: broker-managed codex auth lease handling for openclaw. use when chatgpt needs to acquire, inspect, renew, rotate, or repair an auth manager lease for openclaw or codex, materialize the leased auth payload, write ~/.codex/auth.json, report lease consumption or status, and react to revoked, expired, or replacement-required lease states using an auth manager api key and broker endpoints.
---

# OpenClaw Codex Lease

## Overview

Use this skill to manage a Codex auth lease for OpenClaw through Auth Manager.
This skill assumes these defaults unless the user says otherwise:

- Auth Manager base URL comes from `AUTH_MANAGER_BASE_URL`
- API key comes from `AUTH_MANAGER_API_KEY`
- `machine_id` defaults to hostname
- `agent_id` defaults to `openclaw`
- auth file path defaults to `~/.codex/auth.json`
- auto-rotate is enabled when the lease is `revoked`, `expired`, or `replacement_required=true`
- success means the auth file was written and lease status was re-checked successfully

## Workflow

### 1. Gather runtime configuration

Read these environment variables first:

- `AUTH_MANAGER_BASE_URL`
- `AUTH_MANAGER_API_KEY`
- `AUTH_MANAGER_MACHINE_ID` (optional)
- `AUTH_MANAGER_AGENT_ID` (optional)
- `CODEX_AUTH_PATH` (optional)

If machine or agent IDs are missing, derive them with the helper script.

### 2. Inspect current lease state

Call Auth Manager to inspect the current lease or acquire one if none exists.
Use the bearer token header:

- `Authorization: Bearer <AUTH_MANAGER_API_KEY>`

Important states to handle exactly like the other clients:

- `active`: keep using it
- `rotation_required`: rotate or reacquire
- `revoked`: treat as unusable immediately, request replacement
- `expired`: treat as unusable immediately, request replacement
- `replacement_required=true`: rotate or reacquire
- backend unavailable: stop and report clearly
- no eligible credentials available: stop and report clearly

### 3. Acquire, rotate, or repair lease

Use these broker endpoints when available:

- `POST /api/leases/acquire`
- `GET /api/leases/{lease_id}`
- `POST /api/leases/{lease_id}/renew`
- `POST /api/leases/{lease_id}/release`
- `POST /api/leases/rotate`
- `POST /api/leases/{lease_id}/telemetry`
- `POST /api/leases/{lease_id}/materialize`

Preferred behavior:

1. If there is no stored lease, acquire one.
2. If the stored lease is missing, revoked, expired, or replacement-required, rotate or reacquire.
3. If the lease is near expiry, renew it.
4. Always send `machine_id` and `agent_id` in renew, release, telemetry, rotate, and materialize requests.
5. After successful acquire or rotate, call `materialize` to fetch the auth payload wrapper and extract `credential_material.auth_json`.

### 4. Write Codex auth file safely

Write the materialized payload to `~/.codex/auth.json` or `CODEX_AUTH_PATH`.
Use the helper script in `scripts/write_auth_file.py` for validation and atomic write behavior.
Important: pass only `credential_material.auth_json` into the writer, not the full materialize response body.

Expected payload shape:

```json
{
  "auth_mode": "string",
  "OPENAI_API_KEY": null,
  "tokens": {
    "id_token": "string",
    "access_token": "string",
    "refresh_token": "string",
    "account_id": "string"
  },
  "last_refresh": "string"
}
```

Never log raw tokens.

### 5. Verify the switch

After writing the auth file:

1. confirm the file exists and parses as valid JSON
2. confirm required token fields are present
3. re-check lease status from Auth Manager
4. report the current lease summary back to the user

### 6. Report telemetry back to Auth Manager

The skill may send truthful, minimal telemetry back to Auth Manager so OpenClaw can stay lease-aware.
Only send data the local client can actually observe.

Good telemetry fields:

- `machine_id`
- `agent_id`
- `captured_at`
- `status`
- `last_success_at`
- `last_error_at`
- `utilization_pct` if known from broker status
- `quota_remaining` if known from broker status

If OpenClaw exposes real token usage, send it too:

- `requests_count`
- `tokens_in`
- `tokens_out`

Do not invent request counts or token counts.
Use `scripts/report_lease_telemetry.py` to normalize common OpenClaw/OpenAI usage shapes and post them safely.

## Quick commands

### Ensure lease and auth file

Use this when the user wants OpenClaw repaired or prepared to run:

```bash
python scripts/write_auth_file.py --print-defaults
```

Then:

1. resolve config
2. acquire or repair lease
3. materialize payload
4. write auth file
5. re-check lease state
6. optionally post telemetry

### Report token telemetry

When OpenClaw returns a truthful usage object, pass it to the telemetry helper and let it post lease telemetry:

```bash
python openclaw-skill/scripts/report_lease_telemetry.py --lease-id "$AUTH_MANAGER_LEASE_ID" --input usage.json
```

The helper accepts common shapes like:

```json
{
  "usage": {
    "prompt_tokens": 1234,
    "completion_tokens": 456,
    "total_tokens": 1690
  },
  "model": "gpt-5.4",
  "status": "healthy"
}
```

or direct fields like:

```json
{
  "requests_count": 1,
  "tokens_in": 1234,
  "tokens_out": 456,
  "status": "healthy"
}
```

The helper will map those into Auth Manager lease telemetry and preserve only safe metadata like `model`, `source`, and `total_tokens`.

### Show lease status

Summarize:

- lease id
- credential id
- state
- expires at
- utilization percent
- quota remaining
- replacement required
- rotation recommended
- last auth write time

### Rotate lease

When the broker reports `revoked`, `expired`, or `replacement_required=true`, request rotation first if the endpoint supports it. If that fails, reacquire.
Use only supported broker rotation reasons:

- `approaching_utilization_threshold`
- `low_quota_remaining`
- `unhealthy_credential`
- `expiry_approaching`
- `admin_requested_rotation`

## Tooling guidance

- Prefer shell or Python for HTTP calls and auth file writes.
- Prefer the helper script instead of hand-writing JSON logic repeatedly.
- Keep failure messages explicit and operational.
- If Auth Manager returns no eligible credentials, do not pretend the repair succeeded.

## Resources

- `references/auth_manager_contract.md`: endpoint and behavior reference
- `scripts/write_auth_file.py`: validate and atomically write auth payloads
- `scripts/report_lease_telemetry.py`: normalize observed OpenClaw usage and post token telemetry
