# Auth Manager Contract Reference

## Defaults

- Base URL: `AUTH_MANAGER_BASE_URL`
- API key: `AUTH_MANAGER_API_KEY`
- Machine ID: `AUTH_MANAGER_MACHINE_ID` or hostname
- Agent ID: `AUTH_MANAGER_AGENT_ID` or `openclaw`
- Auth path: `CODEX_AUTH_PATH` or `~/.codex/auth.json`

## Headers

Use bearer auth:

```http
Authorization: Bearer <AUTH_MANAGER_API_KEY>
Content-Type: application/json
```

## Expected endpoints

### Acquire

`POST /api/leases/acquire`

Suggested body:

```json
{
  "machine_id": "<machine_id>",
  "agent_id": "<agent_id>",
  "reason": "openclaw ensure lease"
}
```

### Get lease

`GET /api/leases/{lease_id}`

Expected useful fields:

- `lease_id`
- `credential_id`
- `state`
- `issued_at`
- `expires_at`
- `latest_utilization_pct`
- `latest_quota_remaining`
- `replacement_required`
- `rotation_recommended`
- `reason`

### Renew

`POST /api/leases/{lease_id}/renew`

Suggested body:

```json
{
  "machine_id": "<machine_id>",
  "agent_id": "<agent_id>"
}
```

### Release

`POST /api/leases/{lease_id}/release`

Suggested body:

```json
{
  "machine_id": "<machine_id>",
  "agent_id": "<agent_id>",
  "reason": "openclaw release lease"
}
```

### Rotate

`POST /api/leases/rotate`

Suggested body:

```json
{
  "lease_id": "<lease_id>",
  "machine_id": "<machine_id>",
  "agent_id": "<agent_id>",
  "reason": "expiry_approaching"
}
```

Allowed reasons:

- `approaching_utilization_threshold`
- `low_quota_remaining`
- `unhealthy_credential`
- `expiry_approaching`
- `admin_requested_rotation`

### Materialize auth payload

`POST /api/leases/{lease_id}/materialize`

Suggested body:

```json
{
  "machine_id": "<machine_id>",
  "agent_id": "<agent_id>"
}
```

Expected response body is a wrapper object. The Codex auth payload lives at `credential_material.auth_json`:

```json
{
  "status": "ok",
  "reason": null,
  "lease": {
    "id": "lease_...",
    "credential_id": "sub:auth0|...",
    "machine_id": "openclaw-host",
    "agent_id": "openclaw"
  },
  "credential_material": {
    "label": "profile-label",
    "account_key": "sub:auth0|...",
    "email": "user@example.com",
    "name": "User Name",
    "provider_account_id": "provider-id",
    "auth_json": {
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
  }
}
```

### Telemetry

`POST /api/leases/{lease_id}/telemetry`

Use only truthful, observed values. Suitable fields:

```json
{
  "machine_id": "<machine_id>",
  "agent_id": "<agent_id>",
  "captured_at": "<iso8601>",
  "status": "healthy",
  "requests_count": 1,
  "tokens_in": 1234,
  "tokens_out": 456,
  "last_success_at": "<iso8601 or null>",
  "last_error_at": null,
  "utilization_pct": 42.0,
  "quota_remaining": 12345
}
```

Common observed usage inputs that can be normalized before posting:

OpenAI-style response usage:

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

Direct counters:

```json
{
  "requests_count": 1,
  "tokens_in": 1234,
  "tokens_out": 456,
  "status": "healthy"
}
```

The helper script `scripts/report_lease_telemetry.py` can normalize those shapes and POST them to `/api/leases/{lease_id}/telemetry`.

## Lease handling rules

- `revoked`: current lease is unusable, rotate or reacquire immediately
- `expired`: current lease is unusable, rotate or reacquire immediately
- `replacement_required=true`: rotate or reacquire immediately
- `rotation_recommended=true`: rotate if policy or user intent says to do so
- no eligible credentials: fail clearly and do not claim success

## Verification after auth switch

Minimum success criteria:

1. auth payload validates
2. auth file writes atomically
3. auth file can be read back successfully
4. lease state re-check succeeds after the write
