# Codex Auth Manager (FastAPI)

Small FastAPI service that captures OAuth callbacks, persists Codex auth into `codex-switch`, and shows remaining rate limits per account.

## Requirements

- Python 3.10+
- `codex-switch` installed and on your `PATH`

## Setup

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

## Run

```bash
uvicorn app.main:app --reload --port 8080
```

Open the dashboard at `http://localhost:8080/`.

## Endpoints

- `GET /health`
- `GET /` — dashboard UI (rate limits per account)
- `GET /ui` — alias for the dashboard UI
- `GET /api/accounts` — returns connected accounts + rate limit info (Bearer token required if `INTERNAL_API_TOKEN` is set)
- `GET /oauth/callback` — captures query params and stores them to the callback store
- `POST /oauth/callback` — captures JSON payload; if it includes `label` + `auth_json`, it saves immediately
- `GET /auth/callback` — alias for `/oauth/callback` (matches Codex redirect)
- `POST /auth/callback` — alias for `/oauth/callback`
- `POST /auth/exchange` — exchanges `code` + `code_verifier` for tokens and optionally saves
- `POST /auth/save` — persists `auth_json` to `~/.codex/auth.json` and runs `codex-switch save --label <label>`
- `GET /internal/auths` — returns stored auth JSON (Bearer token required if `INTERNAL_API_TOKEN` is set)

## Dashboard auth

If `INTERNAL_API_TOKEN` is set, the dashboard will prompt for a Bearer token. The token is stored in `localStorage` as `internalToken` for convenience.

## Example payload

```json
{
  "label": "work",
  "auth_json": {
    "access_token": "...",
    "refresh_token": "...",
    "expires_at": 1730000000
  }
}
```

## Token exchange

`/auth/exchange` expects:

```json
{
  "code": "ac_...",
  "code_verifier": "....",
  "label": "work"
}
```

If `label` is provided, the token response is written to `~/.codex/auth.json` and then saved via `codex-switch save --label <label>`.

## Environment

Create a `.env` file if you want to override defaults:

```
CODEX_SWITCH_BIN=codex-switch
CODEX_AUTH_PATH=~/.codex/auth.json
CALLBACK_STORE_DIR=~/.codex-switch/callbacks
CODEX_PROFILES_DIR=~/.codex-switch/profiles
INTERNAL_API_TOKEN=
RATE_LIMIT_PROBE_URL=https://api.openai.com/v1/models
OPENAI_ORGANIZATION=
OPENAI_PROJECT=
OPENAI_TOKEN_URL=
OPENAI_CLIENT_ID=
OPENAI_CLIENT_SECRET=
OPENAI_REDIRECT_URI=http://localhost:1455/auth/callback
```
