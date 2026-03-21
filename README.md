# Codex Auth Manager (FastAPI + React)

Central orchestration service for Codex auth profiles.

Primary architecture:
- Codex CLI performs login and writes `CODEX_AUTH_PATH` (default `/root/.codex/auth.json`)
- `codex-switch` stores/switches labeled profiles in `CODEX_PROFILES_DIR`
- FastAPI backend owns auth/account persistence APIs
- React + TypeScript + Vite frontend (`frontend/`) consumes backend APIs
- Chrome extension relays localhost OAuth callbacks to auth-manager when login runs remotely

Legacy callback/exchange routes are still available, but the primary happy path is now Codex CLI driven.

## Requirements

- Python 3.10+
- `codex` CLI installed and on `PATH` (or set `CODEX_CLI_BIN`)
- `codex-switch` installed and on `PATH` (or set `CODEX_SWITCH_BIN`)

## Setup

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

## Run (Local)

```bash
uvicorn app.main:app --reload --port 8080
```

Open `http://localhost:8080/`.

Frontend dev server:

```bash
cd frontend
npm install
npm run dev
```

## Primary Flow

1. Click **Add account** (or `POST /auth/login/start`) to launch Codex CLI login.
2. Codex CLI produces/updates `CODEX_AUTH_PATH` (default `~/.codex/auth.json`).
3. Callback is relayed to the local Codex listener (`POST /auth/relay-callback`).
4. Once auth finalizes and `auth.json` is updated, auth-manager auto-persists to the matched profile.
5. If no matching profile exists, import/create is available via `POST /auth/import-current`.
6. Switch later via UI/API with `POST /auth/switch`.

## Chrome Extension Relay Flow

Why this exists:
- Codex CLI runs on the management server, not the user machine.
- OAuth redirect is fixed to `http://127.0.0.1:1445/...` or `http://localhost:1445/...`.
- We cannot run a local listener/service on user machines.
- The extension intercepts the localhost callback in the browser and relays it to auth-manager.

Relay sequence:
1. Extension calls `POST /auth/login/start` on auth-manager.
2. auth-manager starts Codex CLI login and returns `auth_url`, `session_id`, `relay_token`, and expiry.
3. Extension opens `auth_url`.
4. OAuth provider redirects browser to localhost callback URL.
5. Extension intercepts localhost navigation and posts callback params to `POST /auth/relay-callback`.
6. auth-manager stores callback for the login session and exposes state via `GET /auth/login/status`.

Current Codex CLI handoff note:
- Relay callback handoff to localhost listener is implemented in `app/codex_cli.py` (`relay_callback_to_login`).
- Callback receipt alone does not persist; persistence happens only after finalized auth is detected.

## Endpoints

Core:
- `GET /health`
- `GET /` and `GET /ui` frontend app shell (React build)
- `GET /api/accounts` fast cached snapshot (profiles + SQLite usage; no blocking live probes)
- `GET /api/accounts/cached` same cached snapshot endpoint
- `GET /api/accounts/stream` SSE live refresh stream (`snapshot`, `account_update`, `aggregate_update`, `complete`, `error`)
- `GET /api/usage/aggregate` aggregated cached usage summary
- `GET /auth/rate-limits` read active Codex session ChatGPT rate limits via `codex app-server`
- `POST /auth/login/start` start Codex CLI login
- `GET /auth/login/status` login status (`wait_seconds` and optional `session_id`)
- `POST /auth/relay-callback` receive relayed localhost callback from extension
- `POST /auth/import-current` import current auth.json and save label
- `POST /auth/switch` switch active profile via `codex-switch`
- `GET /auth/current` metadata for active auth file and guessed/current label
- `GET /auth/export?label=<label>` return stored auth JSON for a label

Legacy/secondary:
- `GET/POST /oauth/callback`
- `GET/POST /auth/callback`
- `POST /auth/save`
- `POST /auth/exchange`
- `GET /internal/auths`

## Internal Token Protection

When `INTERNAL_API_TOKEN` is set, sensitive endpoints require `Authorization: Bearer <token>`.
This includes endpoints that expose raw auth JSON or mutate active auth state, including:
- `/auth/rate-limits`
- `/auth/export`
- `/auth/import-current`
- `/auth/switch`
- `/auth/save`
- `/auth/exchange`
- `/internal/auths`
- `/auth/login/start`

The React frontend stores the API token in `localStorage` key `auth_manager_api_key` for convenience.

## Public Login Gate (Proxy + Internal Bypass)

Use this when the app is internet-facing through Nginx Proxy Manager (or another reverse proxy):

- Public traffic must log in with username/password.
- Internal network traffic bypasses web login.
- `X-Forwarded-For` is trusted only when request source IP matches `TRUSTED_PROXY_IPS`.

Required env vars to enable:
- `WEB_LOGIN_USERNAME`
- `WEB_LOGIN_PASSWORD`
- `WEB_LOGIN_SESSION_SECRET`

Related env vars:
- `TRUSTED_PROXY_IPS` (comma-separated proxy IPs, e.g. your NPM container/host IP)
- `INTERNAL_NETWORK_CIDRS` (comma-separated CIDRs allowed without login)
- `WEB_LOGIN_COOKIE_NAME`
- `WEB_LOGIN_SESSION_TTL_SECONDS`

## Environment

Example `.env` values:

```env
CODEX_CLI_BIN=codex
CODEX_SWITCH_BIN=codex-switch
CODEX_AUTH_PATH=/root/.codex/auth.json
CALLBACK_STORE_DIR=/root/.codex-switch/callbacks
CODEX_PROFILES_DIR=/root/.codex-switch/profiles
USAGE_DB_PATH=/root/.codex-switch/auth-manager.sqlite3
LOGIN_SESSION_TTL_SECONDS=600
WEB_LOGIN_USERNAME=
WEB_LOGIN_PASSWORD=
WEB_LOGIN_SESSION_SECRET=
WEB_LOGIN_COOKIE_NAME=auth_manager_session
WEB_LOGIN_SESSION_TTL_SECONDS=43200
TRUSTED_PROXY_IPS=
INTERNAL_NETWORK_CIDRS=127.0.0.1/32,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16,::1/128,fc00::/7
INTERNAL_API_TOKEN=
RATE_LIMIT_PROBE_URL=https://api.openai.com/v1/models
OPENAI_ORGANIZATION=
OPENAI_PROJECT=
OPENAI_TOKEN_URL=
OPENAI_CLIENT_ID=
OPENAI_CLIENT_SECRET=
OPENAI_REDIRECT_URI=http://localhost:1455/auth/callback
```

## Docker

Build and run:

```bash
docker compose up --build
```

Frontend dev server in stack:

- `auth-manager` API/UI container: `http://localhost:8080`
- `frontend` Vite dev container: `http://localhost:5173`

The Vite service proxies `/api`, `/auth`, `/health`, and `/internal` to `auth-manager:8080`.

Container defaults:
- `CODEX_AUTH_PATH=/root/.codex/auth.json`
- `CALLBACK_STORE_DIR=/root/.codex-switch/callbacks`
- `CODEX_PROFILES_DIR=/root/.codex-switch/profiles`
- `USAGE_DB_PATH=/root/.codex-switch/auth-manager.sqlite3`

Persistent named volumes:
- `/root/.codex`
- `/root/.codex-switch`

No host Codex bind mounts are required; container state is isolated by default.

Note: Dockerfile assumes Codex CLI and `codex-switch` can be installed via npm (`@openai/codex` and `codex-switch`). Override build arg `CODEX_INSTALL_CMD` if your install command differs.

## Chrome Extension Dev Setup

Extension files are in `chrome-extension/`:
- `manifest.json`
- `background.js`
- `popup.html` / `popup.js`
- `options.html` / `options.js`
- `success.html`
- `error.html`

To load unpacked in Chrome:
1. Open `chrome://extensions`.
2. Enable **Developer mode**.
3. Click **Load unpacked**.
4. Select the `chrome-extension/` folder from this repo.
5. Open extension settings (Details -> Extension options) and set:
   - Auth Manager Base URL
   - Internal API Bearer Token (only if `INTERNAL_API_TOKEN` is enabled server-side)
6. Click the extension action icon to open the popup.
7. Click **Start Relay Login** in the popup.

Shortcuts:
- Open popup (browser action): `Ctrl+Shift+Y` (macOS: `Command+Shift+Y`)
- Start relay login directly: `Ctrl+Shift+L` (macOS: `Command+Shift+L`)
- Customize shortcuts at `chrome://extensions/shortcuts`.
UI load behavior:
- First paint uses cached DB-backed snapshot (`/api/accounts/cached`) for fast render.
- Live account usage refresh runs asynchronously via `/api/accounts/stream`.
- SQLite remains the source of last-known state; live probes only improve freshness.
