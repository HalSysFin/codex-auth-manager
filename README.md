# Codex Auth Manager

Auth profile manager for Codex CLI with:
- FastAPI backend
- React + Vite frontend
- Postgres as canonical persistence
- Single active `auth.json` materialized on disk

## Current Architecture

- **Canonical storage**: Postgres (`saved_profiles`, usage tables, snapshots, metadata).
- **Runtime auth file**: only one active auth at `CODEX_AUTH_PATH` (default `/root/.codex/auth.json`).
- **Switching**: internal DB-backed switching.
- **Login/relay**: Codex CLI login start + callback relay into auth-manager.
- **UI loading**: cached-first snapshot, then async SSE refresh.

## Main Flows

### Add Account
1. UI calls `POST /auth/login/start-relay`.
2. Auth URL opens in browser.
3. User pastes callback URL in Add Account modal.
4. UI sends callback to `POST /auth/relay-callback`.
5. Auth finalization/persistence updates saved profile in DB.

### Import Auth
- UI button **Import Auth** opens modal.
- Paste JSON or upload `.json` auth file.
- UI calls `POST /auth/import-json`.
- Auth is matched/saved into DB profiles.

### Switch Account
- UI calls `POST /auth/switch`.
- Backend loads auth JSON from DB by label.
- Backend writes active `CODEX_AUTH_PATH` and updates active label in DB.

## Key Endpoints

- `GET /health`
- `GET /api/public-stats`
- `GET /api/session/status`
- `GET /api/accounts`
- `GET /api/accounts/cached`
- `GET /api/accounts/stream` (SSE)
- `GET /api/usage/aggregate`
- `GET /api/usage/history?range=7d|30d|90d|all`
- `GET /api/accounts/{label}/history?range=7d|30d|90d|all`
- `GET /auth/current`
- `GET /auth/rate-limits`
- `POST /auth/login/start`
- `POST /auth/login/start-relay`
- `GET /auth/login/status`
- `POST /auth/relay-callback`
- `POST /auth/import-current`
- `POST /auth/import-json`
- `POST /auth/switch`
- `POST /auth/rename`
- `POST /auth/delete`
- `GET /auth/export?label=<label>`

## Environment

Use `.env.example` as your template.

Core runtime:
- `CODEX_CLI_BIN`
- `CODEX_AUTH_PATH`
- `CALLBACK_STORE_DIR`
- `CODEX_PROFILES_DIR`
- `USAGE_DB_PATH`
- `DATABASE_URL`
- `AUTH_ENCRYPTION_KEY` (optional)

Postgres container:
- `POSTGRES_DB`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`

Web login/session:
- `WEB_LOGIN_USERNAME`
- `WEB_LOGIN_PASSWORD`
- `WEB_LOGIN_SESSION_SECRET`
- `WEB_LOGIN_COOKIE_NAME`
- `WEB_LOGIN_SESSION_TTL_SECONDS`

API/auth control:
- `INTERNAL_API_TOKEN`
- `LOGIN_SESSION_TTL_SECONDS`
- `TRUSTED_PROXY_IPS`
- `INTERNAL_NETWORK_CIDRS`

OpenAI/OAuth settings (optional/flow-dependent):
- `OPENAI_ORGANIZATION`
- `OPENAI_PROJECT`
- `OPENAI_TOKEN_URL`
- `OPENAI_CLIENT_ID`
- `OPENAI_CLIENT_SECRET`
- `OPENAI_REDIRECT_URI`

Frontend:
- `VITE_API_BASE_URL`

## Local Run

Backend:
```bash
uvicorn app.main:app --reload --port 8080
```

Frontend dev:
```bash
cd frontend
npm install
npm run dev
```

## Docker Run

```bash
docker compose up --build
```

Services:
- API/backend: `http://localhost:8080`
- Frontend dev server: `http://localhost:5173`

## Chrome Extension

The extension is in `chrome-extension/` and relays localhost OAuth callbacks back to auth-manager.

### Install (Unpacked)
1. Open `chrome://extensions`.
2. Enable **Developer mode**.
3. Click **Load unpacked**.
4. Select this repo's `chrome-extension/` folder.

### Configure
1. Open extension **Settings** (Options page).
2. Set **Auth Manager Base URL** (for example `https://your-domain` or `http://localhost:8080`).
3. If your backend uses `INTERNAL_API_TOKEN`, set **Internal API Bearer Token**.
4. Save settings.

### Use
1. Open the extension popup.
2. Click **Start Relay Login**.
3. Complete login in the opened auth tab.
4. Extension captures localhost callback and posts it to `/auth/relay-callback`.
5. Back in Auth Manager UI, paste callback URL in **Add Account** modal when prompted.

### Important: Localhost Port Conflicts (1445/1455)

If VS Code (or any other local tool) is using localhost auth callback ports like `1445` or `1455`, relay auth can fail intermittently.

Typical symptoms:
- auth appears to hang
- callback tab returns a localhost callback URL but nothing happens
- callback is captured but profile is not finalized automatically

To avoid first-run issues:
- close/disable local tools that may intercept localhost callback ports (especially VS Code auth integrations)
- retry **Start Relay Login** after those tools are stopped

If callback was returned but not processed:
- copy the full callback URL
- paste it into the **Add Account** modal in the app
- submit it so auth-manager can relay/finalize it

### Shortcuts
- Open popup: `Ctrl+Shift+Y` (`Command+Shift+Y` on macOS)
- Start relay login: `Ctrl+Shift+L` (`Command+Shift+L` on macOS)

## Notes

- DB is source of truth; active auth file is materialized for runtime integration.
- Cached-first UI means page loads from persisted state first, then refreshes live.
