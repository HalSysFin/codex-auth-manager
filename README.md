# Codex Auth Manager

Auth profile manager for Codex CLI with:
- FastAPI backend
- React + Vite frontend
- Postgres as canonical persistence
- Single active `auth.json` materialized on disk

## Repo Components

This repo now contains several clients around the same Auth Manager backend and lease broker:

- `app/`
  FastAPI backend, broker APIs, account management, analytics, callback relay, and persistence logic.
- `frontend/`
  React + Vite web dashboard for account management, usage analytics, and broker visibility.
- `chrome-extension/`
  Chrome extension for localhost callback capture and relay into Auth Manager during browser-based login flows.
- `vscode-extension/`
  VS Code extension that acquires broker leases, materializes auth into `~/.codex/auth.json`, and keeps the lease healthy inside VS Code.
- `desktop-app/`
  Tauri desktop app for Linux/Windows that performs the same broker-backed auth lifecycle without depending on VS Code.
- `headless-client/`
  Linux-friendly non-GUI CLI/agent that can ensure a lease, materialize auth, and run a background lease-maintenance loop.
- `openclaw-skill/`
  Operational skill and helper scripts for managing OpenClaw Codex leases, auth materialization, and broker telemetry workflows.
- `openclaw-plugin/`
  Lightweight TypeScript runtime helper for capturing observed OpenClaw token usage and posting it back to Auth Manager lease telemetry.
- `packages/lease-runtime/`
  Shared TypeScript broker/runtime helpers used for backend client access, lease lifecycle decisions, auth payload validation, state helpers, and telemetry payload generation.
- `scripts/`
  Installer/bootstrap helpers, including Linux headless-client install and uninstall scripts.
- `systemd/`
  Optional user-service unit files for Linux background agents.
- `tests/`
  Backend test coverage for broker logic, analytics, and account behavior.

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
- `CODEX_PROFILES_DIR` (legacy migration source only)
- `USAGE_DB_PATH` (legacy migration source only)
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
- `ANALYTICS_TIMEZONE` (`1d` / "Today" analytics are computed from local midnight in this timezone)
- `ANALYTICS_SNAPSHOT_INTERVAL_SECONDS` (default `600`, captures absolute + utilization snapshots every 10 minutes)
- `MAX_ASSIGNABLE_UTILIZATION_PERCENT` (default `95`)
- `ROTATION_REQUEST_THRESHOLD_PERCENT` (default `90`)
- `EXHAUSTED_UTILIZATION_PERCENT` (default `100`)
- `MIN_QUOTA_REMAINING` (default `10000`)
- `ALLOW_CLIENT_INITIATED_ROTATION` (default `true`)
- `LEASE_DEFAULT_TTL_SECONDS`
- `LEASE_RENEWAL_MIN_REMAINING_SECONDS`
- `WEEKLY_RESET_CONFIRMATION_REQUIRED` (default `true`)
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

VS Code extension:
```bash
cd vscode-extension
npm install
npm run compile
npm test
```

Desktop app:
```bash
cd desktop-app
npm install
npm test
npm run build
```

Headless Linux client:
```bash
cd headless-client
npm install
npm test
npm run build
```

Shared broker lifecycle suite:
```bash
./scripts/run-broker-lifecycle-tests.sh
```

More detail is in `docs/broker-lifecycle-testing.md`.

## Docker Run

```bash
docker compose pull auth-manager
docker compose up -d
```

Services:
- Auth Manager app: `http://localhost:8080`

The published backend image already contains the built frontend, so the default compose stack does not build a separate frontend container.

## AMD64 / ARM64 Installation Notes

This project supports both:
- `linux/amd64` (x86_64)
- `linux/arm64` (aarch64, e.g. Apple Silicon / Graviton)

The GitHub workflow builds and publishes multi-arch images to GHCR.

### Option A: Use published multi-arch image (default compose behavior)

The included `docker-compose.yml` pulls:

- `ghcr.io/halsysfin/codex-auth-manager:latest` for backend and bundled frontend (AMD64 + ARM64 manifest)

```bash
docker compose pull auth-manager
docker compose up -d
```

Docker automatically pulls the correct backend architecture variant for your host.

### Option B: Build backend locally from source (dev/test)

Use the included local override file (`docker-compose.local.yml`) which swaps
`auth-manager` to a local build and starts the frontend dev server from local source.

Then run:

```bash
docker compose -f docker-compose.yml -f docker-compose.local.yml up --build
```

### Force a specific architecture (only if needed)

If you need to force one explicitly:

```yaml
auth-manager:
  platform: linux/amd64
```

or

```yaml
auth-manager:
  platform: linux/arm64
```

### Verify image architecture

```bash
docker image inspect ghcr.io/halsysfin/codex-auth-manager:latest --format '{{.Architecture}}/{{.Os}}'
```

Container defaults:
- Active auth file: `/root/.codex/auth.json`
- App state volume root: `/var/lib/auth-manager`
  - callbacks: `/var/lib/auth-manager/callbacks`
  - legacy profile migration source: `/var/lib/auth-manager/legacy/profiles`
  - legacy sqlite migration source: `/var/lib/auth-manager/legacy/auth-manager.sqlite3`

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

## VS Code Extension

The VS Code extension lives in `vscode-extension/`.

It can:

- ensure a lease on startup
- refresh, renew, rotate, and release broker leases
- materialize auth into `~/.codex/auth.json`
- display lease and usage status in a sidebar and status bar

Key commands:

- `authManager.ensureLease`
- `authManager.refreshLease`
- `authManager.rotateLease`
- `authManager.releaseLease`
- `authManager.reloadCodexAuth`
- `authManager.reloadWindow`
- `authManager.openDashboard`

See [vscode-extension/README.md](/root/auth_manager/vscode-extension/README.md) for setup and usage details.

## Desktop App

The desktop app lives in `desktop-app/` and provides the same broker-backed auth lifecycle without requiring VS Code.

It can:

- ensure a valid lease on startup
- materialize auth into `~/.codex/auth.json`
- refresh/renew/rotate/reacquire in the background
- show lease, usage, and auth-file status in a native desktop UI

Useful commands:

```bash
cd desktop-app
npm install
npm run tauri:dev
```

See [desktop-app/README.md](/root/auth_manager/desktop-app/README.md) for build and runtime instructions.

## Headless Linux Client

The Linux-friendly non-GUI client lives in `headless-client/`.

It provides:

- `auth-manager-agent ensure`
- `auth-manager-agent status`
- `auth-manager-agent renew`
- `auth-manager-agent rotate`
- `auth-manager-agent release`
- `auth-manager-agent run`
- `auth-manager-agent doctor`

It stores non-secret state under XDG config/state paths, writes `~/.codex/auth.json`, and can run continuously via a user-level systemd service.

Installer and service support:

- installer: [scripts/install-headless-linux.sh](/root/auth_manager/scripts/install-headless-linux.sh)
- uninstall: [scripts/uninstall-headless-linux.sh](/root/auth_manager/scripts/uninstall-headless-linux.sh)
- service unit: [systemd/auth-manager-agent.service](/root/auth_manager/systemd/auth-manager-agent.service)

See [headless-client/README.md](/root/auth_manager/headless-client/README.md) for install and usage details.

## OpenClaw Integration

OpenClaw support in this repo is split into two parts:

- [openclaw-skill/SKILL.md](/root/auth_manager/openclaw-skill/SKILL.md)
  for lease repair, auth materialization, and operational workflows
- [openclaw-plugin/README.md](/root/auth_manager/openclaw-plugin/README.md)
  for runtime token-usage capture and lease telemetry posting

The skill is useful when a human or assistant needs to repair or rotate a lease.
The plugin is useful when OpenClaw itself can observe `prompt_tokens`, `completion_tokens`, or similar usage fields and send them back to Auth Manager.

## Notes

- DB is source of truth; active auth file is materialized for runtime integration.
- Cached-first UI means page loads from persisted state first, then refreshes live.
- `1d` analytics mean `Today`, computed from local midnight to now using `ANALYTICS_TIMEZONE`.
- When absolute usage counters are unavailable, the dashboard switches to fallback mode and shows utilization-based charts without pretending consumption is `0`.
- Usage analytics snapshots are captured every `ANALYTICS_SNAPSHOT_INTERVAL_SECONDS` seconds. The default is 10 minutes and captures both absolute usage state (used, limit, remaining/lifetime context) and utilization percentages per account.

## Lease Broker

Auth Manager can now act as a lease broker for approved credentials already stored as saved profiles.

- `POST /api/leases/acquire` selects the best policy-eligible credential and issues a time-bounded lease.
- `POST /api/leases/{lease_id}/renew` extends an active lease while the credential is still usable.
- `POST /api/leases/{lease_id}/release` voluntarily releases the lease and returns the credential to the pool if policy allows.
- `POST /api/leases/{lease_id}/telemetry` stores time-series lease telemetry and updates the latest lease/credential summary.
- `POST /api/leases/rotate` creates a replacement lease when policy allows and a healthy credential is available.
- `GET /api/leases/{lease_id}` returns the current lease state with the latest telemetry summary.
- `POST /api/admin/credentials/{credential_id}/mark-exhausted` forces a credential into exhausted state for testing/admin intervention.

Lifecycle and policy rules:

- A credential is only assignable when it is not already leased and stays below `MAX_ASSIGNABLE_UTILIZATION_PERCENT`.
- Telemetry at or above `ROTATION_REQUEST_THRESHOLD_PERCENT` marks the active lease as `rotation_required`.
- Telemetry at or above `EXHAUSTED_UTILIZATION_PERCENT` immediately marks the credential exhausted and revokes active leases using it.
- Exhausted or over-threshold credentials stay unavailable until the weekly reset boundary has passed and fresh telemetry/reconciliation confirms the credential is back below policy thresholds.
- Rotation never returns exhausted, revoked, expired, cooldown, already leased, or over-threshold credentials.

Telemetry and reset behavior:

- Lease telemetry is persisted as time-series rows keyed by lease, credential, and machine/agent ownership.
- The latest telemetry summary is copied onto the active lease and credential for quick reads.
- Weekly reset confirmation is explicit. Elapsed time alone does not restore assignability when `WEEKLY_RESET_CONFIRMATION_REQUIRED=true`.

## Build Scope

The production Docker image build is intentionally focused on the backend and frontend runtime only.

These repo components are excluded from Docker image build context:

- `tests/`
- `chrome-extension/`
- `vscode-extension/`
- `desktop-app/`
- `headless-client/`
- `openclaw-plugin/`
- `packages/`
