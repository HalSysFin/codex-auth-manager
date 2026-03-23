# Codex Auth Manager Desktop

This is a standalone Tauri desktop app that performs the same broker-backed Codex auth lease lifecycle as the VS Code extension, but without depending on VS Code.

What it does:

- ensures the local machine has a valid Auth Manager lease on startup
- reacquires, renews, rotates, and releases leases through the existing broker API
- materializes leased auth via the backend and writes `~/.codex/auth.json`
- refreshes lease state in the background
- posts minimal truthful lease telemetry
- persists non-secret lease metadata and local settings across restarts
- shows current lease and usage state in a desktop UI

## Reuse from the VS Code extension

The desktop app reuses the broker/runtime logic patterns from the extension by extracting the pure lease runtime into:

- `packages/lease-runtime/`

That shared runtime includes:

- typed Auth Manager client
- lease lifecycle helpers
- auth payload validation helpers
- minimal telemetry payload builder
- runtime lease state helpers

The desktop app then adds Tauri-specific persistence and auth-file writing on top.

## Backend support expected

The app expects these existing backend endpoints:

- `POST /api/leases/acquire`
- `GET /api/leases/{lease_id}`
- `POST /api/leases/{lease_id}/renew`
- `POST /api/leases/{lease_id}/release`
- `POST /api/leases/{lease_id}/telemetry`
- `POST /api/leases/rotate`
- `POST /api/leases/{lease_id}/materialize`

The materialize endpoint must return `credential_material.auth_json`.

## Settings

The app persists these settings locally:

- backend base URL
- internal API token
- machine ID
- agent ID
- auth file path
- refresh interval seconds
- telemetry interval seconds
- auto renew
- auto rotate
- optional dashboard path

If machine ID or agent ID are blank, the app generates stable defaults and saves them.

## Auth file writing

The app writes `~/.codex/auth.json` by:

1. expanding `~`
2. ensuring the parent directory exists
3. validating the auth payload shape before write
4. writing a temporary file
5. syncing the temporary file
6. renaming it into place atomically

The app never stores raw auth tokens in the local app state. The token payload only lives in the auth file returned by backend materialization.

## Lease lifecycle behavior

- Startup:
  - load persisted local state
  - acquire when there is no lease
  - refresh when there is an existing lease
  - reacquire on 404 / revoked / expired
  - rotate when replacement is required
  - renew near expiry when auto-renew is enabled
- Background:
  - lease refresh loop every 60 seconds by default
  - telemetry loop every 300 seconds by default
- Auth:
  - rewrite `~/.codex/auth.json` after acquire/rotate/materialize

## Development

Install dependencies:

```bash
cd desktop-app
npm install
```

Run the web UI in dev mode:

```bash
cd desktop-app
npm run dev
```

Run tests:

```bash
cd desktop-app
npm test
```

Run the Tauri desktop app in dev mode:

```bash
cd desktop-app
npm run tauri:dev
```

Build the frontend bundle:

```bash
cd desktop-app
npm run build
```

Build the Linux desktop app:

```bash
cd desktop-app
npm run tauri:build
```

Build a Windows bundle from a Windows machine:

```bash
cd desktop-app
npm run tauri:build
```

## Known limitations

- No tray support in this first pass.
- The app logs recent events to a local log file and UI panel, but does not yet have a dedicated log-management screen.
- The shared runtime is reused across the desktop app, headless client, and VS Code extension so lease lifecycle behavior stays aligned.
- Windows packaging cannot be fully verified from this Linux environment.
