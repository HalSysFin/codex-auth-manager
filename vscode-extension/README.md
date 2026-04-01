# Codex Auth Manager VS Code Extension

This extension connects VS Code directly to Codex Auth Manager so the editor can run on a managed lease and keep the local Codex auth file aligned with the active leased account.

## What It Does

- acquires or reuses a lease for the current VS Code machine context
- materializes the leased auth payload into `~/.codex/auth.json`
- refreshes and renews the lease in the background
- rotates or reacquires when the active lease is no longer usable
- reconciles local `~/.codex/auth.json` changes back to Auth Manager when Codex refreshes auth on its own
- pulls fresher auth back down when the manager refreshed first
- posts lightweight lease telemetry back to Codex Auth Manager
- shows current lease and account state in the activity bar view and status bar
- supports Remote SSH / remote workspace identity so different remote hosts do not collapse into one machine

## How It Works

On startup the extension:

1. resolves machine and agent identity
2. loads persisted lease state for that identity
3. acquires or refreshes the current lease
4. materializes the leased auth payload
5. writes the active auth to the local Codex auth file
6. starts background refresh and telemetry timers

The extension expects these manager endpoints:

- `POST /api/leases/acquire`
- `GET /api/leases/{lease_id}`
- `POST /api/leases/{lease_id}/renew`
- `POST /api/leases/{lease_id}/release`
- `POST /api/leases/{lease_id}/telemetry`
- `POST /api/leases/rotate`
- `POST /api/leases/{lease_id}/materialize`
- `POST /api/leases/{lease_id}/reconcile-auth`

Manager auth is sent as:

```http
Authorization: Bearer <authManager.internalApiToken>
```

## Settings

The shipped extension settings are:

- `authManager.baseUrl`
- `authManager.internalApiToken`
- `authManager.machineId`
- `authManager.agentId`
- `authManager.authFilePath`
- `authManager.refreshIntervalSeconds`
- `authManager.telemetryIntervalSeconds`
- `authManager.autoRenew`
- `authManager.autoRotate`
- `authManager.autoReloadWindowOnLeaseChange`
- `authManager.releaseLeaseOnShutdown`
- `authManager.deleteAuthFileOnShutdown`
- `authManager.allowInsecureLocalhost`

Defaults worth knowing:

- `authManager.baseUrl = http://127.0.0.1:8080`
- `authManager.authFilePath = ~/.codex/auth.json`
- `authManager.autoRenew = true`
- `authManager.autoRotate = true`
- `authManager.releaseLeaseOnShutdown = true`
- `authManager.deleteAuthFileOnShutdown = true`

Notes:

- leave `authManager.machineId` blank unless you intentionally want to override the derived machine identity
- leave `authManager.agentId` blank unless you intentionally want to override the default agent identity
- on Remote SSH, the extension includes remote host context in the machine identity so different SSH targets stay distinct
- effective rotation policy comes from the manager lease status response

## Commands

The extension contributes these commands:

- `Auth Manager: Ensure Lease`
- `Auth Manager: Refresh Lease`
- `Auth Manager: Request New Auth Lease`
- `Auth Manager: Rotate Lease`
- `Auth Manager: Release Lease`
- `Auth Manager: Reload Codex Auth`
- `Auth Manager: Reload Window`
- `Auth Manager: Open Dashboard`
- `Auth Manager: Show Lease View`

Practical command behavior:

- `Request New Auth Lease` requests a fresh lease instead of renewing the current one
- `Reload Codex Auth` rewrites the local auth file from the current active lease state
- `Reload Window` is available as a manual fallback after auth replacement
- `Open Dashboard` opens the manager UI

## Auth File Behavior

The extension writes the leased auth payload to `~/.codex/auth.json` by default.

It writes atomically:

1. expands `~`
2. creates parent directories if needed
3. writes a temp file
4. fsyncs the temp file
5. renames it into place

The extension does not persist the full auth payload into extension state. The manager remains the source of truth and the auth payload is only handled when materialized.

## Lease Lifecycle

Normal lifecycle:

- startup: acquire or recover the lease and materialize auth
- background: refresh lease status and post telemetry on intervals
- renewal: renew when lease expiry approaches and auto-renew is enabled
- replacement: rotate or reacquire when the manager reports replacement is required or the lease becomes invalid
- shutdown: best-effort release and auth file cleanup when those settings are enabled

## UI

The extension provides:

- an activity bar view for lease status and manual actions
- a status bar entry showing current account / lease state
- a compact usage-focused panel with a separate lease info tab

The default panel is intentionally concise. Detailed lease metadata lives in the lease info tab instead of the main usage view.

## Local Development

```bash
cd vscode-extension
npm install
npm run compile
npm test
```

To package a VSIX:

```bash
cd vscode-extension
npm run package
```

## Limitations

- telemetry is intentionally lightweight; it does not invent token counts
- the extension depends on the manager being the source of truth for lease validity and materialized auth
- the extension can only do best-effort cleanup on shutdown; hard kills and crashes can skip deactivation logic
