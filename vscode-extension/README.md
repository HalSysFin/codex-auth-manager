# Codex Auth Manager VS Code Extension

This extension lets VS Code participate directly in the Auth Manager lease broker flow.

What it does:

- ensures the local machine has an Auth Manager lease
- refreshes and renews the lease in the background
- rotates or reacquires when the lease is no longer usable
- materializes the leased auth payload into `~/.codex/auth.json`
- posts lightweight lease telemetry back to Auth Manager
- shows current lease status in a sidebar view and a right-aligned status bar that includes the current account label when known

## Shared runtime

The VS Code extension now uses the same shared lease runtime as the desktop app and headless client:

- `packages/lease-runtime/`

That shared runtime provides:

- typed Auth Manager client calls
- lease lifecycle decisions
- runtime lease-state helpers
- telemetry payload shaping
- auth payload validation helpers

The extension keeps VS Code-specific concerns local:

- commands
- webview and status bar UI
- extension globalState persistence
- filesystem writes for the active auth file

## Expected backend support

The extension expects these Auth Manager endpoints:

- `POST /api/leases/acquire`
- `GET /api/leases/{lease_id}`
- `POST /api/leases/{lease_id}/renew`
- `POST /api/leases/{lease_id}/release`
- `POST /api/leases/{lease_id}/telemetry`
- `POST /api/leases/rotate`
- `POST /api/leases/{lease_id}/materialize`

The materialize response is expected to include:

```json
{
  "status": "ok",
  "lease": { "...": "..." },
  "credential_material": {
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

If your backend does not yet support `POST /api/leases/{lease_id}/materialize`, that is the required contract addition for end-to-end auth delivery.

## Extension settings

- `authManager.baseUrl`
- `authManager.internalApiToken`
- `authManager.machineId`
- `authManager.agentId`
- `authManager.authFilePath`
- `authManager.refreshIntervalSeconds`
- `authManager.telemetryIntervalSeconds`
- `authManager.autoRenew`
- `authManager.autoRotate`
- `authManager.openDashboardPath`
- `authManager.allowInsecureLocalhost`

If `machineId` or `agentId` are blank, the extension generates stable defaults and persists them in extension global state.

Backend auth is always sent as:

```http
Authorization: Bearer <authManager.internalApiToken>
```

## Commands

- `authManager.ensureLease`
- `authManager.refreshLease`
- `authManager.requestNewLease`
- `authManager.rotateLease`
- `authManager.releaseLease`
- `authManager.reloadCodexAuth`
- `authManager.reloadWindow`
- `authManager.openDashboard`
- `authManager.showLeaseView`

`authManager.requestNewLease` is a manual fresh-lease action. It does not renew the current lease. Instead, the extension releases the current lease when possible, acquires a new lease from the broker, materializes the new auth payload, and rewrites `~/.codex/auth.json`.

The status bar uses the best available account identity in this order:

1. `credential_material.label`
2. `credential_material.name`
3. `lease.metadata.label`
4. `credential_id`

## Auth file behavior

The extension writes the leased auth payload to `~/.codex/auth.json` by default.

It writes atomically:

1. expands `~`
2. creates parent directories if needed
3. writes to a temp file
4. fsyncs the temp file
5. renames it into place

The extension does not keep the full auth payload in extension state unless the backend sends it during a live materialization request.

## Lease lifecycle behavior

- Startup:
  - load persisted lease metadata
  - if no lease exists, acquire one
  - materialize auth payload and write auth file
  - if a lease exists, refresh it from the backend
- Background:
  - refresh lease status every `authManager.refreshIntervalSeconds`
  - post telemetry every `authManager.telemetryIntervalSeconds`
- Rotation / expiry:
  - rotate when replacement is required or recommended and auto-rotate is enabled
  - renew when close to expiry and auto-renew is enabled
  - reacquire when the lease is revoked or expired
  - reacquire when the backend reports the stored lease no longer exists

## Local development

From the repo root:

```bash
cd vscode-extension
npm install
npm run compile
```

To run tests:

```bash
cd vscode-extension
npm test
```

To package:

```bash
cd vscode-extension
npm run package
```

Then open the repo in VS Code and launch the extension in an Extension Development Host.

## Known limitations

- The extension does not run a separate local daemon in v1.
- Telemetry is intentionally lightweight and does not invent token/request counts.
- The extension does not force-restart VS Code. It offers a reload command instead.
- The extension assumes the backend is the source of truth for lease validity and replacement requirements.
