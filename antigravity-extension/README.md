# CAM Antigravity Extension

This extension connects Antigravity directly to Codex Auth Manager so the editor can run on a managed lease and keep the local Codex auth file aligned with the active leased account.

## What It Does

- acquires or reuses a lease for the current Antigravity machine context
- materializes the leased auth payload into `~/.codex/auth.json`
- refreshes and renews the lease in the background
- rotates or reacquires when the active lease is no longer usable
- reconciles local `~/.codex/auth.json` changes back to Auth Manager when Codex refreshes auth on its own
- pulls fresher auth back down when the manager refreshed first
- posts lightweight lease telemetry back to Codex Auth Manager
- shows current lease and account state in the activity bar view and status bar
- supports Remote SSH / remote workspace identity so different remote hosts do not collapse into one machine

## Why This Is Separate

Antigravity is VS Code-based, so the working Auth Manager lease client can be packaged for Antigravity with the same runtime behavior and an Antigravity-specific install flow. This package keeps the same lease protocol and auth file behavior as the main VS Code package while giving you a dedicated VSIX for the fork.

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

## Install In Antigravity

1. Build the VSIX from this folder.
2. In Antigravity, open the extensions view.
3. Use the install-from-VSIX flow.
4. Configure:
   - `authManager.baseUrl`
   - `authManager.internalApiToken`
5. Reload Antigravity and let the extension acquire its lease.

## Local Development

```bash
cd antigravity-extension
npm install
npm run compile
npm test
```

To package a VSIX:

```bash
cd antigravity-extension
npm run package
```

## Notes

- keep `authManager.machineId` blank unless you intentionally want to override the derived machine identity
- keep `authManager.agentId` blank unless you intentionally want to override the default agent identity
- on Remote SSH, the extension includes remote host context in the machine identity so different SSH targets stay distinct
- effective rotation policy comes from the manager lease status response
