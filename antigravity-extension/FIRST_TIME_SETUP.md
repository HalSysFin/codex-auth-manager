# CAM Antigravity Extension First-Time Setup

## Required

Set these two settings in Antigravity after installing the VSIX:

- `authManager.baseUrl`
- `authManager.internalApiToken`

Example:

```json
{
  "authManager.baseUrl": "http://127.0.0.1:8080",
  "authManager.internalApiToken": "YOUR_INTERNAL_API_TOKEN"
}
```

## Optional

- `authManager.machineId`
- `authManager.agentId`
- `authManager.authFilePath`
- `authManager.autoReloadWindowOnLeaseChange`

Leave `machineId` blank unless you intentionally want a fixed machine identity override.

## First Run

After installing and configuring:

1. reload Antigravity
2. run `Auth Manager: Ensure Lease`
3. confirm the lease view opens and `~/.codex/auth.json` is materialized

## Remote SSH

If you use Antigravity over Remote SSH, the extension includes remote host context in the derived machine identity so different remote targets stay separate in Codex Auth Manager.
