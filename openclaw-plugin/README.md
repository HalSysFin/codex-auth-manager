# Codex Auth Manager Plugin

This plugin connects OpenClaw to Codex Auth Manager so OpenClaw can run on a managed lease, receive the active auth automatically, and report usage back to the manager.

## What It Does

- acquires or reuses a sticky lease for the OpenClaw machine
- materializes the leased auth into the local Codex auth file
- writes OpenClaw-compatible auth files so the leased auth becomes the active OpenClaw auth
- renews the same lease while it remains usable
- reacquires a new lease only when the current auth becomes unusable, revoked, expired, or exhausted
- posts lease telemetry back to Codex Auth Manager
- imports OpenClaw usage export JSON back into the manager on a regular cycle

The intended OpenClaw model is a sticky machine lease:

- the machine keeps the same auth while it is still usable
- the plugin does not proactively rotate away from a healthy auth
- a new auth is only requested when the current leased auth can no longer be used

## How It Works

On startup the plugin:

1. resolves its manager configuration
2. acquires or reuses the current machine lease
3. materializes the leased auth from the manager
4. writes the active auth locally
5. starts the regular lease refresh / renew cycle
6. sends telemetry and usage imports back to the manager

The plugin talks to these manager endpoints:

- `POST /api/leases/acquire`
- `GET /api/leases/{lease_id}`
- `POST /api/leases/{lease_id}/renew`
- `POST /api/leases/{lease_id}/release`
- `POST /api/leases/{lease_id}/materialize`
- `POST /api/leases/{lease_id}/telemetry`
- `POST /api/leases/rotate`
- `POST /api/openclaw/usage/import`

## Configuration

Required:

- `baseUrl` or `brokerAddress`
- `internalApiToken`

Optional:

- `machineId`
- `agentId`
- `authFilePath`
- `leaseProfileId`
- `requestedTtlSeconds`
- `refreshIntervalMs`
- `flushIntervalMs`
- `flushEveryRequests`
- `autoRenew`
- `autoRotate`
- `rotationPolicy`
- `releaseLeaseOnShutdown`
- `usageExportJsonPath`
- `usageExportDays`

Minimal example:

```json
{
  "baseUrl": "https://your-auth-manager.example.com",
  "internalApiToken": "<INTERNAL_API_TOKEN>"
}
```

Example with explicit machine and agent names:

```json
{
  "baseUrl": "https://your-auth-manager.example.com",
  "internalApiToken": "<INTERNAL_API_TOKEN>",
  "machineId": "debian",
  "agentId": "main"
}
```

Notes:

- if `machineId` is omitted, the plugin derives it from the host name
- if `agentId` is `main`, the plugin sends it as `openclaw:main` so the manager UI clearly identifies it as an OpenClaw client
- manager auth is sent as `Authorization: Bearer <internalApiToken>`

## Usage Import

The plugin imports OpenClaw usage JSON back to the manager.

It works in one of two modes:

1. if `usageExportJsonPath` is set, the plugin reads that JSON file directly
2. otherwise, the plugin runs the OpenClaw usage export command and uploads the returned JSON

Default export window:

- `usageExportDays = 30`

The plugin only uploads when the usage payload changes, so identical exports are not re-imported on every cycle.

## Auth Files

The plugin writes the leased auth into the local Codex auth path and OpenClaw-compatible auth storage.

That includes:

- `~/.codex/auth.json`
- `~/.openclaw/auth.json`
- `~/.openclaw/agents/<agentId>/agent/auth-profiles.json`
- `~/.openclaw/credentials/oauth.json`

This allows the leased manager auth to become the active OpenClaw auth without manual profile switching.

## Default Lease Policy

By default the plugin is configured for a sticky OpenClaw lease:

- `autoRenew = true`
- `autoRotate = false`
- `releaseLeaseOnShutdown = false`

That means short restarts do not voluntarily give the auth back, and the plugin only switches auth when the current leased credential is no longer serviceable.

## Duplicate Installs And Upgrades

The plugin keeps stable install identity values:

- npm package name: `openclaw-auth-manager-plugin`
- plugin id: `auth-manager-lease-telemetry`

That means upgrades should replace the existing plugin rather than create a second logical install.

If you still see duplicates, remove stale copies from `~/.openclaw/extensions` and keep only one installed copy of `auth-manager-lease-telemetry`.

## Local Development

```bash
cd openclaw-plugin
npm install
npm test
npm run build
```

To package a tarball:

```bash
cd openclaw-plugin
npm pack
```

## Limitations

- the plugin depends on manager lease/materialize endpoints being available
- usage import is only as good as the OpenClaw usage export available on the machine
- the plugin does not invent usage values; it only forwards real auth and usage data it can obtain
