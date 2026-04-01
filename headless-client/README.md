# Auth Manager Headless Linux Client

This is a Linux-friendly non-GUI Auth Manager client that acquires and maintains a broker lease, materializes auth into `~/.codex/auth.json`, and keeps the lease healthy over time.

It reuses the shared broker runtime in `packages/lease-runtime/` for:

- typed backend client
- lease lifecycle logic
- auth payload validation
- runtime lease-state helpers
- telemetry payload generation

## Commands

- `auth-manager-agent ensure`
- `auth-manager-agent status`
- `auth-manager-agent renew`
- `auth-manager-agent rotate`
- `auth-manager-agent release`
- `auth-manager-agent run`
- `auth-manager-agent doctor`

## What each command does

- `ensure`: ensure a valid lease exists and auth is materialized
- `status`: refresh and print current lease state
- `renew`: attempt lease renewal
- `rotate`: request a replacement lease
- `release`: release the current lease
- `run`: continuous background loop for refresh + telemetry
- `doctor`: validate config, state paths, backend connectivity, and auth path

## Config

Config can come from:

- environment variables
- `~/.config/auth-manager-agent/config.json`
- CLI flags

Supported settings:

- `AUTH_MANAGER_BASE_URL`
- `AUTH_MANAGER_INTERNAL_API_TOKEN`
- `AUTH_MANAGER_MACHINE_ID`
- `AUTH_MANAGER_AGENT_ID`
- `AUTH_MANAGER_AUTH_FILE_PATH`
- `AUTH_MANAGER_REFRESH_INTERVAL_SECONDS`
- `AUTH_MANAGER_TELEMETRY_INTERVAL_SECONDS`
- `AUTH_MANAGER_AUTO_RENEW`
- `AUTH_MANAGER_AUTO_ROTATE`
- `AUTH_MANAGER_ALLOW_INSECURE_LOCALHOST`

Defaults:

- auth file: `~/.codex/auth.json`
- refresh interval: `60`
- telemetry interval: `300`
- auto renew: `true`
- auto rotate: `true`

If `machine_id` or `agent_id` are missing, the client generates stable defaults and persists them in local state.

## Local files

By default the client uses XDG-friendly paths:

- config: `~/.config/auth-manager-agent/config.json`
- state: `~/.local/state/auth-manager-agent/state.json`
- log: `~/.local/state/auth-manager-agent/agent.log`

Local state stores non-secret lease metadata only. Raw auth tokens are written only to the auth file returned by backend materialization.

## Auth file behavior

The client writes `~/.codex/auth.json` by:

1. expanding `~`
2. ensuring the parent directory exists
3. validating the auth payload shape
4. writing a temporary file
5. syncing the file
6. renaming it into place atomically

The client never logs token contents.

## Runtime behavior

`run` mode:

- ensures a valid lease on startup
- refreshes lease state every 60 seconds by default
- posts minimal truthful telemetry every 300 seconds by default
- renews near expiry if auto-renew is enabled
- rotates or reacquires when replacement is required or the lease is no longer usable
- rewrites `~/.codex/auth.json` after successful materialization
- reconciles local `~/.codex/auth.json` changes back to Auth Manager when Codex refreshes auth outside the agent
- rewrites local auth automatically when the manager has newer tokens

## Installation

Use:

```bash
./scripts/install-headless-linux.sh
```

The installer:

- installs/builds the headless client
- writes a default config file if missing
- installs `~/.local/bin/auth-manager-agent`
- optionally installs a user systemd unit

## systemd user service

An optional user service is provided:

```bash
systemctl --user enable --now auth-manager-agent
```

Check logs:

```bash
journalctl --user -u auth-manager-agent -f
```

## Development

```bash
cd headless-client
npm install
npm run build
npm test
node dist/headless-client/src/cli.js ensure
```

## Troubleshooting

- `auth-manager-agent doctor`
- inspect `~/.local/state/auth-manager-agent/agent.log`
- inspect `~/.local/state/auth-manager-agent/state.json`
- verify backend token/base URL in config or environment

Backend auth is always sent as:

```http
Authorization: Bearer <AUTH_MANAGER_INTERNAL_API_TOKEN>
```

## Known limitations

- Linux-focused first version only
- installer assumes Node.js/npm are already available
- no auto-install of Node itself
- no packaging into a single standalone binary in this first pass
