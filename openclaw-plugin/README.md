# OpenClaw Auth Manager Plugin

This package is a plugin-ready TypeScript module for wiring OpenClaw token usage into the existing Auth Manager lease broker.

For OpenClaw, the intended lease model is a sticky machine lease:

- a machine keeps the same assigned auth
- the plugin renews that lease so it stays alive
- the plugin writes the leased auth to `~/.codex/auth.json`
- the plugin does **not** proactively rotate away from a usable auth
- a new auth is only acquired when the current one becomes unusable, exhausted, revoked, or otherwise no longer serviceable

It is intended to complement:

- `openclaw-skill/` for lease acquisition, repair, auth materialization, and manual workflows
- Auth Manager backend lease telemetry for per-lease usage storage

## What it does

- acquires and renews a sticky lease for the OpenClaw machine
- materializes the full leased auth payload to the active Codex auth file
- reuses the same lease on restart when the broker still considers it valid
- reacquires and rewrites auth automatically when the current leased credential is consumed or revoked
- normalizes common usage payload shapes from OpenClaw/OpenAI-style responses
- accumulates per-lease counters in process
- posts truthful lease telemetry back to Auth Manager
- includes a service wrapper and entry module that can be copied into the real OpenClaw plugin loader

The telemetry goes to:

- `POST /api/leases/{lease_id}/telemetry`

Auth Manager already stores:

- `requests_count`
- `tokens_in`
- `tokens_out`
- utilization and quota summary fields

The intent is that OpenClaw receives the full leased auth JSON from Auth Manager, uses that auth for the active machine lease, and then reports back as much truthful token/model usage data as OpenClaw exposes.

## Supported usage shapes

OpenAI-style usage objects:

```json
{
  "usage": {
    "prompt_tokens": 1234,
    "completion_tokens": 456,
    "total_tokens": 1690
  },
  "model": "gpt-5.4",
  "status": "healthy"
}
```

Direct counters:

```json
{
  "requests_count": 1,
  "tokens_in": 1234,
  "tokens_out": 456,
  "source": "openclaw",
  "status": "healthy"
}
```

## Runtime Example

```ts
import { createOpenClawAuthManagerPlugin } from './src/index.js'

const plugin = createOpenClawAuthManagerPlugin({
  baseUrl: process.env.AUTH_MANAGER_BASE_URL!,
  internalApiToken: process.env.AUTH_MANAGER_INTERNAL_API_TOKEN!,
  context: {
    leaseId: process.env.AUTH_MANAGER_LEASE_ID!,
    machineId: process.env.AUTH_MANAGER_MACHINE_ID || 'my-host',
    agentId: process.env.AUTH_MANAGER_AGENT_ID || 'openclaw',
  },
})

plugin.observeUsage({
  usage: {
    prompt_tokens: 1200,
    completion_tokens: 300,
    total_tokens: 1500,
  },
  model: 'gpt-5.4',
  status: 'healthy',
})

await plugin.flushTelemetry()
```

The plugin authenticates to Auth Manager with:

```http
Authorization: Bearer <AUTH_MANAGER_INTERNAL_API_TOKEN>
```

Use `internalApiToken` in plugin code/config and `AUTH_MANAGER_INTERNAL_API_TOKEN` in env-based setups. The older `apiKey` and `AUTH_MANAGER_API_KEY` names are kept only as compatibility fallbacks.

## Plugin-Ready Entry Surface

This repo now includes both:

- this reusable telemetry package
- a working runtime integration prototype wired into a real OpenClaw clone at `/tmp/openclaw`

The package still contains the pieces needed to copy into OpenClaw as a real plugin:

- `src/openclaw-entry.ts`
  plugin-entry style module export
- `src/service.ts`
  service wrapper with timer-based flushing
- `src/config.ts`
  plugin config/env resolution and validation

The intended integration path inside OpenClaw is:

1. load plugin config
2. start the Auth Manager lease service
3. acquire or reuse the machine's sticky lease, then materialize auth to `~/.codex/auth.json`
4. set/update lease context from the active lease
5. call `observeUsage(...)` from OpenClaw's existing assistant-usage path
6. flush on timer and shutdown

## Suggested integration points

Wire the plugin into the place where OpenClaw already sees model response usage.

The working prototype uses these exact OpenClaw files:

- `/tmp/openclaw/src/agents/pi-embedded-subscribe.handlers.messages.ts`
  `handleMessageEnd(...)` forwards assistant `usage` after each real response
- `/tmp/openclaw/src/agents/assistant-usage-observers.ts`
  tiny runtime observer registry used by the lease service
- `/tmp/openclaw/extensions/auth-manager-lease/src/service.ts`
  lease lifecycle owner: acquire, renew, rotate, reacquire, materialize, persist, and telemetry flush

Lease context is injected by the Auth Manager lease service itself after:

- acquire
- reacquire
- lease refresh
- renewal

That keeps telemetry pinned to the active lease and clears pending counters when a lease ID changes.

Typical flow:

1. when a lease is acquired or repaired, call `setLeaseContext(...)`
2. after each model response, call `observeUsage(responseLikeObject)`
3. on a timer or after each request batch, call `flushTelemetry()`
4. when the leased auth becomes unusable, reacquire the next available auth and keep going

The working prototype flushes:

- on a timer
- when the per-request threshold is reached
- during service shutdown

After each telemetry flush it refreshes lease state so `revoked`, `expired`, and exhausted credentials are repaired quickly instead of continuing on a dead lease.

## Default OpenClaw lease policy

By default this plugin is intentionally configured for permanent-ish machine leases:

- `autoRenew = true`
- `autoRotate = false`
- `releaseLeaseOnShutdown = false`

That means:

- the same machine keeps the same auth as long as the broker still considers it usable
- short restarts do not cause the plugin to voluntarily give the auth back
- the plugin only switches auth when the current leased credential can no longer be used

If you want a different behavior, you can still override it with env/config:

- `AUTH_MANAGER_AUTO_RENEW`
- `AUTH_MANAGER_AUTO_ROTATE`
- `AUTH_MANAGER_RELEASE_LEASE_ON_SHUTDOWN`
- `AUTH_MANAGER_ROTATION_POLICY`

## Required OpenClaw-side hook

The missing piece used to be the runtime hook. The prototype now proves that hook in a real OpenClaw clone.

What still depends on the external OpenClaw project is shipping that extension in OpenClaw itself:

- bundle `extensions/auth-manager-lease/`
- keep the assistant usage observer hook in the embedded session path
- expose plugin config through normal OpenClaw plugin installation/config flows

## Commands

```bash
cd openclaw-plugin
npm install
npm test
npm run build
```

## Limitations

- The runtime wiring currently lives in the external OpenClaw clone, not in a published OpenClaw release yet.
- The plugin intentionally does not invent usage values. If OpenClaw does not expose token counts, nothing should be sent except truthful status/utilization context.
