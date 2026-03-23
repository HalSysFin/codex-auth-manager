# OpenClaw Auth Manager Plugin

This package is a small Linux-friendly TypeScript plugin/helper for wiring OpenClaw token usage into the existing Auth Manager lease broker.

It is intended to complement:

- `openclaw-skill/` for lease acquisition, repair, auth materialization, and manual workflows
- Auth Manager backend lease telemetry for per-lease usage storage

## What it does

- accepts an active lease context:
  - `leaseId`
  - `machineId`
  - `agentId`
- normalizes common usage payload shapes from OpenClaw/OpenAI-style responses
- accumulates per-lease counters in process
- posts truthful lease telemetry back to Auth Manager

The telemetry goes to:

- `POST /api/leases/{lease_id}/telemetry`

Auth Manager already stores:

- `requests_count`
- `tokens_in`
- `tokens_out`
- utilization and quota summary fields

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

## Example

```ts
import { createOpenClawAuthManagerPlugin } from './src/index.js'

const plugin = createOpenClawAuthManagerPlugin({
  baseUrl: process.env.AUTH_MANAGER_BASE_URL!,
  apiKey: process.env.AUTH_MANAGER_API_KEY!,
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

## Suggested integration points

Wire the plugin into the place where OpenClaw already sees model response usage.

Typical flow:

1. when a lease is acquired or repaired, call `setLeaseContext(...)`
2. after each model response, call `observeUsage(responseLikeObject)`
3. on a timer or after each request batch, call `flushTelemetry()`
4. when the lease rotates, update the lease context and keep going

## Commands

```bash
cd openclaw-plugin
npm install
npm test
npm run build
```

## Limitations

- This repo does not contain the actual OpenClaw runtime, so the final hook point into OpenClaw still needs to be wired in that project.
- The plugin intentionally does not invent usage values. If OpenClaw does not expose token counts, nothing should be sent except truthful status/utilization context.
