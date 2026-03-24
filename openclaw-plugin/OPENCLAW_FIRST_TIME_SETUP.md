# OpenClaw Auth Manager Plugin — First-Time Setup (Works First Time)

This guide installs the plugin so it can:
1) acquire/renew leases from Auth Manager,
2) materialize auth from lease,
3) report truthful usage telemetry.

---

## 0) Preconditions

- OpenClaw gateway is running on the host.
- You have:
  - `AUTH_MANAGER_BASE_URL` (HTTPS recommended)
  - `AUTH_MANAGER_INTERNAL_API_TOKEN`
  - stable `machineId` (e.g. hostname)
  - `agentId` (e.g. `main`)
- Repo: `HalSysFin/codex-auth-manager` checked out.

---

## 1) Plugin packaging requirements (MANDATORY)

Your plugin package **must** include both:

### A) `package.json` with OpenClaw extension entry

```json
{
  "name": "openclaw-auth-manager-plugin",
  "version": "0.1.1",
  "type": "module",
  "openclaw": {
    "extensions": ["./index.js"]
  }
}
```

### B) `openclaw.plugin.json`

```json
{
  "id": "auth-manager-lease-telemetry",
  "name": "Auth Manager Lease Telemetry",
  "description": "Acquire/renew leases, materialize auth, and post telemetry.",
  "configSchema": {
    "type": "object",
    "additionalProperties": true,
    "properties": {
      "baseUrl": { "type": "string" },
      "internalApiToken": { "type": "string" },
      "machineId": { "type": "string" },
      "agentId": { "type": "string" },
      "authFilePath": { "type": "string" },
      "requestedTtlSeconds": { "type": "number" },
      "refreshIntervalMs": { "type": "number" },
      "autoRenew": { "type": "boolean" },
      "autoRotate": { "type": "boolean" },
      "rotationPolicy": { "type": "string" },
      "flushIntervalMs": { "type": "number" },
      "flushEveryRequests": { "type": "number" }
    }
  }
}
```

### C) `index.js` default export must be plugin object

```js
import { createAuthManagerOpenClawEntry } from './openclaw-entry.js';
export default createAuthManagerOpenClawEntry();
```

---

## 2) Critical runtime fix in `openclaw-entry`

Do **not** construct network client/service with fallback URL at module/register time.

Bad pattern (causes insecure URL failures before config):
- creating service in `register()` with `http://127.0.0.1:8080` and `unset` token.

Good pattern:
- only construct service inside `start(ctx)` after resolved+validated config.

---

## 3) Build + install

```bash
cd openclaw-plugin
npm install
npm run build

# install plugin folder or packaged tarball
openclaw plugins install .
```

Then enable it:

```bash
openclaw plugins enable auth-manager-lease-telemetry
```

---

## 4) OpenClaw config (required)

Set in `~/.openclaw/openclaw.json`:

```json
{
  "plugins": {
    "allow": ["discord", "auth-manager-lease-telemetry"],
    "entries": {
      "auth-manager-lease-telemetry": {
        "enabled": true,
        "config": {
          "baseUrl": "https://your-auth-manager.example.com",
          "internalApiToken": "<TOKEN>",
          "machineId": "debian",
          "agentId": "main",
          "authFilePath": "~/.codex/auth.json",
          "requestedTtlSeconds": 1800,
          "refreshIntervalMs": 60000,
          "autoRenew": true,
          "autoRotate": true,
          "rotationPolicy": "recommended_or_required",
          "flushIntervalMs": 60000,
          "flushEveryRequests": 1
        }
      }
    }
  }
}
```

Notes:
- Keep `plugins.allow` explicit.
- Remove duplicate plugin directories from `~/.openclaw/extensions`.

---

## 5) Restart + verify

```bash
openclaw gateway restart
openclaw plugins list
```

Expected:
- plugin status `loaded`
- no `missing register/activate export`
- no `baseUrl is required...`

Check logs:

```bash
grep -Ei "lease|acquire|materializ|telemetry|auth-manager" /tmp/openclaw/openclaw-$(date +%F).log | tail -n 100
```

---

## 6) Important: make lease auth become the *current* OpenClaw auth

Current implementation in this repo primarily materializes auth to `~/.codex/auth.json`.

To make OpenClaw itself use lease auth as active auth, add an OpenClaw auth adapter that:

1. writes lease credentials into OpenClaw profile store (`~/.openclaw/agents/main/agent/auth-profiles.json`) under a dedicated profile id (e.g. `openai-codex:lease`),
2. updates active profile selection to that lease profile,
3. removes/invalidates old non-lease profiles if strict mode is enabled.

Recommended config flag:

```json
"enforceLeaseAsActiveAuth": true
```

And strict mode:

```json
"disallowNonLeaseAuth": true
```

---

## 7) Safe rollout checklist

- [ ] Plugin has `openclaw.extensions` and `openclaw.plugin.json`
- [ ] No duplicate plugin IDs in extensions path
- [ ] Plugin starts only after config validation
- [ ] Lease acquire succeeds
- [ ] Auth materialization succeeds
- [ ] Telemetry posts succeed
- [ ] Active OpenClaw auth switched to lease profile
- [ ] `/status` reflects lease-backed auth profile

---

## 8) Troubleshooting quick map

- `missing register/activate export`
  - `index.js` default export not plugin object.
- `baseUrl/internalApiToken/machineId required`
  - plugin config not reaching runtime; check `plugins.entries.<id>.config`.
- `Refusing insecure Auth Manager URL: http://...`
  - fallback URL being used too early or non-HTTPS URL.
- Plugin loaded but no lease/auth switch
  - lease/auth adapter logic not wired into OpenClaw auth profile activation.
