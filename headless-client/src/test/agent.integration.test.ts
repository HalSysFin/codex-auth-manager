import test from 'node:test'
import assert from 'node:assert/strict'
import * as fs from 'node:fs/promises'
import * as os from 'node:os'
import * as path from 'node:path'

import { createStateDocument } from '../config.js'
import { HeadlessAgent } from '../agent.js'
import { defaultRuntimeSettings } from '../../../packages/lease-runtime/src/runtimeState.js'

class MemoryOutput {
  readonly infos: string[] = []
  readonly warns: string[] = []
  readonly errors: string[] = []

  info(message: string): void {
    this.infos.push(message)
  }

  warn(message: string): void {
    this.warns.push(message)
  }

  error(message: string): void {
    this.errors.push(message)
  }
}

function buildLease(id: string, overrides: Record<string, unknown> = {}) {
  return {
    id,
    credential_id: `cred-${id}`,
    machine_id: 'linux-machine',
    agent_id: 'headless-client',
    state: 'active',
    issued_at: '2026-03-23T00:00:00.000Z',
    expires_at: '2099-03-23T01:00:00.000Z',
    renewed_at: null,
    revoked_at: null,
    released_at: null,
    rotation_reason: null,
    replacement_lease_id: null,
    last_telemetry_at: null,
    latest_utilization_pct: 10,
    latest_quota_remaining: 990,
    last_success_at: null,
    last_error_at: null,
    reason: null,
    metadata: null,
    created_at: '2026-03-23T00:00:00.000Z',
    updated_at: '2026-03-23T00:00:00.000Z',
    ...overrides,
  }
}

function authPayload(accountId: string) {
  return {
    auth_mode: 'chatgpt',
    OPENAI_API_KEY: null,
    tokens: {
      id_token: `id-${accountId}`,
      access_token: `access-${accountId}`,
      refresh_token: `refresh-${accountId}`,
      account_id: accountId,
    },
  }
}

async function withTempAgent(
  run: (params: {
    root: string
    output: MemoryOutput
    config: {
      settings: ReturnType<typeof defaultRuntimeSettings>
      paths: {
        configDir: string
        stateDir: string
        configFile: string
        stateFile: string
        logFile: string
      }
    }
    document: ReturnType<typeof createStateDocument>
  }) => Promise<void>,
) {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'headless-agent-int-'))
  try {
    const settings = {
      ...defaultRuntimeSettings(),
      baseUrl: 'http://127.0.0.1:8080',
      internalApiToken: 'secret-token',
      machineId: 'linux-machine',
      agentId: 'headless-client',
      authFilePath: path.join(root, 'nested', 'auth.json'),
      allowInsecureLocalhost: true,
    }
    const config = {
      settings,
      paths: {
        configDir: path.join(root, 'config'),
        stateDir: path.join(root, 'state'),
        configFile: path.join(root, 'config', 'config.json'),
        stateFile: path.join(root, 'state', 'state.json'),
        logFile: path.join(root, 'state', 'agent.log'),
      },
    }
    const document = createStateDocument(null, settings)
    const output = new MemoryOutput()
    await run({ root, output, config, document })
  } finally {
    await fs.rm(root, { recursive: true, force: true })
  }
}

test('headless ensure acquires, materializes, and writes auth file', async () => {
  await withTempAgent(async ({ config, document }) => {
    const originalFetch = globalThis.fetch
    globalThis.fetch = (async (input: RequestInfo | URL) => {
      const url = String(input)
      if (url.endsWith('/api/leases/acquire')) {
        return new Response(JSON.stringify({ status: 'ok', reason: null, lease: buildLease('lease-1') }), { status: 200 })
      }
      if (url.endsWith('/api/leases/lease-1/materialize')) {
        return new Response(JSON.stringify({
          status: 'ok',
          reason: null,
          lease: buildLease('lease-1'),
          credential_material: {
            auth_json: authPayload('acct-1'),
          },
        }), { status: 200 })
      }
      throw new Error(`Unexpected fetch: ${url}`)
    }) as typeof fetch

    try {
      const agent = new HeadlessAgent(config, document, new MemoryOutput())
      const summary = await agent.ensure()
      assert.equal(summary.lease.leaseId, 'lease-1')
      const written = JSON.parse(await fs.readFile(config.settings.authFilePath, 'utf8'))
      assert.equal(written.tokens.account_id, 'acct-1')
      assert.ok(summary.lease.lastAuthWriteAt)
    } finally {
      globalThis.fetch = originalFetch
    }
  })
})

test('headless refresh reacquires revoked leases and rewrites auth file', async () => {
  await withTempAgent(async ({ config, document }) => {
    document.lease.leaseId = 'lease-old'
    document.lease.leaseState = 'active'
    document.lease.expiresAt = '2099-03-23T01:00:00.000Z'

    const originalFetch = globalThis.fetch
    globalThis.fetch = (async (input: RequestInfo | URL) => {
      const url = String(input)
      if (url.endsWith('/api/leases/lease-old')) {
        return new Response(JSON.stringify({
          lease_id: 'lease-old',
          credential_id: 'cred-lease-old',
          state: 'revoked',
          issued_at: '2026-03-23T00:00:00.000Z',
          expires_at: '2099-03-23T01:00:00.000Z',
          renewed_at: null,
          machine_id: 'linux-machine',
          agent_id: 'headless-client',
          latest_telemetry_at: null,
          latest_utilization_pct: 95,
          latest_quota_remaining: 5,
          last_success_at: null,
          last_error_at: null,
          rotation_recommended: false,
          replacement_required: false,
          reason: 'credential_exhausted',
          credential_state: 'revoked',
        }), { status: 200 })
      }
      if (url.endsWith('/api/leases/acquire')) {
        return new Response(JSON.stringify({ status: 'ok', reason: null, lease: buildLease('lease-new') }), { status: 200 })
      }
      if (url.endsWith('/api/leases/lease-new/materialize')) {
        return new Response(JSON.stringify({
          status: 'ok',
          reason: null,
          lease: buildLease('lease-new'),
          credential_material: {
            auth_json: authPayload('acct-2'),
          },
        }), { status: 200 })
      }
      throw new Error(`Unexpected fetch: ${url}`)
    }) as typeof fetch

    try {
      const agent = new HeadlessAgent(config, document, new MemoryOutput())
      const summary = await agent.refresh()
      assert.equal(summary.lease.leaseId, 'lease-new')
      const written = JSON.parse(await fs.readFile(config.settings.authFilePath, 'utf8'))
      assert.equal(written.tokens.account_id, 'acct-2')
    } finally {
      globalThis.fetch = originalFetch
    }
  })
})

test('headless ensure reports denied acquire without fake success', async () => {
  await withTempAgent(async ({ config, document, output }) => {
    const originalFetch = globalThis.fetch
    globalThis.fetch = (async (input: RequestInfo | URL) => {
      const url = String(input)
      if (url.endsWith('/api/leases/acquire')) {
        return new Response(JSON.stringify({ status: 'denied', reason: 'no_eligible_credentials_available', lease: null }), { status: 409 })
      }
      throw new Error(`Unexpected fetch: ${url}`)
    }) as typeof fetch

    try {
      const agent = new HeadlessAgent(config, document, output)
      const summary = await agent.ensure()
      assert.equal(summary.lease.leaseId, null)
      assert.match(summary.message ?? '', /no_eligible_credentials_available/)
      assert.equal(summary.backendReachable, true)
    } finally {
      globalThis.fetch = originalFetch
    }
  })
})

test('headless refresh marks backend unavailable on transport failure', async () => {
  await withTempAgent(async ({ config, document, output }) => {
    document.lease.leaseId = 'lease-1'
    document.lease.leaseState = 'active'
    document.lease.expiresAt = '2099-03-23T01:00:00.000Z'

    const originalFetch = globalThis.fetch
    globalThis.fetch = (async () => {
      throw new Error('connect ECONNREFUSED')
    }) as typeof fetch

    try {
      const agent = new HeadlessAgent(config, document, output)
      const summary = await agent.refresh()
      assert.equal(summary.backendReachable, false)
      assert.equal(summary.healthState, 'backend_unavailable')
      assert.match(summary.message ?? '', /ECONNREFUSED/)
    } finally {
      globalThis.fetch = originalFetch
    }
  })
})
