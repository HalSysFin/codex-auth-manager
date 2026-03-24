import assert from 'node:assert/strict'
import * as fs from 'node:fs/promises'
import * as os from 'node:os'
import * as path from 'node:path'
import test from 'node:test'

import { createOpenClawLeaseTelemetryService } from '../service.js'

test('service flushes after request threshold', async () => {
  let posts = 0
  let postedBody: Record<string, unknown> | null = null
  const service = createOpenClawLeaseTelemetryService({
    baseUrl: 'http://127.0.0.1:8080',
    internalApiToken: 'secret',
    allowInsecureLocalhost: true,
    context: {
      leaseId: 'lease_1',
      machineId: 'machine-a',
      agentId: 'openclaw',
    },
    flushEveryRequests: 2,
    fetchImpl: async (_input, init) => {
      posts += 1
      postedBody = JSON.parse(String(init?.body ?? '{}')) as Record<string, unknown>
      return new Response(JSON.stringify({ status: 'ok' }), { status: 200 })
    },
    logger: { info() {}, warn() {}, error() {} },
  })

  service.observeUsage({ usage: { prompt_tokens: 10, completion_tokens: 5, total_tokens: 15 } })
  service.observeUsage({ usage: { prompt_tokens: 20, completion_tokens: 6, total_tokens: 26 } })
  await new Promise((resolve) => setTimeout(resolve, 20))

  assert.equal(posts, 1)
  assert.ok(postedBody)
  const body = postedBody as unknown as Record<string, unknown>
  assert.equal(body.requests_count, 2)
  assert.equal(body.tokens_in, 30)
  assert.equal(body.tokens_out, 11)
})

test('changing lease context resets pending totals before the next flush', async () => {
  const leasePosts: Array<{ leaseId: string; body: Record<string, unknown> }> = []
  const service = createOpenClawLeaseTelemetryService({
    baseUrl: 'http://127.0.0.1:8080',
    internalApiToken: 'secret',
    allowInsecureLocalhost: true,
    context: {
      leaseId: 'lease_1',
      machineId: 'machine-a',
      agentId: 'openclaw',
    },
    flushEveryRequests: 99,
    fetchImpl: async (input, init) => {
      leasePosts.push({
        leaseId: String(input).split('/').at(-2) ?? 'unknown',
        body: JSON.parse(String(init?.body ?? '{}')) as Record<string, unknown>,
      })
      return new Response(JSON.stringify({ status: 'ok' }), { status: 200 })
    },
    logger: { info() {}, warn() {}, error() {} },
  })

  service.observeUsage({ usage: { prompt_tokens: 10, completion_tokens: 5, total_tokens: 15 } })
  service.setLeaseContext({
    leaseId: 'lease_2',
    machineId: 'machine-a',
    agentId: 'openclaw',
  })
  service.observeUsage({ usage: { prompt_tokens: 7, completion_tokens: 3, total_tokens: 10 } })
  await service.flushNow()

  assert.equal(leasePosts.length, 1)
  assert.equal(leasePosts[0].leaseId, 'lease_2')
  assert.equal(leasePosts[0].body.requests_count, 1)
  assert.equal(leasePosts[0].body.tokens_in, 7)
  assert.equal(leasePosts[0].body.tokens_out, 3)
})

test('stop flushes outstanding telemetry once before shutting down', async () => {
  const posts: Array<Record<string, unknown>> = []
  const service = createOpenClawLeaseTelemetryService({
    baseUrl: 'http://127.0.0.1:8080',
    internalApiToken: 'secret',
    allowInsecureLocalhost: true,
    context: {
      leaseId: 'lease_1',
      machineId: 'machine-a',
      agentId: 'openclaw',
    },
    flushEveryRequests: 99,
    fetchImpl: async (_input, init) => {
      posts.push(JSON.parse(String(init?.body ?? '{}')) as Record<string, unknown>)
      return new Response(JSON.stringify({ status: 'ok' }), { status: 200 })
    },
    logger: { info() {}, warn() {}, error() {} },
  })

  service.observeUsage({ usage: { prompt_tokens: 9, completion_tokens: 4, total_tokens: 13 } })
  await service.flushNow()
  service.stop()

  assert.equal(posts.length, 1)
  assert.equal(posts[0].requests_count, 1)
  assert.equal(posts[0].tokens_in, 9)
  assert.equal(posts[0].tokens_out, 4)
})

test('service start acquires a lease, materializes auth, and writes auth.json', async () => {
  const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'openclaw-plugin-'))
  const authPath = path.join(tempDir, 'auth.json')
  const calls: string[] = []
  const service = createOpenClawLeaseTelemetryService({
    baseUrl: 'http://127.0.0.1:8080',
    internalApiToken: 'secret',
    allowInsecureLocalhost: true,
    authFilePath: authPath,
    releaseLeaseOnShutdown: false,
    context: {
      leaseId: 'lease_missing',
      machineId: 'machine-a',
      agentId: 'openclaw',
    },
    fetchImpl: async (input, init) => {
      const url = String(input)
      calls.push(`${init?.method ?? 'GET'} ${url}`)
      if (url.endsWith('/api/leases/lease_missing')) {
        return new Response(JSON.stringify({ detail: 'Lease not found' }), { status: 404 })
      }
      if (url.endsWith('/api/leases/acquire')) {
        return new Response(
          JSON.stringify({
            status: 'ok',
            reason: null,
            lease: {
              id: 'lease_new',
              credential_id: 'cred_1',
              machine_id: 'machine-a',
              agent_id: 'openclaw',
              state: 'active',
              issued_at: '2026-03-24T00:00:00.000Z',
              expires_at: '2026-03-24T00:30:00.000Z',
              renewed_at: null,
              revoked_at: null,
              released_at: null,
              rotation_reason: null,
              replacement_lease_id: null,
              last_seen_at: '2026-03-24T00:00:00.000Z',
              last_telemetry_at: null,
              latest_utilization_pct: 5,
              latest_quota_remaining: 95,
              last_success_at: null,
              last_error_at: null,
              reason: null,
              metadata: null,
              created_at: '2026-03-24T00:00:00.000Z',
              updated_at: '2026-03-24T00:00:00.000Z',
            },
          }),
          { status: 200 },
        )
      }
      if (url.endsWith('/api/leases/lease_new/materialize')) {
        return new Response(
          JSON.stringify({
            status: 'ok',
            reason: null,
            lease: {
              id: 'lease_new',
              credential_id: 'cred_1',
              machine_id: 'machine-a',
              agent_id: 'openclaw',
              state: 'active',
              issued_at: '2026-03-24T00:00:00.000Z',
              expires_at: '2026-03-24T00:30:00.000Z',
              renewed_at: null,
              revoked_at: null,
              released_at: null,
              rotation_reason: null,
              replacement_lease_id: null,
              last_seen_at: '2026-03-24T00:00:00.000Z',
              last_telemetry_at: null,
              latest_utilization_pct: 5,
              latest_quota_remaining: 95,
              last_success_at: null,
              last_error_at: null,
              reason: null,
              metadata: null,
              created_at: '2026-03-24T00:00:00.000Z',
              updated_at: '2026-03-24T00:00:00.000Z',
            },
            credential_material: {
              label: 'test',
              auth_json: {
                auth_mode: 'chatgpt',
                OPENAI_API_KEY: null,
                tokens: {
                  id_token: 'id-token',
                  access_token: 'access-token',
                  refresh_token: 'refresh-token',
                  account_id: 'acct-1',
                },
              },
            },
          }),
          { status: 200 },
        )
      }
      throw new Error(`Unexpected request: ${url}`)
    },
    logger: { info() {}, warn() {}, error() {} },
  })

  await service.start()
  await service.shutdown()

  const written = JSON.parse(await fs.readFile(authPath, 'utf8')) as Record<string, unknown>
  assert.equal((written.tokens as Record<string, unknown>).id_token, 'id-token')
  assert.ok(calls.some((entry) => entry.includes('/api/leases/acquire')))
  assert.ok(calls.some((entry) => entry.includes('/materialize')))
  await fs.rm(tempDir, { recursive: true, force: true })
})
