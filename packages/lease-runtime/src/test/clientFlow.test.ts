import test from 'node:test'
import assert from 'node:assert/strict'

import { AuthManagerClient } from '../authManagerClient.js'
import { prepareAuthPayloadForWrite, validateAuthPayload } from '../authPayload.js'
import { AuthManagerClientError } from '../authManagerClient.js'

test('acquire plus materialize returns a writable auth payload', async () => {
  const seenPaths: string[] = []
  const client = new AuthManagerClient({
    baseUrl: 'http://127.0.0.1:8080',
    internalApiToken: 'secret-token',
    allowInsecureLocalhost: true,
    fetchImpl: async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input)
      seenPaths.push(url)
      if (url.endsWith('/api/leases/acquire')) {
        return new Response(JSON.stringify({
          status: 'ok',
          reason: null,
          lease: {
            id: 'lease-1',
            credential_id: 'cred-1',
            machine_id: 'machine-a',
            agent_id: 'agent-a',
            state: 'active',
            issued_at: '2026-03-23T00:00:00.000Z',
            expires_at: '2026-03-23T01:00:00.000Z',
            renewed_at: null,
            revoked_at: null,
            released_at: null,
            rotation_reason: null,
            replacement_lease_id: null,
            last_telemetry_at: null,
            latest_utilization_pct: null,
            latest_quota_remaining: null,
            last_success_at: null,
            last_error_at: null,
            reason: 'integration test',
            metadata: null,
            created_at: '2026-03-23T00:00:00.000Z',
            updated_at: '2026-03-23T00:00:00.000Z',
          },
        }), { status: 200 })
      }

      assert.equal(init?.method, 'POST')
      return new Response(JSON.stringify({
        status: 'ok',
        reason: null,
        lease: {
          id: 'lease-1',
          credential_id: 'cred-1',
          machine_id: 'machine-a',
          agent_id: 'agent-a',
          state: 'active',
          issued_at: '2026-03-23T00:00:00.000Z',
          expires_at: '2026-03-23T01:00:00.000Z',
          renewed_at: null,
          revoked_at: null,
          released_at: null,
          rotation_reason: null,
          replacement_lease_id: null,
          last_telemetry_at: null,
          latest_utilization_pct: null,
          latest_quota_remaining: null,
          last_success_at: null,
          last_error_at: null,
          reason: null,
          metadata: null,
          created_at: '2026-03-23T00:00:00.000Z',
          updated_at: '2026-03-23T00:00:00.000Z',
        },
        credential_material: {
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
      }), { status: 200 })
    },
  })

  const acquire = await client.acquireLease({
    machineId: 'machine-a',
    agentId: 'agent-a',
    requestedTtlSeconds: 1800,
    reason: 'integration test',
  })
  const materialize = await client.materializeLease('lease-1', {
    machineId: 'machine-a',
    agentId: 'agent-a',
  })

  assert.equal(acquire.status, 'ok')
  assert.ok(seenPaths.some((url) => url.endsWith('/api/leases/acquire')))
  assert.ok(seenPaths.some((url) => url.endsWith('/api/leases/lease-1/materialize')))
  assert.equal(validateAuthPayload(materialize.credential_material?.auth_json), true)

  const prepared = prepareAuthPayloadForWrite(
    materialize.credential_material!.auth_json!,
    '2026-03-23T00:05:00.000Z',
  )
  assert.equal(prepared.last_refresh, '2026-03-23T00:05:00.000Z')
})

test('telemetry post preserves truthful null counters when client has no token data', async () => {
  let postedBody: Record<string, unknown> | null = null
  const client = new AuthManagerClient({
    baseUrl: 'http://127.0.0.1:8080',
    internalApiToken: 'secret-token',
    allowInsecureLocalhost: true,
    fetchImpl: async (_input: RequestInfo | URL, init?: RequestInit) => {
      postedBody = JSON.parse(String(init?.body ?? '{}')) as Record<string, unknown>
      return new Response(JSON.stringify({ status: 'ok', reason: null, lease: null }), { status: 200 })
    },
  })

  await client.postTelemetry('lease-1', {
    machine_id: 'machine-a',
    agent_id: 'agent-a',
    captured_at: '2026-03-23T00:10:00.000Z',
    status: 'ok',
    requests_count: null,
    tokens_in: null,
    tokens_out: null,
    utilization_pct: 42,
    quota_remaining: 580,
    last_success_at: '2026-03-23T00:09:00.000Z',
    last_error_at: null,
    rate_limit_remaining: null,
    error_rate_1h: null,
  })

  assert.ok(postedBody)
  const body = postedBody as Record<string, unknown>
  assert.equal(body.requests_count, null)
  assert.equal(body.tokens_in, null)
  assert.equal(body.tokens_out, null)
  assert.equal(body.utilization_pct, 42)
})

test('reconcile auth posts local auth payload to the lease endpoint', async () => {
  let requestedUrl = ''
  let requestedBody: Record<string, unknown> | null = null
  const client = new AuthManagerClient({
    baseUrl: 'http://127.0.0.1:8080',
    internalApiToken: 'secret-token',
    allowInsecureLocalhost: true,
    fetchImpl: async (input: RequestInfo | URL, init?: RequestInit) => {
      requestedUrl = String(input)
      requestedBody = JSON.parse(String(init?.body ?? '{}')) as Record<string, unknown>
      return new Response(JSON.stringify({
        status: 'ok',
        decision: 'client_updated_manager',
        reason: 'manager_updated',
        profile_label: 'alice',
        credential_auth_updated_at: '2026-03-23T00:11:00.000Z',
        auth_json: null,
      }), { status: 200 })
    },
  })

  const result = await client.reconcileLeaseAuth('lease-1', {
    machineId: 'machine-a',
    agentId: 'agent-a',
    authJson: {
      auth_mode: 'chatgpt',
      OPENAI_API_KEY: null,
      tokens: {
        id_token: 'id-token',
        access_token: 'access-token',
        refresh_token: 'refresh-token',
        account_id: 'acct-1',
      },
      last_refresh: '2026-03-23T00:10:00.000Z',
    },
  })

  assert.equal(requestedUrl, 'http://127.0.0.1:8080/api/leases/lease-1/reconcile-auth')
  assert.deepEqual(requestedBody, {
    machine_id: 'machine-a',
    agent_id: 'agent-a',
    auth_json: {
      auth_mode: 'chatgpt',
      OPENAI_API_KEY: null,
      tokens: {
        id_token: 'id-token',
        access_token: 'access-token',
        refresh_token: 'refresh-token',
        account_id: 'acct-1',
      },
      last_refresh: '2026-03-23T00:10:00.000Z',
    },
  })
  assert.equal(result.decision, 'client_updated_manager')
})

test('rotate plus materialize returns the replacement auth payload', async () => {
  const client = new AuthManagerClient({
    baseUrl: 'http://127.0.0.1:8080',
    internalApiToken: 'secret-token',
    allowInsecureLocalhost: true,
    fetchImpl: async (input: RequestInfo | URL) => {
      const url = String(input)
      if (url.endsWith('/api/leases/rotate')) {
        return new Response(JSON.stringify({
          status: 'ok',
          reason: null,
          lease: {
            id: 'lease-2',
            credential_id: 'cred-2',
            machine_id: 'machine-a',
            agent_id: 'agent-a',
            state: 'active',
            issued_at: '2026-03-23T00:00:00.000Z',
            expires_at: '2026-03-23T01:00:00.000Z',
            renewed_at: null,
            revoked_at: null,
            released_at: null,
            rotation_reason: 'approaching_utilization_threshold',
            replacement_lease_id: null,
            last_telemetry_at: null,
            latest_utilization_pct: 5,
            latest_quota_remaining: 995,
            last_success_at: null,
            last_error_at: null,
            reason: null,
            metadata: null,
            created_at: '2026-03-23T00:00:00.000Z',
            updated_at: '2026-03-23T00:00:00.000Z',
          },
        }), { status: 200 })
      }
      return new Response(JSON.stringify({
        status: 'ok',
        reason: null,
        lease: {
          id: 'lease-2',
          credential_id: 'cred-2',
          machine_id: 'machine-a',
          agent_id: 'agent-a',
          state: 'active',
          issued_at: '2026-03-23T00:00:00.000Z',
          expires_at: '2026-03-23T01:00:00.000Z',
          renewed_at: null,
          revoked_at: null,
          released_at: null,
          rotation_reason: 'approaching_utilization_threshold',
          replacement_lease_id: null,
          last_telemetry_at: null,
          latest_utilization_pct: 5,
          latest_quota_remaining: 995,
          last_success_at: null,
          last_error_at: null,
          reason: null,
          metadata: null,
          created_at: '2026-03-23T00:00:00.000Z',
          updated_at: '2026-03-23T00:00:00.000Z',
        },
        credential_material: {
          auth_json: {
            auth_mode: 'chatgpt',
            OPENAI_API_KEY: null,
            tokens: {
              id_token: 'id-token-2',
              access_token: 'access-token-2',
              refresh_token: 'refresh-token-2',
              account_id: 'acct-2',
            },
          },
        },
      }), { status: 200 })
    },
  })

  const rotated = await client.rotateLease({
    leaseId: 'lease-1',
    machineId: 'machine-a',
    agentId: 'agent-a',
    reason: 'approaching_utilization_threshold',
  })
  const materialized = await client.materializeLease(rotated.lease!.id, {
    machineId: 'machine-a',
    agentId: 'agent-a',
  })

  assert.equal(rotated.lease?.id, 'lease-2')
  assert.equal(materialized.credential_material?.auth_json?.tokens.account_id, 'acct-2')
})

test('backend denial preserves no eligible credentials reason', async () => {
  const client = new AuthManagerClient({
    baseUrl: 'http://127.0.0.1:8080',
    internalApiToken: 'secret-token',
    allowInsecureLocalhost: true,
    fetchImpl: async () =>
      new Response(
        JSON.stringify({
          reason: 'no_eligible_credentials_available',
        }),
        { status: 409 },
      ),
  })

  await assert.rejects(
    () =>
      client.acquireLease({
        machineId: 'machine-a',
        agentId: 'agent-a',
      }),
    (error: unknown) =>
      error instanceof AuthManagerClientError &&
      error.status === 409 &&
      error.code === 'no_eligible_credentials_available',
  )
})

test('backend unavailable errors surface without fake success payloads', async () => {
  const client = new AuthManagerClient({
    baseUrl: 'http://127.0.0.1:8080',
    internalApiToken: 'secret-token',
    allowInsecureLocalhost: true,
    fetchImpl: async () => {
      throw new Error('connect ECONNREFUSED')
    },
  })

  await assert.rejects(
    () =>
      client.getLease('lease-1'),
    /ECONNREFUSED/,
  )
})
