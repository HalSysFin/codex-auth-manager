import assert from 'node:assert/strict'
import test from 'node:test'

import { AuthManagerTelemetryClient } from '../client.js'

test('telemetry client sends Authorization bearer header from internalApiToken', async () => {
  let authHeader = ''
  const client = new AuthManagerTelemetryClient({
    baseUrl: 'http://127.0.0.1:8080',
    internalApiToken: 'secret-token',
    allowInsecureLocalhost: true,
    fetchImpl: async (_input, init) => {
      authHeader = String((init?.headers as Record<string, string>).Authorization)
      return new Response(JSON.stringify({ status: 'ok' }), { status: 200 })
    },
  })

  await client.postLeaseTelemetry(
    { leaseId: 'lease-1', machineId: 'machine-a', agentId: 'openclaw' },
    {
      machine_id: 'machine-a',
      agent_id: 'openclaw',
      captured_at: '2026-03-23T00:00:00.000Z',
      status: 'healthy',
    },
  )

  assert.equal(authHeader, 'Bearer secret-token')
})

test('telemetry client surfaces missing token responses cleanly', async () => {
  const client = new AuthManagerTelemetryClient({
    baseUrl: 'http://127.0.0.1:8080',
    internalApiToken: '',
    allowInsecureLocalhost: true,
    fetchImpl: async () => new Response(JSON.stringify({ detail: 'Missing bearer token' }), { status: 401 }),
  })

  await assert.rejects(
    client.postLeaseTelemetry(
      { leaseId: 'lease-1', machineId: 'machine-a', agentId: 'openclaw' },
      {
        machine_id: 'machine-a',
        agent_id: 'openclaw',
        captured_at: '2026-03-23T00:00:00.000Z',
        status: 'healthy',
      },
    ),
    /Missing bearer token/,
  )
})
