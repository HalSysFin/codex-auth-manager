import test from 'node:test'
import assert from 'node:assert/strict'
import { normalizeBackendErrorPayload } from '../authManagerClient'
import { AuthManagerClient } from '../authManagerClient'

test('normalizeBackendErrorPayload prefers detail', () => {
  assert.deepEqual(normalizeBackendErrorPayload({ detail: 'Bad token' }), { message: 'Bad token' })
})

test('normalizeBackendErrorPayload falls back to reason', () => {
  assert.deepEqual(normalizeBackendErrorPayload({ reason: 'no_eligible_credentials_available' }), {
    message: 'no_eligible_credentials_available',
    code: 'no_eligible_credentials_available',
  })
})

test('materialize uses shared materialize endpoint path', async () => {
  let requestedUrl = ''
  const client = new AuthManagerClient({
    baseUrl: 'http://127.0.0.1:8080',
    allowInsecureLocalhost: true,
    fetchImpl: async (input) => {
      requestedUrl = String(input)
      return new Response(JSON.stringify({ status: 'ok', reason: null, lease: null, credential_material: null }), { status: 200 })
    },
  })

  await client.fetchAuthPayloadForLease('lease-123', {
    machineId: 'machine-a',
    agentId: 'vscode-extension',
  })

  assert.equal(requestedUrl, 'http://127.0.0.1:8080/api/leases/lease-123/materialize')
})
