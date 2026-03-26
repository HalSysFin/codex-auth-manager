import test from 'node:test'
import assert from 'node:assert/strict'
import { normalizeBackendErrorPayload } from '../authManagerClient'
import { AuthManagerClient, AuthManagerClientError } from '../authManagerClient'

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
  let authHeader = ''
  const client = new AuthManagerClient({
    baseUrl: 'http://127.0.0.1:8080',
    internalApiToken: 'secret-token',
    allowInsecureLocalhost: true,
    fetchImpl: async (input, init) => {
      requestedUrl = String(input)
      authHeader = String((init?.headers as Record<string, string>).Authorization)
      return new Response(JSON.stringify({ status: 'ok', reason: null, lease: null, credential_material: null }), { status: 200 })
    },
  })

  await client.fetchAuthPayloadForLease('lease-123', {
    machineId: 'machine-a',
    agentId: 'vscode-extension',
  })

  assert.equal(requestedUrl, 'http://127.0.0.1:8080/api/leases/lease-123/materialize')
  assert.equal(authHeader, 'Bearer secret-token')
})

test('shared client surfaces invalid bearer token responses', async () => {
  const client = new AuthManagerClient({
    baseUrl: 'http://127.0.0.1:8080',
    internalApiToken: 'bad-token',
    allowInsecureLocalhost: true,
    fetchImpl: async () => new Response(JSON.stringify({ detail: 'Invalid bearer token' }), { status: 403 }),
  })

  await assert.rejects(
    client.getLease('lease-123'),
    (error: unknown) => error instanceof AuthManagerClientError && error.status === 403 && error.message === 'Invalid bearer token',
  )
})

test('shared client handles missing bearer token responses', async () => {
  const client = new AuthManagerClient({
    baseUrl: 'http://127.0.0.1:8080',
    internalApiToken: '',
    allowInsecureLocalhost: true,
    fetchImpl: async () => new Response(JSON.stringify({ detail: 'Missing bearer token' }), { status: 401 }),
  })

  await assert.rejects(
    client.acquireLease({
      machineId: 'machine-a',
      agentId: 'vscode-extension',
    }),
    (error: unknown) => error instanceof AuthManagerClientError && error.status === 401 && error.message === 'Missing bearer token',
  )
})
