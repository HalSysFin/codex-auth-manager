import assert from 'node:assert/strict'
import test from 'node:test'

import { createOpenClawLeaseTelemetryService } from '../service.js'

test('service flushes after request threshold', async () => {
  let posts = 0
  let postedBody: Record<string, unknown> | null = null
  const service = createOpenClawLeaseTelemetryService({
    baseUrl: 'http://127.0.0.1:8080',
    internalApiToken: 'secret',
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
