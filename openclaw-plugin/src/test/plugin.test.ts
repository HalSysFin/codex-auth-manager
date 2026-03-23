import assert from 'node:assert/strict'
import test from 'node:test'

import { createOpenClawAuthManagerPlugin } from '../plugin.js'
import { normalizeUsageEvent } from '../usage.js'

test('normalizeUsageEvent maps OpenAI-style usage fields', () => {
  const event = normalizeUsageEvent({
    usage: {
      prompt_tokens: 120,
      completion_tokens: 30,
      total_tokens: 150,
    },
    model: 'gpt-5.4',
    status: 'healthy',
  })

  assert.equal(event.requestsCount, 1)
  assert.equal(event.tokensIn, 120)
  assert.equal(event.tokensOut, 30)
  assert.equal(event.metadata?.model, 'gpt-5.4')
})

test('plugin aggregates usage and posts telemetry', async () => {
  let postedBody: Record<string, unknown> | null = null
  const plugin = createOpenClawAuthManagerPlugin({
    baseUrl: 'http://127.0.0.1:8080',
    apiKey: 'test-token',
    context: {
      leaseId: 'lease_123',
      machineId: 'machine-a',
      agentId: 'openclaw',
    },
    fetchImpl: async (_input, init) => {
      postedBody = JSON.parse(String(init?.body ?? '{}')) as Record<string, unknown>
      return new Response(JSON.stringify({ status: 'ok' }), { status: 200 })
    },
    logger: { info() {}, warn() {}, error() {} },
  })

  plugin.observeUsage({
    usage: { prompt_tokens: 100, completion_tokens: 40, total_tokens: 140 },
    model: 'gpt-5.4',
  })
  plugin.observeUsage({
    requests_count: 2,
    tokens_in: 50,
    tokens_out: 10,
    source: 'openclaw',
  })

  await plugin.flushTelemetry()

  assert.ok(postedBody)
  const body = postedBody as Record<string, unknown>
  assert.equal(body.machine_id, 'machine-a')
  assert.equal(body.agent_id, 'openclaw')
  assert.equal(body.requests_count, 3)
  assert.equal(body.tokens_in, 150)
  assert.equal(body.tokens_out, 50)
})
