import assert from 'node:assert/strict'
import test from 'node:test'

import { resolvePluginConfig, validatePluginConfig } from '../config.js'

test('resolvePluginConfig prefers explicit config and applies defaults', () => {
  const config = resolvePluginConfig(
    {
      baseUrl: 'http://127.0.0.1:8080',
      internalApiToken: 'secret',
      machineId: 'machine-a',
      flushEveryRequests: 5,
    },
    {},
  )

  assert.equal(config.baseUrl, 'http://127.0.0.1:8080')
  assert.equal(config.internalApiToken, 'secret')
  assert.equal(config.machineId, 'machine-a')
  assert.equal(config.agentId, 'openclaw')
  assert.equal(config.flushEveryRequests, 5)
  assert.equal(config.authFilePath, '~/.codex/auth.json')
  assert.equal(config.requestedTtlSeconds, 1800)
  assert.equal(config.leaseProfileId, 'openai-codex:lease')
  assert.equal(config.enforceLeaseAsActiveAuth, true)
  assert.equal(config.disallowNonLeaseAuth, false)
  assert.equal(config.purgeNonLeaseProfilesOnStart, false)
  assert.equal(config.autoRenew, true)
  assert.equal(config.autoRotate, false)
  assert.equal(config.releaseLeaseOnShutdown, false)
  assert.equal(config.enabled, true)
})

test('resolvePluginConfig falls back to AUTH_MANAGER_INTERNAL_API_TOKEN env', () => {
  const config = resolvePluginConfig(undefined, {
    AUTH_MANAGER_BASE_URL: 'http://127.0.0.1:8080',
    AUTH_MANAGER_INTERNAL_API_TOKEN: 'env-secret',
    AUTH_MANAGER_MACHINE_ID: 'machine-a',
    AUTH_MANAGER_AGENT_ID: 'openclaw',
  })

  assert.equal(config.internalApiToken, 'env-secret')
})

test('validatePluginConfig reports missing required fields', () => {
  const errors = validatePluginConfig({
    baseUrl: '',
    internalApiToken: '',
    machineId: '',
    agentId: '',
    authFilePath: '~/.codex/auth.json',
    leaseProfileId: 'openai-codex:lease',
    enforceLeaseAsActiveAuth: true,
    disallowNonLeaseAuth: false,
    purgeNonLeaseProfilesOnStart: false,
    flushIntervalMs: 500,
    flushEveryRequests: 0,
    refreshIntervalMs: 60_000,
    requestedTtlSeconds: 1800,
    autoRenew: true,
    autoRotate: true,
    rotationPolicy: 'replacement_required_only',
    allowInsecureLocalhost: true,
    releaseLeaseOnShutdown: true,
    enabled: true,
  })

  assert.ok(errors.includes('baseUrl is required'))
  assert.ok(errors.includes('internalApiToken is required'))
  assert.ok(errors.includes('machineId is required'))
  assert.ok(errors.includes('agentId is required'))
})
