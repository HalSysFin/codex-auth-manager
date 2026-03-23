import test from 'node:test'
import assert from 'node:assert/strict'
import { formatStatusLines } from '../output.js'
import { buildLeaseTelemetryPayload } from '../../../packages/lease-runtime/src/telemetry.js'
import { selectStartupAction } from '../../../packages/lease-runtime/src/leaseLifecycle.js'

test('formatStatusLines renders readable status output', () => {
  const lines = formatStatusLines({
    backendReachable: true,
    healthState: 'active',
    leaseState: 'active',
    leaseId: 'lease-1',
    credentialId: 'cred-1',
    expiresAt: '2026-03-23T00:00:00.000Z',
    latestUtilizationPct: 12,
    latestQuotaRemaining: 900,
    lastBackendRefreshAt: '2026-03-23T00:01:00.000Z',
    lastAuthWriteAt: '2026-03-23T00:02:00.000Z',
  })
  assert.equal(lines[0], 'Health: active')
  assert.ok(lines.some((line) => line.includes('Lease id: lease-1')))
})

test('selectStartupAction reacquires revoked leases', () => {
  assert.equal(selectStartupAction({
    leaseId: 'lease-1',
    autoRotate: true,
    autoRenew: true,
    leaseStatus: {
      lease_id: 'lease-1',
      credential_id: 'cred-1',
      state: 'revoked',
      issued_at: '2026-03-23T00:00:00.000Z',
      expires_at: '2026-03-23T02:00:00.000Z',
      renewed_at: null,
      machine_id: 'machine-a',
      agent_id: 'headless-client',
      latest_telemetry_at: null,
      latest_utilization_pct: null,
      latest_quota_remaining: null,
      last_success_at: null,
      last_error_at: null,
      rotation_recommended: false,
      replacement_required: false,
      reason: null,
      credential_state: 'revoked',
    },
  }), 'reacquire')
})

test('buildLeaseTelemetryPayload stays truthful', () => {
  const payload = buildLeaseTelemetryPayload({
    machineId: 'machine-a',
    agentId: 'headless-client',
    leaseId: 'lease-1',
    credentialId: 'cred-1',
    issuedAt: null,
    expiresAt: null,
    leaseState: 'active',
    latestTelemetryAt: null,
    latestUtilizationPct: 50,
    latestQuotaRemaining: 500,
    lastAuthWriteAt: null,
    lastBackendRefreshAt: '2026-03-23T00:00:00.000Z',
    replacementRequired: false,
    rotationRecommended: false,
    lastErrorAt: null,
    authFilePath: '~/.codex/auth.json',
  }, '2026-03-23T00:05:00.000Z')
  assert.equal(payload.utilization_pct, 50)
  assert.equal(payload.requests_count, null)
})
