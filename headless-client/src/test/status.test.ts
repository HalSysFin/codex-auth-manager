import test from 'node:test'
import assert from 'node:assert/strict'
import { formatStatusLines } from '../output.js'
import { buildLeaseTelemetryPayload } from '../../../packages/lease-runtime/src/telemetry.js'
import { deriveLeaseHealthState, selectStartupAction } from '../../../packages/lease-runtime/src/leaseLifecycle.js'
import { healthParityCases, startupParityCases } from '../../../packages/lease-runtime/src/test/parityMatrix.ts'

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

test('selectStartupAction rotates replacement-required leases', () => {
  assert.equal(selectStartupAction({
    leaseId: 'lease-1',
    autoRotate: true,
    autoRenew: true,
    leaseStatus: {
      lease_id: 'lease-1',
      credential_id: 'cred-1',
      state: 'active',
      issued_at: '2026-03-23T00:00:00.000Z',
      expires_at: '2026-03-23T02:00:00.000Z',
      renewed_at: null,
      machine_id: 'machine-a',
      agent_id: 'headless-client',
      latest_telemetry_at: null,
      latest_utilization_pct: 91,
      latest_quota_remaining: 100,
      last_success_at: null,
      last_error_at: null,
      rotation_recommended: false,
      replacement_required: true,
      reason: null,
      credential_state: 'leased',
    },
  }), 'rotate')
})

test('formatStatusLines shows backend unavailable clearly', () => {
  const lines = formatStatusLines({
    backendReachable: false,
    healthState: 'backend_unavailable',
    leaseState: null,
    leaseId: null,
    credentialId: null,
    expiresAt: null,
    latestUtilizationPct: null,
    latestQuotaRemaining: null,
    lastBackendRefreshAt: null,
    lastAuthWriteAt: null,
  })
  assert.equal(lines[0], 'Health: backend_unavailable')
  assert.ok(lines.includes('Backend reachable: no'))
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

test('headless client matches the shared startup parity matrix', () => {
  for (const scenario of startupParityCases) {
    assert.equal(
      selectStartupAction({
        leaseId: scenario.leaseId,
        autoRotate: true,
        autoRenew: true,
        leaseStatus: scenario.leaseStatus,
        now: new Date('2026-03-23T00:00:00.000Z'),
      }),
      scenario.expectedAction,
      scenario.name,
    )
  }
})

test('headless client matches the shared health parity matrix', () => {
  for (const scenario of healthParityCases) {
    assert.equal(
      deriveLeaseHealthState(scenario.leaseStatus, new Date('2026-03-23T00:00:00.000Z')),
      scenario.expectedHealth,
      scenario.name,
    )
  }
})
