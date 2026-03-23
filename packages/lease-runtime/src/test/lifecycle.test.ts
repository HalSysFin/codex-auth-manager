import test from 'node:test'
import assert from 'node:assert/strict'

import {
  deriveLeaseHealthState,
  selectStartupAction,
  shouldReacquireAfterLookupError,
} from '../leaseLifecycle.ts'
import type { LeaseStatusResponse } from '../types.ts'
import { deniedReason, healthParityCases, startupParityCases } from './parityMatrix.ts'

function buildLeaseStatus(overrides: Partial<LeaseStatusResponse> = {}): LeaseStatusResponse {
  return {
    lease_id: 'lease-1',
    credential_id: 'cred-1',
    state: 'active',
    issued_at: '2026-03-23T00:00:00.000Z',
    expires_at: '2026-03-23T02:00:00.000Z',
    renewed_at: null,
    machine_id: 'machine-a',
    agent_id: 'client-a',
    latest_telemetry_at: null,
    latest_utilization_pct: 12,
    latest_quota_remaining: 900,
    last_success_at: null,
    last_error_at: null,
    rotation_recommended: false,
    replacement_required: false,
    reason: null,
    credential_state: 'leased',
    ...overrides,
  }
}

test('startup acquires when no lease is stored', () => {
  assert.equal(selectStartupAction({
    leaseId: null,
    leaseStatus: null,
    autoRotate: true,
    autoRenew: true,
  }), 'acquire')
})

test('startup stays on the current lease when it is healthy', () => {
  assert.equal(selectStartupAction({
    leaseId: 'lease-1',
    leaseStatus: buildLeaseStatus(),
    autoRotate: true,
    autoRenew: true,
    now: new Date('2026-03-23T00:00:00.000Z'),
  }), 'noop')
})

test('startup reacquires when stored lease lookup has no status', () => {
  assert.equal(selectStartupAction({
    leaseId: 'lease-1',
    leaseStatus: null,
    autoRotate: true,
    autoRenew: true,
  }), 'reacquire')
})

test('startup reacquires revoked leases', () => {
  assert.equal(selectStartupAction({
    leaseId: 'lease-1',
    leaseStatus: buildLeaseStatus({ state: 'revoked', credential_state: 'revoked' }),
    autoRotate: true,
    autoRenew: true,
  }), 'reacquire')
})

test('startup reacquires expired leases', () => {
  assert.equal(selectStartupAction({
    leaseId: 'lease-1',
    leaseStatus: buildLeaseStatus({ state: 'expired', credential_state: 'expired' }),
    autoRotate: true,
    autoRenew: true,
  }), 'reacquire')
})

test('startup rotates when replacement is required', () => {
  assert.equal(selectStartupAction({
    leaseId: 'lease-1',
    leaseStatus: buildLeaseStatus({ replacement_required: true }),
    autoRotate: true,
    autoRenew: true,
  }), 'rotate')
})

test('startup renews near-expiry leases', () => {
  assert.equal(selectStartupAction({
    leaseId: 'lease-1',
    leaseStatus: buildLeaseStatus({ expires_at: '2026-03-23T00:04:00.000Z' }),
    autoRotate: true,
    autoRenew: true,
    now: new Date('2026-03-23T00:00:00.000Z'),
  }), 'renew')
})

test('lookup helper only reacquires on 404 missing lease responses', () => {
  assert.equal(shouldReacquireAfterLookupError(404), true)
  assert.equal(shouldReacquireAfterLookupError(500), false)
  assert.equal(shouldReacquireAfterLookupError(null), false)
})

test('health state reports rotation required when lease replacement is flagged', () => {
  assert.equal(deriveLeaseHealthState(buildLeaseStatus({
    replacement_required: true,
  }), new Date('2026-03-23T00:00:00.000Z')), 'rotation_required')
})

test('health state reports expiring for near-expiry active leases', () => {
  assert.equal(deriveLeaseHealthState(buildLeaseStatus({
    expires_at: '2026-03-23T00:04:00.000Z',
  }), new Date('2026-03-23T00:00:00.000Z')), 'expiring')
})

test('shared startup parity cases stay aligned', () => {
  for (const scenario of startupParityCases) {
    assert.equal(
      selectStartupAction({
        leaseId: scenario.leaseId,
        leaseStatus: scenario.leaseStatus,
        autoRotate: true,
        autoRenew: true,
        now: new Date('2026-03-23T00:00:00.000Z'),
      }),
      scenario.expectedAction,
      scenario.name,
    )
  }
})

test('shared health parity cases stay aligned', () => {
  for (const scenario of healthParityCases) {
    assert.equal(
      deriveLeaseHealthState(scenario.leaseStatus, new Date('2026-03-23T00:00:00.000Z')),
      scenario.expectedHealth,
      scenario.name,
    )
  }
})

test('denied lease reason constant documents the shared no-eligible path', () => {
  assert.equal(deniedReason, 'no_eligible_credentials_available')
})
