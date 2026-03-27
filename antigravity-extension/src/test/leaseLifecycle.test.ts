import test from 'node:test'
import assert from 'node:assert/strict'
import {
  deriveLeaseHealthState,
  needsReacquire,
  selectStartupAction,
  shouldReacquireAfterLookupError,
  shouldRenewLease,
  shouldRotateLease,
} from '../leaseLifecycle'
import { healthParityCases, startupParityCases } from '../../../packages/lease-runtime/src/test/parityMatrix.js'

test('startup with no lease needs reacquire', () => {
  assert.equal(needsReacquire(null), true)
})

test('startup with active lease stays active', () => {
  const state = deriveLeaseHealthState({
    state: 'active',
    replacement_required: false,
    rotation_recommended: false,
    expires_at: '2099-01-01T00:00:00.000Z',
  }, new Date('2026-03-22T00:00:00.000Z'))
  assert.equal(state, 'active')
})

test('startup with revoked lease needs reacquire', () => {
  assert.equal(needsReacquire({ state: 'revoked' }), true)
})

test('startup with released lease needs reacquire', () => {
  assert.equal(needsReacquire({ state: 'released' }), true)
})

test('expired lease shows revoked health state', () => {
  const state = deriveLeaseHealthState({
    state: 'expired',
    replacement_required: false,
    rotation_recommended: false,
    expires_at: '2026-03-22T00:00:00.000Z',
  }, new Date('2026-03-22T00:00:00.000Z'))
  assert.equal(state, 'revoked')
})

test('released lease shows revoked health state', () => {
  const state = deriveLeaseHealthState({
    state: 'released',
    replacement_required: false,
    rotation_recommended: false,
    expires_at: '2026-03-22T00:00:00.000Z',
  }, new Date('2026-03-22T00:00:00.000Z'))
  assert.equal(state, 'revoked')
})

test('replacement required handling prefers rotate', () => {
  assert.equal(shouldRotateLease({
    state: 'active',
    replacement_required: true,
    rotation_recommended: false,
  }, true), true)
})

test('rotation recommendation alone does not auto-rotate', () => {
  assert.equal(shouldRotateLease({
    state: 'active',
    replacement_required: false,
    rotation_recommended: true,
  }, true), false)
})

test('rotation recommendation can auto-rotate when policy allows it', () => {
  assert.equal(shouldRotateLease({
    state: 'active',
    replacement_required: false,
    rotation_recommended: true,
  }, true, 'recommended_or_required'), true)
})

test('renew handling triggers near expiry', () => {
  assert.equal(shouldRenewLease({
    state: 'active',
    replacement_required: false,
    expires_at: '2026-03-22T00:04:00.000Z',
  }, true, new Date('2026-03-22T00:00:00.000Z')), true)
})

test('replacement required maps to shared startup rotate action', () => {
  assert.equal(
    selectStartupAction({
      leaseId: 'lease-1',
      leaseStatus: {
        lease_id: 'lease-1',
        credential_id: 'cred-1',
        state: 'active',
        issued_at: '2026-03-22T00:00:00.000Z',
        expires_at: '2026-03-22T02:00:00.000Z',
        renewed_at: null,
        machine_id: 'machine-a',
        agent_id: 'vscode-extension',
        latest_telemetry_at: null,
        latest_utilization_pct: 92,
        latest_quota_remaining: 100,
        last_success_at: null,
        last_error_at: null,
        rotation_recommended: false,
        replacement_required: true,
        reason: null,
        credential_state: 'leased',
      },
      autoRotate: true,
      autoRenew: true,
    }),
    'rotate',
  )
})

test('near-expiry active lease maps to shared startup renew action', () => {
  assert.equal(
    selectStartupAction({
      leaseId: 'lease-1',
      leaseStatus: {
        lease_id: 'lease-1',
        credential_id: 'cred-1',
        state: 'active',
        issued_at: '2026-03-22T00:00:00.000Z',
        expires_at: '2026-03-22T00:04:00.000Z',
        renewed_at: null,
        machine_id: 'machine-a',
        agent_id: 'vscode-extension',
        latest_telemetry_at: null,
        latest_utilization_pct: 12,
        latest_quota_remaining: 100,
        last_success_at: null,
        last_error_at: null,
        rotation_recommended: false,
        replacement_required: false,
        reason: null,
        credential_state: 'leased',
      },
      autoRotate: true,
      autoRenew: true,
      now: new Date('2026-03-22T00:00:00.000Z'),
    }),
    'renew',
  )
})

test('healthy active lease maps to shared startup noop action', () => {
  assert.equal(
    selectStartupAction({
      leaseId: 'lease-1',
      leaseStatus: {
        lease_id: 'lease-1',
        credential_id: 'cred-1',
        state: 'active',
        issued_at: '2026-03-22T00:00:00.000Z',
        expires_at: '2026-03-22T01:00:00.000Z',
        renewed_at: null,
        machine_id: 'machine-a',
        agent_id: 'vscode-extension',
        latest_telemetry_at: null,
        latest_utilization_pct: 12,
        latest_quota_remaining: 100,
        last_success_at: null,
        last_error_at: null,
        rotation_recommended: false,
        replacement_required: false,
        reason: null,
        credential_state: 'leased',
      },
      autoRotate: true,
      autoRenew: true,
      now: new Date('2026-03-22T00:00:00.000Z'),
    }),
    'noop',
  )
})

test('404 lease lookup stays on the shared reacquire path', () => {
  assert.equal(shouldReacquireAfterLookupError(404), true)
  assert.equal(shouldReacquireAfterLookupError(500), false)
})

test('vscode extension matches the shared startup parity matrix', () => {
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

test('vscode extension matches the shared health parity matrix', () => {
  for (const scenario of healthParityCases) {
    assert.equal(
      deriveLeaseHealthState(scenario.leaseStatus, new Date('2026-03-23T00:00:00.000Z')),
      scenario.expectedHealth,
      scenario.name,
    )
  }
})
