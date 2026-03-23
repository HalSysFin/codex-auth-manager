import type { LeaseHealthState, LeaseStatusResponse } from '../types.js'

export function buildLeaseStatus(overrides: Partial<LeaseStatusResponse> = {}): LeaseStatusResponse {
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

export const startupParityCases: Array<{
  name: string
  leaseId: string | null
  leaseStatus: LeaseStatusResponse | null
  expectedAction: 'acquire' | 'reacquire' | 'rotate' | 'renew' | 'noop'
}> = [
  {
    name: 'no lease acquires',
    leaseId: null,
    leaseStatus: null,
    expectedAction: 'acquire',
  },
  {
    name: 'active lease remains active',
    leaseId: 'lease-1',
    leaseStatus: buildLeaseStatus(),
    expectedAction: 'noop',
  },
  {
    name: 'missing lookup reacquires',
    leaseId: 'lease-1',
    leaseStatus: null,
    expectedAction: 'reacquire',
  },
  {
    name: 'revoked lease reacquires',
    leaseId: 'lease-1',
    leaseStatus: buildLeaseStatus({ state: 'revoked', credential_state: 'revoked' }),
    expectedAction: 'reacquire',
  },
  {
    name: 'expired lease reacquires',
    leaseId: 'lease-1',
    leaseStatus: buildLeaseStatus({ state: 'expired', credential_state: 'expired' }),
    expectedAction: 'reacquire',
  },
  {
    name: 'replacement required rotates',
    leaseId: 'lease-1',
    leaseStatus: buildLeaseStatus({ replacement_required: true }),
    expectedAction: 'rotate',
  },
  {
    name: 'near expiry renews',
    leaseId: 'lease-1',
    leaseStatus: buildLeaseStatus({ expires_at: '2026-03-23T00:04:00.000Z' }),
    expectedAction: 'renew',
  },
]

export const healthParityCases: Array<{
  name: string
  leaseStatus: LeaseStatusResponse
  expectedHealth: LeaseHealthState
}> = [
  {
    name: 'active health is active',
    leaseStatus: buildLeaseStatus(),
    expectedHealth: 'active',
  },
  {
    name: 'expiring health is expiring',
    leaseStatus: buildLeaseStatus({ expires_at: '2026-03-23T00:04:00.000Z' }),
    expectedHealth: 'expiring',
  },
  {
    name: 'rotation recommended health is rotation_required',
    leaseStatus: buildLeaseStatus({ rotation_recommended: true }),
    expectedHealth: 'rotation_required',
  },
  {
    name: 'replacement required health is rotation_required',
    leaseStatus: buildLeaseStatus({ replacement_required: true }),
    expectedHealth: 'rotation_required',
  },
  {
    name: 'revoked health is revoked',
    leaseStatus: buildLeaseStatus({ state: 'revoked', credential_state: 'revoked' }),
    expectedHealth: 'revoked',
  },
  {
    name: 'expired health is revoked',
    leaseStatus: buildLeaseStatus({ state: 'expired', credential_state: 'expired' }),
    expectedHealth: 'revoked',
  },
]

export const deniedReason = 'no_eligible_credentials_available'
