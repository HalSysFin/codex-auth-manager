import type { LeaseAction, LeaseHealthState, LeaseStatusResponse } from './types.js'

export function secondsUntilExpiry(expiresAt: string, now = new Date()): number {
  return Math.max(0, Math.floor((new Date(expiresAt).getTime() - now.getTime()) / 1000))
}

export function deriveLeaseHealthState(
  lease: Pick<LeaseStatusResponse, 'state' | 'replacement_required' | 'rotation_recommended' | 'expires_at'>,
  now = new Date(),
): LeaseHealthState {
  if (lease.state === 'revoked' || lease.state === 'expired') {
    return 'revoked'
  }
  if (lease.replacement_required || lease.rotation_recommended) {
    return 'rotation_required'
  }
  if (secondsUntilExpiry(lease.expires_at, now) <= 300) {
    return 'expiring'
  }
  return 'active'
}

export function shouldRenewLease(
  lease: Pick<LeaseStatusResponse, 'state' | 'expires_at' | 'replacement_required'>,
  autoRenew: boolean,
  now = new Date(),
): boolean {
  if (!autoRenew || lease.state !== 'active' || lease.replacement_required) {
    return false
  }
  return secondsUntilExpiry(lease.expires_at, now) <= 300
}

export function shouldRotateLease(
  lease: Pick<LeaseStatusResponse, 'state' | 'replacement_required' | 'rotation_recommended'>,
  autoRotate: boolean,
): boolean {
  if (!autoRotate) {
    return false
  }
  if (lease.state === 'revoked' || lease.state === 'expired') {
    return true
  }
  return lease.replacement_required || lease.rotation_recommended
}

export function needsReacquire(lease: Pick<LeaseStatusResponse, 'state'> | null | undefined): boolean {
  if (!lease) {
    return true
  }
  return lease.state === 'revoked' || lease.state === 'expired'
}

export function selectStartupAction(input: {
  leaseId: string | null
  leaseStatus?: LeaseStatusResponse | null
  autoRotate: boolean
  autoRenew: boolean
  now?: Date
}): LeaseAction {
  if (!input.leaseId) {
    return 'acquire'
  }
  if (!input.leaseStatus) {
    return 'reacquire'
  }
  if (needsReacquire(input.leaseStatus)) {
    return 'reacquire'
  }
  if (shouldRotateLease(input.leaseStatus, input.autoRotate)) {
    return 'rotate'
  }
  if (shouldRenewLease(input.leaseStatus, input.autoRenew, input.now)) {
    return 'renew'
  }
  return 'noop'
}

export function shouldReacquireAfterLookupError(statusCode: number | null | undefined): boolean {
  return statusCode === 404
}
