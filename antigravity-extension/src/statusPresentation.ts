import type { LeaseHealthState } from './leaseLifecycle'
import type { MaterializeLeaseResponse } from './authManagerClient'
import type { LeaseState } from './leaseStateStore'

function suffixForHealth(state: LeaseHealthState): string {
  switch (state) {
    case 'expiring':
      return ' (Expiring)'
    case 'rotation_required':
      return ' (Rotate)'
    case 'revoked':
      return ' (Revoked)'
    case 'backend_unavailable':
      return ' (Backend Down)'
    default:
      return ''
  }
}

export function deriveAccountDisplayName(state: Pick<LeaseState, 'accountLabel' | 'accountName' | 'credentialId' | 'leaseId'>): string {
  return state.accountLabel || state.accountName || state.credentialId || 'No Lease'
}

export function formatStatusBarText(state: Pick<LeaseState, 'accountLabel' | 'accountName' | 'credentialId' | 'leaseId'>, healthState: LeaseHealthState): string {
  if (healthState === 'no_lease' || !state.leaseId) {
    return 'Codex: No Lease'
  }
  return `Codex: ${deriveAccountDisplayName(state)}${suffixForHealth(healthState)}`
}

export function formatStatusBarTooltip(
  state: Pick<
    LeaseState,
    | 'accountLabel'
    | 'accountName'
    | 'leaseId'
    | 'credentialId'
    | 'leaseState'
    | 'expiresAt'
    | 'latestUtilizationPct'
    | 'latestQuotaRemaining'
  >,
  healthState: LeaseHealthState,
): string {
  if (!state.leaseId) {
    return 'CAM Antigravity Extension\nNo active lease'
  }
  return [
    `Account: ${deriveAccountDisplayName(state)}`,
    `Lease Id: ${state.leaseId}`,
    `Credential Id: ${state.credentialId || 'Unavailable'}`,
    `Health: ${healthState.replace(/_/g, ' ')}`,
    `Lease State: ${state.leaseState || 'Unavailable'}`,
    `Expires: ${state.expiresAt || 'Unavailable'}`,
    `Utilization: ${state.latestUtilizationPct ?? 'Unavailable'}`,
    `Quota Remaining: ${state.latestQuotaRemaining ?? 'Unavailable'}`,
  ].join('\n')
}

export function extractAccountIdentity(materialized: MaterializeLeaseResponse): {
  accountLabel: string | null
  accountName: string | null
} {
  // TODO: Prefer a dedicated backend display_name/account_label field if the broker starts
  // returning one. For now we fall back through the best materialize fields available.
  const label =
    (typeof materialized.credential_material?.label === 'string' && materialized.credential_material.label.trim()) ||
    (typeof materialized.lease?.metadata?.label === 'string' && materialized.lease.metadata.label.trim()) ||
    null
  const name =
    (typeof materialized.credential_material?.name === 'string' && materialized.credential_material.name.trim()) ||
    null
  return {
    accountLabel: label || null,
    accountName: name || null,
  }
}
