import type { Lease, LeaseStatusResponse, RuntimeLeaseState, RuntimeSettings } from './types.js'

export const DEFAULT_AUTH_FILE_PATH = '~/.codex/auth.json'

export function defaultRuntimeSettings(): RuntimeSettings {
  return {
    baseUrl: 'http://127.0.0.1:8080',
    internalApiToken: '',
    machineId: '',
    agentId: '',
    authFilePath: DEFAULT_AUTH_FILE_PATH,
    refreshIntervalSeconds: 60,
    telemetryIntervalSeconds: 300,
    autoRenew: true,
    autoRotate: true,
    openDashboardPath: '',
    allowInsecureLocalhost: true,
  }
}

export function deriveMachineId(existing: string, prefix = 'desktop'): string {
  if (existing.trim()) {
    return existing.trim()
  }
  return `${prefix}-${crypto.randomUUID().slice(0, 12)}`
}

export function deriveAgentId(existing: string, fallback = 'desktop-app'): string {
  return existing.trim() || fallback
}

export function defaultRuntimeLeaseState(machineId: string, agentId: string, authFilePath = DEFAULT_AUTH_FILE_PATH): RuntimeLeaseState {
  return {
    machineId,
    agentId,
    leaseId: null,
    credentialId: null,
    accountLabel: null,
    accountName: null,
    issuedAt: null,
    expiresAt: null,
    leaseState: null,
    latestTelemetryAt: null,
    latestUtilizationPct: null,
    latestQuotaRemaining: null,
    lastAuthWriteAt: null,
    lastBackendRefreshAt: null,
    replacementRequired: false,
    rotationRecommended: false,
    lastErrorAt: null,
    authFilePath,
  }
}

export function updateRuntimeStateFromLease(state: RuntimeLeaseState, lease: Lease, nowIso = new Date().toISOString()): RuntimeLeaseState {
  return {
    ...state,
    leaseId: lease.id,
    credentialId: lease.credential_id,
    accountLabel: typeof lease.metadata?.label === 'string' ? lease.metadata.label : state.accountLabel,
    issuedAt: lease.issued_at,
    expiresAt: lease.expires_at,
    leaseState: lease.state,
    latestTelemetryAt: lease.last_telemetry_at,
    latestUtilizationPct: lease.latest_utilization_pct,
    latestQuotaRemaining: lease.latest_quota_remaining,
    lastBackendRefreshAt: nowIso,
  }
}

export function updateRuntimeStateFromLeaseStatus(state: RuntimeLeaseState, lease: LeaseStatusResponse, nowIso = new Date().toISOString()): RuntimeLeaseState {
  return {
    ...state,
    leaseId: lease.lease_id,
    credentialId: lease.credential_id,
    issuedAt: lease.issued_at,
    expiresAt: lease.expires_at,
    leaseState: lease.state,
    latestTelemetryAt: lease.latest_telemetry_at,
    latestUtilizationPct: lease.latest_utilization_pct,
    latestQuotaRemaining: lease.latest_quota_remaining,
    replacementRequired: lease.replacement_required,
    rotationRecommended: lease.rotation_recommended,
    lastBackendRefreshAt: nowIso,
  }
}

export function recordAuthWrite(state: RuntimeLeaseState, atIso: string): RuntimeLeaseState {
  return {
    ...state,
    lastAuthWriteAt: atIso,
  }
}

export function recordError(state: RuntimeLeaseState, atIso: string): RuntimeLeaseState {
  return {
    ...state,
    lastErrorAt: atIso,
  }
}
