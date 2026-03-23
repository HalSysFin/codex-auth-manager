export interface Lease {
  id: string
  credential_id: string
  machine_id: string
  agent_id: string
  state: string
  issued_at: string
  expires_at: string
  renewed_at: string | null
  revoked_at: string | null
  released_at: string | null
  rotation_reason: string | null
  replacement_lease_id: string | null
  last_telemetry_at: string | null
  latest_utilization_pct: number | null
  latest_quota_remaining: number | null
  last_success_at: string | null
  last_error_at: string | null
  reason: string | null
  metadata: Record<string, unknown> | null
  created_at: string
  updated_at: string
}

export interface LeaseAcquireResponse {
  status: 'ok' | 'denied'
  reason: string | null
  lease: Lease | null
}

export interface LeaseRotateResponse extends LeaseAcquireResponse {}

export interface LeaseStatusResponse {
  lease_id: string
  credential_id: string
  state: string
  issued_at: string
  expires_at: string
  renewed_at: string | null
  machine_id: string
  agent_id: string
  latest_telemetry_at: string | null
  latest_utilization_pct: number | null
  latest_quota_remaining: number | null
  last_success_at: string | null
  last_error_at: string | null
  rotation_recommended: boolean
  replacement_required: boolean
  reason: string | null
  credential_state: string
}

export interface LeaseTelemetryRequest {
  machine_id: string
  agent_id: string
  captured_at: string
  requests_count?: number | null
  tokens_in?: number | null
  tokens_out?: number | null
  utilization_pct?: number | null
  quota_remaining?: number | null
  rate_limit_remaining?: number | null
  status: string
  last_success_at?: string | null
  last_error_at?: string | null
  error_rate_1h?: number | null
}

export interface AuthPayload {
  auth_mode: string
  OPENAI_API_KEY: null
  tokens: {
    id_token: string
    access_token: string
    refresh_token: string
    account_id: string
  }
  last_refresh?: string
}

export interface MaterializeLeaseResponse extends LeaseAcquireResponse {
  credential_material?: {
    label?: string | null
    account_key?: string | null
    email?: string | null
    name?: string | null
    provider_account_id?: string | null
    auth_json?: AuthPayload | null
  } | null
}

export type LeaseHealthState = 'active' | 'expiring' | 'rotation_required' | 'revoked' | 'no_lease' | 'backend_unavailable'

export interface RuntimeLeaseState {
  machineId: string
  agentId: string
  leaseId: string | null
  credentialId: string | null
  accountLabel: string | null
  accountName: string | null
  issuedAt: string | null
  expiresAt: string | null
  leaseState: string | null
  latestTelemetryAt: string | null
  latestUtilizationPct: number | null
  latestQuotaRemaining: number | null
  lastAuthWriteAt: string | null
  lastBackendRefreshAt: string | null
  replacementRequired: boolean
  rotationRecommended: boolean
  lastErrorAt: string | null
  authFilePath: string
}

export interface RuntimeSettings {
  baseUrl: string
  internalApiToken: string
  machineId: string
  agentId: string
  authFilePath: string
  refreshIntervalSeconds: number
  telemetryIntervalSeconds: number
  autoRenew: boolean
  autoRotate: boolean
  openDashboardPath: string
  allowInsecureLocalhost: boolean
}

export interface PersistedDesktopState {
  settings: RuntimeSettings
  lease: RuntimeLeaseState
}

export type LeaseAction = 'acquire' | 'reacquire' | 'rotate' | 'renew' | 'noop'
