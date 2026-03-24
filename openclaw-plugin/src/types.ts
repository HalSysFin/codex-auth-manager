export type UsageShape = Record<string, unknown>

export type LeaseTelemetryContext = {
  leaseId: string
  machineId: string
  agentId: string
  utilizationPct?: number | null
  quotaRemaining?: number | null
}

export type NormalizedUsageEvent = {
  requestsCount: number | null
  tokensIn: number | null
  tokensOut: number | null
  status: string
  lastSuccessAt?: string | null
  lastErrorAt?: string | null
  utilizationPct?: number | null
  quotaRemaining?: number | null
  rateLimitRemaining?: number | null
  errorRate1h?: number | null
  metadata?: Record<string, unknown>
}

export type AggregatedTelemetry = {
  requestsCount: number
  tokensIn: number
  tokensOut: number
  utilizationPct: number | null
  quotaRemaining: number | null
  rateLimitRemaining: number | null
  status: string
  lastSuccessAt: string | null
  lastErrorAt: string | null
  errorRate1h: number | null
  metadata?: Record<string, unknown>
}

export type TelemetryPostBody = {
  machine_id: string
  agent_id: string
  captured_at: string
  requests_count?: number
  tokens_in?: number
  tokens_out?: number
  utilization_pct?: number | null
  quota_remaining?: number | null
  rate_limit_remaining?: number | null
  status: string
  last_success_at?: string | null
  last_error_at?: string | null
  error_rate_1h?: number | null
  metadata?: Record<string, unknown>
}

export type AuthManagerPluginOptions = {
  baseUrl: string
  internalApiToken: string
  context?: LeaseTelemetryContext
  fetchImpl?: typeof fetch
  logger?: Pick<Console, 'info' | 'warn' | 'error'>
  authFilePath?: string
  leaseProfileId?: string
  enforceLeaseAsActiveAuth?: boolean
  disallowNonLeaseAuth?: boolean
  purgeNonLeaseProfilesOnStart?: boolean
  allowInsecureLocalhost?: boolean
  requestedTtlSeconds?: number
  autoRenew?: boolean
  autoRotate?: boolean
  rotationPolicy?: 'replacement_required_only' | 'recommended_or_required'
  refreshIntervalMs?: number
  releaseLeaseOnShutdown?: boolean
}

export type AuthManagerLeasePluginConfig = {
  baseUrl: string
  internalApiToken: string
  machineId: string
  agentId: string
  leaseId?: string
  authFilePath: string
  leaseProfileId: string
  enforceLeaseAsActiveAuth: boolean
  disallowNonLeaseAuth: boolean
  purgeNonLeaseProfilesOnStart: boolean
  flushIntervalMs: number
  flushEveryRequests: number
  refreshIntervalMs: number
  requestedTtlSeconds: number
  autoRenew: boolean
  autoRotate: boolean
  rotationPolicy: 'replacement_required_only' | 'recommended_or_required'
  allowInsecureLocalhost: boolean
  releaseLeaseOnShutdown: boolean
  enabled: boolean
}

export type OpenClawLeaseTelemetryServiceOptions = AuthManagerPluginOptions & {
  flushIntervalMs?: number
  flushEveryRequests?: number
  refreshIntervalMs?: number
}

export type Lease = {
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
  last_seen_at?: string | null
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

export type LeaseAcquireResponse = {
  status: 'ok' | 'denied'
  reason: string | null
  lease: Lease | null
}

export type LeaseRotateResponse = LeaseAcquireResponse

export type LeaseStatusResponse = {
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

export type AuthPayload = {
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

export type MaterializeLeaseResponse = LeaseAcquireResponse & {
  credential_material?: {
    label?: string | null
    account_key?: string | null
    email?: string | null
    name?: string | null
    provider_account_id?: string | null
    auth_json?: AuthPayload | null
  } | null
}
