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
  apiKey: string
  context?: LeaseTelemetryContext
  fetchImpl?: typeof fetch
  logger?: Pick<Console, 'info' | 'warn' | 'error'>
}
