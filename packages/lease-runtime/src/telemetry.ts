import type { LeaseTelemetryRequest, RuntimeLeaseState } from './types.js'

export function buildLeaseTelemetryPayload(state: RuntimeLeaseState, capturedAt = new Date().toISOString()): LeaseTelemetryRequest {
  return {
    machine_id: state.machineId,
    agent_id: state.agentId,
    captured_at: capturedAt,
    status: state.leaseState === 'active' ? 'ok' : (state.leaseState || 'unknown'),
    last_success_at: state.lastBackendRefreshAt,
    last_error_at: state.lastErrorAt,
    utilization_pct: state.latestUtilizationPct,
    quota_remaining: state.latestQuotaRemaining,
    requests_count: null,
    tokens_in: null,
    tokens_out: null,
    rate_limit_remaining: null,
    error_rate_1h: null,
  }
}
