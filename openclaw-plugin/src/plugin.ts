import { AuthManagerTelemetryClient } from './client.js'
import type {
  AggregatedTelemetry,
  AuthManagerPluginOptions,
  LeaseTelemetryContext,
  NormalizedUsageEvent,
  TelemetryPostBody,
  UsageShape,
} from './types.js'
import { normalizeUsageEvent } from './usage.js'

function mergeMetadata(
  left?: Record<string, unknown>,
  right?: Record<string, unknown>,
): Record<string, unknown> | undefined {
  if (!left && !right) return undefined
  return { ...(left ?? {}), ...(right ?? {}) }
}

function toTelemetryBody(context: LeaseTelemetryContext, aggregate: AggregatedTelemetry): TelemetryPostBody {
  const body: TelemetryPostBody = {
    machine_id: context.machineId,
    agent_id: context.agentId,
    captured_at: new Date().toISOString(),
    status: aggregate.status,
    utilization_pct: aggregate.utilizationPct ?? context.utilizationPct ?? null,
    quota_remaining: aggregate.quotaRemaining ?? context.quotaRemaining ?? null,
    rate_limit_remaining: aggregate.rateLimitRemaining,
    last_success_at: aggregate.lastSuccessAt,
    last_error_at: aggregate.lastErrorAt,
    error_rate_1h: aggregate.errorRate1h,
  }
  if (aggregate.requestsCount > 0) body.requests_count = aggregate.requestsCount
  if (aggregate.tokensIn > 0) body.tokens_in = aggregate.tokensIn
  if (aggregate.tokensOut > 0) body.tokens_out = aggregate.tokensOut
  if (aggregate.metadata) body.metadata = aggregate.metadata
  return body
}

function emptyAggregate(): AggregatedTelemetry {
  return {
    requestsCount: 0,
    tokensIn: 0,
    tokensOut: 0,
    utilizationPct: null,
    quotaRemaining: null,
    rateLimitRemaining: null,
    status: 'healthy',
    lastSuccessAt: null,
    lastErrorAt: null,
    errorRate1h: null,
    metadata: undefined,
  }
}

export class OpenClawAuthManagerPlugin {
  private readonly client: AuthManagerTelemetryClient
  private readonly logger: Pick<Console, 'info' | 'warn' | 'error'>
  private context: LeaseTelemetryContext | null
  private aggregate: AggregatedTelemetry

  constructor(options: AuthManagerPluginOptions) {
    this.client = new AuthManagerTelemetryClient({
      baseUrl: options.baseUrl,
      apiKey: options.apiKey,
      fetchImpl: options.fetchImpl,
    })
    this.logger = options.logger ?? console
    this.context = options.context ?? null
    this.aggregate = emptyAggregate()
  }

  setLeaseContext(context: LeaseTelemetryContext): void {
    this.context = context
  }

  clearLeaseContext(): void {
    this.context = null
    this.aggregate = emptyAggregate()
  }

  observeUsage(raw: UsageShape): NormalizedUsageEvent {
    const normalized = normalizeUsageEvent(raw)
    this.aggregate.requestsCount += normalized.requestsCount ?? 0
    this.aggregate.tokensIn += normalized.tokensIn ?? 0
    this.aggregate.tokensOut += normalized.tokensOut ?? 0
    this.aggregate.status = normalized.status || this.aggregate.status
    this.aggregate.lastSuccessAt = normalized.lastSuccessAt ?? this.aggregate.lastSuccessAt
    this.aggregate.lastErrorAt = normalized.lastErrorAt ?? this.aggregate.lastErrorAt
    this.aggregate.utilizationPct = normalized.utilizationPct ?? this.aggregate.utilizationPct
    this.aggregate.quotaRemaining = normalized.quotaRemaining ?? this.aggregate.quotaRemaining
    this.aggregate.rateLimitRemaining = normalized.rateLimitRemaining ?? this.aggregate.rateLimitRemaining
    this.aggregate.errorRate1h = normalized.errorRate1h ?? this.aggregate.errorRate1h
    this.aggregate.metadata = mergeMetadata(this.aggregate.metadata, normalized.metadata)
    return normalized
  }

  getPendingTotals(): AggregatedTelemetry {
    return { ...this.aggregate, metadata: this.aggregate.metadata ? { ...this.aggregate.metadata } : undefined }
  }

  async flushTelemetry(): Promise<unknown> {
    if (!this.context) {
      throw new Error('No active lease context is configured')
    }
    const body = toTelemetryBody(this.context, this.aggregate)
    const result = await this.client.postLeaseTelemetry(this.context, body)
    this.logger.info?.(
      `[openclaw-plugin] posted telemetry for lease ${this.context.leaseId}: requests=${body.requests_count ?? 0} tokens_in=${body.tokens_in ?? 0} tokens_out=${body.tokens_out ?? 0}`,
    )
    this.aggregate = emptyAggregate()
    return result
  }
}

export function createOpenClawAuthManagerPlugin(options: AuthManagerPluginOptions): OpenClawAuthManagerPlugin {
  return new OpenClawAuthManagerPlugin(options)
}
