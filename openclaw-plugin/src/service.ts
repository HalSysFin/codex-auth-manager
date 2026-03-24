import type {
  LeaseAcquireResponse,
  LeaseStatusResponse,
  LeaseTelemetryContext,
  OpenClawLeaseTelemetryServiceOptions,
  UsageShape,
} from './types.js'
import { OpenClawAuthManagerPlugin } from './plugin.js'
import { AuthManagerClientError, AuthManagerTelemetryClient } from './client.js'
import { writeAuthFile } from './authFile.js'

function countObservedRequests(raw: UsageShape): number {
  const direct = raw.requests_count ?? raw.request_count
  if (typeof direct === 'number' && Number.isFinite(direct) && direct > 0) return Math.trunc(direct)
  if (typeof direct === 'string') {
    const parsed = Number(direct.trim())
    if (Number.isFinite(parsed) && parsed > 0) return Math.trunc(parsed)
  }
  const usage = raw.usage
  if (usage && typeof usage === 'object') {
    const u = usage as Record<string, unknown>
    if (
      typeof u.prompt_tokens === 'number' ||
      typeof u.input_tokens === 'number' ||
      typeof u.completion_tokens === 'number' ||
      typeof u.output_tokens === 'number' ||
      typeof u.total_tokens === 'number'
    ) {
      return 1
    }
  }
  return 0
}

export class OpenClawLeaseTelemetryService {
  private readonly plugin: OpenClawAuthManagerPlugin
  private readonly client: AuthManagerTelemetryClient
  private readonly logger: Pick<Console, 'info' | 'warn' | 'error'>
  private readonly flushIntervalMs: number
  private readonly flushEveryRequests: number
  private readonly refreshIntervalMs: number
  private readonly requestedTtlSeconds: number
  private readonly autoRenew: boolean
  private readonly autoRotate: boolean
  private readonly rotationPolicy: 'replacement_required_only' | 'recommended_or_required'
  private readonly releaseLeaseOnShutdown: boolean
  private readonly machineId: string
  private readonly agentId: string
  private readonly authFilePath: string
  private flushTimer: NodeJS.Timeout | null = null
  private refreshTimer: NodeJS.Timeout | null = null
  private observedSinceFlush = 0
  private context: LeaseTelemetryContext | null = null
  private lastKnownLeaseState: string | null = null
  private lastKnownExpiresAt: string | null = null

  constructor(options: OpenClawLeaseTelemetryServiceOptions) {
    this.client = new AuthManagerTelemetryClient({
      baseUrl: options.baseUrl,
      internalApiToken: options.internalApiToken,
      allowInsecureLocalhost: options.allowInsecureLocalhost,
      fetchImpl: options.fetchImpl,
    })
    this.plugin = new OpenClawAuthManagerPlugin(options)
    this.logger = options.logger ?? console
    this.flushIntervalMs = options.flushIntervalMs ?? 60_000
    this.flushEveryRequests = options.flushEveryRequests ?? 10
    this.refreshIntervalMs = options.refreshIntervalMs ?? 60_000
    this.requestedTtlSeconds = options.requestedTtlSeconds ?? 1800
    this.autoRenew = options.autoRenew ?? true
    this.autoRotate = options.autoRotate ?? true
    this.rotationPolicy = options.rotationPolicy ?? 'replacement_required_only'
    this.releaseLeaseOnShutdown = options.releaseLeaseOnShutdown ?? true
    this.machineId = options.context?.machineId ?? 'openclaw'
    this.agentId = options.context?.agentId ?? 'openclaw'
    this.authFilePath = options.authFilePath ?? '~/.codex/auth.json'
    this.context = options.context ?? null
    this.lastKnownLeaseState = null
    this.lastKnownExpiresAt = null
  }

  async start(): Promise<void> {
    this.stop()
    await this.ensureLease()
    this.flushTimer = setInterval(() => {
      void this.flushIfNeeded()
    }, this.flushIntervalMs)
    this.flushTimer.unref?.()
    this.refreshTimer = setInterval(() => {
      void this.refreshLease()
    }, this.refreshIntervalMs)
    this.refreshTimer.unref?.()
  }

  stop(): void {
    if (this.flushTimer) {
      clearInterval(this.flushTimer)
      this.flushTimer = null
    }
    if (this.refreshTimer) {
      clearInterval(this.refreshTimer)
      this.refreshTimer = null
    }
  }

  setLeaseContext(context: LeaseTelemetryContext): void {
    this.context = context
    this.plugin.setLeaseContext(context)
  }

  clearLeaseContext(): void {
    this.context = null
    this.plugin.clearLeaseContext()
    this.observedSinceFlush = 0
  }

  observeUsage(raw: UsageShape): void {
    this.plugin.observeUsage(raw)
    this.observedSinceFlush += countObservedRequests(raw)
    if (this.observedSinceFlush >= this.flushEveryRequests) {
      void this.flushIfNeeded()
    }
  }

  getPendingTotals() {
    return this.plugin.getPendingTotals()
  }

  async flushIfNeeded(force = false): Promise<void> {
    const pending = this.plugin.getPendingTotals()
    const hasAnything =
      pending.requestsCount > 0 ||
      pending.tokensIn > 0 ||
      pending.tokensOut > 0 ||
      pending.lastSuccessAt != null ||
      pending.lastErrorAt != null ||
      pending.utilizationPct != null ||
      pending.quotaRemaining != null
    if (!force && !hasAnything) return
    try {
      await this.plugin.flushTelemetry()
      this.observedSinceFlush = 0
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error)
      this.logger.warn?.(`[openclaw-plugin] telemetry flush failed: ${message}`)
    }
  }

  async flushNow(): Promise<void> {
    await this.flushIfNeeded(true)
  }

  async shutdown(): Promise<void> {
    this.stop()
    await this.flushIfNeeded(true)
    if (!this.releaseLeaseOnShutdown || !this.context?.leaseId) {
      return
    }
    try {
      await this.client.releaseLease(this.context.leaseId, {
        machineId: this.machineId,
        agentId: this.agentId,
        reason: 'openclaw_shutdown',
      })
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error)
      this.logger.warn?.(`[openclaw-plugin] shutdown lease release failed: ${message}`)
    } finally {
      this.clearLeaseContext()
    }
  }

  private async ensureLease(): Promise<void> {
    if (!this.context?.leaseId) {
      await this.acquireAndMaterialize('startup_acquire')
      return
    }
    try {
      const status = await this.client.getLease(this.context.leaseId)
      this.captureLeaseStatus(status)
      if (this.needsReacquire(status)) {
        await this.acquireAndMaterialize('startup_reacquire')
        return
      }
      if (this.shouldRotate(status)) {
        await this.rotateAndMaterialize('approaching_utilization_threshold')
        return
      }
      if (this.shouldRenew(status)) {
        await this.renewLease()
      }
    } catch (error) {
      if (error instanceof AuthManagerClientError && error.status === 404) {
        await this.acquireAndMaterialize('startup_reacquire_missing')
        return
      }
      throw error
    }
  }

  private async refreshLease(): Promise<void> {
    if (!this.context?.leaseId) {
      await this.acquireAndMaterialize('scheduled_acquire')
      return
    }
    try {
      const status = await this.client.getLease(this.context.leaseId)
      this.captureLeaseStatus(status)
      if (this.needsReacquire(status)) {
        await this.acquireAndMaterialize('scheduled_reacquire')
        return
      }
      if (this.shouldRotate(status)) {
        await this.rotateAndMaterialize('approaching_utilization_threshold')
        return
      }
      if (this.shouldRenew(status)) {
        await this.renewLease()
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error)
      this.logger.warn?.(`[openclaw-plugin] lease refresh failed: ${message}`)
    }
  }

  private async acquireAndMaterialize(reason: string): Promise<void> {
    const response = await this.client.acquireLease({
      machineId: this.machineId,
      agentId: this.agentId,
      requestedTtlSeconds: this.requestedTtlSeconds,
      reason,
    })
    const lease = this.consumeLeaseResponse(response, 'lease acquire denied')
    await this.materializeAndWriteAuth(lease.id)
  }

  private async rotateAndMaterialize(reason: string): Promise<void> {
    if (!this.context?.leaseId) {
      await this.acquireAndMaterialize(`rotate_without_lease:${reason}`)
      return
    }
    const response = await this.client.rotateLease({
      leaseId: this.context.leaseId,
      machineId: this.machineId,
      agentId: this.agentId,
      reason,
    })
    const lease = this.consumeLeaseResponse(response, 'lease rotation denied')
    await this.materializeAndWriteAuth(lease.id)
  }

  private async renewLease(): Promise<void> {
    if (!this.context?.leaseId) {
      return
    }
    const response = await this.client.renewLease(this.context.leaseId, {
      machineId: this.machineId,
      agentId: this.agentId,
    })
    const lease = this.consumeLeaseResponse(response, 'lease renew denied')
    this.context = {
      leaseId: lease.id,
      machineId: this.machineId,
      agentId: this.agentId,
      utilizationPct: lease.latest_utilization_pct,
      quotaRemaining: lease.latest_quota_remaining,
    }
    this.plugin.setLeaseContext(this.context)
  }

  private async materializeAndWriteAuth(leaseId: string): Promise<void> {
    const response = await this.client.materializeLease(leaseId, {
      machineId: this.machineId,
      agentId: this.agentId,
    })
    const lease = this.consumeLeaseResponse(response, 'lease materialize denied')
    const authPayload = response.credential_material?.auth_json
    if (!authPayload) {
      throw new Error('Lease materialization did not return auth_json')
    }
    await writeAuthFile(this.authFilePath, authPayload)
    this.context = {
      leaseId: lease.id,
      machineId: this.machineId,
      agentId: this.agentId,
      utilizationPct: lease.latest_utilization_pct,
      quotaRemaining: lease.latest_quota_remaining,
    }
    this.plugin.setLeaseContext(this.context)
  }

  private consumeLeaseResponse(response: LeaseAcquireResponse, fallbackMessage: string) {
    if (response.status !== 'ok' || !response.lease) {
      throw new Error(response.reason || fallbackMessage)
    }
    this.lastKnownLeaseState = response.lease.state
    this.lastKnownExpiresAt = response.lease.expires_at
    return response.lease
  }

  private captureLeaseStatus(status: LeaseStatusResponse): void {
    this.lastKnownLeaseState = status.state
    this.lastKnownExpiresAt = status.expires_at
    if (this.context) {
      this.context = {
        ...this.context,
        utilizationPct: status.latest_utilization_pct,
        quotaRemaining: status.latest_quota_remaining,
      }
      this.plugin.setLeaseContext(this.context)
    }
  }

  private needsReacquire(status: LeaseStatusResponse): boolean {
    const badLeaseState = new Set(['released', 'revoked', 'expired', 'missing', 'denied'])
    const badCredentialState = new Set(['revoked', 'expired', 'exhausted', 'unavailable_for_assignment'])
    return badLeaseState.has(status.state) || badCredentialState.has(status.credential_state)
  }

  private shouldRotate(status: LeaseStatusResponse): boolean {
    if (!this.autoRotate) {
      return false
    }
    if (status.replacement_required) {
      return true
    }
    return this.rotationPolicy === 'recommended_or_required' && status.rotation_recommended
  }

  private shouldRenew(status: LeaseStatusResponse): boolean {
    if (!this.autoRenew || !status.expires_at) {
      return false
    }
    const expiresAt = Date.parse(status.expires_at)
    if (!Number.isFinite(expiresAt)) {
      return false
    }
    return expiresAt - Date.now() <= 5 * 60 * 1000
  }
}

export function createOpenClawLeaseTelemetryService(
  options: OpenClawLeaseTelemetryServiceOptions,
): OpenClawLeaseTelemetryService {
  return new OpenClawLeaseTelemetryService(options)
}
