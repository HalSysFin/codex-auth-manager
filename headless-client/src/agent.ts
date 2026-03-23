import { setTimeout as delay } from 'node:timers/promises'
import { AuthManagerClient, AuthManagerClientError } from '../../packages/lease-runtime/src/authManagerClient.js'
import {
  deriveLeaseHealthState,
  needsReacquire,
  selectStartupAction,
  shouldReacquireAfterLookupError,
  shouldRenewLease,
  shouldRotateLease,
} from '../../packages/lease-runtime/src/leaseLifecycle.js'
import {
  defaultRuntimeLeaseState,
  recordAuthWrite,
  recordError,
  updateRuntimeStateFromLease,
  updateRuntimeStateFromLeaseStatus,
} from '../../packages/lease-runtime/src/runtimeState.js'
import { buildLeaseTelemetryPayload } from '../../packages/lease-runtime/src/telemetry.js'
import type {
  LeaseAcquireResponse,
  LeaseHealthState,
  LeaseStatusResponse,
  RuntimeLeaseState,
} from '../../packages/lease-runtime/src/types.js'
import { authFileExists, appendLog, ensureConfigDir, readRecentLogs, writeAuthFile } from './fsStore.js'
import { saveHeadlessState } from './fsStore.js'
import type { HeadlessOutput, HeadlessStateDocument, HeadlessStatusSummary, ResolvedCliConfig } from './types.js'

const REQUESTED_TTL_SECONDS = 1800

export class HeadlessAgent {
  private readonly client: AuthManagerClient
  private backendReachable = true
  private lastMessage: string | null = null

  constructor(
    private readonly config: ResolvedCliConfig,
    private readonly document: HeadlessStateDocument,
    private readonly output: HeadlessOutput,
  ) {
    this.client = new AuthManagerClient({
      baseUrl: this.config.settings.baseUrl,
      internalApiToken: this.config.settings.internalApiToken,
      allowInsecureLocalhost: this.config.settings.allowInsecureLocalhost,
    })
  }

  statusSummary(): HeadlessStatusSummary {
    return {
      backendReachable: this.backendReachable,
      healthState: this.currentHealthState(),
      lease: this.document.lease,
      message: this.lastMessage,
    }
  }

  async ensure(): Promise<HeadlessStatusSummary> {
    await this.log('Starting ensure flow')
    if (!this.document.lease.leaseId) {
      await this.acquireAndMaterialize('ensure acquire')
      return this.statusSummary()
    }
    try {
      const status = await this.client.getLease(this.document.lease.leaseId)
      this.backendReachable = true
      this.document.lease = updateRuntimeStateFromLeaseStatus(this.document.lease, status)
      const action = selectStartupAction({
        leaseId: this.document.lease.leaseId,
        leaseStatus: status,
        autoRotate: this.config.settings.autoRotate,
        autoRenew: this.config.settings.autoRenew,
      })
      if (action === 'reacquire') {
        this.document.lease = defaultRuntimeLeaseState(this.config.settings.machineId, this.config.settings.agentId, this.config.settings.authFilePath)
        await this.acquireAndMaterialize('ensure reacquire')
      } else if (action === 'rotate') {
        await this.rotate()
      } else if (action === 'renew') {
        await this.renew()
      } else if (!(await authFileExists(this.config.settings.authFilePath))) {
        const leaseId = this.document.lease.leaseId
        if (leaseId) {
          await this.materializeAndWriteAuth(leaseId)
        }
      } else {
        this.setMessage('Lease is healthy.')
      }
    } catch (error) {
      if (error instanceof AuthManagerClientError && shouldReacquireAfterLookupError(error.status)) {
        this.document.lease = defaultRuntimeLeaseState(this.config.settings.machineId, this.config.settings.agentId, this.config.settings.authFilePath)
        await this.acquireAndMaterialize('ensure reacquire missing lease')
      } else {
        this.backendReachable = false
        await this.handleError(error, 'Unable to ensure lease')
      }
    }
    await this.persist()
    return this.statusSummary()
  }

  async refresh(): Promise<HeadlessStatusSummary> {
    if (!this.document.lease.leaseId) {
      return this.ensure()
    }
    await this.log(`Refreshing lease ${this.document.lease.leaseId}`)
    try {
      const status = await this.client.getLease(this.document.lease.leaseId)
      this.backendReachable = true
      this.document.lease = updateRuntimeStateFromLeaseStatus(this.document.lease, status)
      if (needsReacquire(status)) {
        this.document.lease = defaultRuntimeLeaseState(this.config.settings.machineId, this.config.settings.agentId, this.config.settings.authFilePath)
        await this.acquireAndMaterialize('refresh reacquire')
      } else if (shouldRotateLease(status, this.config.settings.autoRotate)) {
        await this.rotate()
      } else if (shouldRenewLease(status, this.config.settings.autoRenew)) {
        await this.renew()
      } else {
        this.setMessage(`Lease refreshed at ${new Date().toISOString()}`)
      }
    } catch (error) {
      if (error instanceof AuthManagerClientError && shouldReacquireAfterLookupError(error.status)) {
        this.document.lease = defaultRuntimeLeaseState(this.config.settings.machineId, this.config.settings.agentId, this.config.settings.authFilePath)
        await this.acquireAndMaterialize('refresh reacquire missing lease')
      } else {
        this.backendReachable = false
        await this.handleError(error, 'Unable to refresh lease')
      }
    }
    await this.persist()
    return this.statusSummary()
  }

  async renew(): Promise<HeadlessStatusSummary> {
    if (!this.document.lease.leaseId) {
      return this.ensure()
    }
    await this.log(`Renewing lease ${this.document.lease.leaseId}`)
    try {
      const response = await this.client.renewLease(this.document.lease.leaseId, {
        machineId: this.config.settings.machineId,
        agentId: this.config.settings.agentId,
      })
      this.document.lease = this.consumeLeaseResponse(response, 'Lease renew denied')
      this.backendReachable = true
      this.setMessage('Lease renewed.')
    } catch (error) {
      await this.handleError(error, 'Unable to renew lease')
    }
    await this.persist()
    return this.statusSummary()
  }

  async rotate(): Promise<HeadlessStatusSummary> {
    if (!this.document.lease.leaseId) {
      return this.ensure()
    }
    await this.log(`Rotating lease ${this.document.lease.leaseId}`)
    try {
      const response = await this.client.rotateLease({
        leaseId: this.document.lease.leaseId,
        machineId: this.config.settings.machineId,
        agentId: this.config.settings.agentId,
        reason: 'approaching_utilization_threshold',
      })
      this.document.lease = this.consumeLeaseResponse(response, 'Lease rotation denied')
      this.backendReachable = true
      if (this.document.lease.leaseId) {
        await this.materializeAndWriteAuth(this.document.lease.leaseId)
      }
      this.setMessage('Lease rotated and auth file updated.')
    } catch (error) {
      await this.handleError(error, 'Unable to rotate lease')
    }
    await this.persist()
    return this.statusSummary()
  }

  async release(): Promise<HeadlessStatusSummary> {
    if (!this.document.lease.leaseId) {
      this.setMessage('No active lease to release.')
      return this.statusSummary()
    }
    await this.log(`Releasing lease ${this.document.lease.leaseId}`)
    try {
      const response = await this.client.releaseLease(this.document.lease.leaseId, {
        machineId: this.config.settings.machineId,
        agentId: this.config.settings.agentId,
        reason: 'Released from headless client',
      })
      if (response.status !== 'ok') {
        throw new Error(response.reason || 'Lease release denied')
      }
      this.backendReachable = true
      this.document.lease = defaultRuntimeLeaseState(this.config.settings.machineId, this.config.settings.agentId, this.config.settings.authFilePath)
      this.setMessage('Lease released.')
    } catch (error) {
      await this.handleError(error, 'Unable to release lease')
    }
    await this.persist()
    return this.statusSummary()
  }

  async doctor(): Promise<{ ok: boolean; checks: string[] }> {
    const checks: string[] = []
    await ensureConfigDir(this.config.paths)
    checks.push(`Config file: ${this.config.paths.configFile}`)
    checks.push(`State file: ${this.config.paths.stateFile}`)
    checks.push(`Log file: ${this.config.paths.logFile}`)
    checks.push(`Auth file path: ${this.config.settings.authFilePath}`)
    checks.push(`Backend URL: ${this.config.settings.baseUrl}`)
    try {
      if (this.document.lease.leaseId) {
        await this.client.getLease(this.document.lease.leaseId)
        checks.push('Backend connectivity: ok (lease lookup succeeded)')
      } else {
        const response = await fetch(new URL('/health', this.config.settings.baseUrl).toString())
        checks.push(`Backend connectivity: ${response.ok ? 'ok' : `unexpected status ${response.status}`}`)
      }
    } catch (error) {
      checks.push(`Backend connectivity: failed (${error instanceof Error ? error.message : String(error)})`)
      return { ok: false, checks }
    }
    checks.push(`Recent log lines: ${(await readRecentLogs(this.config.paths, 5)).length}`)
    return { ok: true, checks }
  }

  async run(): Promise<void> {
    this.output.info('Starting headless lease loop. Press Ctrl+C to stop.')
    await this.ensure()
    let nextTelemetry = 0
    while (true) {
      const now = Date.now()
      if (now >= nextTelemetry) {
        await this.postTelemetry()
        nextTelemetry = now + (this.config.settings.telemetryIntervalSeconds * 1000)
      }
      await this.refresh()
      await delay(this.config.settings.refreshIntervalSeconds * 1000)
    }
  }

  private async postTelemetry(): Promise<void> {
    if (!this.document.lease.leaseId) {
      return
    }
    try {
      await this.client.postTelemetry(this.document.lease.leaseId, buildLeaseTelemetryPayload(this.document.lease))
      this.backendReachable = true
      await this.log(`Posted telemetry for lease ${this.document.lease.leaseId}`)
    } catch (error) {
      await this.handleError(error, 'Unable to post telemetry', false)
    }
  }

  private currentHealthState(): LeaseHealthState {
    if (!this.backendReachable && this.document.lease.lastErrorAt) {
      return 'backend_unavailable'
    }
    if (!this.document.lease.leaseId || !this.document.lease.leaseState || !this.document.lease.expiresAt) {
      return 'no_lease'
    }
    return deriveLeaseHealthState({
      state: this.document.lease.leaseState,
      replacement_required: this.document.lease.replacementRequired,
      rotation_recommended: this.document.lease.rotationRecommended,
      expires_at: this.document.lease.expiresAt,
    })
  }

  private async acquireAndMaterialize(reason: string): Promise<void> {
    await this.log(`Acquiring lease (${reason})`)
    try {
      const response = await this.client.acquireLease({
        machineId: this.config.settings.machineId,
        agentId: this.config.settings.agentId,
        requestedTtlSeconds: REQUESTED_TTL_SECONDS,
        reason,
      })
      this.document.lease = this.consumeLeaseResponse(response, 'No eligible credentials available')
      this.backendReachable = true
      if (this.document.lease.leaseId) {
        await this.materializeAndWriteAuth(this.document.lease.leaseId)
      }
      this.setMessage('Lease acquired and auth file written.')
    } catch (error) {
      await this.handleError(error, 'Unable to acquire lease')
    }
  }

  private consumeLeaseResponse(response: LeaseAcquireResponse, deniedMessage: string): RuntimeLeaseState {
    if (response.status !== 'ok' || !response.lease) {
      throw new Error(response.reason || deniedMessage)
    }
    return updateRuntimeStateFromLease(this.document.lease, response.lease)
  }

  private async materializeAndWriteAuth(leaseId: string): Promise<void> {
    const materialized = await this.client.materializeLease(leaseId, {
      machineId: this.config.settings.machineId,
      agentId: this.config.settings.agentId,
    })
    if (materialized.status !== 'ok' || !materialized.credential_material?.auth_json) {
      throw new Error(materialized.reason || 'Backend did not return auth payload for this lease')
    }
    const result = await writeAuthFile(this.config.settings.authFilePath, materialized.credential_material.auth_json)
    this.document.lease = recordAuthWrite(this.document.lease, result.writtenAt)
    await this.log(`Wrote auth file to ${result.path}`)
    if (materialized.lease) {
      this.document.lease = updateRuntimeStateFromLease(this.document.lease, materialized.lease)
    }
  }

  private setMessage(message: string | null): void {
    this.lastMessage = message
  }

  private async persist(): Promise<void> {
    this.document.settings = this.config.settings
    await saveHeadlessState(this.config.paths, this.document)
  }

  private async log(message: string): Promise<void> {
    const line = `[${new Date().toISOString()}] ${message}`
    this.output.info(line)
    await appendLog(this.config.paths, line)
  }

  private async handleError(error: unknown, prefix: string, surface = true): Promise<void> {
    const message = error instanceof Error ? error.message : String(error)
    this.document.lease = recordError(this.document.lease, new Date().toISOString())
    this.setMessage(`${prefix}: ${message}`)
    if (surface) {
      this.output.warn(`${prefix}: ${message}`)
      await appendLog(this.config.paths, `[${new Date().toISOString()}] ${prefix}: ${message}`)
    }
  }
}
