import {
  AuthManagerClient,
  AuthManagerClientError,
} from '../../packages/lease-runtime/src/authManagerClient.ts'
import { authPayloadFingerprint, prepareAuthPayloadForWrite } from '../../packages/lease-runtime/src/authPayload.ts'
import {
  deriveLeaseHealthState,
  needsReacquire,
  selectStartupAction,
  shouldReacquireAfterLookupError,
  shouldRenewLease,
  shouldRotateLease,
} from '../../packages/lease-runtime/src/leaseLifecycle.ts'
import {
  defaultRuntimeLeaseState,
  recordAuthWrite,
  recordError,
  updateRuntimeStateFromLease,
  updateRuntimeStateFromLeaseStatus,
} from '../../packages/lease-runtime/src/runtimeState.ts'
import { buildLeaseTelemetryPayload } from '../../packages/lease-runtime/src/telemetry.ts'
import type {
  LeaseAcquireResponse,
  LeaseStatusResponse,
  LeaseHealthState,
  PersistedDesktopState,
  RuntimeLeaseState,
  RuntimeSettings,
} from '../../packages/lease-runtime/src/types.ts'
import { appendLogLine, authFileExists, openTarget, readAuthFile, readRecentLogLines, savePersistedState, writeAuthFile } from './bridge'

export interface ControllerSnapshot {
  settings: RuntimeSettings
  lease: RuntimeLeaseState
  healthState: LeaseHealthState
  backendReachable: boolean
  message: string | null
  logs: string[]
}

export interface ControllerView {
  render(snapshot: ControllerSnapshot): void
}

const REQUESTED_TTL_SECONDS = 1800

export class DesktopLeaseController {
  private state: PersistedDesktopState
  private client: AuthManagerClient
  private backendReachable = true
  private message: string | null = null
  private logs: string[] = []
  private refreshTimer: number | null = null
  private telemetryTimer: number | null = null
  private ensuring = false

  constructor(
    initialState: PersistedDesktopState,
    private readonly view: ControllerView,
  ) {
    this.state = initialState
    this.client = this.buildClient()
  }

  async initialize(): Promise<void> {
    this.logs = await readRecentLogLines(120)
    this.render()
    await this.ensureLease()
    this.restartTimers()
  }

  updateSettings(settings: RuntimeSettings): void {
    this.state = {
      settings,
      lease: {
        ...this.state.lease,
        machineId: settings.machineId,
        agentId: settings.agentId,
        authFilePath: settings.authFilePath,
      },
    }
    this.client = this.buildClient()
    void this.persist()
    this.restartTimers()
    this.render()
  }

  getSnapshot(): ControllerSnapshot {
    return {
      settings: this.state.settings,
      lease: this.state.lease,
      healthState: this.currentHealthState(),
      backendReachable: this.backendReachable,
      message: this.message,
      logs: this.logs,
    }
  }

  async ensureLease(): Promise<void> {
    if (this.ensuring) {
      return
    }
    this.ensuring = true
    try {
      await this.log('Starting startup lease sync')
      if (!this.state.lease.leaseId) {
        await this.acquireAndMaterialize('startup acquire')
        return
      }
      try {
        await this.reconcileLocalAuthIfNeeded(this.state.lease.leaseId)
        const status = await this.client.getLease(this.state.lease.leaseId)
        this.backendReachable = true
        this.state.lease = updateRuntimeStateFromLeaseStatus(this.state.lease, status)
        const action = selectStartupAction({
          leaseId: this.state.lease.leaseId,
          leaseStatus: status,
          autoRotate: this.state.settings.autoRotate,
          autoRenew: this.state.settings.autoRenew,
        })
        if (action === 'reacquire') {
          await this.log(`Lease ${status.lease_id} requires reacquire`)
          this.state.lease = defaultRuntimeLeaseState(this.state.settings.machineId, this.state.settings.agentId, this.state.settings.authFilePath)
          await this.acquireAndMaterialize('startup reacquire')
          return
        }
        if (action === 'rotate') {
          await this.rotateLease()
          return
        }
        if (action === 'renew') {
          await this.renewLease()
        }
        if (this.shouldRematerializeAuth(status)) {
          await this.log(`Credential auth changed for lease ${status.lease_id}; rematerializing`)
          await this.materializeAndWriteAuth(status.lease_id)
        }
        if (!(await authFileExists(this.state.settings.authFilePath))) {
          const leaseId = this.state.lease.leaseId
          if (leaseId) {
            await this.materializeAndWriteAuth(leaseId)
          }
        }
        this.setMessage('Lease is healthy.')
      } catch (error) {
        if (error instanceof AuthManagerClientError && shouldReacquireAfterLookupError(error.status)) {
          await this.log(`Stored lease ${this.state.lease.leaseId} missing; reacquiring`)
          this.state.lease = defaultRuntimeLeaseState(this.state.settings.machineId, this.state.settings.agentId, this.state.settings.authFilePath)
          await this.acquireAndMaterialize('startup reacquire missing lease')
          return
        }
        this.backendReachable = false
        await this.handleError(error, 'Unable to refresh lease on startup')
      }
    } finally {
      this.ensuring = false
      await this.persist()
      this.render()
    }
  }

  async refreshLease(): Promise<void> {
    if (!this.state.lease.leaseId) {
      await this.ensureLease()
      return
    }
    await this.log(`Refreshing lease ${this.state.lease.leaseId}`)
    try {
      await this.reconcileLocalAuthIfNeeded(this.state.lease.leaseId)
      const status = await this.client.getLease(this.state.lease.leaseId)
      this.backendReachable = true
      this.state.lease = updateRuntimeStateFromLeaseStatus(this.state.lease, status)
      if (needsReacquire(status)) {
        await this.log(`Lease ${status.lease_id} is no longer usable; reacquiring`)
        this.state.lease = defaultRuntimeLeaseState(this.state.settings.machineId, this.state.settings.agentId, this.state.settings.authFilePath)
        await this.acquireAndMaterialize('refresh reacquire')
        return
      }
      if (shouldRotateLease(status, this.state.settings.autoRotate)) {
        await this.rotateLease()
        return
      }
      if (shouldRenewLease(status, this.state.settings.autoRenew)) {
        await this.renewLease()
        return
      }
      if (this.shouldRematerializeAuth(status)) {
        await this.log(`Credential auth changed for lease ${status.lease_id}; rematerializing`)
        await this.materializeAndWriteAuth(status.lease_id)
        return
      }
      this.setMessage(`Lease refreshed at ${new Date().toLocaleTimeString()}.`)
    } catch (error) {
      if (error instanceof AuthManagerClientError && shouldReacquireAfterLookupError(error.status)) {
        await this.log(`Stored lease ${this.state.lease.leaseId} missing during refresh; reacquiring`)
        this.state.lease = defaultRuntimeLeaseState(this.state.settings.machineId, this.state.settings.agentId, this.state.settings.authFilePath)
        await this.acquireAndMaterialize('refresh reacquire missing lease')
        return
      }
      this.backendReachable = false
      await this.handleError(error, 'Unable to refresh lease')
    } finally {
      await this.persist()
      this.render()
    }
  }

  async renewLease(): Promise<void> {
    if (!this.state.lease.leaseId) {
      await this.ensureLease()
      return
    }
    await this.log(`Renewing lease ${this.state.lease.leaseId}`)
    try {
      const response = await this.client.renewLease(this.state.lease.leaseId, {
        machineId: this.state.settings.machineId,
        agentId: this.state.settings.agentId,
      })
      this.backendReachable = true
      this.state.lease = await this.consumeLeaseResponse(response, 'Lease renew denied')
      this.setMessage('Lease renewed.')
    } catch (error) {
      await this.handleError(error, 'Unable to renew lease')
    } finally {
      await this.persist()
      this.render()
    }
  }

  async rotateLease(): Promise<void> {
    if (!this.state.lease.leaseId) {
      await this.acquireAndMaterialize('rotate with no lease')
      return
    }
    await this.log(`Rotating lease ${this.state.lease.leaseId}`)
    try {
      const response = await this.client.rotateLease({
        leaseId: this.state.lease.leaseId,
        machineId: this.state.settings.machineId,
        agentId: this.state.settings.agentId,
        reason: 'approaching_utilization_threshold',
      })
      this.backendReachable = true
      this.state.lease = await this.consumeLeaseResponse(response, 'Lease rotation denied')
      await this.materializeAndWriteAuth(this.state.lease.leaseId!)
      this.setMessage('Lease rotated and auth file updated.')
    } catch (error) {
      await this.handleError(error, 'Unable to rotate lease')
    } finally {
      await this.persist()
      this.render()
    }
  }

  async releaseLease(): Promise<void> {
    if (!this.state.lease.leaseId) {
      this.setMessage('No active lease to release.')
      this.render()
      return
    }
    await this.log(`Releasing lease ${this.state.lease.leaseId}`)
    try {
      const response = await this.client.releaseLease(this.state.lease.leaseId, {
        machineId: this.state.settings.machineId,
        agentId: this.state.settings.agentId,
        reason: 'Released from desktop app',
      })
      this.backendReachable = true
      if (response.status !== 'ok') {
        throw new Error(response.reason || 'Lease release denied')
      }
      this.state.lease = defaultRuntimeLeaseState(this.state.settings.machineId, this.state.settings.agentId, this.state.settings.authFilePath)
      this.setMessage('Lease released.')
    } catch (error) {
      await this.handleError(error, 'Unable to release lease')
    } finally {
      await this.persist()
      this.render()
    }
  }

  async rewriteAuthFile(): Promise<void> {
    if (!this.state.lease.leaseId) {
      await this.ensureLease()
      return
    }
    try {
      await this.materializeAndWriteAuth(this.state.lease.leaseId)
      this.setMessage('Auth file updated from active lease.')
    } catch (error) {
      await this.handleError(error, 'Unable to rewrite auth file')
    } finally {
      await this.persist()
      this.render()
    }
  }

  async openDashboard(): Promise<void> {
    const baseUrl = this.state.settings.baseUrl.replace(/\/+$/, '')
    const path = this.state.settings.openDashboardPath.trim()
    const target = path ? new URL(path, `${baseUrl}/`).toString() : baseUrl
    await openTarget(target)
  }

  async openAuthFileLocation(): Promise<void> {
    await openTarget(this.state.settings.authFilePath)
  }

  async postTelemetry(): Promise<void> {
    if (!this.state.lease.leaseId) {
      return
    }
    try {
      await this.client.postTelemetry(this.state.lease.leaseId, buildLeaseTelemetryPayload(this.state.lease))
      this.backendReachable = true
      await this.reconcileLocalAuthIfNeeded(this.state.lease.leaseId)
      const status = await this.client.getLease(this.state.lease.leaseId)
      this.state.lease = updateRuntimeStateFromLeaseStatus(this.state.lease, status)
      if (this.shouldRematerializeAuth(status)) {
        await this.log(`Credential auth changed during telemetry for lease ${status.lease_id}; rematerializing`)
        await this.materializeAndWriteAuth(status.lease_id)
      }
      await this.log(`Posted telemetry for lease ${this.state.lease.leaseId}`)
    } catch (error) {
      await this.handleError(error, 'Unable to post telemetry', false)
    } finally {
      await this.persist()
      this.render()
    }
  }

  private buildClient(): AuthManagerClient {
    return new AuthManagerClient({
      baseUrl: this.state.settings.baseUrl,
      internalApiToken: this.state.settings.internalApiToken,
      allowInsecureLocalhost: this.state.settings.allowInsecureLocalhost,
    })
  }

  private async acquireAndMaterialize(reason: string): Promise<void> {
    await this.log(`Acquiring lease (${reason})`)
    try {
      const response = await this.client.acquireLease({
        machineId: this.state.settings.machineId,
        agentId: this.state.settings.agentId,
        requestedTtlSeconds: REQUESTED_TTL_SECONDS,
        reason,
      })
      this.backendReachable = true
      this.state.lease = await this.consumeLeaseResponse(response, 'No eligible credentials available')
      await this.materializeAndWriteAuth(this.state.lease.leaseId!)
      this.setMessage('Lease acquired and auth file written.')
    } catch (error) {
      await this.handleError(error, 'Unable to acquire lease')
    }
  }

  private async materializeAndWriteAuth(leaseId: string): Promise<void> {
    const materialized = await this.client.materializeLease(leaseId, {
      machineId: this.state.settings.machineId,
      agentId: this.state.settings.agentId,
    })
    if (materialized.status !== 'ok' || !materialized.credential_material?.auth_json) {
      throw new Error(materialized.reason || 'Backend did not return auth payload for this lease')
    }
    const payload = prepareAuthPayloadForWrite(materialized.credential_material.auth_json)
    const result = await writeAuthFile(this.state.settings.authFilePath, payload)
    const fingerprint = await authPayloadFingerprint(payload)
    this.state.lease = recordAuthWrite(this.state.lease, result.writtenAt, fingerprint)
    await this.log(`Wrote auth file to ${result.path}`)
    if (materialized.lease) {
      this.state.lease = updateRuntimeStateFromLease(this.state.lease, materialized.lease)
    }
  }

  private async reconcileLocalAuthIfNeeded(leaseId: string | null): Promise<void> {
    if (!leaseId) {
      return
    }
    const localAuth = await readAuthFile(this.state.settings.authFilePath)
    if (!localAuth) {
      return
    }
    const fingerprint = await authPayloadFingerprint(localAuth)
    if (this.state.lease.lastAuthFingerprint && this.state.lease.lastAuthFingerprint === fingerprint) {
      return
    }
    const reconciled = await this.client.reconcileLeaseAuth(leaseId, {
      machineId: this.state.settings.machineId,
      agentId: this.state.settings.agentId,
      authJson: localAuth,
    })
    if (reconciled.credential_auth_updated_at) {
      this.state.lease = { ...this.state.lease, credentialAuthUpdatedAt: reconciled.credential_auth_updated_at }
    }
    if (reconciled.decision === 'manager_updated_client' && reconciled.auth_json) {
      await this.log(`Manager auth is newer for lease ${leaseId}; rewriting local auth file`)
      const result = await writeAuthFile(this.state.settings.authFilePath, reconciled.auth_json)
      const managerFingerprint = await authPayloadFingerprint(reconciled.auth_json)
      this.state.lease = recordAuthWrite(this.state.lease, result.writtenAt, managerFingerprint)
      return
    }
    const acknowledgedAt = reconciled.credential_auth_updated_at || localAuth.last_refresh || new Date().toISOString()
    this.state.lease = recordAuthWrite(this.state.lease, acknowledgedAt, fingerprint)
    if (reconciled.decision === 'client_updated_manager') {
      await this.log(`Uploaded fresher local auth to manager for lease ${leaseId}`)
    }
  }

  private async consumeLeaseResponse(response: LeaseAcquireResponse, deniedMessage: string): Promise<RuntimeLeaseState> {
    if (response.status !== 'ok' || !response.lease) {
      throw new Error(response.reason || deniedMessage)
    }
    return updateRuntimeStateFromLease(this.state.lease, response.lease)
  }

  private async handleError(error: unknown, prefix: string, logAndSurface = true): Promise<void> {
    const message = error instanceof Error ? error.message : String(error)
    this.state.lease = recordError(this.state.lease, new Date().toISOString())
    this.setMessage(`${prefix}: ${message}`)
    if (logAndSurface) {
      await this.log(`${prefix}: ${message}`)
    }
  }

  private async log(message: string): Promise<void> {
    const line = `[${new Date().toISOString()}] ${message}`
    this.logs = [...this.logs.slice(-199), line]
    await appendLogLine(line)
  }

  private setMessage(message: string | null): void {
    this.message = message
  }

  private shouldRematerializeAuth(status: LeaseStatusResponse): boolean {
    if (!this.state.lease.leaseId || !status.auth_refresh_required) {
      return false
    }
    if (!status.credential_auth_updated_at) {
      return true
    }
    if (!this.state.lease.lastAuthWriteAt) {
      return true
    }
    return status.credential_auth_updated_at > this.state.lease.lastAuthWriteAt
  }

  private currentHealthState(): LeaseHealthState {
    if (!this.backendReachable && this.state.lease.lastErrorAt) {
      return 'backend_unavailable'
    }
    if (!this.state.lease.leaseId || !this.state.lease.leaseState || !this.state.lease.expiresAt) {
      return 'no_lease'
    }
    return deriveLeaseHealthState({
      state: this.state.lease.leaseState,
      replacement_required: this.state.lease.replacementRequired,
      rotation_recommended: this.state.lease.rotationRecommended,
      expires_at: this.state.lease.expiresAt,
    })
  }

  private restartTimers(): void {
    if (this.refreshTimer !== null) {
      window.clearInterval(this.refreshTimer)
    }
    if (this.telemetryTimer !== null) {
      window.clearInterval(this.telemetryTimer)
    }
    const refreshMs = Math.max(15, this.state.settings.refreshIntervalSeconds) * 1000
    const telemetryMs = Math.max(60, this.state.settings.telemetryIntervalSeconds) * 1000
    this.refreshTimer = window.setInterval(() => void this.refreshLease(), refreshMs)
    this.telemetryTimer = window.setInterval(() => void this.postTelemetry(), telemetryMs)
  }

  private async persist(): Promise<void> {
    await savePersistedState(this.state)
  }

  private render(): void {
    this.view.render(this.getSnapshot())
  }
}
