import * as vscode from 'vscode'
import { AuthManagerClient, AuthManagerClientError, type AuthPayload, type LeaseStatusResponse } from './authManagerClient'
import { authFileExists, deleteAuthFile, writeAuthFile } from './authFile'
import {
  deriveLeaseHealthState,
  selectStartupAction,
  shouldReacquireAfterLookupError,
  type LeaseHealthState,
} from './leaseLifecycle'
import { requestFreshLease } from './requestFreshLease'
import { LeaseStateStore, type LeaseState } from './leaseStateStore'
import { deriveAccountDisplayName, extractAccountIdentity, formatStatusBarText, formatStatusBarTooltip } from './statusPresentation'
import { buildLeaseTelemetryPayload } from './telemetry'
import { LeaseWebviewProvider, type LeaseViewModel } from './views/leaseWebview'

type ManualAction = 'refresh' | 'renew' | 'rotate' | 'release' | 'reload' | 'requestNew'

class AuthManagerController {
  private readonly output = vscode.window.createOutputChannel('Codex Auth Manager')
  private readonly statusBar = vscode.window.createStatusBarItem(vscode.StatusBarAlignment.Right, 100)
  private readonly stateStore: LeaseStateStore
  private readonly webviewProvider: LeaseWebviewProvider
  private refreshTimer: NodeJS.Timeout | undefined
  private telemetryTimer: NodeJS.Timeout | undefined
  private state!: LeaseState
  private client!: AuthManagerClient
  private backendReachable = false
  private lastMessage: string | null = null
  private runningEnsure = false

  constructor(private readonly context: vscode.ExtensionContext) {
    this.stateStore = new LeaseStateStore(context.globalState)
    this.webviewProvider = new LeaseWebviewProvider(context.extensionUri, {
      onRefresh: () => void this.refreshLease('refresh'),
      onRenew: () => void this.renewLease(),
      onRotate: () => void this.rotateLease(),
      onRequestNewLease: () => void this.requestNewLease(),
      onRelease: () => void this.releaseLease(),
      onReloadAuth: () => void this.reloadCodexAuth(),
      onReloadWindow: () => void this.reloadWindow(),
      onOpenDashboard: () => void this.openDashboard(),
      onVisible: () => void this.refreshLease('refresh'),
    })
    this.statusBar.command = 'authManager.showLeaseView'
    this.statusBar.show()
  }


  private machineHostContext(): string | undefined {
    const authority = vscode.workspace.workspaceFolders?.[0]?.uri.authority?.trim()
    if (authority) {
      return authority
    }
    const remoteName = vscode.env.remoteName?.trim()
    if (remoteName) {
      return remoteName
    }
    return undefined
  }

  async activate(): Promise<void> {
    const configuration = vscode.workspace.getConfiguration()
    const machineId = await this.stateStore.getOrCreateMachineId(
      configuration.get<string>('authManager.machineId'),
      vscode.env.machineId,
      this.machineHostContext(),
    )
    const agentId = await this.stateStore.getOrCreateAgentId(configuration.get<string>('authManager.agentId'))
    this.state = this.stateStore.load(machineId, agentId, this.authFilePath())
    this.rebuildClient()
    this.registerCommands()
    this.context.subscriptions.push(
      this.output,
      this.statusBar,
      vscode.window.registerWebviewViewProvider(LeaseWebviewProvider.viewType, this.webviewProvider),
      vscode.workspace.onDidChangeConfiguration((event) => {
        if (event.affectsConfiguration('authManager')) {
          this.state = { ...this.state, authFilePath: this.authFilePath() }
          this.rebuildClient()
          void this.ensureLease()
          this.restartTimers()
        }
      }),
    )
    this.updatePresentation()
    await this.ensureLease()
    this.restartTimers()
  }

  private registerCommands(): void {
    this.context.subscriptions.push(
      vscode.commands.registerCommand('authManager.ensureLease', async () => this.ensureLease()),
      vscode.commands.registerCommand('authManager.refreshLease', async () => this.refreshLease('refresh')),
      vscode.commands.registerCommand('authManager.requestNewLease', async () => this.requestNewLease()),
      vscode.commands.registerCommand('authManager.rotateLease', async () => this.rotateLease()),
      vscode.commands.registerCommand('authManager.releaseLease', async () => this.releaseLease()),
      vscode.commands.registerCommand('authManager.reloadCodexAuth', async () => this.reloadCodexAuth()),
      vscode.commands.registerCommand('authManager.reloadWindow', async () => this.reloadWindow()),
      vscode.commands.registerCommand('authManager.openDashboard', async () => this.openDashboard()),
      vscode.commands.registerCommand('authManager.showLeaseView', async () => this.showLeaseView()),
    )
  }

  private rebuildClient(): void {
    const config = vscode.workspace.getConfiguration()
    this.client = new AuthManagerClient({
      baseUrl: config.get<string>('authManager.baseUrl', 'http://127.0.0.1:8080'),
      internalApiToken: config.get<string>('authManager.internalApiToken', ''),
      allowInsecureLocalhost: config.get<boolean>('authManager.allowInsecureLocalhost', true),
    })
  }


  private autoReloadWindowOnLeaseChange(): boolean {
    return vscode.workspace
      .getConfiguration()
      .get<boolean>('authManager.autoReloadWindowOnLeaseChange', false)
  }

  private releaseLeaseOnShutdown(): boolean {
    return vscode.workspace
      .getConfiguration()
      .get<boolean>('authManager.releaseLeaseOnShutdown', true)
  }

  private deleteAuthFileOnShutdown(): boolean {
    return vscode.workspace
      .getConfiguration()
      .get<boolean>('authManager.deleteAuthFileOnShutdown', true)
  }

  private restartTimers(): void {
    if (this.refreshTimer) {
      clearInterval(this.refreshTimer)
    }
    if (this.telemetryTimer) {
      clearInterval(this.telemetryTimer)
    }
    const refreshMs = Math.max(15, vscode.workspace.getConfiguration().get<number>('authManager.refreshIntervalSeconds', 60)) * 1000
    const telemetryMs = Math.max(60, vscode.workspace.getConfiguration().get<number>('authManager.telemetryIntervalSeconds', 300)) * 1000
    this.refreshTimer = setInterval(() => void this.refreshLease('refresh'), refreshMs)
    this.telemetryTimer = setInterval(() => void this.postTelemetry(), telemetryMs)
  }

  private log(message: string): void {
    this.output.appendLine(`[${new Date().toISOString()}] ${message}`)
  }

  private setMessage(message: string | null): void {
    this.lastMessage = message
    this.updatePresentation()
  }

  private updatePresentation(): void {
    const healthState = this.currentHealthState()
    this.statusBar.text = formatStatusBarText(this.state, healthState)
    this.statusBar.tooltip = formatStatusBarTooltip(this.state, healthState)
    this.webviewProvider.update(this.currentViewModel())
  }

  private currentHealthState(): LeaseHealthState {
    if (!this.backendReachable && this.state.lastErrorAt) {
      return 'backend_unavailable'
    }
    if (!this.state.leaseId || !this.state.leaseState) {
      return 'no_lease'
    }
    if (!this.state.expiresAt) {
      return 'no_lease'
    }
    return deriveLeaseHealthState({
      state: this.state.leaseState,
      replacement_required: this.state.replacementRequired,
      rotation_recommended: this.state.rotationRecommended,
      expires_at: this.state.expiresAt,
    })
  }

  private currentViewModel(): LeaseViewModel {
    const config = vscode.workspace.getConfiguration()
    return {
      state: this.state,
      healthState: this.currentHealthState(),
      baseUrl: config.get<string>('authManager.baseUrl', 'http://127.0.0.1:8080'),
      authFilePath: config.get<string>('authManager.authFilePath', '~/.codex/auth.json'),
      backendReachable: this.backendReachable,
      lastMessage: this.lastMessage,
    }
  }

  async ensureLease(): Promise<void> {
    if (this.runningEnsure) {
      return
    }
    this.runningEnsure = true
    try {
      this.log('Starting ensureLease flow')
      if (!this.state.leaseId) {
        await this.acquireAndMaterializeLease('startup ensure', false)
        return
      }
      let status: LeaseStatusResponse
      try {
        status = await this.client.getLease(this.state.leaseId)
        this.backendReachable = true
      } catch (error) {
        if (shouldReacquireAfterLookupError(error instanceof AuthManagerClientError ? error.status : null)) {
          this.log(`Stored lease ${this.state.leaseId} is gone; reacquiring`)
          this.state = await this.stateStore.clear(this.state.machineId, this.state.agentId, this.authFilePath())
          await this.acquireAndMaterializeLease('startup reacquire missing lease', false)
          return
        }
        this.backendReachable = false
        await this.handleBackendError(error, 'Failed to refresh current lease on startup', false)
        return
      }
      this.state = await this.stateStore.updateFromLeaseStatus(this.state, status)
      const startupAction = selectStartupAction({
        leaseId: this.state.leaseId,
        leaseStatus: status,
        autoRotate: vscode.workspace.getConfiguration().get<boolean>('authManager.autoRotate', true),
        rotationPolicy: status.effective_rotation_policy ?? 'replacement_required_only',
        autoRenew: vscode.workspace.getConfiguration().get<boolean>('authManager.autoRenew', true),
      })
      if (startupAction === 'reacquire') {
        this.log(`Lease ${status.lease_id} is no longer usable; acquiring replacement`)
        await this.acquireAndMaterializeLease('startup reacquire', false)
        return
      }
      if (startupAction === 'rotate') {
        await this.rotateLease(false)
        return
      }
      if (startupAction === 'renew') {
        await this.renewLease(false)
      }
      if (!(await authFileExists(this.authFilePath()))) {
        this.log('Auth file missing; materializing active lease')
        await this.materializeAndWriteAuth(status.lease_id)
      }
      this.setMessage('Lease is healthy.')
    } finally {
      this.runningEnsure = false
      this.updatePresentation()
    }
  }

  async refreshLease(origin: ManualAction): Promise<void> {
    if (!this.state.leaseId) {
      await this.ensureLease()
      return
    }
    this.log(`Refreshing lease state (${origin})`)
    try {
      const status = await this.client.getLease(this.state.leaseId)
      this.backendReachable = true
      this.state = await this.stateStore.updateFromLeaseStatus(this.state, status)
      const refreshAction = selectStartupAction({
        leaseId: this.state.leaseId,
        leaseStatus: status,
        autoRotate: vscode.workspace.getConfiguration().get<boolean>('authManager.autoRotate', true),
        rotationPolicy: status.effective_rotation_policy ?? 'replacement_required_only',
        autoRenew: vscode.workspace.getConfiguration().get<boolean>('authManager.autoRenew', true),
      })
      if (refreshAction === 'reacquire') {
        this.log(`Lease ${status.lease_id} is no longer usable during refresh; reacquiring`)
        this.state = await this.stateStore.clear(this.state.machineId, this.state.agentId, this.authFilePath())
        await this.acquireAndMaterializeLease('refresh reacquire', false)
        return
      }
      if (refreshAction === 'rotate') {
        await this.rotateLease(false)
        return
      }
      if (refreshAction === 'renew') {
        await this.renewLease(false)
      }
      this.setMessage(`Lease refreshed at ${new Date().toLocaleTimeString()}.`)
    } catch (error) {
      if (shouldReacquireAfterLookupError(error instanceof AuthManagerClientError ? error.status : null)) {
        this.log(`Stored lease ${this.state.leaseId} was not found during refresh; reacquiring`)
        this.state = await this.stateStore.clear(this.state.machineId, this.state.agentId, this.authFilePath())
        await this.acquireAndMaterializeLease('refresh reacquire missing lease', false)
        return
      }
      this.backendReachable = false
      await this.handleBackendError(error, 'Unable to refresh lease state', false)
      this.setMessage('Backend unavailable; keeping current lease and retrying in background.')
    } finally {
      this.updatePresentation()
    }
  }

  async renewLease(showPopup = true): Promise<void> {
    if (!this.state.leaseId) {
      await this.ensureLease()
      return
    }
    this.log(`Renewing lease ${this.state.leaseId}`)
    try {
      const response = await this.client.renewLease(this.state.leaseId, {
        machineId: this.state.machineId,
        agentId: this.state.agentId,
      })
      this.backendReachable = true
      if (response.status !== 'ok' || !response.lease) {
        throw new Error(response.reason || 'Lease renew denied')
      }
      this.state = await this.stateStore.updateFromLease(this.state, response.lease)
      this.setMessage('Lease renewed.')
    } catch (error) {
      await this.handleBackendError(error, 'Unable to renew lease', showPopup)
      if (!showPopup) {
        this.setMessage('Backend unavailable during renew; keeping current lease.')
      }
    } finally {
      this.updatePresentation()
    }
  }

  async rotateLease(showPopup = true): Promise<void> {
    if (!this.state.leaseId) {
      await this.acquireAndMaterializeLease('rotate with no lease')
      return
    }
    this.log(`Rotating lease ${this.state.leaseId}`)
    const previousLeaseId = this.state.leaseId
    try {
      const response = await this.client.rotateLease({
        leaseId: this.state.leaseId,
        machineId: this.state.machineId,
        agentId: this.state.agentId,
        reason: 'approaching_utilization_threshold',
      })
      this.backendReachable = true
      if (response.status !== 'ok' || !response.lease) {
        throw new Error(response.reason || 'Lease rotation denied')
      }
      this.state = await this.stateStore.updateFromLease(this.state, response.lease)
      await this.materializeAndWriteAuth(response.lease.id)
      this.setMessage('Lease rotated and auth file updated.')
      if (
        this.autoReloadWindowOnLeaseChange()
        && previousLeaseId
        && response.lease.id
        && response.lease.id !== previousLeaseId
      ) {
        this.log(`Lease changed (${previousLeaseId} -> ${response.lease.id}); reloading window`)
        await vscode.commands.executeCommand('workbench.action.reloadWindow')
      }
    } catch (error) {
      await this.handleBackendError(error, 'Unable to rotate lease', showPopup)
      if (!showPopup) {
        this.setMessage('Backend unavailable during rotate; keeping current lease.')
      }
    } finally {
      this.updatePresentation()
    }
  }

  async requestNewLease(): Promise<void> {
    this.log('Requesting a fresh auth lease')
    try {
      await requestFreshLease({
        currentLeaseId: this.state.leaseId,
        releaseCurrentLease: async () => {
          if (!this.state.leaseId) {
            return
          }
          try {
            await this.client.releaseLease(this.state.leaseId, {
              machineId: this.state.machineId,
              agentId: this.state.agentId,
              reason: 'Manual fresh lease request from VS Code extension',
            })
          } catch (error) {
            this.log(`Existing lease release before fresh acquire failed: ${error instanceof Error ? error.message : String(error)}`)
          } finally {
            this.state = await this.stateStore.clear(this.state.machineId, this.state.agentId, this.authFilePath())
          }
        },
        acquireFreshLease: async () => {
          await this.acquireAndMaterializeLease('manual request new lease')
        },
      })
      this.setMessage(`Fresh auth lease acquired for ${deriveAccountDisplayName(this.state)}.`)
      void vscode.window.showInformationMessage(`Fresh auth lease acquired for ${deriveAccountDisplayName(this.state)}.`)
    } catch (error) {
      await this.handleBackendError(error, 'Unable to request a fresh auth lease')
    } finally {
      this.updatePresentation()
    }
  }

  async releaseLease(): Promise<void> {
    if (!this.state.leaseId) {
      this.setMessage('No active lease to release.')
      return
    }
    this.log(`Releasing lease ${this.state.leaseId}`)
    try {
      const response = await this.client.releaseLease(this.state.leaseId, {
        machineId: this.state.machineId,
        agentId: this.state.agentId,
        reason: 'Released from VS Code extension',
      })
      this.backendReachable = true
      if (response.status !== 'ok') {
        throw new Error(response.reason || 'Lease release denied')
      }
      this.state = await this.stateStore.clear(this.state.machineId, this.state.agentId, this.authFilePath())
      this.setMessage('Lease released.')
    } catch (error) {
      await this.handleBackendError(error, 'Unable to release lease')
    } finally {
      this.updatePresentation()
    }
  }

  async reloadCodexAuth(): Promise<void> {
    if (!this.state.leaseId) {
      await this.ensureLease()
      return
    }
    this.log(`Reloading Codex auth from lease ${this.state.leaseId}`)
    try {
      await this.materializeAndWriteAuth(this.state.leaseId)
      const selection = await vscode.window.showInformationMessage(
        'Codex auth file updated. Reload the VS Code window if you want dependent tooling to reconnect immediately.',
        'Reload Window',
        'Later',
      )
      if (selection === 'Reload Window') {
        await vscode.commands.executeCommand('workbench.action.reloadWindow')
      }
    } catch (error) {
      await this.handleBackendError(error, 'Unable to reload Codex auth')
    } finally {
      this.updatePresentation()
    }
  }

  async reloadWindow(): Promise<void> {
    await vscode.commands.executeCommand('workbench.action.reloadWindow')
  }

  async openDashboard(): Promise<void> {
    const config = vscode.workspace.getConfiguration()
    const baseUrl = config.get<string>('authManager.baseUrl', 'http://127.0.0.1:8080').replace(/\/+$/, '')
    const target = `${baseUrl}/ui`
    await vscode.env.openExternal(vscode.Uri.parse(target))
  }

  async deactivate(): Promise<void> {
    if (this.refreshTimer) {
      clearInterval(this.refreshTimer)
      this.refreshTimer = undefined
    }
    if (this.telemetryTimer) {
      clearInterval(this.telemetryTimer)
      this.telemetryTimer = undefined
    }

    if (this.releaseLeaseOnShutdown() && this.state?.leaseId) {
      try {
        await this.client.releaseLease(this.state.leaseId, {
          machineId: this.state.machineId,
          agentId: this.state.agentId,
          reason: 'Released on VS Code shutdown',
        })
        this.log(`Released lease ${this.state.leaseId} during shutdown`)
      } catch (error) {
        this.log(`Shutdown release failed (continuing): ${error instanceof Error ? error.message : String(error)}`)
      }
    }

    if (this.deleteAuthFileOnShutdown()) {
      try {
        const deleted = await deleteAuthFile(this.authFilePath())
        this.log(deleted ? `Deleted auth file at ${this.authFilePath()} on shutdown` : 'No auth file to delete on shutdown')
      } catch (error) {
        this.log(`Shutdown auth file delete failed (continuing): ${error instanceof Error ? error.message : String(error)}`)
      }
      this.state = await this.stateStore.clear(this.state.machineId, this.state.agentId, this.authFilePath())
    }
  }

  private async acquireAndMaterializeLease(reason: string, showPopup = true): Promise<void> {
    this.log(`Acquiring lease (${reason})`)
    const previousLeaseId = this.state.leaseId
    try {
      const response = await this.client.acquireLease({
        machineId: this.state.machineId,
        agentId: this.state.agentId,
        requestedTtlSeconds: 1800,
        reason,
      })
      this.backendReachable = true
      if (response.status !== 'ok' || !response.lease) {
        throw new Error(response.reason || 'No eligible credentials available')
      }
      this.state = await this.stateStore.updateFromLease(this.state, response.lease)
      await this.materializeAndWriteAuth(response.lease.id)
      this.setMessage('Lease acquired and auth file written.')
      if (
        this.autoReloadWindowOnLeaseChange()
        && previousLeaseId
        && response.lease.id
        && response.lease.id !== previousLeaseId
      ) {
        this.log(`Lease changed (${previousLeaseId} -> ${response.lease.id}); reloading window`)
        await vscode.commands.executeCommand('workbench.action.reloadWindow')
      }
    } catch (error) {
      await this.handleBackendError(error, 'Unable to acquire lease', showPopup)
      if (!showPopup) {
        this.setMessage('Backend unavailable; will retry acquiring lease in background.')
      }
    } finally {
      this.updatePresentation()
    }
  }

  private async materializeAndWriteAuth(leaseId: string): Promise<void> {
    const materialized = await this.client.fetchAuthPayloadForLease(leaseId, {
      machineId: this.state.machineId,
      agentId: this.state.agentId,
    })
    if (materialized.status !== 'ok' || !materialized.credential_material?.auth_json) {
      throw new Error(materialized.reason || 'Backend did not return auth payload for this lease')
    }
    const payload = materialized.credential_material.auth_json
    await this.writePayloadToAuthFile(payload)
    const identity = extractAccountIdentity(materialized)
    this.state = {
      ...this.state,
      accountLabel: identity.accountLabel,
      accountName: identity.accountName,
    }
    await this.stateStore.save(this.state)
    if (materialized.lease) {
      this.state = await this.stateStore.updateFromLease(this.state, materialized.lease)
    }
  }

  private async writePayloadToAuthFile(payload: AuthPayload): Promise<void> {
    const result = await writeAuthFile(this.authFilePath(), payload)
    this.state = { ...this.state, authFilePath: this.authFilePath() }
    this.state = await this.stateStore.recordAuthWrite(this.state, result.writtenAt)
    this.log(`Wrote auth file to ${result.path}`)
  }

  private async postTelemetry(): Promise<void> {
    if (!this.state.leaseId) {
      return
    }
    try {
      await this.client.postTelemetry(this.state.leaseId, buildLeaseTelemetryPayload(this.state))
      this.backendReachable = true
      this.log(`Posted telemetry for lease ${this.state.leaseId}`)
    } catch (error) {
      await this.handleBackendError(error, 'Unable to post telemetry', false)
    } finally {
      this.updatePresentation()
    }
  }

  private authFilePath(): string {
    return vscode.workspace.getConfiguration().get<string>('authManager.authFilePath', '~/.codex/auth.json')
  }

  private async showLeaseView(): Promise<void> {
    await vscode.commands.executeCommand('workbench.view.extension.authManager')
    await vscode.commands.executeCommand('authManager.leaseView.focus')
  }

  private async handleBackendError(error: unknown, userMessage: string, showPopup = true): Promise<void> {
    const message = error instanceof Error ? error.message : String(error)
    this.log(`${userMessage}: ${message}`)
    this.state = await this.stateStore.recordError(this.state, new Date().toISOString())
    this.setMessage(message)
    if (showPopup) {
      void vscode.window.showWarningMessage(`${userMessage}: ${message}`)
    }
  }
}

export async function activate(context: vscode.ExtensionContext): Promise<void> {
  controllerInstance = new AuthManagerController(context)
  await controllerInstance.activate()
}

let controllerInstance: AuthManagerController | null = null

export async function deactivate(): Promise<void> {
  if (!controllerInstance) {
    return
  }
  await controllerInstance.deactivate()
  controllerInstance = null
}
