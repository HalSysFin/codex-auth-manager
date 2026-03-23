import * as vscode from 'vscode'
import type { LeaseHealthState } from '../leaseLifecycle'
import type { LeaseState } from '../leaseStateStore'

export interface LeaseViewModel {
  state: LeaseState
  healthState: LeaseHealthState
  baseUrl: string
  authFilePath: string
  backendReachable: boolean
  lastMessage: string | null
}

export interface LeaseWebviewCommandHandlers {
  onRefresh(): void
  onRenew(): void
  onRotate(): void
  onRequestNewLease(): void
  onRelease(): void
  onReloadAuth(): void
  onReloadWindow(): void
  onOpenDashboard(): void
  onVisible(): void
}

export class LeaseWebviewProvider implements vscode.WebviewViewProvider {
  public static readonly viewType = 'authManager.leaseView'
  private view: vscode.WebviewView | undefined

  constructor(
    private readonly extensionUri: vscode.Uri,
    private readonly handlers: LeaseWebviewCommandHandlers,
  ) {}

  resolveWebviewView(webviewView: vscode.WebviewView): void {
    this.view = webviewView
    const webview = webviewView.webview
    webview.options = {
      enableScripts: true,
      localResourceRoots: [vscode.Uri.joinPath(this.extensionUri, 'media')],
    }
    const cssUri = webview.asWebviewUri(vscode.Uri.joinPath(this.extensionUri, 'media', 'lease.css'))
    webview.html = this.renderHtml(cssUri)
    webview.onDidReceiveMessage((message: { command?: string }) => {
      switch (message.command) {
        case 'refresh':
          this.handlers.onRefresh()
          break
        case 'renew':
          this.handlers.onRenew()
          break
        case 'rotate':
          this.handlers.onRotate()
          break
        case 'requestNewLease':
          this.handlers.onRequestNewLease()
          break
        case 'release':
          this.handlers.onRelease()
          break
        case 'reloadAuth':
          this.handlers.onReloadAuth()
          break
        case 'reloadWindow':
          this.handlers.onReloadWindow()
          break
        case 'openDashboard':
          this.handlers.onOpenDashboard()
          break
        default:
          break
      }
    })
    webviewView.onDidChangeVisibility(() => {
      if (webviewView.visible) {
        this.handlers.onVisible()
      }
    })
  }

  update(model: LeaseViewModel): void {
    if (!this.view) {
      return
    }
    this.view.webview.postMessage({
      type: 'state',
      payload: model,
    })
  }

  private renderHtml(cssUri: vscode.Uri): string {
    return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <link rel="stylesheet" href="${cssUri}" />
    <title>Codex Auth Manager</title>
  </head>
  <body>
    <div class="wrap">
      <div class="top">
        <h1>Codex Auth Manager</h1>
        <div id="healthPill" class="pill">No Lease</div>
      </div>
      <div id="accountName" class="accountName">No Lease</div>
      <div class="actions">
        <button data-command="refresh">Refresh</button>
        <button data-command="renew">Renew</button>
        <button data-command="requestNewLease">Request New Auth Lease</button>
        <button data-command="rotate">Rotate</button>
        <button data-command="release">Release</button>
        <button data-command="openDashboard">Open Dashboard</button>
        <button data-command="reloadAuth">Reload Codex Auth</button>
        <button data-command="reloadWindow">Reload Window</button>
      </div>
      <div id="message" class="message"></div>
      <div class="grid" id="details"></div>
    </div>
    <script>
      const vscode = acquireVsCodeApi();
      document.querySelectorAll('[data-command]').forEach((button) => {
        button.addEventListener('click', () => {
          vscode.postMessage({ command: button.getAttribute('data-command') });
        });
      });
      function fmt(value) {
        if (value === null || value === undefined || value === '') {
          return 'Unavailable';
        }
        return String(value);
      }
      function render(payload) {
        const healthPill = document.getElementById('healthPill');
        const accountName = document.getElementById('accountName');
        const details = document.getElementById('details');
        const message = document.getElementById('message');
        const state = payload.state;
        const titleMap = {
          active: 'Active',
          expiring: 'Expiring',
          rotation_required: 'Rotation Required',
          revoked: 'Revoked',
          no_lease: 'No Lease',
          backend_unavailable: 'Backend Unavailable',
        };
        healthPill.textContent = titleMap[payload.healthState] || payload.healthState.replace(/_/g, ' ');
        healthPill.className = 'pill ' + payload.healthState;
        accountName.textContent = state.accountLabel || state.accountName || state.credentialId || 'No Lease';
        message.textContent = payload.lastMessage || '';
        message.style.display = payload.lastMessage ? 'block' : 'none';
        const rows = [
          ['Account', state.accountLabel || state.accountName || state.credentialId],
          ['Lease State', state.leaseState],
          ['Credential Id', state.credentialId],
          ['Lease Id', state.leaseId],
          ['Issued At', state.issuedAt],
          ['Expires At', state.expiresAt],
          ['Latest Utilization %', state.latestUtilizationPct],
          ['Latest Quota Remaining', state.latestQuotaRemaining],
          ['Latest Telemetry At', state.latestTelemetryAt],
          ['Last Auth File Write', state.lastAuthWriteAt],
          ['Last Backend Refresh', state.lastBackendRefreshAt],
          ['Replacement Required', state.replacementRequired],
          ['Rotation Recommended', state.rotationRecommended],
          ['Machine Id', state.machineId],
          ['Agent Id', state.agentId],
          ['Backend Base URL', payload.baseUrl],
          ['Auth File Path', payload.authFilePath],
          ['Backend Reachable', payload.backendReachable],
        ];
        details.innerHTML = rows.map(([label, value]) => {
          return '<div class="card"><label>' + label + '</label><div class="value">' + fmt(value) + '</div></div>';
        }).join('');
      }
      window.addEventListener('message', (event) => {
        if (event.data?.type === 'state') {
          render(event.data.payload);
        }
      });
    </script>
  </body>
</html>`
  }
}
