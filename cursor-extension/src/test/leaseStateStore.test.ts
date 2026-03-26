import test from 'node:test'
import assert from 'node:assert/strict'
import { LeaseStateStore, derivePersistedAgentId, derivePersistedMachineId } from '../leaseStateStore'

class MemoryMemento {
  private readonly store = new Map<string, unknown>()

  get<T>(key: string, defaultValue?: T): T | undefined {
    return (this.store.has(key) ? this.store.get(key) : defaultValue) as T | undefined
  }

  async update(key: string, value: unknown): Promise<void> {
    this.store.set(key, value)
  }
}

test('derivePersistedMachineId uses configured value when present', () => {
  assert.equal(derivePersistedMachineId('machine-a'), 'machine-a')
})

test('derivePersistedAgentId falls back to vscode-extension', () => {
  assert.equal(derivePersistedAgentId(''), 'vscode-extension')
})

test('LeaseStateStore persists lease metadata', async () => {
  const store = new LeaseStateStore(new MemoryMemento())
  const machineId = await store.getOrCreateMachineId('machine-a')
  const agentId = await store.getOrCreateAgentId('agent-a')
  let state = store.load(machineId, agentId, '~/.codex/auth.json')
  state = await store.updateFromLease(state, {
    id: 'lease-1',
    credential_id: 'cred-1',
    machine_id: machineId,
    agent_id: agentId,
    state: 'active',
    issued_at: '2026-03-22T00:00:00.000Z',
    expires_at: '2026-03-22T01:00:00.000Z',
    renewed_at: null,
    revoked_at: null,
    released_at: null,
    rotation_reason: null,
    replacement_lease_id: null,
    last_telemetry_at: null,
    latest_utilization_pct: 12,
    latest_quota_remaining: 1234,
    last_success_at: null,
    last_error_at: null,
    reason: null,
    metadata: { label: 'max' },
    created_at: '2026-03-22T00:00:00.000Z',
    updated_at: '2026-03-22T00:00:00.000Z',
  })
  assert.equal(state.leaseId, 'lease-1')
  assert.equal(state.credentialId, 'cred-1')
  assert.equal(state.accountLabel, 'max')
  assert.equal(state.latestUtilizationPct, 12)
  assert.equal(state.authFilePath, '~/.codex/auth.json')
  assert.ok(state.lastBackendRefreshAt)
})


test('derivePersistedMachineId prefers runtime machine fingerprint when config is blank', () => {
  assert.equal(derivePersistedMachineId('', 'runtime-machine-id'), 'vscode-runtime-machine-id')
})

test('derivePersistedMachineId includes host context for remote targets', () => {
  assert.equal(
    derivePersistedMachineId('', 'runtime-machine-id', 'ssh-remote+server-a'),
    'vscode-runtime-machine-id-ssh-remote-server-a',
  )
})

test('derivePersistedMachineId includes remote name plus hostname context', () => {
  assert.equal(
    derivePersistedMachineId('', 'runtime-machine-id', 'ssh-remote+debian'),
    'vscode-runtime-machine-id-ssh-remote-debian',
  )
})

test('getOrCreateMachineId replaces legacy generated ids with runtime fingerprint ids', async () => {
  const memento = new MemoryMemento()
  await memento.update('authManager.machineId', 'vscode-oldhost-1a2b3c4d')
  const store = new LeaseStateStore(memento)
  const machineId = await store.getOrCreateMachineId('', 'runtime-machine-id', 'ssh-remote+server-a')
  assert.equal(machineId, 'vscode-runtime-machine-id-ssh-remote-server-a')
})

test('getOrCreateMachineId honors configured ids over persisted ids', async () => {
  const memento = new MemoryMemento()
  await memento.update('authManager.machineId', 'old-machine')
  const store = new LeaseStateStore(memento)
  const machineId = await store.getOrCreateMachineId('new-machine', 'runtime-machine-id', 'ssh-remote+server-a')
  assert.equal(machineId, 'new-machine')
})

test('load clears stale lease state when machine id changes', async () => {
  const store = new LeaseStateStore(new MemoryMemento())
  await store.save({
    ...store.load('machine-a', 'agent-a', '~/.codex/auth.json'),
    leaseId: 'lease-1',
    leaseState: 'active',
  })
  const state = store.load('machine-b', 'agent-a', '~/.codex/auth.json')
  assert.equal(state.machineId, 'machine-b')
  assert.equal(state.leaseId, null)
  assert.equal(state.leaseState, null)
})


test('getOrCreateMachineId changes across remote hosts for the same local client', async () => {
  const store = new LeaseStateStore(new MemoryMemento())
  const machineA = await store.getOrCreateMachineId('', 'runtime-machine-id', 'ssh-remote+server-a')
  const machineB = await store.getOrCreateMachineId('', 'runtime-machine-id', 'ssh-remote+server-b')
  assert.equal(machineA, 'vscode-runtime-machine-id-ssh-remote-server-a')
  assert.equal(machineB, 'vscode-runtime-machine-id-ssh-remote-server-b')
})

test('getOrCreateMachineId replaces generic remote ids with host-specific ids', async () => {
  const memento = new MemoryMemento()
  await memento.update('authManager.machineId', 'vscode-runtime-machine-id-ssh-remote')
  const store = new LeaseStateStore(memento)
  const machineId = await store.getOrCreateMachineId('', 'runtime-machine-id', 'ssh-remote+debian')
  assert.equal(machineId, 'vscode-runtime-machine-id-ssh-remote-debian')
})
