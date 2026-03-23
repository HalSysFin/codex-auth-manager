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
    metadata: null,
    created_at: '2026-03-22T00:00:00.000Z',
    updated_at: '2026-03-22T00:00:00.000Z',
  })
  assert.equal(state.leaseId, 'lease-1')
  assert.equal(state.credentialId, 'cred-1')
  assert.equal(state.latestUtilizationPct, 12)
  assert.equal(state.authFilePath, '~/.codex/auth.json')
  assert.ok(state.lastBackendRefreshAt)
})
