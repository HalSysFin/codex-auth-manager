import { randomUUID } from 'node:crypto'
import type * as vscode from 'vscode'
import {
  defaultRuntimeLeaseState,
  recordAuthWrite as sharedRecordAuthWrite,
  recordError as sharedRecordError,
  updateRuntimeStateFromLease,
  updateRuntimeStateFromLeaseStatus,
} from '../../packages/lease-runtime/src/runtimeState.js'
import type { Lease, LeaseStatusResponse, RuntimeLeaseState } from '../../packages/lease-runtime/src/types.js'

const STATE_KEY = 'authManager.leaseState'
const MACHINE_ID_KEY = 'authManager.machineId'
const AGENT_ID_KEY = 'authManager.agentId'

export type LeaseState = RuntimeLeaseState

export interface MementoLike {
  get<T>(key: string, defaultValue?: T): T | undefined
  update(key: string, value: unknown): Thenable<void>
}

export function defaultLeaseState(machineId: string, agentId: string, authFilePath = '~/.codex/auth.json'): LeaseState {
  return defaultRuntimeLeaseState(machineId, agentId, authFilePath)
}

function sanitizeMachineFragment(value: string): string {
  const sanitized = value.replace(/[^a-zA-Z0-9._-]/g, '-')
  return sanitized.length > 32 ? sanitized.slice(0, 32) : sanitized
}

function isLegacyGeneratedMachineId(value: string): boolean {
  return /^vscode-[a-zA-Z0-9._-]+-[a-f0-9]{8}$/.test(value)
}

export function derivePersistedMachineId(configured?: string, runtimeMachineId?: string, hostContext?: string): string {
  const trimmed = configured?.trim()
  if (trimmed) {
    return trimmed
  }
  const runtimeTrimmed = runtimeMachineId?.trim()
  const hostTrimmed = hostContext?.trim()
  if (runtimeTrimmed) {
    const fragments = [sanitizeMachineFragment(runtimeTrimmed)]
    if (hostTrimmed) {
      fragments.push(sanitizeMachineFragment(hostTrimmed))
    }
    return `vscode-${fragments.join('-')}`
  }
  if (hostTrimmed) {
    return `vscode-${sanitizeMachineFragment(hostTrimmed)}-${randomUUID().slice(0, 8)}`
  }
  return `vscode-${randomUUID().slice(0, 12)}`
}

export function derivePersistedAgentId(configured?: string): string {
  const trimmed = configured?.trim()
  if (trimmed) {
    return trimmed
  }
  return 'vscode-extension'
}

export class LeaseStateStore {
  constructor(private readonly memento: MementoLike) {}

  async getOrCreateMachineId(configured?: string, runtimeMachineId?: string, hostContext?: string): Promise<string> {
    const configuredTrimmed = configured?.trim()
    if (configuredTrimmed) {
      await this.memento.update(MACHINE_ID_KEY, configuredTrimmed)
      return configuredTrimmed
    }

    const existing = this.memento.get<string>(MACHINE_ID_KEY)?.trim()
    const derivedRuntimeId = runtimeMachineId?.trim() || hostContext?.trim()
      ? derivePersistedMachineId(undefined, runtimeMachineId, hostContext)
      : undefined

    if (derivedRuntimeId) {
      if (!existing || isLegacyGeneratedMachineId(existing) || existing !== derivedRuntimeId) {
        await this.memento.update(MACHINE_ID_KEY, derivedRuntimeId)
        return derivedRuntimeId
      }
      return existing
    }

    if (existing) {
      return existing
    }

    const machineId = derivePersistedMachineId()
    await this.memento.update(MACHINE_ID_KEY, machineId)
    return machineId
  }

  async getOrCreateAgentId(configured?: string): Promise<string> {
    const existing = this.memento.get<string>(AGENT_ID_KEY)
    if (existing?.trim()) {
      return existing
    }
    const agentId = derivePersistedAgentId(configured)
    await this.memento.update(AGENT_ID_KEY, agentId)
    return agentId
  }

  load(machineId: string, agentId: string, authFilePath = '~/.codex/auth.json'): LeaseState {
    const stored = this.memento.get<LeaseState>(STATE_KEY)
    if (!stored || stored.machineId !== machineId || stored.agentId !== agentId) {
      return defaultLeaseState(machineId, agentId, authFilePath)
    }
    return {
      ...defaultLeaseState(machineId, agentId, authFilePath),
      ...stored,
      machineId,
      agentId,
      authFilePath,
    }
  }

  async save(state: LeaseState): Promise<void> {
    await this.memento.update(STATE_KEY, state)
  }

  async clear(machineId: string, agentId: string, authFilePath = '~/.codex/auth.json'): Promise<LeaseState> {
    const next = defaultLeaseState(machineId, agentId, authFilePath)
    await this.save(next)
    return next
  }

  async updateFromLease(state: LeaseState, lease: Lease): Promise<LeaseState> {
    const next = updateRuntimeStateFromLease(state, lease)
    await this.save(next)
    return next
  }

  async updateFromLeaseStatus(state: LeaseState, status: LeaseStatusResponse): Promise<LeaseState> {
    const next = updateRuntimeStateFromLeaseStatus(state, status)
    await this.save(next)
    return next
  }

  async recordAuthWrite(state: LeaseState, atIso: string): Promise<LeaseState> {
    const next = sharedRecordAuthWrite(state, atIso)
    await this.save(next)
    return next
  }

  async recordError(state: LeaseState, atIso: string): Promise<LeaseState> {
    const next = sharedRecordError(state, atIso)
    await this.save(next)
    return next
  }
}
