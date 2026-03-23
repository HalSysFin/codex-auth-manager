import type { LeaseHealthState, RuntimeLeaseState, RuntimeSettings } from '../../packages/lease-runtime/src/types.js'

export interface HeadlessConfigPaths {
  configDir: string
  stateDir: string
  configFile: string
  stateFile: string
  logFile: string
}

export interface HeadlessOutput {
  info(message: string): void
  warn(message: string): void
  error(message: string): void
}

export interface HeadlessStateDocument {
  settings: RuntimeSettings
  lease: RuntimeLeaseState
}

export interface HeadlessStatusSummary {
  healthState: LeaseHealthState
  lease: RuntimeLeaseState
  backendReachable: boolean
  message: string | null
}

export interface ResolvedCliConfig {
  settings: RuntimeSettings
  paths: HeadlessConfigPaths
}

export interface ParsedCliArgs {
  command: string
  flags: Record<string, string | boolean>
}
