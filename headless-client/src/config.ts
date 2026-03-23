import * as os from 'node:os'
import * as path from 'node:path'
import * as fs from 'node:fs/promises'
import {
  defaultRuntimeLeaseState,
  defaultRuntimeSettings,
  deriveAgentId,
  deriveMachineId,
} from '../../packages/lease-runtime/src/runtimeState.js'
import type { RuntimeSettings } from '../../packages/lease-runtime/src/types.js'
import type { HeadlessConfigPaths, HeadlessStateDocument, ParsedCliArgs, ResolvedCliConfig } from './types.js'

function boolFromValue(value: string | boolean | undefined, fallback: boolean): boolean {
  if (typeof value === 'boolean') {
    return value
  }
  if (typeof value !== 'string' || !value.trim()) {
    return fallback
  }
  return ['1', 'true', 'yes', 'on'].includes(value.trim().toLowerCase())
}

function numberFromValue(value: string | boolean | undefined, fallback: number): number {
  if (typeof value !== 'string') {
    return fallback
  }
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : fallback
}

function stringFromValue(value: string | boolean | undefined, fallback: string): string {
  return typeof value === 'string' && value.trim() ? value.trim() : fallback
}

export function xdgPaths(env = process.env, homeDir = os.homedir()): HeadlessConfigPaths {
  const configDir = env.XDG_CONFIG_HOME?.trim() || path.join(homeDir, '.config')
  const stateDir = env.XDG_STATE_HOME?.trim() || path.join(homeDir, '.local', 'state')
  const appConfigDir = path.join(configDir, 'auth-manager-agent')
  const appStateDir = path.join(stateDir, 'auth-manager-agent')
  return {
    configDir: appConfigDir,
    stateDir: appStateDir,
    configFile: path.join(appConfigDir, 'config.json'),
    stateFile: path.join(appStateDir, 'state.json'),
    logFile: path.join(appStateDir, 'agent.log'),
  }
}

async function readOptionalJson<T>(filePath: string): Promise<T | null> {
  try {
    const content = await fs.readFile(filePath, 'utf8')
    return JSON.parse(content) as T
  } catch {
    return null
  }
}

export async function loadHeadlessState(paths: HeadlessConfigPaths): Promise<HeadlessStateDocument | null> {
  return readOptionalJson<HeadlessStateDocument>(paths.stateFile)
}

export async function loadConfigFile(configFile: string): Promise<Partial<RuntimeSettings> | null> {
  return readOptionalJson<Partial<RuntimeSettings>>(configFile)
}

export function parseCliArgs(argv = process.argv.slice(2)): ParsedCliArgs {
  const [command = 'ensure', ...rest] = argv
  const flags: Record<string, string | boolean> = {}
  for (let index = 0; index < rest.length; index += 1) {
    const token = rest[index]
    if (!token.startsWith('--')) {
      continue
    }
    const normalized = token.slice(2)
    if (normalized.includes('=')) {
      const [key, value] = normalized.split(/=(.*)/s, 2)
      flags[key] = value
      continue
    }
    const next = rest[index + 1]
    if (!next || next.startsWith('--')) {
      flags[normalized] = true
      continue
    }
    flags[normalized] = next
    index += 1
  }
  return { command, flags }
}

export async function resolveCliConfig(parsed: ParsedCliArgs, env = process.env): Promise<ResolvedCliConfig> {
  const paths = xdgPaths(env)
  const state = await loadHeadlessState(paths)
  const configFile = stringFromValue(parsed.flags.config, stringFromValue(env.AUTH_MANAGER_CONFIG_FILE, paths.configFile))
  const configFileSettings = await loadConfigFile(configFile)
  const defaults = defaultRuntimeSettings()

  const baseSettings: RuntimeSettings = {
    baseUrl: stringFromValue(parsed.flags['base-url'], stringFromValue(env.AUTH_MANAGER_BASE_URL, configFileSettings?.baseUrl || defaults.baseUrl)),
    internalApiToken: stringFromValue(parsed.flags.token, stringFromValue(env.AUTH_MANAGER_INTERNAL_API_TOKEN, configFileSettings?.internalApiToken || defaults.internalApiToken)),
    machineId: stringFromValue(parsed.flags['machine-id'], stringFromValue(env.AUTH_MANAGER_MACHINE_ID, configFileSettings?.machineId || state?.settings.machineId || '')),
    agentId: stringFromValue(parsed.flags['agent-id'], stringFromValue(env.AUTH_MANAGER_AGENT_ID, configFileSettings?.agentId || state?.settings.agentId || '')),
    authFilePath: stringFromValue(parsed.flags['auth-file'], stringFromValue(env.AUTH_MANAGER_AUTH_FILE_PATH, configFileSettings?.authFilePath || state?.settings.authFilePath || defaults.authFilePath)),
    refreshIntervalSeconds: numberFromValue(parsed.flags['refresh-interval'], Number(env.AUTH_MANAGER_REFRESH_INTERVAL_SECONDS || configFileSettings?.refreshIntervalSeconds || state?.settings.refreshIntervalSeconds || defaults.refreshIntervalSeconds)),
    telemetryIntervalSeconds: numberFromValue(parsed.flags['telemetry-interval'], Number(env.AUTH_MANAGER_TELEMETRY_INTERVAL_SECONDS || configFileSettings?.telemetryIntervalSeconds || state?.settings.telemetryIntervalSeconds || defaults.telemetryIntervalSeconds)),
    autoRenew: boolFromValue(parsed.flags['auto-renew'], boolFromValue(env.AUTH_MANAGER_AUTO_RENEW, configFileSettings?.autoRenew ?? state?.settings.autoRenew ?? defaults.autoRenew)),
    autoRotate: boolFromValue(parsed.flags['auto-rotate'], boolFromValue(env.AUTH_MANAGER_AUTO_ROTATE, configFileSettings?.autoRotate ?? state?.settings.autoRotate ?? defaults.autoRotate)),
    openDashboardPath: stringFromValue(parsed.flags['dashboard-path'], stringFromValue(env.AUTH_MANAGER_OPEN_DASHBOARD_PATH, configFileSettings?.openDashboardPath || defaults.openDashboardPath)),
    allowInsecureLocalhost: boolFromValue(parsed.flags['allow-insecure-localhost'], boolFromValue(env.AUTH_MANAGER_ALLOW_INSECURE_LOCALHOST, configFileSettings?.allowInsecureLocalhost ?? defaults.allowInsecureLocalhost)),
  }

  const settings: RuntimeSettings = {
    ...baseSettings,
    machineId: deriveMachineId(baseSettings.machineId, 'linux'),
    agentId: deriveAgentId(baseSettings.agentId, 'headless-client'),
  }

  return { settings, paths: { ...paths, configFile } }
}

export function createStateDocument(existing: HeadlessStateDocument | null, settings: RuntimeSettings): HeadlessStateDocument {
  return {
    settings,
    lease: {
      ...defaultRuntimeLeaseState(settings.machineId, settings.agentId, settings.authFilePath),
      ...(existing?.lease || {}),
      machineId: settings.machineId,
      agentId: settings.agentId,
      authFilePath: settings.authFilePath,
    },
  }
}
