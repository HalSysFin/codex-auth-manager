import * as fs from 'node:fs/promises'
import * as os from 'node:os'
import * as path from 'node:path'

import type { AuthPayload } from './types.js'

export function validateAuthPayload(payload: unknown): payload is AuthPayload {
  if (!payload || typeof payload !== 'object') {
    return false
  }
  const record = payload as Record<string, unknown>
  if (typeof record.auth_mode !== 'string') {
    return false
  }
  if (record.OPENAI_API_KEY !== null) {
    return false
  }
  const tokens = record.tokens
  if (!tokens || typeof tokens !== 'object') {
    return false
  }
  const tokenRecord = tokens as Record<string, unknown>
  return ['id_token', 'access_token', 'refresh_token', 'account_id'].every((key) => typeof tokenRecord[key] === 'string')
}

export function expandHomePath(rawPath: string): string {
  if (!rawPath.startsWith('~')) {
    return path.resolve(rawPath)
  }
  const homeDir = os.homedir().replace(/[\\/]+$/, '')
  const trimmedPath = rawPath.slice(1).replace(/^[/\\]+/, '')
  return path.resolve(`${homeDir}/${trimmedPath}`.replace(/\\/g, '/'))
}

export function prepareAuthPayloadForWrite(payload: AuthPayload, nowIso = new Date().toISOString()): AuthPayload {
  if (!validateAuthPayload(payload)) {
    throw new Error('Invalid auth payload shape')
  }
  return {
    ...payload,
    last_refresh: payload.last_refresh || nowIso,
  }
}

async function atomicWriteJson(fullPath: string, content: unknown): Promise<void> {
  const dir = path.dirname(fullPath)
  const tempPath = `${fullPath}.tmp-${process.pid}-${Date.now()}`
  await fs.mkdir(dir, { recursive: true })
  const fileHandle = await fs.open(tempPath, 'w')
  try {
    await fileHandle.writeFile(`${JSON.stringify(content, null, 2)}\n`, 'utf8')
    await fileHandle.sync()
  } finally {
    await fileHandle.close()
  }
  await fs.rename(tempPath, fullPath)
}

export async function writeAuthFile(authFilePath: string, payload: AuthPayload): Promise<{ path: string; writtenAt: string }> {
  const fullPath = expandHomePath(authFilePath)
  const writtenAt = new Date().toISOString()
  const finalPayload = prepareAuthPayloadForWrite(payload, writtenAt)
  await atomicWriteJson(fullPath, finalPayload)
  return { path: fullPath, writtenAt }
}

type OpenClawAuthProfile = {
  access: string
  accountId: string
  expires: number
  provider: 'openai-codex'
  refresh: string
  type: 'oauth'
}

type OpenClawProfilesFile = {
  version?: number
  profiles?: Record<string, OpenClawAuthProfile>
  usageStats?: Record<string, unknown>
}

type OpenClawConfigFile = {
  auth?: {
    profiles?: Record<string, { provider: string; mode: string }>
    order?: Record<string, string[]>
  }
}

export async function applyLeaseAuthToOpenClaw(options: {
  payload: AuthPayload
  leaseProfileId: string
  expiresAtIso: string
  enforceLeaseAsActiveAuth: boolean
  disallowNonLeaseAuth: boolean
}): Promise<void> {
  const authProfilesPath = expandHomePath('~/.openclaw/agents/main/agent/auth-profiles.json')
  const openclawConfigPath = expandHomePath('~/.openclaw/openclaw.json')

  const profileData: OpenClawProfilesFile = await readJsonOrDefault(authProfilesPath, { version: 1, profiles: {}, usageStats: {} })
  const configData: OpenClawConfigFile = await readJsonOrDefault(openclawConfigPath, {})

  const expiresMs = Number.isFinite(Date.parse(options.expiresAtIso)) ? Date.parse(options.expiresAtIso) : Date.now() + 30 * 60 * 1000

  const leaseProfile: OpenClawAuthProfile = {
    access: options.payload.tokens.access_token,
    refresh: options.payload.tokens.refresh_token,
    accountId: options.payload.tokens.account_id,
    expires: expiresMs,
    provider: 'openai-codex',
    type: 'oauth',
  }

  const profiles = profileData.profiles ?? {}
  profiles[options.leaseProfileId] = leaseProfile

  if (options.disallowNonLeaseAuth) {
    for (const key of Object.keys(profiles)) {
      if (key !== options.leaseProfileId && key.startsWith('openai-codex:')) {
        delete profiles[key]
      }
    }
    if (profileData.usageStats) {
      for (const key of Object.keys(profileData.usageStats)) {
        if (key !== options.leaseProfileId && key.startsWith('openai-codex:')) {
          delete (profileData.usageStats as Record<string, unknown>)[key]
        }
      }
    }
  }

  profileData.profiles = profiles

  const auth = configData.auth ?? {}
  const cfgProfiles = auth.profiles ?? {}
  cfgProfiles[options.leaseProfileId] = { provider: 'openai-codex', mode: 'oauth' }

  let order = (auth.order?.['openai-codex'] ?? []).filter((id) => id && id !== options.leaseProfileId)
  if (options.disallowNonLeaseAuth) {
    order = []
    for (const key of Object.keys(cfgProfiles)) {
      if (key !== options.leaseProfileId && key.startsWith('openai-codex:')) {
        delete cfgProfiles[key]
      }
    }
  }

  if (options.enforceLeaseAsActiveAuth) {
    order.unshift(options.leaseProfileId)
  } else {
    order.push(options.leaseProfileId)
  }

  auth.profiles = cfgProfiles
  auth.order = { ...(auth.order ?? {}), 'openai-codex': Array.from(new Set(order)) }
  configData.auth = auth

  await atomicWriteJson(authProfilesPath, profileData)
  await atomicWriteJson(openclawConfigPath, configData)
}

async function readJsonOrDefault<T>(fullPath: string, fallback: T): Promise<T> {
  try {
    const raw = await fs.readFile(fullPath, 'utf8')
    return JSON.parse(raw) as T
  } catch {
    return fallback
  }
}
