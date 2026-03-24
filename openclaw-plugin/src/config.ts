import type { AuthManagerLeasePluginConfig } from './types.js'

const DEFAULT_FLUSH_INTERVAL_MS = 60_000
const DEFAULT_FLUSH_EVERY_REQUESTS = 10
const DEFAULT_REFRESH_INTERVAL_MS = 60_000
const DEFAULT_REQUESTED_TTL_SECONDS = 1800
const DEFAULT_AUTO_RENEW = true
const DEFAULT_AUTO_ROTATE = false
const DEFAULT_RELEASE_LEASE_ON_SHUTDOWN = false
const DEFAULT_LEASE_PROFILE_ID = 'openai-codex:lease'

function asString(value: unknown): string | undefined {
  if (typeof value !== 'string') return undefined
  const trimmed = value.trim()
  return trimmed || undefined
}

function asNumber(value: unknown, fallback: number): number {
  if (typeof value === 'number' && Number.isFinite(value)) return value
  if (typeof value === 'string') {
    const parsed = Number(value.trim())
    if (Number.isFinite(parsed)) return parsed
  }
  return fallback
}

function asBoolean(value: unknown, fallback: boolean): boolean {
  if (typeof value === 'boolean') return value
  if (typeof value === 'string') {
    const normalized = value.trim().toLowerCase()
    if (normalized === 'true' || normalized === '1' || normalized === 'yes' || normalized === 'on') return true
    if (normalized === 'false' || normalized === '0' || normalized === 'no' || normalized === 'off') return false
  }
  return fallback
}

export function resolvePluginConfig(
  rawConfig: Record<string, unknown> | undefined,
  env: NodeJS.ProcessEnv = process.env,
): AuthManagerLeasePluginConfig {
  const baseUrl = asString(rawConfig?.baseUrl) ?? asString(env.AUTH_MANAGER_BASE_URL) ?? ''
  const internalApiToken =
    asString(rawConfig?.internalApiToken) ??
    asString(rawConfig?.apiKey) ??
    asString(env.AUTH_MANAGER_INTERNAL_API_TOKEN) ??
    asString(env.AUTH_MANAGER_API_KEY) ??
    ''
  const machineId = asString(rawConfig?.machineId) ?? asString(env.AUTH_MANAGER_MACHINE_ID) ?? ''
  const agentId = asString(rawConfig?.agentId) ?? asString(env.AUTH_MANAGER_AGENT_ID) ?? 'openclaw'
  const leaseId = asString(rawConfig?.leaseId) ?? asString(env.AUTH_MANAGER_LEASE_ID)
  const authFilePath = asString(rawConfig?.authFilePath) ?? asString(env.AUTH_MANAGER_AUTH_FILE_PATH) ?? '~/.codex/auth.json'
  const leaseProfileId =
    asString(rawConfig?.leaseProfileId) ?? asString(env.AUTH_MANAGER_LEASE_PROFILE_ID) ?? DEFAULT_LEASE_PROFILE_ID
  const enforceLeaseAsActiveAuth = asBoolean(
    rawConfig?.enforceLeaseAsActiveAuth ?? env.AUTH_MANAGER_ENFORCE_LEASE_ACTIVE_AUTH,
    true,
  )
  const disallowNonLeaseAuth = asBoolean(rawConfig?.disallowNonLeaseAuth ?? env.AUTH_MANAGER_DISALLOW_NON_LEASE_AUTH, false)
  const purgeNonLeaseProfilesOnStart = asBoolean(
    rawConfig?.purgeNonLeaseProfilesOnStart ?? env.AUTH_MANAGER_PURGE_NON_LEASE_PROFILES_ON_START,
    false,
  )
  const flushIntervalMs = Math.max(
    1_000,
    asNumber(rawConfig?.flushIntervalMs ?? env.AUTH_MANAGER_TELEMETRY_INTERVAL_MS, DEFAULT_FLUSH_INTERVAL_MS),
  )
  const flushEveryRequests = Math.max(
    1,
    Math.trunc(
      asNumber(rawConfig?.flushEveryRequests ?? env.AUTH_MANAGER_TELEMETRY_FLUSH_EVERY, DEFAULT_FLUSH_EVERY_REQUESTS),
    ),
  )
  const refreshIntervalMs = Math.max(
    15_000,
    asNumber(rawConfig?.refreshIntervalMs ?? env.AUTH_MANAGER_REFRESH_INTERVAL_MS, DEFAULT_REFRESH_INTERVAL_MS),
  )
  const requestedTtlSeconds = Math.max(
    60,
    Math.trunc(asNumber(rawConfig?.requestedTtlSeconds ?? env.AUTH_MANAGER_REQUESTED_TTL_SECONDS, DEFAULT_REQUESTED_TTL_SECONDS)),
  )
  const autoRenew = asBoolean(rawConfig?.autoRenew ?? env.AUTH_MANAGER_AUTO_RENEW, DEFAULT_AUTO_RENEW)
  const autoRotate = asBoolean(rawConfig?.autoRotate ?? env.AUTH_MANAGER_AUTO_ROTATE, DEFAULT_AUTO_ROTATE)
  const rotationPolicyRaw =
    asString(rawConfig?.rotationPolicy) ??
    asString(env.AUTH_MANAGER_ROTATION_POLICY) ??
    'replacement_required_only'
  const rotationPolicy =
    rotationPolicyRaw === 'recommended_or_required' ? 'recommended_or_required' : 'replacement_required_only'
  const allowInsecureLocalhost = asBoolean(
    rawConfig?.allowInsecureLocalhost ?? env.AUTH_MANAGER_ALLOW_INSECURE_LOCALHOST,
    true,
  )
  const releaseLeaseOnShutdown = asBoolean(
    rawConfig?.releaseLeaseOnShutdown ?? env.AUTH_MANAGER_RELEASE_LEASE_ON_SHUTDOWN,
    DEFAULT_RELEASE_LEASE_ON_SHUTDOWN,
  )
  const enabled = asBoolean(rawConfig?.enabled ?? env.AUTH_MANAGER_TELEMETRY_ENABLED, true)

  return {
    baseUrl,
    internalApiToken,
    machineId,
    agentId,
    leaseId,
    authFilePath,
    leaseProfileId,
    enforceLeaseAsActiveAuth,
    disallowNonLeaseAuth,
    purgeNonLeaseProfilesOnStart,
    flushIntervalMs,
    flushEveryRequests,
    refreshIntervalMs,
    requestedTtlSeconds,
    autoRenew,
    autoRotate,
    rotationPolicy,
    allowInsecureLocalhost,
    releaseLeaseOnShutdown,
    enabled,
  }
}

export function validatePluginConfig(config: AuthManagerLeasePluginConfig): string[] {
  const errors: string[] = []
  if (!config.baseUrl) errors.push('baseUrl is required')
  if (!config.internalApiToken) errors.push('internalApiToken is required')
  if (!config.machineId) errors.push('machineId is required')
  if (!config.agentId) errors.push('agentId is required')
  if (!config.leaseProfileId) errors.push('leaseProfileId is required')
  if (config.flushIntervalMs < 1_000) errors.push('flushIntervalMs must be at least 1000')
  if (config.flushEveryRequests < 1) errors.push('flushEveryRequests must be at least 1')
  return errors
}
