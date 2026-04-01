import type { AuthPayload } from './types.js'

async function digestSha256Hex(input: string): Promise<string> {
  const bytes = new TextEncoder().encode(input)
  const digest = await crypto.subtle.digest('SHA-256', bytes)
  return Array.from(new Uint8Array(digest))
    .map((value) => value.toString(16).padStart(2, '0'))
    .join('')
}

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

export function expandHomePath(rawPath: string, homeDir: string): string {
  if (!rawPath.startsWith('~')) {
    return rawPath
  }
  const trimmedHome = homeDir.replace(/[\\/]+$/, '')
  const trimmedPath = rawPath.slice(1).replace(/^[/\\]+/, '')
  return `${trimmedHome}/${trimmedPath}`.replace(/\\/g, '/')
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

export async function authPayloadFingerprint(payload: AuthPayload): Promise<string> {
  const prepared = prepareAuthPayloadForWrite(payload, payload.last_refresh || new Date(0).toISOString())
  return digestSha256Hex(JSON.stringify(prepared))
}
