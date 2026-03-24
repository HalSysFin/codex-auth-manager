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

export async function writeAuthFile(authFilePath: string, payload: AuthPayload): Promise<{ path: string; writtenAt: string }> {
  const fullPath = expandHomePath(authFilePath)
  const dir = path.dirname(fullPath)
  const writtenAt = new Date().toISOString()
  const finalPayload = prepareAuthPayloadForWrite(payload, writtenAt)
  const tempPath = `${fullPath}.tmp-${process.pid}-${Date.now()}`
  await fs.mkdir(dir, { recursive: true })
  const fileHandle = await fs.open(tempPath, 'w')
  try {
    await fileHandle.writeFile(`${JSON.stringify(finalPayload, null, 2)}\n`, 'utf8')
    await fileHandle.sync()
  } finally {
    await fileHandle.close()
  }
  await fs.rename(tempPath, fullPath)
  return { path: fullPath, writtenAt }
}
