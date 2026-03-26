import * as fs from 'node:fs/promises'
import * as os from 'node:os'
import * as path from 'node:path'
import {
  expandHomePath as sharedExpandHomePath,
  prepareAuthPayloadForWrite,
  validateAuthPayload,
} from '../../packages/lease-runtime/src/authPayload.js'
import type { AuthPayload } from '../../packages/lease-runtime/src/types.js'

export { validateAuthPayload }

export function expandHomePath(rawPath: string): string {
  return path.resolve(sharedExpandHomePath(rawPath, os.homedir()))
}

export async function authFileExists(authFilePath: string): Promise<boolean> {
  try {
    await fs.access(expandHomePath(authFilePath))
    return true
  } catch {
    return false
  }
}

export async function readAuthFile(authFilePath: string): Promise<AuthPayload | null> {
  const fullPath = expandHomePath(authFilePath)
  try {
    const content = await fs.readFile(fullPath, 'utf8')
    const parsed = JSON.parse(content)
    return validateAuthPayload(parsed) ? parsed : null
  } catch {
    return null
  }
}

export async function writeAuthFile(authFilePath: string, payload: AuthPayload): Promise<{ path: string; writtenAt: string }> {
  if (!validateAuthPayload(payload)) {
    throw new Error('Invalid auth payload shape')
  }
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

export async function deleteAuthFile(authFilePath: string): Promise<boolean> {
  const fullPath = expandHomePath(authFilePath)
  try {
    await fs.unlink(fullPath)
    return true
  } catch (error: any) {
    if (error?.code === 'ENOENT') {
      return false
    }
    throw error
  }
}
