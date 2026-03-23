import * as fs from 'node:fs/promises'
import * as os from 'node:os'
import * as path from 'node:path'
import { expandHomePath, prepareAuthPayloadForWrite, validateAuthPayload } from '../../packages/lease-runtime/src/authPayload.js'
import type { AuthPayload } from '../../packages/lease-runtime/src/types.js'
import type { HeadlessConfigPaths, HeadlessStateDocument } from './types.js'

async function writeJsonAtomic(filePath: string, value: unknown): Promise<void> {
  await fs.mkdir(path.dirname(filePath), { recursive: true })
  const tempFile = `${filePath}.tmp-${process.pid}-${Date.now()}`
  const handle = await fs.open(tempFile, 'w')
  try {
    await handle.writeFile(`${JSON.stringify(value, null, 2)}\n`, 'utf8')
    await handle.sync()
  } finally {
    await handle.close()
  }
  await fs.rename(tempFile, filePath)
}

export async function saveHeadlessState(paths: HeadlessConfigPaths, document: HeadlessStateDocument): Promise<void> {
  await writeJsonAtomic(paths.stateFile, document)
}

export async function ensureConfigDir(paths: HeadlessConfigPaths): Promise<void> {
  await fs.mkdir(paths.configDir, { recursive: true })
  await fs.mkdir(paths.stateDir, { recursive: true })
}

export async function writeDefaultConfig(paths: HeadlessConfigPaths, config: Partial<Record<string, unknown>>): Promise<void> {
  await writeJsonAtomic(paths.configFile, config)
}

export async function appendLog(paths: HeadlessConfigPaths, line: string): Promise<void> {
  await fs.mkdir(paths.stateDir, { recursive: true })
  await fs.appendFile(paths.logFile, `${line}\n`, 'utf8')
}

export async function readRecentLogs(paths: HeadlessConfigPaths, limit = 50): Promise<string[]> {
  try {
    const content = await fs.readFile(paths.logFile, 'utf8')
    const lines = content.trim().split('\n').filter(Boolean)
    return lines.slice(-limit)
  } catch {
    return []
  }
}

export async function authFileExists(authFilePath: string, homeDir = os.homedir()): Promise<boolean> {
  try {
    await fs.access(expandHomePath(authFilePath, homeDir))
    return true
  } catch {
    return false
  }
}

export async function readAuthFile(authFilePath: string, homeDir = os.homedir()): Promise<AuthPayload | null> {
  try {
    const fullPath = expandHomePath(authFilePath, homeDir)
    const content = await fs.readFile(fullPath, 'utf8')
    const parsed = JSON.parse(content)
    return validateAuthPayload(parsed) ? parsed : null
  } catch {
    return null
  }
}

export async function writeAuthFile(authFilePath: string, payload: AuthPayload, homeDir = os.homedir()): Promise<{ path: string; writtenAt: string }> {
  const prepared = prepareAuthPayloadForWrite(payload)
  const fullPath = expandHomePath(authFilePath, homeDir)
  await writeJsonAtomic(fullPath, prepared)
  return {
    path: fullPath,
    writtenAt: prepared.last_refresh || new Date().toISOString(),
  }
}
