#!/usr/bin/env node
import { createStateDocument, loadHeadlessState, parseCliArgs, resolveCliConfig } from './config.js'
import { ensureConfigDir } from './fsStore.js'
import { HeadlessAgent } from './agent.js'
import { ConsoleOutput, formatStatusLines } from './output.js'

async function main(): Promise<void> {
  const parsed = parseCliArgs()
  const resolved = await resolveCliConfig(parsed)
  await ensureConfigDir(resolved.paths)
  const existing = await loadHeadlessState(resolved.paths)
  const document = createStateDocument(existing, resolved.settings)
  const output = new ConsoleOutput()
  const agent = new HeadlessAgent(resolved, document, output)

  switch (parsed.command) {
    case 'ensure': {
      const status = await agent.ensure()
      for (const line of formatStatusLines({
        backendReachable: status.backendReachable,
        healthState: status.healthState,
        leaseState: status.lease.leaseState,
        leaseId: status.lease.leaseId,
        credentialId: status.lease.credentialId,
        expiresAt: status.lease.expiresAt,
        latestUtilizationPct: status.lease.latestUtilizationPct,
        latestQuotaRemaining: status.lease.latestQuotaRemaining,
        lastBackendRefreshAt: status.lease.lastBackendRefreshAt,
        lastAuthWriteAt: status.lease.lastAuthWriteAt,
      })) {
        output.info(line)
      }
      break
    }
    case 'status': {
      const status = await agent.refresh()
      for (const line of formatStatusLines({
        backendReachable: status.backendReachable,
        healthState: status.healthState,
        leaseState: status.lease.leaseState,
        leaseId: status.lease.leaseId,
        credentialId: status.lease.credentialId,
        expiresAt: status.lease.expiresAt,
        latestUtilizationPct: status.lease.latestUtilizationPct,
        latestQuotaRemaining: status.lease.latestQuotaRemaining,
        lastBackendRefreshAt: status.lease.lastBackendRefreshAt,
        lastAuthWriteAt: status.lease.lastAuthWriteAt,
      })) {
        output.info(line)
      }
      break
    }
    case 'renew':
      await agent.renew()
      break
    case 'rotate':
      await agent.rotate()
      break
    case 'release':
      await agent.release()
      break
    case 'run':
      await agent.run()
      break
    case 'doctor': {
      const result = await agent.doctor()
      for (const line of result.checks) {
        output.info(line)
      }
      process.exitCode = result.ok ? 0 : 1
      break
    }
    default:
      output.error(`Unknown command: ${parsed.command}`)
      process.exitCode = 1
  }
}

void main().catch((error) => {
  process.stderr.write(`ERROR: ${error instanceof Error ? error.message : String(error)}\n`)
  process.exit(1)
})
