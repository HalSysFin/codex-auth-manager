import type { HeadlessOutput } from './types.js'

export class ConsoleOutput implements HeadlessOutput {
  info(message: string): void {
    process.stdout.write(`${message}\n`)
  }

  warn(message: string): void {
    process.stderr.write(`WARN: ${message}\n`)
  }

  error(message: string): void {
    process.stderr.write(`ERROR: ${message}\n`)
  }
}

export function formatStatusLines(input: {
  backendReachable: boolean
  healthState: string
  leaseState: string | null
  leaseId: string | null
  credentialId: string | null
  expiresAt: string | null
  latestUtilizationPct: number | null
  latestQuotaRemaining: number | null
  lastBackendRefreshAt: string | null
  lastAuthWriteAt: string | null
}): string[] {
  return [
    `Health: ${input.healthState}`,
    `Backend reachable: ${input.backendReachable ? 'yes' : 'no'}`,
    `Lease state: ${input.leaseState || 'none'}`,
    `Lease id: ${input.leaseId || 'none'}`,
    `Credential id: ${input.credentialId || 'none'}`,
    `Expires at: ${input.expiresAt || 'n/a'}`,
    `Latest utilization %: ${input.latestUtilizationPct ?? 'n/a'}`,
    `Latest quota remaining: ${input.latestQuotaRemaining ?? 'n/a'}`,
    `Last backend refresh: ${input.lastBackendRefreshAt || 'n/a'}`,
    `Last auth write: ${input.lastAuthWriteAt || 'n/a'}`,
  ]
}
