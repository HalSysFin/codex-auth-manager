import type { LeaseTelemetryContext, TelemetryPostBody } from './types.js'

export type ClientOptions = {
  baseUrl: string
  apiKey: string
  fetchImpl?: typeof fetch
}

export class AuthManagerTelemetryClient {
  private readonly baseUrl: string
  private readonly apiKey: string
  private readonly fetchImpl: typeof fetch

  constructor(options: ClientOptions) {
    this.baseUrl = options.baseUrl.replace(/\/+$/, '')
    this.apiKey = options.apiKey
    this.fetchImpl = options.fetchImpl ?? fetch
  }

  async postLeaseTelemetry(context: LeaseTelemetryContext, payload: TelemetryPostBody): Promise<unknown> {
    const response = await this.fetchImpl(`${this.baseUrl}/api/leases/${context.leaseId}/telemetry`, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${this.apiKey}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(payload),
    })
    const raw = await response.text()
    const data = raw ? JSON.parse(raw) : null
    if (!response.ok) {
      throw new Error(
        typeof data?.detail === 'string'
          ? data.detail
          : typeof data?.reason === 'string'
            ? data.reason
            : `Telemetry post failed with HTTP ${response.status}`,
      )
    }
    return data
  }
}
