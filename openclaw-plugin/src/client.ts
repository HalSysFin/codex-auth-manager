import type {
  LeaseAcquireResponse,
  LeaseRotateResponse,
  LeaseStatusResponse,
  LeaseTelemetryContext,
  MaterializeLeaseResponse,
  TelemetryPostBody,
} from './types.js'

export type ClientOptions = {
  baseUrl: string
  internalApiToken: string
  allowInsecureLocalhost?: boolean
  fetchImpl?: typeof fetch
}

export class AuthManagerClientError extends Error {
  readonly status: number
  readonly code?: string

  constructor(message: string, status: number, code?: string) {
    super(message)
    this.name = 'AuthManagerClientError'
    this.status = status
    this.code = code
  }
}

function normalizeErrorPayload(payload: unknown): { message: string; code?: string } {
  if (!payload || typeof payload !== 'object') {
    return { message: 'Unknown backend error' }
  }
  const record = payload as Record<string, unknown>
  if (typeof record.detail === 'string' && record.detail.trim()) {
    return { message: record.detail }
  }
  if (typeof record.reason === 'string' && record.reason.trim()) {
    return { message: record.reason, code: record.reason }
  }
  if (typeof record.message === 'string' && record.message.trim()) {
    return { message: record.message }
  }
  return { message: 'Unknown backend error' }
}

function sanitizeBaseUrl(raw: string, allowInsecureLocalhost: boolean): string {
  const parsed = new URL(raw)
  const localHosts = new Set(['127.0.0.1', 'localhost', '::1'])
  const isLocalhost = localHosts.has(parsed.hostname)
  if (parsed.protocol !== 'https:' && !(allowInsecureLocalhost && isLocalhost)) {
    throw new Error(`Refusing insecure Auth Manager URL: ${parsed.toString()}`)
  }
  return parsed.toString().replace(/\/+$/, '')
}

export class AuthManagerTelemetryClient {
  private readonly baseUrl: string
  private readonly internalApiToken: string
  private readonly fetchImpl: typeof fetch

  constructor(options: ClientOptions) {
    this.baseUrl = sanitizeBaseUrl(options.baseUrl, Boolean(options.allowInsecureLocalhost))
    this.internalApiToken = options.internalApiToken.trim()
    this.fetchImpl = options.fetchImpl ?? fetch
  }

  async acquireLease(input: {
    machineId: string
    agentId: string
    requestedTtlSeconds?: number
    reason?: string
  }): Promise<LeaseAcquireResponse> {
    return this.request<LeaseAcquireResponse>('/api/leases/acquire', {
      method: 'POST',
      body: {
        machine_id: input.machineId,
        agent_id: input.agentId,
        requested_ttl_seconds: input.requestedTtlSeconds,
        reason: input.reason,
      },
    })
  }

  async getLease(leaseId: string): Promise<LeaseStatusResponse> {
    return this.request<LeaseStatusResponse>(`/api/leases/${encodeURIComponent(leaseId)}`, {
      method: 'GET',
    })
  }

  async renewLease(leaseId: string, input: { machineId: string; agentId: string }): Promise<LeaseAcquireResponse> {
    return this.request<LeaseAcquireResponse>(`/api/leases/${encodeURIComponent(leaseId)}/renew`, {
      method: 'POST',
      body: {
        machine_id: input.machineId,
        agent_id: input.agentId,
      },
    })
  }

  async releaseLease(leaseId: string, input: { machineId: string; agentId: string; reason?: string }): Promise<LeaseAcquireResponse> {
    return this.request<LeaseAcquireResponse>(`/api/leases/${encodeURIComponent(leaseId)}/release`, {
      method: 'POST',
      body: {
        machine_id: input.machineId,
        agent_id: input.agentId,
        reason: input.reason,
      },
    })
  }

  async rotateLease(input: {
    leaseId: string
    machineId: string
    agentId: string
    reason: string
  }): Promise<LeaseRotateResponse> {
    return this.request<LeaseRotateResponse>('/api/leases/rotate', {
      method: 'POST',
      body: {
        lease_id: input.leaseId,
        machine_id: input.machineId,
        agent_id: input.agentId,
        reason: input.reason,
      },
    })
  }

  async materializeLease(leaseId: string, input: { machineId: string; agentId: string }): Promise<MaterializeLeaseResponse> {
    return this.request<MaterializeLeaseResponse>(`/api/leases/${encodeURIComponent(leaseId)}/materialize`, {
      method: 'POST',
      body: {
        machine_id: input.machineId,
        agent_id: input.agentId,
      },
    })
  }

  async postLeaseTelemetry(context: LeaseTelemetryContext, payload: TelemetryPostBody): Promise<LeaseAcquireResponse> {
    return this.request<LeaseAcquireResponse>(`/api/leases/${context.leaseId}/telemetry`, {
      method: 'POST',
      body: payload,
    })
  }

  private async request<T>(path: string, options: { method: string; body?: unknown }): Promise<T> {
    const headers: Record<string, string> = {
      Accept: 'application/json',
      Authorization: `Bearer ${this.internalApiToken}`,
    }
    let body: string | undefined
    if (options.body !== undefined) {
      headers['Content-Type'] = 'application/json'
      body = JSON.stringify(options.body)
    }
    const response = await this.fetchImpl(`${this.baseUrl}${path}`, {
      method: options.method,
      headers,
      body,
    })
    const raw = await response.text()
    let data: unknown = null
    if (raw.trim()) {
      try {
        data = JSON.parse(raw)
      } catch {
        data = raw
      }
    }
    if (!response.ok) {
      const normalized = normalizeErrorPayload(data)
      throw new AuthManagerClientError(normalized.message, response.status, normalized.code)
    }
    return data as T
  }
}

