import {
  AuthManagerClient as SharedAuthManagerClient,
  AuthManagerClientError,
  normalizeBackendErrorPayload,
} from '../../packages/lease-runtime/src/authManagerClient.js'
import type {
  AuthPayload,
  Lease,
  LeaseAcquireResponse,
  LeaseRotateResponse,
  LeaseStatusResponse,
  LeaseTelemetryRequest,
  MaterializeLeaseResponse,
} from '../../packages/lease-runtime/src/types.js'

export type {
  AuthPayload,
  Lease,
  LeaseAcquireResponse,
  LeaseRotateResponse,
  LeaseStatusResponse,
  LeaseTelemetryRequest,
  MaterializeLeaseResponse,
}

export { AuthManagerClientError, normalizeBackendErrorPayload }

export type AuthManagerClientOptions = ConstructorParameters<typeof SharedAuthManagerClient>[0]

export class AuthManagerClient extends SharedAuthManagerClient {
  async fetchAuthPayloadForLease(
    leaseId: string,
    input: { machineId: string; agentId: string },
  ): Promise<MaterializeLeaseResponse> {
    return this.materializeLease(leaseId, input)
  }
}
