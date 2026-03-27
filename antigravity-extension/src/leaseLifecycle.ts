export {
  deriveLeaseHealthState,
  needsReacquire,
  secondsUntilExpiry,
  selectStartupAction,
  shouldReacquireAfterLookupError,
  shouldRenewLease,
  shouldRotateLease,
} from '../../packages/lease-runtime/src/leaseLifecycle.js'

export type { LeaseAction, LeaseHealthState } from '../../packages/lease-runtime/src/types.js'
