export interface RequestFreshLeaseDeps {
  currentLeaseId: string | null
  releaseCurrentLease(): Promise<void>
  acquireFreshLease(): Promise<void>
}

export async function requestFreshLease(deps: RequestFreshLeaseDeps): Promise<void> {
  if (deps.currentLeaseId) {
    await deps.releaseCurrentLease()
  }
  await deps.acquireFreshLease()
}

