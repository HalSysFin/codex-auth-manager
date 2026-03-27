import test from 'node:test'
import assert from 'node:assert/strict'
import { requestFreshLease } from '../requestFreshLease'

test('manual request-new-lease releases current lease before acquiring a new one', async () => {
  const calls: string[] = []
  await requestFreshLease({
    currentLeaseId: 'lease-old',
    releaseCurrentLease: async () => {
      calls.push('release')
    },
    acquireFreshLease: async () => {
      calls.push('acquire')
    },
  })
  assert.deepEqual(calls, ['release', 'acquire'])
})

test('manual request-new-lease acquires directly when no lease is active', async () => {
  const calls: string[] = []
  await requestFreshLease({
    currentLeaseId: null,
    releaseCurrentLease: async () => {
      calls.push('release')
    },
    acquireFreshLease: async () => {
      calls.push('acquire')
    },
  })
  assert.deepEqual(calls, ['acquire'])
})

