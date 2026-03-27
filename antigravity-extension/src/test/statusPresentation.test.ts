import test from 'node:test'
import assert from 'node:assert/strict'
import { extractAccountIdentity, formatStatusBarText, formatStatusBarTooltip } from '../statusPresentation'

test('status bar text uses account label when known', () => {
  assert.equal(
    formatStatusBarText(
      {
        accountLabel: 'max',
        accountName: 'Max',
        credentialId: 'cred-1',
        leaseId: 'lease-1',
      },
      'active',
    ),
    'Codex: max',
  )
})

test('status bar text falls back to credential id when account name is missing', () => {
  assert.equal(
    formatStatusBarText(
      {
        accountLabel: null,
        accountName: null,
        credentialId: 'cred-1',
        leaseId: 'lease-1',
      },
      'expiring',
    ),
    'Codex: cred-1 (Expiring)',
  )
})

test('status bar tooltip includes account, lease, credential, and usage details', () => {
  const tooltip = formatStatusBarTooltip(
    {
      accountLabel: 'max',
      accountName: null,
      leaseId: 'lease-1',
      credentialId: 'cred-1',
      leaseState: 'active',
      expiresAt: '2026-03-23T12:00:00.000Z',
      latestUtilizationPct: 42,
      latestQuotaRemaining: 1234,
    },
    'active',
  )
  assert.match(tooltip, /Account: max/)
  assert.match(tooltip, /Lease Id: lease-1/)
  assert.match(tooltip, /Credential Id: cred-1/)
  assert.match(tooltip, /Utilization: 42/)
  assert.match(tooltip, /Quota Remaining: 1234/)
})

test('extractAccountIdentity prefers materialize label and name fields', () => {
  const identity = extractAccountIdentity({
    status: 'ok',
    reason: null,
    lease: {
      id: 'lease-1',
      credential_id: 'cred-1',
      machine_id: 'machine-a',
      agent_id: 'vscode-extension',
      state: 'active',
      issued_at: '2026-03-23T10:00:00.000Z',
      expires_at: '2026-03-23T11:00:00.000Z',
      renewed_at: null,
      revoked_at: null,
      released_at: null,
      rotation_reason: null,
      replacement_lease_id: null,
      last_telemetry_at: null,
      latest_utilization_pct: null,
      latest_quota_remaining: null,
      last_success_at: null,
      last_error_at: null,
      reason: null,
      metadata: { label: 'fallback-label' },
      created_at: '2026-03-23T10:00:00.000Z',
      updated_at: '2026-03-23T10:00:00.000Z',
    },
    credential_material: {
      label: 'max',
      name: 'Max',
      auth_json: null,
    },
  })
  assert.equal(identity.accountLabel, 'max')
  assert.equal(identity.accountName, 'Max')
})

