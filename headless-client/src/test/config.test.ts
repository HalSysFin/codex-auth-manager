import test from 'node:test'
import assert from 'node:assert/strict'
import * as fs from 'node:fs/promises'
import * as os from 'node:os'
import * as path from 'node:path'
import { createStateDocument, parseCliArgs, xdgPaths } from '../config.js'
import { saveHeadlessState } from '../fsStore.js'
import { defaultRuntimeSettings } from '../../../packages/lease-runtime/src/runtimeState.js'

test('parseCliArgs reads command and key-value flags', () => {
  const parsed = parseCliArgs(['ensure', '--base-url', 'http://127.0.0.1:8080', '--auto-renew=false'])
  assert.equal(parsed.command, 'ensure')
  assert.equal(parsed.flags['base-url'], 'http://127.0.0.1:8080')
  assert.equal(parsed.flags['auto-renew'], 'false')
})

test('xdgPaths respects XDG env variables', () => {
  const paths = xdgPaths({
    XDG_CONFIG_HOME: '/tmp/config-home',
    XDG_STATE_HOME: '/tmp/state-home',
  } as NodeJS.ProcessEnv, '/home/tester')
  assert.equal(paths.configFile, '/tmp/config-home/auth-manager-agent/config.json')
  assert.equal(paths.stateFile, '/tmp/state-home/auth-manager-agent/state.json')
})

test('createStateDocument preserves existing lease and updates ids', () => {
  const settings = {
    ...defaultRuntimeSettings(),
    machineId: 'machine-a',
    agentId: 'agent-a',
  }
  const document = createStateDocument({
    settings,
    lease: {
      machineId: 'old-machine',
      agentId: 'old-agent',
      leaseId: 'lease-1',
      credentialId: 'cred-1',
      issuedAt: null,
      expiresAt: null,
      leaseState: 'active',
      latestTelemetryAt: null,
      latestUtilizationPct: 10,
      latestQuotaRemaining: 900,
      lastAuthWriteAt: null,
      lastBackendRefreshAt: null,
      replacementRequired: false,
      rotationRecommended: false,
      lastErrorAt: null,
      authFilePath: settings.authFilePath,
    },
  }, settings)
  assert.equal(document.lease.machineId, 'machine-a')
  assert.equal(document.lease.leaseId, 'lease-1')
})

test('saveHeadlessState persists state document to disk', async () => {
  const tempRoot = await fs.mkdtemp(path.join(os.tmpdir(), 'headless-state-'))
  const paths = xdgPaths({
    XDG_CONFIG_HOME: path.join(tempRoot, 'config'),
    XDG_STATE_HOME: path.join(tempRoot, 'state'),
  } as NodeJS.ProcessEnv, tempRoot)
  const settings = {
    ...defaultRuntimeSettings(),
    machineId: 'machine-a',
    agentId: 'agent-a',
  }
  const document = createStateDocument(null, settings)
  document.lease.leaseId = 'lease-1'
  await saveHeadlessState(paths, document)
  const content = JSON.parse(await fs.readFile(paths.stateFile, 'utf8')) as { lease: { leaseId: string } }
  assert.equal(content.lease.leaseId, 'lease-1')
})
