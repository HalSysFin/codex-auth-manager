import test from 'node:test'
import assert from 'node:assert/strict'
import * as fs from 'node:fs/promises'
import * as os from 'node:os'
import * as path from 'node:path'
import { authFileExists, readAuthFile, writeAuthFile } from '../fsStore.js'
import { expandHomePath, validateAuthPayload } from '../../../packages/lease-runtime/src/authPayload.js'

test('expandHomePath expands leading tilde for linux paths', () => {
  assert.equal(expandHomePath('~/.codex/auth.json', '/home/tester'), '/home/tester/.codex/auth.json')
})

test('validateAuthPayload accepts correct auth shape', () => {
  assert.equal(validateAuthPayload({
    auth_mode: 'chatgpt',
    OPENAI_API_KEY: null,
    tokens: {
      id_token: 'id',
      access_token: 'access',
      refresh_token: 'refresh',
      account_id: 'acct',
    },
  }), true)
})

test('writeAuthFile writes and reads atomically', async () => {
  const tempRoot = await fs.mkdtemp(path.join(os.tmpdir(), 'auth-manager-agent-'))
  const homeDir = tempRoot
  const target = '~/.codex/auth.json'
  const payload = {
    auth_mode: 'chatgpt',
    OPENAI_API_KEY: null,
    tokens: {
      id_token: 'id',
      access_token: 'access',
      refresh_token: 'refresh',
      account_id: 'acct',
    },
  }
  const result = await writeAuthFile(target, payload, homeDir)
  assert.equal(await authFileExists(target, homeDir), true)
  const parsed = await readAuthFile(target, homeDir)
  assert.equal(result.path, path.join(tempRoot, '.codex', 'auth.json'))
  assert.ok(parsed)
  assert.equal(parsed?.tokens.account_id, 'acct')
})
