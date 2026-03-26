import test from 'node:test'
import assert from 'node:assert/strict'
import * as fs from 'node:fs/promises'
import * as path from 'node:path'
import { expandHomePath, validateAuthPayload } from '../authFile'
import { deleteAuthFile, writeAuthFile } from '../authFile'
import { prepareAuthPayloadForWrite } from '../../../packages/lease-runtime/src/authPayload.js'

test('expandHomePath expands leading tilde', () => {
  const expanded = expandHomePath('~/.codex/auth.json')
  assert.ok(expanded.endsWith('.codex/auth.json'))
  assert.ok(!expanded.startsWith('~/'))
})

test('validateAuthPayload accepts correct shape', () => {
  assert.equal(
    validateAuthPayload({
      auth_mode: 'chatgpt',
      OPENAI_API_KEY: null,
      tokens: {
        id_token: 'id',
        access_token: 'access',
        refresh_token: 'refresh',
        account_id: 'acct',
      },
      last_refresh: '2026-03-22T00:00:00.000Z',
    }),
    true,
  )
})

test('validateAuthPayload rejects incomplete shape', () => {
  assert.equal(
    validateAuthPayload({
      auth_mode: 'chatgpt',
      OPENAI_API_KEY: null,
      tokens: {
        access_token: 'access',
      },
    }),
    false,
  )
})

test('prepareAuthPayloadForWrite populates last_refresh through shared helper', () => {
  const prepared = prepareAuthPayloadForWrite({
    auth_mode: 'chatgpt',
    OPENAI_API_KEY: null,
    tokens: {
      id_token: 'id',
      access_token: 'access',
      refresh_token: 'refresh',
      account_id: 'acct',
    },
  }, '2026-03-22T00:00:00.000Z')
  assert.equal(prepared.last_refresh, '2026-03-22T00:00:00.000Z')
})

test('writeAuthFile creates parent directories and rewrites auth contents', async () => {
  const tempRoot = await fs.mkdtemp(path.join(process.cwd(), 'tmp-vscode-auth-file-'))
  const authPath = path.join(tempRoot, 'nested', 'auth.json')
  try {
    const first = await writeAuthFile(authPath, {
      auth_mode: 'chatgpt',
      OPENAI_API_KEY: null,
      tokens: {
        id_token: 'id-1',
        access_token: 'access-1',
        refresh_token: 'refresh-1',
        account_id: 'acct-1',
      },
    })
    const second = await writeAuthFile(authPath, {
      auth_mode: 'chatgpt',
      OPENAI_API_KEY: null,
      tokens: {
        id_token: 'id-2',
        access_token: 'access-2',
        refresh_token: 'refresh-2',
        account_id: 'acct-2',
      },
    })
    const content = JSON.parse(await fs.readFile(authPath, 'utf8'))
    assert.equal(first.path, authPath)
    assert.equal(second.path, authPath)
    assert.equal(content.tokens.account_id, 'acct-2')
  } finally {
    await fs.rm(tempRoot, { recursive: true, force: true })
  }
})

test('deleteAuthFile removes auth.json and is idempotent', async () => {
  const tempRoot = await fs.mkdtemp(path.join(process.cwd(), 'tmp-vscode-auth-delete-'))
  const authPath = path.join(tempRoot, 'nested', 'auth.json')
  try {
    await writeAuthFile(authPath, {
      auth_mode: 'chatgpt',
      OPENAI_API_KEY: null,
      tokens: {
        id_token: 'id-1',
        access_token: 'access-1',
        refresh_token: 'refresh-1',
        account_id: 'acct-1',
      },
    })
    assert.equal(await deleteAuthFile(authPath), true)
    assert.equal(await deleteAuthFile(authPath), false)
  } finally {
    await fs.rm(tempRoot, { recursive: true, force: true })
  }
})
