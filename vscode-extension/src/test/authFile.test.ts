import test from 'node:test'
import assert from 'node:assert/strict'
import { expandHomePath, validateAuthPayload } from '../authFile'
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
