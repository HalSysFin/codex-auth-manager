import React, { useEffect, useMemo, useRef, useState } from 'react'
import { createRoot } from 'react-dom/client'
import './styles.css'

type RefreshStatus = {
  state?: 'idle' | 'ok' | 'failed' | 'refreshing'
  last_attempt_at?: string | null
  last_success_at?: string | null
  last_error?: string | null
  is_stale?: boolean
}

type Limit = {
  limit?: number
  used?: number
  remaining?: number
  percent?: number
  usedPercent?: number
  resetsAt?: string | number | null
  resetAt?: string | number | null
  nextResetAt?: string | number | null
  reset?: string | number | null
  windowDurationMins?: number
}

type UsageTracking = {
  usage_limit?: number
  usage_in_window?: number
  lifetime_used?: number
  rate_limit_refresh_at?: string | null
  last_usage_sync_at?: string | null
  updated_at?: string | null
}

type Account = {
  label: string
  account_key: string
  display_label: string | null
  email: string | null
  is_current: boolean
  rate_limits?: {
    primary?: Limit | null
    secondary?: Limit | null
    requests?: Limit | null
    tokens?: Limit | null
    error?: string
  }
  usage_tracking?: UsageTracking | null
  refresh_status?: RefreshStatus
}

type Aggregate = {
  accounts: number
  total_current_window_used: number
  total_current_window_limit: number
  total_remaining: number
  aggregate_utilization_percent: number
  lifetime_total_used: number
  total_wasted: number
  stale_accounts: number
  failed_accounts: number
  last_refresh_time: string | null
}

type AccountsCachedResponse = {
  accounts: Account[]
  current_label: string | null
  aggregate: Aggregate
}

type StreamSnapshot = {
  accounts: Account[]
  current_label: string | null
  aggregate: Aggregate
  pending_labels: string[]
}

type ViewMode = 'manager' | 'stats'
type AccountHistoryResponse = {
  label: string
  account_key: string
  display_label: string | null
  email: string | null
  usage_tracking?: UsageTracking | null
  rollovers: Array<{
    window_started_at?: string
    window_ended_at?: string
    usage_limit?: number
    usage_used?: number
    usage_wasted?: number
  }>
  summary?: {
    window_count?: number
    total_wasted?: number
    total_used_completed?: number
    total_limit_completed?: number
    avg_completed_utilization_percent?: number | null
    current_wasted_if_rollover_now?: number
  }
}

const defaultAggregate: Aggregate = {
  accounts: 0,
  total_current_window_used: 0,
  total_current_window_limit: 0,
  total_remaining: 0,
  aggregate_utilization_percent: 0,
  lifetime_total_used: 0,
  total_wasted: 0,
  stale_accounts: 0,
  failed_accounts: 0,
  last_refresh_time: null,
}

function authHeaders(token: string): Record<string, string> {
  return token ? { Authorization: `Bearer ${token}` } : {}
}

async function requestJson<T>(path: string, token: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, {
    ...init,
    headers: {
      'Content-Type': 'application/json',
      ...authHeaders(token),
      ...(init?.headers || {}),
    },
  })
  if (!res.ok) {
    const raw = await res.text()
    try {
      const d = JSON.parse(raw)
      if (typeof d.detail === 'string') throw new Error(d.detail)
      if (d.detail?.message) throw new Error(d.detail.message)
    } catch {
      throw new Error(raw || `HTTP ${res.status}`)
    }
    throw new Error(raw || `HTTP ${res.status}`)
  }
  return (await res.json()) as T
}

function fmtTs(value: string | number | null | undefined): string {
  if (!value) return '--'
  let d: Date
  if (typeof value === 'number' && Number.isFinite(value)) {
    // upstream often sends epoch seconds
    d = new Date(value * 1000)
  } else if (typeof value === 'string' && /^\\d+$/.test(value.trim())) {
    d = new Date(Number(value.trim()) * 1000)
  } else {
    d = new Date(value)
  }
  if (Number.isNaN(d.getTime())) return String(value)
  return d.toLocaleString('en-US', {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  })
}

function limitPercent(limit?: Limit | null): number | null {
  if (!limit) return null
  if (typeof limit.percent === 'number' && Number.isFinite(limit.percent)) return limit.percent
  if (typeof limit.usedPercent === 'number' && Number.isFinite(limit.usedPercent)) return limit.usedPercent
  return null
}

function averagePrimaryPercent(accounts: Account[]): number {
  const vals = accounts
    .map((a) => limitPercent(a.rate_limits?.requests || a.rate_limits?.primary))
    .filter((v): v is number => typeof v === 'number' && Number.isFinite(v))
  if (!vals.length) return 0
  return Math.round(vals.reduce((a, b) => a + b, 0) / vals.length)
}

function normalizeAccount(account: Account, currentLabel: string | null): Account {
  return {
    ...account,
    is_current: account.label === currentLabel,
    refresh_status: account.refresh_status ?? { state: 'idle', is_stale: true },
  }
}

function pctClass(p: number): string {
  if (p > 85) return 'danger'
  if (p > 60) return 'warn'
  return 'ok'
}

function resetDate(limit?: Limit | null, usage?: UsageTracking | null): Date | null {
  const raw = limit?.resetsAt ?? limit?.resetAt ?? limit?.nextResetAt ?? limit?.reset ?? usage?.rate_limit_refresh_at
  if (!raw) return null
  const date = typeof raw === 'number' ? new Date(raw * 1000) : new Date(String(raw))
  return Number.isNaN(date.getTime()) ? null : date
}

function refreshBadge(limitA?: Limit | null, limitB?: Limit | null, usage?: UsageTracking | null): { text: string; style: React.CSSProperties } | null {
  const a = resetDate(limitA, usage)
  const b = resetDate(limitB, usage)
  const stamps = [a, b].filter(Boolean).map((d) => (d as Date).getTime())
  if (!stamps.length) return null
  const ms = Math.min(...stamps) - Date.now()
  const minMs = 60 * 1000
  const maxMs = 7 * 24 * 60 * 60 * 1000
  const clamped = Math.max(minMs, Math.min(maxMs, ms))
  const ratio = (clamped - minMs) / (maxMs - minMs)
  const hue = Math.round(ratio * 120)

  let text = 'due'
  if (ms > minMs) {
    const mins = Math.floor(ms / 60000)
    const days = Math.floor(mins / (24 * 60))
    const hrs = Math.floor((mins % (24 * 60)) / 60)
    const rem = mins % 60
    if (days > 0) text = `${days}d ${hrs}h`
    else if (hrs > 0) text = `${hrs}h ${rem}m`
    else text = `${rem}m`
  }

  return {
    text,
    style: {
      color: `hsl(${hue}, 82%, 55%)`,
      borderColor: `hsla(${hue}, 82%, 55%, .35)`,
      background: `hsla(${hue}, 82%, 55%, .12)`,
    },
  }
}

function App() {
  const [draftToken, setDraftToken] = useState(localStorage.getItem('auth_manager_api_key') ?? '')
  const [apiKey, setApiKey] = useState(localStorage.getItem('auth_manager_api_key') ?? '')
  const [mode, setMode] = useState<ViewMode>('manager')
  const [accounts, setAccounts] = useState<Account[]>([])
  const [currentLabel, setCurrentLabel] = useState<string | null>(null)
  const [aggregate, setAggregate] = useState<Aggregate>(defaultAggregate)
  const [status, setStatus] = useState('Ready')
  const [err, setErr] = useState<string | null>(null)
  const [refreshing, setRefreshing] = useState(false)
  const [history, setHistory] = useState<Array<{ t: number; value: number }>>([])
  const [openMenuFor, setOpenMenuFor] = useState<string | null>(null)
  const [historyModalOpen, setHistoryModalOpen] = useState(false)
  const [historyLoading, setHistoryLoading] = useState(false)
  const [historyError, setHistoryError] = useState<string | null>(null)
  const [historyData, setHistoryData] = useState<AccountHistoryResponse | null>(null)
  const streamRef = useRef<EventSource | null>(null)
  const currentLabelRef = useRef<string | null>(currentLabel)

  const accountCount = accounts.length
  const profilesWithToken = accountCount
  const valid5hr = accounts
    .map((a) => limitPercent(a.rate_limits?.requests || a.rate_limits?.primary))
    .filter((v): v is number => typeof v === 'number' && Number.isFinite(v))
  const valid7d = accounts
    .map((a) => limitPercent(a.rate_limits?.tokens || a.rate_limits?.secondary))
    .filter((v): v is number => typeof v === 'number' && Number.isFinite(v))
  const avg5hr = valid5hr.length ? Math.round(valid5hr.reduce((a, b) => a + b, 0) / valid5hr.length) : null
  const avg7d = valid7d.length ? Math.round(valid7d.reduce((a, b) => a + b, 0) / valid7d.length) : null
  const displayUtilization = aggregate.aggregate_utilization_percent > 0
    ? aggregate.aggregate_utilization_percent
    : averagePrimaryPercent(accounts)
  const recommended = accounts.reduce<Account | null>((best, a) => {
    const p1 = Number((a.rate_limits?.requests || a.rate_limits?.primary)?.percent || 100)
    const p2 = Number((a.rate_limits?.tokens || a.rate_limits?.secondary)?.percent || 100)
    const score = p1 + p2
    if (!best) return a
    const bp1 = Number((best.rate_limits?.requests || best.rate_limits?.primary)?.percent || 100)
    const bp2 = Number((best.rate_limits?.tokens || best.rate_limits?.secondary)?.percent || 100)
    return score < bp1 + bp2 ? a : best
  }, null)

  const graphPoints = useMemo(() => {
    if (!history.length) return ''
    const maxY = Math.max(100, ...history.map((p) => p.value))
    const width = 760
    const height = 180
    return history
      .map((p, i) => {
        const x = (i / Math.max(history.length - 1, 1)) * width
        const y = height - (p.value / maxY) * height
        return `${x},${y}`
      })
      .join(' ')
  }, [history])

  const stopStream = () => {
    if (streamRef.current) {
      streamRef.current.close()
      streamRef.current = null
    }
  }

  const loadCached = async (token: string) => {
    const payload = await requestJson<AccountsCachedResponse>('/api/accounts/cached', token)
    setCurrentLabel(payload.current_label)
    currentLabelRef.current = payload.current_label
    setAccounts(payload.accounts.map((a) => normalizeAccount(a, payload.current_label)))
    setAggregate(payload.aggregate)
    setHistory([{ t: Date.now(), value: payload.aggregate.aggregate_utilization_percent || averagePrimaryPercent(payload.accounts) }])
    setStatus('Loaded cached snapshot')
  }

  const startStream = (token: string) => {
    if (!token.trim()) return
    stopStream()
    setRefreshing(true)
    setStatus('Refreshing usage...')

    const es = new EventSource(`/api/accounts/stream?api_key=${encodeURIComponent(token)}`)
    streamRef.current = es

    es.addEventListener('snapshot', (ev) => {
      const data = JSON.parse((ev as MessageEvent).data) as StreamSnapshot
      setCurrentLabel(data.current_label)
      currentLabelRef.current = data.current_label
      const pending = new Set(data.pending_labels || [])
      setAccounts(
        data.accounts.map((a) => {
          const n = normalizeAccount(a, data.current_label)
          if (pending.has(a.label)) {
            n.refresh_status = { ...(n.refresh_status || {}), state: 'refreshing' }
          }
          return n
        }),
      )
      setAggregate(data.aggregate)
      const util = data.aggregate.aggregate_utilization_percent || averagePrimaryPercent(data.accounts)
      setHistory((prev) => [...prev, { t: Date.now(), value: util }].slice(-50))
    })

    es.addEventListener('account_update', (ev) => {
      const data = JSON.parse((ev as MessageEvent).data) as { account: Account; ok: boolean }
      setAccounts((prev) =>
        prev.map((a) =>
          a.label === data.account.label
            ? {
                ...normalizeAccount(data.account, currentLabelRef.current),
                refresh_status: {
                  ...(data.account.refresh_status || {}),
                  state: data.ok ? 'ok' : 'failed',
                },
              }
            : a,
        ),
      )
    })

    es.addEventListener('aggregate_update', (ev) => {
      const data = JSON.parse((ev as MessageEvent).data) as Aggregate
      setAggregate(data)
      setHistory((prev) => [...prev, { t: Date.now(), value: data.aggregate_utilization_percent || displayUtilization }].slice(-50))
    })

    es.addEventListener('error', (ev) => {
      const msg = (ev as MessageEvent).data || 'Refresh error'
      setErr(msg)
    })

    es.addEventListener('complete', () => {
      setRefreshing(false)
      setStatus('Refresh complete')
      stopStream()
    })

    es.onerror = () => {
      setRefreshing(false)
      setStatus('Refresh stream closed')
      stopStream()
    }
  }

  const applyToken = async () => {
    if (!draftToken.trim()) {
      setErr('Enter INTERNAL_API_TOKEN')
      return
    }
    setErr(null)
    localStorage.setItem('auth_manager_api_key', draftToken)
    setApiKey(draftToken)
    await loadCached(draftToken)
    startStream(draftToken)
  }

  const clearToken = () => {
    setDraftToken('')
    setApiKey('')
    localStorage.removeItem('auth_manager_api_key')
    stopStream()
    setAccounts([])
    setAggregate(defaultAggregate)
    setHistory([])
    setStatus('Token cleared')
    setErr(null)
  }

  const refreshNow = async () => {
    if (!apiKey.trim()) return
    setErr(null)
    await loadCached(apiKey)
    startStream(apiKey)
  }

  const importCurrent = async () => {
    if (!apiKey.trim()) return
    setErr(null)
    setStatus('Importing current auth...')
    await requestJson('/auth/import-current', apiKey, { method: 'POST', body: '{}' })
    await refreshNow()
    setStatus('Imported current auth')
  }

  const addAccount = async () => {
    if (!apiKey.trim()) return
    setErr(null)
    try {
      const start = await requestJson<{ auth_url?: string; session_id?: string; relay_token?: string; instructions?: string }>(
        '/auth/login/start-relay',
        apiKey,
        { method: 'POST', body: '{}' },
      )
      if (start.auth_url) window.open(start.auth_url, '_blank', 'noopener,noreferrer')
      const callbackUrl = window.prompt('Paste full callback URL from the auth tab:')?.trim()
      if (!callbackUrl) return
      const label = window.prompt('Optional profile label (leave empty for auto):')?.trim() || undefined
      await requestJson(
        '/auth/relay-callback',
        apiKey,
        {
          method: 'POST',
          body: JSON.stringify({
            callback_url: callbackUrl,
            relay_token: start.relay_token,
            session_id: start.session_id,
            label,
          }),
        },
      )
      setStatus('Callback relayed. Refreshing...')
      await refreshNow()
    } catch (e) {
      setErr(e instanceof Error ? e.message : 'Add account failed')
    }
  }

  const switchAccount = async (label: string) => {
    await requestJson('/auth/switch', apiKey, { method: 'POST', body: JSON.stringify({ label }) })
    setStatus(`Switched to ${label}`)
    setOpenMenuFor(null)
    await refreshNow()
  }

  const renameAccount = async (oldLabel: string, currentName: string) => {
    const next = window.prompt('New profile label:', currentName)?.trim()
    if (!next || next === oldLabel) return
    await requestJson('/auth/rename', apiKey, {
      method: 'POST',
      body: JSON.stringify({ old_label: oldLabel, new_label: next }),
    })
    setStatus(`Renamed ${oldLabel} -> ${next}`)
    setOpenMenuFor(null)
    await refreshNow()
  }

  const deleteAccount = async (label: string) => {
    if (!window.confirm(`Delete profile '${label}'?`)) return
    await requestJson('/auth/delete', apiKey, { method: 'POST', body: JSON.stringify({ label }) })
    setStatus(`Deleted ${label}`)
    setOpenMenuFor(null)
    await refreshNow()
  }

  const exportAccount = async (label: string) => {
    const res = await fetch(`/auth/export?label=${encodeURIComponent(label)}`, { headers: authHeaders(apiKey) })
    if (!res.ok) throw new Error(await res.text())
    const data = await res.json()
    const blob = new Blob([JSON.stringify(data.auth_json, null, 2)], { type: 'application/json' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `${label}.auth.json`
    a.click()
    URL.revokeObjectURL(url)
    setOpenMenuFor(null)
  }

  const openAccountHistory = async (label: string) => {
    if (!apiKey.trim()) return
    setHistoryModalOpen(true)
    setHistoryLoading(true)
    setHistoryError(null)
    setHistoryData(null)
    try {
      const payload = await requestJson<AccountHistoryResponse>(
        `/api/accounts/${encodeURIComponent(label)}/usage-history`,
        apiKey,
      )
      setHistoryData(payload)
    } catch (e) {
      setHistoryError(e instanceof Error ? e.message : 'Unable to load account history')
    } finally {
      setHistoryLoading(false)
    }
  }

  useEffect(() => {
    if (!apiKey.trim()) return
    void loadCached(apiKey)
      .then(() => startStream(apiKey))
      .catch((e: unknown) => setErr(e instanceof Error ? e.message : 'Load failed'))
    return () => stopStream()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  if (!apiKey.trim()) {
    return (
      <div className="page">
        <div className="login-card panel">
          <div className="brand"><span className="dot" />Auth Manager</div>
          <h2>Enter API Token</h2>
          <p>Use your INTERNAL_API_TOKEN to unlock account management.</p>
          <input
            type="password"
            value={draftToken}
            onChange={(e) => setDraftToken(e.target.value)}
            placeholder="INTERNAL_API_TOKEN"
          />
          <div className="top-actions">
            <button className="btn primary" onClick={() => void applyToken()}>Continue</button>
          </div>
          {err ? <div className="error">{err}</div> : null}
        </div>
      </div>
    )
  }

  return (
    <div className="page">
      <header className="top">
        <div className="brand"><span className="dot" />Auth Manager</div>
        <div className="top-actions">
          {mode === 'manager' ? <button className="btn primary" onClick={() => void addAccount()}>+ Add Account</button> : null}
          {mode === 'manager' ? <button className="btn" onClick={() => void importCurrent()}>Import Current</button> : null}
          <button className="btn" onClick={() => setMode((m) => (m === 'manager' ? 'stats' : 'manager'))}>
            {mode === 'manager' ? 'Overall Stats' : 'Back to Manager'}
          </button>
          <button className="btn" onClick={() => void refreshNow()} disabled={refreshing}>
            {refreshing ? 'Refreshing...' : 'Refresh'}
          </button>
        </div>
      </header>

      <section className="token-row">
        <input
          type="password"
          value={draftToken}
          onChange={(e) => setDraftToken(e.target.value)}
          placeholder="INTERNAL_API_TOKEN"
        />
        <button className="btn" onClick={() => void applyToken()}>Apply</button>
        <button className="btn" onClick={clearToken}>Clear</button>
      </section>

      {err ? <div className="error panel">{err}</div> : null}
      <div className="status-line">{status}</div>

      {mode === 'manager' ? (
        <>
          <section className="dashboard-grid">
            <div className="panel">
              <h4 className="panel-title">System Overview</h4>
              <label>Accounts Managed</label>
              <strong>{accountCount}</strong>
              <label>Profiles With Token</label>
              <strong>{profilesWithToken}</strong>
              <label>Current Profile</label>
              <strong>{currentLabel || '--'}</strong>
            </div>
            <div className="panel">
              <h4 className="panel-title">Aggregated Usage</h4>
              <div className="limit-row"><span>5hr</span><div className="bar"><span className={`fill ${pctClass(avg5hr || 0)}`} style={{ width: `${avg5hr || 0}%` }} /></div><span>{avg5hr === null ? '--' : `${avg5hr}%`}</span></div>
              <div className="muted">{avg5hr === null ? 'No live 5hr data available.' : `${Math.max(0, 100 - avg5hr)}% remaining across cluster`}</div>
              <div className="limit-row" style={{ marginTop: 8 }}><span>7d</span><div className="bar"><span className={`fill ${pctClass(avg7d || 0)}`} style={{ width: `${avg7d || 0}%` }} /></div><span>{avg7d === null ? '--' : `${avg7d}%`}</span></div>
              <div className="muted">{avg7d === null ? 'No live 7d data available.' : `${Math.max(0, 100 - avg7d)}% remaining across cluster`}</div>
              <label style={{ marginTop: 10 }}>Recommended Profile</label>
              <div>
                {recommended ? (
                  <button className="btn btn-sm rec-btn" onClick={() => void switchAccount(recommended.label)}>
                    Switch to {recommended.display_label || recommended.label}
                  </button>
                ) : (
                  <span className="muted">--</span>
                )}
              </div>
            </div>
          </section>

          <section className="panel">
            <div className="saved-head">
              <h3>Saved Profiles</h3>
              <span className="pill">{accountCount} account{accountCount === 1 ? '' : 's'}</span>
            </div>
            <div className="table-head">
              <span>Profile</span>
              <span>Rate Limits</span>
              <span>Rate Limit Reset</span>
              <span>Actions</span>
            </div>
            {accounts.length === 0 ? <div className="empty">No accounts found.</div> : null}
            {accounts.map((a) => {
              const primary = a.rate_limits?.requests || a.rate_limits?.primary
              const secondary = a.rate_limits?.tokens || a.rate_limits?.secondary
              const p1Raw = limitPercent(primary)
              const p2Raw = limitPercent(secondary)
              const p1 = p1Raw === null ? null : Math.max(0, Math.min(100, p1Raw))
              const p2 = p2Raw === null ? null : Math.max(0, Math.min(100, p2Raw))
              const rateError = typeof a.rate_limits?.error === 'string' ? a.rate_limits.error : ''
              const scopeError = rateError.includes('Missing scopes') ? 'Live rate-limit scopes are missing for this account.' : rateError
              const badge = refreshBadge(secondary, null, undefined)
              return (
                <div className="row" key={`${a.account_key}:${a.label}`}>
                  <div>
                    <div className="profile-title">
                      <button className="profile-link-btn" onClick={() => void openAccountHistory(a.label)}>
                        {a.display_label || a.label}
                      </button>
                      {badge ? <span className="pill" style={badge.style}>{badge.text}</span> : null}
                    </div>
                    <div className="muted">{a.email || 'email unavailable'}</div>
                    <div className="muted mono">Profile label: {a.label}</div>
                  </div>
                  <div>
                    {p1 !== null || p2 !== null ? (
                      <>
                        <div className="limit-row"><span>5hr</span><div className="bar"><span className={`fill ${pctClass(p1 || 0)}`} style={{ width: `${p1 || 0}%` }} /></div><span>{p1 === null ? '--' : `${p1}%`}</span></div>
                        <div className="limit-row"><span>7d</span><div className="bar"><span className={`fill ${pctClass(p2 || 0)}`} style={{ width: `${p2 || 0}%` }} /></div><span>{p2 === null ? '--' : `${p2}%`}</span></div>
                      </>
                    ) : (
                      <div className="muted">{scopeError || 'No live rate-limit data available.'}</div>
                    )}
                  </div>
                  <div>
                    <div className="muted">{p1 !== null ? fmtTs(primary?.resetsAt || primary?.resetAt) : '--'}</div>
                    <div className="muted">{p2 !== null ? fmtTs(secondary?.resetsAt || secondary?.resetAt) : '--'}</div>
                  </div>
                  <div className="actions-col">
                    <div className="menu-root">
                      <button className="btn btn-sm" onClick={() => setOpenMenuFor(openMenuFor === a.label ? null : a.label)}>
                        Actions
                      </button>
                      {openMenuFor === a.label ? (
                        <div className="menu-panel">
                          <button className="menu-item" onClick={() => void switchAccount(a.label)}>{a.is_current ? 'Switch (Current)' : 'Switch'}</button>
                          <button className="menu-item" onClick={() => void renameAccount(a.label, a.display_label || a.label)}>Change profile label</button>
                          <button className="menu-item" onClick={() => void exportAccount(a.label)}>Export</button>
                          <button className="menu-item danger" onClick={() => void deleteAccount(a.label)}>Delete</button>
                        </div>
                      ) : null}
                    </div>
                  </div>
                </div>
              )
            })}
          </section>
        </>
      ) : (
        <section className="aggregate panel">
          <h2>Aggregated Usage Analytics</h2>
          <div className="cards">
            <div><label>Total Used</label><strong>{aggregate.total_current_window_limit > 0 ? aggregate.total_current_window_used : '--'}</strong></div>
            <div><label>Total Limit</label><strong>{aggregate.total_current_window_limit > 0 ? aggregate.total_current_window_limit : '--'}</strong></div>
            <div><label>Total Remaining</label><strong>{aggregate.total_current_window_limit > 0 ? aggregate.total_remaining : '--'}</strong></div>
            <div><label>Utilization</label><strong>{displayUtilization}%</strong></div>
            <div><label>Lifetime Used</label><strong>{aggregate.lifetime_total_used}</strong></div>
            <div><label>Total Wasted</label><strong>{aggregate.total_wasted}</strong></div>
            <div><label>Stale Accounts</label><strong>{aggregate.stale_accounts}</strong></div>
            <div><label>Failed Accounts</label><strong>{aggregate.failed_accounts}</strong></div>
            <div><label>Last Refresh</label><strong>{fmtTs(aggregate.last_refresh_time)}</strong></div>
          </div>
          {aggregate.total_current_window_limit === 0 ? (
            <div className="muted" style={{ marginTop: 10 }}>
              Absolute token totals are unavailable from this source; utilization is derived from account rate-limit percentages.
            </div>
          ) : null}
          <div className="graph">
            <svg viewBox="0 0 760 180" preserveAspectRatio="none">
              <polyline points={graphPoints} fill="none" stroke="#10b981" strokeWidth="3" />
            </svg>
          </div>
        </section>
      )}

      {historyModalOpen ? (
        <div className="modal-overlay" onClick={() => setHistoryModalOpen(false)}>
          <div className="modal-card" onClick={(e) => e.stopPropagation()}>
            <div className="modal-head">
              <h3>Account History</h3>
              <button className="btn btn-sm" onClick={() => setHistoryModalOpen(false)}>Close</button>
            </div>
            {historyLoading ? <div className="muted">Loading...</div> : null}
            {historyError ? <div className="error">{historyError}</div> : null}
            {historyData ? (
              <div>
                <div className="muted"><strong>{historyData.display_label || historyData.label}</strong> · {historyData.email || 'email unavailable'}</div>
                <div className="cards" style={{ marginTop: 12 }}>
                  <div><label>Completed Windows</label><strong>{historyData.summary?.window_count ?? 0}</strong></div>
                  <div><label>Total Wasted</label><strong>{historyData.summary?.total_wasted ?? 0}</strong></div>
                  <div><label>Avg Utilization</label><strong>{historyData.summary?.avg_completed_utilization_percent ?? '--'}</strong></div>
                </div>
                <div style={{ marginTop: 12 }}>
                  <div className="muted" style={{ marginBottom: 8 }}>Recent rollovers</div>
                  {historyData.rollovers?.length ? historyData.rollovers.slice(-10).reverse().map((r, idx) => (
                    <div key={idx} className="rollover-row">
                      <span>{fmtTs(r.window_ended_at || '')}</span>
                      <span>used {r.usage_used ?? 0}/{r.usage_limit ?? 0}</span>
                      <span>wasted {r.usage_wasted ?? 0}</span>
                    </div>
                  )) : <div className="muted">No rollover history yet.</div>}
                </div>
              </div>
            ) : null}
          </div>
        </div>
      ) : null}
    </div>
  )
}

createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
)
