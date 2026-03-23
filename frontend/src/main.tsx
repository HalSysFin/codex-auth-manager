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
  account_type?: string | null
  rate_limits?: {
    primary?: Limit | null
    secondary?: Limit | null
    requests?: Limit | null
    tokens?: Limit | null
    error?: string
  }
  usage_tracking?: UsageTracking | null
  refresh_status?: RefreshStatus
  active_lease?: {
    lease_id: string
    machine_id: string
    agent_id: string
    state?: string | null
    issued_at?: string | null
    expires_at?: string | null
  } | null
}

type Aggregate = {
  accounts: number
  fleet_capacity_units: number
  fleet_used_units: number
  fleet_remaining_units: number
  fleet_utilization_pct: number
  fleet_efficiency_pct: number
  total_current_window_used: number
  total_current_window_limit: number
  total_remaining: number
  aggregate_utilization_percent: number
  lifetime_total_used: number
  total_wasted_units: number
  stale_accounts: number
  failed_accounts: number
  last_refresh_time: string | null
}

type AccountsCachedResponse = {
  accounts: Account[]
  current_label: string | null
  aggregate: Aggregate
}

type LoginStatusResponse = {
  status?: string
  callback_received?: boolean
  error?: string | null
  relay?: {
    next_action?: string | null
  }
  auto_persist?: {
    status?: string | null
    reason?: string | null
    label?: string | null
    error?: string | null
    matched_existing_profile?: boolean
    created_new_profile?: boolean
  }
}

type StreamSnapshot = {
  accounts: Account[]
  current_label: string | null
  aggregate: Aggregate
  pending_labels: string[]
}

type ViewMode = 'manager' | 'stats'
type RangeKey = '1d' | '7d' | '30d' | '90d' | 'all'
type AccountSortKey =
  | 'consumption_asc'
  | 'consumption_desc'
  | 'name_asc'
  | 'name_desc'
  | 'weekly_refresh_asc'
  | 'weekly_refresh_desc'

type AccountHistoryResponse = {
  label: string
  account_key: string
  display_label: string | null
  email: string | null
  account_type?: string | null
  range: RangeKey
  range_metadata?: {
    label?: string
    window_label?: string
    timezone?: string
    boundary_mode?: string
  }
  summary?: {
    absolute_usage_available?: boolean
    total_consumed_in_range: number | null
    average_daily_consumption: number | null
    current_total_used: number | null
    current_total_limit: number | null
    current_total_remaining: number | null
    total_wasted: number
    stale_account_count: number
    failed_account_count: number
    last_refresh_time: string | null
    last_refresh_label?: string | null
    fallback_mode?: boolean
    fallback_reason?: string | null
    modeled_usage_basis?: string | null
    weekly_utilization_now?: number | null
    average_weekly_utilization_in_range?: number | null
  }
  current_state: {
    absolute_usage_available?: boolean
    usage_in_window: number | null
    usage_limit: number | null
    remaining: number | null
    utilization_percent?: number | null
    weekly_used_units?: number | null
    weekly_remaining_units?: number | null
    efficiency_pct?: number | null
    next_reset?: string | null
    lifetime_used: number | null
    last_sync?: string | null
    refresh_status?: RefreshStatus
  }
  consumption_trend: {
    cumulative_usage: Array<{ day: string; cumulative: number; consumed: number }>
    daily_usage: Array<{ day: string; consumed: number }>
    total_consumed_in_range: number | null
    average_daily_consumption: number | null
    absolute_usage_available?: boolean
    fallback_mode?: boolean
    modeled_usage_basis?: string | null
    daily_weekly_utilization?: Array<{ day: string; value: number }>
    hourly_weekly_utilization?: Array<{ t: string; value: number }>
  }
  completed_windows: Array<{
    window_start?: string
    window_end?: string
    used: number
    limit: number
    wasted: number
    utilization_percent?: number | null
    rolled_over_at?: string | null
    primary_percent_at_reset?: number | null
    secondary_percent_at_reset?: number | null
  }>
  wastage_series: {
    daily_wasted: Array<{ day: string; value: number }>
    daily_used: Array<{ day: string; value: number }>
    total_wasted: number
  }
  freshness: {
    coverage_start?: string | null
    coverage_end?: string | null
    snapshot_points: number
    daily_points: number
    is_sparse: boolean
  }
}

type UsageHistoryResponse = {
  range: RangeKey
  range_metadata?: {
    label?: string
    window_label?: string
    timezone?: string
    boundary_mode?: string
  }
  summary: {
    absolute_usage_available?: boolean
    total_consumed_in_range: number | null
    average_daily_consumption: number | null
    current_total_used: number | null
    current_total_limit: number | null
    current_total_remaining: number | null
    total_wasted: number
    stale_account_count: number
    failed_account_count: number
    last_refresh_time: string | null
    last_refresh_label?: string | null
    fallback_mode?: boolean
    fallback_reason?: string | null
    modeled_usage_basis?: string | null
    weekly_utilization_now?: number | null
    average_weekly_utilization_in_range?: number | null
  }
  series: {
    cumulative_usage: Array<{ day: string; cumulative: number; consumed: number }>
    daily_usage: Array<{ day: string; consumed: number }>
    daily_rollover_wasted: Array<{ day: string; value: number }>
    daily_rollover_used: Array<{ day: string; value: number }>
    daily_weekly_utilization?: Array<{ day: string; value: number }>
    hourly_weekly_utilization?: Array<{ t: string; value: number }>
  }
  sections: {
    top_consuming_accounts: Array<{
      account_key: string
      label: string
      display_label: string | null
      email: string | null
      consumed: number
    }>
    top_consuming_accounts_available?: boolean
    stale_accounts: Array<{
      account_key: string
      label: string
      display_label: string | null
      email: string | null
      last_success_at?: string | null
      last_error?: string | null
    }>
    failed_accounts: Array<{
      account_key: string
      label: string
      display_label: string | null
      email: string | null
      last_attempt_at?: string | null
      last_error?: string | null
    }>
    recent_rollovers: Array<{
      label: string
      display_label: string | null
      email: string | null
      window_ended_at?: string | null
      usage_used?: number
      usage_limit?: number
      usage_wasted?: number
      rolled_over_at?: string | null
    }>
  }
  freshness: {
    coverage_start?: string | null
    coverage_end?: string | null
    snapshot_points: number
    daily_points: number
    is_sparse: boolean
  }
}

type SessionStatus = {
  web_login_enabled: boolean
  session_valid: boolean
}

const defaultAggregate: Aggregate = {
  accounts: 0,
  fleet_capacity_units: 0,
  fleet_used_units: 0,
  fleet_remaining_units: 0,
  fleet_utilization_pct: 0,
  fleet_efficiency_pct: 100,
  total_current_window_used: 0,
  total_current_window_limit: 0,
  total_remaining: 0,
  aggregate_utilization_percent: 0,
  lifetime_total_used: 0,
  total_wasted_units: 0,
  stale_accounts: 0,
  failed_accounts: 0,
  last_refresh_time: null,
}

const SESSION_TOKEN = '__session__'
const ACTION_API_KEY_STORAGE = 'auth_manager_action_api_key'
const PRIVACY_MODE_STORAGE = 'auth_manager_privacy_mode'
const RANGE_LABELS: Record<RangeKey, string> = {
  '1d': 'Today',
  '7d': '7d',
  '30d': '30d',
  '90d': '90d',
  all: 'All',
}

function authHeaders(token: string): Record<string, string> {
  if (!token || token === SESSION_TOKEN) return {}
  return { Authorization: `Bearer ${token}` }
}

async function requestJson<T>(path: string, token: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, {
    ...init,
    credentials: 'include',
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

async function copyText(text: string): Promise<boolean> {
  if (!text) return false
  if (navigator.clipboard?.writeText) {
    try {
      await navigator.clipboard.writeText(text)
      return true
    } catch {
      // fall through to execCommand path
    }
  }
  try {
    const area = document.createElement('textarea')
    area.value = text
    area.setAttribute('readonly', 'true')
    area.style.position = 'fixed'
    area.style.opacity = '0'
    document.body.appendChild(area)
    area.select()
    const ok = document.execCommand('copy')
    document.body.removeChild(area)
    return ok
  } catch {
    return false
  }
}

function fmtTs(value: string | number | null | undefined): string {
  if (!value) return '--'
  let d: Date
  if (typeof value === 'number' && Number.isFinite(value)) {
    // upstream often sends epoch seconds
    d = new Date(value * 1000)
  } else if (typeof value === 'string' && /^\d+$/.test(value.trim())) {
    d = new Date(Number(value.trim()) * 1000)
  } else {
    d = new Date(value)
  }
  if (Number.isNaN(d.getTime())) return String(value)
  const now = new Date()
  const diff = d.getTime() - now.getTime()
  const absDiff = Math.abs(diff)
  
  if (absDiff < 86400000) { // Less than 24 hours
    if (diff > 0) { // Future date
      const h = Math.floor(diff / 3600000)
      const m = Math.floor((diff % 3600000) / 60000)
      return h > 0 ? `in ${h}h ${m}m` : `in ${m}m`
    } else { // Past date
      const h = Math.floor(absDiff / 3600000)
      const m = Math.floor((absDiff % 3600000) / 60000)
      if (h === 0 && m === 0) return 'just now'
      return h > 0 ? `${h}h ${m}m ago` : `${m}m ago`
    }
  }
  return d.toLocaleString('en-US', {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  })
}

function fmtNullableNumber(value: number | null | undefined, suffix = ''): string {
  if (value == null || !Number.isFinite(value)) return 'Unavailable'
  return `${value}${suffix}`
}

function CodexBadge(): React.ReactElement {
  return (
    <span className="codex-badge" aria-label="Codex managed account" title="Codex managed account">
      <span className="codex-badge-mark" aria-hidden="true" />
      <span className="codex-badge-text">Codex</span>
    </span>
  )
}

function rangeLabel(range: RangeKey, meta?: { label?: string; window_label?: string }): string {
  if (range === '1d') return meta?.label || 'Today'
  return meta?.label || range
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
    text: `reset in ${text}`,
    style: {
      color: `hsl(${hue}, 82%, 55%)`,
      borderColor: `hsla(${hue}, 82%, 55%, .2)`,
      background: `hsla(${hue}, 82%, 55%, .08)`,
      textTransform: 'lowercase',
    },
  }
}

function accountDisplayName(account: Account): string {
  return (account.display_label || account.label || account.email || '').trim().toLowerCase()
}

function accountConsumptionValue(account: Account): number {
  const primary = limitPercent(account.rate_limits?.requests || account.rate_limits?.primary)
  const secondary = limitPercent(account.rate_limits?.tokens || account.rate_limits?.secondary)
  const candidate = secondary ?? primary
  return candidate === null ? Number.POSITIVE_INFINITY : candidate
}

function accountWeeklyRefreshValue(account: Account): number {
  const secondary = account.rate_limits?.tokens || account.rate_limits?.secondary
  const refreshAt = resetDate(secondary, account.usage_tracking)
  return refreshAt ? refreshAt.getTime() : Number.POSITIVE_INFINITY
}

function redactText(value: string | null | undefined): string {
  const source = String(value || '')
  if (!source) return 'Hidden'
  const masked = source.replace(/[A-Za-z0-9]/g, '•')
  return masked.trim() || 'Hidden'
}

function App() {
  const [apiKey, setApiKey] = useState('')
  const [actionApiKey, setActionApiKey] = useState('')
  const [apiKeyModalOpen, setApiKeyModalOpen] = useState(false)
  const [settingsModalOpen, setSettingsModalOpen] = useState(false)
  const [privacyMode, setPrivacyMode] = useState(false)
  const [apiKeyInput, setApiKeyInput] = useState('')
  const [addAccountModalOpen, setAddAccountModalOpen] = useState(false)
  const [addAccountLoading, setAddAccountLoading] = useState(false)
  const [addAuthUrl, setAddAuthUrl] = useState('')
  const [addSessionId, setAddSessionId] = useState('')
  const [addRelayToken, setAddRelayToken] = useState('')
  const [addCallbackUrl, setAddCallbackUrl] = useState('')
  const [addLabelInput, setAddLabelInput] = useState('')
  const [addAccountFeedback, setAddAccountFeedback] = useState<string | null>(null)
  const [addAccountFeedbackTone, setAddAccountFeedbackTone] = useState<'info' | 'success' | 'error'>('info')
  const [importAuthModalOpen, setImportAuthModalOpen] = useState(false)
  const [importAuthText, setImportAuthText] = useState('')
  const [importAuthLabel, setImportAuthLabel] = useState('')
  const [importAuthLoading, setImportAuthLoading] = useState(false)
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [loginLoading, setLoginLoading] = useState(false)
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
  const [usageHistory, setUsageHistory] = useState<UsageHistoryResponse | null>(null)
  const [accountSort, setAccountSort] = useState<AccountSortKey>('consumption_asc')
  const [selectedRange, setSelectedRange] = useState<RangeKey>('30d')
  const [statsChartMode, setStatsChartMode] = useState<'cumulative' | 'daily'>('cumulative')
  const [accountChartMode, setAccountChartMode] = useState<'cumulative' | 'daily'>('cumulative')
  const [accountHistoryRange, setAccountHistoryRange] = useState<RangeKey>('30d')
  const [activeHistoryLabel, setActiveHistoryLabel] = useState<string | null>(null)
  const streamRef = useRef<EventSource | null>(null)
  const currentLabelRef = useRef<string | null>(currentLabel)
  const hasActionApiKey = actionApiKey.trim().length > 0
  const sensitiveText = (value: string | null | undefined): string => (privacyMode ? redactText(value) : (value || ''))

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
    const getScore = (acc: Account) => {
      const p1 = limitPercent(acc.rate_limits?.requests || acc.rate_limits?.primary) ?? 100
      const p2 = limitPercent(acc.rate_limits?.tokens || acc.rate_limits?.secondary) ?? 100
      const r2 = resetDate(acc.rate_limits?.tokens || acc.rate_limits?.secondary, acc.usage_tracking)
      const now = Date.now()
      const msLeft = r2 ? Math.max(0, r2.getTime() - now) : 7 * 24 * 3600 * 1000
      const usage = Math.max(p1, p2)
      if (usage >= 90) { return 1e15 + msLeft }
      return msLeft
    }
    if (!best) return a
    return getScore(a) < getScore(best) ? a : best
  }, null)
  const sortedAccounts = useMemo(() => {
    const copy = [...accounts]
    copy.sort((left, right) => {
      switch (accountSort) {
        case 'consumption_desc':
          return accountConsumptionValue(right) - accountConsumptionValue(left)
        case 'name_asc':
          return accountDisplayName(left).localeCompare(accountDisplayName(right))
        case 'name_desc':
          return accountDisplayName(right).localeCompare(accountDisplayName(left))
        case 'weekly_refresh_asc':
          return accountWeeklyRefreshValue(left) - accountWeeklyRefreshValue(right)
        case 'weekly_refresh_desc':
          return accountWeeklyRefreshValue(right) - accountWeeklyRefreshValue(left)
        case 'consumption_asc':
        default:
          return accountConsumptionValue(left) - accountConsumptionValue(right)
      }
    })
    return copy
  }, [accounts, accountSort])

  const statsDaily = usageHistory?.series.daily_usage || []
  const statsCumulative = usageHistory?.series.cumulative_usage || []
  const statsSummary = usageHistory?.summary
  const statsRangeMeta = usageHistory?.range_metadata
  const weeklyUtilizationSeries = selectedRange === '1d'
    ? (usageHistory?.series.hourly_weekly_utilization || [])
    : (usageHistory?.series.daily_weekly_utilization || [])
  const statsFallbackMode = Boolean(usageHistory?.summary.fallback_mode)
  const statsModeledFallback = Boolean(statsFallbackMode && usageHistory?.summary.modeled_usage_basis)
  const statsPrimarySeries = statsModeledFallback
    ? (statsChartMode === 'daily' ? statsDaily : statsCumulative)
    : (statsFallbackMode
        ? weeklyUtilizationSeries
        : (statsChartMode === 'daily' ? statsDaily : statsCumulative))
  const statsMaxValue = statsModeledFallback
    ? Math.max(1, ...statsPrimarySeries.map((d: any) => Number((d as any).consumed ?? (d as any).cumulative ?? 0)))
    : (statsFallbackMode
        ? 100
        : Math.max(1, ...statsPrimarySeries.map((d: any) => Number((d as any).consumed ?? (d as any).cumulative ?? 0))))
  const wastedSeries = usageHistory?.series.daily_rollover_wasted || []
  const wastedMaxValue = Math.max(1, ...wastedSeries.map((d) => Number(d.value || 0)))
  const weeklyPercents = accounts
    .map((a) => limitPercent(a.rate_limits?.tokens || a.rate_limits?.secondary))
    .filter((v): v is number => typeof v === 'number' && Number.isFinite(v))
  const weeklyUtilizationNow = weeklyPercents.length
    ? Math.round(weeklyPercents.reduce((sum, value) => sum + value, 0) / weeklyPercents.length)
    : null
  const weeklyAtCapCount = weeklyPercents.filter((v) => v >= 100).length
  const chartRangeLabel = rangeLabel(selectedRange, statsRangeMeta)

  const buildLinePath = (values: number[], width = 1000, height = 220, pad = 24) => {
    if (!values.length) return ''
    const maxVal = Math.max(1, ...values)
    const usableW = width - pad * 2
    const usableH = height - pad * 2
    return values
      .map((v, i) => {
        const x = pad + (i / Math.max(values.length - 1, 1)) * usableW
        const y = height - pad - (v / maxVal) * usableH
        return `${i === 0 ? 'M' : 'L'} ${x.toFixed(2)} ${y.toFixed(2)}`
      })
      .join(' ')
  }

  const graphPath = useMemo(() => {
    if (!history.length) return ''
    const maxY = Math.max(100, ...history.map((p) => p.value))
    const width = 100
    const height = 100
    const points = history.map((p, i) => {
      const x = (i / Math.max(history.length - 1, 1)) * width
      const y = height - (p.value / maxY) * height
      return `${x},${y}`
    })
    const line = points.join(' L ')
    return `M ${points[0]} L ${line} L ${width},${height} L 0,${height} Z`
  }, [history])

  const graphLine = useMemo(() => {
    if (!history.length) return ''
    const maxY = Math.max(100, ...history.map((p) => p.value))
    const width = 100
    const height = 100
    const points = history.map((p, i) => {
      const x = (i / Math.max(history.length - 1, 1)) * width
      const y = height - (p.value / maxY) * height
      return `${x},${y}`
    })
    return `M ${points.join(' L ')}`
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
    
    // Keep manager sparkline responsive with short-range daily usage.
    try {
      setHistoryLoading(true)
      const historyPayload = await requestJson<UsageHistoryResponse>(`/api/usage/history?range=7d`, token)
      setUsageHistory(historyPayload)
      if (historyPayload.series?.daily_usage?.length) {
        const cutoff = Date.now() - 24 * 60 * 60 * 1000
        const last24h = historyPayload.series.daily_usage
          .map((d) => ({ t: new Date(`${d.day}T00:00:00Z`).getTime(), value: Number(d.consumed || 0) }))
          .filter((d) => Number.isFinite(d.t) && d.t >= cutoff)
        if (last24h.length) {
          setHistory(last24h)
        } else {
          const fallback = historyPayload.summary.total_consumed_in_range ?? payload.aggregate.total_current_window_used ?? 0
          setHistory([{ t: Date.now(), value: fallback }])
        }
      } else {
        const fallback = payload.aggregate.total_current_window_used ?? 0
        setHistory([{ t: Date.now(), value: fallback }])
      }
    } catch {
      const fallback = payload.aggregate.total_current_window_used ?? 0
      setHistory([{ t: Date.now(), value: fallback }])
    } finally {
      setHistoryLoading(false)
    }
    setStatus('Loaded cached snapshot')
  }

  const loadUsageHistory = async (token: string, range: RangeKey) => {
    const data = await requestJson<UsageHistoryResponse>(`/api/usage/history?range=${range}`, token)
    setUsageHistory(data)
  }

  const loadSessionStatus = async (): Promise<SessionStatus> => {
    const res = await fetch('/api/session/status', { credentials: 'include' })
    if (!res.ok) {
      return { web_login_enabled: true, session_valid: false }
    }
    const payload = (await res.json()) as SessionStatus
    return {
      web_login_enabled: Boolean(payload.web_login_enabled),
      session_valid: Boolean(payload.session_valid),
    }
  }

  const startStream = (token: string) => {
    if (!token.trim()) return
    stopStream()
    setRefreshing(true)
    setStatus('Refreshing usage...')

    const streamUrl = token === SESSION_TOKEN
      ? '/api/accounts/stream'
      : `/api/accounts/stream?api_key=${encodeURIComponent(token)}`
    const es = new EventSource(streamUrl)
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
      void loadUsageHistory(token, selectedRange).catch(() => {})
    })

    es.addEventListener('error', (ev) => {
      const raw = (ev as MessageEvent).data
      let msg = 'Refresh error'
      try {
        const parsed = typeof raw === 'string' ? JSON.parse(raw) : raw
        if (parsed && typeof parsed === 'object') {
          const label = typeof parsed.label === 'string' ? parsed.label : null
          const reason = typeof parsed.message === 'string' ? parsed.message : null
          if (label && reason) {
            msg = `${label}: ${reason}`
          } else if (reason) {
            msg = reason
          }
        } else if (typeof raw === 'string' && raw.trim()) {
          msg = raw
        }
      } catch {
        if (typeof raw === 'string' && raw.trim()) {
          msg = raw
        }
      }
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

  const loginWithPassword = async () => {
    if (!username.trim() || !password) {
      setErr('Enter username and password')
      return
    }
    setErr(null)
    setLoginLoading(true)
    try {
      await requestJson('/login', '', {
        method: 'POST',
        body: JSON.stringify({ username: username.trim(), password, next: '/' }),
      })
      setApiKey(SESSION_TOKEN)
      setStatus('Signed in')
      setPassword('')
    } catch (e) {
      setErr(e instanceof Error ? e.message : 'Login failed')
    } finally {
      setLoginLoading(false)
    }
  }

  const logoutSession = async () => {
    await requestJson('/logout', '', { method: 'POST', body: '{}' })
    stopStream()
    setAccounts([])
    setAggregate(defaultAggregate)
    setHistory([])
    setApiKey('')
    setStatus('Signed out')
    setErr(null)
  }

  const saveActionApiKey = () => {
    const next = apiKeyInput.trim()
    setActionApiKey(next)
    if (next) {
      localStorage.setItem(ACTION_API_KEY_STORAGE, next)
      setStatus('API key saved')
    } else {
      localStorage.removeItem(ACTION_API_KEY_STORAGE)
      setStatus('API key cleared (read-only mode)')
    }
    setApiKeyModalOpen(false)
  }

  const togglePrivacyMode = () => {
    setPrivacyMode((current) => {
      const next = !current
      if (next) localStorage.setItem(PRIVACY_MODE_STORAGE, '1')
      else localStorage.removeItem(PRIVACY_MODE_STORAGE)
      setStatus(next ? 'Privacy mode enabled' : 'Privacy mode disabled')
      return next
    })
  }

  const requireActionApiKey = (actionLabel: string): boolean => {
    if (hasActionApiKey) return true
    setErr(`API key required for ${actionLabel}.`)
    setApiKeyModalOpen(true)
    return false
  }

  const refreshNow = async () => {
    if (!apiKey.trim()) return
    setErr(null)
    await loadCached(apiKey)
    startStream(apiKey)
  }

  const startAddAccount = async () => {
    if (!apiKey.trim()) return
    setErr(null)
    setAddAccountLoading(true)
    try {
      const start = await requestJson<{ auth_url?: string; session_id?: string; relay_token?: string; instructions?: string }>(
        '/auth/login/start-relay',
        apiKey,
        { method: 'POST', body: '{}' },
      )
      const authUrl = (start.auth_url || '').trim()
      setAddAuthUrl(authUrl)
      setAddSessionId((start.session_id || '').trim())
      setAddRelayToken((start.relay_token || '').trim())
      setAddCallbackUrl('')
      setAddLabelInput('')
      setAddAccountFeedback(null)
      setAddAccountFeedbackTone('info')
      setAddAccountModalOpen(true)
      setStatus('Add Account link ready. Copy the auth URL, complete login, then paste the callback URL.')
    } catch (e) {
      setErr(e instanceof Error ? e.message : 'Add account failed')
    } finally {
      setAddAccountLoading(false)
    }
  }

  const pollAddAccountFinalization = async (): Promise<LoginStatusResponse> => {
    let last: LoginStatusResponse = {}
    for (let attempt = 0; attempt < 18; attempt += 1) {
      const next = await requestJson<LoginStatusResponse>('/auth/login/status', apiKey, { method: 'GET' })
      last = next
      if (next.status === 'complete' || next.status === 'failed') {
        return next
      }
      await new Promise((resolve) => window.setTimeout(resolve, 1000))
    }
    return last
  }

  const submitAddAccountCallback = async () => {
    if (!apiKey.trim()) return
    const fullUrl = addCallbackUrl.trim()
    if (!fullUrl || (!fullUrl.includes('code=') && !fullUrl.includes('error='))) {
      setErr('Paste a full callback URL containing code= or error=')
      return
    }
    if (!addSessionId || !addRelayToken) {
      setErr('Login session is missing. Start Add Account again.')
      return
    }
    setErr(null)
    setAddAccountLoading(true)
    setAddAccountFeedback('Submitting callback to Codex CLI...')
    setAddAccountFeedbackTone('info')
    try {
      const parsedUrl = new URL(fullUrl)
      await requestJson('/auth/relay-callback', apiKey, {
        method: 'POST',
        body: JSON.stringify({
          code: parsedUrl.searchParams.get('code') || undefined,
          state: parsedUrl.searchParams.get('state') || undefined,
          error: parsedUrl.searchParams.get('error') || undefined,
          error_description: parsedUrl.searchParams.get('error_description') || undefined,
          full_url: fullUrl,
          relay_token: addRelayToken,
          session_id: addSessionId,
          label: addLabelInput.trim() || undefined,
        }),
      })
      setAddAccountFeedback('Callback accepted. Waiting for Codex CLI to finalize auth...')
      setAddAccountFeedbackTone('info')
      const loginStatus = await pollAddAccountFinalization()
      const autoPersist = loginStatus.auto_persist || {}
      const nextAction = loginStatus.relay?.next_action || loginStatus.error || autoPersist.error || 'Add Account did not complete.'

      if (loginStatus.status === 'complete' && (autoPersist.status === 'persisted' || autoPersist.status === 'skipped')) {
        const label = autoPersist.label ? ` (${autoPersist.label})` : ''
        const successMessage =
          autoPersist.status === 'persisted'
            ? autoPersist.created_new_profile
              ? `Auth saved as a new profile${label}.`
              : `Auth updated the matching saved profile${label}.`
            : `Auth finalized successfully${label}.`
        setAddAccountFeedback(successMessage)
        setAddAccountFeedbackTone('success')
        setStatus(successMessage)
        await refreshNow()
        window.setTimeout(() => {
          setAddAccountModalOpen(false)
          setAddAccountFeedback(null)
        }, 1200)
        return
      }

      setAddAccountFeedback(nextAction)
      setAddAccountFeedbackTone('error')
      setStatus('Add Account needs attention.')
    } catch (e) {
      const message = e instanceof Error ? e.message : 'Callback relay failed'
      setErr(message)
      setAddAccountFeedback(message)
      setAddAccountFeedbackTone('error')
    } finally {
      setAddAccountLoading(false)
    }
  }

  const openImportAuthModal = () => {
    if (!requireActionApiKey('import auth')) return
    setImportAuthModalOpen(true)
  }

  const importAuthFromModal = async () => {
    if (!apiKey.trim()) return
    if (!requireActionApiKey('import auth')) return
    const raw = importAuthText.trim()
    if (!raw) {
      setErr('Paste auth JSON or upload a JSON file')
      return
    }
    let parsed: unknown
    try {
      parsed = JSON.parse(raw)
    } catch {
      setErr('Invalid JSON')
      return
    }
    const base = parsed as Record<string, unknown>
    const authJson =
      base && typeof base === 'object' && base.authJson && typeof base.authJson === 'object'
        ? (base.authJson as Record<string, unknown>)
        : (parsed as Record<string, unknown>)

    setErr(null)
    setImportAuthLoading(true)
    try {
      await requestJson('/auth/import-json', actionApiKey, {
        method: 'POST',
        body: JSON.stringify({
          auth_json: authJson,
          label: importAuthLabel.trim() || undefined,
        }),
      })
      setImportAuthModalOpen(false)
      setImportAuthText('')
      setImportAuthLabel('')
      setStatus('Auth imported')
      await refreshNow()
    } catch (e) {
      setErr(e instanceof Error ? e.message : 'Import auth failed')
    } finally {
      setImportAuthLoading(false)
    }
  }

  const onImportAuthFile = async (file: File | null) => {
    if (!file) return
    try {
      const text = await file.text()
      setImportAuthText(text)
      setStatus(`Loaded ${file.name}`)
    } catch {
      setErr('Unable to read uploaded file')
    }
  }

  const switchAccount = async (label: string) => {
    if (!requireActionApiKey('switch')) return
    await requestJson('/auth/switch', actionApiKey, { method: 'POST', body: JSON.stringify({ label }) })
    setStatus(`Switched to ${label}`)
    setOpenMenuFor(null)
    await refreshNow()
  }

  const renameAccount = async (oldLabel: string, currentName: string) => {
    if (!requireActionApiKey('rename')) return
    const next = window.prompt('New profile label:', currentName)?.trim()
    if (!next || next === oldLabel) return
    await requestJson('/auth/rename', actionApiKey, {
      method: 'POST',
      body: JSON.stringify({ old_label: oldLabel, new_label: next }),
    })
    setStatus(`Renamed ${oldLabel} -> ${next}`)
    setOpenMenuFor(null)
    await refreshNow()
  }

  const deleteAccount = async (label: string) => {
    if (!requireActionApiKey('delete')) return
    if (!window.confirm(`Delete profile '${label}'?`)) return
    await requestJson('/auth/delete', actionApiKey, { method: 'POST', body: JSON.stringify({ label }) })
    setStatus(`Deleted ${label}`)
    setOpenMenuFor(null)
    await refreshNow()
  }

  const exportAccount = async (label: string) => {
    if (!requireActionApiKey('export')) return
    const res = await fetch(`/auth/export?label=${encodeURIComponent(label)}`, {
      credentials: 'include',
      headers: authHeaders(actionApiKey),
    })
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
    setActiveHistoryLabel(label)
    setAccountHistoryRange('30d')
    setAccountChartMode('cumulative')
    setHistoryModalOpen(true)
    setHistoryLoading(true)
    setHistoryError(null)
    setHistoryData(null)
    try {
      const histPayload = await requestJson<AccountHistoryResponse>(
        `/api/accounts/${encodeURIComponent(label)}/history?range=30d`,
        apiKey,
      )
      setHistoryData(histPayload)
    } catch (e) {
      setHistoryError(e instanceof Error ? e.message : 'Unable to load account history')
    } finally {
      setHistoryLoading(false)
    }
  }

  const reloadAccountHistory = async (range: RangeKey) => {
    if (!apiKey.trim() || !activeHistoryLabel) return
    setAccountHistoryRange(range)
    setHistoryLoading(true)
    setHistoryError(null)
    try {
      const histPayload = await requestJson<AccountHistoryResponse>(
        `/api/accounts/${encodeURIComponent(activeHistoryLabel)}/history?range=${range}`,
        apiKey,
      )
      setHistoryData(histPayload)
    } catch (e) {
      setHistoryError(e instanceof Error ? e.message : 'Unable to load account history')
    } finally {
      setHistoryLoading(false)
    }
  }

  useEffect(() => {
    const handleOutsideClick = (e: MouseEvent) => {
      const target = e.target as HTMLElement
      if (!target.closest('.menu-root')) {
        setOpenMenuFor(null)
      }
    }
    document.addEventListener('click', handleOutsideClick)
    return () => document.removeEventListener('click', handleOutsideClick)
  }, [])

  useEffect(() => {
    const stored = localStorage.getItem(ACTION_API_KEY_STORAGE) || ''
    setActionApiKey(stored)
    setApiKeyInput(stored)
    setPrivacyMode(localStorage.getItem(PRIVACY_MODE_STORAGE) === '1')
  }, [])

  useEffect(() => {
    if (!apiKey.trim()) {
      void loadSessionStatus()
        .then((status) => {
          if (status.session_valid) {
            setApiKey(SESSION_TOKEN)
          } else if (!status.web_login_enabled && actionApiKey.trim()) {
            // In API-key mode, reuse the saved action key for read requests too.
            setApiKey(actionApiKey.trim())
          }
        })
        .catch(() => {})
      return
    }
    void loadCached(apiKey)
      .then(async () => {
        await loadUsageHistory(apiKey, selectedRange)
        startStream(apiKey)
      })
      .catch((e: unknown) => {
        const msg = e instanceof Error ? e.message : 'Load failed'
        setErr(msg)
        if (apiKey === SESSION_TOKEN && /api key required|401|403/i.test(msg)) {
          setApiKey('')
        }
      })
    return () => stopStream()
  }, [apiKey, actionApiKey])

  useEffect(() => {
    if (!apiKey.trim()) return
    void loadUsageHistory(apiKey, selectedRange).catch(() => {})
  }, [selectedRange])

  if (!apiKey.trim()) {
    return (
      <div className="page">
        <div className="login-card panel">
          <div className="brand"><span className="dot" />Auth Manager</div>
          <h2>Sign In</h2>
          <p>Use the username/password from environment configuration.</p>
          <input
            type="text"
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            placeholder="Username"
            style={{ marginBottom: 8 }}
          />
          <input
            type="password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            placeholder="Password"
          />
          <div className="top-actions">
            <button className="btn primary" onClick={() => void loginWithPassword()} disabled={loginLoading}>
              {loginLoading ? 'Signing in...' : 'Continue'}
            </button>
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
          <button className="btn settings-btn" onClick={() => setSettingsModalOpen(true)} title="Settings" aria-label="Settings">
            <span aria-hidden="true">⚙</span>
          </button>
          <button
            className={`btn api-key-btn ${hasActionApiKey ? 'ready' : 'missing'}`}
            onClick={() => setApiKeyModalOpen(true)}
          >
            {hasActionApiKey ? 'API Key: Set' : 'API Key: Missing'}
          </button>
          {mode === 'manager' ? <button className="btn primary" onClick={() => void startAddAccount()} disabled={addAccountLoading}>+ Add Account</button> : null}
          {mode === 'manager' ? <button className="btn" onClick={openImportAuthModal} disabled={!hasActionApiKey}>Import Auth</button> : null}
          <button className="btn" onClick={() => setMode((m) => (m === 'manager' ? 'stats' : 'manager'))}>
            {mode === 'manager' ? 'Overall Stats' : 'Back to Manager'}
          </button>
          <button className="btn" onClick={() => void refreshNow()} disabled={refreshing}>
            {refreshing ? (
              <span className="btn-with-spinner">
                <span className="spinner" aria-hidden="true" />
                Refresh
              </span>
            ) : (
              'Refresh'
            )}
          </button>
          <button className="btn" onClick={() => void logoutSession()}>Logout</button>
        </div>
      </header>

      {err ? <div className="error panel">{err}</div> : null}

      {mode === 'manager' ? (
        <>
          <section className="dashboard-grid">
            <div className="panel overflow-hidden">
              <h4 className="panel-title">System Overview</h4>
              <div className="summary-grid">
                <div>
                  <label>Managed Accounts</label>
                  <strong>{accountCount}</strong>
                </div>
                <div>
                  <label>Next Refresh</label>
                  <strong style={{ color: '#10b981' }}>{refreshBadge(recommended?.rate_limits?.requests || recommended?.rate_limits?.primary, recommended?.rate_limits?.tokens || recommended?.rate_limits?.secondary, recommended?.usage_tracking)?.text.replace('reset in ', '') || '--'}</strong>
                </div>
              </div>
              
              <div className="sparkline-container" style={{ marginTop: 16 }}>
                 <div className="graph-label">Usage Trend (24h)</div>
                 <div className="sparkline">
                    <svg viewBox="0 0 100 100" preserveAspectRatio="none">
                       {history.length > 0 && <path d={graphPath} fill="rgba(16, 185, 129, 0.05)" />}
                       {history.length > 0 && <path d={graphLine} fill="none" stroke="#10b981" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />}
                    </svg>
                 </div>
              </div>
            </div>
            <div className="panel">
              <h4 className="panel-title">Aggregated Usage</h4>
              <div className="limit-row"><span>5hr</span><div className="bar"><span className={`fill ${pctClass(avg5hr || 0)}`} style={{ width: `${avg5hr || 0}%` }} /></div><span>{avg5hr === null ? '--' : `${avg5hr}%`}</span></div>
              <div className="muted">{avg5hr === null ? 'No live 5hr data available.' : `${Math.max(0, 100 - avg5hr)}% remaining across cluster`}</div>
              <div className="limit-row" style={{ marginTop: 8 }}><span>7d</span><div className="bar"><span className={`fill ${pctClass(avg7d || 0)}`} style={{ width: `${avg7d || 0}%` }} /></div><span>{avg7d === null ? '--' : `${avg7d}%`}</span></div>
              <div className="muted">{avg7d === null ? 'No live 7d data available.' : `${Math.max(0, 100 - avg7d)}% remaining across cluster`}</div>
              <label style={{ marginTop: 10 }}>Recommended Profile</label>
              <div style={{ marginTop: 8 }}>
                {recommended ? (
                  <button className="btn btn-sm rec-btn" onClick={() => void switchAccount(recommended.label)}>
                    Switch to {sensitiveText(recommended.display_label || recommended.label)}
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
              <div className="saved-head-actions">
                <label className="sort-control">
                  <span>Sort</span>
                  <select value={accountSort} onChange={(e) => setAccountSort(e.target.value as AccountSortKey)}>
                    <option value="consumption_asc">Consumption: least to most</option>
                    <option value="consumption_desc">Consumption: most to least</option>
                    <option value="name_asc">Name: A to Z</option>
                    <option value="name_desc">Name: Z to A</option>
                    <option value="weekly_refresh_asc">Weekly refresh: soonest first</option>
                    <option value="weekly_refresh_desc">Weekly refresh: latest first</option>
                  </select>
                </label>
                <span className="pill">{accountCount} account{accountCount === 1 ? '' : 's'}</span>
              </div>
            </div>
            <div className="table-head">
              <span>Profile</span>
              <span>Rate Limits</span>
              <span>Rate Limit Reset</span>
              <span>Actions</span>
            </div>
            {accounts.length === 0 ? <div className="empty">No accounts found.</div> : null}
            {sortedAccounts.map((a) => {
              const primary = a.rate_limits?.requests || a.rate_limits?.primary
              const secondary = a.rate_limits?.tokens || a.rate_limits?.secondary
              const p1Raw = limitPercent(primary)
              const p2Raw = limitPercent(secondary)
              const p1 = p1Raw === null ? null : Math.max(0, Math.min(100, p1Raw))
              const p2 = p2Raw === null ? null : Math.max(0, Math.min(100, p2Raw))
              const rateError = typeof a.rate_limits?.error === 'string' ? a.rate_limits.error : ''
              const scopeError = rateError.includes('Missing scopes') ? 'Live rate-limit scopes are missing for this account.' : rateError
              const badge = refreshBadge(secondary, null, undefined)
              const refreshState = a.refresh_status?.state || 'idle'
              const refreshLabel =
                refreshState === 'refreshing'
                  ? 'Refreshing'
                  : refreshState === 'failed'
                    ? 'Failed'
                    : refreshState === 'ok'
                      ? 'Updated'
                      : 'Cached'
              return (
                <div className="row" key={`${a.account_key}:${a.label}`}>
                  <div>
                    <div className="profile-title">
                      <button className="profile-link-btn" onClick={() => void openAccountHistory(a.label)}>
                        {sensitiveText(a.display_label || a.label)}
                      </button>
                      <span className={`refresh-indicator ${refreshState}`}>
                        <span className="refresh-dot" />
                        {refreshLabel}
                      </span>
                      {badge ? <span className="pill" style={badge.style}>{badge.text}</span> : null}
                    </div>
                    <div className="muted account-meta-line">
                      <span>{privacyMode ? sensitiveText(a.email || 'email unavailable') : (a.email || 'email unavailable')}</span>
                      <span aria-hidden="true">·</span>
                      <span className="mono">{a.account_type || 'ChatGPT Plus'}</span>
                      <CodexBadge />
                    </div>
                    <div className="muted mono">Profile label: {sensitiveText(a.label)}</div>
                    {a.active_lease ? (
                      <div className="muted mono">
                        Leased to {a.active_lease.machine_id} / {a.active_lease.agent_id}
                      </div>
                    ) : null}
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
                    <div className="menu-root" onMouseLeave={() => setOpenMenuFor(null)}>
                      <button className="btn btn-sm" onClick={() => setOpenMenuFor(openMenuFor === a.label ? null : a.label)}>
                        Actions
                      </button>
                      {openMenuFor === a.label ? (
                        <div className="menu-panel">
                          <button className="menu-item" onClick={() => void switchAccount(a.label)} disabled={!hasActionApiKey}>{a.is_current ? 'Switch (Current)' : 'Switch'}</button>
                          <button className="menu-item" onClick={() => void renameAccount(a.label, a.display_label || a.label)} disabled={!hasActionApiKey}>Change profile label</button>
                          <button className="menu-item" onClick={() => void exportAccount(a.label)} disabled={!hasActionApiKey}>Export</button>
                          <button className="menu-item danger" onClick={() => void deleteAccount(a.label)} disabled={!hasActionApiKey}>Delete</button>
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
          <div className="aggregate-header">
            <h2>Aggregated Usage Analytics</h2>
            <p className="muted">
              {selectedRange === '1d'
                ? `${chartRangeLabel} usage since midnight${statsRangeMeta?.timezone ? ` (${statsRangeMeta.timezone})` : ''}.`
                : 'Consumption over time across all accounts.'}
            </p>
          </div>
          <div className="top-actions" style={{ marginBottom: 12 }}>
                {(['1d', '7d', '30d', '90d', 'all'] as RangeKey[]).map((r) => (
                  <button key={r} className={`btn btn-sm ${selectedRange === r ? 'primary' : ''}`} onClick={() => setSelectedRange(r)}>
                    {RANGE_LABELS[r]}
                  </button>
                ))}
            <button className={`btn btn-sm ${statsChartMode === 'cumulative' ? 'primary' : ''}`} onClick={() => setStatsChartMode('cumulative')}>Cumulative</button>
            <button className={`btn btn-sm ${statsChartMode === 'daily' ? 'primary' : ''}`} onClick={() => setStatsChartMode('daily')}>Daily</button>
          </div>
          {statsFallbackMode ? (
            <div className="fallback-banner">
              {statsModeledFallback
                ? 'Absolute usage counters unavailable. Showing modeled usage from 10-minute utilization snapshots.'
                : 'Absolute usage counters unavailable. Showing utilization-based fallback data.'}
            </div>
          ) : null}
          <div className="cards analytics-cards">
            <div>
              <label>{selectedRange === '1d' ? 'Used Today' : 'Consumed In Range'}</label>
              <div className="unit-value"><strong>{fmtNullableNumber(statsSummary?.total_consumed_in_range)}</strong></div>
              <div className="muted small">
                {statsModeledFallback
                  ? 'Modeled from utilization deltas since midnight'
                  : (statsFallbackMode
                      ? 'Unavailable in fallback mode'
                      : (selectedRange === '1d' ? 'Measured since local midnight' : 'Measured absolute usage'))}
              </div>
            </div>
            <div>
              <label>Current Used</label>
              <div className="unit-value"><strong>{fmtNullableNumber(statsSummary?.current_total_used)}</strong></div>
              <div className={`muted small ${pctClass(statsSummary?.weekly_utilization_now || 0)}`}>
                {statsSummary?.weekly_utilization_now == null ? 'No live utilization snapshot' : `${statsSummary.weekly_utilization_now}% weekly utilization now`}
              </div>
            </div>
            <div>
              <label>Current Remaining</label>
              <div className="unit-value"><strong>{fmtNullableNumber(statsSummary?.current_total_remaining)}</strong></div>
              <div className="muted small">
                {statsModeledFallback
                  ? 'Normalized remaining capacity across accounts'
                  : (statsFallbackMode ? 'Unavailable in fallback mode' : 'Current remaining across accounts')}
              </div>
            </div>
            <div className={(aggregate.fleet_efficiency_pct < 80 && !statsFallbackMode) ? 'warn-card' : ''}>
              <label>Current Limit</label>
              <div className="unit-value"><strong>{fmtNullableNumber(statsSummary?.current_total_limit)}</strong></div>
              <div className="muted small">
                {statsModeledFallback
                  ? 'Normalized capacity at 100 units per account'
                  : (statsFallbackMode ? 'Unavailable in fallback mode' : 'Current total limit across accounts')}
              </div>
            </div>
            <div className={aggregate.total_wasted_units > 0 ? 'warn-card' : ''}>
              <label>Quota Leakage</label>
              <div className="unit-value"><strong>{statsSummary?.total_wasted ?? 0}</strong> <span className="u">CU</span></div>
              <div className="muted small">Wasted at weekly reset</div>
            </div>
            <div><label>Stale Accounts</label><strong>{statsSummary?.stale_account_count ?? aggregate.stale_accounts}</strong></div>
            <div><label>Failed Accounts</label><strong>{statsSummary?.failed_account_count ?? aggregate.failed_accounts}</strong></div>
            <div><label>{statsSummary?.last_refresh_label || 'Last Refresh'}</label><strong>{fmtTs(statsSummary?.last_refresh_time || aggregate.last_refresh_time)}</strong></div>
          </div>
          <div className="graph-container">
            <div className="graph-label">
              {statsModeledFallback
                ? `${chartRangeLabel} Modeled Consumption`
                : (statsFallbackMode
                  ? `${chartRangeLabel} Utilization Trend (Fallback)`
                  : (statsChartMode === 'cumulative'
                    ? `${chartRangeLabel} Cumulative Consumption`
                    : `${chartRangeLabel} Consumption`))}
            </div>
            <div className="chart-legend">
              <span className="legend-item">
                <span className="legend-dot legend-dot-teal" />
                {statsModeledFallback
                  ? 'Usage line = normalized units reconstructed from utilization snapshots (100 per account)'
                  : (statsFallbackMode
                      ? 'Usage line = weekly utilization % from stored snapshots (absolute counters unavailable)'
                      : 'Usage line = consumed units (from lifetime deltas), not utilization %')}
              </span>
              <span className="legend-item">
                <span className="legend-dot legend-dot-amber" />
                Weekly utilization now: {weeklyUtilizationNow == null ? '--' : `${weeklyUtilizationNow}%`}
                {weeklyPercents.length ? ` (${weeklyAtCapCount}/${weeklyPercents.length} at 100%)` : ''}
              </span>
            </div>
            <div className="graph">
              {statsPrimarySeries.length ? (
                <svg viewBox="0 0 1000 220" preserveAspectRatio="none">
                  <line x1="24" y1="196" x2="976" y2="196" stroke="#334155" strokeWidth="1" />
                  <path
                    d={buildLinePath(statsPrimarySeries.map((p: any) => Number(p.value ?? p.cumulative ?? p.consumed ?? 0)))}
                    fill="none"
                    stroke="#10b981"
                    strokeWidth="3"
                    strokeLinecap="round"
                    strokeLinejoin="round"
                  />
                  {statsPrimarySeries.map((point: any, i: number) => {
                    const value = Number(point.value ?? point.cumulative ?? point.consumed ?? 0)
                    const x = 24 + (i / Math.max(statsPrimarySeries.length - 1, 1)) * (1000 - 48)
                    const y = 196 - (value / statsMaxValue) * (220 - 48)
                    return <circle key={`${point.day ?? point.t ?? i}-${value}-${i}`} cx={x} cy={y} r="3" fill="#10b981" />
                  })}
                </svg>
              ) : <div className="muted">No usage history yet for this range.</div>}
            </div>
          </div>
          <div className="graph-container">
            <div className="graph-label">Wasted At Weekly Rollover (Over Time)</div>
            <div className="graph">
              {wastedSeries.length ? (
                <svg viewBox="0 0 1000 220" preserveAspectRatio="none">
                  <line x1="24" y1="196" x2="976" y2="196" stroke="#334155" strokeWidth="1" />
                  {wastedSeries.map((point, i) => {
                    const barW = (1000 - 48) / Math.max(wastedSeries.length, 1)
                    const x = 24 + i * barW + 2
                    const value = Number(point.value || 0)
                    const h = ((220 - 48) * value) / wastedMaxValue
                    const y = 196 - h
                    return (
                      <rect
                        key={`w-${point.day}-${i}`}
                        x={x}
                        y={y}
                        width={Math.max(2, barW - 4)}
                        height={Math.max(1, h)}
                        rx={2}
                        fill="#f59e0b"
                        opacity="0.9"
                      />
                    )
                  })}
                </svg>
              ) : <div className="muted">No rollover wastage history yet.</div>}
            </div>
          </div>

          <div className="cards" style={{ marginTop: 16 }}>
            <div>
              <label>{statsModeledFallback ? 'Top Consuming Accounts (Modeled)' : 'Top Consuming Accounts'}</label>
              {usageHistory?.sections.top_consuming_accounts_available === false ? (
                <div className="muted">Unavailable while using utilization fallback data.</div>
              ) : (
                (usageHistory?.sections.top_consuming_accounts || []).slice(0, 5).map((item) => (
                  <div key={item.account_key} className="muted" style={{ marginTop: 4 }}>
                    {sensitiveText(item.display_label || item.label)}: <span className="mono">{item.consumed}</span>
                  </div>
                ))
              )}
            </div>
            <div>
              <label>Stale / Failed Accounts</label>
              <div className="muted">Stale: {(usageHistory?.sections.stale_accounts || []).length}</div>
              <div className="muted">Failed: {(usageHistory?.sections.failed_accounts || []).length}</div>
            </div>
            <div>
              <label>Recent Rollover Events</label>
              {(usageHistory?.sections.recent_rollovers || []).slice(0, 5).map((item, idx) => (
                <div key={`r-${idx}`} className="muted" style={{ marginTop: 4 }}>
                  {sensitiveText(item.display_label || item.label)} · {fmtTs(item.window_ended_at || item.rolled_over_at)}
                </div>
              ))}
            </div>
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
                <div className="muted"><strong>{sensitiveText(historyData.display_label || historyData.label)}</strong> · {privacyMode ? sensitiveText(historyData.email || 'email unavailable') : (historyData.email || 'email unavailable')}</div>
                <div className="muted" style={{ marginTop: 4 }}>
                  {accountHistoryRange === '1d'
                    ? `${rangeLabel(accountHistoryRange, historyData.range_metadata)} since midnight${historyData.range_metadata?.timezone ? ` (${historyData.range_metadata.timezone})` : ''}.`
                    : `Usage history for ${rangeLabel(accountHistoryRange, historyData.range_metadata)}.`}
                </div>

                <div className="top-actions" style={{ marginTop: 10 }}>
                  {(['1d', '7d', '30d', '90d', 'all'] as RangeKey[]).map((r) => (
                    <button key={r} className={`btn btn-sm ${accountHistoryRange === r ? 'primary' : ''}`} onClick={() => void reloadAccountHistory(r)}>
                      {RANGE_LABELS[r]}
                    </button>
                  ))}
                  <button className={`btn btn-sm ${accountChartMode === 'cumulative' ? 'primary' : ''}`} onClick={() => setAccountChartMode('cumulative')}>Cumulative</button>
                  <button className={`btn btn-sm ${accountChartMode === 'daily' ? 'primary' : ''}`} onClick={() => setAccountChartMode('daily')}>Daily</button>
                </div>

                <div className="cards analytics-cards" style={{ marginTop: 12 }}>
                  {(() => {
                    const accountFallbackMode = Boolean(historyData.summary?.fallback_mode || historyData.consumption_trend.fallback_mode)
                    const accountModeledFallback = Boolean(
                      historyData.summary?.modeled_usage_basis || historyData.consumption_trend.modeled_usage_basis,
                    )
                    return (
                      <>
                  <div>
                    <label>Account Type</label>
                    <div className="unit-value">
                      <strong>{historyData.account_type || 'ChatGPT Plus'}</strong>
                      <CodexBadge />
                    </div>
                    <div className="muted small">Plan category</div>
                  </div>
                  <div>
                    <label>{accountHistoryRange === '1d' ? 'Used Today' : 'Consumed In Range'}</label>
                    <div className="unit-value"><strong>{fmtNullableNumber(historyData.summary?.total_consumed_in_range ?? historyData.consumption_trend.total_consumed_in_range)}</strong></div>
                    <div className="muted small">
                      {accountModeledFallback
                        ? 'Modeled from utilization deltas in stored snapshots'
                        : (accountFallbackMode ? 'Unavailable in fallback mode' : 'Measured absolute usage')}
                    </div>
                  </div>
                  <div>
                    <label>Current Used</label>
                    <div className="unit-value"><strong>{fmtNullableNumber(historyData.summary?.current_total_used ?? historyData.current_state.usage_in_window)}</strong></div>
                    <div className="muted small">
                      {historyData.summary?.weekly_utilization_now == null ? 'No live utilization snapshot' : `${historyData.summary.weekly_utilization_now}% weekly utilization now`}
                    </div>
                  </div>
                  <div>
                    <label>Current Remaining</label>
                    <div className="unit-value"><strong>{fmtNullableNumber(historyData.summary?.current_total_remaining ?? historyData.current_state.remaining)}</strong></div>
                    <div className="muted small">
                      {accountModeledFallback
                        ? 'Normalized remaining capacity'
                        : (accountFallbackMode ? 'Unavailable in fallback mode' : 'Current remaining')}
                    </div>
                  </div>
                  <div>
                    <label>Current Limit</label>
                    <div className="unit-value"><strong>{fmtNullableNumber(historyData.summary?.current_total_limit ?? historyData.current_state.usage_limit)}</strong></div>
                    <div className="muted small">
                      {accountModeledFallback
                        ? 'Normalized capacity fixed at 100 units'
                        : (accountFallbackMode ? 'Unavailable in fallback mode' : 'Current limit')}
                    </div>
                  </div>
                  <div className={typeof historyData.current_state.efficiency_pct === 'number' && historyData.current_state.efficiency_pct < 80 ? 'warn-card' : ''}>
                    <label>Account Efficiency</label>
                    <div className="unit-value"><strong>{fmtNullableNumber(historyData.current_state.efficiency_pct, '%')}</strong></div>
                    <div className="muted small">
                      {accountFallbackMode ? 'Unavailable in fallback mode' : 'Used vs Waste'}
                    </div>
                  </div>
                  <div><label>Lifetime Used</label><strong>{fmtNullableNumber(historyData.current_state.lifetime_used)}</strong></div>
                  <div><label>Next 5hr Reset</label><strong>{fmtTs(historyData.current_state.next_reset || null)}</strong></div>
                  <div><label>{historyData.summary?.last_refresh_label || 'Last Refresh'}</label><strong>{fmtTs(historyData.summary?.last_refresh_time || historyData.current_state.last_sync || null)}</strong></div>
                      </>
                    )
                  })()}
                </div>

                <div className="graph-container" style={{ marginTop: 16 }}>
                  {historyData.summary?.fallback_mode || historyData.consumption_trend.fallback_mode ? (
                    <div className="fallback-banner" style={{ marginBottom: 12 }}>
                      {historyData.summary?.modeled_usage_basis || historyData.consumption_trend.modeled_usage_basis
                        ? 'Absolute usage counters unavailable. Showing modeled usage from 10-minute utilization snapshots.'
                        : 'Absolute usage counters unavailable. Showing utilization-based fallback data.'}
                    </div>
                  ) : null}
                  <div className="graph-label">
                    {((historyData.summary?.modeled_usage_basis || historyData.consumption_trend.modeled_usage_basis))
                      ? `${rangeLabel(accountHistoryRange, historyData.range_metadata)} Modeled Consumption`
                      : ((historyData.summary?.fallback_mode || historyData.consumption_trend.fallback_mode)
                        ? `${rangeLabel(accountHistoryRange, historyData.range_metadata)} Utilization Trend (Fallback)`
                      : (accountChartMode === 'cumulative'
                          ? `${rangeLabel(accountHistoryRange, historyData.range_metadata)} Cumulative Consumption`
                          : `${rangeLabel(accountHistoryRange, historyData.range_metadata)} Consumption`))}
                  </div>
                  <div className="graph">
                    {(() => {
                      const accountFallbackMode = Boolean(historyData.summary?.fallback_mode || historyData.consumption_trend.fallback_mode)
                      const accountModeledFallback = Boolean(
                        historyData.summary?.modeled_usage_basis || historyData.consumption_trend.modeled_usage_basis,
                      )
                      const points = accountModeledFallback
                        ? (accountChartMode === 'cumulative'
                            ? historyData.consumption_trend.cumulative_usage
                            : historyData.consumption_trend.daily_usage)
                        : (accountFallbackMode
                            ? (accountHistoryRange === '1d'
                                ? (historyData.consumption_trend.hourly_weekly_utilization || [])
                                : (historyData.consumption_trend.daily_weekly_utilization || []))
                            : (accountChartMode === 'cumulative'
                                ? historyData.consumption_trend.cumulative_usage
                                : historyData.consumption_trend.daily_usage))
                      if (!points.length) return <div className="muted">No history yet for this range.</div>
                      const values = points.map((p: any) => Number((p as any).value ?? (p as any).cumulative ?? (p as any).consumed ?? 0))
                      const maxVal = Math.max(0, ...values)
                      if (maxVal <= 0) {
                        return <div className="muted">No measurable consumption in this range yet.</div>
                      }
                      const chartMax = (accountFallbackMode && !accountModeledFallback) ? 100 : Math.max(1, maxVal)
                      const width = 1000
                      const height = 220
                      const pad = 24
                      const usableW = width - pad * 2
                      const usableH = height - pad * 2
                      const path = points
                        .map((p: any, i: number) => {
                          const value = Number((p as any).value ?? (p as any).cumulative ?? (p as any).consumed ?? 0)
                          const x = pad + (i / Math.max(points.length - 1, 1)) * usableW
                          const y = height - pad - (value / chartMax) * usableH
                          return `${i === 0 ? 'M' : 'L'} ${x.toFixed(2)} ${y.toFixed(2)}`
                        })
                        .join(' ')
                      return (
                        <svg viewBox="0 0 1000 220" preserveAspectRatio="none">
                          <line x1="24" y1="196" x2="976" y2="196" stroke="#334155" strokeWidth="1" />
                          <path
                            d={path}
                            fill="none"
                            stroke="#10b981"
                            strokeWidth="3"
                            strokeLinecap="round"
                            strokeLinejoin="round"
                          />
                          {points.map((p: any, i: number) => {
                            const value = Number((p as any).value ?? (p as any).cumulative ?? (p as any).consumed ?? 0)
                            const x = 24 + (i / Math.max(points.length - 1, 1)) * (1000 - 48)
                            const y = 196 - (value / chartMax) * (220 - 48)
                            return <circle key={`${p.day ?? p.t ?? i}-${value}-${i}`} cx={x} cy={y} r="3" fill="#10b981" />
                          })}
                        </svg>
                      )
                    })()}
                  </div>
                </div>

                <div className="history-table">
                  <div className="history-head history-head-7">
                    <span>Window Start</span>
                    <span>Window End</span>
                    <span>Used</span>
                    <span>Limit</span>
                    <span>Wasted</span>
                    <span>Utilization</span>
                    <span>Rolled Over</span>
                  </div>
                  {historyData.completed_windows?.length ? historyData.completed_windows.slice(0, 30).map((r, idx) => (
                    <div key={idx} className="history-row history-row-7">
                      <span>{fmtTs(r.window_start || '')}</span>
                      <span>{fmtTs(r.window_end || '')}</span>
                      <span className="mono">{r.used}</span>
                      <span className="mono">{r.limit}</span>
                      <span className="mono">{r.wasted}</span>
                      <span className="mono">{r.utilization_percent == null ? '--' : `${r.utilization_percent}%`}</span>
                      <span>{fmtTs(r.rolled_over_at || '')}</span>
                    </div>
                  )) : <div className="muted" style={{ padding: 12 }}>No completed windows yet.</div>}
                </div>
              </div>
            ) : null}
          </div>
        </div>
      ) : null}

      {apiKeyModalOpen ? (
        <div className="modal-overlay" onClick={() => setApiKeyModalOpen(false)}>
          <div className="modal-card" onClick={(e) => e.stopPropagation()}>
            <div className="modal-head">
              <h3>API Key</h3>
              <button className="btn btn-sm" onClick={() => setApiKeyModalOpen(false)}>Close</button>
            </div>
            <p className="muted" style={{ marginTop: 0 }}>
              Required for refresh, import, rename, delete, and export. Read-only viewing and Add Account work without it.
            </p>
            <input
              type="password"
              value={apiKeyInput}
              onChange={(e) => setApiKeyInput(e.target.value)}
              placeholder="Paste API key"
              className="api-key-input"
            />
            <div className="top-actions" style={{ marginTop: 12 }}>
              <button className="btn primary" onClick={saveActionApiKey}>Save</button>
              <button
                className="btn"
                onClick={() => {
                  setApiKeyInput('')
                  setActionApiKey('')
                  localStorage.removeItem(ACTION_API_KEY_STORAGE)
                  setStatus('API key cleared (read-only mode)')
                  setApiKeyModalOpen(false)
                }}
              >
                Clear
              </button>
            </div>
          </div>
        </div>
      ) : null}

      {settingsModalOpen ? (
        <div className="modal-overlay" onClick={() => setSettingsModalOpen(false)}>
          <div className="modal-card" onClick={(e) => e.stopPropagation()}>
            <div className="modal-head">
              <h3>Settings</h3>
              <button className="btn btn-sm" onClick={() => setSettingsModalOpen(false)}>Close</button>
            </div>
            <div className="settings-section">
              <div>
                <div className="settings-title">Privacy Mode</div>
                <div className="muted">
                  Redact sensitive account details across the dashboard, including emails, account names, and profile labels.
                </div>
              </div>
              <button className={`btn ${privacyMode ? 'primary' : ''}`} onClick={togglePrivacyMode}>
                {privacyMode ? 'On' : 'Off'}
              </button>
            </div>
          </div>
        </div>
      ) : null}

      {addAccountModalOpen ? (
        <div className="modal-overlay" onClick={() => setAddAccountModalOpen(false)}>
          <div className="modal-card" onClick={(e) => e.stopPropagation()}>
            <div className="modal-head">
              <h3>Add Account</h3>
              <button className="btn btn-sm" onClick={() => setAddAccountModalOpen(false)}>Close</button>
            </div>
            <p className="muted" style={{ marginTop: 0 }}>
              Use this manual Add Account flow for same-device or cross-device login. Auth Manager will pass the pasted callback to Codex CLI, then save a new profile or update the matching existing profile based on the finalized auth identity.
            </p>
            {addAuthUrl ? (
              <div className="manual-auth-panel">
                <div className="panel-title" style={{ marginBottom: 8 }}>Manual Auth Steps</div>
                <ol className="manual-auth-steps">
                  <li>Copy the auth link below.</li>
                  <li>Paste it into a browser on any machine and sign in with OpenAI.</li>
                  <li>Copy the full callback URL from the browser address bar.</li>
                  <li>Paste that callback URL into the field below and submit it.</li>
                </ol>
                <div className="manual-auth-link-row">
                  <pre className="manual-auth-link">{addAuthUrl}</pre>
                  <div className="top-actions">
                    <button
                      className="btn"
                      onClick={() => {
                        void copyText(addAuthUrl).then((ok) => {
                          setStatus(ok ? 'Auth URL copied to clipboard.' : 'Unable to copy auth URL automatically.')
                        })
                      }}
                    >
                      Copy Auth Link
                    </button>
                    <a className="btn" href={addAuthUrl} target="_blank" rel="noreferrer">Open Link</a>
                  </div>
                </div>
              </div>
            ) : null}
            {addAccountFeedback ? (
              <div className={`inline-feedback ${addAccountFeedbackTone}`}>
                {addAccountFeedback}
              </div>
            ) : null}
            <textarea
              className="auth-json-input"
              value={addCallbackUrl}
              onChange={(e) => setAddCallbackUrl(e.target.value)}
              placeholder="Paste full callback URL (http://localhost.../auth/callback?... )"
              rows={5}
            />
            <input
              type="text"
              value={addLabelInput}
              onChange={(e) => setAddLabelInput(e.target.value)}
              placeholder="Optional profile label"
              className="api-key-input"
              style={{ marginTop: 10 }}
            />
            <div className="top-actions" style={{ marginTop: 12 }}>
              <button className="btn primary" onClick={() => void submitAddAccountCallback()} disabled={addAccountLoading}>
                {addAccountLoading ? 'Submitting...' : 'Submit Callback'}
              </button>
              <button className="btn" onClick={() => setAddAccountModalOpen(false)}>Cancel</button>
            </div>
          </div>
        </div>
      ) : null}

      {importAuthModalOpen ? (
        <div className="modal-overlay" onClick={() => setImportAuthModalOpen(false)}>
          <div className="modal-card" onClick={(e) => e.stopPropagation()}>
            <div className="modal-head">
              <h3>Import Auth</h3>
              <button className="btn btn-sm" onClick={() => setImportAuthModalOpen(false)}>Close</button>
            </div>
            <p className="muted" style={{ marginTop: 0 }}>
              Paste auth JSON directly or upload a JSON file. The auth will be added to the system and matched/saved to a profile.
            </p>
            <textarea
              className="auth-json-input"
              value={importAuthText}
              onChange={(e) => setImportAuthText(e.target.value)}
              placeholder='Paste auth JSON here (raw auth object or wrapper containing "authJson")'
              rows={10}
            />
            <div style={{ marginTop: 10 }}>
              <input
                type="file"
                accept="application/json,.json"
                onChange={(e) => void onImportAuthFile(e.target.files?.[0] || null)}
              />
            </div>
            <input
              type="text"
              value={importAuthLabel}
              onChange={(e) => setImportAuthLabel(e.target.value)}
              placeholder="Optional profile label"
              className="api-key-input"
              style={{ marginTop: 10 }}
            />
            <div className="top-actions" style={{ marginTop: 12 }}>
              <button className="btn primary" onClick={() => void importAuthFromModal()} disabled={importAuthLoading}>
                {importAuthLoading ? 'Importing...' : 'Import Auth'}
              </button>
              <button className="btn" onClick={() => setImportAuthModalOpen(false)}>Cancel</button>
            </div>
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
