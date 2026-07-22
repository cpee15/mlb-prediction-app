import React, { useEffect, useMemo, useState } from 'react'

const API = import.meta.env.VITE_API_BASE_URL || ''
const DASHBOARD_SESSION_STORAGE_KEY = 'mlbgpt_dashboard_session_token'
const CACHE_PREFIX = 'my-dashboard-workbench:v1:'
const ACTIVE_OBJECT_KEY = 'my-dashboard-workbench:active-object'
const SELECTED_FIELDS_KEY = 'my-dashboard-workbench:selected-fields'

const OBJECTS = [
  {
    key: 'hitters',
    label: 'Hitters',
    apiLabel: 'My Top Hitters Today',
    description: 'Stored 365 Batter vs Arsenal board with pitch usage, xwOBA, EV, LA, hard-hit quality, and model context.',
    type: 'Player object',
  },
  {
    key: 'pitchers',
    label: 'Pitchers',
    apiLabel: 'My Top Pitchers Today',
    description: 'Pitcher lean board using K profile, contact suppression, opponent offense, and arsenal context.',
    type: 'Player object',
  },
  {
    key: 'teams',
    label: 'Teams',
    apiLabel: 'My Top Teams Today',
    description: 'Team board from model side edge, expected runs, offense profile, and opponent weaknesses.',
    type: 'Team object',
  },
  {
    key: 'totals',
    label: 'Totals',
    apiLabel: 'My Top Totals Today',
    description: 'Game total watchlist from projected runs, run environment, and simulation context.',
    type: 'Game object',
  },
  {
    key: 'overall_players',
    label: 'Overall Players',
    apiLabel: 'My Top Overall Players Today',
    description: 'Combined player board blending hitter and pitcher model-solver scores.',
    type: 'Union object',
  },
]

const ACTIVE_LINEUP_OBJECTS = new Set(['hitters', 'overall_players'])
const BASE_FIELD_DEFS = [
  { accessor: 'rank', label: 'Rank', group: 'Identity' },
  { accessor: 'entity_name', label: 'Name', group: 'Identity' },
  { accessor: 'entity_id', label: 'Entity ID', group: 'Identity' },
  { accessor: 'entity_type', label: 'Entity Type', group: 'Identity' },
  { accessor: 'player_type', label: 'Player Type', group: 'Identity' },
  { accessor: 'team', label: 'Team', group: 'Matchup' },
  { accessor: 'opponent', label: 'Opponent', group: 'Matchup' },
  { accessor: 'game_pk', label: 'Game PK', group: 'Matchup' },
  { accessor: 'pitch_type', label: 'Pitch Type', group: 'Matchup' },
  { accessor: 'pitch_name', label: 'Pitch Name', group: 'Matchup' },
  { accessor: 'category', label: 'Category', group: 'Filterable' },
  { accessor: 'score', label: 'Score', group: 'Scoring' },
  { accessor: 'base_score', label: 'Base Score', group: 'Scoring' },
  { accessor: 'adjusted_score', label: 'Adjusted Score', group: 'Scoring' },
  { accessor: 'confidence', label: 'Confidence', group: 'Scoring' },
  { accessor: 'source', label: 'Source', group: 'Audit' },
  { accessor: 'primary_reason', label: 'Primary Reason', group: 'Audit' },
  { accessor: 'lineup_verified', label: 'Lineup Verified', group: 'Audit' },
  { accessor: 'lineup_source', label: 'Lineup Source', group: 'Audit' },
]

const DEFAULT_SELECTED_FIELDS = ['rank', 'entity_name', 'team', 'opponent', 'score', 'confidence']
const DEFAULT_METRICS = {
  hitters: ['xwOBA', 'xBA', 'EV', 'LA', 'HardHit', 'Usage', 'Pitcher xWoba', 'Pitches Seen', 'PA'],
  pitchers: ['K%', 'BB%', 'xwOBA Allowed', 'HardHit Allowed', 'Opp K%', 'Opp ISO', 'Score'],
  teams: ['Edge Score', 'Win Edge', 'Run Diff', 'ISO', 'OBP', 'SLG'],
  totals: ['Projected Total', 'Raw Total', 'Run Index', 'Score'],
  overall_players: ['Score', 'xwOBA', 'EV', 'K%', 'xwOBA Allowed'],
}

const C = {
  bg: '#08111f',
  panel: '#111827',
  panel2: '#0f172a',
  panel3: '#172033',
  border: 'rgba(148, 163, 184, 0.2)',
  text: '#e5edf8',
  muted: '#94a3b8',
  subtle: '#64748b',
  blue: '#60a5fa',
  green: '#34d399',
  amber: '#fbbf24',
  red: '#f87171',
}

function todayIso() {
  return new Date().toISOString().slice(0, 10)
}

function safeArray(value) {
  return Array.isArray(value) ? value : []
}

function clone(value) {
  return JSON.parse(JSON.stringify(value))
}

function readSessionToken() {
  if (typeof window === 'undefined') return ''
  return window.localStorage.getItem(DASHBOARD_SESSION_STORAGE_KEY) || ''
}

function writeSessionToken(token) {
  if (typeof window === 'undefined') return
  if (token) window.localStorage.setItem(DASHBOARD_SESSION_STORAGE_KEY, token)
  else window.localStorage.removeItem(DASHBOARD_SESSION_STORAGE_KEY)
}

function readLocalJson(key, fallback) {
  if (typeof window === 'undefined') return fallback
  try {
    const raw = window.localStorage.getItem(key)
    return raw ? JSON.parse(raw) : fallback
  } catch {
    return fallback
  }
}

function writeLocalJson(key, value) {
  if (typeof window === 'undefined') return
  try { window.localStorage.setItem(key, JSON.stringify(value)) } catch {}
}

function cacheKey(kind, payload) {
  return `${CACHE_PREFIX}${kind}:${JSON.stringify(payload)}`
}

function readSessionCache(key) {
  if (typeof window === 'undefined') return null
  try {
    const raw = window.sessionStorage.getItem(key)
    return raw ? JSON.parse(raw) : null
  } catch {
    return null
  }
}

function writeSessionCache(key, value) {
  if (typeof window === 'undefined') return
  try { window.sessionStorage.setItem(key, JSON.stringify(value)) } catch {}
}

function emptyFilters() {
  return {
    search_text: '',
    team: '',
    opponent: '',
    min_score: '',
    max_score: '',
    min_confidence: '',
    category: '',
    player_type: '',
    pitch_type: '',
    source: '',
    metrics: {},
    weights: {},
  }
}

function defaultFilters() {
  return Object.fromEntries(OBJECTS.map(object => [object.key, emptyFilters()]))
}

function cleanFilters(filters) {
  const cleaned = {}
  Object.entries(filters || {}).forEach(([key, value]) => {
    if (key === 'metrics' || key === 'weights') return
    if (value !== '' && value !== null && value !== undefined) cleaned[key] = value
  })
  const metrics = {}
  Object.entries(filters?.metrics || {}).forEach(([metric, rules]) => {
    const entry = {}
    if (rules?.min !== '' && rules?.min !== null && rules?.min !== undefined) entry.min = Number(rules.min)
    if (rules?.max !== '' && rules?.max !== null && rules?.max !== undefined) entry.max = Number(rules.max)
    if (Object.keys(entry).length) metrics[metric] = entry
  })
  if (Object.keys(metrics).length) cleaned.metrics = metrics
  const weights = {}
  Object.entries(filters?.weights || {}).forEach(([metric, value]) => {
    if (value === '' || value === null || value === undefined) return
    const num = Number(value)
    if (Number.isFinite(num) && num !== 1) weights[metric] = num
  })
  if (Object.keys(weights).length) cleaned.weights = weights
  return cleaned
}

function getValueByPath(row, accessor) {
  if (!row || !accessor) return null
  if (accessor.startsWith('metrics.')) return row.metrics?.[accessor.slice('metrics.'.length)] ?? null
  return accessor.split('.').reduce((acc, key) => acc == null ? null : acc[key], row)
}

function formatCell(value) {
  if (value === null || value === undefined || value === '') return '—'
  if (typeof value === 'number') return Math.abs(value) >= 10 ? value.toFixed(1) : value.toFixed(3)
  if (typeof value === 'boolean') return value ? 'Yes' : 'No'
  if (Array.isArray(value)) return value.slice(0, 3).join(', ')
  if (typeof value === 'object') return JSON.stringify(value)
  return String(value)
}

function uniqueFields(fields) {
  const seen = new Set()
  return fields.filter(field => {
    if (!field?.accessor || seen.has(field.accessor)) return false
    seen.add(field.accessor)
    return true
  })
}

function titleCase(value) {
  return String(value || '')
    .replace(/[_-]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .replace(/\b\w/g, ch => ch.toUpperCase())
}

function fieldsForResult(result, objectKey) {
  const available = result?.available_filters || {}
  const fromItems = new Set()
  safeArray(result?.items).forEach(item => {
    Object.keys(item || {}).forEach(key => {
      if (!['chart_data', 'reasoning', 'missing_data', 'best_pitch_angles', 'metrics'].includes(key)) fromItems.add(key)
    })
    Object.keys(item?.metrics || {}).forEach(metric => fromItems.add(`metrics.${metric}`))
  })
  const metricFields = (available.metrics?.length ? available.metrics : DEFAULT_METRICS[objectKey] || []).map(metric => ({ accessor: `metrics.${metric}`, label: metric, group: 'Metrics' }))
  const runtimeFields = Array.from(fromItems).map(accessor => ({ accessor, label: accessor.startsWith('metrics.') ? accessor.slice(8) : titleCase(accessor), group: accessor.startsWith('metrics.') ? 'Metrics' : 'Runtime' }))
  return uniqueFields([...BASE_FIELD_DEFS, ...metricFields, ...runtimeFields])
}

function useViewport() {
  const [width, setWidth] = useState(() => typeof window === 'undefined' ? 1200 : window.innerWidth)
  useEffect(() => {
    function onResize() { setWidth(window.innerWidth) }
    window.addEventListener('resize', onResize)
    return () => window.removeEventListener('resize', onResize)
  }, [])
  return { width, isMobile: width < 760, isNarrow: width < 1080 }
}

function StatusPill({ children, tone = 'default' }) {
  const color = tone === 'success' ? C.green : tone === 'warning' ? C.amber : tone === 'danger' ? C.red : C.blue
  return <span style={{ ...styles.pill, color, borderColor: `${color}55`, background: `${color}16` }}>{children}</span>
}

function ObjectRail({ objects, activeKey, setActiveKey, results, loading, isMobile }) {
  return (
    <aside style={isMobile ? styles.mobileObjectRail : styles.objectRail}>
      <div style={styles.railHeader}>
        <div style={styles.eyebrow}>Object Manager</div>
        <div style={styles.railTitle}>Dashboard Objects</div>
      </div>
      <div style={isMobile ? styles.mobileObjectList : styles.objectList}>
        {objects.map(object => {
          const count = safeArray(results[object.key]?.items).length
          const active = object.key === activeKey
          return (
            <button key={object.key} type="button" onClick={() => setActiveKey(object.key)} style={active ? styles.objectButtonActive : styles.objectButton}>
              <span>
                <strong>{object.label}</strong>
                {!isMobile ? <small>{object.type}</small> : null}
              </span>
              <span style={styles.objectCount}>{loading[object.key] ? '…' : count}</span>
            </button>
          )
        })}
      </div>
    </aside>
  )
}

function AuthGate({ authChecked, profile, form, setForm, saving, error, onSubmit }) {
  if (!authChecked) return <div style={styles.stateView}>Loading My Dashboard shell…</div>
  if (profile) return null
  return (
    <main style={styles.authPage}>
      <section style={styles.authCard}>
        <div style={styles.eyebrow}>My Dashboard Workbench</div>
        <h1 style={styles.authTitle}>Create your analyst profile</h1>
        <p style={styles.authCopy}>Use the existing dashboard session system so saved reports, folders, and Workbench views remain tied to your account.</p>
        <form onSubmit={onSubmit} style={styles.authForm}>
          <input required style={styles.input} placeholder="Email" value={form.email} onChange={e => setForm(prev => ({ ...prev, email: e.target.value }))} />
          <input required style={styles.input} placeholder="Username" value={form.username} onChange={e => setForm(prev => ({ ...prev, username: e.target.value }))} />
          <input style={styles.input} placeholder="Password" type="password" value={form.password} onChange={e => setForm(prev => ({ ...prev, password: e.target.value }))} />
          {error ? <div style={styles.errorBanner}>{error}</div> : null}
          <button type="submit" style={styles.primaryButton} disabled={saving}>{saving ? 'Creating…' : 'Enter Workbench'}</button>
        </form>
      </section>
    </main>
  )
}

function WorkbenchFilters({ objectKey, filters, setBasicFilter, setMetricFilter, setWeight, availableFields, runBoard, loading }) {
  const metricFields = availableFields.filter(field => field.accessor.startsWith('metrics.')).slice(0, 10)
  return (
    <section style={styles.card}>
      <div style={styles.cardHeader}>
        <div>
          <div style={styles.cardTitle}>Filters</div>
          <div style={styles.cardSubtitle}>Flexible field criteria connected to the existing dashboard filters.</div>
        </div>
        <button type="button" onClick={() => runBoard(objectKey)} style={styles.primaryButton} disabled={loading}>{loading ? 'Running…' : 'Run'}</button>
      </div>
      <div style={styles.filterGrid}>
        <input style={styles.input} placeholder="Search text" value={filters.search_text || ''} onChange={e => setBasicFilter(objectKey, 'search_text', e.target.value)} />
        <input style={styles.input} placeholder="Team contains" value={filters.team || ''} onChange={e => setBasicFilter(objectKey, 'team', e.target.value)} />
        <input style={styles.input} placeholder="Opponent contains" value={filters.opponent || ''} onChange={e => setBasicFilter(objectKey, 'opponent', e.target.value)} />
        <select style={styles.input} value={filters.min_confidence || ''} onChange={e => setBasicFilter(objectKey, 'min_confidence', e.target.value)}>
          <option value="">Any confidence</option>
          <option value="low">Low+</option>
          <option value="medium">Medium+</option>
          <option value="high">High only</option>
        </select>
        <input style={styles.input} placeholder="Min score" inputMode="decimal" value={filters.min_score || ''} onChange={e => setBasicFilter(objectKey, 'min_score', e.target.value)} />
        <input style={styles.input} placeholder="Max score" inputMode="decimal" value={filters.max_score || ''} onChange={e => setBasicFilter(objectKey, 'max_score', e.target.value)} />
        <input style={styles.input} placeholder="Category exact" value={filters.category || ''} onChange={e => setBasicFilter(objectKey, 'category', e.target.value)} />
        <input style={styles.input} placeholder="Pitch type/name" value={filters.pitch_type || ''} onChange={e => setBasicFilter(objectKey, 'pitch_type', e.target.value)} />
      </div>
      <div style={styles.sectionLabel}>Metric thresholds</div>
      <div style={styles.metricEditorGrid}>
        {metricFields.map(field => {
          const metric = field.accessor.slice('metrics.'.length)
          return (
            <div key={`metric-${metric}`} style={styles.metricEditor}>
              <strong>{field.label}</strong>
              <input style={styles.miniInput} placeholder="min" inputMode="decimal" value={filters.metrics?.[metric]?.min || ''} onChange={e => setMetricFilter(objectKey, metric, 'min', e.target.value)} />
              <input style={styles.miniInput} placeholder="max" inputMode="decimal" value={filters.metrics?.[metric]?.max || ''} onChange={e => setMetricFilter(objectKey, metric, 'max', e.target.value)} />
            </div>
          )
        })}
      </div>
      <div style={styles.sectionLabel}>Scoring weights</div>
      <div style={styles.metricEditorGrid}>
        {metricFields.slice(0, 6).map(field => {
          const metric = field.accessor.slice('metrics.'.length)
          const value = Number(filters.weights?.[metric] ?? 1)
          return (
            <label key={`weight-${metric}`} style={styles.weightEditor}>
              <span>{field.label}</span>
              <input type="range" min="0" max="2" step="0.1" value={value} onChange={e => setWeight(objectKey, metric, e.target.value)} />
              <b>{value.toFixed(1)}</b>
            </label>
          )
        })}
      </div>
    </section>
  )
}

function FieldPicker({ fields, selectedFields, setSelectedFields }) {
  const selected = new Set(selectedFields)
  const grouped = fields.reduce((acc, field) => {
    const group = field.group || 'Other'
    acc[group] = acc[group] || []
    acc[group].push(field)
    return acc
  }, {})
  function toggle(accessor) {
    const next = selected.has(accessor) ? selectedFields.filter(field => field !== accessor) : [...selectedFields, accessor]
    const normalized = next.length ? next : DEFAULT_SELECTED_FIELDS
    setSelectedFields(normalized)
    writeLocalJson(SELECTED_FIELDS_KEY, normalized)
  }
  return (
    <section style={styles.card}>
      <div style={styles.cardHeader}>
        <div>
          <div style={styles.cardTitle}>Available Fields</div>
          <div style={styles.cardSubtitle}>{fields.length} fields exposed by this dashboard object.</div>
        </div>
        <StatusPill>{selectedFields.length} selected</StatusPill>
      </div>
      <div style={styles.fieldGroups}>
        {Object.entries(grouped).map(([group, groupFields]) => (
          <div key={group} style={styles.fieldGroup}>
            <div style={styles.sectionLabel}>{group}</div>
            <div style={styles.fieldList}>
              {groupFields.map(field => (
                <button key={field.accessor} type="button" onClick={() => toggle(field.accessor)} style={selected.has(field.accessor) ? styles.fieldButtonActive : styles.fieldButton}>
                  <span>{field.label}</span>
                  <small>{field.accessor}</small>
                </button>
              ))}
            </div>
          </div>
        ))}
      </div>
    </section>
  )
}

function ResultsTable({ objectMeta, result, selectedFields, fields, isMobile }) {
  const rows = safeArray(result?.items)
  const fieldByAccessor = Object.fromEntries(fields.map(field => [field.accessor, field]))
  const selected = selectedFields.map(accessor => fieldByAccessor[accessor] || { accessor, label: titleCase(accessor) })
  if (!result) {
    return (
      <section style={styles.card}>
        <div style={styles.emptyState}>Select an object and click Run. The page does not run expensive formulas automatically on load anymore.</div>
      </section>
    )
  }
  return (
    <section style={styles.card}>
      <div style={styles.cardHeader}>
        <div>
          <div style={styles.cardTitle}>{objectMeta.apiLabel}</div>
          <div style={styles.cardSubtitle}>Table-first output. Card summaries are no longer the only way to inspect results.</div>
        </div>
        <div style={styles.pillRow}>
          <StatusPill>{rows.length} rows</StatusPill>
          <StatusPill tone="success">Before {result.result_count_before_filters ?? '—'}</StatusPill>
          <StatusPill tone="warning">After {result.result_count_after_filters ?? '—'}</StatusPill>
        </div>
      </div>
      {result.filter_warnings?.length ? <div style={styles.warningBanner}>{result.filter_warnings.join(' • ')}</div> : null}
      <div style={styles.tableWrap}>
        <table style={{ ...styles.table, minWidth: isMobile ? 780 : '100%' }}>
          <thead>
            <tr>{selected.map(field => <th key={`h-${field.accessor}`} style={styles.th}>{field.label}</th>)}</tr>
          </thead>
          <tbody>
            {rows.length === 0 ? (
              <tr><td style={styles.td} colSpan={selected.length || 1}>No rows returned.</td></tr>
            ) : rows.map((row, index) => (
              <tr key={`${objectMeta.key}-${row.entity_id || row.entity_name || index}`}>
                {selected.map(field => <td key={`${index}-${field.accessor}`} style={styles.td}>{formatCell(getValueByPath(row, field.accessor))}</td>)}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <details style={styles.details}>
        <summary style={styles.summary}>Audit payload</summary>
        <pre style={styles.pre}>{JSON.stringify({ filters_applied: result.filters_applied, available_filters: result.available_filters, data_quality: result.data_quality, missing_data: result.missing_data, lineup_filter: result.lineup_filter }, null, 2)}</pre>
      </details>
    </section>
  )
}

export default function MyDashboardWorkbenchPage() {
  const today = todayIso()
  const viewport = useViewport()
  const [authChecked, setAuthChecked] = useState(false)
  const [profile, setProfile] = useState(null)
  const [workspace, setWorkspace] = useState(null)
  const [form, setForm] = useState({ email: '', username: '', password: '', feature_interests: ['Matchups', 'Model Projections'], wants_newsletter: false, plan_type: 'free' })
  const [savingProfile, setSavingProfile] = useState(false)
  const [authError, setAuthError] = useState(null)
  const [activeObject, setActiveObjectState] = useState(() => readLocalJson(ACTIVE_OBJECT_KEY, OBJECTS[0].key))
  const [filters, setFilters] = useState(defaultFilters)
  const [results, setResults] = useState({})
  const [loading, setLoading] = useState({})
  const [errors, setErrors] = useState({})
  const [activeLineupsOnly, setActiveLineupsOnly] = useState(false)
  const [selectedFields, setSelectedFields] = useState(() => readLocalJson(SELECTED_FIELDS_KEY, DEFAULT_SELECTED_FIELDS))
  const [saveMessage, setSaveMessage] = useState('')

  const activeMeta = OBJECTS.find(object => object.key === activeObject) || OBJECTS[0]
  const activeResult = results[activeObject]
  const activeFields = useMemo(() => fieldsForResult(activeResult, activeObject), [activeResult, activeObject])
  const activeFilters = filters[activeObject] || emptyFilters()
  const folders = workspace?.folders || []
  const todayFolderId = workspace?.today_folder_id

  function setActiveObject(next) {
    setActiveObjectState(next)
    writeLocalJson(ACTIVE_OBJECT_KEY, next)
  }

  async function apiJson(url, options = {}) {
    const token = readSessionToken()
    const headers = { ...(options.headers || {}) }
    if (token) headers['X-Dashboard-Session'] = token
    const res = await fetch(url, { credentials: 'include', ...options, headers })
    const json = await res.json().catch(() => ({}))
    if (json?.session_token) writeSessionToken(json.session_token)
    if (res.status === 401) {
      writeSessionToken('')
      setProfile(null)
      throw new Error(typeof json?.detail === 'string' ? json.detail : 'Dashboard sign-in required')
    }
    if (!res.ok) throw new Error(typeof json?.detail === 'string' ? json.detail : JSON.stringify(json.detail || json))
    return json
  }

  async function loadWorkspace() {
    const json = await apiJson(`${API}/my-dashboard/workspace`)
    setWorkspace(json)
    return json
  }

  useEffect(() => {
    let cancelled = false
    const controller = new AbortController()
    const timeout = window.setTimeout(() => controller.abort(), 8000)
    async function bootstrap() {
      try {
        const token = readSessionToken()
        const res = await fetch(`${API}/my-dashboard/profile`, {
          credentials: 'include',
          signal: controller.signal,
          headers: token ? { 'X-Dashboard-Session': token } : {},
        })
        const json = await res.json().catch(() => ({}))
        if (cancelled) return
        if (json?.session_token) writeSessionToken(json.session_token)
        if (json.authenticated) {
          setProfile(json.user)
          await loadWorkspace()
        } else {
          setProfile(null)
        }
      } catch (err) {
        if (!cancelled) setAuthError(err?.name === 'AbortError' ? 'Dashboard profile load timed out.' : 'Unable to load dashboard profile.')
      } finally {
        window.clearTimeout(timeout)
        if (!cancelled) setAuthChecked(true)
      }
    }
    bootstrap()
    return () => {
      cancelled = true
      window.clearTimeout(timeout)
      controller.abort()
    }
  }, [])

  async function handleProfileSubmit(event) {
    event.preventDefault()
    setSavingProfile(true)
    setAuthError(null)
    try {
      const json = await apiJson(`${API}/my-dashboard/profile`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(form),
      })
      if (json?.session_token) writeSessionToken(json.session_token)
      const profileJson = await apiJson(`${API}/my-dashboard/profile`)
      setProfile(profileJson.user)
      await loadWorkspace()
    } catch (err) {
      setAuthError(err.message || 'Failed to create dashboard profile')
    } finally {
      setSavingProfile(false)
      setAuthChecked(true)
    }
  }

  function setBasicFilter(objectKey, key, value) {
    setFilters(prev => ({ ...prev, [objectKey]: { ...prev[objectKey], [key]: value } }))
  }

  function setMetricFilter(objectKey, metric, side, value) {
    setFilters(prev => {
      const next = clone(prev)
      next[objectKey] = next[objectKey] || emptyFilters()
      next[objectKey].metrics = next[objectKey].metrics || {}
      const entry = { ...(next[objectKey].metrics[metric] || {}) }
      entry[side] = value
      if ((entry.min || '') === '' && (entry.max || '') === '') delete next[objectKey].metrics[metric]
      else next[objectKey].metrics[metric] = entry
      return next
    })
  }

  function setWeight(objectKey, metric, value) {
    setFilters(prev => ({
      ...prev,
      [objectKey]: {
        ...prev[objectKey],
        weights: { ...(prev[objectKey]?.weights || {}), [metric]: value },
      },
    }))
  }

  async function runBoard(objectKey, options = {}) {
    const activeLineups = options.activeLineups ?? (activeLineupsOnly && ACTIVE_LINEUP_OBJECTS.has(objectKey))
    const payload = { date: today, component: objectKey, filters: cleanFilters(filters[objectKey] || {}) }
    const endpoint = activeLineups ? `${API}/my-dashboard/solver/active-lineups` : `${API}/my-dashboard/solver`
    const key = cacheKey(activeLineups ? 'active' : 'standard', payload)

    if (options.preferCache) {
      const cached = readSessionCache(key)
      if (cached) {
        setResults(prev => ({ ...prev, [objectKey]: cached }))
        return cached
      }
    }

    setLoading(prev => ({ ...prev, [objectKey]: true }))
    setErrors(prev => ({ ...prev, [objectKey]: null }))
    try {
      const json = await apiJson(endpoint, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) })
      writeSessionCache(key, json)
      setResults(prev => ({ ...prev, [objectKey]: json }))
      return json
    } catch (err) {
      setErrors(prev => ({ ...prev, [objectKey]: err.message || 'Board run failed' }))
      return null
    } finally {
      setLoading(prev => ({ ...prev, [objectKey]: false }))
    }
  }

  async function runAllProgressively() {
    setErrors({})
    for (const object of OBJECTS) {
      await runBoard(object.key, { preferCache: true })
    }
  }

  async function saveCurrentView() {
    const folderId = Number(todayFolderId || folders[0]?.id)
    const board = results[activeObject]
    if (!folderId) {
      setSaveMessage('Create or load a folder before saving this Workbench view.')
      return
    }
    if (!board?.items?.length) {
      setSaveMessage('Run the selected object before saving this Workbench view.')
      return
    }
    try {
      await apiJson(`${API}/my-dashboard/items`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          folder_id: folderId,
          source_tab: 'my-dashboard',
          source_type: 'workbench_view',
          title: `${activeMeta.label} Workbench | ${today}`,
          subtitle: activeMeta.description,
          notes: 'Saved from the mobile-first My Dashboard Workbench.',
          payload_json: {
            saved_from_component: activeObject,
            saved_on_date: today,
            board_state: board,
            workbench_state: { selectedFields, activeLineupsOnly },
          },
          filter_json: cleanFilters(filters[activeObject] || {}),
          sort_json: { by: 'score', direction: 'desc', component: activeObject, workbench: true },
        }),
      })
      await loadWorkspace()
      setSaveMessage(`Saved ${activeMeta.label} Workbench view.`)
    } catch (err) {
      setSaveMessage(err.message || 'Failed to save Workbench view.')
    }
  }

  const shellStyle = viewport.isNarrow ? styles.shellNarrow : styles.shell
  const contentStyle = viewport.isMobile ? styles.contentMobile : styles.content

  return (
    <>
      <AuthGate authChecked={authChecked} profile={profile} form={form} setForm={setForm} saving={savingProfile} error={authError} onSubmit={handleProfileSubmit} />
      {profile ? (
        <main style={viewport.isMobile ? styles.pageMobile : styles.page}>
          <section style={viewport.isNarrow ? styles.heroNarrow : styles.hero}>
            <div>
              <div style={styles.eyebrow}>My Dashboard Workbench</div>
              <h1 style={styles.title}>Custom baseball reports</h1>
              <p style={styles.subtitle}>Object picker, available fields, filters, and table output. Expensive formulas no longer block initial page load.</p>
              <div style={styles.pillRow}>
                <StatusPill>Signed in: {profile.username}</StatusPill>
                <StatusPill tone="success">Objects: {OBJECTS.length}</StatusPill>
                <StatusPill tone="warning">Runs on demand</StatusPill>
              </div>
            </div>
            <div style={styles.heroActions}>
              <button type="button" style={styles.primaryButton} onClick={() => runBoard(activeObject)} disabled={loading[activeObject]}>{loading[activeObject] ? 'Running…' : `Run ${activeMeta.label}`}</button>
              <button type="button" style={styles.secondaryButton} onClick={runAllProgressively}>Run all progressively</button>
              <button type="button" style={styles.secondaryButton} onClick={loadWorkspace}>Refresh folders</button>
              <button type="button" style={styles.ghostButton} onClick={saveCurrentView}>Save view</button>
            </div>
          </section>

          {saveMessage ? <div style={saveMessage.startsWith('Failed') ? styles.errorBanner : styles.successBanner}>{saveMessage}</div> : null}
          {errors[activeObject] ? <div style={styles.errorBanner}>{errors[activeObject]}</div> : null}

          <div style={shellStyle}>
            <ObjectRail objects={OBJECTS} activeKey={activeObject} setActiveKey={setActiveObject} results={results} loading={loading} isMobile={viewport.isMobile} />
            <div style={contentStyle}>
              <section style={styles.card}>
                <div style={styles.cardHeader}>
                  <div>
                    <div style={styles.cardTitle}>{activeMeta.label}</div>
                    <div style={styles.cardSubtitle}>{activeMeta.description}</div>
                  </div>
                  <label style={styles.checkRow}>
                    <input type="checkbox" checked={activeLineupsOnly} disabled={!ACTIVE_LINEUP_OBJECTS.has(activeObject)} onChange={e => setActiveLineupsOnly(e.target.checked)} />
                    Confirmed 1–9 only
                  </label>
                </div>
              </section>

              <div style={viewport.isNarrow ? styles.workbenchGridNarrow : styles.workbenchGrid}>
                <div style={styles.leftColumn}>
                  <WorkbenchFilters
                    objectKey={activeObject}
                    filters={activeFilters}
                    setBasicFilter={setBasicFilter}
                    setMetricFilter={setMetricFilter}
                    setWeight={setWeight}
                    availableFields={activeFields}
                    runBoard={runBoard}
                    loading={loading[activeObject]}
                  />
                  <FieldPicker fields={activeFields} selectedFields={selectedFields} setSelectedFields={setSelectedFields} />
                </div>
                <ResultsTable objectMeta={activeMeta} result={activeResult} selectedFields={selectedFields} fields={activeFields} isMobile={viewport.isMobile} />
              </div>
            </div>
          </div>
        </main>
      ) : null}
    </>
  )
}

const styles = {
  page: { minHeight: '100vh', background: `linear-gradient(180deg, ${C.bg}, #050a14)`, color: C.text, padding: 24, boxSizing: 'border-box' },
  pageMobile: { minHeight: '100vh', background: `linear-gradient(180deg, ${C.bg}, #050a14)`, color: C.text, padding: 12, boxSizing: 'border-box' },
  authPage: { minHeight: '100vh', display: 'grid', placeItems: 'center', background: C.bg, color: C.text, padding: 16 },
  authCard: { width: '100%', maxWidth: 520, background: C.panel, border: `1px solid ${C.border}`, borderRadius: 24, padding: 24, boxSizing: 'border-box' },
  authTitle: { margin: '8px 0', fontSize: 32 },
  authCopy: { margin: 0, color: C.muted, lineHeight: 1.6 },
  authForm: { display: 'grid', gap: 12, marginTop: 18 },
  stateView: { minHeight: '100vh', display: 'grid', placeItems: 'center', background: C.bg, color: C.text },
  hero: { display: 'grid', gridTemplateColumns: 'minmax(0, 1fr) auto', gap: 18, alignItems: 'start', background: C.panel, border: `1px solid ${C.border}`, borderRadius: 28, padding: 24, marginBottom: 18 },
  heroNarrow: { display: 'grid', gap: 18, alignItems: 'start', background: C.panel, border: `1px solid ${C.border}`, borderRadius: 22, padding: 18, marginBottom: 14 },
  heroActions: { display: 'flex', gap: 10, flexWrap: 'wrap', justifyContent: 'flex-end' },
  eyebrow: { color: C.blue, fontSize: 12, fontWeight: 800, letterSpacing: '0.12em', textTransform: 'uppercase' },
  title: { margin: '8px 0', fontSize: 'clamp(26px, 6vw, 44px)', lineHeight: 1.05 },
  subtitle: { margin: 0, color: C.muted, lineHeight: 1.6, maxWidth: 780 },
  shell: { display: 'grid', gridTemplateColumns: '280px minmax(0, 1fr)', gap: 18, alignItems: 'start' },
  shellNarrow: { display: 'grid', gap: 14, alignItems: 'start' },
  objectRail: { position: 'sticky', top: 18, background: C.panel, border: `1px solid ${C.border}`, borderRadius: 24, padding: 16, display: 'grid', gap: 14 },
  mobileObjectRail: { background: C.panel, border: `1px solid ${C.border}`, borderRadius: 20, padding: 12, display: 'grid', gap: 12, overflow: 'hidden' },
  railHeader: { display: 'grid', gap: 4 },
  railTitle: { fontSize: 20, fontWeight: 850 },
  objectList: { display: 'grid', gap: 10 },
  mobileObjectList: { display: 'flex', gap: 8, overflowX: 'auto', paddingBottom: 4 },
  objectButton: { width: '100%', display: 'flex', justifyContent: 'space-between', gap: 12, alignItems: 'center', textAlign: 'left', background: C.panel2, color: C.text, border: `1px solid ${C.border}`, borderRadius: 16, padding: 12, cursor: 'pointer', minWidth: 150 },
  objectButtonActive: { width: '100%', display: 'flex', justifyContent: 'space-between', gap: 12, alignItems: 'center', textAlign: 'left', background: 'rgba(96, 165, 250, 0.18)', color: C.text, border: '1px solid rgba(96, 165, 250, 0.5)', borderRadius: 16, padding: 12, cursor: 'pointer', minWidth: 150 },
  objectCount: { minWidth: 30, height: 30, borderRadius: 999, display: 'grid', placeItems: 'center', background: C.panel3, color: C.muted, fontWeight: 800 },
  content: { display: 'grid', gap: 16, minWidth: 0 },
  contentMobile: { display: 'grid', gap: 12, minWidth: 0 },
  workbenchGrid: { display: 'grid', gridTemplateColumns: '390px minmax(0, 1fr)', gap: 16, alignItems: 'start' },
  workbenchGridNarrow: { display: 'grid', gap: 14, alignItems: 'start' },
  leftColumn: { display: 'grid', gap: 14, minWidth: 0 },
  card: { background: C.panel, border: `1px solid ${C.border}`, borderRadius: 22, padding: 16, minWidth: 0, boxSizing: 'border-box' },
  cardHeader: { display: 'flex', justifyContent: 'space-between', gap: 12, alignItems: 'flex-start', marginBottom: 12, flexWrap: 'wrap' },
  cardTitle: { fontSize: 19, fontWeight: 850 },
  cardSubtitle: { color: C.muted, fontSize: 13, lineHeight: 1.5, marginTop: 4 },
  pillRow: { display: 'flex', gap: 8, flexWrap: 'wrap', alignItems: 'center' },
  pill: { display: 'inline-flex', border: '1px solid', borderRadius: 999, padding: '6px 10px', fontSize: 12, fontWeight: 800, whiteSpace: 'nowrap' },
  input: { width: '100%', minWidth: 0, boxSizing: 'border-box', borderRadius: 12, border: `1px solid ${C.border}`, background: C.panel2, color: C.text, padding: '11px 12px', outline: 'none' },
  miniInput: { width: '100%', minWidth: 0, boxSizing: 'border-box', borderRadius: 10, border: `1px solid ${C.border}`, background: C.panel2, color: C.text, padding: '8px 9px', outline: 'none' },
  primaryButton: { border: '1px solid rgba(96, 165, 250, 0.5)', background: 'rgba(96, 165, 250, 0.2)', color: C.text, borderRadius: 14, padding: '11px 14px', fontWeight: 850, cursor: 'pointer' },
  secondaryButton: { border: `1px solid ${C.border}`, background: C.panel2, color: C.text, borderRadius: 14, padding: '11px 14px', fontWeight: 750, cursor: 'pointer' },
  ghostButton: { border: `1px solid ${C.border}`, background: 'transparent', color: C.muted, borderRadius: 14, padding: '11px 14px', fontWeight: 750, cursor: 'pointer' },
  checkRow: { display: 'inline-flex', alignItems: 'center', gap: 8, color: C.muted, fontSize: 13 },
  filterGrid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(150px, 1fr))', gap: 9 },
  sectionLabel: { margin: '14px 0 8px', color: C.subtle, fontSize: 12, fontWeight: 850, letterSpacing: '0.08em', textTransform: 'uppercase' },
  metricEditorGrid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(138px, 1fr))', gap: 8 },
  metricEditor: { display: 'grid', gap: 7, background: C.panel2, border: `1px solid ${C.border}`, borderRadius: 14, padding: 10, fontSize: 12 },
  weightEditor: { display: 'grid', gap: 6, background: C.panel2, border: `1px solid ${C.border}`, borderRadius: 14, padding: 10, fontSize: 12, color: C.muted },
  fieldGroups: { display: 'grid', gap: 12, maxHeight: 520, overflowY: 'auto', paddingRight: 3 },
  fieldGroup: { display: 'grid', gap: 8 },
  fieldList: { display: 'grid', gap: 7 },
  fieldButton: { display: 'grid', gap: 2, textAlign: 'left', background: C.panel2, color: C.text, border: `1px solid ${C.border}`, borderRadius: 12, padding: 10, cursor: 'pointer' },
  fieldButtonActive: { display: 'grid', gap: 2, textAlign: 'left', background: 'rgba(52, 211, 153, 0.13)', color: C.text, border: '1px solid rgba(52, 211, 153, 0.42)', borderRadius: 12, padding: 10, cursor: 'pointer' },
  tableWrap: { width: '100%', overflowX: 'auto', border: `1px solid ${C.border}`, borderRadius: 16 },
  table: { width: '100%', borderCollapse: 'collapse', background: C.panel2 },
  th: { textAlign: 'left', color: C.muted, fontSize: 12, padding: '12px 13px', borderBottom: `1px solid ${C.border}`, whiteSpace: 'nowrap' },
  td: { color: C.text, fontSize: 13, padding: '12px 13px', borderBottom: `1px solid ${C.border}`, verticalAlign: 'top', maxWidth: 360, overflow: 'hidden', textOverflow: 'ellipsis' },
  details: { marginTop: 12 },
  summary: { color: C.blue, cursor: 'pointer', fontWeight: 750 },
  pre: { overflowX: 'auto', background: '#020617', color: C.muted, border: `1px solid ${C.border}`, borderRadius: 14, padding: 12, fontSize: 12 },
  emptyState: { color: C.muted, background: C.panel2, border: `1px dashed ${C.border}`, borderRadius: 16, padding: 16, lineHeight: 1.5 },
  warningBanner: { background: 'rgba(251, 191, 36, 0.12)', border: '1px solid rgba(251, 191, 36, 0.35)', color: '#fde68a', borderRadius: 14, padding: 12, marginBottom: 12 },
  errorBanner: { background: 'rgba(248, 113, 113, 0.12)', border: '1px solid rgba(248, 113, 113, 0.35)', color: '#fecaca', borderRadius: 14, padding: 12, marginBottom: 12 },
  successBanner: { background: 'rgba(52, 211, 153, 0.12)', border: '1px solid rgba(52, 211, 153, 0.35)', color: '#a7f3d0', borderRadius: 14, padding: 12, marginBottom: 12 },
}
