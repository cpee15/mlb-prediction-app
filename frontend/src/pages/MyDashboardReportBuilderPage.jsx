import React, { useEffect, useMemo, useState } from 'react'

const API = import.meta.env.VITE_API_BASE_URL || ''
const SESSION_KEY = 'mlbgpt_dashboard_session_token'
const BUILDER_KEY = 'mlbgpt-report-builder:v2'

const OBJECTS = [
  { key: 'hitters', label: 'Hitters', description: 'Batter vs arsenal, quality-of-contact, matchup, and model fields.' },
  { key: 'pitchers', label: 'Pitchers', description: 'Pitcher skills, contact suppression, opponent profile, and model fields.' },
  { key: 'teams', label: 'Teams', description: 'Team offense, projected runs, side edge, and matchup fields.' },
  { key: 'totals', label: 'Totals', description: 'Projected game totals, run environment, and simulation fields.' },
  { key: 'overall_players', label: 'Overall Players', description: 'Combined hitter and pitcher report object.' },
]

const ACTIVE_LINEUP_OBJECTS = new Set(['hitters', 'overall_players'])
const DEFAULT_FIELDS = ['rank', 'entity_name', 'team', 'opponent', 'score', 'confidence']
const BASE_FIELDS = [
  ['rank', 'Rank', 'Identity'], ['entity_name', 'Name', 'Identity'], ['entity_id', 'Entity ID', 'Identity'],
  ['entity_type', 'Entity Type', 'Identity'], ['player_type', 'Player Type', 'Identity'], ['team', 'Team', 'Matchup'],
  ['opponent', 'Opponent', 'Matchup'], ['game_pk', 'Game PK', 'Matchup'], ['pitch_type', 'Pitch Type', 'Matchup'],
  ['pitch_name', 'Pitch Name', 'Matchup'], ['category', 'Category', 'Classification'], ['score', 'Score', 'Scoring'],
  ['base_score', 'Base Score', 'Scoring'], ['adjusted_score', 'Adjusted Score', 'Scoring'], ['confidence', 'Confidence', 'Scoring'],
  ['source', 'Source', 'Audit'], ['primary_reason', 'Primary Reason', 'Audit'], ['lineup_verified', 'Lineup Verified', 'Audit'],
  ['lineup_source', 'Lineup Source', 'Audit'],
].map(([accessor, label, group]) => ({ accessor, label, group }))

const DEFAULT_METRICS = {
  hitters: ['xwOBA', 'xBA', 'EV', 'LA', 'HardHit', 'Usage', 'Pitcher xWoba', 'Pitches Seen', 'PA'],
  pitchers: ['K%', 'BB%', 'xwOBA Allowed', 'HardHit Allowed', 'Opp K%', 'Opp ISO', 'Score'],
  teams: ['Edge Score', 'Win Edge', 'Run Diff', 'ISO', 'OBP', 'SLG'],
  totals: ['Projected Total', 'Raw Total', 'Run Index', 'Score'],
  overall_players: ['Score', 'xwOBA', 'EV', 'K%', 'xwOBA Allowed'],
}

const C = {
  bg: '#07101d', panel: '#111827', panel2: '#0b1322', panel3: '#182235', border: 'rgba(148,163,184,.22)',
  text: '#e8eef8', muted: '#94a3b8', subtle: '#64748b', blue: '#60a5fa', green: '#34d399', amber: '#fbbf24', red: '#f87171',
}

function todayIso() { return new Date().toISOString().slice(0, 10) }
function safeArray(value) { return Array.isArray(value) ? value : [] }
function titleCase(value) { return String(value || '').replace(/[_.-]+/g, ' ').replace(/\s+/g, ' ').trim().replace(/\b\w/g, c => c.toUpperCase()) }
function readJson(key, fallback) { try { return JSON.parse(localStorage.getItem(key)) || fallback } catch { return fallback } }
function writeJson(key, value) { try { localStorage.setItem(key, JSON.stringify(value)) } catch {} }
function readToken() { try { return localStorage.getItem(SESSION_KEY) || '' } catch { return '' } }
function writeToken(value) { try { value ? localStorage.setItem(SESSION_KEY, value) : localStorage.removeItem(SESSION_KEY) } catch {} }
function emptyFilters() { return { search_text: '', team: '', opponent: '', min_score: '', max_score: '', min_confidence: '', category: '', pitch_type: '', metrics: {}, weights: {} } }
function cleanFilters(filters) {
  const out = {}
  Object.entries(filters || {}).forEach(([key, value]) => {
    if (key === 'metrics' || key === 'weights') return
    if (value !== '' && value != null) out[key] = value
  })
  const metrics = {}
  Object.entries(filters?.metrics || {}).forEach(([metric, rule]) => {
    const next = {}
    if (rule?.min !== '' && rule?.min != null) next.min = Number(rule.min)
    if (rule?.max !== '' && rule?.max != null) next.max = Number(rule.max)
    if (Object.keys(next).length) metrics[metric] = next
  })
  if (Object.keys(metrics).length) out.metrics = metrics
  const weights = {}
  Object.entries(filters?.weights || {}).forEach(([metric, value]) => {
    const number = Number(value)
    if (Number.isFinite(number) && number !== 1) weights[metric] = number
  })
  if (Object.keys(weights).length) out.weights = weights
  return out
}
function getValue(row, accessor) {
  if (accessor.startsWith('metrics.')) return row?.metrics?.[accessor.slice(8)]
  return accessor.split('.').reduce((value, key) => value == null ? null : value[key], row)
}
function formatCell(value) {
  if (value === '' || value == null) return '—'
  if (typeof value === 'number') return Math.abs(value) >= 10 ? value.toFixed(1) : value.toFixed(3)
  if (typeof value === 'boolean') return value ? 'Yes' : 'No'
  if (Array.isArray(value)) return value.slice(0, 4).join(', ')
  if (typeof value === 'object') return JSON.stringify(value)
  return String(value)
}
function compareValues(a, b) {
  if (a == null && b == null) return 0
  if (a == null) return 1
  if (b == null) return -1
  const an = Number(a), bn = Number(b)
  if (Number.isFinite(an) && Number.isFinite(bn)) return an - bn
  return String(a).localeCompare(String(b), undefined, { numeric: true, sensitivity: 'base' })
}
function fieldsForResult(result, objectKey) {
  const seen = new Map(BASE_FIELDS.map(field => [field.accessor, field]))
  const metrics = result?.available_filters?.metrics?.length ? result.available_filters.metrics : DEFAULT_METRICS[objectKey] || []
  metrics.forEach(metric => seen.set(`metrics.${metric}`, { accessor: `metrics.${metric}`, label: metric, group: 'Metrics' }))
  safeArray(result?.items).forEach(row => {
    Object.keys(row || {}).forEach(key => {
      if (!['chart_data', 'reasoning', 'missing_data', 'best_pitch_angles', 'metrics'].includes(key) && !seen.has(key)) seen.set(key, { accessor: key, label: titleCase(key), group: 'Runtime' })
    })
    Object.keys(row?.metrics || {}).forEach(metric => {
      const accessor = `metrics.${metric}`
      if (!seen.has(accessor)) seen.set(accessor, { accessor, label: metric, group: 'Metrics' })
    })
  })
  return Array.from(seen.values())
}
function workspaceItems(workspace) {
  const direct = safeArray(workspace?.items)
  const nested = safeArray(workspace?.folders).flatMap(folder => safeArray(folder?.items).map(item => ({ ...item, folder_name: folder.name || folder.title })))
  return [...direct, ...nested].filter(item => ['workbench_view', 'report_view', 'dashboard_report'].includes(item?.source_type))
}

function Pill({ children, tone = 'blue' }) {
  const color = tone === 'green' ? C.green : tone === 'amber' ? C.amber : tone === 'red' ? C.red : C.blue
  return <span style={{ ...s.pill, color, borderColor: `${color}55`, background: `${color}16` }}>{children}</span>
}

function AuthGate({ checked, profile, form, setForm, saving, error, submit }) {
  if (!checked) return <div style={s.loadingPage}>Loading Report Builder…</div>
  if (profile) return null
  return <main style={s.authPage}><section style={s.authCard}>
    <div style={s.eyebrow}>MLBGPT Report Builder</div>
    <h1 style={s.authTitle}>Create your analyst profile</h1>
    <p style={s.copy}>Your saved reports, folders, filters, and report definitions remain tied to this dashboard profile.</p>
    <form onSubmit={submit} style={s.stack}>
      <input required style={s.input} placeholder="Email" value={form.email} onChange={e => setForm(v => ({ ...v, email: e.target.value }))} />
      <input required style={s.input} placeholder="Username" value={form.username} onChange={e => setForm(v => ({ ...v, username: e.target.value }))} />
      <input style={s.input} type="password" placeholder="Password" value={form.password} onChange={e => setForm(v => ({ ...v, password: e.target.value }))} />
      {error ? <div style={s.error}>{error}</div> : null}
      <button style={s.primary} disabled={saving}>{saving ? 'Creating…' : 'Enter Report Builder'}</button>
    </form>
  </section></main>
}

function ObjectManager({ active, setActive, results }) {
  return <aside style={s.objectManager}>
    <div><div style={s.eyebrow}>Object Manager</div><div style={s.panelTitle}>Report Objects</div></div>
    <div style={s.objectList}>{OBJECTS.map(object => <button key={object.key} style={active === object.key ? s.objectActive : s.objectButton} onClick={() => setActive(object.key)}>
      <span><strong>{object.label}</strong><small>{object.description}</small></span><b>{safeArray(results[object.key]?.items).length}</b>
    </button>)}</div>
  </aside>
}

function FilterPanel({ objectKey, filters, fields, setBasic, setMetric, setWeight }) {
  const metrics = fields.filter(field => field.accessor.startsWith('metrics.')).slice(0, 10)
  return <section style={s.card}>
    <div style={s.cardHeader}><div><div style={s.panelTitle}>Filters</div><div style={s.copySmall}>Define the report criteria before generating rows.</div></div></div>
    <div style={s.filterGrid}>
      <input style={s.input} placeholder="Search text" value={filters.search_text || ''} onChange={e => setBasic(objectKey, 'search_text', e.target.value)} />
      <input style={s.input} placeholder="Team contains" value={filters.team || ''} onChange={e => setBasic(objectKey, 'team', e.target.value)} />
      <input style={s.input} placeholder="Opponent contains" value={filters.opponent || ''} onChange={e => setBasic(objectKey, 'opponent', e.target.value)} />
      <select style={s.input} value={filters.min_confidence || ''} onChange={e => setBasic(objectKey, 'min_confidence', e.target.value)}><option value="">Any confidence</option><option value="low">Low+</option><option value="medium">Medium+</option><option value="high">High only</option></select>
      <input style={s.input} inputMode="decimal" placeholder="Minimum score" value={filters.min_score || ''} onChange={e => setBasic(objectKey, 'min_score', e.target.value)} />
      <input style={s.input} inputMode="decimal" placeholder="Maximum score" value={filters.max_score || ''} onChange={e => setBasic(objectKey, 'max_score', e.target.value)} />
      <input style={s.input} placeholder="Category" value={filters.category || ''} onChange={e => setBasic(objectKey, 'category', e.target.value)} />
      <input style={s.input} placeholder="Pitch type" value={filters.pitch_type || ''} onChange={e => setBasic(objectKey, 'pitch_type', e.target.value)} />
    </div>
    <div style={s.sectionLabel}>Metric thresholds</div>
    <div style={s.metricGrid}>{metrics.map(field => {
      const metric = field.accessor.slice(8)
      return <div style={s.metricCard} key={field.accessor}><strong>{field.label}</strong><div style={s.twoCol}>
        <input style={s.miniInput} placeholder="Min" value={filters.metrics?.[metric]?.min || ''} onChange={e => setMetric(objectKey, metric, 'min', e.target.value)} />
        <input style={s.miniInput} placeholder="Max" value={filters.metrics?.[metric]?.max || ''} onChange={e => setMetric(objectKey, metric, 'max', e.target.value)} />
      </div></div>
    })}</div>
    <div style={s.sectionLabel}>Scoring weights</div>
    <div style={s.metricGrid}>{metrics.slice(0, 6).map(field => {
      const metric = field.accessor.slice(8), value = Number(filters.weights?.[metric] ?? 1)
      return <label style={s.metricCard} key={`weight-${field.accessor}`}><span>{field.label}</span><input type="range" min="0" max="2" step="0.1" value={value} onChange={e => setWeight(objectKey, metric, e.target.value)} /><b>{value.toFixed(1)}</b></label>
    })}</div>
  </section>
}

function FieldLibrary({ fields, selected, setSelected }) {
  const selectedSet = new Set(selected)
  const grouped = fields.reduce((map, field) => ({ ...map, [field.group]: [...(map[field.group] || []), field] }), {})
  function toggle(accessor) {
    const next = selectedSet.has(accessor) ? selected.filter(value => value !== accessor) : [...selected, accessor]
    setSelected(next.length ? next : DEFAULT_FIELDS)
  }
  return <section style={s.card}>
    <div style={s.cardHeader}><div><div style={s.panelTitle}>Field Library</div><div style={s.copySmall}>Choose the columns that belong in the next report.</div></div><Pill>{selected.length} selected</Pill></div>
    <div style={s.fieldGroups}>{Object.entries(grouped).map(([group, groupFields]) => <div key={group}>
      <div style={s.sectionLabel}>{group}</div><div style={s.fieldGrid}>{groupFields.map(field => <button key={field.accessor} style={selectedSet.has(field.accessor) ? s.fieldActive : s.fieldButton} onClick={() => toggle(field.accessor)}><span>{field.label}</span><small>{field.accessor}</small></button>)}</div>
    </div>)}</div>
  </section>
}

function SavedReports({ workspace, openSaved, refresh }) {
  const items = workspaceItems(workspace)
  return <section style={s.card}>
    <div style={s.cardHeader}><div><div style={s.panelTitle}>Saved Reports</div><div style={s.copySmall}>Open a prior generated report definition and its saved rows.</div></div><button style={s.secondary} onClick={refresh}>Refresh</button></div>
    {items.length ? <div style={s.savedList}>{items.map((item, index) => <button key={item.id || index} style={s.savedItem} onClick={() => openSaved(item)}>
      <span><strong>{item.title || 'Saved report'}</strong><small>{item.folder_name || item.subtitle || 'Dashboard report'}</small></span><span>Open →</span>
    </button>)}</div> : <div style={s.empty}>No saved reports yet. Populate a report, then save it to your dashboard folder.</div>}
  </section>
}

function ReportWorkspace({ open, close, objectMeta, result, fields, builderFields, generatedAt, initialColumns, initialSort, onSave }) {
  const [columns, setColumns] = useState(initialColumns)
  const [hidden, setHidden] = useState([])
  const [sort, setSort] = useState(initialSort || { accessor: 'score', direction: 'desc' })
  useEffect(() => { setColumns(initialColumns); setHidden([]); setSort(initialSort || { accessor: 'score', direction: 'desc' }) }, [initialColumns, initialSort, result])
  const fieldMap = useMemo(() => Object.fromEntries(fields.map(field => [field.accessor, field])), [fields])
  const visible = columns.filter(accessor => !hidden.includes(accessor))
  const rows = useMemo(() => [...safeArray(result?.items)].sort((a, b) => {
    const direction = sort.direction === 'asc' ? 1 : -1
    return compareValues(getValue(a, sort.accessor), getValue(b, sort.accessor)) * direction
  }), [result, sort])
  if (!open) return null
  function sortBy(accessor) { setSort(current => current.accessor === accessor ? { accessor, direction: current.direction === 'desc' ? 'asc' : 'desc' } : { accessor, direction: 'desc' }) }
  function move(accessor, delta) {
    setColumns(current => {
      const from = current.indexOf(accessor), to = from + delta
      if (from < 0 || to < 0 || to >= current.length) return current
      const next = [...current], [item] = next.splice(from, 1); next.splice(to, 0, item); return next
    })
  }
  return <div style={s.overlay} role="dialog" aria-modal="true">
    <section style={s.reportSurface}>
      <header style={s.reportHeader}><div><div style={s.eyebrow}>Report Workspace</div><h2 style={s.reportTitle}>{objectMeta.label} Report</h2><div style={s.copySmall}>Generated {generatedAt ? new Date(generatedAt).toLocaleString() : 'now'} · {rows.length} rows · {visible.length} visible columns</div></div>
        <div style={s.actions}><button style={s.secondary} onClick={onSave}>Save Report</button><button style={s.primary} onClick={close}>Back to Builder</button></div>
      </header>
      <div style={s.reportBody}>
        <aside style={s.columnPanel}><div style={s.panelTitle}>Report Columns</div><div style={s.copySmall}>Hide or reorder columns without changing the builder field selection.</div>
          <div style={s.columnList}>{columns.map((accessor, index) => {
            const field = fieldMap[accessor] || { label: titleCase(accessor) }, isHidden = hidden.includes(accessor)
            return <div style={s.columnRow} key={accessor}><label style={s.columnLabel}><input type="checkbox" checked={!isHidden} onChange={() => setHidden(current => isHidden ? current.filter(v => v !== accessor) : [...current, accessor])} /><span>{field.label}</span></label><div><button style={s.iconButton} disabled={index === 0} onClick={() => move(accessor, -1)}>↑</button><button style={s.iconButton} disabled={index === columns.length - 1} onClick={() => move(accessor, 1)}>↓</button></div></div>
          })}</div>
          <button style={s.secondaryWide} onClick={() => { setColumns(builderFields); setHidden([]) }}>Reset to Builder Fields</button>
        </aside>
        <main style={s.gridPanel}>
          <div style={s.gridToolbar}><div style={s.pillRow}><Pill>{rows.length} rows</Pill><Pill tone="green">Sort: {fieldMap[sort.accessor]?.label || titleCase(sort.accessor)} {sort.direction === 'desc' ? 'high → low' : 'low → high'}</Pill>{result?.lineup_filter?.enabled ? <Pill tone="amber">Confirmed lineup filter</Pill> : null}</div></div>
          {result?.filter_warnings?.length ? <div style={s.warning}>{result.filter_warnings.join(' • ')}</div> : null}
          <div style={s.dataGridWrap}><table style={s.table}><thead><tr>{visible.map(accessor => <th key={accessor} style={s.th}><button style={s.sortButton} onClick={() => sortBy(accessor)}>{fieldMap[accessor]?.label || titleCase(accessor)} {sort.accessor === accessor ? (sort.direction === 'desc' ? '↓' : '↑') : '↕'}</button></th>)}</tr></thead>
            <tbody>{rows.length ? rows.map((row, rowIndex) => <tr key={row.entity_id || `${objectMeta.key}-${rowIndex}`}>{visible.map(accessor => <td style={s.td} key={`${rowIndex}-${accessor}`}>{formatCell(getValue(row, accessor))}</td>)}</tr>) : <tr><td style={s.td} colSpan={Math.max(visible.length, 1)}>No rows returned.</td></tr>}</tbody>
          </table></div>
        </main>
      </div>
    </section>
  </div>
}

export default function MyDashboardReportBuilderPage() {
  const persisted = typeof window === 'undefined' ? {} : readJson(BUILDER_KEY, {})
  const [authChecked, setAuthChecked] = useState(false)
  const [profile, setProfile] = useState(null)
  const [workspace, setWorkspace] = useState(null)
  const [form, setForm] = useState({ email: '', username: '', password: '', feature_interests: ['Matchups', 'Model Projections'], wants_newsletter: false, plan_type: 'free' })
  const [savingProfile, setSavingProfile] = useState(false)
  const [authError, setAuthError] = useState('')
  const [activeObject, setActiveObject] = useState(persisted.activeObject || 'hitters')
  const [filters, setFilters] = useState(persisted.filters || Object.fromEntries(OBJECTS.map(object => [object.key, emptyFilters()])))
  const [selectedFields, setSelectedFields] = useState(persisted.selectedFields || DEFAULT_FIELDS)
  const [activeLineupsOnly, setActiveLineupsOnly] = useState(Boolean(persisted.activeLineupsOnly))
  const [results, setResults] = useState({})
  const [loading, setLoading] = useState({})
  const [error, setError] = useState('')
  const [saveMessage, setSaveMessage] = useState('')
  const [reportOpen, setReportOpen] = useState(false)
  const [reportObject, setReportObject] = useState('hitters')
  const [reportResult, setReportResult] = useState(null)
  const [reportColumns, setReportColumns] = useState(DEFAULT_FIELDS)
  const [generatedAt, setGeneratedAt] = useState(null)

  const activeMeta = OBJECTS.find(object => object.key === activeObject) || OBJECTS[0]
  const activeResult = results[activeObject]
  const activeFields = useMemo(() => fieldsForResult(activeResult, activeObject), [activeResult, activeObject])
  const activeFilters = filters[activeObject] || emptyFilters()
  const folders = safeArray(workspace?.folders)
  const folderId = Number(workspace?.today_folder_id || folders[0]?.id)

  useEffect(() => { if (typeof window !== 'undefined') writeJson(BUILDER_KEY, { activeObject, filters, selectedFields, activeLineupsOnly }) }, [activeObject, filters, selectedFields, activeLineupsOnly])

  async function apiJson(url, options = {}) {
    const headers = { ...(options.headers || {}) }, token = readToken()
    if (token) headers['X-Dashboard-Session'] = token
    const response = await fetch(url, { credentials: 'include', ...options, headers })
    const json = await response.json().catch(() => ({}))
    if (json?.session_token) writeToken(json.session_token)
    if (response.status === 401) { writeToken(''); setProfile(null); throw new Error('Dashboard sign-in required') }
    if (!response.ok) throw new Error(typeof json?.detail === 'string' ? json.detail : JSON.stringify(json.detail || json))
    return json
  }
  async function loadWorkspace() { const json = await apiJson(`${API}/my-dashboard/workspace`); setWorkspace(json); return json }

  useEffect(() => {
    let cancelled = false
    async function load() {
      try {
        const headers = readToken() ? { 'X-Dashboard-Session': readToken() } : {}
        const response = await fetch(`${API}/my-dashboard/profile`, { credentials: 'include', headers })
        const json = await response.json().catch(() => ({}))
        if (cancelled) return
        if (json?.session_token) writeToken(json.session_token)
        if (json.authenticated) { setProfile(json.user); await loadWorkspace() }
      } catch { if (!cancelled) setAuthError('Unable to load dashboard profile.') }
      finally { if (!cancelled) setAuthChecked(true) }
    }
    load(); return () => { cancelled = true }
  }, [])

  async function submitProfile(event) {
    event.preventDefault(); setSavingProfile(true); setAuthError('')
    try {
      const created = await apiJson(`${API}/my-dashboard/profile`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(form) })
      if (created?.session_token) writeToken(created.session_token)
      const current = await apiJson(`${API}/my-dashboard/profile`); setProfile(current.user); await loadWorkspace()
    } catch (err) { setAuthError(err.message || 'Failed to create profile') }
    finally { setSavingProfile(false); setAuthChecked(true) }
  }
  function setBasic(objectKey, key, value) { setFilters(current => ({ ...current, [objectKey]: { ...(current[objectKey] || emptyFilters()), [key]: value } })) }
  function setMetric(objectKey, metric, side, value) { setFilters(current => ({ ...current, [objectKey]: { ...(current[objectKey] || emptyFilters()), metrics: { ...(current[objectKey]?.metrics || {}), [metric]: { ...(current[objectKey]?.metrics?.[metric] || {}), [side]: value } } } })) }
  function setWeight(objectKey, metric, value) { setFilters(current => ({ ...current, [objectKey]: { ...(current[objectKey] || emptyFilters()), weights: { ...(current[objectKey]?.weights || {}), [metric]: value } } })) }

  async function populateReport(objectKey = activeObject) {
    const useLineups = activeLineupsOnly && ACTIVE_LINEUP_OBJECTS.has(objectKey)
    const endpoint = useLineups ? `${API}/my-dashboard/solver/active-lineups` : `${API}/my-dashboard/solver`
    const payload = { date: todayIso(), component: objectKey, filters: cleanFilters(filters[objectKey] || {}) }
    setLoading(current => ({ ...current, [objectKey]: true })); setError('')
    try {
      const json = await apiJson(endpoint, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) })
      setResults(current => ({ ...current, [objectKey]: json }))
      setReportObject(objectKey); setReportResult(json); setReportColumns([...selectedFields]); setGeneratedAt(new Date().toISOString()); setReportOpen(true)
      return json
    } catch (err) { setError(err.message || 'Report generation failed'); return null }
    finally { setLoading(current => ({ ...current, [objectKey]: false })) }
  }

  function openSaved(item) {
    const payload = item?.payload_json || {}
    const board = payload.board_state || payload.report_state || payload
    const component = payload.saved_from_component || item?.sort_json?.component || activeObject
    const columns = payload.workbench_state?.selectedFields || payload.report_columns || selectedFields
    if (!board?.items) { setSaveMessage('This saved item does not contain report rows.'); return }
    setReportObject(component); setReportResult(board); setReportColumns(columns); setGeneratedAt(payload.saved_on_date || item.created_at || new Date().toISOString()); setReportOpen(true)
  }

  async function saveReport() {
    if (!folderId || !reportResult?.items?.length) { setSaveMessage(!folderId ? 'No dashboard folder is available.' : 'Populate a report before saving.'); return }
    const meta = OBJECTS.find(object => object.key === reportObject) || activeMeta
    try {
      await apiJson(`${API}/my-dashboard/items`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({
        folder_id: folderId, source_tab: 'my-dashboard', source_type: 'report_view', title: `${meta.label} Report | ${todayIso()}`, subtitle: meta.description,
        notes: 'Saved from the MLBGPT Report Workspace.', payload_json: { saved_from_component: reportObject, saved_on_date: todayIso(), board_state: reportResult, report_columns: reportColumns, workbench_state: { selectedFields, activeLineupsOnly } },
        filter_json: cleanFilters(filters[reportObject] || {}), sort_json: { by: 'score', direction: 'desc', component: reportObject, report_workspace: true },
      }) })
      await loadWorkspace(); setSaveMessage(`Saved ${meta.label} report.`)
    } catch (err) { setSaveMessage(err.message || 'Failed to save report.') }
  }

  const reportMeta = OBJECTS.find(object => object.key === reportObject) || activeMeta
  const reportFields = fieldsForResult(reportResult, reportObject)

  return <>
    <AuthGate checked={authChecked} profile={profile} form={form} setForm={setForm} saving={savingProfile} error={authError} submit={submitProfile} />
    {profile ? <main style={s.page}>
      <section style={s.hero}><div><div style={s.eyebrow}>MLBGPT Report Builder</div><h1 style={s.title}>Build the report. Open the workspace.</h1><p style={s.copy}>Choose an object, configure fields and filters, then generate a dedicated report surface for sorting and column management.</p><div style={s.pillRow}><Pill>Signed in: {profile.username}</Pill><Pill tone="green">{OBJECTS.length} report objects</Pill><Pill tone="amber">Runs on demand</Pill></div></div>
        <div style={s.actions}><button style={s.primary} disabled={loading[activeObject]} onClick={() => populateReport()}>{loading[activeObject] ? 'Populating…' : 'Populate Report'}</button><button style={s.secondary} onClick={loadWorkspace}>Refresh Saved Reports</button></div>
      </section>
      {saveMessage ? <div style={saveMessage.startsWith('Failed') || saveMessage.startsWith('No ') ? s.error : s.success}>{saveMessage}</div> : null}
      {error ? <div style={s.error}>{error}</div> : null}
      <div style={s.builderShell}><ObjectManager active={activeObject} setActive={setActiveObject} results={results} />
        <div style={s.mainStack}><section style={s.card}><div style={s.cardHeader}><div><div style={s.panelTitle}>{activeMeta.label}</div><div style={s.copySmall}>{activeMeta.description}</div></div><label style={s.check}><input type="checkbox" checked={activeLineupsOnly} disabled={!ACTIVE_LINEUP_OBJECTS.has(activeObject)} onChange={e => setActiveLineupsOnly(e.target.checked)} />Confirmed 1–9 only</label></div></section>
          <div style={s.builderColumns}><div style={s.stack}><FilterPanel objectKey={activeObject} filters={activeFilters} fields={activeFields} setBasic={setBasic} setMetric={setMetric} setWeight={setWeight} /><FieldLibrary fields={activeFields} selected={selectedFields} setSelected={setSelectedFields} /></div>
            <div style={s.stack}><section style={s.previewCard}><div><div style={s.panelTitle}>Report Preview</div><div style={s.copySmall}>The report grid opens separately so this builder remains clean and editable.</div></div><div style={s.previewStats}><Pill>{selectedFields.length} columns</Pill><Pill tone="green">{Object.keys(cleanFilters(activeFilters)).length} active filter groups</Pill></div><button style={s.populateWide} disabled={loading[activeObject]} onClick={() => populateReport()}>{loading[activeObject] ? 'Populating Report…' : 'Populate Report'}</button>{activeResult ? <button style={s.secondaryWide} onClick={() => { setReportObject(activeObject); setReportResult(activeResult); setReportColumns([...selectedFields]); setGeneratedAt(new Date().toISOString()); setReportOpen(true) }}>Open Last Report</button> : null}</section><SavedReports workspace={workspace} openSaved={openSaved} refresh={loadWorkspace} /></div>
          </div>
        </div>
      </div>
      <ReportWorkspace open={reportOpen} close={() => setReportOpen(false)} objectMeta={reportMeta} result={reportResult} fields={reportFields} builderFields={selectedFields} initialColumns={reportColumns} generatedAt={generatedAt} onSave={saveReport} />
    </main> : null}
  </>
}

const s = {
  page: { minHeight: '100vh', boxSizing: 'border-box', padding: 'clamp(12px,2.5vw,28px)', color: C.text, background: `linear-gradient(180deg,${C.bg},#030712)`, overflowX: 'hidden' },
  loadingPage: { minHeight: '100vh', display: 'grid', placeItems: 'center', color: C.text, background: C.bg }, authPage: { minHeight: '100vh', display: 'grid', placeItems: 'center', padding: 16, background: C.bg, color: C.text },
  authCard: { width: '100%', maxWidth: 520, padding: 24, borderRadius: 24, border: `1px solid ${C.border}`, background: C.panel }, authTitle: { margin: '8px 0', fontSize: 32 },
  hero: { display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', flexWrap: 'wrap', gap: 18, padding: 24, marginBottom: 18, borderRadius: 26, border: `1px solid ${C.border}`, background: C.panel },
  eyebrow: { color: C.blue, fontSize: 12, fontWeight: 900, letterSpacing: '.11em', textTransform: 'uppercase' }, title: { margin: '8px 0', fontSize: 'clamp(28px,5vw,46px)', lineHeight: 1.05 }, copy: { margin: 0, maxWidth: 780, color: C.muted, lineHeight: 1.6 }, copySmall: { marginTop: 4, color: C.muted, fontSize: 13, lineHeight: 1.45 },
  actions: { display: 'flex', gap: 10, flexWrap: 'wrap' }, stack: { display: 'grid', gap: 14 }, builderShell: { display: 'grid', gridTemplateColumns: 'minmax(230px,280px) minmax(0,1fr)', gap: 16, alignItems: 'start' }, mainStack: { display: 'grid', gap: 14, minWidth: 0 }, builderColumns: { display: 'grid', gridTemplateColumns: 'minmax(0,1.25fr) minmax(280px,.75fr)', gap: 14, alignItems: 'start' },
  objectManager: { position: 'sticky', top: 16, padding: 15, display: 'grid', gap: 14, borderRadius: 22, border: `1px solid ${C.border}`, background: C.panel }, objectList: { display: 'grid', gap: 8 }, objectButton: { display: 'flex', justifyContent: 'space-between', gap: 10, textAlign: 'left', padding: 12, color: C.text, background: C.panel2, border: `1px solid ${C.border}`, borderRadius: 14, cursor: 'pointer' }, objectActive: { display: 'flex', justifyContent: 'space-between', gap: 10, textAlign: 'left', padding: 12, color: C.text, background: 'rgba(96,165,250,.16)', border: '1px solid rgba(96,165,250,.5)', borderRadius: 14, cursor: 'pointer' },
  card: { padding: 16, minWidth: 0, borderRadius: 20, border: `1px solid ${C.border}`, background: C.panel }, previewCard: { padding: 18, display: 'grid', gap: 16, borderRadius: 20, border: `1px solid ${C.border}`, background: C.panel }, cardHeader: { display: 'flex', justifyContent: 'space-between', gap: 12, flexWrap: 'wrap', alignItems: 'flex-start' }, panelTitle: { fontSize: 19, fontWeight: 900 },
  input: { width: '100%', boxSizing: 'border-box', minWidth: 0, padding: '11px 12px', color: C.text, background: C.panel2, border: `1px solid ${C.border}`, borderRadius: 11, outline: 'none' }, miniInput: { width: '100%', boxSizing: 'border-box', padding: '8px 9px', color: C.text, background: C.panel2, border: `1px solid ${C.border}`, borderRadius: 9 }, filterGrid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit,minmax(150px,1fr))', gap: 8 }, metricGrid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit,minmax(145px,1fr))', gap: 8 }, metricCard: { display: 'grid', gap: 7, padding: 10, fontSize: 12, color: C.muted, background: C.panel2, border: `1px solid ${C.border}`, borderRadius: 12 }, twoCol: { display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 6 }, sectionLabel: { margin: '14px 0 8px', color: C.subtle, fontSize: 11, fontWeight: 900, letterSpacing: '.08em', textTransform: 'uppercase' },
  fieldGroups: { maxHeight: 560, overflowY: 'auto', paddingRight: 3 }, fieldGrid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit,minmax(135px,1fr))', gap: 7 }, fieldButton: { display: 'grid', gap: 2, textAlign: 'left', padding: 9, color: C.text, background: C.panel2, border: `1px solid ${C.border}`, borderRadius: 10, cursor: 'pointer' }, fieldActive: { display: 'grid', gap: 2, textAlign: 'left', padding: 9, color: C.text, background: 'rgba(52,211,153,.13)', border: '1px solid rgba(52,211,153,.42)', borderRadius: 10, cursor: 'pointer' },
  primary: { padding: '11px 15px', color: C.text, fontWeight: 900, background: 'rgba(96,165,250,.2)', border: '1px solid rgba(96,165,250,.5)', borderRadius: 12, cursor: 'pointer' }, secondary: { padding: '11px 15px', color: C.text, fontWeight: 800, background: C.panel2, border: `1px solid ${C.border}`, borderRadius: 12, cursor: 'pointer' }, secondaryWide: { width: '100%', padding: '11px 15px', color: C.text, fontWeight: 800, background: C.panel2, border: `1px solid ${C.border}`, borderRadius: 12, cursor: 'pointer' }, populateWide: { width: '100%', padding: '14px 16px', color: C.text, fontWeight: 900, fontSize: 15, background: 'rgba(96,165,250,.22)', border: '1px solid rgba(96,165,250,.55)', borderRadius: 13, cursor: 'pointer' },
  check: { display: 'inline-flex', gap: 8, alignItems: 'center', color: C.muted, fontSize: 13 }, pillRow: { display: 'flex', gap: 8, flexWrap: 'wrap', alignItems: 'center' }, pill: { display: 'inline-flex', padding: '6px 10px', border: '1px solid', borderRadius: 999, fontSize: 12, fontWeight: 850 }, previewStats: { display: 'flex', gap: 8, flexWrap: 'wrap' }, empty: { padding: 14, color: C.muted, lineHeight: 1.5, border: `1px dashed ${C.border}`, borderRadius: 13, background: C.panel2 }, savedList: { display: 'grid', gap: 8 }, savedItem: { display: 'flex', justifyContent: 'space-between', gap: 10, textAlign: 'left', padding: 11, color: C.text, background: C.panel2, border: `1px solid ${C.border}`, borderRadius: 12, cursor: 'pointer' }, success: { marginBottom: 14, padding: 12, color: '#a7f3d0', background: 'rgba(52,211,153,.12)', border: '1px solid rgba(52,211,153,.35)', borderRadius: 12 }, error: { marginBottom: 14, padding: 12, color: '#fecaca', background: 'rgba(248,113,113,.12)', border: '1px solid rgba(248,113,113,.35)', borderRadius: 12 }, warning: { padding: 11, color: '#fde68a', background: 'rgba(251,191,36,.12)', border: '1px solid rgba(251,191,36,.35)', borderRadius: 12 },
  overlay: { position: 'fixed', inset: 0, zIndex: 1000, padding: 'clamp(8px,2vw,22px)', boxSizing: 'border-box', background: 'rgba(2,6,23,.88)', backdropFilter: 'blur(8px)', overflow: 'hidden' }, reportSurface: { height: '100%', display: 'grid', gridTemplateRows: 'auto minmax(0,1fr)', overflow: 'hidden', color: C.text, background: C.panel, border: `1px solid ${C.border}`, borderRadius: 20, boxShadow: '0 30px 90px rgba(0,0,0,.5)' }, reportHeader: { display: 'flex', justifyContent: 'space-between', gap: 16, flexWrap: 'wrap', alignItems: 'center', padding: 18, borderBottom: `1px solid ${C.border}` }, reportTitle: { margin: '5px 0', fontSize: 28 }, reportBody: { minHeight: 0, display: 'grid', gridTemplateColumns: '280px minmax(0,1fr)' }, columnPanel: { minHeight: 0, padding: 14, overflowY: 'auto', borderRight: `1px solid ${C.border}`, background: C.panel2 }, columnList: { display: 'grid', gap: 7, margin: '13px 0' }, columnRow: { display: 'flex', justifyContent: 'space-between', gap: 8, alignItems: 'center', padding: 8, border: `1px solid ${C.border}`, borderRadius: 10, background: C.panel }, columnLabel: { display: 'flex', gap: 7, alignItems: 'center', minWidth: 0, fontSize: 13 }, iconButton: { marginLeft: 4, padding: '4px 7px', color: C.text, background: C.panel3, border: `1px solid ${C.border}`, borderRadius: 7, cursor: 'pointer' }, gridPanel: { minWidth: 0, minHeight: 0, padding: 14, display: 'grid', gridTemplateRows: 'auto auto minmax(0,1fr)', gap: 10 }, gridToolbar: { display: 'flex', justifyContent: 'space-between', gap: 10, flexWrap: 'wrap' }, dataGridWrap: { minWidth: 0, minHeight: 0, overflow: 'auto', border: `1px solid ${C.border}`, borderRadius: 12, background: C.panel2 }, table: { width: 'max-content', minWidth: '100%', borderCollapse: 'separate', borderSpacing: 0 }, th: { position: 'sticky', top: 0, zIndex: 2, padding: 0, textAlign: 'left', whiteSpace: 'nowrap', background: C.panel3, borderBottom: `1px solid ${C.border}` }, sortButton: { width: '100%', padding: '11px 12px', textAlign: 'left', color: C.muted, fontWeight: 900, background: 'transparent', border: 0, cursor: 'pointer' }, td: { maxWidth: 360, padding: '11px 12px', color: C.text, fontSize: 13, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis', borderBottom: `1px solid ${C.border}` },
}

if (typeof document !== 'undefined' && !document.getElementById('mlbgpt-report-builder-responsive')) {
  const style = document.createElement('style'); style.id = 'mlbgpt-report-builder-responsive'; style.textContent = `
    @media (max-width: 1050px){main[style] .ignore{} }
    @media (max-width: 900px){.mlbgpt-never-used{} }
  `; document.head.appendChild(style)
}
