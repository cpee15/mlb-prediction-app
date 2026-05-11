import React, { useEffect, useMemo, useState } from 'react'

const API = import.meta.env.VITE_API_BASE_URL || ''
const SAVED_DASHBOARDS_KEY = 'mlb_saved_dashboards_v1'
const LOCAL_USER_KEY = 'mlb_dashboard_user_id'

const COMPONENTS = [
  { key: 'hitters', title: 'My Top Hitters Today', description: 'Unique hitter board from Batter vs Arsenal, pitch usage, damage quality, and model context.' },
  { key: 'pitchers', title: 'My Top Pitchers Today', description: 'Pitcher lean board using K profile, contact suppression, opponent offense, and arsenal context.' },
  { key: 'teams', title: 'My Top Teams Today', description: 'Team board from model side edge, expected runs, offense profile, and opponent weaknesses.' },
  { key: 'totals', title: 'My Top Totals Today', description: 'Game total watchlist from projected runs, run environment, and simulation context.' },
  { key: 'overall_players', title: 'My Top Overall Players Today', description: 'Combined unique player board blending hitter and pitcher model-solver scores.' },
]

const DEFAULT_METRICS = {
  hitters: ['xwOBA', 'xBA', 'EV', 'LA', 'HardHit', 'Usage', 'Pitcher xwOBA'],
  pitchers: ['K%', 'BB%', 'xwOBA Allowed', 'HardHit Allowed', 'Opp K%', 'Opp ISO', 'Score'],
  teams: ['Edge Score', 'Win Edge', 'Run Diff', 'ISO', 'OBP', 'SLG'],
  totals: ['Projected Total', 'Raw Total', 'Run Index', 'Score'],
  overall_players: ['Score', 'xwOBA', 'EV', 'K%', 'xwOBA Allowed'],
}

function ensureLocalUserId() {
  let id = window.localStorage.getItem(LOCAL_USER_KEY)
  if (!id) {
    id = `local-${Date.now()}-${Math.random().toString(16).slice(2)}`
    window.localStorage.setItem(LOCAL_USER_KEY, id)
  }
  return id
}

function loadSavedDashboards() {
  try {
    return JSON.parse(window.localStorage.getItem(SAVED_DASHBOARDS_KEY) || '[]')
  } catch {
    return []
  }
}

function saveSavedDashboards(rows) {
  window.localStorage.setItem(SAVED_DASHBOARDS_KEY, JSON.stringify(rows))
}

function cleanFilters(filters) {
  const out = {}
  Object.entries(filters || {}).forEach(([key, value]) => {
    if (key === 'metrics' || key === 'weights') return
    if (value !== '' && value !== null && value !== undefined) out[key] = value
  })
  const metrics = {}
  Object.entries(filters?.metrics || {}).forEach(([metric, rule]) => {
    const entry = {}
    if (rule?.min !== '' && rule?.min !== undefined && rule?.min !== null) entry.min = Number(rule.min)
    if (rule?.max !== '' && rule?.max !== undefined && rule?.max !== null) entry.max = Number(rule.max)
    if (Object.keys(entry).length) metrics[metric] = entry
  })
  const weights = {}
  Object.entries(filters?.weights || {}).forEach(([metric, value]) => {
    if (value !== '' && value !== undefined && value !== null && Number(value) !== 1) weights[metric] = Number(value)
  })
  if (Object.keys(metrics).length) out.metrics = metrics
  if (Object.keys(weights).length) out.weights = weights
  return out
}

function activeFilterChips(filters) {
  const clean = cleanFilters(filters)
  const chips = []
  Object.entries(clean).forEach(([key, value]) => {
    if (key === 'metrics') {
      Object.entries(value).forEach(([metric, rule]) => chips.push(`${metric}: ${rule.min ?? ''}${rule.min !== undefined ? '+' : ''}${rule.max !== undefined ? ` <= ${rule.max}` : ''}`))
    } else if (key === 'weights') {
      Object.entries(value).forEach(([metric, weight]) => chips.push(`${metric} weight ${weight}`))
    } else {
      chips.push(`${key}: ${value}`)
    }
  })
  return chips
}

export default function MyDashboardPage() {
  const today = new Date().toISOString().slice(0, 10)
  const [date, setDate] = useState(today)
  const [dateMode, setDateMode] = useState('today')
  const [results, setResults] = useState({})
  const [filters, setFilters] = useState({})
  const [enabled, setEnabled] = useState(Object.fromEntries(COMPONENTS.map(c => [c.key, true])))
  const [loading, setLoading] = useState({})
  const [errors, setErrors] = useState({})
  const [savedDashboards, setSavedDashboards] = useState([])
  const [dashboardName, setDashboardName] = useState('')
  const [dashboardDescription, setDashboardDescription] = useState('')
  const [userId, setUserId] = useState('')

  useEffect(() => {
    setUserId(ensureLocalUserId())
    setSavedDashboards(loadSavedDashboards())
  }, [])

  const effectiveDate = dateMode === 'today' ? today : date

  async function runSolver(component, overrideFilters = filters[component] || {}) {
    setLoading(prev => ({ ...prev, [component]: true }))
    setErrors(prev => ({ ...prev, [component]: null }))
    try {
      const payload = { date: effectiveDate, component, filters: cleanFilters(overrideFilters) }
      const res = await fetch(`${API}/my-dashboard/solver`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      })
      const json = await res.json()
      if (!res.ok) throw new Error(JSON.stringify(json.detail || json))
      setResults(prev => ({ ...prev, [component]: json }))
    } catch (err) {
      console.error('My Dashboard solver failed:', err)
      setErrors(prev => ({ ...prev, [component]: err.message || 'Solver failed' }))
    } finally {
      setLoading(prev => ({ ...prev, [component]: false }))
    }
  }

  async function runAll() {
    for (const component of COMPONENTS) {
      if (!enabled[component.key]) continue
      // eslint-disable-next-line no-await-in-loop
      await runSolver(component.key)
    }
  }

  function updateComponentFilters(component, nextFilters) {
    setFilters(prev => ({ ...prev, [component]: nextFilters }))
  }

  function resetComponentFilters(component) {
    const next = { ...filters, [component]: {} }
    setFilters(next)
    runSolver(component, {})
  }

  function currentDashboardConfig() {
    const components = {}
    COMPONENTS.forEach(component => {
      components[component.key] = {
        enabled: Boolean(enabled[component.key]),
        filters: cleanFilters(filters[component.key] || {}),
        weights: cleanFilters(filters[component.key] || {}).weights || {},
      }
    })
    return { components }
  }

  function saveCurrentDashboard() {
    const name = dashboardName.trim() || `My Dashboard ${new Date().toLocaleString()}`
    const now = new Date().toISOString()
    const row = {
      id: `dash-${Date.now()}-${Math.random().toString(16).slice(2)}`,
      user_id: userId || ensureLocalUserId(),
      name,
      description: dashboardDescription.trim(),
      date_mode: dateMode,
      fixed_date: dateMode === 'fixed' ? date : null,
      config: currentDashboardConfig(),
      created_at: now,
      updated_at: now,
      last_run_at: null,
      persistence: 'localStorage_v1',
    }
    const next = [row, ...savedDashboards]
    setSavedDashboards(next)
    saveSavedDashboards(next)
    setDashboardName('')
    setDashboardDescription('')
  }

  function deleteSavedDashboard(id) {
    const next = savedDashboards.filter(row => row.id !== id)
    setSavedDashboards(next)
    saveSavedDashboards(next)
  }

  function loadSavedDashboard(row) {
    setDateMode(row.date_mode || 'today')
    if (row.fixed_date) setDate(row.fixed_date)
    const nextFilters = {}
    const nextEnabled = { ...enabled }
    Object.entries(row.config?.components || {}).forEach(([component, config]) => {
      nextEnabled[component] = config.enabled !== false
      nextFilters[component] = { ...(config.filters || {}), weights: config.weights || config.filters?.weights || {} }
    })
    setEnabled(nextEnabled)
    setFilters(nextFilters)
  }

  async function rerunSavedDashboard(row) {
    loadSavedDashboard(row)
    const runDate = row.date_mode === 'fixed' && row.fixed_date ? row.fixed_date : today
    const nextResults = {}
    for (const component of COMPONENTS) {
      const config = row.config?.components?.[component.key]
      if (!config || config.enabled === false) continue
      setLoading(prev => ({ ...prev, [component.key]: true }))
      try {
        const payload = { date: runDate, component: component.key, filters: config.filters || {} }
        const res = await fetch(`${API}/my-dashboard/solver`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) })
        const json = await res.json()
        if (!res.ok) throw new Error(JSON.stringify(json.detail || json))
        nextResults[component.key] = json
      } catch (err) {
        setErrors(prev => ({ ...prev, [component.key]: err.message || 'Saved dashboard rerun failed' }))
      } finally {
        setLoading(prev => ({ ...prev, [component.key]: false }))
      }
    }
    setResults(prev => ({ ...prev, ...nextResults }))
    const now = new Date().toISOString()
    const next = savedDashboards.map(item => item.id === row.id ? { ...item, last_run_at: now } : item)
    setSavedDashboards(next)
    saveSavedDashboards(next)
  }

  return (
    <div>
      <section style={heroStyle}>
        <div>
          <div style={eyebrowStyle}>Personal analyst workspace prototype</div>
          <h1 style={titleStyle}>My Dashboard</h1>
          <p style={subtitleStyle}>Your daily analyst board for model edges, players, teams, totals, notes, future saved picks, and rerunnable report configurations.</p>
        </div>
        <div style={dateBoxStyle}>
          <label style={labelStyle}>Date Mode</label>
          <select value={dateMode} onChange={e => setDateMode(e.target.value)} style={inputStyle}>
            <option value="today">Today each time it runs</option>
            <option value="fixed">Fixed selected date</option>
          </select>
          <label style={labelStyle}>Board Date</label>
          <input type="date" value={date} onChange={e => setDate(e.target.value)} style={inputStyle} disabled={dateMode === 'today'} />
          <button onClick={runAll} style={primaryButtonStyle}>Populate All</button>
        </div>
      </section>

      <section style={signupBannerStyle}>
        <div>
          <strong>Save dashboards so your filters, weights, and model-solver setup can be rerun later.</strong>
          <p style={{ margin: '6px 0 0', color: '#8b949e', lineHeight: 1.5 }}>Full sign-in is coming soon. For now, saved dashboards are tied to this browser/session: <code>{userId || 'loading'}</code>.</p>
        </div>
        <button disabled style={disabledButtonStyle}>Real sign-in coming soon</button>
      </section>

      <SavedDashboardsPanel
        savedDashboards={savedDashboards}
        dashboardName={dashboardName}
        setDashboardName={setDashboardName}
        dashboardDescription={dashboardDescription}
        setDashboardDescription={setDashboardDescription}
        onSave={saveCurrentDashboard}
        onLoad={loadSavedDashboard}
        onRerun={rerunSavedDashboard}
        onDelete={deleteSavedDashboard}
      />

      <section style={gridStyle}>
        {COMPONENTS.map(component => (
          <DashboardCard
            key={component.key}
            component={component}
            result={results[component.key]}
            loading={loading[component.key]}
            error={errors[component.key]}
            filters={filters[component.key] || {}}
            enabled={enabled[component.key]}
            onEnabledChange={(value) => setEnabled(prev => ({ ...prev, [component.key]: value }))}
            onFilterChange={(next) => updateComponentFilters(component.key, next)}
            onRun={() => runSolver(component.key)}
            onReset={() => resetComponentFilters(component.key)}
          />
        ))}
      </section>
    </div>
  )
}

function SavedDashboardsPanel({ savedDashboards, dashboardName, setDashboardName, dashboardDescription, setDashboardDescription, onSave, onLoad, onRerun, onDelete }) {
  return (
    <section style={savedPanelStyle}>
      <div style={{ display: 'flex', justifyContent: 'space-between', gap: '14px', flexWrap: 'wrap', alignItems: 'flex-start' }}>
        <div>
          <h2 style={{ ...cardTitleStyle, fontSize: '22px' }}>My Saved Dashboards</h2>
          <p style={cardDescriptionStyle}>Save the report configuration, not stale result rows. Rerun uses fresh app-owned model data for the saved date mode and filters.</p>
        </div>
        <div style={saveBoxStyle}>
          <input value={dashboardName} onChange={e => setDashboardName(e.target.value)} placeholder="Dashboard name" style={inputStyle} />
          <input value={dashboardDescription} onChange={e => setDashboardDescription(e.target.value)} placeholder="Optional description" style={inputStyle} />
          <button onClick={onSave} style={primaryButtonStyle}>Save Current Dashboard</button>
        </div>
      </div>
      {savedDashboards.length === 0 ? (
        <div style={emptyStyle}>No saved dashboards yet. Configure filters/weights, then save the current dashboard so you can rerun it later.</div>
      ) : (
        <div style={savedListStyle}>
          {savedDashboards.map(row => (
            <div key={row.id} style={savedItemStyle}>
              <div>
                <strong>{row.name}</strong>
                <div style={metaStyle}>{row.description || 'No description'} | {row.date_mode} {row.fixed_date ? `| ${row.fixed_date}` : ''}</div>
                <div style={metaStyle}>Created {formatDate(row.created_at)} | Last run {row.last_run_at ? formatDate(row.last_run_at) : 'never'} | Components {Object.values(row.config?.components || {}).filter(c => c.enabled !== false).length}</div>
              </div>
              <div style={actionsStyle}>
                <button onClick={() => onLoad(row)} style={miniButtonStyle}>Load</button>
                <button onClick={() => onRerun(row)} style={miniPrimaryButtonStyle}>Rerun</button>
                <button onClick={() => onDelete(row.id)} style={miniDangerButtonStyle}>Delete</button>
              </div>
            </div>
          ))}
        </div>
      )}
    </section>
  )
}

function DashboardCard({ component, result, loading, error, filters, enabled, onEnabledChange, onFilterChange, onRun, onReset }) {
  const [showFilters, setShowFilters] = useState(false)
  const items = result?.items || []
  const metricNames = useMemo(() => {
    const fromResult = result?.available_filters?.metrics || []
    const fromItems = Array.from(new Set(items.flatMap(item => Object.keys(item.metrics || {}))))
    return fromResult.length ? fromResult : fromItems.length ? fromItems : (DEFAULT_METRICS[component.key] || [])
  }, [result, items, component.key])
  const chips = activeFilterChips(filters)
  return (
    <div style={{ ...cardStyle, opacity: enabled ? 1 : 0.68 }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', gap: '12px', alignItems: 'flex-start' }}>
        <div>
          <h2 style={cardTitleStyle}>{component.title}</h2>
          <p style={cardDescriptionStyle}>{component.description}</p>
        </div>
        <span style={countBadgeStyle}>{items.length || 0}/10</span>
      </div>

      <div style={toolbarStyle}>
        <label style={checkLabelStyle}><input type="checkbox" checked={enabled} onChange={e => onEnabledChange(e.target.checked)} /> Enabled</label>
        <button onClick={() => setShowFilters(!showFilters)} style={miniButtonStyle}>{showFilters ? 'Hide Filters' : 'Edit Filters'}</button>
      </div>

      <button onClick={onRun} disabled={loading || !enabled} style={solverButtonStyle}>{loading ? 'Solving...' : 'Use Model Solver To Populate Top 10 Today'}</button>
      {chips.length > 0 && <div style={chipWrapStyle}>{chips.map(chip => <span key={chip} style={filterChipStyle}>{chip}</span>)}</div>}
      {showFilters && <FilterPanel component={component} filters={filters} metricNames={metricNames} available={result?.available_filters} onChange={onFilterChange} onApply={onRun} onReset={onReset} />}
      {error && <div style={errorStyle}>{error}</div>}
      {!result && !loading && !error && <div style={emptyStyle}>No saved results yet. Run the model solver to populate this component from existing app-owned data.</div>}
      <div style={{ display: 'grid', gap: '10px', marginTop: '14px' }}>{items.map(item => <ResultItem key={item.dedupe_key || `${item.entity_type}-${item.entity_id}-${item.rank}`} item={item} />)}</div>
      {result && <details style={{ marginTop: '14px' }}><summary style={summaryStyle}>Data quality, filters, and missing data</summary><pre style={preStyle}>{JSON.stringify({ filters_applied: result.filters_applied, available_filters: result.available_filters, result_count_before_filters: result.result_count_before_filters, result_count_after_filters: result.result_count_after_filters, filter_warnings: result.filter_warnings, data_quality: result.data_quality, missing_data: result.missing_data }, null, 2)}</pre></details>}
    </div>
  )
}

function FilterPanel({ filters, metricNames, available, onChange, onApply, onReset }) {
  function setBasic(key, value) { onChange({ ...filters, [key]: value }) }
  function setMetric(metric, key, value) { onChange({ ...filters, metrics: { ...(filters.metrics || {}), [metric]: { ...((filters.metrics || {})[metric] || {}), [key]: value } } }) }
  function setWeight(metric, value) { onChange({ ...filters, weights: { ...(filters.weights || {}), [metric]: value } }) }
  return (
    <div style={filterPanelStyle}>
      <div style={filterSectionTitleStyle}>Basic Filters</div>
      <div style={filterGridStyle}>
        <input placeholder="Search player/team/reason" value={filters.search_text || ''} onChange={e => setBasic('search_text', e.target.value)} style={inputStyle} />
        <input placeholder="Team contains" value={filters.team || ''} onChange={e => setBasic('team', e.target.value)} style={inputStyle} />
        <input placeholder="Opponent contains" value={filters.opponent || ''} onChange={e => setBasic('opponent', e.target.value)} style={inputStyle} />
        <select value={filters.min_confidence || ''} onChange={e => setBasic('min_confidence', e.target.value)} style={inputStyle}><option value="">Any confidence</option><option value="low">Low+</option><option value="medium">Medium+</option><option value="high">High only</option></select>
        <input type="number" step="0.01" placeholder="Min score" value={filters.min_score || ''} onChange={e => setBasic('min_score', e.target.value)} style={inputStyle} />
        <input type="number" step="0.01" placeholder="Max score" value={filters.max_score || ''} onChange={e => setBasic('max_score', e.target.value)} style={inputStyle} />
        <input placeholder="Category exact" value={filters.category || ''} onChange={e => setBasic('category', e.target.value)} style={inputStyle} />
        <input placeholder="Player type hitter/pitcher" value={filters.player_type || ''} onChange={e => setBasic('player_type', e.target.value)} style={inputStyle} />
      </div>
      <div style={filterSectionTitleStyle}>Metric Filters</div>
      <div style={metricFilterGridStyle}>{metricNames.map(metric => <div key={metric} style={metricFilterStyle}><strong>{metric}</strong><input type="number" step="0.001" placeholder="min" value={filters.metrics?.[metric]?.min ?? ''} onChange={e => setMetric(metric, 'min', e.target.value)} style={smallInputStyle} /><input type="number" step="0.001" placeholder="max" value={filters.metrics?.[metric]?.max ?? ''} onChange={e => setMetric(metric, 'max', e.target.value)} style={smallInputStyle} /></div>)}</div>
      <div style={filterSectionTitleStyle}>Scoring Weights</div>
      <div style={metricFilterGridStyle}>{metricNames.map(metric => <div key={metric} style={metricFilterStyle}><strong>{metric}</strong><input type="range" min="0" max="2" step="0.05" value={filters.weights?.[metric] ?? 1} onChange={e => setWeight(metric, e.target.value)} /><span style={smallTextStyle}>{filters.weights?.[metric] ?? 1}</span></div>)}</div>
      {available?.categories?.length > 0 && <div style={smallTextStyle}>Known categories: {available.categories.join(', ')}</div>}
      <div style={actionsStyle}><button onClick={onApply} style={miniPrimaryButtonStyle}>Apply Filters</button><button onClick={onReset} style={miniButtonStyle}>Reset Filters</button><button disabled style={placeholderButtonStyle}>Save View coming soon</button></div>
    </div>
  )
}

function ResultItem({ item }) {
  const metrics = item.metrics || {}
  const chart = item.chart_data || { labels: [], values: [] }
  const maxValue = Math.max(...(chart.values || []).map(v => Math.abs(Number(v) || 0)), 1)
  return <div style={itemStyle}><div style={itemHeaderStyle}><div><div style={rankStyle}>#{item.rank} {item.entity_name || item.entity_id}</div><div style={metaStyle}>{item.player_type ? `${item.player_type} | ` : ''}{item.team || 'Team missing'} vs {item.opponent || 'Opponent missing'} {item.game_pk ? `| Game ${item.game_pk}` : ''}</div></div><div style={{ textAlign: 'right' }}><div style={scoreStyle}>{formatNumber(item.score)}</div><div style={confidenceStyle(item.confidence)}>{item.confidence || 'low'}</div></div></div><p style={reasonStyle}>{item.primary_reason || 'Model solver ranked this item from app-owned data.'}</p>{item.weight_explanation?.length > 0 && <div style={smallTextStyle}>{item.weight_explanation.join(' | ')}</div>}<ScoreBar value={Number(item.score) || 0} />{chart.labels?.length > 0 && <div style={barsWrapStyle}>{chart.labels.map((label, idx) => { const value = Number(chart.values[idx]) || 0; const width = Math.max(8, Math.min(100, Math.abs(value) / maxValue * 100)); return <div key={`${label}-${idx}`} style={metricRowStyle}><span style={metricLabelStyle}>{label}</span><div style={metricTrackStyle}><div style={{ ...metricFillStyle, width: `${width}%` }} /></div><span style={metricValueStyle}>{formatNumber(value)}</span></div> })}</div>}{item.best_pitch_angles?.length > 0 && <div style={pitchAnglesStyle}><strong>Best pitch angles:</strong>{item.best_pitch_angles.map((angle, idx) => <div key={`${angle.pitch_type}-${idx}`} style={smallTextStyle}>• {angle.reason}</div>)}</div>}<details><summary style={summaryStyle}>View reasoning</summary><ul style={reasonListStyle}>{(item.reasoning || []).map((reason, idx) => <li key={idx}>{reason}</li>)}</ul><pre style={preStyle}>{JSON.stringify({ metrics, base_score: item.base_score, adjusted_score: item.adjusted_score, missing_data: item.missing_data || [], source: item.source, dedupe_key: item.dedupe_key }, null, 2)}</pre></details><div style={actionsStyle}><button disabled style={placeholderButtonStyle}>Save</button><button disabled style={placeholderButtonStyle}>Tag</button><button disabled style={placeholderButtonStyle}>Add Note</button></div></div>
}

function ScoreBar({ value }) { const width = Math.max(4, Math.min(100, Math.abs(value) * 35)); return <div style={scoreTrackStyle}><div style={{ ...scoreFillStyle, width: `${width}%` }} /></div> }
function formatNumber(value) { const num = Number(value); if (!Number.isFinite(num)) return 'N/A'; return Math.abs(num) >= 10 ? num.toFixed(1) : num.toFixed(3) }
function formatDate(value) { try { return new Date(value).toLocaleString() } catch { return value || 'N/A' } }

const heroStyle = { display: 'flex', justifyContent: 'space-between', gap: '20px', flexWrap: 'wrap', background: 'linear-gradient(135deg, #161b22, #0d1117)', border: '1px solid #30363d', borderRadius: '16px', padding: '24px', marginBottom: '16px' }
const eyebrowStyle = { color: '#58a6ff', fontSize: '13px', fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.08em' }
const titleStyle = { margin: '6px 0 8px', fontSize: '34px', color: '#e6edf3' }
const subtitleStyle = { margin: 0, color: '#8b949e', maxWidth: '720px', lineHeight: 1.55 }
const dateBoxStyle = { minWidth: '220px', display: 'grid', gap: '8px', alignContent: 'start' }
const labelStyle = { color: '#8b949e', fontSize: '13px' }
const inputStyle = { background: '#0d1117', color: '#e6edf3', border: '1px solid #30363d', borderRadius: '8px', padding: '10px' }
const smallInputStyle = { ...inputStyle, padding: '7px', width: '82px' }
const primaryButtonStyle = { background: '#238636', color: '#fff', border: 0, borderRadius: '8px', padding: '10px 12px', cursor: 'pointer', fontWeight: 700 }
const signupBannerStyle = { display: 'flex', justifyContent: 'space-between', gap: '16px', alignItems: 'center', flexWrap: 'wrap', background: '#0f1a2e', border: '1px solid #1f6feb55', borderRadius: '14px', padding: '16px', marginBottom: '18px', color: '#e6edf3' }
const disabledButtonStyle = { background: '#21262d', color: '#8b949e', border: '1px solid #30363d', borderRadius: '8px', padding: '10px 12px' }
const savedPanelStyle = { ...signupBannerStyle, display: 'block', background: '#111827' }
const saveBoxStyle = { display: 'grid', gap: '8px', minWidth: '280px' }
const savedListStyle = { display: 'grid', gap: '10px', marginTop: '14px' }
const savedItemStyle = { display: 'flex', justifyContent: 'space-between', gap: '10px', flexWrap: 'wrap', background: '#0d1117', border: '1px solid #30363d', borderRadius: '10px', padding: '12px' }
const gridStyle = { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(360px, 1fr))', gap: '16px' }
const cardStyle = { background: '#161b22', border: '1px solid #30363d', borderRadius: '14px', padding: '16px', color: '#e6edf3' }
const cardTitleStyle = { margin: 0, fontSize: '20px' }
const cardDescriptionStyle = { color: '#8b949e', lineHeight: 1.45, margin: '6px 0 0', fontSize: '13px' }
const countBadgeStyle = { background: '#0d1117', border: '1px solid #30363d', borderRadius: '999px', padding: '4px 8px', color: '#8b949e', fontSize: '12px' }
const toolbarStyle = { display: 'flex', justifyContent: 'space-between', gap: '8px', alignItems: 'center', marginTop: '12px' }
const checkLabelStyle = { color: '#8b949e', fontSize: '12px' }
const solverButtonStyle = { width: '100%', marginTop: '12px', background: '#58a6ff', color: '#07111f', border: 0, borderRadius: '8px', padding: '10px', fontWeight: 800, cursor: 'pointer' }
const chipWrapStyle = { display: 'flex', gap: '6px', flexWrap: 'wrap', marginTop: '10px' }
const filterChipStyle = { background: '#0f1a2e', border: '1px solid #1f6feb55', borderRadius: '999px', color: '#c9d1d9', padding: '4px 8px', fontSize: '11px' }
const filterPanelStyle = { marginTop: '12px', background: '#0d1117', border: '1px solid #30363d', borderRadius: '10px', padding: '12px' }
const filterSectionTitleStyle = { color: '#58a6ff', fontSize: '12px', fontWeight: 800, textTransform: 'uppercase', margin: '10px 0 8px' }
const filterGridStyle = { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(150px, 1fr))', gap: '8px' }
const metricFilterGridStyle = { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: '8px' }
const metricFilterStyle = { background: '#010409', border: '1px solid #30363d', borderRadius: '8px', padding: '8px', display: 'grid', gap: '6px', color: '#c9d1d9', fontSize: '12px' }
const miniButtonStyle = { background: '#21262d', color: '#c9d1d9', border: '1px solid #30363d', borderRadius: '6px', padding: '7px 9px', cursor: 'pointer' }
const miniPrimaryButtonStyle = { ...miniButtonStyle, background: '#238636', color: '#fff' }
const miniDangerButtonStyle = { ...miniButtonStyle, background: '#3b1111', color: '#ffb4b4' }
const errorStyle = { background: '#2d1b1b', border: '1px solid #a33', borderRadius: '8px', padding: '10px', marginTop: '12px', color: '#ffb4b4' }
const emptyStyle = { marginTop: '14px', padding: '14px', background: '#0d1117', border: '1px dashed #30363d', borderRadius: '10px', color: '#8b949e', lineHeight: 1.45 }
const itemStyle = { background: '#0d1117', border: '1px solid #30363d', borderRadius: '10px', padding: '12px' }
const itemHeaderStyle = { display: 'flex', justifyContent: 'space-between', gap: '12px', alignItems: 'start' }
const rankStyle = { fontWeight: 800, color: '#e6edf3' }
const metaStyle = { color: '#8b949e', fontSize: '12px', marginTop: '4px' }
const scoreStyle = { fontWeight: 900, color: '#58a6ff' }
const confidenceStyle = (level) => ({ marginTop: '4px', color: level === 'high' ? '#3fb950' : level === 'medium' ? '#d29922' : '#8b949e', fontSize: '12px', textTransform: 'uppercase', fontWeight: 700 })
const reasonStyle = { color: '#c9d1d9', lineHeight: 1.45, fontSize: '13px' }
const scoreTrackStyle = { height: '8px', background: '#21262d', borderRadius: '999px', overflow: 'hidden', margin: '10px 0' }
const scoreFillStyle = { height: '100%', background: '#58a6ff', borderRadius: '999px' }
const barsWrapStyle = { display: 'grid', gap: '6px', margin: '10px 0' }
const metricRowStyle = { display: 'grid', gridTemplateColumns: '92px 1fr 52px', alignItems: 'center', gap: '8px' }
const metricLabelStyle = { color: '#8b949e', fontSize: '11px' }
const metricTrackStyle = { height: '6px', background: '#21262d', borderRadius: '999px', overflow: 'hidden' }
const metricFillStyle = { height: '100%', background: '#3fb950', borderRadius: '999px' }
const metricValueStyle = { color: '#c9d1d9', fontSize: '11px', textAlign: 'right' }
const pitchAnglesStyle = { margin: '10px 0', color: '#c9d1d9', fontSize: '12px', lineHeight: 1.5 }
const smallTextStyle = { color: '#8b949e', marginTop: '3px', fontSize: '12px' }
const summaryStyle = { cursor: 'pointer', color: '#58a6ff', fontSize: '13px', marginTop: '8px' }
const reasonListStyle = { color: '#c9d1d9', fontSize: '13px', lineHeight: 1.5, paddingLeft: '18px' }
const preStyle = { background: '#010409', border: '1px solid #30363d', borderRadius: '8px', padding: '10px', overflowX: 'auto', color: '#8b949e', fontSize: '11px' }
const actionsStyle = { display: 'flex', gap: '8px', marginTop: '10px', flexWrap: 'wrap' }
const placeholderButtonStyle = { background: '#21262d', color: '#8b949e', border: '1px solid #30363d', borderRadius: '6px', padding: '6px 8px', fontSize: '12px' }
