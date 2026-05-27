import React, { useEffect, useMemo, useState } from 'react'

const API = import.meta.env.VITE_API_BASE_URL || ''
const CACHE_PREFIX = 'my-dashboard:v2:'

const COMPONENTS = [
  { key: 'hitters', title: 'My Top Hitters Today', description: 'Stored 365 hitter board with pitch-type matchup, EV, LA, and arsenal context.' },
  { key: 'pitchers', title: 'My Top Pitchers Today', description: 'Pitcher lean board using K profile, contact suppression, and opponent context.' },
  { key: 'teams', title: 'My Top Teams Today', description: 'Team board from model side edge, projected runs, and offense profile.' },
  { key: 'totals', title: 'My Top Totals Today', description: 'Game total board from projected runs, run environment, and simulation context.' },
  { key: 'overall_players', title: 'My Top Overall Players Today', description: 'Combined player board blending hitter and pitcher solver outputs.' },
]

const FEATURE_CHOICES = ['Matchups', 'Daily Odds', 'Model Projections', 'News', 'Props', 'Pitchers', 'Batters']
const BASIC_FILTERS = { search_text: '', team: '', opponent: '', min_score: '', max_score: '', min_confidence: '', category: '', player_type: '', pitch_type: '', source: '' }

function todayIso() { return new Date().toISOString().slice(0, 10) }
function clone(value) { return JSON.parse(JSON.stringify(value)) }
function emptyFilters() { return { ...BASIC_FILTERS, metrics: {}, weights: {} } }
function defaultFiltersByComponent() { return Object.fromEntries(COMPONENTS.map(component => [component.key, emptyFilters()])) }
function formatNumber(value) { const num = Number(value); if (!Number.isFinite(num)) return '—'; return Math.abs(num) >= 10 ? num.toFixed(1) : num.toFixed(3) }
function cacheKey(kind, payload) { return `${CACHE_PREFIX}${kind}:${JSON.stringify(payload)}` }
function readSessionCache(key) { if (typeof window === 'undefined' || !window.sessionStorage) return null; try { const raw = window.sessionStorage.getItem(key); return raw ? JSON.parse(raw) : null } catch { return null } }
function writeSessionCache(key, value) { if (typeof window === 'undefined' || !window.sessionStorage) return; try { window.sessionStorage.setItem(key, JSON.stringify(value)) } catch {} }

function cleanFilters(filters) {
  const source = filters || {}
  const cleaned = {}
  for (const [key, value] of Object.entries(source)) {
    if (key === 'metrics' || key === 'weights') continue
    if (value !== '' && value !== null && value !== undefined) cleaned[key] = value
  }
  const metrics = {}
  for (const [metric, rules] of Object.entries(source.metrics || {})) {
    const entry = {}
    if (rules?.min !== '' && rules?.min !== null && rules?.min !== undefined) entry.min = Number(rules.min)
    if (rules?.max !== '' && rules?.max !== null && rules?.max !== undefined) entry.max = Number(rules.max)
    if (Object.keys(entry).length) metrics[metric] = entry
  }
  if (Object.keys(metrics).length) cleaned.metrics = metrics
  const weights = {}
  for (const [metric, value] of Object.entries(source.weights || {})) {
    if (value === '' || value === null || value === undefined) continue
    const num = Number(value)
    if (Number.isFinite(num)) weights[metric] = num
  }
  if (Object.keys(weights).length) cleaned.weights = weights
  return cleaned
}

function available(result, componentKey) {
  const defaults = {
    hitters: { pitch_types: [], suggested_metric_filters: ['EV', 'LA', 'Pitches Seen', 'xwOBA', 'HardHit', 'Usage'], suggested_weight_metrics: ['EV', 'LA', 'Pitches Seen', 'xwOBA', 'Usage'] },
    pitchers: { suggested_metric_filters: ['K%', 'xwOBA Allowed', 'HardHit Allowed', 'Opp K%', 'Score'], suggested_weight_metrics: ['K%', 'xwOBA Allowed', 'HardHit Allowed', 'Score'] },
    teams: { suggested_metric_filters: ['Edge Score', 'Win Edge', 'Run Diff', 'ISO', 'OBP'], suggested_weight_metrics: ['Edge Score', 'Win Edge', 'Run Diff', 'ISO'] },
    totals: { suggested_metric_filters: ['Projected Total', 'Raw Total', 'Run Index', 'Score'], suggested_weight_metrics: ['Projected Total', 'Run Index', 'Score'] },
    overall_players: { suggested_metric_filters: ['Score'], suggested_weight_metrics: ['Score'] },
  }
  return { ...(defaults[componentKey] || {}), ...(result?.available_filters || {}) }
}

export default function MyDashboardWorkspacePage() {
  const today = todayIso()
  const [authChecked, setAuthChecked] = useState(false)
  const [profile, setProfile] = useState(null)
  const [workspace, setWorkspace] = useState(null)
  const [results, setResults] = useState({})
  const [filters, setFilters] = useState(defaultFiltersByComponent)
  const [loading, setLoading] = useState({})
  const [runErrors, setRunErrors] = useState({})
  const [saveMessage, setSaveMessage] = useState(null)
  const [authError, setAuthError] = useState(null)
  const [savingProfile, setSavingProfile] = useState(false)
  const [form, setForm] = useState({ email: '', username: '', password: '', feature_interests: ['Matchups', 'Model Projections'], wants_newsletter: false, plan_type: 'free' })

  useEffect(() => {
    async function bootstrap() {
      try {
        const res = await fetch(`${API}/my-dashboard/profile`, { credentials: 'include' })
        const json = await res.json()
        if (json.authenticated) {
          setProfile(json.user)
          await loadWorkspace()
          await runAllBoards({ preferCache: true })
        }
      } finally {
        setAuthChecked(true)
      }
    }
    bootstrap()
  }, [])

  async function loadWorkspace() {
    const res = await fetch(`${API}/my-dashboard/workspace`, { credentials: 'include' })
    const json = await res.json()
    if (!res.ok) throw new Error(typeof json.detail === 'string' ? json.detail : JSON.stringify(json.detail || json))
    setWorkspace(json)
    return json
  }

  async function handleProfileSubmit(event) {
    event.preventDefault()
    setSavingProfile(true)
    setAuthError(null)
    try {
      const res = await fetch(`${API}/my-dashboard/profile`, { method: 'POST', credentials: 'include', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(form) })
      const json = await res.json()
      if (!res.ok) throw new Error(typeof json.detail === 'string' ? json.detail : JSON.stringify(json.detail || json))
      setProfile(json.user)
      await loadWorkspace()
      await runAllBoards({ preferCache: true })
    } catch (err) {
      setAuthError(err.message || 'Failed to create dashboard profile')
    } finally {
      setSavingProfile(false)
      setAuthChecked(true)
    }
  }

  function toggleInterest(choice) {
    setForm(prev => ({ ...prev, feature_interests: prev.feature_interests.includes(choice) ? prev.feature_interests.filter(item => item !== choice) : [...prev.feature_interests, choice] }))
  }

  function setBasicFilter(componentKey, key, value) {
    setFilters(prev => ({ ...prev, [componentKey]: { ...prev[componentKey], [key]: value } }))
  }

  function setMetricFilter(componentKey, metric, side, value) {
    setFilters(prev => {
      const next = clone(prev)
      const entry = { ...(next[componentKey].metrics?.[metric] || {}) }
      entry[side] = value
      if ((entry.min || '') === '' && (entry.max || '') === '') delete next[componentKey].metrics[metric]
      else next[componentKey].metrics[metric] = entry
      return next
    })
  }

  function setWeight(componentKey, metric, value) {
    setFilters(prev => ({ ...prev, [componentKey]: { ...prev[componentKey], weights: { ...(prev[componentKey].weights || {}), [metric]: value } } }))
  }

  function resetFilters(componentKey) {
    setFilters(prev => ({ ...prev, [componentKey]: emptyFilters() }))
  }

  async function runBoard(componentKey, options = {}) {
    const payload = { date: today, component: componentKey, filters: cleanFilters(filters[componentKey] || {}) }
    const sessionKey = cacheKey('board', payload)
    if (options.preferCache) {
      const cached = readSessionCache(sessionKey)
      if (cached) {
        setResults(prev => ({ ...prev, [componentKey]: cached }))
        return
      }
    }
    setLoading(prev => ({ ...prev, [componentKey]: true }))
    setRunErrors(prev => ({ ...prev, [componentKey]: null }))
    try {
      const res = await fetch(`${API}/my-dashboard/solver`, { method: 'POST', credentials: 'include', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) })
      const json = await res.json()
      if (!res.ok) throw new Error(typeof json.detail === 'string' ? json.detail : JSON.stringify(json.detail || json))
      writeSessionCache(sessionKey, json)
      setResults(prev => ({ ...prev, [componentKey]: json }))
    } catch (err) {
      setRunErrors(prev => ({ ...prev, [componentKey]: err.message || 'Board run failed' }))
    } finally {
      setLoading(prev => ({ ...prev, [componentKey]: false }))
    }
  }

  async function runAllBoards(options = {}) {
    const keys = COMPONENTS.map(component => component.key)
    const payload = { date: today, components: keys, filters_by_component: Object.fromEntries(keys.map(key => [key, cleanFilters(filters[key] || {})])), active_lineups: false }
    const sessionKey = cacheKey('batch', payload)
    if (options.preferCache) {
      const cached = readSessionCache(sessionKey)
      if (cached?.results) {
        setResults(prev => ({ ...prev, ...(cached.results || {}) }))
        return
      }
    }
    setLoading(Object.fromEntries(keys.map(key => [key, true])))
    setRunErrors({})
    try {
      const res = await fetch(`${API}/my-dashboard/solver/batch`, { method: 'POST', credentials: 'include', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) })
      const json = await res.json()
      if (!res.ok) throw new Error(typeof json.detail === 'string' ? json.detail : JSON.stringify(json.detail || json))
      writeSessionCache(sessionKey, json)
      setResults(prev => ({ ...prev, ...(json.results || {}) }))
    } catch (err) {
      setRunErrors(prev => ({ ...prev, _all: err.message || 'Populate all failed' }))
    } finally {
      setLoading(Object.fromEntries(keys.map(key => [key, false])))
    }
  }

  async function saveItemToToday(component, item) {
    if (!workspace?.today_folder_id) return
    setSaveMessage(null)
    try {
      const res = await fetch(`${API}/my-dashboard/items`, {
        method: 'POST', credentials: 'include', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({
          folder_id: workspace.today_folder_id,
          source_tab: 'my-dashboard',
          source_type: 'solver_result',
          title: `${component.title} | ${item.entity_name || 'Saved item'}`,
          subtitle: item.primary_reason || component.description,
          payload_json: { saved_from_component: component.key, saved_on_date: today, entity_name: item.entity_name, entity_id: item.entity_id, entity_type: item.entity_type, score: item.score, confidence: item.confidence, metrics: item.metrics || {}, reasoning: item.reasoning || [], max_filters: 10 },
          filter_json: cleanFilters(filters[component.key] || {}),
          sort_json: { by: 'score', direction: 'desc' },
        }) })
      const json = await res.json()
      if (!res.ok) throw new Error(typeof json.detail === 'string' ? json.detail : JSON.stringify(json.detail || json))
      await loadWorkspace()
      setSaveMessage(`Saved ${item.entity_name || 'item'} to Today`)
    } catch (err) {
      setSaveMessage(err.message || 'Failed to save item')
    }
  }

  const todayFolder = useMemo(() => (workspace?.folders || []).find(folder => folder.id === workspace?.today_folder_id), [workspace])

  if (!authChecked) return <div style={stateStyle}>Loading dashboard workspace…</div>

  if (!profile) {
    return <div style={pageStyle}><section style={heroStyle}><div><div style={eyebrowStyle}>My Dashboard</div><h1 style={titleStyle}>Create your analyst profile</h1><p style={subtitleStyle}>Restore stronger daily board filters, slider weights, and faster reruns from cached board results.</p></div></section><form onSubmit={handleProfileSubmit} style={panelStyle}><label style={labelStyle}>Email</label><input style={inputStyle} value={form.email} onChange={e => setForm(prev => ({ ...prev, email: e.target.value }))} /><label style={labelStyle}>Username</label><input style={inputStyle} value={form.username} onChange={e => setForm(prev => ({ ...prev, username: e.target.value }))} /><label style={labelStyle}>Password (optional)</label><input style={inputStyle} type="password" value={form.password} onChange={e => setForm(prev => ({ ...prev, password: e.target.value }))} /><div style={pillWrapStyle}>{FEATURE_CHOICES.map(choice => <button key={choice} type="button" onClick={() => toggleInterest(choice)} style={form.feature_interests.includes(choice) ? activePillStyle : pillStyle}>{choice}</button>)}</div>{authError && <div style={errorStyle}>{authError}</div>}<button type="submit" style={primaryButtonStyle} disabled={savingProfile}>{savingProfile ? 'Creating…' : 'Enter My Dashboard'}</button></form></div>
  }

  return (
    <div style={pageStyle}>
      <section style={heroStyle}>
        <div>
          <div style={eyebrowStyle}>My Dashboard</div>
          <h1 style={titleStyle}>Welcome back, {profile.username}</h1>
          <p style={subtitleStyle}>Restore stronger filters, hitter pitch-type controls, EV/LA thresholds, pitches-seen filters, and weight sliders. Solver reruns are also cached in your browser session.</p>
        </div>
        <div style={summaryCardStyle}>
          <div style={metaStyle}>Email: {profile.email}</div>
          <div style={metaStyle}>Plan: {profile.preferences?.plan_type || 'free'}</div>
          <div style={metaStyle}>Today saved: {todayFolder?.item_count || 0}</div>
        </div>
      </section>

      <section style={panelStyle}>
        <div style={rowStyle}><h2 style={sectionTitleStyle}>Daily boards</h2><div style={rowStyle}><button onClick={() => runAllBoards()} style={primaryButtonStyle}>Populate all</button><button onClick={loadWorkspace} style={secondaryButtonStyle}>Refresh workspace</button></div></div>
        {runErrors._all && <div style={errorStyle}>{runErrors._all}</div>}
        {saveMessage && <div style={successStyle}>{saveMessage}</div>}
        <div style={boardGridStyle}>
          {COMPONENTS.map(component => {
            const result = results[component.key]
            const items = result?.items || []
            const helper = available(result, component.key)
            const metricFilters = (helper.suggested_metric_filters || []).slice(0, 6)
            const weightMetrics = (helper.suggested_weight_metrics || []).slice(0, 5)
            const filterState = filters[component.key] || emptyFilters()
            return <div key={component.key} style={boardCardStyle}>
              <div style={rowStyle}><div><div style={boardTitleStyle}>{component.title}</div><div style={boardDescriptionStyle}>{component.description}</div></div><div style={countBadgeStyle}>{items.length}/10</div></div>
              <div style={metaStyle}>Before/after filters: {result?.result_count_before_filters ?? '—'} / {result?.result_count_after_filters ?? '—'}</div>
              {result?.filter_warnings?.length ? <div style={warningStyle}>{result.filter_warnings.join(' • ')}</div> : null}

              <div style={filterGridStyle}>
                <input style={smallInputStyle} placeholder="Search" value={filterState.search_text || ''} onChange={e => setBasicFilter(component.key, 'search_text', e.target.value)} />
                <input style={smallInputStyle} placeholder="Team" value={filterState.team || ''} onChange={e => setBasicFilter(component.key, 'team', e.target.value)} />
                <input style={smallInputStyle} placeholder="Opponent" value={filterState.opponent || ''} onChange={e => setBasicFilter(component.key, 'opponent', e.target.value)} />
                <input style={smallInputStyle} placeholder="Min score" value={filterState.min_score || ''} onChange={e => setBasicFilter(component.key, 'min_score', e.target.value)} />
                <input style={smallInputStyle} placeholder="Max score" value={filterState.max_score || ''} onChange={e => setBasicFilter(component.key, 'max_score', e.target.value)} />
                <select style={smallInputStyle} value={filterState.min_confidence || ''} onChange={e => setBasicFilter(component.key, 'min_confidence', e.target.value)}><option value="">Any confidence</option><option value="low">Low+</option><option value="medium">Medium+</option><option value="high">High only</option></select>
                {component.key === 'hitters' ? <select style={smallInputStyle} value={filterState.pitch_type || ''} onChange={e => setBasicFilter(component.key, 'pitch_type', e.target.value)}><option value="">Any pitch type</option>{(helper.pitch_types || []).map(value => <option key={value} value={value}>{value}</option>)}</select> : null}
                {(helper.categories || []).length ? <select style={smallInputStyle} value={filterState.category || ''} onChange={e => setBasicFilter(component.key, 'category', e.target.value)}><option value="">Any category</option>{(helper.categories || []).map(value => <option key={value} value={value}>{value}</option>)}</select> : null}
              </div>

              <div style={metricGridStyle}>{metricFilters.map(metric => <div key={`${component.key}-${metric}`} style={metricCardStyle}><div style={metricTitleStyle}>{metric}</div><div style={metricInputRowStyle}><input style={smallInputStyle} placeholder="Min" value={filterState.metrics?.[metric]?.min || ''} onChange={e => setMetricFilter(component.key, metric, 'min', e.target.value)} /><input style={smallInputStyle} placeholder="Max" value={filterState.metrics?.[metric]?.max || ''} onChange={e => setMetricFilter(component.key, metric, 'max', e.target.value)} /></div></div>)}</div>

              <div style={sliderGridStyle}>{weightMetrics.map(metric => { const value = Number(filterState.weights?.[metric] ?? 1); return <div key={`${component.key}-weight-${metric}`} style={metricCardStyle}><div style={metricTitleStyle}>{metric}</div><input type="range" min="0" max="2" step="0.1" value={value} onChange={e => setWeight(component.key, metric, e.target.value)} style={{ width: '100%' }} /><div style={metaStyle}>Weight {value.toFixed(1)}</div></div> })}</div>

              <div style={rowStyle}><button onClick={() => runBoard(component.key)} style={secondaryButtonStyle} disabled={loading[component.key]}>{loading[component.key] ? 'Running…' : 'Run board'}</button><button onClick={() => runBoard(component.key, { preferCache: true })} style={ghostButtonStyle}>Use cached</button><button onClick={() => resetFilters(component.key)} style={ghostButtonStyle}>Reset filters</button></div>
              {runErrors[component.key] && <div style={errorStyle}>{runErrors[component.key]}</div>}

              <div style={resultsGridStyle}>{items.length === 0 ? <div style={emptyStyle}>No results yet. Run this board to populate fresh discovery for today.</div> : items.map((item, idx) => <div key={`${component.key}-${idx}-${item.entity_id || item.entity_name || idx}`} style={resultCardStyle}><div style={rowStyle}><div><div style={resultTitleStyle}>{item.entity_name || 'Ranked item'}</div><div style={metaStyle}>{item.team || 'Team unavailable'} {item.opponent ? `vs ${item.opponent}` : ''}</div></div><div style={scorePillStyle}>{formatNumber(item.score)}</div></div><div style={resultBodyStyle}>{item.primary_reason || 'Model solver ranked this item.'}</div><div style={metaStyle}>Confidence: {item.confidence || 'low'}</div>{item.pitch_name || item.pitch_type ? <div style={metaStyle}>Pitch context: {item.pitch_name || item.pitch_type}</div> : null}<div style={pillWrapStyle}>{Object.entries(item.metrics || {}).slice(0, 6).map(([key, value]) => <span key={`${item.entity_id}-${key}`} style={metricPillStyle}>{key}: {value ?? '—'}</span>)}</div>{item.weight_explanation?.length ? <div style={metaStyle}>Weights: {item.weight_explanation.join(' • ')}</div> : null}<div style={rowStyle}><button onClick={() => saveItemToToday(component, item)} style={miniPrimaryStyle}>Save to Today</button></div></div>)}</div>
            </div>
          })}
        </div>
      </section>
    </div>
  )
}

const pageStyle = { display: 'grid', gap: 18 }
const heroStyle = { display: 'flex', justifyContent: 'space-between', gap: 18, flexWrap: 'wrap', background: 'linear-gradient(135deg, #141925, #0d1117)', border: '1px solid #2a3243', borderRadius: 18, padding: 24 }
const eyebrowStyle = { color: '#8ab4ff', fontSize: 12, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: 8 }
const titleStyle = { margin: 0, fontSize: 34, color: '#e6edf3' }
const subtitleStyle = { margin: '10px 0 0', color: '#97a3b6', maxWidth: 760, lineHeight: 1.6 }
const panelStyle = { background: '#161b22', border: '1px solid #30363d', borderRadius: 18, padding: 20, display: 'grid', gap: 14 }
const summaryCardStyle = { minWidth: 260, background: '#161b22', border: '1px solid #30363d', borderRadius: 16, padding: 18, display: 'grid', gap: 8 }
const sectionTitleStyle = { margin: 0, color: '#e6edf3', fontSize: 24 }
const rowStyle = { display: 'flex', justifyContent: 'space-between', gap: 12, flexWrap: 'wrap', alignItems: 'center' }
const boardGridStyle = { display: 'grid', gap: 18 }
const boardCardStyle = { background: '#0d1117', border: '1px solid #30363d', borderRadius: 16, padding: 16, display: 'grid', gap: 12 }
const boardTitleStyle = { color: '#e6edf3', fontWeight: 700, fontSize: 18 }
const boardDescriptionStyle = { color: '#97a3b6', fontSize: 13, lineHeight: 1.5, marginTop: 4 }
const countBadgeStyle = { background: '#1d2432', border: '1px solid #334155', color: '#c9d1d9', borderRadius: 999, padding: '6px 10px', fontSize: 12, fontWeight: 700 }
const filterGridStyle = { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(160px, 1fr))', gap: 10 }
const metricGridStyle = { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: 10 }
const sliderGridStyle = { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: 10 }
const resultsGridStyle = { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(240px, 1fr))', gap: 12 }
const metricCardStyle = { background: '#111827', border: '1px solid #30363d', borderRadius: 12, padding: 10, display: 'grid', gap: 8 }
const metricTitleStyle = { color: '#e6edf3', fontWeight: 600, fontSize: 12 }
const metricInputRowStyle = { display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8 }
const resultCardStyle = { background: '#111827', border: '1px solid #30363d', borderRadius: 14, padding: 12, display: 'grid', gap: 8 }
const resultTitleStyle = { color: '#e6edf3', fontWeight: 700 }
const resultBodyStyle = { color: '#97a3b6', fontSize: 13, lineHeight: 1.5 }
const scorePillStyle = { background: '#1d4ed8', color: '#fff', borderRadius: 999, padding: '6px 10px', fontSize: 12, fontWeight: 700, height: 'fit-content' }
const pillWrapStyle = { display: 'flex', flexWrap: 'wrap', gap: 6 }
const metricPillStyle = { background: '#0b1220', border: '1px solid #223049', color: '#c9d1d9', borderRadius: 999, padding: '4px 8px', fontSize: 11 }
const labelStyle = { color: '#c9d1d9', fontSize: 13, fontWeight: 600 }
const inputStyle = { background: '#0d1117', color: '#e6edf3', border: '1px solid #30363d', borderRadius: 10, padding: '10px 12px' }
const smallInputStyle = { background: '#111827', color: '#e6edf3', border: '1px solid #30363d', borderRadius: 8, padding: '8px 10px', fontSize: 12, width: '100%' }
const primaryButtonStyle = { background: '#7c3aed', color: '#fff', border: 0, borderRadius: 10, padding: '12px 16px', fontWeight: 700, cursor: 'pointer' }
const secondaryButtonStyle = { background: '#0d1117', color: '#e6edf3', border: '1px solid #30363d', borderRadius: 10, padding: '10px 14px', fontWeight: 600, cursor: 'pointer' }
const ghostButtonStyle = { background: '#0f172a', color: '#97a3b6', border: '1px solid #30363d', borderRadius: 10, padding: '10px 12px', fontWeight: 600, cursor: 'pointer' }
const miniPrimaryStyle = { background: '#2563eb', color: '#fff', border: 0, borderRadius: 8, padding: '8px 10px', fontSize: 12, fontWeight: 600, cursor: 'pointer' }
const pillStyle = { background: '#0d1117', color: '#c9d1d9', border: '1px solid #30363d', borderRadius: 999, padding: '8px 12px', cursor: 'pointer' }
const activePillStyle = { ...pillStyle, background: '#1c365d', borderColor: '#3b82f6' }
const errorStyle = { background: '#2a1215', border: '1px solid #5f1d24', color: '#ff9aa2', borderRadius: 10, padding: '10px 12px' }
const warningStyle = { background: '#2a2112', border: '1px solid #6b4f1d', color: '#f6d28b', borderRadius: 10, padding: '10px 12px' }
const successStyle = { background: '#112818', border: '1px solid #1f6f3d', color: '#9be9a8', borderRadius: 10, padding: '10px 12px' }
const stateStyle = { padding: 30, color: '#97a3b6' }
const metaStyle = { color: '#97a3b6', fontSize: 12 }
const emptyStyle = { background: '#0b1220', border: '1px dashed #30363d', color: '#97a3b6', borderRadius: 12, padding: 14, textAlign: 'center' }
