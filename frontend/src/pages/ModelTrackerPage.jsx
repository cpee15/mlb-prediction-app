import React, { useEffect, useMemo, useState } from 'react'
import { API_BASE, getMlbLiveDate } from '../lib/api'

const API = API_BASE

const s = {
  page: { display: 'grid', gap: 18, minWidth: 0, overflowX: 'hidden' },
  hero: { border: '1px solid var(--border-subtle)', borderRadius: 18, padding: 22, background: 'rgba(15, 23, 42, 0.72)', minWidth: 0 },
  header: { display: 'flex', justifyContent: 'space-between', gap: 16, alignItems: 'flex-start', flexWrap: 'wrap', minWidth: 0 },
  controls: { display: 'flex', gap: 10, alignItems: 'center', flexWrap: 'wrap' },
  statsScroller: { overflowX: 'auto', overflowY: 'hidden', WebkitOverflowScrolling: 'touch', scrollbarGutter: 'stable', padding: '2px 2px 10px', margin: '-2px -2px 0', scrollSnapType: 'x proximity' },
  statRail: { display: 'flex', gap: 12, width: 'max-content', minWidth: '100%' },
  card: { border: '1px solid var(--border-subtle)', borderRadius: 14, padding: 14, background: 'rgba(7, 11, 18, 0.55)', flex: '0 0 170px', minWidth: 170, scrollSnapAlign: 'start' },
  value: { fontSize: 28, fontWeight: 900, color: 'var(--text-primary)', letterSpacing: '-0.05em' },
  label: { fontSize: 11, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.08em', fontWeight: 850 },
  section: { border: '1px solid var(--border-subtle)', borderRadius: 16, padding: 16, background: 'rgba(15, 23, 42, 0.56)', minWidth: 0, overflow: 'hidden' },
  sectionHeader: { display: 'flex', justifyContent: 'space-between', gap: 12, alignItems: 'center', flexWrap: 'wrap', marginBottom: 12, minWidth: 0 },
  sectionTitle: { fontSize: 18, fontWeight: 900, color: 'var(--text-primary)' },
  filters: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: 10 },
  input: { width: '100%', boxSizing: 'border-box' },
  button: { cursor: 'pointer' },
  gameCard: { border: '1px solid var(--border-subtle)', borderRadius: 14, padding: 14, background: 'rgba(7, 11, 18, 0.42)', marginTop: 12, minWidth: 0, overflow: 'hidden' },
  gameTop: { display: 'flex', justifyContent: 'space-between', gap: 12, alignItems: 'flex-start', flexWrap: 'wrap', minWidth: 0 },
  gameTitle: { fontSize: 17, fontWeight: 900, color: 'var(--text-primary)' },
  rowRail: { overflowX: 'auto', overflowY: 'hidden', WebkitOverflowScrolling: 'touch', scrollbarGutter: 'stable', padding: '2px 2px 10px', margin: '10px -2px -2px', scrollSnapType: 'x proximity' },
  rowRailInner: { display: 'flex', gap: 12, width: 'max-content', minWidth: '100%', alignItems: 'stretch' },
  rowCard: { flex: '0 0 clamp(300px, 32vw, 380px)', minWidth: 300, maxWidth: 380, scrollSnapAlign: 'start', border: '1px solid var(--border-subtle)', borderRadius: 14, padding: 12, background: 'rgba(17, 24, 39, 0.82)', display: 'grid', gap: 10 },
  rowTitle: { color: 'var(--text-primary)', fontSize: 14, fontWeight: 900, overflowWrap: 'anywhere' },
  rowMeta: { color: 'var(--text-muted)', fontSize: 12, lineHeight: 1.45, overflowWrap: 'anywhere' },
  chipRail: { display: 'flex', flexWrap: 'wrap', gap: 8 },
  chip: { border: '1px solid rgba(148,163,184,0.24)', borderRadius: 999, padding: '4px 8px', fontSize: 11, color: 'var(--text-secondary)', fontWeight: 800, background: 'rgba(30,41,59,0.44)' },
  metricGrid: { display: 'grid', gridTemplateColumns: 'repeat(2, minmax(0, 1fr))', gap: 8 },
  metricCard: { border: '1px solid rgba(148,163,184,0.16)', borderRadius: 12, padding: 10, background: 'rgba(15,23,42,0.54)' },
  metricLabel: { color: 'var(--text-muted)', fontSize: 10, textTransform: 'uppercase', letterSpacing: '0.08em', fontWeight: 900, marginBottom: 6 },
  metricValue: { color: 'var(--text-primary)', fontWeight: 900, fontSize: 16, lineHeight: 1.2 },
  block: { border: '1px solid rgba(148,163,184,0.14)', borderRadius: 12, padding: 10, background: 'rgba(8,12,20,0.36)' },
  blockTitle: { color: 'var(--text-primary)', fontSize: 11, textTransform: 'uppercase', letterSpacing: '0.08em', fontWeight: 900, marginBottom: 8 },
  bulletList: { margin: 0, paddingLeft: 16, color: 'var(--text-secondary)', fontSize: 12, lineHeight: 1.45 },
  flipButton: { borderRadius: 10, border: '1px solid rgba(148,163,184,0.24)', background: 'rgba(30,41,59,0.52)', color: 'var(--text-primary)', padding: '9px 12px', fontSize: 12, fontWeight: 900, cursor: 'pointer' },
  tableWrap: { overflowX: 'auto', overflowY: 'hidden', WebkitOverflowScrolling: 'touch', border: '1px solid var(--border-subtle)', borderRadius: 14, scrollbarGutter: 'stable' },
  table: { width: 'max-content', minWidth: '100%', borderCollapse: 'collapse', fontSize: 12 },
  th: { textAlign: 'left', padding: '9px 10px', borderBottom: '1px solid var(--border-subtle)', color: 'var(--text-muted)', whiteSpace: 'nowrap' },
  td: { padding: '9px 10px', borderBottom: '1px solid rgba(148, 163, 184, 0.14)', color: 'var(--text-secondary)', whiteSpace: 'nowrap' },
}

function StatCard({ label, value }) {
  return <div style={s.card}><div style={s.label}>{label}</div><div style={s.value}>{value ?? 0}</div></div>
}

function fmt(value, digits = 3) {
  if (value === null || value === undefined || value === '') return 'Unavailable'
  const n = Number(value)
  if (Number.isNaN(n)) return String(value)
  return Number.isInteger(n) ? String(n) : n.toFixed(digits)
}

function textValue(value) {
  if (value === null || value === undefined || value === '') return ''
  if (typeof value === 'string') return value
  try { return JSON.stringify(value, null, 2) } catch { return String(value) }
}

function parseMaybeJson(value) {
  if (!value) return null
  if (typeof value === 'object') return value
  try { return JSON.parse(value) } catch { return null }
}

function safeArray(value) {
  return Array.isArray(value) ? value : []
}

function compactValue(value) {
  if (value === null || value === undefined || value === '') return 'Unavailable'
  if (Array.isArray(value)) return value.filter(Boolean).slice(0, 3).map(textValue).join(', ') || 'Unavailable'
  if (typeof value === 'object') {
    const keys = Object.keys(value)
    if (!keys.length) return 'Unavailable'
    return keys.slice(0, 3).map(key => `${key}: ${compactValue(value[key])}`).join(' · ')
  }
  return String(value)
}

function gradeLabel(row) {
  if (row.grade === 'pending') return 'Snapshot Pending'
  if (row.result_status === 'live') return 'Live Tracking'
  if (row.result_status === 'final' && row.grade === 'ungraded') return 'Final Ungraded'
  if (['won', 'lost', 'push', 'partial'].includes(row.grade)) return 'Graded'
  if (row.grade === 'watchlist_only') return 'Watchlist Only'
  if (row.grade === 'ungraded') return 'Needs Result Mapping'
  return row.grade || 'Untracked'
}

function topReasonList(row) {
  if (Array.isArray(row.reasoning)) return row.reasoning.slice(0, 4).map(item => compactValue(item))
  return [textValue(row.primary_reason)].filter(Boolean)
}

function topFeatureList(row) {
  return safeArray(row.features_used).filter(feature => feature && feature.name).slice(0, 6).map(feature => `${feature.name}: ${compactValue(feature.value)}`)
}

function metricList(row) {
  return [
    ['Score', fmt(row.score)], ['Confidence', fmt(row.confidence)], ['Edge', fmt(row.edge)], ['EV', fmt(row.expected_value)],
    ['Model Prob', fmt(row.model_probability)], ['Market Prob', fmt(row.market_implied_probability)],
    ['Home Win', fmt(row.home_win_probability)], ['Away Win', fmt(row.away_win_probability)],
    ['Proj Total', fmt(row.projected_total)], ['Proj Home', fmt(row.projected_home_runs)], ['Proj Away', fmt(row.projected_away_runs)],
  ].filter(([, value]) => value !== 'Unavailable').slice(0, 8)
}

function summarySentence(row) {
  const parts = []
  if (row.away_team || row.home_team) parts.push(`${row.away_team || 'Away'} at ${row.home_team || 'Home'} is the game context.`)
  if (row.pick_label || row.player_name || row.team_name) parts.push(`${row.pick_label || row.player_name || row.team_name} is the focus of this row.`)
  if (row.market_type || row.pick_type) parts.push(`The market lens is ${row.market_type || row.pick_type}.`)
  if (row.model_probability !== null && row.model_probability !== undefined) parts.push(`Model probability is ${fmt(row.model_probability)} versus market implied probability ${fmt(row.market_implied_probability)}.`)
  if (row.edge !== null && row.edge !== undefined) parts.push(`Current edge is ${fmt(row.edge)} with confidence ${fmt(row.confidence)}.`)
  const reason = topReasonList(row)[0]
  if (reason) parts.push(`Primary driver: ${reason}.`)
  if (textValue(row.grade_reason)) parts.push(`Result note: ${textValue(row.grade_reason)}.`)
  else if (row.grade === 'pending') parts.push('This snapshot is waiting for game resolution.')
  else if (row.grade === 'watchlist_only' || row.grade === 'ungraded') parts.push('This row is preserved for review and is not fully graded yet.')
  const text = parts.join(' ')
  return text.length > 620 ? `${text.slice(0, 617).trim()}...` : text
}

function gameFilterValue(value) {
  return String(value || 'ungrouped')
}

function gameLabel(rowOrGame) {
  const away = rowOrGame?.away_team || 'Away'
  const home = rowOrGame?.home_team || 'Home'
  if (rowOrGame?.away_team || rowOrGame?.home_team) return `${away} @ ${home}`
  if (rowOrGame?.game_pk) return `Game ${rowOrGame.game_pk}`
  return 'Ungrouped'
}

function TrackerRowCard({ row }) {
  const [flipped, setFlipped] = useState(false)
  const metrics = metricList(row)
  const reasons = topReasonList(row)
  const features = topFeatureList(row)
  const title = row.pick_label || row.player_name || row.team_name || row.model_name || 'Tracked output'

  return <article style={s.rowCard}>
    {!flipped ? <>
      <div>
        <div style={s.rowTitle}>{title}</div>
        <div style={s.rowMeta}>{row.source} / {row.source_component} · {row.market_type || row.pick_type || 'model'} · {gradeLabel(row)}</div>
        <div style={s.rowMeta}>{gameLabel(row)} · Game PK {row.game_pk || 'N/A'}{row.player_name ? ` · Player ${row.player_name}` : ''}</div>
      </div>
      <div style={s.chipRail}>
        {row.model_name && <span style={s.chip}>Model: {row.model_name}</span>}
        {row.model_version && <span style={s.chip}>Version: {row.model_version}</span>}
        {row.daily_odds_diagnostics?.confidence_tier && <span style={s.chip}>Tier: {row.daily_odds_diagnostics.confidence_tier}</span>}
        {row.daily_odds_diagnostics?.recommendation_status && <span style={s.chip}>Status: {row.daily_odds_diagnostics.recommendation_status}</span>}
      </div>
      <div style={s.metricGrid}>
        {metrics.map(([label, value]) => <div key={label} style={s.metricCard}><div style={s.metricLabel}>{label}</div><div style={s.metricValue}>{value}</div></div>)}
      </div>
      <div style={s.block}>
        <div style={s.blockTitle}>Reasoning</div>
        {reasons.length ? <ul style={s.bulletList}>{reasons.map((item, idx) => <li key={idx}>{item}</li>)}</ul> : <div style={s.rowMeta}>No explicit reasoning bullets were stored for this row.</div>}
      </div>
      <div style={s.block}>
        <div style={s.blockTitle}>Features used</div>
        {features.length ? <ul style={s.bulletList}>{features.map((item, idx) => <li key={idx}>{item}</li>)}</ul> : <div style={s.rowMeta}>No structured feature list was stored for this row.</div>}
      </div>
      <button type="button" style={s.flipButton} onClick={() => setFlipped(true)}>Flip for analyst view</button>
    </> : <>
      <div>
        <div style={s.rowTitle}>Analyst readout</div>
        <div style={s.rowMeta}>{title} · {gradeLabel(row)}</div>
      </div>
      <div style={s.block}><div style={s.rowMeta}>{summarySentence(row)}</div></div>
      <div style={s.block}><div style={s.blockTitle}>Result context</div><div style={s.rowMeta}>{textValue(row.grade_reason) || textValue(row.primary_reason) || 'No additional graded result context is available for this card.'}</div></div>
      <div style={s.block}><div style={s.blockTitle}>Formula snapshot</div><div style={s.rowMeta}>Model {row.model_name || 'unknown'} · score {fmt(row.score)} · confidence {fmt(row.confidence)} · edge {fmt(row.edge)} · EV {fmt(row.expected_value)}.</div></div>
      <button type="button" style={s.flipButton} onClick={() => setFlipped(false)}>Back to formulas</button>
    </>}
  </article>
}

function GameTrackerCard({ game }) {
  const title = gameLabel(game)
  const rows = safeArray(game.rows).slice().sort((a, b) => {
    const weight = row => {
      if (['won', 'lost', 'push', 'partial'].includes(row.grade)) return 5
      if (row.result_status === 'live') return 4
      if (row.grade === 'pending') return 3
      if (row.grade === 'watchlist_only') return 2
      return 1
    }
    return weight(b) - weight(a)
  }).slice(0, 10)

  return <article style={s.gameCard}>
    <div style={s.gameTop}>
      <div>
        <div style={s.gameTitle}>{title}</div>
        <div style={s.rowMeta}>Game PK {game.game_pk || 'N/A'} · {game.row_count} tracked outputs · Sources: {(game.sources || []).join(', ') || 'none'}</div>
      </div>
      <span className="status-badge">{Object.entries(game.grades || {}).map(([k, v]) => `${k}: ${v}`).join(' · ') || 'No grades'}</span>
    </div>
    {rows.length > 2 && <div style={s.rowMeta}>Swipe horizontally to inspect richer tracker cards and flip each card for analyst context.</div>}
    <div style={s.rowRail}><div style={s.rowRailInner}>{rows.length ? rows.map(row => <TrackerRowCard key={row.id || row.tracker_key} row={row} />) : <div style={s.rowMeta}>No model rows available for this game.</div>}</div></div>
  </article>
}

export default function ModelTrackerPage() {
  const [date, setDate] = useState(getMlbLiveDate())
  const [payload, setPayload] = useState(null)
  const [cacheByDate, setCacheByDate] = useState({})
  const [loading, setLoading] = useState(false)
  const [refreshing, setRefreshing] = useState(false)
  const [resultRefreshing, setResultRefreshing] = useState(false)
  const [error, setError] = useState(null)
  const [sourceFilter, setSourceFilter] = useState('all')
  const [gradeFilter, setGradeFilter] = useState('all')
  const [gameFilter, setGameFilter] = useState('all')
  const [search, setSearch] = useState('')

  function storePayload(nextDate, json) {
    setPayload(json)
    setCacheByDate(prev => ({ ...prev, [nextDate]: json }))
  }

  function load(force = false) {
    if (!force && cacheByDate[date]) {
      setPayload(cacheByDate[date])
      setLoading(false)
      setError(null)
      return
    }
    setLoading(true)
    setError(null)
    fetch(`${API}/model-tracker?date=${date}`)
      .then(async r => { if (!r.ok) throw new Error(`${r.status} ${r.statusText}: ${await r.text()}`); return r.json() })
      .then(json => { storePayload(date, json); setLoading(false) })
      .catch(err => { setError(String(err?.message || err)); setLoading(false) })
  }

  function refreshSnapshot() {
    setRefreshing(true)
    setError(null)
    fetch(`${API}/model-tracker/snapshot?date=${date}`, { method: 'POST' })
      .then(async r => {
        const text = await r.text()
        let json = null
        try { json = text ? JSON.parse(text) : null } catch { json = { raw: text } }
        if (!r.ok) throw new Error(`${r.status} ${r.statusText}: ${text}`)
        if (!json || Number(json.rows_collected || 0) === 0) {
          const errorText = JSON.stringify(json?.errors || json || {}, null, 2)
          throw new Error(`Snapshot saved 0 rows. Backend response:\n${errorText}`)
        }
        return json
      })
      .then(() => { setRefreshing(false); load(true) })
      .catch(err => { setError(String(err?.message || err)); setRefreshing(false) })
  }

  function refreshResults() {
    setResultRefreshing(true)
    setError(null)
    fetch(`${API}/model-tracker/results/refresh?date=${date}`, { method: 'POST' })
      .then(async r => { if (!r.ok) throw new Error(`${r.status} ${r.statusText}: ${await r.text()}`); return r.json() })
      .then(() => { setResultRefreshing(false); load(true) })
      .catch(err => { setError(String(err?.message || err)); setResultRefreshing(false) })
  }

  useEffect(() => { load(false) }, [date])

  const rows = useMemo(() => (payload?.rows || []).map(row => ({
    ...row,
    reasoning: parseMaybeJson(row.reasoning) || row.reasoning,
    features_used: parseMaybeJson(row.features_used) || row.features_used,
    missing_inputs: parseMaybeJson(row.missing_inputs) || row.missing_inputs,
    raw_payload: parseMaybeJson(row.raw_payload) || row.raw_payload,
    actual_result: parseMaybeJson(row.actual_result) || row.actual_result,
  })), [payload])

  const games = payload?.games || []
  const q = search.trim().toLowerCase()

  const searchMatches = row => {
    if (!q) return true
    return [row.pick_label, row.player_name, row.team_name, row.away_team, row.home_team, row.model_name, textValue(row.primary_reason), textValue(row.grade_reason)]
      .some(value => String(value || '').toLowerCase().includes(q))
  }

  const rowsForSourceOptions = useMemo(() => rows.filter(row => {
    if (gradeFilter !== 'all' && row.grade !== gradeFilter) return false
    if (gameFilter !== 'all' && gameFilterValue(row.game_pk) !== gameFilter) return false
    return true
  }), [rows, gradeFilter, gameFilter])

  const rowsForGradeOptions = useMemo(() => rows.filter(row => {
    if (sourceFilter !== 'all' && row.source !== sourceFilter) return false
    if (gameFilter !== 'all' && gameFilterValue(row.game_pk) !== gameFilter) return false
    return true
  }), [rows, sourceFilter, gameFilter])

  const rowsForGameOptions = useMemo(() => rows.filter(row => {
    if (sourceFilter !== 'all' && row.source !== sourceFilter) return false
    if (gradeFilter !== 'all' && row.grade !== gradeFilter) return false
    return true
  }), [rows, sourceFilter, gradeFilter])

  const sourceOptions = useMemo(() => ['all', ...Array.from(new Set(rowsForSourceOptions.map(r => r.source).filter(Boolean))).sort()], [rowsForSourceOptions])
  const gradeOptions = useMemo(() => ['all', ...Array.from(new Set(rowsForGradeOptions.map(r => r.grade).filter(Boolean))).sort()], [rowsForGradeOptions])
  const gameOptions = useMemo(() => {
    const gameMap = new Map()
    safeArray(games).forEach(game => {
      const value = gameFilterValue(game.game_pk)
      gameMap.set(value, { value, label: gameLabel(game) })
    })
    rowsForGameOptions.forEach(row => {
      const value = gameFilterValue(row.game_pk)
      if (!gameMap.has(value)) gameMap.set(value, { value, label: gameLabel(row) })
    })
    return [{ value: 'all', label: 'all' }, ...Array.from(gameMap.values()).sort((a, b) => a.label.localeCompare(b.label))]
  }, [games, rowsForGameOptions])

  useEffect(() => {
    if (!sourceOptions.includes(sourceFilter)) setSourceFilter('all')
  }, [sourceOptions, sourceFilter])

  useEffect(() => {
    if (!gradeOptions.includes(gradeFilter)) setGradeFilter('all')
  }, [gradeOptions, gradeFilter])

  useEffect(() => {
    if (!gameOptions.some(option => option.value === gameFilter)) setGameFilter('all')
  }, [gameOptions, gameFilter])

  const filteredRows = useMemo(() => {
    return rows.filter(row => {
      if (sourceFilter !== 'all' && row.source !== sourceFilter) return false
      if (gradeFilter !== 'all' && row.grade !== gradeFilter) return false
      if (gameFilter !== 'all' && gameFilterValue(row.game_pk) !== gameFilter) return false
      return searchMatches(row)
    })
  }, [rows, sourceFilter, gradeFilter, gameFilter, search])

  const filteredGames = useMemo(() => {
    const allowedIds = new Set(filteredRows.map(row => gameFilterValue(row.game_pk)))
    const filteredRowKeys = new Set(filteredRows.map(row => row.id || row.tracker_key).filter(Boolean))
    return games
      .map(game => ({ ...game, rows: safeArray(game.rows).filter(row => filteredRowKeys.has(row.id || row.tracker_key)) }))
      .filter(game => allowedIds.has(gameFilterValue(game.game_pk)))
  }, [games, filteredRows])

  const filteredSummary = useMemo(() => ({
    total_rows: filteredRows.length,
    games_tracked: filteredGames.length,
    pending_rows: filteredRows.filter(r => r.grade === 'pending').length,
    live_rows: filteredRows.filter(r => r.result_status === 'live').length,
    graded_rows: filteredRows.filter(r => ['won', 'lost', 'push', 'partial'].includes(r.grade)).length,
    won: filteredRows.filter(r => r.grade === 'won').length,
    lost: filteredRows.filter(r => r.grade === 'lost').length,
    ungraded_rows: filteredRows.filter(r => ['ungraded', 'watchlist_only'].includes(r.grade)).length,
  }), [filteredRows, filteredGames])

  return <div style={s.page}>
    <section style={s.hero}>
      <div style={s.header}>
        <div>
          <p className="page-kicker">Model Accountability</p>
          <h1 className="page-title">Model Tracker</h1>
          <p className="page-subtitle">Daily snapshots of model outputs, live comparison state, final-result grading where safe, and ungraded watchlist preservation.</p>
        </div>
        <div style={s.controls}>
          <input className="input-control" type="date" value={date} onChange={e => setDate(e.target.value)} />
          <button className="button-primary" type="button" style={s.button} onClick={refreshSnapshot} disabled={refreshing}>{refreshing ? 'Saving...' : 'Refresh Snapshot'}</button>
          <button className="button-secondary" type="button" style={s.button} onClick={refreshResults} disabled={resultRefreshing}>{resultRefreshing ? 'Comparing...' : 'Refresh Results'}</button>
        </div>
      </div>
    </section>

    {error && <div className="state-panel error">{error}</div>}
    {loading && <div className="state-panel">Loading tracker rows...</div>}

    <section style={s.statsScroller} aria-label="Model Tracker summary cards"><div style={s.statRail}><StatCard label="Visible Rows" value={filteredSummary.total_rows} /><StatCard label="Visible Games" value={filteredSummary.games_tracked} /><StatCard label="Pending" value={filteredSummary.pending_rows} /><StatCard label="Live" value={filteredSummary.live_rows} /><StatCard label="Graded" value={filteredSummary.graded_rows} /><StatCard label="Won" value={filteredSummary.won} /><StatCard label="Lost" value={filteredSummary.lost} /><StatCard label="Ungraded" value={filteredSummary.ungraded_rows} /></div></section>

    <section style={s.section}>
      <div style={s.sectionHeader}><div style={s.sectionTitle}>Filters</div><span className="status-badge">{filteredRows.length} visible rows · {cacheByDate[date] ? 'cached date loaded' : 'fresh request'}</span></div>
      <div style={s.filters}>
        <label><div style={s.label}>Source</div><select className="input-control" style={s.input} value={sourceFilter} onChange={e => setSourceFilter(e.target.value)}>{sourceOptions.map(source => <option key={source} value={source}>{source}</option>)}</select></label>
        <label><div style={s.label}>Grade</div><select className="input-control" style={s.input} value={gradeFilter} onChange={e => setGradeFilter(e.target.value)}>{gradeOptions.map(grade => <option key={grade} value={grade}>{grade}</option>)}</select></label>
        <label><div style={s.label}>Game</div><select className="input-control" style={s.input} value={gameFilter} onChange={e => setGameFilter(e.target.value)}>{gameOptions.map(game => <option key={game.value} value={game.value}>{game.label}</option>)}</select></label>
        <label><div style={s.label}>Search</div><input className="input-control" style={s.input} value={search} onChange={e => setSearch(e.target.value)} placeholder="player, team, pick, reason" /></label>
      </div>
    </section>

    <section style={s.section}>
      <div style={s.sectionHeader}><div><div style={s.sectionTitle}>Game Grouped Tracker</div><div style={s.rowMeta}>The front of each card shows formulas, metrics, features, and missing inputs. Flip the card for a concise analyst readout tied to the same stored row.</div></div></div>
      {filteredGames.length === 0 && <div className="state-panel">No tracker rows found. Click Refresh Snapshot to save today's model outputs.</div>}
      {filteredGames.map(game => <GameTrackerCard key={gameFilterValue(game.game_pk)} game={game} />)}
    </section>

    <section style={s.section}>
      <div style={s.sectionHeader}><div><div style={s.sectionTitle}>Table View</div><div style={s.rowMeta}>Every stored model output, including ungraded watchlist rows and final comparison state. Scroll horizontally to inspect every column.</div></div></div>
      <div style={s.tableWrap}><table style={s.table}><thead><tr><th style={s.th}>Date</th><th style={s.th}>Source</th><th style={s.th}>Game</th><th style={s.th}>Type</th><th style={s.th}>Pick</th><th style={s.th}>Player/Team</th><th style={s.th}>Model</th><th style={s.th}>Score</th><th style={s.th}>Confidence</th><th style={s.th}>Line</th><th style={s.th}>Price</th><th style={s.th}>Status</th><th style={s.th}>Grade</th><th style={s.th}>Reason</th></tr></thead><tbody>{filteredRows.map(row => <tr key={row.id || row.tracker_key}><td style={s.td}>{row.snapshot_date}</td><td style={s.td}>{row.source}</td><td style={s.td}>{gameLabel(row)}</td><td style={s.td}>{row.market_type || row.pick_type}</td><td style={s.td}>{row.pick_label || 'N/A'}</td><td style={s.td}>{row.player_name || row.team_name || 'N/A'}</td><td style={s.td}>{row.model_name || 'N/A'}</td><td style={s.td}>{fmt(row.score)}</td><td style={s.td}>{fmt(row.confidence)}</td><td style={s.td}>{fmt(row.line)}</td><td style={s.td}>{fmt(row.price, 0)}</td><td style={s.td}>{row.result_status}</td><td style={s.td}>{row.grade}</td><td style={{ ...s.td, whiteSpace: 'pre-wrap', minWidth: 300, maxWidth: 520 }}>{textValue(row.grade_reason || row.primary_reason || 'N/A')}</td></tr>)}</tbody></table></div>
    </section>
  </div>
}
