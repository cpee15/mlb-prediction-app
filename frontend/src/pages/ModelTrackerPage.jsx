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
  scrollHint: { color: 'var(--text-muted)', fontSize: 11, marginTop: 8, letterSpacing: '0.04em', textTransform: 'uppercase', fontWeight: 800 },
  miniScroller: { overflowX: 'auto', overflowY: 'hidden', WebkitOverflowScrolling: 'touch', scrollbarGutter: 'stable', padding: '2px 2px 10px', margin: '10px -2px -2px', scrollSnapType: 'x proximity' },
  miniRail: { display: 'flex', gap: 12, width: 'max-content', minWidth: '100%', alignItems: 'stretch' },
  rowShell: { position: 'relative', flex: '0 0 clamp(296px, 31vw, 380px)', minWidth: 296, maxWidth: 380, scrollSnapAlign: 'start', minHeight: 410, perspective: '1400px' },
  rowFlip: { position: 'relative', width: '100%', minHeight: 410, transition: 'transform 420ms cubic-bezier(0.22, 1, 0.36, 1)', transformStyle: 'preserve-3d' },
  rowFace: { position: 'absolute', inset: 0, border: '1px solid var(--border-subtle)', borderRadius: 16, padding: 14, background: 'linear-gradient(180deg, rgba(17,24,39,0.92) 0%, rgba(10,14,23,0.94) 100%)', boxSizing: 'border-box', backfaceVisibility: 'hidden', display: 'grid', gap: 12, minHeight: 410, overflow: 'hidden', boxShadow: '0 14px 40px rgba(2, 6, 23, 0.26)' },
  rowFaceBack: { transform: 'rotateY(180deg)' },
  faceHeader: { display: 'flex', justifyContent: 'space-between', gap: 10, alignItems: 'flex-start' },
  rowTitle: { color: 'var(--text-primary)', fontSize: 14, fontWeight: 900, lineHeight: 1.35, overflowWrap: 'anywhere' },
  rowMeta: { color: 'var(--text-muted)', fontSize: 12, lineHeight: 1.45, overflowWrap: 'anywhere' },
  chipRail: { display: 'flex', flexWrap: 'wrap', gap: 8 },
  chip: { border: '1px solid rgba(148,163,184,0.24)', borderRadius: 999, padding: '5px 8px', fontSize: 11, color: 'var(--text-secondary)', fontWeight: 800, letterSpacing: '0.02em', background: 'rgba(30,41,59,0.44)' },
  chipStrong: { border: '1px solid rgba(96,165,250,0.35)', background: 'rgba(30,64,175,0.22)', color: '#dbeafe' },
  metricGrid: { display: 'grid', gridTemplateColumns: 'repeat(2, minmax(0, 1fr))', gap: 8 },
  metricCard: { border: '1px solid rgba(148,163,184,0.16)', borderRadius: 12, padding: 10, background: 'rgba(15,23,42,0.54)', minHeight: 62 },
  metricValue: { color: 'var(--text-primary)', fontWeight: 900, fontSize: 16, lineHeight: 1.2 },
  metricLabel: { color: 'var(--text-muted)', fontSize: 10, textTransform: 'uppercase', letterSpacing: '0.08em', fontWeight: 900, marginBottom: 6 },
  sectionBlock: { border: '1px solid rgba(148,163,184,0.14)', borderRadius: 12, padding: 10, background: 'rgba(8,12,20,0.36)' },
  sectionBlockTitle: { color: 'var(--text-primary)', fontSize: 11, textTransform: 'uppercase', letterSpacing: '0.08em', fontWeight: 900, marginBottom: 8 },
  bulletList: { margin: 0, paddingLeft: 16, color: 'var(--text-secondary)', fontSize: 12, lineHeight: 1.45 },
  flipButton: { alignSelf: 'end', justifySelf: 'start', borderRadius: 10, border: '1px solid rgba(148,163,184,0.24)', background: 'rgba(30,41,59,0.52)', color: 'var(--text-primary)', padding: '9px 12px', fontSize: 12, fontWeight: 900, letterSpacing: '0.03em', cursor: 'pointer' },
  summaryBox: { border: '1px solid rgba(96,165,250,0.2)', borderRadius: 14, background: 'linear-gradient(180deg, rgba(30,41,59,0.78) 0%, rgba(15,23,42,0.84) 100%)', padding: 12, color: 'var(--text-secondary)', fontSize: 12.5, lineHeight: 1.58, overflow: 'auto' },
  tableWrap: { overflowX: 'auto', overflowY: 'hidden', WebkitOverflowScrolling: 'touch', border: '1px solid var(--border-subtle)', borderRadius: 14, scrollbarGutter: 'stable' },
  table: { width: 'max-content', minWidth: '100%', borderCollapse: 'collapse', fontSize: 12 },
  th: { textAlign: 'left', padding: '9px 10px', borderBottom: '1px solid var(--border-subtle)', color: 'var(--text-muted)', whiteSpace: 'nowrap' },
  td: { padding: '9px 10px', borderBottom: '1px solid rgba(148, 163, 184, 0.14)', color: 'var(--text-secondary)', whiteSpace: 'nowrap' },
  statusLine: { display: 'flex', justifyContent: 'space-between', gap: 10, alignItems: 'center', flexWrap: 'wrap' },
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

function safeArray(value) {
  return Array.isArray(value) ? value : []
}

function safeObject(value) {
  return value && typeof value === 'object' && !Array.isArray(value) ? value : {}
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

function parseMaybeJson(value) {
  if (!value) return null
  if (typeof value === 'object') return value
  try {
    return JSON.parse(value)
  } catch {
    return null
  }
}

function compactValue(value) {
  if (value === null || value === undefined || value === '') return 'Unavailable'
  if (Array.isArray(value)) return value.filter(Boolean).slice(0, 3).join(', ') || 'Unavailable'
  if (typeof value === 'object') {
    const keys = Object.keys(value)
    if (keys.length === 0) return 'Unavailable'
    return keys.slice(0, 3).map(key => `${key}: ${compactValue(value[key])}`).join(' · ')
  }
  return String(value)
}

function topFeatureList(row) {
  const features = safeArray(row.features_used)
  return features
    .filter(feature => feature && feature.name)
    .slice(0, 6)
    .map(feature => `${feature.name}: ${compactValue(feature.value)}`)
}

function topReasonList(row) {
  const reasoning = row.reasoning
  if (Array.isArray(reasoning)) {
    return reasoning.slice(0, 4).map(item => compactValue(item))
  }
  const canonical = safeObject(row.canonical_diagnostics)
  const dailyOdds = safeObject(row.daily_odds_diagnostics)
  const drivers = safeArray(dailyOdds.drivers)
  const items = [
    ...drivers.slice(0, 3),
    canonical.final_probability_source ? `Probability source: ${canonical.final_probability_source}` : null,
    dailyOdds.recommendation_status ? `Recommendation: ${dailyOdds.recommendation_status}` : null,
  ].filter(Boolean)
  if (items.length) return items.slice(0, 4)
  return row.primary_reason ? [row.primary_reason] : []
}

function missingInputSummary(row) {
  const missing = safeArray(row.missing_inputs)
  if (!missing.length) return 'No material missing inputs surfaced for this row.'
  return missing.slice(0, 5).join(', ')
}

function cardTone(row) {
  if (row.grade === 'won') return { badge: 'Won', accent: '#86efac' }
  if (row.grade === 'lost') return { badge: 'Lost', accent: '#fda4af' }
  if (row.grade === 'push') return { badge: 'Push', accent: '#fcd34d' }
  if (row.result_status === 'live') return { badge: 'Live', accent: '#93c5fd' }
  if (row.grade === 'watchlist_only') return { badge: 'Watchlist', accent: '#c4b5fd' }
  if (row.grade === 'ungraded') return { badge: 'Ungraded', accent: '#fdba74' }
  return { badge: 'Pending', accent: '#67e8f9' }
}

function summarySentence(row) {
  const bits = []
  const teams = row.away_team || row.home_team ? `${row.away_team || 'Away'} at ${row.home_team || 'Home'}` : null
  const entity = row.player_name || row.team_name || row.pick_label || row.model_name
  if (teams) bits.push(`${teams} is the game context.`)
  if (entity) bits.push(`${entity} is the focal output on this card.`)
  if (row.market_type || row.pick_type) bits.push(`The market lens is ${row.market_type || row.pick_type}.`)
  if (row.model_probability !== null && row.model_probability !== undefined) bits.push(`Model probability sits at ${fmt(row.model_probability)} with market implied probability at ${fmt(row.market_implied_probability)}.`)
  if (row.edge !== null && row.edge !== undefined) bits.push(`The current edge reads ${fmt(row.edge)} and confidence is ${fmt(row.confidence)}.`)
  if (row.expected_value !== null && row.expected_value !== undefined && row.expected_value !== 'Unavailable') bits.push(`Expected value is ${fmt(row.expected_value)}.`)
  const reason = topReasonList(row)[0]
  if (reason) bits.push(`Primary driver: ${reason}.`)
  if (row.grade_reason) bits.push(`Result note: ${row.grade_reason}.`)
  else if (row.grade === 'watchlist_only' || row.grade === 'ungraded') bits.push('This row is preserved for review and is not fully graded yet.')
  else if (row.grade === 'pending') bits.push('The snapshot is saved and waiting for game resolution.')
  const text = bits.join(' ')
  return text.length > 620 ? `${text.slice(0, 617).trim()}...` : text
}

function keyMetrics(row) {
  return [
    { label: 'Score', value: fmt(row.score) },
    { label: 'Confidence', value: fmt(row.confidence) },
    { label: 'Edge', value: fmt(row.edge) },
    { label: 'EV', value: fmt(row.expected_value) },
    { label: 'Model Prob', value: fmt(row.model_probability) },
    { label: 'Market Prob', value: fmt(row.market_implied_probability) },
    { label: 'Home Win', value: fmt(row.home_win_probability) },
    { label: 'Away Win', value: fmt(row.away_win_probability) },
    { label: 'Proj Total', value: fmt(row.projected_total) },
    { label: 'Proj Home', value: fmt(row.projected_home_runs) },
    { label: 'Proj Away', value: fmt(row.projected_away_runs) },
    { label: 'Line / Price', value: `${fmt(row.line)} / ${fmt(row.price, 0)}` },
  ].filter(metric => metric.value !== 'Unavailable' && metric.value !== 'Unavailable / Unavailable').slice(0, 8)
}

function RowFlipCard({ row, isFlipped, onToggle, reducedMotion }) {
  const tone = cardTone(row)
  const title = row.pick_label || row.player_name || row.team_name || row.model_name || 'Tracked output'
  const features = topFeatureList(row)
  const reasons = topReasonList(row)
  const metrics = keyMetrics(row)
  const flipTransform = isFlipped ? 'rotateY(180deg)' : 'rotateY(0deg)'
  const flipStyle = reducedMotion ? { ...s.rowFlip, transition: 'none', transform: 'none' } : { ...s.rowFlip, transform: flipTransform }
  const frontVisibility = reducedMotion ? { display: isFlipped ? 'none' : 'grid' } : null
  const backVisibility = reducedMotion ? { display: isFlipped ? 'grid' : 'none' } : null

  return <article style={s.rowShell}>
    <div style={flipStyle}>
      <section aria-hidden={isFlipped && !reducedMotion} style={{ ...s.rowFace, borderColor: tone.accent, ...frontVisibility }}>
        <div style={s.faceHeader}>
          <div>
            <div style={s.rowTitle}>{title}</div>
            <div style={s.rowMeta}>{row.source} / {row.source_component} · {row.market_type || row.pick_type || 'model'} · {gradeLabel(row)}</div>
            <div style={s.rowMeta}>{row.away_team || 'Away'} @ {row.home_team || 'Home'} · Game PK {row.game_pk || 'N/A'}{row.player_name ? ` · Player ${row.player_name}` : ''}</div>
          </div>
          <span className="status-badge" style={{ borderColor: tone.accent, color: tone.accent }}>{tone.badge}</span>
        </div>

        <div style={s.chipRail}>
          {row.model_name && <span style={{ ...s.chip, ...s.chipStrong }}>Model: {row.model_name}</span>}
          {row.model_version && <span style={s.chip}>Version: {row.model_version}</span>}
          {row.daily_odds_diagnostics?.confidence_tier && <span style={s.chip}>Tier: {row.daily_odds_diagnostics.confidence_tier}</span>}
          {row.daily_odds_diagnostics?.recommendation_status && <span style={s.chip}>Status: {row.daily_odds_diagnostics.recommendation_status}</span>}
          {row.daily_odds_diagnostics?.rejection_reason && <span style={s.chip}>Reject: {row.daily_odds_diagnostics.rejection_reason}</span>}
        </div>

        <div style={s.metricGrid}>
          {metrics.map(metric => <div key={metric.label} style={s.metricCard}><div style={s.metricLabel}>{metric.label}</div><div style={s.metricValue}>{metric.value}</div></div>)}
        </div>

        <div style={s.sectionBlock}>
          <div style={s.sectionBlockTitle}>Reasoning</div>
          {reasons.length ? <ul style={s.bulletList}>{reasons.map((item, idx) => <li key={idx}>{item}</li>)}</ul> : <div style={s.rowMeta}>{row.primary_reason || 'No explicit reasoning bullets were stored for this row.'}</div>}
        </div>

        <div style={s.sectionBlock}>
          <div style={s.sectionBlockTitle}>Features used</div>
          {features.length ? <ul style={s.bulletList}>{features.map((item, idx) => <li key={idx}>{item}</li>)}</ul> : <div style={s.rowMeta}>No structured feature list was stored for this row.</div>}
        </div>

        <div style={s.sectionBlock}>
          <div style={s.sectionBlockTitle}>Missing inputs</div>
          <div style={s.rowMeta}>{missingInputSummary(row)}</div>
        </div>

        <button type="button" style={s.flipButton} aria-pressed={isFlipped} aria-label={`${isFlipped ? 'Show formula side' : 'Show analyst summary side'} for ${title}`} onClick={onToggle}>{isFlipped ? 'Show formulas' : 'Flip for analyst view'}</button>
      </section>

      <section aria-hidden={!isFlipped && !reducedMotion} style={{ ...s.rowFace, ...s.rowFaceBack, borderColor: tone.accent, ...backVisibility }}>
        <div style={s.faceHeader}>
          <div>
            <div style={s.rowTitle}>Analyst readout</div>
            <div style={s.rowMeta}>{title} · {gradeLabel(row)}</div>
          </div>
          <span className="status-badge" style={{ borderColor: tone.accent, color: tone.accent }}>{tone.badge}</span>
        </div>

        <div style={s.summaryBox}>{summarySentence(row)}</div>

        <div style={s.sectionBlock}>
          <div style={s.sectionBlockTitle}>Result context</div>
          <div style={s.rowMeta}>{row.grade_reason || row.primary_reason || 'No additional graded result context is available for this card.'}</div>
        </div>

        <div style={s.sectionBlock}>
          <div style={s.sectionBlockTitle}>Formula snapshot</div>
          <div style={s.rowMeta}>Model {row.model_name || 'unknown'} · score {fmt(row.score)} · confidence {fmt(row.confidence)} · edge {fmt(row.edge)} · EV {fmt(row.expected_value)}.</div>
        </div>

        <button type="button" style={s.flipButton} aria-pressed={isFlipped} aria-label={`${isFlipped ? 'Show formula side' : 'Show analyst summary side'} for ${title}`} onClick={onToggle}>{isFlipped ? 'Back to formulas' : 'Flip to summary'}</button>
      </section>
    </div>
  </article>
}

function GameTrackerCard({ game, reducedMotion }) {
  const title = game.game_pk ? `${game.away_team || 'Away'} @ ${game.home_team || 'Home'}` : 'Ungrouped model outputs'
  const rows = game.rows || []
  const prioritizedRows = rows
    .filter(row => row)
    .sort((a, b) => {
      const weight = row => {
        if (['won', 'lost', 'push', 'partial'].includes(row.grade)) return 5
        if (row.result_status === 'live') return 4
        if (row.grade === 'pending') return 3
        if (row.grade === 'watchlist_only') return 2
        return 1
      }
      return weight(b) - weight(a)
    })
    .slice(0, 10)
  const [flipped, setFlipped] = useState({})

  return <article style={s.gameCard}>
    <div style={s.gameTop}>
      <div>
        <div style={s.gameTitle}>{title}</div>
        <div style={s.rowMeta}>Game PK {game.game_pk || 'N/A'} · {game.row_count} tracked outputs · Sources: {(game.sources || []).join(', ') || 'none'}</div>
      </div>
      <span className="status-badge">{Object.entries(game.grades || {}).map(([k, v]) => `${k}: ${v}`).join(' · ') || 'No grades'}</span>
    </div>
    {prioritizedRows.length > 2 && <div style={s.scrollHint}>Swipe horizontally to inspect richer tracker cards and flip each card for analyst context.</div>}
    <div style={s.miniScroller}>
      <div style={s.miniRail}>
        {prioritizedRows.length === 0 && <div style={s.rowMeta}>No model rows available for this game.</div>}
        {prioritizedRows.map(row => <RowFlipCard key={row.id || row.tracker_key} row={row} reducedMotion={reducedMotion} isFlipped={Boolean(flipped[row.id || row.tracker_key])} onToggle={() => setFlipped(prev => ({ ...prev, [row.id || row.tracker_key]: !prev[row.id || row.tracker_key] }))} />)}
      </div>
    </div>
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
  const reducedMotion = typeof window !== 'undefined' && window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches

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
  const sources = useMemo(() => ['all', ...Array.from(new Set(rows.map(r => r.source).filter(Boolean))).sort()], [rows])
  const grades = useMemo(() => ['all', ...Array.from(new Set(rows.map(r => r.grade).filter(Boolean))).sort()], [rows])
  const gameOptions = useMemo(() => ['all', ...games.map(g => String(g.game_pk || 'ungrouped'))], [games])

  const filteredRows = useMemo(() => {
    const q = search.trim().toLowerCase()
    return rows.filter(row => {
      if (sourceFilter !== 'all' && row.source !== sourceFilter) return false
      if (gradeFilter !== 'all' && row.grade !== gradeFilter) return false
      if (gameFilter !== 'all' && String(row.game_pk || 'ungrouped') !== gameFilter) return false
      if (!q) return true
      return [row.pick_label, row.player_name, row.team_name, row.away_team, row.home_team, row.model_name, row.primary_reason, row.grade_reason]
        .some(value => String(value || '').toLowerCase().includes(q))
    })
  }, [rows, sourceFilter, gradeFilter, gameFilter, search])

  const filteredGames = useMemo(() => {
    const allowedIds = new Set(filteredRows.map(row => String(row.game_pk || 'ungrouped')))
    return games.map(game => ({ ...game, rows: (game.rows || []).map(row => filteredRows.find(fr => fr.id === row.id || fr.tracker_key === row.tracker_key)).filter(Boolean) }))
      .filter(game => allowedIds.has(String(game.game_pk || 'ungrouped')))
  }, [games, filteredRows])

  const summary = payload?.summary || {}

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

    <section style={s.statsScroller} aria-label="Model Tracker summary cards">
      <div style={s.statRail}>
        <StatCard label="Tracked Rows" value={summary.total_rows || filteredRows.length} />
        <StatCard label="Games Tracked" value={summary.games_tracked} />
        <StatCard label="Pending" value={summary.pending_rows} />
        <StatCard label="Live" value={summary.live_rows} />
        <StatCard label="Graded" value={summary.graded_rows} />
        <StatCard label="Won" value={summary.won} />
        <StatCard label="Lost" value={summary.lost} />
        <StatCard label="Ungraded" value={summary.ungraded_rows} />
      </div>
    </section>

    <section style={s.section}>
      <div style={s.sectionHeader}>
        <div style={s.sectionTitle}>Filters</div>
        <span className="status-badge">{filteredRows.length} visible rows · {cacheByDate[date] ? 'cached date loaded' : 'fresh request'}</span>
      </div>
      <div style={s.filters}>
        <label><div style={s.label}>Source</div><select className="input-control" style={s.input} value={sourceFilter} onChange={e => setSourceFilter(e.target.value)}>{sources.map(source => <option key={source} value={source}>{source}</option>)}</select></label>
        <label><div style={s.label}>Grade</div><select className="input-control" style={s.input} value={gradeFilter} onChange={e => setGradeFilter(e.target.value)}>{grades.map(grade => <option key={grade} value={grade}>{grade}</option>)}</select></label>
        <label><div style={s.label}>Game</div><select className="input-control" style={s.input} value={gameFilter} onChange={e => setGameFilter(e.target.value)}>{gameOptions.map(game => <option key={game} value={game}>{game}</option>)}</select></label>
        <label><div style={s.label}>Search</div><input className="input-control" style={s.input} value={search} onChange={e => setSearch(e.target.value)} placeholder="player, team, pick, reason" /></label>
      </div>
    </section>

    <section style={s.section}>
      <div style={s.sectionHeader}>
        <div><div style={s.sectionTitle}>Game Grouped Tracker</div><div style={s.rowMeta}>The front of each card shows formulas, metrics, features, and missing inputs. Flip the card for a concise analyst readout tied to the same stored row.</div></div>
      </div>
      {filteredGames.length === 0 && <div className="state-panel">No tracker rows found. Click Refresh Snapshot to save today's model outputs.</div>}
      {filteredGames.map(game => <GameTrackerCard key={String(game.game_pk || 'ungrouped')} game={game} reducedMotion={reducedMotion} />)}
    </section>

    <section style={s.section}>
      <div style={s.sectionHeader}>
        <div><div style={s.sectionTitle}>Table View</div><div style={s.rowMeta}>Every stored model output, including ungraded watchlist rows and final comparison state. Scroll horizontally to inspect every column.</div></div>
      </div>
      <div style={s.tableWrap}>
        <table style={s.table}>
          <thead><tr><th style={s.th}>Date</th><th style={s.th}>Source</th><th style={s.th}>Game</th><th style={s.th}>Type</th><th style={s.th}>Pick</th><th style={s.th}>Player/Team</th><th style={s.th}>Model</th><th style={s.th}>Score</th><th style={s.th}>Confidence</th><th style={s.th}>Line</th><th style={s.th}>Price</th><th style={s.th}>Status</th><th style={s.th}>Grade</th><th style={s.th}>Reason</th></tr></thead>
          <tbody>
            {filteredRows.map(row => <tr key={row.id || row.tracker_key}>
              <td style={s.td}>{row.snapshot_date}</td>
              <td style={s.td}>{row.source}</td>
              <td style={s.td}>{row.away_team || 'Away'} @ {row.home_team || 'Home'}</td>
              <td style={s.td}>{row.market_type || row.pick_type}</td>
              <td style={s.td}>{row.pick_label || 'N/A'}</td>
              <td style={s.td}>{row.player_name || row.team_name || 'N/A'}</td>
              <td style={s.td}>{row.model_name || 'N/A'}</td>
              <td style={s.td}>{fmt(row.score)}</td>
              <td style={s.td}>{fmt(row.confidence)}</td>
              <td style={s.td}>{fmt(row.line)}</td>
              <td style={s.td}>{fmt(row.price, 0)}</td>
              <td style={s.td}>{row.result_status}</td>
              <td style={s.td}>{row.grade}</td>
              <td style={{ ...s.td, whiteSpace: 'normal', minWidth: 300, maxWidth: 520 }}>{row.grade_reason || row.primary_reason || 'N/A'}</td>
            </tr>)}
          </tbody>
        </table>
      </div>
    </section>
  </div>
}