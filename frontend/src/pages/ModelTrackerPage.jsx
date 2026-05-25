import React, { useEffect, useMemo, useState } from 'react'
import { API_BASE, getMlbLiveDate } from '../lib/api'

const API = API_BASE

const s = {
  page: { display: 'grid', gap: 18, minWidth: 0, overflowX: 'hidden' },
  hero: { border: '1px solid var(--border-subtle)', borderRadius: 18, padding: 22, background: 'rgba(15, 23, 42, 0.72)', minWidth: 0 },
  header: { display: 'flex', justifyContent: 'space-between', gap: 16, alignItems: 'flex-start', flexWrap: 'wrap', minWidth: 0 },
  controls: { display: 'flex', gap: 10, alignItems: 'center', flexWrap: 'wrap' },
  statsScroller: {
    overflowX: 'auto',
    overflowY: 'hidden',
    WebkitOverflowScrolling: 'touch',
    scrollbarGutter: 'stable',
    padding: '2px 2px 10px',
    margin: '-2px -2px 0',
    scrollSnapType: 'x proximity',
  },
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
  miniScroller: {
    overflowX: 'auto',
    overflowY: 'hidden',
    WebkitOverflowScrolling: 'touch',
    scrollbarGutter: 'stable',
    padding: '2px 2px 10px',
    margin: '10px -2px -2px',
    scrollSnapType: 'x proximity',
  },
  miniRail: { display: 'flex', gap: 10, width: 'max-content', minWidth: '100%', alignItems: 'stretch' },
  rowCard: {
    border: '1px solid var(--border-subtle)',
    borderRadius: 12,
    padding: 11,
    background: 'rgba(17, 24, 39, 0.72)',
    flex: '0 0 clamp(250px, 28vw, 330px)',
    maxWidth: 330,
    minWidth: 250,
    scrollSnapAlign: 'start',
    boxSizing: 'border-box',
  },
  rowTitle: { color: 'var(--text-primary)', fontSize: 13, fontWeight: 850, overflowWrap: 'anywhere' },
  rowMeta: { color: 'var(--text-muted)', fontSize: 12, marginTop: 5, lineHeight: 1.45, overflowWrap: 'anywhere' },
  tableWrap: { overflowX: 'auto', overflowY: 'hidden', WebkitOverflowScrolling: 'touch', border: '1px solid var(--border-subtle)', borderRadius: 14, scrollbarGutter: 'stable' },
  table: { width: 'max-content', minWidth: '100%', borderCollapse: 'collapse', fontSize: 12 },
  th: { textAlign: 'left', padding: '9px 10px', borderBottom: '1px solid var(--border-subtle)', color: 'var(--text-muted)', whiteSpace: 'nowrap' },
  td: { padding: '9px 10px', borderBottom: '1px solid rgba(148, 163, 184, 0.14)', color: 'var(--text-secondary)', whiteSpace: 'nowrap' },
  detail: { marginTop: 8, whiteSpace: 'pre-wrap', color: 'var(--text-muted)', fontSize: 12, lineHeight: 1.45 },
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
  try {
    return JSON.stringify(value, null, 2)
  } catch {
    return String(value)
  }
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

function GameTrackerCard({ game }) {
  const title = game.game_pk ? `${game.away_team || 'Away'} @ ${game.home_team || 'Home'}` : 'Ungrouped model outputs'
  const rows = game.rows || []
  const sideRows = rows.filter(row => ['side', 'moneyline'].includes(String(row.pick_type || '').toLowerCase())).slice(0, 2)
  const totalRows = rows.filter(row => String(row.market_type || '').toLowerCase().includes('total')).slice(0, 2)
  const hitterRows = rows.filter(row => String(row.source_component || '').includes('hitter')).slice(0, 3)
  const pitcherRows = rows.filter(row => String(row.source_component || '').includes('pitcher')).slice(0, 3)
  const propRows = rows.filter(row => String(row.pick_type || '').includes('prop') || String(row.market_type || '').includes('prop')).slice(0, 4)
  const previewRows = [...sideRows, ...totalRows, ...hitterRows, ...pitcherRows, ...propRows].slice(0, 8)

  return <article style={s.gameCard}>
    <div style={s.gameTop}>
      <div>
        <div style={s.gameTitle}>{title}</div>
        <div style={s.rowMeta}>Game PK {game.game_pk || 'N/A'} · {game.row_count} tracked outputs · Sources: {(game.sources || []).join(', ') || 'none'}</div>
      </div>
      <span className="status-badge">{Object.entries(game.grades || {}).map(([k, v]) => `${k}: ${v}`).join(' · ') || 'No grades'}</span>
    </div>
    {previewRows.length > 3 && <div style={s.scrollHint}>Swipe horizontally to review all preview cards</div>}
    <div style={s.miniScroller}>
      <div style={s.miniRail}>
        {previewRows.length === 0 && <div style={s.rowMeta}>No model rows available for this game.</div>}
        {previewRows.map(row => <div key={row.id || row.tracker_key} style={s.rowCard}>
          <div style={s.rowTitle}>{row.pick_label || row.player_name || row.team_name || row.model_name || 'Tracked output'}</div>
          <div style={s.rowMeta}>{row.source} / {row.source_component} · {row.market_type || 'model'} · {gradeLabel(row)}</div>
          <div style={s.rowMeta}>Score {fmt(row.score)} · Confidence {fmt(row.confidence)} · Edge {fmt(row.edge)}</div>
          {textValue(row.primary_reason) && <details style={s.detail}><summary>Reason</summary>{textValue(row.primary_reason)}</details>}
        </div>)}
      </div>
    </div>
  </article>
}

export default function ModelTrackerPage() {
  const [date, setDate] = useState(getMlbLiveDate())
  const [payload, setPayload] = useState(null)
  const [loading, setLoading] = useState(false)
  const [refreshing, setRefreshing] = useState(false)
  const [resultRefreshing, setResultRefreshing] = useState(false)
  const [error, setError] = useState(null)
  const [sourceFilter, setSourceFilter] = useState('all')
  const [gradeFilter, setGradeFilter] = useState('all')
  const [gameFilter, setGameFilter] = useState('all')
  const [search, setSearch] = useState('')

  function load() {
    setLoading(true)
    setError(null)
    fetch(`${API}/model-tracker?date=${date}`)
      .then(async r => { if (!r.ok) throw new Error(`${r.status} ${r.statusText}: ${await r.text()}`); return r.json() })
      .then(json => { setPayload(json); setLoading(false) })
      .catch(err => { setError(String(err?.message || err)); setLoading(false) })
  }

  function refreshSnapshot() {
    setRefreshing(true)
    setError(null)

    fetch(`${API}/model-tracker/snapshot?date=${date}`, { method: 'POST' })
      .then(async r => {
        const text = await r.text()
        let json = null

        try {
          json = text ? JSON.parse(text) : null
        } catch {
          json = { raw: text }
        }

        if (!r.ok) {
          throw new Error(`${r.status} ${r.statusText}: ${text}`)
        }

        if (!json || Number(json.rows_collected || 0) === 0) {
          const errorText = JSON.stringify(json?.errors || json || {}, null, 2)
          throw new Error(`Snapshot saved 0 rows. Backend response:\n${errorText}`)
        }

        return json
      })
      .then(() => {
        setRefreshing(false)
        load()
      })
      .catch(err => {
        setError(String(err?.message || err))
        setRefreshing(false)
      })
  }

  function refreshResults() {
    setResultRefreshing(true)
    setError(null)
    fetch(`${API}/model-tracker/results/refresh?date=${date}`, { method: 'POST' })
      .then(async r => { if (!r.ok) throw new Error(`${r.status} ${r.statusText}: ${await r.text()}`); return r.json() })
      .then(() => { setResultRefreshing(false); load() })
      .catch(err => { setError(String(err?.message || err)); setResultRefreshing(false) })
  }

  useEffect(() => { load() }, [date])

  const rows = payload?.rows || []
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
      return [row.pick_label, row.player_name, row.team_name, row.away_team, row.home_team, row.model_name, textValue(row.primary_reason), textValue(row.grade_reason)]
        .some(value => String(value || '').toLowerCase().includes(q))
    })
  }, [rows, sourceFilter, gradeFilter, gameFilter, search])

  const filteredGames = useMemo(() => {
    const allowedIds = new Set(filteredRows.map(row => String(row.game_pk || 'ungrouped')))
    const filteredRowKeys = new Set(filteredRows.map(row => row.id || row.tracker_key).filter(Boolean))
    return games.map(game => ({
      ...game,
      rows: (game.rows || []).filter(row => filteredRowKeys.has(row.id || row.tracker_key)),
    })).filter(game => allowedIds.has(String(game.game_pk || 'ungrouped')))
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
        <span className="status-badge">{filteredRows.length} visible rows</span>
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
        <div><div style={s.sectionTitle}>Game Grouped Tracker</div><div style={s.rowMeta}>Grouped by MLB game, with sides, totals, hitters, pitchers, props, watchlists, and grades in one place.</div></div>
      </div>
      {filteredGames.length === 0 && <div className="state-panel">No tracker rows found. Click Refresh Snapshot to save today's model outputs.</div>}
      {filteredGames.map(game => <GameTrackerCard key={String(game.game_pk || 'ungrouped')} game={game} />)}
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
              <td style={{ ...s.td, whiteSpace: 'pre-wrap', minWidth: 300, maxWidth: 520 }}>{textValue(row.grade_reason || row.primary_reason || 'N/A')}</td>
            </tr>)}
          </tbody>
        </table>
      </div>
    </section>
  </div>
}