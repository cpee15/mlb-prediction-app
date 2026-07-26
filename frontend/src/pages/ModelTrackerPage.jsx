import React, { useEffect, useMemo, useState } from 'react'
import { Bar, BarChart, CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import { API_BASE, getMlbLiveDate } from '../lib/api'
import {
  UNIT_SIZE_DOLLARS,
  buildPeriodComparison,
  comparePlayRank,
  dailySeries,
  effectivePrice,
  gameFilterValue,
  gameLabel,
  groupProfit,
  groupRowsByGame,
  maxDrawdown,
  numericScore,
  periodDateKeys,
  productionBucketLabel,
  projectionValue,
  recommendationBucket,
  rowHasPrice,
  summarizePnl,
  toNumber,
  topModelProjectionRows,
} from '../lib/modelTrackerPnl.mjs'

const API = API_BASE
const TABS = ['plays', 'results', 'pnl', 'quality', 'details']
const TAB_LABELS = { plays: 'Games', results: 'Results', pnl: 'P&L', quality: 'Quality', details: 'Details' }
const GRADED = ['won', 'lost', 'push', 'partial']
const PERIODS = ['dod', 'wow', 'mom', 'rolling7', 'rolling30', 'season']
const PERIOD_LABELS = { dod: 'DoD', wow: 'WoW', mom: 'MoM', rolling7: 'Rolling 7', rolling30: 'Rolling 30', season: 'Season' }

const s = {
  page: { display: 'grid', gap: 18, minWidth: 0, overflowX: 'hidden' },
  hero: { border: '1px solid var(--border-subtle)', borderRadius: 18, padding: 22, background: 'rgba(15,23,42,0.72)', minWidth: 0 },
  header: { display: 'flex', justifyContent: 'space-between', gap: 16, alignItems: 'flex-start', flexWrap: 'wrap' },
  controls: { display: 'flex', gap: 10, alignItems: 'center', flexWrap: 'wrap' },
  tabs: { display: 'flex', gap: 8, flexWrap: 'wrap', marginTop: 16 },
  tab: active => ({ border: '1px solid rgba(148,163,184,0.24)', borderRadius: 999, padding: '9px 12px', cursor: 'pointer', fontWeight: 900, color: active ? '#020617' : 'var(--text-secondary)', background: active ? 'linear-gradient(135deg,#67e8f9,#a7f3d0)' : 'rgba(15,23,42,0.72)' }),
  statsScroller: { overflowX: 'auto', overflowY: 'hidden', WebkitOverflowScrolling: 'touch', scrollbarGutter: 'stable', paddingBottom: 10 },
  statRail: { display: 'flex', gap: 12, width: 'max-content', minWidth: '100%' },
  card: { border: '1px solid var(--border-subtle)', borderRadius: 14, padding: 14, background: 'rgba(7,11,18,0.55)', flex: '0 0 172px', minWidth: 172 },
  value: { fontSize: 25, fontWeight: 900, color: 'var(--text-primary)', letterSpacing: '-0.05em' },
  label: { fontSize: 11, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.08em', fontWeight: 850 },
  section: { border: '1px solid var(--border-subtle)', borderRadius: 16, padding: 16, background: 'rgba(15,23,42,0.56)', minWidth: 0, overflow: 'hidden' },
  sectionHeader: { display: 'flex', justifyContent: 'space-between', gap: 12, alignItems: 'center', flexWrap: 'wrap', marginBottom: 12 },
  sectionTitle: { fontSize: 18, fontWeight: 900, color: 'var(--text-primary)' },
  rowMeta: { color: 'var(--text-muted)', fontSize: 12, lineHeight: 1.45, overflowWrap: 'anywhere' },
  filters: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(178px, 1fr))', gap: 10 },
  input: { width: '100%', boxSizing: 'border-box' },
  chartGrid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(315px, 1fr))', gap: 14 },
  chartBox: { border: '1px solid rgba(148,163,184,0.16)', borderRadius: 14, padding: 12, minHeight: 292, background: 'rgba(7,11,18,0.38)' },
  accordionStack: { display: 'grid', gap: 10 },
  gameButton: { width: '100%', border: '1px solid rgba(148,163,184,0.18)', borderRadius: 14, padding: 14, background: 'rgba(8,12,20,0.58)', cursor: 'pointer', textAlign: 'left' },
  gameHeader: { display: 'grid', gridTemplateColumns: 'minmax(220px, 1fr) auto', gap: 12, alignItems: 'center' },
  gameTitle: { color: 'var(--text-primary)', fontSize: 16, fontWeight: 950 },
  chipRail: { display: 'flex', flexWrap: 'wrap', gap: 8 },
  chip: { border: '1px solid rgba(148,163,184,0.24)', borderRadius: 999, padding: '4px 8px', fontSize: 11, color: 'var(--text-secondary)', fontWeight: 800, background: 'rgba(30,41,59,0.44)' },
  hotChip: { border: '1px solid rgba(34,197,94,0.42)', borderRadius: 999, padding: '4px 8px', fontSize: 11, color: '#bbf7d0', fontWeight: 900, background: 'rgba(22,101,52,0.28)' },
  warnChip: { border: '1px solid rgba(250,204,21,0.42)', borderRadius: 999, padding: '4px 8px', fontSize: 11, color: '#fef08a', fontWeight: 900, background: 'rgba(113,63,18,0.26)' },
  bucketGrid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(260px, 1fr))', gap: 12, marginTop: 12 },
  bucket: { border: '1px solid rgba(148,163,184,0.14)', borderRadius: 14, padding: 12, background: 'rgba(15,23,42,0.44)', minWidth: 0 },
  playRow: { display: 'grid', gap: 6, borderTop: '1px solid rgba(148,163,184,0.12)', paddingTop: 9, marginTop: 9 },
  topGrid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(250px, 1fr))', gap: 10 },
  topCard: { border: '1px solid rgba(148,163,184,0.16)', borderRadius: 14, padding: 12, background: 'rgba(8,12,20,0.42)', minWidth: 0 },
  rowTitle: { color: 'var(--text-primary)', fontSize: 13, fontWeight: 900, overflowWrap: 'anywhere' },
  tableWrap: { overflowX: 'auto', overflowY: 'hidden', WebkitOverflowScrolling: 'touch', border: '1px solid var(--border-subtle)', borderRadius: 14, scrollbarGutter: 'stable' },
  table: { width: 'max-content', minWidth: '100%', borderCollapse: 'collapse', fontSize: 12 },
  th: { textAlign: 'left', padding: '9px 10px', borderBottom: '1px solid var(--border-subtle)', color: 'var(--text-muted)', whiteSpace: 'nowrap' },
  td: { padding: '9px 10px', borderBottom: '1px solid rgba(148,163,184,0.14)', color: 'var(--text-secondary)', whiteSpace: 'nowrap' },
}

function StatCard({ label, value }) { return <div style={s.card}><div style={s.label}>{label}</div><div style={s.value}>{value ?? 0}</div></div> }
function fmt(value, digits = 3) { if (value === null || value === undefined || value === '') return 'Unavailable'; const n = Number(value); if (Number.isNaN(n)) return String(value); return Number.isInteger(n) ? String(n) : n.toFixed(digits) }
function fmtPct(value, digits = 1) { const n = toNumber(value); return n === null ? 'Unavailable' : `${(n * 100).toFixed(digits)}%` }
function fmtMoney(value) { const n = toNumber(value); if (n === null) return 'Unavailable'; return `${n > 0 ? '+' : n < 0 ? '-' : ''}$${Math.abs(n).toFixed(0)}` }
function fmtUnits(value) { const n = toNumber(value); return n === null ? 'Unavailable' : `${n > 0 ? '+' : ''}${n.toFixed(2)}u` }
function fmtPrice(value) { const n = toNumber(value); return n === null ? null : n > 0 ? `+${n}` : String(n) }
function textValue(value) { if (value === null || value === undefined || value === '') return ''; if (typeof value === 'string') return value; try { return JSON.stringify(value) } catch { return String(value) } }
function parseMaybeJson(value) { if (!value) return null; if (typeof value === 'object') return value; try { return JSON.parse(value) } catch { return null } }
function compactValue(value) { if (value === null || value === undefined || value === '') return 'Unavailable'; if (Array.isArray(value)) return value.filter(Boolean).slice(0, 3).map(textValue).join(', ') || 'Unavailable'; if (typeof value === 'object') return Object.keys(value).slice(0, 3).map(key => `${key}: ${compactValue(value[key])}`).join(' · ') || 'Unavailable'; return String(value) }
function rowIdentity(row) { return row.pick_label || row.player_name || row.team_name || row.model_name || 'Tracked output' }
function gradeLabel(row) { if (row.grade === 'pending') return 'Pending Result'; if (row.result_status === 'live') return 'Live Tracking'; if (row.result_status === 'final' && row.grade === 'ungraded') return 'Final Ungraded'; if (GRADED.includes(row.grade)) return 'Graded'; if (row.grade === 'watchlist_only') return 'Watchlist'; if (row.grade === 'ungraded') return 'Awaiting Result'; return row.grade || 'Untracked' }
function topReason(row) { if (Array.isArray(row.reasoning) && row.reasoning.length) return compactValue(row.reasoning[0]); return textValue(row.primary_reason) || textValue(row.grade_reason) || '' }
function shortReason(row) { const reason = topReason(row); return reason.length > 130 ? `${reason.slice(0, 130)}...` : reason }
function normalizePayloadRows(payload) { return (payload?.rows || []).map(row => {
  const next = { ...row, reasoning: parseMaybeJson(row.reasoning) || row.reasoning, features_used: parseMaybeJson(row.features_used) || row.features_used, missing_inputs: parseMaybeJson(row.missing_inputs) || row.missing_inputs, raw_payload: parseMaybeJson(row.raw_payload) || row.raw_payload, actual_result: parseMaybeJson(row.actual_result) || row.actual_result }
  next.bucket = next.reportable && next.odds_available && rowHasPrice(next) && ((toNumber(next.edge) || 0) > 0 || (toNumber(next.expected_value) || 0) > 0) ? 'recommended' : (next.row_type === 'model_signal' ? 'lean' : recommendationBucket(next))
  return next
}) }
function signalChip(row) { const probability = numericScore(row); if (probability !== null) return `Confidence ${fmtPct(probability)}`; const projection = projectionValue(row); if (projection !== null) return `Projection ${fmt(projection, 2)}`; return null }
function outputTypeLabel(row) { return projectionValue(row) !== null && numericScore(row) === null ? 'Model Projection' : productionBucketLabel(row.bucket) }

function ChartBox({ title, subtitle, children }) {
  return <div style={s.chartBox}><div style={s.sectionHeader}><div><div style={s.sectionTitle}>{title}</div>{subtitle && <div style={s.rowMeta}>{subtitle}</div>}</div></div><div style={{ height: 215 }}>{children}</div></div>
}
function EmptyState({ children }) { return <div className="state-panel">{children}</div> }
function DataTable({ headers, rows, renderRow }) { return <div style={s.tableWrap}><table style={s.table}><thead><tr>{headers.map(h => <th key={h} style={s.th}>{h}</th>)}</tr></thead><tbody>{rows.map(renderRow)}</tbody></table></div> }

function MetricChips({ row }) {
  const chips = [outputTypeLabel(row), signalChip(row)]
  const edge = toNumber(row.edge)
  const ev = toNumber(row.expected_value)
  const price = fmtPrice(effectivePrice(row))
  if (edge !== null) chips.push(`Edge ${fmtPct(edge)}`)
  if (ev !== null) chips.push(`EV ${fmt(ev, 3)}`)
  if (price) chips.push(price)
  return <div style={s.chipRail}>{chips.filter(Boolean).map((chip, index) => <span key={`${chip}-${index}`} style={index === 0 && row.bucket === 'recommended' ? s.hotChip : index === 0 && row.bucket === 'lean' ? s.warnChip : s.chip}>{chip}</span>)}</div>
}

function CompactPlayRow({ row }) {
  const reason = shortReason(row)
  return <div style={s.playRow}>
    <div><div style={s.rowTitle}>{rowIdentity(row)}</div><div style={s.rowMeta}>{row.market_type || row.pick_type || 'model'} · {gradeLabel(row)} · {gameLabel(row)}</div></div>
    <MetricChips row={row} />
    {reason && <div style={s.rowMeta}>{reason}</div>}
  </div>
}

function TopProjectionCard({ row }) {
  const reason = shortReason(row)
  return <article style={s.topCard}>
    <div style={s.rowTitle}>{rowIdentity(row)}</div>
    <div style={s.rowMeta}>{gameLabel(row)} · {row.market_type || row.pick_type || 'model'}</div>
    <div style={{ marginTop: 8 }}><MetricChips row={row} /></div>
    {reason && <div style={{ ...s.rowMeta, marginTop: 8 }}>{reason}</div>}
  </article>
}

function GameAccordion({ game, defaultOpen = false }) {
  const [open, setOpen] = useState(defaultOpen)
  const priceCoverage = game.rows.length ? `${game.price_count}/${game.rows.length} priced` : 'No prices'
  const bestSignal = game.best_projection !== null ? `Top Projection ${fmt(game.best_projection, 2)}` : `Top Confidence ${fmtPct(game.best_score)}`
  return <article>
    <button type="button" style={s.gameButton} onClick={() => setOpen(prev => !prev)} aria-expanded={open}>
      <div style={s.gameHeader}>
        <div><div style={s.gameTitle}>{game.label}</div><div style={s.rowMeta}>{game.game_time || game.start_time || 'Time unavailable'} · {game.game_status || game.status || 'Scheduled'} · {game.rows.length} outputs</div></div>
        <div style={s.rowMeta}>{open ? 'Collapse' : 'Open'}</div>
      </div>
      <div style={{ ...s.chipRail, marginTop: 10 }}>
        <span style={s.hotChip}>{game.buckets.recommended.length} Recommendations</span>
        <span style={s.warnChip}>{game.buckets.lean.length} Model/Lean</span>
        <span style={s.chip}>{game.buckets.low_confidence.length} Low Confidence</span>
        <span style={s.chip}>{bestSignal}</span>
        {game.best_edge !== null && <span style={s.chip}>Best Edge {fmtPct(game.best_edge)}</span>}
        <span style={s.chip}>{priceCoverage}</span>
      </div>
    </button>
    {open && <div style={s.bucketGrid}>
      <BucketColumn title="Recommendations" rows={game.buckets.recommended} empty="No recommendations for this game." />
      <BucketColumn title="Model Projections / Leans" rows={game.buckets.lean} empty="No model projections or leans for this game." />
      <BucketColumn title="Low Confidence / No Play" rows={game.buckets.low_confidence} empty="No low-confidence outputs for this game." />
    </div>}
  </article>
}

function BucketColumn({ title, rows, empty }) {
  const visibleRows = rows.slice(0, 6)
  return <section style={s.bucket}><div style={s.sectionHeader}><div><div style={s.rowTitle}>{title}</div><div style={s.rowMeta}>{rows.length} outputs</div></div></div>{visibleRows.length ? visibleRows.map(row => <CompactPlayRow key={row.id || row.tracker_key || rowIdentity(row)} row={row} />) : <div style={s.rowMeta}>{empty}</div>}{rows.length > visibleRows.length && <div style={{ ...s.rowMeta, marginTop: 10 }}>+{rows.length - visibleRows.length} more in Details</div>}</section>
}

function Filters({ options, values, setters }) {
  return <section style={s.section}>
    <div style={s.sectionHeader}><div style={s.sectionTitle}>Filters</div><span className="status-badge">{options.visibleCount} visible</span></div>
    <div style={s.filters}>
      <label><div style={s.label}>Source</div><select className="input-control" style={s.input} value={values.sourceFilter} onChange={e => setters.setSourceFilter(e.target.value)}>{options.sourceOptions.map(source => <option key={source} value={source}>{source}</option>)}</select></label>
      <label><div style={s.label}>Grade</div><select className="input-control" style={s.input} value={values.gradeFilter} onChange={e => setters.setGradeFilter(e.target.value)}>{options.gradeOptions.map(grade => <option key={grade} value={grade}>{grade}</option>)}</select></label>
      <label><div style={s.label}>Game</div><select className="input-control" style={s.input} value={values.gameFilter} onChange={e => setters.setGameFilter(e.target.value)}>{options.gameOptions.map(game => <option key={game.value} value={game.value}>{game.label}</option>)}</select></label>
      <label><div style={s.label}>Bucket</div><select className="input-control" style={s.input} value={values.bucketFilter} onChange={e => setters.setBucketFilter(e.target.value)}><option value="all">all</option><option value="recommended">recommendations</option><option value="lean">model projections / leans</option><option value="low_confidence">low confidence / no play</option></select></label>
      <label><div style={s.label}>Minimum Confidence</div><select className="input-control" style={s.input} value={values.confidenceFilter} onChange={e => setters.setConfidenceFilter(e.target.value)}><option value="all">all</option><option value="0.50">.50+</option><option value="0.55">.55+</option><option value="0.60">.60+</option></select></label>
      <label><div style={s.label}>Has Price</div><select className="input-control" style={s.input} value={values.priceFilter} onChange={e => setters.setPriceFilter(e.target.value)}><option value="all">all</option><option value="has_price">has price</option><option value="missing_price">missing price</option></select></label>
      <label><div style={s.label}>Has Edge</div><select className="input-control" style={s.input} value={values.edgeFilter} onChange={e => setters.setEdgeFilter(e.target.value)}><option value="all">all</option><option value="positive_edge">positive edge</option><option value="no_edge">no edge</option></select></label>
      <label><div style={s.label}>Search</div><input className="input-control" style={s.input} value={values.search} onChange={e => setters.setSearch(e.target.value)} placeholder="player, team, pick, reason" /></label>
    </div>
  </section>
}

function PlaysTab({ groupedGames, topRows, gameFilter }) {
  const [gamesOpen, setGamesOpen] = useState(gameFilter !== 'all')
  useEffect(() => { if (gameFilter !== 'all') setGamesOpen(true) }, [gameFilter])
  const reportableRows = topRows.filter(row => row.reportable && row.odds_available)
  const modelOnlyRows = topRows.filter(row => row.row_type === 'model_signal')
  const visibleTopRows = reportableRows.slice(0, 12)
  return <div style={s.page}>
    <section style={s.section}>
      <div style={s.sectionHeader}><div><div style={s.sectionTitle}>Bet105 Odds-Backed Plays</div><div style={s.rowMeta}>Actionable selections require a real Bet105 price and positive model economics.</div></div><span className="status-badge">{reportableRows.length} decisions</span></div>
      {visibleTopRows.length ? <div style={s.topGrid}>{visibleTopRows.map(row => <TopProjectionCard key={row.id || row.tracker_key || rowIdentity(row)} row={row} />)}</div> : <EmptyState>No Bet105 odds-backed decisions match the current filters.</EmptyState>}
      {topRows.length > visibleTopRows.length && <div style={{ ...s.rowMeta, marginTop: 12 }}>Showing top {visibleTopRows.length}; use filters or Details for the full slate.</div>}
    </section>
    <section style={s.section}>
      <div style={s.sectionHeader}><div><div style={s.sectionTitle}>Model-Only Signals</div><div style={s.rowMeta}>Projection only. No Bet105 odds available. Teams and totals may become decisions when a matching market arrives; player props stay on this watchlist unless Bet105 prices them.</div></div><span className="status-badge">{modelOnlyRows.length} watchlist</span></div>
      {modelOnlyRows.length ? <div style={s.topGrid}>{modelOnlyRows.slice(0, 12).map(row => <TopProjectionCard key={row.id || row.tracker_key || rowIdentity(row)} row={row} />)}</div> : <EmptyState>No projection-only signals match the current filters.</EmptyState>}
    </section>
    <section style={s.section}>
      <div style={s.sectionHeader}><div><div style={s.sectionTitle}>Teams, Totals & Player Props Watchlist</div><div style={s.rowMeta}>Collapsed by default so the slate stays scannable.</div></div><button className="button-secondary" type="button" onClick={() => setGamesOpen(prev => !prev)}>{gamesOpen ? 'Hide Games' : `Show Games (${groupedGames.length})`}</button></div>
      {gamesOpen && (groupedGames.length ? <div style={s.accordionStack}>{groupedGames.map(game => <GameAccordion key={game.key} game={game} defaultOpen={gameFilter !== 'all'} />)}</div> : <EmptyState>No model plays found for this slate.</EmptyState>)}
    </section>
  </div>
}

function mergeTrendSeries(currentSeries, previousSeries) {
  const map = new Map()
  currentSeries.forEach(point => map.set(point.day_index, { day_index: point.day_index, Current: point.current_cumulative, current_date: point.date }))
  previousSeries.forEach(point => map.set(point.day_index, { ...(map.get(point.day_index) || { day_index: point.day_index }), Previous: point.previous_cumulative, previous_date: point.date }))
  return Array.from(map.values()).sort((a, b) => a.day_index - b.day_index)
}

function ResultsTab({ rows, selectedDate, period, setPeriod, loading }) {
  const periodData = useMemo(() => buildPeriodComparison(rows, period, selectedDate, UNIT_SIZE_DOLLARS), [rows, period, selectedDate])
  const currentPnlRows = periodData.comparison.current.rows.filter(row => row.profit !== null)
  const breakdowns = useMemo(() => ({
    bucket: groupProfit(currentPnlRows, row => productionBucketLabel(row.bucket)),
    market: groupProfit(currentPnlRows, row => row.market_type || row.pick_type || 'Unknown'),
    confidence: groupProfit(currentPnlRows, row => row.confidence_band),
    edge: groupProfit(currentPnlRows, row => row.edge_band),
  }), [currentPnlRows])
  const pnlBars = [{ name: 'Current', profit: periodData.comparison.current.profit, units: periodData.comparison.current.units }, { name: 'Previous', profit: periodData.comparison.previous.profit, units: periodData.comparison.previous.units }]
  const efficiencyBars = [{ name: 'Win Rate', Current: periodData.comparison.current.win_rate, Previous: periodData.comparison.previous.win_rate }, { name: 'ROI', Current: periodData.comparison.current.roi, Previous: periodData.comparison.previous.roi }]
  const trend = mergeTrendSeries(periodData.currentSeries, periodData.previousSeries)
  return <div style={s.page}>
    <section style={s.section}>
      <div style={s.sectionHeader}><div><div style={s.sectionTitle}>Results Performance</div><div style={s.rowMeta}>Current period: {periodData.windows.current.label}{periodData.windows.previous ? ` · Previous: ${periodData.windows.previous.label}` : ''}</div></div><div style={s.tabs}>{PERIODS.map(key => <button key={key} type="button" style={s.tab(period === key)} onClick={() => setPeriod(key)}>{PERIOD_LABELS[key]}</button>)}</div></div>
      {loading && <div className="state-panel">Loading period results...</div>}
      <section style={s.statsScroller}><div style={s.statRail}>
        <StatCard label="Current P&L" value={fmtMoney(periodData.comparison.current.profit)} />
        <StatCard label="Previous P&L" value={fmtMoney(periodData.comparison.previous.profit)} />
        <StatCard label="Delta P&L" value={fmtMoney(periodData.comparison.deltas.profit)} />
        <StatCard label="Current ROI" value={fmtPct(periodData.comparison.current.roi)} />
        <StatCard label="Previous ROI" value={fmtPct(periodData.comparison.previous.roi)} />
        <StatCard label="Win Rate" value={fmtPct(periodData.comparison.current.win_rate)} />
        <StatCard label="Graded" value={periodData.comparison.current.graded_count} />
        <StatCard label="Pending" value={periodData.comparison.current.pending_count} />
      </div></section>
    </section>
    <section style={s.chartGrid}>
      <ChartBox title="Current vs Previous P&L" subtitle="Net result by selected period.">{pnlBars.some(row => row.profit) ? <ResponsiveContainer width="100%" height="100%"><BarChart data={pnlBars}><CartesianGrid strokeDasharray="3 3" stroke="rgba(148,163,184,0.18)" /><XAxis dataKey="name" stroke="#94a3b8" /><YAxis stroke="#94a3b8" /><Tooltip formatter={(v, n, row) => n === 'profit' ? [fmtMoney(v), 'P&L'] : [fmtUnits(row?.payload?.units), 'Units']} /><Bar dataKey="profit" fill="#38bdf8" /></BarChart></ResponsiveContainer> : <EmptyState>No graded plays in this period yet.</EmptyState>}</ChartBox>
      <ChartBox title="Period Trend" subtitle="Current period cumulative result overlaid with previous period.">{trend.length ? <ResponsiveContainer width="100%" height="100%"><LineChart data={trend}><CartesianGrid strokeDasharray="3 3" stroke="rgba(148,163,184,0.18)" /><XAxis dataKey="day_index" stroke="#94a3b8" /><YAxis stroke="#94a3b8" /><Tooltip formatter={v => fmtMoney(v)} labelFormatter={v => `Day ${v}`} /><Line type="monotone" dataKey="Current" stroke="#67e8f9" strokeWidth={2} dot={false} /><Line type="monotone" dataKey="Previous" stroke="#facc15" strokeWidth={2} dot={false} /></LineChart></ResponsiveContainer> : <EmptyState>No graded plays in this period yet.</EmptyState>}</ChartBox>
      <ChartBox title="Win Rate / ROI" subtitle="Current and previous efficiency comparison.">{efficiencyBars.some(row => row.Current !== null || row.Previous !== null) ? <ResponsiveContainer width="100%" height="100%"><BarChart data={efficiencyBars}><CartesianGrid strokeDasharray="3 3" stroke="rgba(148,163,184,0.18)" /><XAxis dataKey="name" stroke="#94a3b8" /><YAxis stroke="#94a3b8" tickFormatter={v => `${Math.round(v * 100)}%`} /><Tooltip formatter={v => fmtPct(v)} /><Bar dataKey="Current" fill="#22c55e" /><Bar dataKey="Previous" fill="#f59e0b" /></BarChart></ResponsiveContainer> : <EmptyState>No efficiency data in this period yet.</EmptyState>}</ChartBox>
    </section>
    <section style={s.chartGrid}>
      <BreakdownBar title="By Output Type" data={breakdowns.bucket} />
      <BreakdownBar title="By Market" data={breakdowns.market} />
      <BreakdownBar title="By Confidence" data={breakdowns.confidence} />
      <BreakdownBar title="By Edge" data={breakdowns.edge} />
    </section>
    <section style={s.section}><div style={s.sectionHeader}><div><div style={s.sectionTitle}>Period Detail</div><div style={s.rowMeta}>Compact ledger for the selected period.</div></div></div><PeriodDetailTable rows={periodData.comparison.current.rows} /></section>
  </div>
}

function BreakdownBar({ title, data }) {
  return <ChartBox title={title}>{data.length ? <ResponsiveContainer width="100%" height="100%"><BarChart data={data}><CartesianGrid strokeDasharray="3 3" stroke="rgba(148,163,184,0.18)" /><XAxis dataKey="label" stroke="#94a3b8" /><YAxis stroke="#94a3b8" /><Tooltip formatter={(v, n) => n === 'profit' ? fmtMoney(v) : v} /><Bar dataKey="profit" fill="#38bdf8" /></BarChart></ResponsiveContainer> : <EmptyState>No graded plays in this period yet.</EmptyState>}</ChartBox>
}

function PeriodDetailTable({ rows }) {
  return <DataTable headers={['Date', 'Game', 'Pick', 'Type', 'Confidence', 'Edge', 'Price', 'Grade', 'Profit', 'Units']} rows={rows} renderRow={row => <tr key={row.id || row.tracker_key || `${row.snapshot_date}-${rowIdentity(row)}`}><td style={s.td}>{row.snapshot_date}</td><td style={s.td}>{gameLabel(row)}</td><td style={s.td}>{rowIdentity(row)}</td><td style={s.td}>{outputTypeLabel(row)}</td><td style={s.td}>{numericScore(row) !== null ? fmtPct(numericScore(row)) : 'Unavailable'}</td><td style={s.td}>{fmtPct(row.edge)}</td><td style={s.td}>{fmtPrice(effectivePrice(row)) || 'Unavailable'}</td><td style={s.td}>{row.grade || 'pending'}</td><td style={s.td}>{fmtMoney(row.profit)}</td><td style={s.td}>{fmtUnits(row.units)}</td></tr>} />
}

function PnlTab({ rows }) {
  const pnl = summarizePnl(rows, UNIT_SIZE_DOLLARS)
  const series = dailySeries(pnl.rows.filter(row => row.profit !== null))
  const bestDay = series.slice().sort((a, b) => b.profit - a.profit)[0]
  const worstDay = series.slice().sort((a, b) => a.profit - b.profit)[0]
  return <div style={s.page}>
    <section style={s.section}><div style={s.sectionHeader}><div><div style={s.sectionTitle}>P&L — $100 Units</div><div style={s.rowMeta}>Only reportable Bet105 odds-backed decisions are included; model-only watchlist rows never affect this ledger.</div></div><span className="status-badge">$100 fixed unit</span></div><section style={s.statsScroller}><div style={s.statRail}><StatCard label="Net P&L" value={fmtMoney(pnl.profit)} /><StatCard label="Net Units" value={fmtUnits(pnl.units)} /><StatCard label="Total Risked" value={fmtMoney(pnl.total_risked)} /><StatCard label="ROI" value={fmtPct(pnl.roi)} /><StatCard label="Win Rate" value={fmtPct(pnl.win_rate)} /><StatCard label="Best Day" value={bestDay ? fmtMoney(bestDay.profit) : 'Unavailable'} /><StatCard label="Worst Day" value={worstDay ? fmtMoney(worstDay.profit) : 'Unavailable'} /><StatCard label="Max Drawdown" value={fmtMoney(maxDrawdown(series))} /></div></section></section>
    <section style={s.chartGrid}><ChartBox title="Cumulative P&L"><ResponsiveContainer width="100%" height="100%"><LineChart data={series}><CartesianGrid strokeDasharray="3 3" stroke="rgba(148,163,184,0.18)" /><XAxis dataKey="date" stroke="#94a3b8" /><YAxis stroke="#94a3b8" /><Tooltip formatter={v => fmtMoney(v)} /><Line type="monotone" dataKey="cumulative" stroke="#67e8f9" strokeWidth={2} dot={false} /></LineChart></ResponsiveContainer></ChartBox><ChartBox title="Daily P&L"><ResponsiveContainer width="100%" height="100%"><BarChart data={series}><CartesianGrid strokeDasharray="3 3" stroke="rgba(148,163,184,0.18)" /><XAxis dataKey="date" stroke="#94a3b8" /><YAxis stroke="#94a3b8" /><Tooltip formatter={v => fmtMoney(v)} /><Bar dataKey="profit" fill="#22c55e" /></BarChart></ResponsiveContainer></ChartBox></section>
    <section style={s.section}><div style={s.sectionHeader}><div><div style={s.sectionTitle}>Ledger</div><div style={s.rowMeta}>Every realized row behind the P&L cards.</div></div></div><PeriodDetailTable rows={pnl.rows.filter(row => row.profit !== null)} /></section>
  </div>
}

function QualityTable({ rows }) {
  return <DataTable headers={['Type', 'Pick / Player / Team', 'Source', 'Game', 'Price', 'Result State', 'Missing Inputs', 'Reason']} rows={rows} renderRow={row => <tr key={row.id || row.tracker_key}><td style={s.td}>{outputTypeLabel(row)}</td><td style={s.td}>{rowIdentity(row)}</td><td style={s.td}>{row.source || 'Unavailable'}</td><td style={s.td}>{gameLabel(row)}</td><td style={s.td}>{rowHasPrice(row) ? 'Available' : 'Unavailable'}</td><td style={s.td}>{gradeLabel(row)}</td><td style={s.td}>{compactValue(row.missing_inputs)}</td><td style={s.td}>{topReason(row)}</td></tr>} />
}

function DetailsTable({ rows }) {
  return <DataTable headers={['Date', 'Source', 'Game', 'Type', 'Pick', 'Player / Team', 'Model', 'Signal', 'Line', 'Price', 'Status', 'Grade', 'Reason']} rows={rows} renderRow={row => <tr key={row.id || row.tracker_key}><td style={s.td}>{row.snapshot_date}</td><td style={s.td}>{row.source}</td><td style={s.td}>{gameLabel(row)}</td><td style={s.td}>{row.market_type || row.pick_type}</td><td style={s.td}>{row.pick_label}</td><td style={s.td}>{row.player_name || row.team_name}</td><td style={s.td}>{row.model_name}</td><td style={s.td}>{signalChip(row) || 'Unavailable'}</td><td style={s.td}>{fmt(row.line)}</td><td style={s.td}>{fmtPrice(effectivePrice(row)) || 'Unavailable'}</td><td style={s.td}>{row.result_status}</td><td style={s.td}>{row.grade}</td><td style={s.td}>{topReason(row)}</td></tr>} />
}

export default function ModelTrackerPage() {
  const [date, setDate] = useState(getMlbLiveDate())
  const [payload, setPayload] = useState(null)
  const [cacheByDate, setCacheByDate] = useState({})
  const [loading, setLoading] = useState(false)
  const [refreshing, setRefreshing] = useState(false)
  const [resultRefreshing, setResultRefreshing] = useState(false)
  const [periodLoading, setPeriodLoading] = useState(false)
  const [error, setError] = useState(null)
  const [activeTab, setActiveTab] = useState('plays')
  const [resultsPeriod, setResultsPeriod] = useState('dod')
  const [sourceFilter, setSourceFilter] = useState('all')
  const [gradeFilter, setGradeFilter] = useState('all')
  const [gameFilter, setGameFilter] = useState('all')
  const [bucketFilter, setBucketFilter] = useState('all')
  const [confidenceFilter, setConfidenceFilter] = useState('all')
  const [priceFilter, setPriceFilter] = useState('all')
  const [edgeFilter, setEdgeFilter] = useState('all')
  const [search, setSearch] = useState('')

  function storePayload(nextDate, json) { setPayload(json); setCacheByDate(prev => ({ ...prev, [nextDate]: json })) }
  function load(force = false) { if (!force && cacheByDate[date]) { setPayload(cacheByDate[date]); setLoading(false); setError(null); return } setLoading(true); setError(null); fetch(`${API}/model-tracker?date=${date}`).then(async r => { if (!r.ok) throw new Error(`${r.status} ${r.statusText}: ${await r.text()}`); return r.json() }).then(json => { storePayload(date, json); setLoading(false) }).catch(err => { setError(String(err?.message || err)); setLoading(false) }) }
  function refreshPlays() { setRefreshing(true); setError(null); fetch(`${API}/model-tracker/${['snap', 'shot'].join('')}?date=${date}`, { method: 'POST' }).then(async r => { const text = await r.text(); let json = null; try { json = text ? JSON.parse(text) : null } catch { json = { message: text } } if (!r.ok) throw new Error(`${r.status} ${r.statusText}: ${text}`); if (!json || Number(json.rows_collected || 0) === 0) throw new Error('Refresh returned no model plays for this date.'); return json }).then(() => { setRefreshing(false); load(true) }).catch(err => { setError(String(err?.message || err)); setRefreshing(false) }) }
  function refreshResults() { setResultRefreshing(true); setError(null); fetch(`${API}/model-tracker/results/refresh?date=${date}`, { method: 'POST' }).then(async r => { if (!r.ok) throw new Error(`${r.status} ${r.statusText}: ${await r.text()}`); return r.json() }).then(() => { setResultRefreshing(false); load(true) }).catch(err => { setError(String(err?.message || err)); setResultRefreshing(false) }) }
  function loadPeriodDates(keys) {
    if (!keys.length) return
    const start = [...keys].sort()[0]
    const end = [...keys].sort().at(-1)
    const cacheKey = `range:${start}:${end}`
    if (cacheByDate[cacheKey]) return
    setPeriodLoading(true)
    fetch(`${API}/model-tracker/range?start=${start}&end=${end}`).then(async r => {
      if (!r.ok) throw new Error(`${r.status} ${r.statusText}: ${await r.text()}`)
      return r.json()
    }).then(json => {
      setCacheByDate(prev => ({ ...prev, [cacheKey]: json }))
      setPeriodLoading(false)
    }).catch(err => { setError(String(err?.message || err)); setPeriodLoading(false) })
  }

  useEffect(() => { load(false) }, [date])
  useEffect(() => { if (activeTab === 'results') loadPeriodDates(periodDateKeys(resultsPeriod, date)) }, [activeTab, resultsPeriod, date])

  const rows = useMemo(() => normalizePayloadRows(payload), [payload])
  const games = payload?.games || []
  const cachedRows = useMemo(() => Object.values(cacheByDate).flatMap(normalizePayloadRows), [cacheByDate])
  const q = search.trim().toLowerCase()
  const sourceOptions = useMemo(() => ['all', ...Array.from(new Set(rows.map(r => r.source).filter(Boolean))).sort()], [rows])
  const gradeOptions = useMemo(() => ['all', ...Array.from(new Set(rows.map(r => r.grade).filter(Boolean))).sort()], [rows])
  const gameOptions = useMemo(() => { const gameMap = new Map(); (games || []).forEach(game => gameMap.set(gameFilterValue(game.game_pk), { value: gameFilterValue(game.game_pk), label: gameLabel(game) })); rows.forEach(row => { const value = gameFilterValue(row.game_pk); if (!gameMap.has(value)) gameMap.set(value, { value, label: gameLabel(row) }) }); return [{ value: 'all', label: 'all' }, ...Array.from(gameMap.values()).sort((a, b) => a.label.localeCompare(b.label))] }, [games, rows])

  function filterRow(row) {
    if (sourceFilter !== 'all' && row.source !== sourceFilter) return false
    if (gradeFilter !== 'all' && row.grade !== gradeFilter) return false
    if (gameFilter !== 'all' && gameFilterValue(row.game_pk) !== gameFilter) return false
    const productionBucket = row.bucket === 'recommended' || row.bucket === 'lean' ? row.bucket : 'low_confidence'
    if (bucketFilter !== 'all' && productionBucket !== bucketFilter) return false
    const score = numericScore(row)
    if (confidenceFilter !== 'all' && (score === null || score < Number(confidenceFilter))) return false
    if (priceFilter === 'has_price' && !rowHasPrice(row)) return false
    if (priceFilter === 'missing_price' && rowHasPrice(row)) return false
    const edge = toNumber(row.edge)
    if (edgeFilter === 'positive_edge' && (edge === null || edge <= 0)) return false
    if (edgeFilter === 'no_edge' && edge !== null && edge > 0) return false
    if (!q) return true
    return [row.pick_label, row.player_name, row.team_name, row.away_team, row.home_team, row.model_name, row.source, row.source_component, textValue(row.primary_reason), textValue(row.grade_reason)].some(value => String(value || '').toLowerCase().includes(q))
  }

  const filteredRows = useMemo(() => rows.filter(filterRow).sort(comparePlayRank), [rows, sourceFilter, gradeFilter, gameFilter, bucketFilter, confidenceFilter, priceFilter, edgeFilter, search])
  const periodRows = useMemo(() => cachedRows.filter(filterRow).sort(comparePlayRank), [cachedRows, sourceFilter, gradeFilter, gameFilter, bucketFilter, confidenceFilter, priceFilter, edgeFilter, search])
  const groupedGames = useMemo(() => groupRowsByGame(filteredRows, games), [filteredRows, games])
  const topRows = useMemo(() => topModelProjectionRows(filteredRows), [filteredRows])
  const pnl = useMemo(() => summarizePnl(filteredRows, UNIT_SIZE_DOLLARS), [filteredRows])
  const summary = { total: filteredRows.length, recommended: filteredRows.filter(r => r.reportable && r.odds_available && r.bucket === 'recommended').length, lean: filteredRows.filter(r => r.row_type === 'model_signal').length, low: filteredRows.filter(r => !r.reportable && r.row_type !== 'model_signal').length, pending: filteredRows.filter(r => r.reportable && r.grade === 'pending').length, graded: filteredRows.filter(r => r.reportable && GRADED.includes(r.grade)).length, price: filteredRows.filter(r => r.reportable && rowHasPrice(r)).length }
  const qualityRows = filteredRows.filter(row => !['recommended', 'lean'].includes(row.bucket))

  const filterProps = { options: { sourceOptions, gradeOptions, gameOptions, visibleCount: filteredRows.length }, values: { sourceFilter, gradeFilter, gameFilter, bucketFilter, confidenceFilter, priceFilter, edgeFilter, search }, setters: { setSourceFilter, setGradeFilter, setGameFilter, setBucketFilter, setConfidenceFilter, setPriceFilter, setEdgeFilter, setSearch } }

  return <div style={s.page}>
    <section style={s.hero}><div style={s.header}><div><p className="page-kicker">Model Accountability</p><h1 className="page-title">Model Tracker</h1><p className="page-subtitle">Bet105-backed decisions are kept separate from projection-only MyDashboard and player watchlist signals.</p></div><div style={s.controls}><input className="input-control" type="date" value={date} onChange={e => setDate(e.target.value)} /><button className="button-primary" type="button" onClick={refreshPlays} disabled={refreshing}>{refreshing ? 'Refreshing...' : 'Refresh Model Plays'}</button><button className="button-secondary" type="button" onClick={refreshResults} disabled={resultRefreshing}>{resultRefreshing ? 'Refreshing...' : 'Refresh Results'}</button></div></div><div style={s.tabs}>{TABS.map(key => <button key={key} type="button" style={s.tab(activeTab === key)} onClick={() => setActiveTab(key)}>{TAB_LABELS[key]}</button>)}</div></section>
    {error && <div className="state-panel error">{error}</div>}{loading && <div className="state-panel">Loading model plays...</div>}
    <section style={s.statsScroller}><div style={s.statRail}><StatCard label="Visible Outputs" value={summary.total} /><StatCard label="Recommendations" value={summary.recommended} /><StatCard label="Model / Lean" value={summary.lean} /><StatCard label="Low Confidence" value={summary.low} /><StatCard label="Graded" value={summary.graded} /><StatCard label="Pending" value={summary.pending} /><StatCard label="Price Available" value={summary.price} /><StatCard label="Net P&L" value={fmtMoney(pnl.profit)} /></div></section>
    <Filters {...filterProps} />
    {activeTab === 'plays' && <PlaysTab groupedGames={groupedGames} topRows={topRows} gameFilter={gameFilter} />}
    {activeTab === 'results' && <ResultsTab rows={periodRows.length ? periodRows : filteredRows} selectedDate={date} period={resultsPeriod} setPeriod={setResultsPeriod} loading={periodLoading} />}
    {activeTab === 'pnl' && <PnlTab rows={filteredRows} />}
    {activeTab === 'quality' && <section style={s.section}><div style={s.sectionHeader}><div><div style={s.sectionTitle}>Quality Review</div><div style={s.rowMeta}>Low-confidence and incomplete rows are separated from performance without hiding them.</div></div></div><QualityTable rows={qualityRows.length ? qualityRows : filteredRows} /></section>}
    {activeTab === 'details' && <section style={s.section}><div style={s.sectionHeader}><div><div style={s.sectionTitle}>Details</div><div style={s.rowMeta}>Full table view for the selected slate and filters.</div></div></div><DetailsTable rows={filteredRows} /></section>}
  </div>
}
