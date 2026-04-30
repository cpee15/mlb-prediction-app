import React, { useEffect, useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import { API_BASE } from '../lib/api'

const API = API_BASE

const s = {
  page: { display: 'grid', gap: 18 },
  hero: { background: 'linear-gradient(135deg, #161b22 0%, #0d1117 58%, #101826 100%)', border: '1px solid #30363d', borderRadius: 16, padding: 22, boxShadow: '0 18px 48px rgba(0,0,0,0.24)' },
  header: { display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', gap: 16, flexWrap: 'wrap' },
  eyebrow: { color: '#58a6ff', fontSize: 12, fontWeight: 900, textTransform: 'uppercase', letterSpacing: 1.2, marginBottom: 8 },
  title: { fontSize: 30, lineHeight: 1.05, fontWeight: 900, color: '#e6edf3', margin: 0 },
  subtitle: { color: '#8b949e', fontSize: 14, marginTop: 8, maxWidth: 820 },
  controls: { display: 'flex', gap: 10, alignItems: 'center', flexWrap: 'wrap' },
  input: { background: '#0d1117', border: '1px solid #30363d', color: '#e6edf3', borderRadius: 10, padding: '10px 12px', fontSize: 14, outline: 'none' },
  select: { background: '#0d1117', border: '1px solid #30363d', color: '#e6edf3', borderRadius: 10, padding: '9px 11px', fontSize: 13, outline: 'none', maxWidth: '100%' },
  button: { background: '#238636', border: '1px solid #2ea043', color: '#fff', borderRadius: 10, padding: '10px 14px', fontSize: 13, fontWeight: 900, cursor: 'pointer' },
  mutedButton: { background: '#21262d', border: '1px solid #30363d', color: '#58a6ff', borderRadius: 9, padding: '8px 11px', fontSize: 12, fontWeight: 900, cursor: 'pointer' },
  stats: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(145px, 1fr))', gap: 10, marginTop: 18 },
  statCard: { background: 'rgba(13,17,23,0.72)', border: '1px solid #30363d', borderRadius: 12, padding: '13px 14px' },
  statLabel: { color: '#8b949e', fontSize: 10, textTransform: 'uppercase', letterSpacing: 0.9, fontWeight: 900 },
  statValue: { color: '#e6edf3', fontSize: 24, fontWeight: 900, marginTop: 5 },
  toolbar: { display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 12, flexWrap: 'wrap', background: '#161b22', border: '1px solid #30363d', borderRadius: 12, padding: '12px 14px' },
  toolbarText: { color: '#8b949e', fontSize: 13 },
  grid: { display: 'grid', gap: 12 },
  card: { background: '#161b22', border: '1px solid #30363d', borderRadius: 14, padding: 0, overflow: 'hidden' },
  cardTop: { display: 'grid', gridTemplateColumns: 'minmax(0, 1fr) auto', gap: 14, alignItems: 'center', padding: '15px 16px', borderBottom: '1px solid #30363d', background: '#111820' },
  matchup: { color: '#e6edf3', fontSize: 18, fontWeight: 900 },
  metaRow: { display: 'flex', gap: 8, flexWrap: 'wrap', marginTop: 8 },
  chip: { color: '#8b949e', border: '1px solid #30363d', background: '#0d1117', borderRadius: 999, padding: '4px 8px', fontSize: 11, fontWeight: 800 },
  badge: matched => ({ display: 'inline-block', borderRadius: 999, padding: '5px 10px', fontSize: 11, fontWeight: 900, background: matched ? 'rgba(35,134,54,0.18)' : 'rgba(248,81,73,0.14)', border: matched ? '1px solid rgba(63,185,80,0.45)' : '1px solid rgba(248,81,73,0.45)', color: matched ? '#3fb950' : '#f85149' }),
  markets: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: 10, padding: '14px 16px' },
  market: { border: '1px solid #30363d', borderRadius: 12, padding: 12, background: '#0d1117' },
  marketTitle: { color: '#8b949e', fontSize: 10, textTransform: 'uppercase', letterSpacing: 0.9, fontWeight: 900, marginBottom: 9 },
  oddsLine: { display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 10, color: '#e6edf3', fontSize: 13, marginTop: 6, padding: '5px 0', borderTop: '1px solid rgba(48,54,61,0.55)' },
  price: { fontWeight: 900, color: '#e6edf3', whiteSpace: 'nowrap' },
  props: { borderTop: '1px solid #30363d', padding: '15px 16px 16px', background: '#111820' },
  propControls: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(210px, 1fr))', alignItems: 'end', gap: 10, marginBottom: 12 },
  controlLabel: { color: '#8b949e', fontSize: 10, textTransform: 'uppercase', letterSpacing: 0.8, fontWeight: 900, marginBottom: 6 },
  propsGrid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(235px, 1fr))', gap: 9 },
  propCard: { border: '1px solid #30363d', borderRadius: 10, padding: 10, background: '#0d1117' },
  propMarket: { color: '#d29922', fontSize: 10, fontWeight: 900, textTransform: 'uppercase', letterSpacing: 0.6, marginBottom: 6 },
  propName: { color: '#e6edf3', fontSize: 13, fontWeight: 900, lineHeight: 1.25 },
  propDetail: { color: '#8b949e', fontSize: 12, marginTop: 4 },
  table: { width: '100%', borderCollapse: 'collapse', fontSize: 12 },
  th: { textAlign: 'left', padding: '7px 8px', color: '#8b949e', borderBottom: '1px solid #30363d', fontWeight: 900, whiteSpace: 'nowrap' },
  td: { padding: '7px 8px', color: '#c9d1d9', borderBottom: '1px solid #21262d', whiteSpace: 'nowrap' },
  modelPanel: { borderTop: '1px solid #30363d', padding: '14px 16px 16px', background: '#0f1720' },
  modelTitle: { color: '#e6edf3', fontSize: 14, fontWeight: 900 },
  modelSubtitle: { color: '#8b949e', fontSize: 12, marginTop: 4 },
  section: { background: '#161b22', border: '1px solid #30363d', borderRadius: 14, padding: 16 },
  sectionHeader: { display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 12, flexWrap: 'wrap', marginBottom: 12 },
  sectionTitle: { color: '#e6edf3', fontSize: 18, fontWeight: 900 },
  error: { color: '#f85149', background: '#1f1116', border: '1px solid #3b2222', borderRadius: 12, padding: 14 },
  loader: { color: '#8b949e', textAlign: 'center', padding: 40 },
  empty: { color: '#8b949e', textAlign: 'center', padding: 34, border: '1px solid #30363d', borderRadius: 14, background: '#161b22' },
}

function normalizeTeamName(name) { return String(name || '').toLowerCase().replace(/[^a-z0-9]/g, '').replace(/^the/, '') }
function matchupKey(away, home) { return `${normalizeTeamName(away)}@${normalizeTeamName(home)}` }
function keyFromMatchup(m) { return matchupKey(m.away_team_name || m.away_team || m.away_name, m.home_team_name || m.home_team || m.home_name) }
function keyFromEvent(e) { return matchupKey(e?.away_team?.name || e?.away_team || '', e?.home_team?.name || e?.home_team || '') }
function keyFromModelGame(game) { return matchupKey(game?.away_team || game?.away_team_name || game?.away || '', game?.home_team || game?.home_team_name || game?.home || '') }
function american(v) { if (v == null || v === '') return '—'; const n = Number(v); if (Number.isNaN(n)) return String(v); return n > 0 ? `+${n}` : `${n}` }
function pct(v) { if (v == null || v === '') return '—'; const n = Number(v); if (Number.isNaN(n)) return String(v); const pctValue = n <= 1 ? n * 100 : n; return `${Math.round(pctValue)}%` }
function formatTime(iso) { if (!iso) return '—'; try { return new Date(iso).toLocaleString('en-US', { month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit', timeZone: 'America/New_York' }) + ' ET' } catch { return '—' } }
function cleanMarketName(name) { return String(name || 'Market').replaceAll('_', ' ') }
function getMarkets(event) { return Array.isArray(event?.markets) ? event.markets : [] }
function findMarket(event, keys) { const wanted = Array.isArray(keys) ? keys : [keys]; return getMarkets(event).find(m => wanted.includes(m.market_key) || wanted.includes(m.market_type) || wanted.includes(m.market_name)) }
function selectionLabel(sel) { return `${sel?.name || sel?.description || '—'}${sel?.line != null ? ` ${sel.line}` : ''}` }
function asArray(value) { return Array.isArray(value) ? value : [] }
function firstDefined(...values) { return values.find(v => v !== undefined && v !== null && v !== '') }
function modelGamesFromPayload(payload) { if (Array.isArray(payload)) return payload; if (Array.isArray(payload?.games)) return payload.games; if (Array.isArray(payload?.models)) return payload.models; if (Array.isArray(payload?.game_models)) return payload.game_models; return [] }
function propCandidatesFromPayload(payload) { if (Array.isArray(payload?.top_prop_model_candidates)) return payload.top_prop_model_candidates; if (Array.isArray(payload?.top_props)) return payload.top_props; if (Array.isArray(payload?.prop_candidates)) return payload.prop_candidates; if (Array.isArray(payload?.props)) return payload.props; return [] }

function marketKey(market) { return String(market?.market_key || market?.market_type || market?.market_name || '') }
function propCategory(market) {
  const key = marketKey(market).toLowerCase()
  if (key.startsWith('batter_')) return 'Batter Props'
  if (key.startsWith('pitcher_')) return 'Pitcher Props'
  if (['h2h', 'spreads', 'totals'].includes(key)) return 'Game Lines'
  return 'Other Props'
}
function propOptionLabel(market) { return cleanMarketName(market?.market_name || market?.market_key || market?.market_type) }
function groupMarketsByCategory(markets) {
  return markets.reduce((acc, market) => {
    const category = propCategory(market)
    if (!acc[category]) acc[category] = []
    acc[category].push(market)
    return acc
  }, {})
}

function MarketBox({ label, market }) {
  const selections = market?.selections || []
  return <div style={s.market}><div style={s.marketTitle}>{label}</div>{selections.length === 0 && <div style={s.oddsLine}><span>Unavailable</span><strong style={s.price}>—</strong></div>}{selections.slice(0, 3).map((sel, idx) => <div key={`${label}-${idx}`} style={s.oddsLine}><span>{selectionLabel(sel)}</span><strong style={s.price}>{american(sel.price)}</strong></div>)}</div>
}

function PropsDropdownBoard({ eventId }) {
  const [open, setOpen] = useState(false)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [data, setData] = useState(null)
  const [category, setCategory] = useState('')
  const [marketIndex, setMarketIndex] = useState('0')
  const [selectionIndex, setSelectionIndex] = useState('0')

  function loadProps() {
    if (!eventId || loading) return
    setLoading(true)
    setError(null)
    fetch(`${API}/odds/draftkings/event/${eventId}/props`)
      .then(async r => { if (!r.ok) throw new Error(`${r.status} ${r.statusText}: ${await r.text()}`); return r.json() })
      .then(json => { setData(json); setLoading(false) })
      .catch(err => { setError(String(err?.message || err)); setLoading(false) })
  }

  function toggle() {
    const next = !open
    setOpen(next)
    if (next && !data) loadProps()
  }

  const rawMarkets = useMemo(() => {
    const direct = asArray(data?.markets)
    const nested = asArray(data?.event?.markets)
    const markets = direct.length ? direct : nested
    return markets.filter(m => asArray(m?.selections).length > 0)
  }, [data])

  const grouped = useMemo(() => groupMarketsByCategory(rawMarkets), [rawMarkets])
  const categories = useMemo(() => Object.keys(grouped), [grouped])
  const activeCategory = category || categories[0] || ''
  const activeMarkets = grouped[activeCategory] || []
  const activeMarket = activeMarkets[Number(marketIndex)] || activeMarkets[0] || null
  const selections = asArray(activeMarket?.selections)
  const activeSelection = selections[Number(selectionIndex)] || selections[0] || null

  useEffect(() => {
    if (!categories.includes(category)) setCategory(categories[0] || '')
  }, [categories.join('|')])

  useEffect(() => { setMarketIndex('0'); setSelectionIndex('0') }, [activeCategory])
  useEffect(() => { setSelectionIndex('0') }, [marketIndex])

  return <div style={s.props}>
    <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12, alignItems: 'center', flexWrap: 'wrap', marginBottom: open ? 12 : 0 }}>
      <div><div style={s.modelTitle}>DraftKings Props</div><div style={s.modelSubtitle}>Game → type of prop → dropdown of every available prop in that category.</div></div>
      <button type="button" style={s.mutedButton} onClick={toggle}>{open ? 'Hide Prop Board' : 'Load Prop Board'}</button>
    </div>

    {open && loading && <div style={s.loader}>Loading DraftKings props...</div>}
    {open && error && <div style={s.error}>Props error: {error}</div>}
    {open && !loading && !error && data && rawMarkets.length === 0 && <div style={s.empty}>No DraftKings prop markets returned for this game.</div>}

    {open && !loading && !error && rawMarkets.length > 0 && <>
      <div style={s.propControls}>
        <label><div style={s.controlLabel}>Type of Prop</div><select value={activeCategory} onChange={e => setCategory(e.target.value)} style={s.select}>{categories.map(cat => <option key={cat} value={cat}>{cat} ({grouped[cat].length})</option>)}</select></label>
        <label><div style={s.controlLabel}>Prop Market</div><select value={marketIndex} onChange={e => setMarketIndex(e.target.value)} style={s.select}>{activeMarkets.map((market, idx) => <option key={`${marketKey(market)}-${idx}`} value={String(idx)}>{propOptionLabel(market)} ({asArray(market.selections).length})</option>)}</select></label>
        <label><div style={s.controlLabel}>Available Props</div><select value={selectionIndex} onChange={e => setSelectionIndex(e.target.value)} style={s.select}>{selections.map((sel, idx) => <option key={`${sel.description || sel.name}-${sel.line}-${sel.price}-${idx}`} value={String(idx)}>{sel.description || sel.name || 'Selection'} · {sel.name || ''}{sel.line != null ? ` ${sel.line}` : ''} · {american(sel.price)}</option>)}</select></label>
      </div>

      {activeSelection && <div style={{ ...s.propCard, marginBottom: 12, borderColor: '#58a6ff' }}>
        <div style={s.propMarket}>{activeCategory} · {propOptionLabel(activeMarket)}</div>
        <div style={s.propName}>{activeSelection.description || activeSelection.name || '—'}</div>
        <div style={s.propDetail}>{selectionLabel(activeSelection)} · <strong style={{ color: '#e6edf3' }}>{american(activeSelection.price)}</strong> · Implied {pct(activeSelection?.odds?.implied_probability)}</div>
      </div>}

      <div style={{ overflowX: 'auto' }}>
        <table style={s.table}>
          <thead><tr><th style={s.th}>Player / Side</th><th style={s.th}>Selection</th><th style={s.th}>Line</th><th style={s.th}>Price</th><th style={s.th}>Implied</th><th style={s.th}>Market</th></tr></thead>
          <tbody>{selections.map((sel, idx) => <tr key={`${sel.description || sel.name}-${sel.line}-${sel.price}-${idx}`}><td style={s.td}>{sel.description || sel.name || '—'}</td><td style={s.td}>{sel.name || '—'}</td><td style={s.td}>{sel.line ?? '—'}</td><td style={{ ...s.td, fontWeight: 900, color: '#e6edf3' }}>{american(sel.price)}</td><td style={s.td}>{pct(sel?.odds?.implied_probability)}</td><td style={s.td}>{propOptionLabel(activeMarket)}</td></tr>)}</tbody>
        </table>
      </div>
    </>}
  </div>
}

function ModelSummary({ model }) {
  if (!model) return null
  const root = model.models || model
  const moneyline = root.moneyline || {}
  const spread = root.spread || root.run_line || {}
  const total = root.total || {}
  const cards = [
    ['Moneyline', moneyline],
    ['Run Line', spread],
    ['Total', total],
  ].filter(([, m]) => m && Object.keys(m).length)
  if (!cards.length) return null
  return <div style={s.modelPanel}><div style={s.modelTitle}>Model Snapshot</div><div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(210px, 1fr))', gap: 10, marginTop: 10 }}>{cards.map(([title, m]) => <div key={title} style={s.market}><div style={s.marketTitle}>{title}</div><div style={s.propName}>{m.pick || 'No pick'}</div><div style={s.propDetail}>Model {pct(m.model_probability)} · Market {pct(m.market_implied_probability)} · Edge {m.edge ?? '—'}</div></div>)}</div></div>
}

function TopPropModelCandidates({ candidates }) {
  const rows = asArray(candidates).slice(0, 20)
  return <section style={s.section}><div style={s.sectionHeader}><div><div style={s.sectionTitle}>Top Prop Model Candidates</div><div style={s.modelSubtitle}>Ranked output from /daily-odds/models. Actual DraftKings prop menus are inside each game card.</div></div><span style={s.chip}>{rows.length} shown</span></div>{rows.length === 0 ? <div style={s.empty}>No top prop model candidates returned.</div> : <div style={s.propsGrid}>{rows.map((candidate, idx) => <div key={`${candidate.player_name || candidate.pick}-${idx}`} style={s.propCard}><div style={s.propMarket}>{cleanMarketName(candidate.market || candidate.market_name)}</div><div style={s.propName}>{candidate.player_name || candidate.pick || 'Candidate'}</div><div style={s.propDetail}>{candidate.pick || candidate.selection || '—'} · Edge {candidate.edge ?? '—'} · Confidence {pct(candidate.confidence)}</div></div>)}</div>}</section>
}

export default function DailyOddsPage() {
  const today = new Date().toISOString().slice(0, 10)
  const [date, setDate] = useState(today)
  const [matchups, setMatchups] = useState([])
  const [events, setEvents] = useState([])
  const [modelPayload, setModelPayload] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [modelError, setModelError] = useState(null)
  const [lastRefreshed, setLastRefreshed] = useState(null)

  function load() {
    setLoading(true)
    setError(null)
    setModelError(null)
    Promise.all([
      fetch(`${API}/matchups?date=${date}`).then(async r => { if (!r.ok) throw new Error(`/matchups failed: ${r.status} ${r.statusText}: ${await r.text()}`); return r.json() }),
      fetch(`${API}/odds/draftkings/events?date=${date}`).then(async r => { if (!r.ok) throw new Error(`/odds/draftkings/events failed: ${r.status} ${r.statusText}: ${await r.text()}`); return r.json() }),
      fetch(`${API}/daily-odds/models?date=${date}`).then(async r => { if (!r.ok) throw new Error(`/daily-odds/models failed: ${r.status} ${r.statusText}: ${await r.text()}`); return r.json() }).catch(err => ({ __modelError: String(err?.message || err) })),
    ]).then(([matchupData, oddsData, modelsData]) => {
      setMatchups(Array.isArray(matchupData) ? matchupData : [])
      setEvents(Array.isArray(oddsData?.events) ? oddsData.events : [])
      if (modelsData?.__modelError) { setModelPayload(null); setModelError(modelsData.__modelError) } else { setModelPayload(modelsData) }
      setLastRefreshed(new Date())
      setLoading(false)
    }).catch(err => { setError(String(err?.message || err)); setLoading(false) })
  }

  useEffect(() => { load() }, [date])

  const matchupByKey = useMemo(() => { const map = new Map(); matchups.forEach(m => { const key = keyFromMatchup(m); if (key !== '@') map.set(key, m) }); return map }, [matchups])
  const modelByKey = useMemo(() => { const map = new Map(); modelGamesFromPayload(modelPayload).forEach(game => { const key = keyFromModelGame(game); if (key !== '@') map.set(key, game) }); return map }, [modelPayload])
  const topPropCandidates = useMemo(() => propCandidatesFromPayload(modelPayload), [modelPayload])
  const rows = useMemo(() => events.map(event => { const key = keyFromEvent(event); const matchup = matchupByKey.get(key); const model = modelByKey.get(key); return { event, matchup, model, matched: Boolean(matchup), key } }), [events, matchupByKey, modelByKey])
  const matchedCount = rows.filter(r => r.matched).length
  const modelCount = modelGamesFromPayload(modelPayload).length

  return <div style={s.page}>
    <section style={s.hero}>
      <div style={s.header}>
        <div><div style={s.eyebrow}>DraftKings board</div><h1 style={s.title}>Daily Odds</h1><div style={s.subtitle}>DraftKings odds organized by game, then prop type, then every available prop in that category. Open a game card and load the prop board.</div></div>
        <div style={s.controls}><input type="date" value={date} onChange={e => setDate(e.target.value)} style={s.input} /><button type="button" style={s.button} onClick={load} disabled={loading}>{loading ? 'Refreshing...' : 'Refresh Odds'}</button></div>
      </div>
      <div style={s.stats}>
        <div style={s.statCard}><div style={s.statLabel}>MLB Games</div><div style={s.statValue}>{matchups.length}</div></div>
        <div style={s.statCard}><div style={s.statLabel}>DK Events</div><div style={s.statValue}>{events.length}</div></div>
        <div style={s.statCard}><div style={s.statLabel}>Matched</div><div style={s.statValue}>{matchedCount}</div></div>
        <div style={s.statCard}><div style={s.statLabel}>Model Games</div><div style={s.statValue}>{modelCount}</div></div>
        <div style={s.statCard}><div style={s.statLabel}>Top Props</div><div style={s.statValue}>{topPropCandidates.length}</div></div>
        <div style={s.statCard}><div style={s.statLabel}>Last Refreshed</div><div style={{ ...s.statValue, fontSize: 15 }}>{lastRefreshed ? lastRefreshed.toLocaleTimeString() : '—'}</div></div>
      </div>
    </section>

    <div style={s.toolbar}><div style={s.toolbarText}>{rows.length} DraftKings games loaded for {date}</div><div style={s.toolbarText}>Game → Type of Prop → Prop Market → Available Props</div></div>
    {error && <div style={s.error}>{error}</div>}
    {modelError && <div style={s.error}>Model panel error: {modelError}</div>}
    {loading && <div style={s.loader}>Loading daily odds...</div>}
    {!loading && !error && rows.length === 0 && <div style={s.empty}>No DraftKings events returned for {date}. Confirm PR #130 is deployed, ODDS_API_KEY is valid, and the provider has events for this slate.</div>}
    {!loading && !error && <TopPropModelCandidates candidates={topPropCandidates} />}

    <div style={s.grid}>{rows.map(({ event, matchup, model, matched, key }, idx) => {
      const away = event?.away_team?.name || event?.away_team || matchup?.away_team_name || 'Away'
      const home = event?.home_team?.name || event?.home_team || matchup?.home_team_name || 'Home'
      const moneyline = findMarket(event, 'h2h')
      const spread = findMarket(event, 'spreads')
      const total = findMarket(event, 'totals')
      return <article key={`${event.event_id || key || idx}`} style={s.card}>
        <div style={s.cardTop}>
          <div><div style={s.matchup}>{away} @ {home}</div><div style={s.metaRow}><span style={s.chip}>Time: {formatTime(matchup?.game_time || event?.start_time || event?.commence_time)}</span><span style={s.chip}>MLB: {matchup?.game_pk ? <Link to={`/matchup/${matchup.game_pk}`} style={{ color: '#58a6ff', textDecoration: 'none' }}>{matchup.game_pk}</Link> : '—'}</span><span style={s.chip}>DK: {event.event_id || '—'}</span><span style={s.chip}>Model: {model ? 'loaded' : 'none'}</span></div></div>
          <span style={s.badge(matched)}>{matched ? 'MATCHED' : 'UNMATCHED'}</span>
        </div>
        <div style={s.markets}><MarketBox label="Moneyline" market={moneyline} /><MarketBox label="Run Line" market={spread} /><MarketBox label="Total" market={total} /></div>
        <PropsDropdownBoard eventId={event.event_id} />
        <ModelSummary model={model} />
      </article>
    })}</div>
  </div>
}
