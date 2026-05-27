import React, { useEffect, useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import { API_BASE, fetchJson, readCachedJson } from '../lib/api'

const API = API_BASE
const MATCHUPS_TTL_SECONDS = 120
const ODDS_EVENTS_TTL_SECONDS = 90
const DAILY_ODDS_MODELS_TTL_SECONDS = 120
const DAILY_ODDS_UNIFIED_TTL_SECONDS = 180
const EVENT_PROPS_TTL_SECONDS = 90

const s = {
  page: { display: 'grid', gap: 18 },
  hero: { background: 'linear-gradient(135deg, #161b22 0%, #0d1117 58%, #101826 100%)', border: '1px solid #30363d', borderRadius: 16, padding: 22, boxShadow: '0 18px 48px rgba(0,0,0,0.24)' },
  header: { display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', gap: 16, flexWrap: 'wrap' },
  eyebrow: { color: '#58a6ff', fontSize: 12, fontWeight: 900, textTransform: 'uppercase', letterSpacing: 1.2, marginBottom: 8 },
  title: { fontSize: 30, lineHeight: 1.05, fontWeight: 900, color: '#e6edf3', margin: 0 },
  subtitle: { color: '#8b949e', fontSize: 14, marginTop: 8, maxWidth: 900 },
  controls: { display: 'flex', gap: 10, alignItems: 'center', flexWrap: 'wrap' },
  input: { background: '#0d1117', border: '1px solid #30363d', color: '#e6edf3', borderRadius: 10, padding: '10px 12px', fontSize: 14, outline: 'none' },
  button: { background: '#238636', border: '1px solid #2ea043', color: '#fff', borderRadius: 10, padding: '10px 14px', fontSize: 13, fontWeight: 900, cursor: 'pointer' },
  mutedButton: { background: '#21262d', border: '1px solid #30363d', color: '#58a6ff', borderRadius: 9, padding: '9px 12px', fontSize: 12, fontWeight: 900, cursor: 'pointer' },
  stats: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(145px, 1fr))', gap: 10, marginTop: 18 },
  statCard: { background: 'rgba(13,17,23,0.72)', border: '1px solid #30363d', borderRadius: 12, padding: '13px 14px' },
  statLabel: { color: '#8b949e', fontSize: 10, textTransform: 'uppercase', letterSpacing: 0.9, fontWeight: 900 },
  statValue: { color: '#e6edf3', fontSize: 24, fontWeight: 900, marginTop: 5 },
  section: { background: '#161b22', border: '1px solid #30363d', borderRadius: 14, padding: 16 },
  sectionHeader: { display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 12, flexWrap: 'wrap', marginBottom: 12 },
  sectionTitle: { color: '#e6edf3', fontSize: 18, fontWeight: 900 },
  modelSubtitle: { color: '#8b949e', fontSize: 12, marginTop: 4 },
  boardGrid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(230px, 1fr))', gap: 12, alignItems: 'end' },
  label: { color: '#8b949e', fontSize: 10, textTransform: 'uppercase', letterSpacing: 0.8, fontWeight: 900, marginBottom: 6 },
  select: { width: '100%', background: '#0d1117', border: '1px solid #30363d', color: '#e6edf3', borderRadius: 10, padding: '10px 11px', fontSize: 13, outline: 'none' },
  detailGrid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: 10, marginTop: 14 },
  panel: { border: '1px solid #30363d', borderRadius: 12, background: '#0d1117', padding: 12 },
  panelTitle: { color: '#58a6ff', fontSize: 11, fontWeight: 900, textTransform: 'uppercase', letterSpacing: 0.7, marginBottom: 8 },
  propName: { color: '#e6edf3', fontSize: 14, fontWeight: 900, lineHeight: 1.25 },
  propDetail: { color: '#8b949e', fontSize: 12, marginTop: 5, lineHeight: 1.45 },
  chip: { color: '#8b949e', border: '1px solid #30363d', background: '#0d1117', borderRadius: 999, padding: '4px 8px', fontSize: 11, fontWeight: 800 },
  badge: matched => ({ display: 'inline-block', borderRadius: 999, padding: '5px 10px', fontSize: 11, fontWeight: 900, background: matched ? 'rgba(35,134,54,0.18)' : 'rgba(248,81,73,0.14)', border: matched ? '1px solid rgba(63,185,80,0.45)' : '1px solid rgba(248,81,73,0.45)', color: matched ? '#3fb950' : '#f85149' }),
  card: { background: '#161b22', border: '1px solid #30363d', borderRadius: 14, padding: 0, overflow: 'hidden' },
  cardTop: { display: 'grid', gridTemplateColumns: 'minmax(0, 1fr) auto', gap: 14, alignItems: 'center', padding: '15px 16px', borderBottom: '1px solid #30363d', background: '#111820' },
  matchup: { color: '#e6edf3', fontSize: 18, fontWeight: 900 },
  metaRow: { display: 'flex', gap: 8, flexWrap: 'wrap', marginTop: 8 },
  markets: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: 10, padding: '14px 16px' },
  market: { border: '1px solid #30363d', borderRadius: 12, padding: 12, background: '#0d1117' },
  marketTitle: { color: '#8b949e', fontSize: 10, textTransform: 'uppercase', letterSpacing: 0.9, fontWeight: 900, marginBottom: 9 },
  oddsLine: { display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 10, color: '#e6edf3', fontSize: 13, marginTop: 6, padding: '5px 0', borderTop: '1px solid rgba(48,54,61,0.55)' },
  price: { fontWeight: 900, color: '#e6edf3', whiteSpace: 'nowrap' },
  tableWrap: { overflowX: 'auto', marginTop: 12, border: '1px solid #30363d', borderRadius: 12 },
  table: { width: '100%', borderCollapse: 'collapse', fontSize: 12 },
  th: { textAlign: 'left', padding: '9px 10px', color: '#8b949e', borderBottom: '1px solid #30363d', fontWeight: 900, whiteSpace: 'nowrap', background: '#111820' },
  td: { padding: '9px 10px', color: '#c9d1d9', borderBottom: '1px solid #21262d', whiteSpace: 'nowrap' },
  recapGrid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(240px, 1fr))', gap: 10 },
  error: { color: '#f85149', background: '#1f1116', border: '1px solid #3b2222', borderRadius: 12, padding: 14 },
  loader: { color: '#8b949e', textAlign: 'center', padding: 30 },
  empty: { color: '#8b949e', textAlign: 'center', padding: 30, border: '1px solid #30363d', borderRadius: 14, background: '#161b22' },
}

function asArray(value) { return Array.isArray(value) ? value : [] }
function normalizeTeamName(name) { return String(name || '').toLowerCase().replace(/[^a-z0-9]/g, '').replace(/^the/, '') }
function matchupKey(away, home) { return `${normalizeTeamName(away)}@${normalizeTeamName(home)}` }
function keyFromMatchup(m) { return matchupKey(m.away_team_name || m.away_team || m.away_name, m.home_team_name || m.home_team || m.home_name) }
function keyFromEvent(e) { return matchupKey(e?.away_team?.name || e?.away_team || '', e?.home_team?.name || e?.home_team || '') }
function american(v) { if (v == null || v === '') return 'Unavailable'; const n = Number(v); if (Number.isNaN(n)) return String(v); return n > 0 ? `+${n}` : `${n}` }
function pct(v) { if (v == null || v === '') return 'Unavailable'; const n = Number(v); if (Number.isNaN(n)) return String(v); const pctValue = n <= 1 ? n * 100 : n; return `${Math.round(pctValue)}%` }
function cleanMarketName(name) { return String(name || 'Market').replaceAll('_', ' ') }
function formatTime(iso) { if (!iso) return 'Time pending'; try { return new Date(iso).toLocaleString('en-US', { month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit', timeZone: 'America/New_York' }) + ' ET' } catch { return 'Time pending' } }
function marketKey(market) { return String(market?.market_key || market?.market_type || market?.market_name || '') }
function getMarkets(event) { return Array.isArray(event?.markets) ? event.markets : [] }
function findMarket(event, keys) { const wanted = Array.isArray(keys) ? keys : [keys]; return getMarkets(event).find(m => wanted.includes(marketKey(m)) || wanted.includes(m.market_name)) }
function selectionLabel(sel) { return `${sel?.description || sel?.name || 'Selection'}${sel?.name && sel?.description ? ` ${sel.name}` : ''}${sel?.line != null ? ` ${sel.line}` : ''}` }
function gameLabel(event) { return `${event?.away_team?.name || event?.away_team || 'Away'} @ ${event?.home_team?.name || event?.home_team || 'Home'}` }
function propCategory(market) {
  const key = marketKey(market).toLowerCase()
  if (key.startsWith('batter_')) return 'Batter Props'
  if (key.startsWith('pitcher_')) return 'Pitcher Props'
  if (['h2h', 'spreads', 'totals'].includes(key)) return 'Game Lines'
  return 'Other Props'
}
function groupMarketsByCategory(markets) {
  return asArray(markets).filter(m => asArray(m?.selections).length > 0).reduce((acc, market) => {
    const category = propCategory(market)
    if (!acc[category]) acc[category] = []
    acc[category].push(market)
    return acc
  }, {})
}
function modelGamesFromPayload(payload) { if (Array.isArray(payload?.games)) return payload.games; if (Array.isArray(payload?.models)) return payload.models; return [] }

function MarketBox({ label, market }) {
  const selections = asArray(market?.selections).slice(0, 3)
  return <div style={s.market}><div style={s.marketTitle}>{label}</div>{selections.length === 0 ? <div style={s.oddsLine}><span>Unavailable</span><strong style={s.price}>Unavailable</strong></div> : selections.map((sel, idx) => <div key={`${label}-${idx}`} style={s.oddsLine}><span>{selectionLabel(sel)}</span><strong style={s.price}>{american(sel.price)}</strong></div>)}</div>
}

function DailyRecapPanel({ unifiedPayload, loading, onLoad }) {
  const recap = unifiedPayload?.daily_recap
  const hitters = asArray(unifiedPayload?.top_hitter_angles).slice(0, 3)
  const pitchers = asArray(unifiedPayload?.top_pitcher_angles).slice(0, 3)
  const arsenal = asArray(unifiedPayload?.batter_vs_arsenal_edges).slice(0, 3)
  return <section style={s.section}>
    <div style={s.sectionHeader}>
      <div><div style={s.sectionTitle}>Daily Recap</div><div style={s.modelSubtitle}>Loads the heavier My Dashboard, AI Data Assistant, Model Projection, and Batter vs Arsenal context only when requested.</div></div>
      <button type="button" style={s.mutedButton} onClick={onLoad} disabled={loading || Boolean(unifiedPayload)}>{loading ? 'Loading Recap...' : unifiedPayload ? 'Recap Loaded' : 'Load Model Recap'}</button>
    </div>
    {!unifiedPayload && <div style={s.empty}>Fast odds are loaded. Click “Load Model Recap” when you want the heavier model intelligence on this page.</div>}
    {recap && <div style={s.recapGrid}>
      <SummaryCard title="Best Side" item={recap.best_side} primary={recap.best_side?.pick} />
      <SummaryCard title="Best Total" item={recap.best_total} primary={recap.best_total?.entity_name || recap.best_total?.category} />
      <SummaryCard title="Best Pitcher" item={recap.best_pitcher} primary={recap.best_pitcher?.entity_name} />
      <SummaryCard title="Best Hitter" item={recap.best_hitter} primary={recap.best_hitter?.entity_name} />
    </div>}
    {unifiedPayload && <div style={{ ...s.detailGrid, marginTop: 12 }}>
      <CompactList title="Top Hitter Angles" rows={hitters} />
      <CompactList title="Top Pitcher Angles" rows={pitchers} />
      <CompactList title="Batter vs Arsenal" rows={arsenal} />
    </div>}
  </section>
}

function SummaryCard({ title, item, primary }) {
  return <div style={s.panel}><div style={s.panelTitle}>{title}</div><div style={s.propName}>{primary || 'No signal loaded'}</div>{item?.primary_reason && <div style={s.propDetail}>{item.primary_reason}</div>}<div style={s.propDetail}>Score {item?.score ?? 'Unavailable'} · Confidence {typeof item?.confidence === 'number' ? pct(item.confidence) : item?.confidence || 'Unavailable'}</div></div>
}

function CompactList({ title, rows }) {
  return <div style={s.panel}><div style={s.panelTitle}>{title}</div>{rows.length === 0 ? <div style={s.propDetail}>No rows returned.</div> : rows.map((row, idx) => <div key={`${title}-${idx}`} style={{ borderTop: idx ? '1px solid #30363d' : 'none', paddingTop: idx ? 8 : 0, marginTop: idx ? 8 : 0 }}><div style={s.propName}>{row.entity_name || row.pick || row.player_name || 'Signal'}</div><div style={s.propDetail}>{row.primary_reason || asArray(row.reasoning)[0] || row.source || 'Model signal'}</div></div>)}</div>
}

function OddsBoard({ events, selectedEventId, setSelectedEventId, propsData, propsLoading, propsError }) {
  const selectedEvent = events.find(event => String(event.event_id) === String(selectedEventId)) || events[0]
  const gameMarkets = getMarkets(selectedEvent)
  const propMarkets = asArray(propsData?.markets).length ? propsData.markets : asArray(propsData?.event?.markets)
  const allMarkets = [...gameMarkets, ...propMarkets]
  const grouped = useMemo(() => groupMarketsByCategory(allMarkets), [allMarkets])
  const categories = Object.keys(grouped)
  const [category, setCategory] = useState('')
  const [marketIndex, setMarketIndex] = useState('0')
  const [selectionIndex, setSelectionIndex] = useState('0')
  const activeCategory = category && categories.includes(category) ? category : categories[0] || ''
  const activeMarkets = grouped[activeCategory] || []
  const activeMarket = activeMarkets[Number(marketIndex)] || activeMarkets[0] || null
  const selections = asArray(activeMarket?.selections)
  const activeSelection = selections[Number(selectionIndex)] || selections[0] || null

  useEffect(() => { if (!category && categories[0]) setCategory(categories[0]) }, [categories.join('|')])
  useEffect(() => { setMarketIndex('0'); setSelectionIndex('0') }, [activeCategory, selectedEventId])
  useEffect(() => { setSelectionIndex('0') }, [marketIndex])

  if (!selectedEvent) return <section style={s.section}><div style={s.empty}>No DraftKings events returned.</div></section>

  return <section style={s.section}>
    <div style={s.sectionHeader}>
      <div><div style={s.sectionTitle}>Organized Odds Board</div><div style={s.modelSubtitle}>Game → Odds Category → Market → Player / selection. Props load only for the selected game.</div></div>
      <span style={s.chip}>{propsLoading ? 'Loading selected game props...' : `${allMarkets.length} markets`}</span>
    </div>
    <div style={s.boardGrid}>
      <label><div style={s.label}>Game</div><select style={s.select} value={selectedEvent?.event_id || ''} onChange={e => setSelectedEventId(e.target.value)}>{events.map(event => <option key={event.event_id} value={event.event_id}>{gameLabel(event)}</option>)}</select></label>
      <label><div style={s.label}>Odds Category</div><select style={s.select} value={activeCategory} onChange={e => setCategory(e.target.value)}>{categories.map(cat => <option key={cat} value={cat}>{cat} ({grouped[cat].length})</option>)}</select></label>
      <label><div style={s.label}>Market</div><select style={s.select} value={marketIndex} onChange={e => setMarketIndex(e.target.value)}>{activeMarkets.map((market, idx) => <option key={`${marketKey(market)}-${idx}`} value={String(idx)}>{cleanMarketName(market.market_name || marketKey(market))} ({asArray(market.selections).length})</option>)}</select></label>
      <label><div style={s.label}>Player / Selection</div><select style={s.select} value={selectionIndex} onChange={e => setSelectionIndex(e.target.value)}>{selections.map((sel, idx) => <option key={`${sel.description || sel.name}-${sel.line}-${sel.price}-${idx}`} value={String(idx)}>{selectionLabel(sel)} · {american(sel.price)}</option>)}</select></label>
    </div>
    {propsError && <div style={{ ...s.error, marginTop: 12 }}>Selected game props unavailable: {propsError}</div>}
    {activeSelection && <div style={s.detailGrid}>
      <div style={s.panel}><div style={s.panelTitle}>Selected Bet</div><div style={s.propName}>{activeSelection.description || activeSelection.name || 'Selection'}</div><div style={s.propDetail}>{selectionLabel(activeSelection)}</div></div>
      <div style={s.panel}><div style={s.panelTitle}>Price</div><div style={s.propName}>{american(activeSelection.price)}</div><div style={s.propDetail}>Implied {pct(activeSelection?.odds?.implied_probability)}</div></div>
      <div style={s.panel}><div style={s.panelTitle}>Market</div><div style={s.propName}>{cleanMarketName(activeMarket?.market_name || marketKey(activeMarket))}</div><div style={s.propDetail}>{activeCategory}</div></div>
    </div>}
    {selections.length > 0 && <div style={s.tableWrap}><table style={s.table}><thead><tr><th style={s.th}>Player / Side</th><th style={s.th}>Selection</th><th style={s.th}>Line</th><th style={s.th}>Price</th><th style={s.th}>Implied</th><th style={s.th}>Market</th></tr></thead><tbody>{selections.map((sel, idx) => <tr key={`${sel.description || sel.name}-${sel.line}-${sel.price}-${idx}`}><td style={s.td}>{sel.description || sel.name || 'Selection'}</td><td style={s.td}>{sel.name || 'Selection'}</td><td style={s.td}>{sel.line ?? 'Unavailable'}</td><td style={{ ...s.td, fontWeight: 900, color: '#e6edf3' }}>{american(sel.price)}</td><td style={s.td}>{pct(sel?.odds?.implied_probability)}</td><td style={s.td}>{cleanMarketName(activeMarket?.market_name || marketKey(activeMarket))}</td></tr>)}</tbody></table></div>}
  </section>
}

export default function DailyOddsPage() {
  const today = new Date().toISOString().slice(0, 10)
  const [date, setDate] = useState(today)
  const initialMatchupsUrl = `${API}/matchups?date=${today}`
  const initialEventsUrl = `${API}/odds/draftkings/events?date=${today}`
  const initialModelsUrl = `${API}/daily-odds/models?date=${today}`
  const [matchups, setMatchups] = useState(() => {
    const cached = readCachedJson(initialMatchupsUrl, MATCHUPS_TTL_SECONDS)
    return Array.isArray(cached) ? cached : []
  })
  const [events, setEvents] = useState(() => {
    const cached = readCachedJson(initialEventsUrl, ODDS_EVENTS_TTL_SECONDS)
    return Array.isArray(cached?.events) ? cached.events : []
  })
  const [modelPayload, setModelPayload] = useState(() => readCachedJson(initialModelsUrl, DAILY_ODDS_MODELS_TTL_SECONDS))
  const [unifiedPayload, setUnifiedPayload] = useState(null)
  const [selectedEventId, setSelectedEventId] = useState('')
  const [propsData, setPropsData] = useState(null)
  const [propsLoading, setPropsLoading] = useState(false)
  const [propsError, setPropsError] = useState(null)
  const [loading, setLoading] = useState(matchups.length === 0 && events.length === 0)
  const [unifiedLoading, setUnifiedLoading] = useState(false)
  const [error, setError] = useState(null)
  const [modelError, setModelError] = useState(null)
  const [lastRefreshed, setLastRefreshed] = useState(null)

  function load(forceRefresh = false) {
    setLoading(true)
    setError(null)
    setModelError(null)
    setUnifiedPayload(null)
    const matchupsUrl = `${API}/matchups?date=${date}`
    const eventsUrl = `${API}/odds/draftkings/events?date=${date}`
    const modelsUrl = `${API}/daily-odds/models?date=${date}`
    Promise.all([
      fetchJson(matchupsUrl, { ttlSeconds: MATCHUPS_TTL_SECONDS, forceRefresh }),
      fetchJson(eventsUrl, { ttlSeconds: ODDS_EVENTS_TTL_SECONDS, forceRefresh }),
      fetchJson(modelsUrl, { ttlSeconds: DAILY_ODDS_MODELS_TTL_SECONDS, forceRefresh }).catch(err => ({ __modelError: String(err?.message || err) })),
    ]).then(([matchupData, oddsData, modelsData]) => {
      const nextEvents = Array.isArray(oddsData?.events) ? oddsData.events : []
      setMatchups(Array.isArray(matchupData) ? matchupData : [])
      setEvents(nextEvents)
      setSelectedEventId(nextEvents[0]?.event_id || '')
      if (modelsData?.__modelError) { setModelPayload(null); setModelError(modelsData.__modelError) } else { setModelPayload(modelsData) }
      setLastRefreshed(new Date())
      setLoading(false)
    }).catch(err => { setError(String(err?.message || err)); setLoading(false) })
  }

  function loadUnified(forceRefresh = false) {
    setUnifiedLoading(true)
    const unifiedUrl = `${API}/daily-odds/models?date=${date}&include_unified=true`
    fetchJson(unifiedUrl, { ttlSeconds: DAILY_ODDS_UNIFIED_TTL_SECONDS, forceRefresh })
      .then(json => { setUnifiedPayload(json); setUnifiedLoading(false) })
      .catch(err => { setModelError(String(err?.message || err)); setUnifiedLoading(false) })
  }

  useEffect(() => { load(false) }, [date])

  useEffect(() => {
    if (!selectedEventId) { setPropsData(null); return }
    setPropsLoading(true)
    setPropsError(null)
    const propsUrl = `${API}/odds/draftkings/event/${selectedEventId}/props`
    fetchJson(propsUrl, { ttlSeconds: EVENT_PROPS_TTL_SECONDS })
      .then(json => { setPropsData(json); setPropsLoading(false) })
      .catch(err => { setPropsData(null); setPropsError(String(err?.message || err)); setPropsLoading(false) })
  }, [selectedEventId])

  const matchupByKey = useMemo(() => { const map = new Map(); matchups.forEach(m => { const key = keyFromMatchup(m); if (key !== '@') map.set(key, m) }); return map }, [matchups])
  const modelByKey = useMemo(() => { const map = new Map(); modelGamesFromPayload(modelPayload).forEach(game => { const key = matchupKey(game?.away_team || game?.away_team_name || game?.away || '', game?.home_team || game?.home_team_name || game?.home || ''); if (key !== '@') map.set(key, game) }); return map }, [modelPayload])
  const rows = useMemo(() => events.map(event => { const key = keyFromEvent(event); return { event, matchup: matchupByKey.get(key), model: modelByKey.get(key), matched: Boolean(matchupByKey.get(key)), key } }), [events, matchupByKey, modelByKey])
  const matchedCount = rows.filter(r => r.matched).length
  const selectedRow = rows.find(row => String(row.event?.event_id) === String(selectedEventId))

  return <div style={s.page}>
    <section style={s.hero}>
      <div style={s.header}>
        <div><div style={s.eyebrow}>DraftKings board</div><h1 style={s.title}>Daily Odds</h1><div style={s.subtitle}>Odds are organized by game, category, market, and player. The page loads fast first; heavier model recap data is pulled only when requested.</div></div>
        <div style={s.controls}><input type="date" value={date} onChange={e => setDate(e.target.value)} style={s.input} /><button type="button" style={s.button} onClick={() => load(true)} disabled={loading}>{loading ? 'Refreshing...' : 'Refresh Odds'}</button></div>
      </div>
      <div style={s.stats}>
        <div style={s.statCard}><div style={s.statLabel}>MLB Games</div><div style={s.statValue}>{matchups.length}</div></div>
        <div style={s.statCard}><div style={s.statLabel}>DK Events</div><div style={s.statValue}>{events.length}</div></div>
        <div style={s.statCard}><div style={s.statLabel}>Matched</div><div style={s.statValue}>{matchedCount}</div></div>
        <div style={s.statCard}><div style={s.statLabel}>Model Rows</div><div style={s.statValue}>{modelGamesFromPayload(modelPayload).length}</div></div>
        <div style={s.statCard}><div style={s.statLabel}>Unified Recap</div><div style={{ ...s.statValue, fontSize: 15 }}>{unifiedPayload ? 'Loaded' : 'On demand'}</div></div>
        <div style={s.statCard}><div style={s.statLabel}>Last Refreshed</div><div style={{ ...s.statValue, fontSize: 15 }}>{lastRefreshed ? lastRefreshed.toLocaleTimeString() : 'Not loaded'}</div></div>
      </div>
    </section>

    {error && <div style={s.error}>{error}</div>}
    {modelError && <div style={s.error}>Model panel error: {modelError}</div>}
    {loading && <div style={s.loader}>Loading daily odds...</div>}
    {!loading && !error && events.length === 0 && <div style={s.empty}>No DraftKings events returned for {date}. Internal model rows may still exist, but no sportsbook event board can be built without events.</div>}

    {!loading && !error && events.length > 0 && <OddsBoard events={events} selectedEventId={selectedEventId} setSelectedEventId={setSelectedEventId} propsData={propsData} propsLoading={propsLoading} propsError={propsError} />}

    <DailyRecapPanel unifiedPayload={unifiedPayload} loading={unifiedLoading} onLoad={() => loadUnified(false)} />

    <section style={s.section}>
      <div style={s.sectionHeader}><div><div style={s.sectionTitle}>Game Market Snapshot</div><div style={s.modelSubtitle}>Compact game-line view. Props are handled in the organized board above, not scattered cards.</div></div><span style={s.chip}>{rows.length} games</span></div>
      <div style={{ display: 'grid', gap: 12 }}>{rows.map(({ event, matchup, model, matched, key }, idx) => {
        const away = event?.away_team?.name || event?.away_team || matchup?.away_team_name || 'Away'
        const home = event?.home_team?.name || event?.home_team || matchup?.home_team_name || 'Home'
        return <article key={`${event.event_id || key || idx}`} style={{ ...s.card, borderColor: String(event.event_id) === String(selectedEventId) ? '#58a6ff' : '#30363d' }}>
          <div style={s.cardTop}>
            <div><div style={s.matchup}>{away} @ {home}</div><div style={s.metaRow}><span style={s.chip}>{formatTime(matchup?.game_time || event?.start_time || event?.commence_time)}</span><span style={s.chip}>MLB: {matchup?.game_pk ? <Link to={`/matchup/${matchup.game_pk}`} style={{ color: '#58a6ff', textDecoration: 'none' }}>{matchup.game_pk}</Link> : 'Unmatched'}</span><span style={s.chip}>DK: {event.event_id || 'Unavailable'}</span><span style={s.chip}>Model: {model ? 'loaded' : 'pending'}</span></div></div>
            <span style={s.badge(matched)}>{matched ? 'MATCHED' : 'UNMATCHED'}</span>
          </div>
          <div style={s.markets}><MarketBox label="Moneyline" market={findMarket(event, 'h2h')} /><MarketBox label="Run Line" market={findMarket(event, 'spreads')} /><MarketBox label="Total" market={findMarket(event, 'totals')} /></div>
          <div style={{ padding: '0 16px 14px' }}><button type="button" style={s.mutedButton} onClick={() => setSelectedEventId(event.event_id)}>Open in Odds Board</button>{selectedRow?.event?.event_id === event.event_id && <span style={{ ...s.chip, marginLeft: 8 }}>Selected</span>}</div>
        </article>
      })}</div>
    </section>
  </div>
}
