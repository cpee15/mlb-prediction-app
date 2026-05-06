import React, { useEffect, useMemo, useState } from 'react'
import { API_BASE } from '../lib/api'

const API = API_BASE

const s = {
  page: { display: 'grid', gap: 18 },
  hero: { background: '#161b22', border: '1px solid #30363d', borderRadius: 16, padding: 22 },
  header: { display: 'flex', justifyContent: 'space-between', gap: 16, flexWrap: 'wrap' },
  title: { fontSize: 30, fontWeight: 900, color: '#e6edf3', margin: 0 },
  subtitle: { color: '#8b949e', fontSize: 14, marginTop: 8 },
  input: { background: '#0d1117', border: '1px solid #30363d', color: '#e6edf3', borderRadius: 10, padding: '10px 12px' },
  button: { background: '#238636', border: '1px solid #2ea043', color: '#fff', borderRadius: 10, padding: '10px 14px', fontWeight: 900 },
  stats: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(145px, 1fr))', gap: 10, marginTop: 18 },
  statCard: { background: '#0d1117', border: '1px solid #30363d', borderRadius: 12, padding: 14 },
  statLabel: { color: '#8b949e', fontSize: 10, textTransform: 'uppercase', fontWeight: 900 },
  statValue: { color: '#e6edf3', fontSize: 24, fontWeight: 900, marginTop: 5 },
  section: { background: '#161b22', border: '1px solid #30363d', borderRadius: 14, padding: 16 },
  sectionHeader: { display: 'flex', justifyContent: 'space-between', gap: 12, flexWrap: 'wrap', marginBottom: 12 },
  sectionTitle: { color: '#e6edf3', fontSize: 18, fontWeight: 900 },
  small: { color: '#8b949e', fontSize: 12, marginTop: 4 },
  grid: { display: 'grid', gap: 12 },
  chip: { color: '#8b949e', border: '1px solid #30363d', background: '#0d1117', borderRadius: 999, padding: '4px 8px', fontSize: 11, fontWeight: 800 },
  recapCard: { border: '1px solid #30363d', borderRadius: 14, background: '#0d1117', overflow: 'hidden' },
  recapSummary: { cursor: 'pointer', listStyle: 'none', padding: 16, background: '#111820', borderBottom: '1px solid #30363d' },
  matchup: { color: '#e6edf3', fontSize: 18, fontWeight: 900 },
  metaRow: { display: 'flex', gap: 8, flexWrap: 'wrap', marginTop: 8 },
  recapBody: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(225px, 1fr))', gap: 12, padding: 14 },
  recapPanel: { border: '1px solid #30363d', borderRadius: 12, background: '#161b22', padding: 12 },
  recapPanelTitle: { color: '#58a6ff', fontSize: 12, fontWeight: 900, textTransform: 'uppercase', letterSpacing: 0.8, marginBottom: 8 },
  bulletList: { margin: 0, paddingLeft: 18, color: '#c9d1d9', fontSize: 12, lineHeight: 1.55 },
  card: { background: '#161b22', border: '1px solid #30363d', borderRadius: 14, overflow: 'hidden' },
  cardTop: { display: 'flex', justifyContent: 'space-between', gap: 12, padding: 16, borderBottom: '1px solid #30363d', background: '#111820' },
  markets: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: 10, padding: 16 },
  market: { border: '1px solid #30363d', borderRadius: 12, padding: 12, background: '#0d1117' },
  marketTitle: { color: '#8b949e', fontSize: 10, textTransform: 'uppercase', fontWeight: 900, marginBottom: 9 },
  oddsLine: { display: 'flex', justifyContent: 'space-between', gap: 10, color: '#e6edf3', fontSize: 13, marginTop: 6, padding: '5px 0', borderTop: '1px solid rgba(48,54,61,0.55)' },
  price: { fontWeight: 900, color: '#e6edf3' },
  loader: { color: '#8b949e', textAlign: 'center', padding: 40 },
  error: { color: '#f85149', background: '#1f1116', border: '1px solid #3b2222', borderRadius: 12, padding: 14 },
  empty: { color: '#8b949e', textAlign: 'center', padding: 34, border: '1px solid #30363d', borderRadius: 14, background: '#161b22' },
}

const TODAY = () => new Date().toISOString().slice(0, 10)
const arr = v => Array.isArray(v) ? v : []
const norm = v => String(v || '').toLowerCase().replace(/[^a-z0-9]/g, '').replace(/^the/, '')
const key = (a, h) => `${norm(a)}@${norm(h)}`
const matchupKey = m => key(m?.away_team_name || m?.away_team || m?.away_name || m?.away?.name, m?.home_team_name || m?.home_team || m?.home_name || m?.home?.name)
const eventKey = e => key(e?.away_team?.name || e?.away_team, e?.home_team?.name || e?.home_team)
const modelKey = g => key(g?.away_team?.name || g?.away_team_name || g?.away_team || g?.away || g?.teams?.away?.name, g?.home_team?.name || g?.home_team_name || g?.home_team || g?.home || g?.teams?.home?.name)
const good = v => v !== null && v !== undefined && String(v).trim() && !['N/A', 'NA', 'undefined', 'null', 'None', 'nan'].includes(String(v).trim())
const n = v => { const x = Number(v); return Number.isFinite(x) ? x : null }
const pct = v => n(v) === null ? null : `${Math.round((Math.abs(n(v)) <= 1 ? n(v) * 100 : n(v)))}%`
const dec = (v, d = 3) => n(v) === null ? null : n(v).toFixed(d)
const num = (v, d = 1) => n(v) === null ? null : n(v).toFixed(d)
const fmt = (label, value, f = String) => good(value) && good(f(value)) ? `${label} ${f(value)}` : null
const line = parts => parts.filter(good).join(' | ')
const rows = (items, fallback) => items.filter(good).length ? items.filter(good) : [fallback]
const time = iso => { try { return good(iso) ? new Date(iso).toLocaleString('en-US', { month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit', timeZone: 'America/New_York' }) + ' ET' : null } catch { return null } }
const american = v => n(v) === null ? '' : n(v) > 0 ? `+${n(v)}` : `${n(v)}`

function objects(root) {
  const out = [], seen = new WeakSet()
  const walk = v => {
    if (!v || typeof v !== 'object' || seen.has(v)) return
    seen.add(v)
    if (Array.isArray(v)) return v.forEach(walk)
    out.push(v)
    Object.values(v).forEach(walk)
  }
  walk(root)
  return out
}
function deep(root, keys) { const wanted = new Set(keys.map(k => String(k).toLowerCase())); for (const obj of objects(root)) for (const [k, v] of Object.entries(obj)) if (wanted.has(k.toLowerCase()) && good(v)) return v; return null }
function first(sources, keys) { for (const src of sources.filter(Boolean)) { const v = deep(src, keys); if (good(v)) return v } return null }

const playerName = o => o?.player_name || o?.batter_name || o?.hitter_name || o?.name || o?.player?.name || o?.batter?.name || o?.hitter?.name
const hitKeys = { xwoba: ['xwoba', 'expected_woba', 'batter_xwoba'], arsenal: ['arsenal_edge', 'arsenal_score', 'pitcher_vs_arsenal_score', 'pitch_type_matchup_score', 'pitch_matchup_score'], hardHit: ['hard_hit_pct', 'hardhit_pct', 'hard_hit_rate'], barrel: ['barrel_pct', 'barrel_rate'], contact: ['contact_edge', 'contact_score', 'contact_rate'], platoon: ['platoon_advantage', 'platoon_edge'], lineup: ['lineup_position', 'batting_order', 'order'] }
function hitCandidates(row, topProps) {
  const pools = [row.projection, row.model, row.matchup, row.event, ...arr(topProps).filter(c => c?.match_key === row.key || c?.game_pk === row.gamePk)]
  const map = new Map()
  pools.flatMap(objects).forEach(o => {
    const market = `${o.market || ''} ${o.market_name || ''} ${o.model_name || ''} ${o.prop_type || ''}`.toLowerCase()
    const hasHitSignal = market.includes('hit') || Object.values(hitKeys).flat().some(k => Object.prototype.hasOwnProperty.call(o, k))
    if (!good(playerName(o)) || !hasHitSignal) return
    const metrics = { xwoba: deep(o, hitKeys.xwoba), arsenal: deep(o, hitKeys.arsenal), hardHit: deep(o, hitKeys.hardHit), barrel: deep(o, hitKeys.barrel), contact: deep(o, hitKeys.contact), platoon: deep(o, hitKeys.platoon), lineup: deep(o, hitKeys.lineup) }
    if (!Object.values(metrics).some(good)) return
    const score = (n(metrics.xwoba) || 0) * 1000 + (n(metrics.arsenal) || 0) + (n(metrics.hardHit) || 0) * 20 + (n(metrics.barrel) || 0) * 35 + (n(metrics.contact) || 0) + (n(metrics.platoon) || 0) * 10 - ((n(metrics.lineup) || 9) * .5)
    const candidate = { name: String(playerName(o)), metrics, score }
    if (!map.get(candidate.name) || score > map.get(candidate.name).score) map.set(candidate.name, candidate)
  })
  return [...map.values()].sort((a, b) => b.score - a.score).slice(0, 3)
}
function hitLine(c) { return line([`${c.name}:`, fmt('xwOBA', c.metrics.xwoba, v => dec(v, 3)), fmt('Arsenal Edge', c.metrics.arsenal, v => num(v, 0)), fmt('Hard Hit', c.metrics.hardHit, pct), fmt('Barrel', c.metrics.barrel, pct), fmt('Contact Edge', c.metrics.contact, v => num(v, 0)), fmt('Platoon', c.metrics.platoon, v => num(v, 2)), fmt('Lineup', c.metrics.lineup, v => num(v, 0))]).replace(': |', ':') }
function sim(model) { return model?.workspace?.bullpenAdjustedGameSimulation || model?.sharedSimulation || model?.bullpenAdjustedGameSimulation || {} }
function bullpenLine(team, model, matchup, side) {
  const sources = [model?.workspace?.[`${side}BullpenProfile`], model?.workspace?.[`${side}_bullpen_profile`], model?.teams?.[side]?.bullpen, model?.teams?.[side]?.bullpen_profile, model?.sharedSimulation?.direct_inputs?.[`${side}_bullpen_profile`], model?.sharedSimulation?.direct_inputs?.[`${side}_bullpen`], model?.[`${side}_bullpen`], matchup?.[`${side}_bullpen`], sim(model)]
  const text = line([fmt('Adj Win', first(sources, [`${side}_win_probability`, `${side}_bullpen_adjusted_win_probability`, 'win_probability']), pct), fmt('Late Runs', first(sources, [`${side}_late_runs_allowed`, `${side}_bullpen_runs_allowed`, 'late_runs_allowed', 'projected_runs_allowed']), v => num(v, 1)), fmt('ERA', first(sources, ['bullpen_era', 'era', 'relief_era']), v => num(v, 2)), fmt('WHIP', first(sources, ['bullpen_whip', 'whip', 'relief_whip']), v => num(v, 2)), fmt('K%', first(sources, ['bullpen_k_pct', 'k_pct', 'strikeout_pct']), pct), fmt('BB%', first(sources, ['bullpen_bb_pct', 'bb_pct', 'walk_pct']), pct), fmt('HR/9', first(sources, ['hr_per_9', 'hr9', 'bullpen_hr9']), v => num(v, 2)), fmt('Fatigue', first(sources, ['fatigue_score', 'workload_score', 'recent_workload', 'bullpen_fatigue']), v => num(v, 1)), fmt('Collapse', first(sources, ['collapse_risk', 'bullpen_collapse_risk', 'leverage_risk']), pct)])
  return text ? `${team}: ${text}` : null
}
function Panel({ title, bullets }) { const clean = bullets.filter(good); return <div style={s.recapPanel}><div style={s.recapPanelTitle}>{title}</div><ul style={s.bulletList}>{clean.map((b, i) => <li key={i}>{b}</li>)}</ul></div> }

function RecapCard({ row, topProps, isLock }) {
  const model = row.projection || row.model || {}, m = row.matchup || {}, e = row.event || {}
  const away = e?.away_team?.name || e?.away_team || m.away_team_name || model?.away_team?.name || model.away_team || 'Away'
  const home = e?.home_team?.name || e?.home_team || m.home_team_name || model?.home_team?.name || model.home_team || 'Home'
  const awayP = m.away_pitcher_name || model?.away_pitcher?.name || model?.teams?.away?.pitcher_name || 'Away Pitcher'
  const homeP = m.home_pitcher_name || model?.home_pitcher?.name || model?.teams?.home?.pitcher_name || 'Home Pitcher'
  const env = model?.workspace?.environmentProfile || m.environment || m.weather || {}
  const awayWin = first([sim(model), m, model?.models?.moneyline], ['away_win_probability', 'away_win_prob', 'model_probability'])
  const homeWin = first([sim(model), m], ['home_win_probability', 'home_win_prob'])
  const winner = n(awayWin) > n(homeWin) ? `${away} ${pct(awayWin)}` : n(homeWin) > n(awayWin) ? `${home} ${pct(homeWin)}` : null
  const strong = [model?.models?.moneyline, model?.models?.spread, model?.models?.run_line, model?.models?.total].filter(Boolean).sort((a, b) => Math.abs(n(b.edge) || 0) - Math.abs(n(a.edge) || 0))[0]
  const pitcherBullets = rows([`${awayP}: ${line([fmt('K%', first([m.away_pitcher_features], ['k_pct']), pct), fmt('BB%', first([m.away_pitcher_features], ['bb_pct']), pct), fmt('xwOBA', first([m.away_pitcher_features], ['xwoba']), v => dec(v, 3)), fmt('Hard Hit', first([m.away_pitcher_features], ['hard_hit_pct']), pct), fmt('Velo', first([m.away_pitcher_features], ['avg_velocity']), num)])}`, `${homeP}: ${line([fmt('K%', first([m.home_pitcher_features], ['k_pct']), pct), fmt('BB%', first([m.home_pitcher_features], ['bb_pct']), pct), fmt('xwOBA', first([m.home_pitcher_features], ['xwoba']), v => dec(v, 3)), fmt('Hard Hit', first([m.home_pitcher_features], ['hard_hit_pct']), pct), fmt('Velo', first([m.home_pitcher_features], ['avg_velocity']), num)])}`].filter(x => !x.endsWith(': ')), 'Pitcher model data unavailable')
  const hits = hitCandidates(row, topProps).map(hitLine)
  const bullpen = rows([bullpenLine(away, model, m, 'away'), bullpenLine(home, model, m, 'home')], 'Bullpen model data unavailable')
  const envRows = rows([line([fmt('Temp', first([env, m.weather], ['temperature', 'temp_f', 'temp']), v => `${num(v, 0)}°F`), fmt('Wind', first([env, m.weather], ['wind_speed', 'wind_mph']), v => `${num(v, 0)} mph`), first([env, m.weather], ['wind_direction', 'wind_label'])]), fmt('Humidity', first([env, m.weather], ['humidity', 'humidity_pct']), pct), fmt('Park Factor', first([env, m], ['park_factor', 'run_factor', 'park_run_factor']), v => num(v, 2)), fmt('Weather Risk', first([env], ['weather_risk', 'weather_risk_score', 'risk_score']), pct)], 'Environment model data unavailable')
  const consensus = rows([strong?.pick ? `Best projected edge: ${strong.pick}` : null, winner ? `Win probability leader: ${winner}` : null, model?.models?.total?.pick ? `Run projection lean: ${model.models.total.pick}` : null, fmt('Confidence', strong?.confidence || strong?.model_probability, pct), isLock ? 'LOCK OF THE DAY: model-ranked top edge' : null], 'Consensus model data unavailable')
  return <details style={s.recapCard} open={isLock}><summary style={s.recapSummary}><div style={s.matchup}>{away} @ {home}</div><div style={s.metaRow}>{time(m.game_time || m.game_datetime || e.commence_time) && <span style={s.chip}>Time: {time(m.game_time || m.game_datetime || e.commence_time)}</span>}{good(m.game_pk || row.gamePk) && <span style={s.chip}>Game PK: {m.game_pk || row.gamePk}</span>}</div></summary><div style={s.recapBody}><Panel title="PITCHERS" bullets={pitcherBullets}/><Panel title="HIT CANDIDATES" bullets={hits.length ? hits : ['Player hit candidates unavailable from current payload']}/><Panel title="BULLPEN" bullets={bullpen}/><Panel title="ENVIRONMENT" bullets={envRows}/><Panel title="CONSENSUS" bullets={consensus}/></div></details>
}

function Market({ label, market }) { const sels = arr(market?.selections).filter(x => good(x?.price) && good(x?.name || x?.description)); return <div style={s.market}><div style={s.marketTitle}>{label}</div>{sels.length ? sels.slice(0, 3).map((x, i) => <div style={s.oddsLine} key={i}><span>{x.name || x.description}{x.line != null ? ` ${x.line}` : ''}</span><b style={s.price}>{american(x.price)}</b></div>) : <div style={s.oddsLine}>Market unavailable</div>}</div> }
const market = (e, keys) => arr(e?.markets).find(m => keys.includes(m.market_key) || keys.includes(m.market_type) || keys.includes(m.market_name))
function OddsCard({ row }) { const e = row.event || {}, m = row.matchup || {}, model = row.model || {}; const away = e?.away_team?.name || e?.away_team || m.away_team_name || model.away_team || 'Away'; const home = e?.home_team?.name || e?.home_team || m.home_team_name || model.home_team || 'Home'; return <article style={s.card}><div style={s.cardTop}><div><div style={s.matchup}>{away} @ {home}</div><div style={s.metaRow}>{time(m.game_time || e.commence_time) && <span style={s.chip}>Time: {time(m.game_time || e.commence_time)}</span>}</div></div><span style={s.chip}>{row.event ? 'Odds matched' : 'Odds pending'}</span></div><div style={s.markets}><Market label="Moneyline" market={market(e, ['h2h'])}/><Market label="Run Line" market={market(e, ['spreads'])}/><Market label="Total" market={market(e, ['totals'])}/></div></article> }

export default function DailyOddsPage() {
  const [date, setDate] = useState(TODAY())
  const [data, setData] = useState({ matchups: [], events: [], models: [], projections: [], topProps: [] })
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  function load() {
    setLoading(true); setError(null)
    Promise.all([fetch(`${API}/matchups?date=${date}`).then(r => r.ok ? r.json() : Promise.reject(new Error(`matchups ${r.status}`))), fetch(`${API}/odds/draftkings/events?date=${date}`).then(r => r.ok ? r.json() : Promise.reject(new Error(`odds ${r.status}`))), fetch(`${API}/daily-odds/models?date=${date}`).then(r => r.ok ? r.json() : Promise.reject(new Error(`daily models ${r.status}`))), fetch(`${API}/models/projections?date=${date}`).then(r => r.ok ? r.json() : Promise.reject(new Error(`model projections ${r.status}`)))])
      .then(([mp, op, dp, pp]) => { setData({ matchups: arr(mp?.games || mp?.matchups || mp), events: arr(op?.events || op?.games || op), models: arr(dp?.games || dp?.models || dp?.game_models || dp), projections: arr(pp?.games || pp?.models || pp?.game_models || pp), topProps: arr(dp?.top_prop_model_candidates || dp?.top_props || dp?.prop_candidates || pp?.top_prop_model_candidates) }); setLoading(false) })
      .catch(e => { setError(String(e?.message || e)); setLoading(false) })
  }
  useEffect(() => { load() }, [date])
  const merged = useMemo(() => { const mm = new Map(data.matchups.map(x => [matchupKey(x), x])); const em = new Map(data.events.map(x => [eventKey(x), x])); const dm = new Map(data.models.map(x => [modelKey(x), x])); const pm = new Map(data.projections.map(x => [modelKey(x), x])); return [...new Set([...mm.keys(), ...em.keys(), ...dm.keys(), ...pm.keys()].filter(good))].map(k => ({ key: k, matchup: mm.get(k), event: em.get(k), model: dm.get(k), projection: pm.get(k), gamePk: mm.get(k)?.game_pk || dm.get(k)?.game_pk || pm.get(k)?.game_pk })).filter(r => r.matchup || r.event || r.model || r.projection) }, [data])
  const lockKey = merged.map(r => ({ key: r.key, c: [r.projection?.models?.moneyline, r.projection?.models?.spread, r.projection?.models?.total, r.model?.models?.moneyline, r.model?.models?.spread, r.model?.models?.total].filter(Boolean).sort((a,b)=>Math.abs(n(b.edge)||0)-Math.abs(n(a.edge)||0))[0] })).filter(x => x.c).sort((a,b)=>(Math.abs(n(b.c.edge)||0)+n(b.c.confidence)||0)-(Math.abs(n(a.c.edge)||0)+n(a.c.confidence)||0))[0]?.key
  return <div style={s.page}><section style={s.hero}><div style={s.header}><div><h1 style={s.title}>MLB Daily Odds + Model Recap</h1><p style={s.subtitle}>Daily Odds merged with Model Projections for real bullpen context and top player hit candidates.</p></div><div><input style={s.input} type="date" value={date} onChange={e=>setDate(e.target.value)}/> <button style={s.button} onClick={load}>Refresh</button></div></div><div style={s.stats}><div style={s.statCard}><div style={s.statLabel}>Games</div><div style={s.statValue}>{merged.length}</div></div><div style={s.statCard}><div style={s.statLabel}>Odds Matched</div><div style={s.statValue}>{merged.filter(r=>r.event).length}</div></div><div style={s.statCard}><div style={s.statLabel}>Projection Games</div><div style={s.statValue}>{merged.filter(r=>r.projection).length}</div></div></div></section>{error && <div style={s.error}>Daily Odds load error: {error}</div>}{loading && <div style={s.loader}>Loading Daily Odds, matchup analyzer, and Model Projections...</div>}{!loading && !error && !merged.length && <div style={s.empty}>No games returned for this date.</div>}{!loading && !error && !!merged.length && <section style={s.section}><div style={s.sectionHeader}><div><div style={s.sectionTitle}>Daily Recap</div><div style={s.small}>Five-panel recap. Missing values are filtered in React before rendering.</div></div><span style={s.chip}>{merged.length} games</span></div><div style={s.grid}>{merged.map((r,i)=><RecapCard key={r.key} row={r} topProps={data.topProps} isLock={r.key===lockKey}/>)}</div></section>}{!loading && !error && !!merged.length && <section style={s.section}><div style={s.sectionHeader}><div><div style={s.sectionTitle}>Game Lines</div><div style={s.small}>DraftKings markets matched back to each MLB game.</div></div></div><div style={s.grid}>{merged.map((r,i)=><OddsCard key={`${r.key}-${i}`} row={r}/>)}</div></section>}</div>
}
