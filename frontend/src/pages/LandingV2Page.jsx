import React, { useEffect, useMemo, useState } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import { API_BASE, fetchJson, getMlbLiveDate, readCachedJson } from '../lib/api'

const COMPONENTS = [
  { key: 'hitters', title: 'My Top Hitters Today', shortTitle: 'Hitters', description: 'Stored 365 hitter board with pitch-type matchup, EV, LA, and arsenal context.' },
  { key: 'pitchers', title: 'My Top Pitchers Today', shortTitle: 'Pitchers', description: 'Pitcher lean board using K profile, contact suppression, and opponent context.' },
  { key: 'teams', title: 'My Top Teams Today', shortTitle: 'Teams', description: 'Team board from model side edge, projected runs, and offense profile.' },
  { key: 'totals', title: 'My Top Totals Today', shortTitle: 'Totals', description: 'Game total board from projected runs, run environment, and simulation context.' },
  { key: 'overall_players', title: 'My Top Overall Players Today', shortTitle: 'Overall', description: 'Combined player board blending hitter and pitcher solver outputs.' },
]

const ACTIVE_LINEUP_COMPONENTS = new Set(['hitters', 'overall_players'])
const BOARD_TTL_SECONDS = 300
const MATCHUPS_TTL_SECONDS = 120

const C = {
  bg: '#070b14',
  border: '#21304a',
  text: '#eef5ff',
  muted: '#91a1bb',
  green: '#42f58d',
  blue: '#56b7ff',
  yellow: '#ffd166',
  red: '#ff6b7a',
}

function boardPayload(date, activeLineups = false) {
  const components = activeLineups ? COMPONENTS.filter(component => ACTIVE_LINEUP_COMPONENTS.has(component.key)).map(component => component.key) : COMPONENTS.map(component => component.key)
  return {
    date,
    components,
    filters_by_component: Object.fromEntries(components.map(component => [component, {}])),
    active_lineups: activeLineups,
  }
}

function boardUrl(date, activeLineups = false) {
  return `${API_BASE}/my-dashboard/solver/batch::__${activeLineups ? 'confirmed' : 'pre'}__${date}`
}

async function fetchBoard(date, activeLineups = false) {
  const payload = boardPayload(date, activeLineups)
  const response = await fetch(`${API_BASE}/my-dashboard/solver/batch`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  })
  const json = await response.json().catch(() => ({}))
  if (!response.ok) throw new Error(typeof json?.detail === 'string' ? json.detail : JSON.stringify(json.detail || json))
  return json
}

function fmt(value) {
  const num = Number(value)
  if (!Number.isFinite(num)) return value == null || value === '' ? '—' : String(value)
  return Math.abs(num) >= 10 ? num.toFixed(1) : num.toFixed(3)
}

function pct(value) {
  const num = Number(value)
  if (!Number.isFinite(num)) return '—'
  return `${Math.round(num * 100)}%`
}

function title(item) {
  return item?.entity_name || item?.player || item?.game || item?.teams || 'Dashboard result'
}

function subtitle(item, component) {
  return item?.primary_reason || item?.market || item?.pick || item?.entity_type || component?.description || ''
}

function metrics(item) {
  return Object.entries(item?.metrics || {}).filter(([, value]) => value !== null && value !== undefined && value !== '')
}

function dateLabel(date) {
  try {
    return new Date(`${date}T12:00:00`).toLocaleDateString('en-US', { month: 'short', day: 'numeric' })
  } catch {
    return date
  }
}

function gameLabel(game) {
  const away = game?.away_team_name || game?.away_team || game?.away_name
  const home = game?.home_team_name || game?.home_team || game?.home_name
  if (away && home) return `${away} @ ${home}`
  return 'MLB matchup'
}

function timeLabel(iso) {
  if (!iso) return null
  try {
    return new Date(iso).toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit', timeZone: 'America/New_York' }) + ' ET'
  } catch {
    return null
  }
}

function weatherLabel(weather) {
  if (!weather) return null
  const parts = []
  if (weather.temp_f != null) parts.push(`${weather.temp_f}°F`)
  if (weather.condition) parts.push(weather.condition)
  if (weather.wind) parts.push(weather.wind)
  return parts.length ? parts.join(' · ') : null
}

function Pill({ children, tone = '' }) {
  if (children === null || children === undefined || children === '') return null
  return <span className={`status-badge ${tone}`}>{children}</span>
}

function Metric({ label, value }) {
  if (value === null || value === undefined || value === '') return null
  return (
    <div style={s.metric}>
      <span style={s.metricLabel}>{label}</span>
      <strong style={s.metricValue}>{value}</strong>
    </div>
  )
}

function useLandingData(date) {
  const preCacheKey = boardUrl(date, false)
  const confirmedCacheKey = boardUrl(date, true)
  const matchupsUrl = `${API_BASE}/matchups?date=${date}`

  const [preBoard, setPreBoard] = useState(() => readCachedJson(preCacheKey, BOARD_TTL_SECONDS))
  const [confirmedBoard, setConfirmedBoard] = useState(() => readCachedJson(confirmedCacheKey, BOARD_TTL_SECONDS))
  const [matchups, setMatchups] = useState(() => {
    const cached = readCachedJson(matchupsUrl, MATCHUPS_TTL_SECONDS)
    return Array.isArray(cached) ? cached : []
  })
  const [preLoading, setPreLoading] = useState(!preBoard)
  const [confirmedLoading, setConfirmedLoading] = useState(false)
  const [matchupsLoading, setMatchupsLoading] = useState(matchups.length === 0)
  const [errors, setErrors] = useState({})

  useEffect(() => {
    let cancelled = false
    const startedAt = Date.now()

    async function run() {
      setErrors({})
      setPreLoading(!readCachedJson(preCacheKey, BOARD_TTL_SECONDS))
      setMatchupsLoading(!readCachedJson(matchupsUrl, MATCHUPS_TTL_SECONDS))

      fetchJson(matchupsUrl, { ttlSeconds: MATCHUPS_TTL_SECONDS })
        .then(data => { if (!cancelled) setMatchups(Array.isArray(data) ? data : []) })
        .catch(err => { if (!cancelled) setErrors(prev => ({ ...prev, matchups: err.message || String(err) })) })
        .finally(() => { if (!cancelled) setMatchupsLoading(false) })

      try {
        const pre = await fetchBoard(date, false)
        if (cancelled) return
        sessionStorage.setItem(`mlb-json-cache:v1:${preCacheKey}`, JSON.stringify({ createdAt: Date.now(), value: pre }))
        setPreBoard({ ...pre, loaded_ms: Date.now() - startedAt })
      } catch (err) {
        if (!cancelled) setErrors(prev => ({ ...prev, pre: err.message || String(err) }))
      } finally {
        if (!cancelled) setPreLoading(false)
      }

      setConfirmedLoading(true)
      try {
        const confirmed = await fetchBoard(date, true)
        if (cancelled) return
        sessionStorage.setItem(`mlb-json-cache:v1:${confirmedCacheKey}`, JSON.stringify({ createdAt: Date.now(), value: confirmed }))
        setConfirmedBoard({ ...confirmed, loaded_ms: Date.now() - startedAt })
      } catch (err) {
        if (!cancelled) setErrors(prev => ({ ...prev, confirmed: err.message || String(err) }))
      } finally {
        if (!cancelled) setConfirmedLoading(false)
      }
    }

    run()
    return () => { cancelled = true }
  }, [date, preCacheKey, confirmedCacheKey, matchupsUrl])

  const activeBoard = confirmedBoard?.results ? { ...preBoard, results: { ...(preBoard?.results || {}), ...(confirmedBoard.results || {}) }, active_lineups: true, loaded_ms: confirmedBoard.loaded_ms } : preBoard
  return { activeBoard, preBoard, confirmedBoard, matchups, preLoading, confirmedLoading, matchupsLoading, errors }
}

function buildSections(results) {
  return COMPONENTS.map(component => ({
    ...component,
    items: Array.isArray(results?.[component.key]?.items) ? results[component.key].items.slice(0, 5) : [],
    result: results?.[component.key] || null,
  })).filter(section => section.items.length > 0)
}

function flattenTopCards(sections) {
  return sections.flatMap(section => section.items.slice(0, 2).map(item => ({ component: section, item })))
    .sort((a, b) => Number(b.item?.score || -999) - Number(a.item?.score || -999))
    .slice(0, 8)
}

function DashboardCard({ item, component, active, onClick }) {
  const confidence = String(item?.confidence || '').toLowerCase()
  const tone = confidence.includes('high') ? 'success' : confidence.includes('medium') ? 'warning' : confidence.includes('low') ? 'danger' : ''
  const itemMetrics = metrics(item).slice(0, 3)
  return (
    <button type="button" onClick={onClick} style={active ? s.previewCardActive : s.previewCard}>
      <div style={s.cardTop}>
        <div>
          <div style={s.cardTitle}>{title(item)}</div>
          <div style={s.cardMeta}>{subtitle(item, component)}</div>
        </div>
        <div style={s.scoreBox}>
          <span style={s.scoreLabel}>Score</span>
          <strong style={s.score}>{fmt(item?.score)}</strong>
        </div>
      </div>
      <div style={s.pills}>
        <Pill>{item?.entity_type}</Pill>
        <Pill tone="success">{item?.team}</Pill>
        {item?.opponent ? <Pill tone="warning">vs {item.opponent}</Pill> : null}
        <Pill tone={tone}>{item?.confidence}</Pill>
      </div>
      {itemMetrics.length ? <div style={s.miniMetrics}>{itemMetrics.map(([key, value]) => <span key={key}>{key}: <strong>{fmt(value)}</strong></span>)}</div> : null}
    </button>
  )
}

function ModelRunPanel({ preLoading, confirmedLoading, matchupsLoading, activeBoard, preBoard, confirmedBoard, errors }) {
  const phase = activeBoard?.active_lineups ? 'Confirmed-lineup board live' : preBoard?.results ? 'Pre-lineup board live' : 'Preparing board'
  return (
    <section style={s.modelPanel}>
      <div>
        <p style={s.kicker}>Model Run Status</p>
        <h2 style={s.h2}>{phase}</h2>
        <p style={s.sub}>The page loads cached pre-lineup rankings first, then refreshes confirmed-lineup hitter and overall boards in the background.</p>
      </div>
      <div style={s.runGrid}>
        <RunStep label="Slate + weather" active={matchupsLoading} done={!matchupsLoading && !errors.matchups} error={errors.matchups} />
        <RunStep label="Pre-lineup board" active={preLoading} done={!!preBoard?.results && !preLoading} error={errors.pre} detail={preBoard?.loaded_ms ? `${preBoard.loaded_ms}ms` : null} />
        <RunStep label="Confirmed lineup refresh" active={confirmedLoading} done={!!confirmedBoard?.results && !confirmedLoading} error={errors.confirmed} detail={confirmedBoard?.loaded_ms ? `${confirmedBoard.loaded_ms}ms` : null} />
      </div>
    </section>
  )
}

function RunStep({ label, active, done, error, detail }) {
  const symbol = error ? '!' : active ? '↻' : done ? '✓' : '•'
  const tone = error ? C.red : active ? C.yellow : done ? C.green : C.muted
  return (
    <div style={s.runStep}>
      <span style={{ ...s.runDot, color: tone }}>{symbol}</span>
      <div>
        <strong>{label}</strong>
        <div style={s.cardMeta}>{error ? String(error).slice(0, 90) : active ? 'Running model…' : done ? (detail || 'Ready') : 'Waiting'}</div>
      </div>
    </div>
  )
}

function MatchupStrip({ matchups }) {
  if (!matchups.length) return null
  return (
    <section style={s.section}>
      <div style={s.sectionHead}>
        <div>
          <h2 style={s.h2}>Today’s slate context</h2>
          <p style={s.sub}>Matchups, starters, model win probability, weather, and game state from the production slate.</p>
        </div>
        <Link to="/" style={s.secondary}>View full slate</Link>
      </div>
      <div style={s.matchupGrid}>{matchups.slice(0, 6).map((game, index) => (
        <Link key={game.game_pk || index} to={game.game_pk ? `/matchup/${game.game_pk}` : '/'} style={s.matchupCard}>
          <div style={s.cardTitle}>{gameLabel(game)}</div>
          <div style={s.cardMeta}>{timeLabel(game.game_time) || game.status || 'Time pending'}{game.venue ? ` · ${game.venue}` : ''}</div>
          <div style={s.pitcherLine}>{game.away_pitcher_name || 'Away starter pending'} vs {game.home_pitcher_name || 'Home starter pending'}</div>
          <div style={s.probRow}><span>{pct(game.away_win_prob)}</span><span>{pct(game.home_win_prob)}</span></div>
          {weatherLabel(game.weather) ? <div style={s.cardMeta}>{weatherLabel(game.weather)}</div> : null}
        </Link>
      ))}</div>
    </section>
  )
}

function SelectedBreakdown({ selected, onUnlock }) {
  if (!selected) {
    return (
      <section style={s.section}>
        <div style={s.panelLarge}>
          <p style={s.kicker}>Selected Play Breakdown</p>
          <h2 style={s.h2}>Why this grades well</h2>
          <p style={s.sub}>Select a preview card to inspect its real dashboard fields.</p>
        </div>
      </section>
    )
  }

  const { component, item } = selected
  const itemMetrics = metrics(item)
  const reasoning = Array.isArray(item?.reasoning) ? item.reasoning.filter(Boolean) : []

  return (
    <section style={s.section}>
      <div style={s.panelLarge}>
        <div style={s.sectionHead}>
          <div>
            <p style={s.kicker}>{component.title}</p>
            <h2 style={s.h2}>Why this grades well</h2>
            <p style={s.sub}>{title(item)} — {subtitle(item, component)}</p>
          </div>
          <button type="button" onClick={onUnlock} style={s.primary}>Unlock Today’s Card</button>
        </div>
        <div style={s.metricGrid}>
          <Metric label="Score" value={fmt(item?.score)} />
          <Metric label="Confidence" value={item?.confidence || '—'} />
          <Metric label="Team" value={item?.team} />
          <Metric label="Opponent" value={item?.opponent ? `vs ${item.opponent}` : null} />
        </div>
        {reasoning.length ? <div style={s.reasonBox}><h3 style={s.h3}>Reason / explanation</h3><ul style={s.reasons}>{reasoning.slice(0, 5).map((reason, index) => <li key={`${title(item)}-${index}`}>{reason}</li>)}</ul></div> : null}
        {itemMetrics.length ? <div style={s.reasonBox}><h3 style={s.h3}>Supporting metrics</h3><div style={s.metricGrid}>{itemMetrics.slice(0, 12).map(([key, value]) => <Metric key={key} label={key} value={fmt(value)} />)}</div></div> : null}
      </div>
    </section>
  )
}

export default function LandingV2Page() {
  const navigate = useNavigate()
  const date = getMlbLiveDate()
  const { activeBoard, preBoard, confirmedBoard, matchups, preLoading, confirmedLoading, matchupsLoading, errors } = useLandingData(date)
  const sections = useMemo(() => buildSections(activeBoard?.results || {}), [activeBoard])
  const topCards = useMemo(() => flattenTopCards(sections), [sections])
  const [selected, setSelected] = useState(null)

  useEffect(() => {
    if (!selected && topCards[0]) setSelected(topCards[0])
  }, [topCards, selected])

  function unlock() {
    navigate('/my-dashboard')
  }

  return (
    <div style={s.page}>
      <section style={s.hero}>
        <div>
          <div style={s.eyebrow}>● Live MLB Prediction Engine</div>
          <h1 style={s.h1}>Today’s MLB card, ranked by edge.</h1>
          <p style={s.heroText}>MLBGPT scans every matchup using pitcher trends, bullpen form, team offense, weather, odds, and AI model projections to surface the strongest betting angles before first pitch.</p>
          <div style={s.buttons}><button type="button" onClick={unlock} style={s.primary}>Unlock Today’s Card</button><Link to="/" style={s.secondary}>View Matchups</Link></div>
        </div>
        <div style={s.terminal}>
          <div style={s.terminalHead}><strong>{activeBoard?.active_lineups ? 'Confirmed-Lineup Board' : 'Pre-Lineup Board'}</strong><span style={s.green}>{confirmedLoading ? 'Model Running' : activeBoard?.results ? 'Live Data' : 'Loading'}</span><span style={s.muted}>{dateLabel(date)}</span></div>
          <div style={s.boardRows}>{topCards.length ? topCards.slice(0, 5).map(({ component, item }, index) => <div key={`${component.key}-${item?.entity_id || index}`} style={s.heroRow}><span style={s.rowTitle}>{title(item)}</span><span>{component.shortTitle}</span><strong style={s.green}>{fmt(item?.score)}</strong><strong style={s.blue}>{item?.confidence || '—'}</strong></div>) : <div style={s.emptyPreview}>{preLoading ? 'Running pre-lineup model…' : 'No board cards returned yet.'}</div>}</div>
          {selected ? <div style={s.heroMetrics}><Metric label="Selected" value={title(selected.item)} /><Metric label="Score" value={fmt(selected.item?.score)} /><Metric label="Board" value={selected.component.shortTitle} /></div> : null}
        </div>
      </section>

      <ModelRunPanel preLoading={preLoading} confirmedLoading={confirmedLoading} matchupsLoading={matchupsLoading} activeBoard={activeBoard} preBoard={preBoard} confirmedBoard={confirmedBoard} errors={errors} />
      <MatchupStrip matchups={matchups} />

      <section style={s.section}>
        <div style={s.sectionHead}><div><h2 style={s.h2}>Today’s Best Scores</h2><p style={s.sub}>Real board outputs from the same solver that powers My Dashboard. Pre-lineup cards appear first; confirmed-lineup cards replace them when available.</p></div><button type="button" onClick={unlock} style={s.secondary}>Unlock Full Card</button></div>
        {sections.length ? <div style={s.previewGrid}>{sections.map(section => <div key={section.key} style={s.panel}><h3 style={s.h3}>{section.title}</h3><p style={s.cardMeta}>{section.description}</p><div style={s.stack}>{section.items.map((item, index) => <DashboardCard key={`${section.key}-${item?.entity_id || index}`} item={item} component={section} active={selected?.item === item} onClick={() => setSelected({ component: section, item })} />)}</div></div>)}</div> : <div className="state-panel" style={{ textAlign: 'left' }}><strong>{preLoading ? 'Model is running.' : 'No fabricated picks are displayed.'}</strong><p style={{ margin: '8px 0 0' }}>{preLoading ? 'The pre-lineup board is being prepared from the existing solver.' : 'The solver did not return public preview cards yet.'}</p></div>}
      </section>

      <SelectedBreakdown selected={selected} onUnlock={unlock} />

      <section style={s.section}>
        <h2 style={s.h2}>Built like a sportsbook dashboard. Explained like an analyst.</h2>
        <p style={s.sub}>The preview now uses board rankings, matchup context, starters, probabilities, weather, metrics, and reasoning without adding client-side scoring logic.</p>
        <div style={s.proofGrid}><div style={s.panel}><h3 style={s.h3}>Board sources</h3>{COMPONENTS.map(component => <div key={component.key} style={component.key === (selected?.component?.key || 'hitters') ? s.navActive : s.navItem}>{component.shortTitle}</div>)}</div><div style={s.panel}><h3 style={s.h3}>Fast loading strategy</h3><p style={s.sub}>Client session cache plus server solver cache. Pre-lineup board first, confirmed-lineup refresh in background.</p></div><div style={s.panel}><h3 style={s.h3}>No new model logic</h3><p style={s.sub}>This route consumes existing solver and matchup endpoints and only formats returned fields.</p></div></div>
      </section>

      <section style={s.section}><div style={s.assistant}><div><h2 style={s.h2}>Ask MLBGPT before you bet.</h2><p style={s.sub}>The existing AI Data Assistant is available for slate, matchup, model-edge, and data-quality questions.</p></div><div style={s.prompt}><strong>Prompt:</strong> What is the strongest model edge?<div style={s.answer}>Open the AI Data Assistant to ask this against live app-owned data.</div><Link to="/ai-data-assistant" style={{ ...s.secondary, display: 'inline-flex', marginTop: 14 }}>Open AI Data Assistant</Link></div></div></section>

      <section style={s.final}><h2 style={s.finalTitle}>Stop building your MLB card from scratch.</h2><p style={s.finalText}>Let MLBGPT rank the board, explain the edge, and show the risk before first pitch.</p><button type="button" onClick={unlock} style={s.primary}>Unlock Today’s Card</button></section>
    </div>
  )
}

const s = {
  page: { margin: '-34px calc(50% - 50vw) -56px', padding: '76px 28px 0', minHeight: '100vh', color: C.text, background: `radial-gradient(circle at top left, rgba(66,245,141,.12), transparent 32%), radial-gradient(circle at 75% 10%, rgba(86,183,255,.12), transparent 28%), ${C.bg}` },
  hero: { maxWidth: 1220, margin: '0 auto', paddingBottom: 36, display: 'grid', gridTemplateColumns: 'repeat(auto-fit,minmax(320px,1fr))', gap: 40, alignItems: 'center' },
  eyebrow: { display: 'inline-flex', padding: '8px 12px', border: `1px solid ${C.border}`, borderRadius: 999, background: 'rgba(13,20,36,.72)', color: C.green, fontSize: 13, fontWeight: 800, marginBottom: 22 },
  h1: { fontSize: 'clamp(48px,7vw,78px)', lineHeight: .94, letterSpacing: '-0.075em', margin: '0 0 24px' },
  h2: { fontSize: 'clamp(32px,5vw,42px)', lineHeight: 1, letterSpacing: '-0.05em', margin: '0 0 14px' },
  h3: { margin: '0 0 10px', fontSize: 21, letterSpacing: '-0.03em' },
  heroText: { color: '#b6c4d9', fontSize: 19, lineHeight: 1.55, maxWidth: 600, margin: '0 0 30px' },
  buttons: { display: 'flex', gap: 14, flexWrap: 'wrap', alignItems: 'center', marginTop: 18 },
  primary: { background: `linear-gradient(135deg, ${C.green}, ${C.blue})`, color: '#04100b', padding: '15px 22px', borderRadius: 14, fontWeight: 900, border: 0, textDecoration: 'none', cursor: 'pointer' },
  secondary: { border: `1px solid ${C.border}`, color: C.text, padding: '14px 20px', borderRadius: 14, fontWeight: 800, background: 'rgba(13,20,36,.56)', textDecoration: 'none' },
  terminal: { border: `1px solid ${C.border}`, borderRadius: 26, background: 'linear-gradient(180deg,rgba(16,26,46,.96),rgba(7,11,20,.98))', boxShadow: '0 30px 100px rgba(0,0,0,.38)', overflow: 'hidden' },
  terminalHead: { display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 12, flexWrap: 'wrap', padding: '18px 20px', borderBottom: `1px solid ${C.border}` },
  boardRows: { padding: 18 },
  heroRow: { display: 'grid', gridTemplateColumns: '1fr .6fr .35fr .55fr', gap: 12, alignItems: 'center', padding: '14px', marginBottom: 10, border: '1px solid rgba(255,255,255,.06)', borderRadius: 16, background: 'rgba(255,255,255,.035)' },
  rowTitle: { fontWeight: 850 },
  heroMetrics: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit,minmax(140px,1fr))', gap: 10, padding: '0 18px 18px' },
  emptyPreview: { padding: 18, border: '1px solid rgba(255,255,255,.06)', borderRadius: 16, color: C.muted, background: 'rgba(255,255,255,.035)' },
  section: { maxWidth: 1180, margin: '0 auto', padding: '42px 0' },
  modelPanel: { maxWidth: 1180, margin: '0 auto', border: `1px solid ${C.border}`, borderRadius: 28, padding: 24, background: '#0a1020', display: 'grid', gridTemplateColumns: 'repeat(auto-fit,minmax(280px,1fr))', gap: 20, alignItems: 'center' },
  runGrid: { display: 'grid', gap: 10 },
  runStep: { display: 'grid', gridTemplateColumns: '34px 1fr', gap: 12, alignItems: 'center', padding: 12, borderRadius: 16, background: 'rgba(255,255,255,.04)', border: '1px solid rgba(255,255,255,.06)' },
  runDot: { fontSize: 20, fontWeight: 900 },
  sectionHead: { display: 'flex', justifyContent: 'space-between', gap: 18, alignItems: 'start', flexWrap: 'wrap', marginBottom: 22 },
  sub: { color: C.muted, fontSize: 17, lineHeight: 1.55, margin: 0 },
  previewGrid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit,minmax(300px,1fr))', gap: 18 },
  matchupGrid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit,minmax(260px,1fr))', gap: 14 },
  matchupCard: { border: `1px solid ${C.border}`, borderRadius: 18, padding: 16, background: 'rgba(13,20,36,.72)', textDecoration: 'none', color: C.text },
  pitcherLine: { color: C.blue, fontSize: 13, marginTop: 10, fontWeight: 750 },
  probRow: { display: 'flex', justifyContent: 'space-between', marginTop: 12, color: C.green, fontWeight: 900 },
  panel: { border: `1px solid ${C.border}`, borderRadius: 22, padding: 20, background: 'rgba(13,20,36,.72)' },
  panelLarge: { border: `1px solid ${C.border}`, borderRadius: 28, padding: 24, background: '#0a1020', boxShadow: '0 24px 80px rgba(0,0,0,.28)' },
  stack: { display: 'grid', gap: 12, marginTop: 14 },
  previewCard: { width: '100%', border: '1px solid rgba(255,255,255,.08)', borderRadius: 18, padding: 14, background: 'rgba(255,255,255,.035)', color: C.text, textAlign: 'left' },
  previewCardActive: { width: '100%', border: '1px solid rgba(66,245,141,.42)', borderRadius: 18, padding: 14, background: 'rgba(66,245,141,.10)', color: C.text, textAlign: 'left' },
  cardTop: { display: 'flex', justifyContent: 'space-between', gap: 16, marginBottom: 12 },
  cardTitle: { fontSize: 16, fontWeight: 850, color: C.text },
  cardMeta: { color: C.muted, fontSize: 12, lineHeight: 1.5 },
  scoreBox: { textAlign: 'right' },
  scoreLabel: { display: 'block', color: C.muted, fontSize: 10, textTransform: 'uppercase', letterSpacing: '.08em' },
  score: { display: 'block', color: C.green, fontSize: 22 },
  pills: { display: 'flex', gap: 8, flexWrap: 'wrap' },
  miniMetrics: { display: 'grid', gap: 4, marginTop: 12, color: C.muted, fontSize: 12 },
  metricGrid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit,minmax(140px,1fr))', gap: 10, marginTop: 18 },
  metric: { background: 'rgba(86,183,255,.06)', border: '1px solid rgba(86,183,255,.14)', borderRadius: 16, padding: 14 },
  metricLabel: { display: 'block', color: C.muted, fontSize: 10, textTransform: 'uppercase', letterSpacing: '.08em', marginBottom: 7, fontWeight: 850 },
  metricValue: { display: 'block', color: C.text, fontSize: 19, fontWeight: 950, overflowWrap: 'anywhere' },
  reason: { marginTop: 18, borderLeft: `3px solid ${C.green}`, paddingLeft: 14, color: '#b6c4d9', lineHeight: 1.55 },
  risk: { marginTop: 14, borderLeft: `3px solid ${C.yellow}`, paddingLeft: 14, color: '#b6c4d9', lineHeight: 1.55 },
  sub: { color: C.muted, fontSize: 17, lineHeight: 1.55, margin: 0 },
  status: { maxWidth: 1220, margin: '0 auto', display: 'flex', flexWrap: 'wrap', gap: 10, color: C.muted, fontSize: 12, border: `1px solid ${C.border}`, background: 'rgba(13,20,36,.52)', borderRadius: 999, padding: '10px 14px' },
  section: { maxWidth: 1180, margin: '0 auto', padding: '48px 0' },
  grid4: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit,minmax(230px,1fr))', gap: 14, marginTop: 18 },
  panel: { border: `1px solid ${C.border}`, borderRadius: 20, padding: 18, background: 'rgba(13,20,36,.72)', color: C.text, lineHeight: 1.55 },
  panelTitle: { color: C.green, fontSize: 13, fontWeight: 900, textTransform: 'uppercase', letterSpacing: '.08em', marginBottom: 10 },
  lockGrid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit,minmax(260px,1fr))', gap: 18, marginTop: 22 },
  lockCard: { border: `1px solid ${C.border}`, borderRadius: 22, padding: 20, background: 'rgba(13,20,36,.72)' },
  lockText: { color: C.yellow, fontSize: 12, fontWeight: 900, letterSpacing: '.12em', marginBottom: 10 },
  blurRows: { display: 'grid', gap: 9, marginTop: 18, filter: 'blur(2px)' },
  blurLine: { display: 'block', height: 16, borderRadius: 999, background: 'rgba(255,255,255,.16)' },
  blurLineShort: { display: 'block', width: '72%', height: 16, borderRadius: 999, background: 'rgba(255,255,255,.16)' },
  lockButton: { marginTop: 16, width: '100%', border: `1px solid ${C.border}`, color: C.text, padding: '12px 16px', borderRadius: 12, fontWeight: 900, background: 'rgba(255,255,255,.05)', cursor: 'pointer' },
  final: { textAlign: 'center', padding: '76px 28px 90px' },
  finalTitle: { fontSize: 'clamp(38px,6vw,66px)', lineHeight: .95, letterSpacing: '-0.065em', margin: '0 auto 20px', maxWidth: 760 },
  finalText: { color: C.muted, fontSize: 18, marginBottom: 28 },
  green: { color: C.green, fontWeight: 850 },
  blue: { color: C.blue, fontWeight: 850 },
  muted: { color: C.muted },
  kicker: { margin: '0 0 8px', color: C.green, fontSize: 12, fontWeight: 850, letterSpacing: '.12em', textTransform: 'uppercase' },
}
