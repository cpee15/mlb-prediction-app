import React, { useEffect, useMemo, useState } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import { API_BASE, fetchJson, getMlbLiveDate, readCachedJson } from '../lib/api'
import { selectBetOfTheDay } from '../lib/landing/selectBetOfTheDay.mjs'

const TTL = { matchups: 120, events: 90, models: 120, board: 300 }
const C = { bg: '#070b14', border: '#21304a', text: '#eef5ff', muted: '#91a1bb', green: '#42f58d', blue: '#56b7ff', yellow: '#ffd166', red: '#ff6b7a' }

const pct = v => Number.isFinite(Number(v)) ? `${(Number(v) * 100).toFixed(1)}%` : 'Unavailable'
const odds = v => Number.isFinite(Number(v)) ? (Math.round(Number(v)) > 0 ? `+${Math.round(Number(v))}` : `${Math.round(Number(v))}`) : 'Unavailable'
const money = v => Number.isFinite(Number(v)) ? `${Number(v) >= 0 ? '+' : '-'}$${Math.abs(Number(v)).toFixed(1)}` : 'Unavailable'
const dateLabel = d => { try { return new Date(`${d}T12:00:00`).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }) } catch { return d } }
const impliedFormula = o => Number(o) > 0 ? `100 / (${Number(o)} + 100)` : `${Math.abs(Number(o))} / (${Math.abs(Number(o))} + 100)`

function useLandingBet(date) {
  const matchupsUrl = `${API_BASE}/matchups?date=${date}`
  const eventsUrl = `${API_BASE}/odds/draftkings/events?date=${date}`
  const modelsUrl = `${API_BASE}/daily-odds/models?date=${date}`
  const boardKey = `${API_BASE}/my-dashboard/solver/batch::landing-bet-of-day::${date}`
  const [matchups, setMatchups] = useState(() => readCachedJson(matchupsUrl, TTL.matchups) || [])
  const [eventsPayload, setEventsPayload] = useState(() => readCachedJson(eventsUrl, TTL.events))
  const [modelPayload, setModelPayload] = useState(() => readCachedJson(modelsUrl, TTL.models))
  const [boardPayload, setBoardPayload] = useState(() => readCachedJson(boardKey, TTL.board))
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  useEffect(() => {
    let cancelled = false
    async function load() {
      setLoading(true)
      setError('')
      try {
        const [m, e, models] = await Promise.all([
          fetchJson(matchupsUrl, { ttlSeconds: TTL.matchups }),
          fetchJson(eventsUrl, { ttlSeconds: TTL.events }),
          fetchJson(modelsUrl, { ttlSeconds: TTL.models }).catch(() => null),
        ])
        if (cancelled) return
        setMatchups(Array.isArray(m) ? m : [])
        setEventsPayload(e)
        setModelPayload(models)
      } catch (err) {
        if (!cancelled) setError(String(err?.message || err))
      } finally {
        if (!cancelled) setLoading(false)
      }

      try {
        const res = await fetch(`${API_BASE}/my-dashboard/solver/batch`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ date, components: ['teams', 'totals'], filters_by_component: { teams: {}, totals: {} }, active_lineups: false }),
        })
        const json = await res.json().catch(() => null)
        if (!cancelled && res.ok) {
          sessionStorage.setItem(`mlb-json-cache:v1:${boardKey}`, JSON.stringify({ createdAt: Date.now(), value: json }))
          setBoardPayload(json)
        }
      } catch {}
    }
    load()
    return () => { cancelled = true }
  }, [date, matchupsUrl, eventsUrl, modelsUrl, boardKey])

  const events = Array.isArray(eventsPayload?.events) ? eventsPayload.events : []
  const bet = useMemo(() => selectBetOfTheDay({ matchups, events, modelPayload, boardPayload }), [matchups, events, modelPayload, boardPayload])
  const modelRows = Array.isArray(modelPayload?.games) ? modelPayload.games.length : Array.isArray(modelPayload?.models) ? modelPayload.models.length : 0
  return { bet, loading, error, counts: { matchups: matchups.length, events: events.length, modelRows } }
}

function ValueMetric({ label, value, accent }) {
  return <div style={s.metric}><span style={s.metricLabel}>{label}</span><strong style={{ ...s.metricValue, color: accent ? C.green : C.text }}>{value}</strong></div>
}

function BetCard({ bet, loading, onUnlock }) {
  if (!bet) return <article style={s.betCard}>
    <div style={s.kicker}>Bet of the Day</div>
    <h2 style={s.betTitle}>{loading ? 'Finding today’s best value...' : 'Today’s full card is available inside Pro.'}</h2>
    <p style={s.sub}>{loading ? 'Scanning model probabilities and market prices.' : 'No public Bet of the Day is available yet because no eligible play has both model probability and odds. No fake play is shown.'}</p>
    <div style={s.blurRows}><span style={s.blurLine} /><span style={s.blurLine} /><span style={s.blurLineShort} /></div>
    <button type="button" onClick={onUnlock} style={{ ...s.primary, marginTop: 18 }}>Unlock Today’s Card</button>
  </article>

  return <article style={s.betCard}>
    <div style={s.cardTop}><span style={s.kicker}>Bet of the Day</span><span style={s.livePill}>Value math</span></div>
    <h2 style={s.betTitle}>{bet.matchup}</h2>
    <div style={s.pickLine}>{bet.pick}</div>
    <div style={s.marketLine}>{bet.market}</div>
    <div style={s.valueGrid}>
      <ValueMetric label="Sportsbook Price" value={odds(bet.americanOdds)} />
      <ValueMetric label="MLBGPT Fair Price" value={odds(bet.fairOdds)} />
      <ValueMetric label="Model Probability" value={pct(bet.modelProbability)} />
      <ValueMetric label="Market Implied" value={pct(bet.impliedProbability)} />
      <ValueMetric label="Edge" value={`+${pct(bet.edgePct)}`} accent />
      <ValueMetric label="EV per $100" value={money(bet.evPer100)} accent />
    </div>
    <div style={s.reason}><strong>Why it grades well:</strong><p>{bet.reason}</p></div>
    <div style={s.risk}><strong>Risk note:</strong><p>{bet.riskNote}</p></div>
    <div style={s.buttons}><button type="button" onClick={onUnlock} style={s.primary}>Unlock Full Card</button>{bet.href && <Link to={bet.href} style={s.secondary}>View Matchup</Link>}</div>
  </article>
}

function FormulaPanel({ bet }) {
  return <section style={s.section}>
    <h2 style={s.h2}>How MLBGPT finds value</h2>
    <p style={s.sub}>The landing page calculates odds math only. It does not create baseball projections.</p>
    {bet ? <div style={s.grid4}>
      <Formula title="1. Convert odds" body={`For ${odds(bet.americanOdds)}: ${impliedFormula(bet.americanOdds)} = ${pct(bet.impliedProbability)}`} />
      <Formula title="2. Compare probability" body={`Model probability: ${pct(bet.modelProbability)}. Market probability: ${pct(bet.impliedProbability)}.`} />
      <Formula title="3. Calculate edge" body={`${pct(bet.modelProbability)} - ${pct(bet.impliedProbability)} = +${pct(bet.edgePct)}`} />
      <Formula title="4. Estimate EV" body={`EV per $100 = ${money(bet.evPer100)} using the sportsbook price and model probability.`} />
    </div> : <div style={s.panel}>Formula panel appears only when a real candidate has both model probability and odds.</div>}
  </section>
}

function Formula({ title, body }) {
  return <div style={s.panel}><div style={s.panelTitle}>{title}</div><div>{body}</div></div>
}

function LockedPreview({ onUnlock }) {
  const cards = [['Sides & Totals', 'Moneylines, run lines, totals, F5 angles'], ['Hitter Props', 'Hits, total bases, home runs, runs, RBI'], ['Pitcher Props', 'Strikeouts, outs, walks, earned runs']]
  return <section style={s.section}>
    <h2 style={s.h2}>The full card stays inside Pro.</h2>
    <p style={s.sub}>See every qualified side, total, F5 angle, hitter prop, and pitcher prop with model context, odds value, and risk notes.</p>
    <div style={s.lockGrid}>{cards.map(([title, body]) => <div key={title} style={s.lockCard}><div style={s.lockText}>LOCKED</div><h3 style={s.h3}>{title}</h3><p style={s.sub}>{body}</p><div style={s.blurRows}><span style={s.blurLine} /><span style={s.blurLine} /><span style={s.blurLineShort} /></div><button type="button" onClick={onUnlock} style={s.lockButton}>Unlock Pro</button></div>)}</div>
  </section>
}

function ProductProof() {
  const cards = [['Model Projection', 'MLBGPT starts with a projected probability from the model.'], ['Market Odds', 'The app compares that projection against live or stored sportsbook prices.'], ['Implied Probability', 'Odds are converted into market-implied probability.'], ['Expected Value', 'The difference between the model probability and market price creates the edge.']]
  return <section style={s.section}><h2 style={s.h2}>What MLBGPT evaluates</h2><div style={s.grid4}>{cards.map(([title, body]) => <div key={title} style={s.panel}><div style={s.panelTitle}>{title}</div><p style={s.sub}>{body}</p></div>)}</div></section>
}

export default function LandingV2Page() {
  const navigate = useNavigate()
  const date = getMlbLiveDate()
  const { bet, loading, error, counts } = useLandingBet(date)
  const unlock = () => navigate('/my-dashboard')
  return <div style={s.page}>
    <section style={s.hero}><div><div style={s.eyebrow}>Live MLB Prediction Engine</div><h1 style={s.h1}>Today’s MLB card, ranked by edge.</h1><p style={s.heroText}>MLBGPT compares model projections, sportsbook odds, implied probability, and market value to find the best MLB betting angles before first pitch.</p><div style={s.buttons}><button type="button" onClick={unlock} style={s.primary}>Unlock Today’s Card</button><Link to="/" style={s.secondary}>View Matchups</Link></div></div><BetCard bet={bet} loading={loading} onUnlock={unlock} /></section>
    <div style={s.status}><span>{dateLabel(date)}</span><span>{loading ? 'Scanning prices...' : 'Scan complete'}</span><span>{counts.matchups} matchups | {counts.events} events | {counts.modelRows} model rows</span>{error && <span style={{ color: C.red }}>{error.slice(0, 90)}</span>}</div>
    <FormulaPanel bet={bet} />
    <LockedPreview onUnlock={unlock} />
    <ProductProof />
    <section style={s.final}><h2 style={s.finalTitle}>Want the full card?</h2><p style={s.finalText}>Unlock every qualified play with odds value, model context, and risk notes.</p><button type="button" onClick={unlock} style={s.primary}>Unlock Today’s Card</button></section>
  </div>
}

const s = {
  page: { margin: '-34px calc(50% - 50vw) -56px', padding: '76px 28px 0', minHeight: '100vh', color: C.text, background: `radial-gradient(circle at top left, rgba(66,245,141,.12), transparent 32%), radial-gradient(circle at 75% 10%, rgba(86,183,255,.12), transparent 28%), ${C.bg}` },
  hero: { maxWidth: 1220, margin: '0 auto', paddingBottom: 28, display: 'grid', gridTemplateColumns: 'repeat(auto-fit,minmax(340px,1fr))', gap: 40, alignItems: 'center' },
  eyebrow: { display: 'inline-flex', padding: '8px 12px', border: `1px solid ${C.border}`, borderRadius: 999, background: 'rgba(13,20,36,.72)', color: C.green, fontSize: 13, fontWeight: 800, marginBottom: 22 },
  h1: { fontSize: 'clamp(48px,7vw,78px)', lineHeight: .94, letterSpacing: '-0.075em', margin: '0 0 24px' },
  h2: { fontSize: 'clamp(32px,5vw,42px)', lineHeight: 1, letterSpacing: '-0.05em', margin: '0 0 14px' },
  h3: { margin: '0 0 10px', fontSize: 21, letterSpacing: '-0.03em' },
  heroText: { color: '#b6c4d9', fontSize: 19, lineHeight: 1.55, maxWidth: 600, margin: '0 0 30px' },
  buttons: { display: 'flex', gap: 14, flexWrap: 'wrap', alignItems: 'center', marginTop: 18 },
  primary: { background: `linear-gradient(135deg, ${C.green}, ${C.blue})`, color: '#04100b', padding: '15px 22px', borderRadius: 14, fontWeight: 900, border: 0, textDecoration: 'none', cursor: 'pointer' },
  secondary: { border: `1px solid ${C.border}`, color: C.text, padding: '14px 20px', borderRadius: 14, fontWeight: 800, background: 'rgba(13,20,36,.56)', textDecoration: 'none' },
  betCard: { border: `1px solid ${C.border}`, borderRadius: 28, background: 'linear-gradient(180deg,rgba(16,26,46,.96),rgba(7,11,20,.98))', boxShadow: '0 30px 100px rgba(0,0,0,.38)', padding: 24 },
  cardTop: { display: 'flex', justifyContent: 'space-between', gap: 14, alignItems: 'center', marginBottom: 12 },
  kicker: { color: C.green, fontSize: 12, fontWeight: 850, letterSpacing: '.12em', textTransform: 'uppercase' },
  livePill: { color: C.green, border: '1px solid rgba(66,245,141,.35)', borderRadius: 999, padding: '6px 10px', fontSize: 12, fontWeight: 850, background: 'rgba(66,245,141,.08)' },
  betTitle: { margin: 0, fontSize: 32, letterSpacing: '-0.04em', lineHeight: 1.05 },
  pickLine: { color: C.green, fontSize: 27, fontWeight: 950, marginTop: 12 },
  marketLine: { color: C.muted, fontSize: 14, fontWeight: 800, textTransform: 'uppercase', letterSpacing: '.08em', marginTop: 4 },
  valueGrid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit,minmax(145px,1fr))', gap: 10, marginTop: 20 },
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
}
