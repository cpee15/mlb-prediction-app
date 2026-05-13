import React, { useState, useEffect, useMemo } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import { API_BASE, getMlbLiveDate } from '../lib/api'

const API = API_BASE

const s = {
  matchupGrid: { display: 'grid', gap: '16px' },
  card: { cursor: 'pointer' },
  slateMeta: { display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16, gap: 12, flexWrap: 'wrap' },
  venue: { display: 'flex', gap: 8, alignItems: 'center', flexWrap: 'wrap', color: 'var(--text-muted)', fontSize: 12 },
  matchupRow: { display: 'grid', gridTemplateColumns: '1fr auto 1fr', alignItems: 'stretch', gap: 16 },
  teamPanel: { border: '1px solid var(--border-subtle)', borderRadius: 'var(--radius-md)', padding: 14, background: 'rgba(7, 11, 18, 0.34)' },
  teamPanelRight: { textAlign: 'right' },
  teamName: { fontSize: 18, fontWeight: 850, color: 'var(--text-primary)', letterSpacing: '-0.03em' },
  record: { fontSize: 12, color: 'var(--text-muted)', marginTop: 2 },
  pitcher: { fontSize: 13, color: 'var(--accent)', marginTop: 10, minHeight: 20 },
  prob: { fontSize: 30, fontWeight: 900, letterSpacing: '-0.06em', marginTop: 12 },
  vs: { display: 'grid', placeItems: 'center', color: 'var(--text-muted)', fontWeight: 900, letterSpacing: '0.18em', fontSize: 12 },
  oddsBox: { marginTop: 16, padding: 14, border: '1px solid var(--border-subtle)', borderRadius: 'var(--radius-md)', background: 'rgba(7, 11, 18, 0.46)' },
  oddsHeader: { display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 12, marginBottom: 12 },
  oddsTitle: { color: 'var(--text-primary)', fontSize: 13, fontWeight: 850, letterSpacing: '0.02em' },
  oddsSubtle: { color: 'var(--text-muted)', fontSize: 12 },
  oddsGrid: { display: 'grid', gridTemplateColumns: 'repeat(3, minmax(0, 1fr))', gap: 10 },
  marketCard: { border: '1px solid var(--border-subtle)', borderRadius: 'var(--radius-sm)', padding: 10, background: 'rgba(17, 24, 39, 0.76)' },
  marketLabel: { color: 'var(--text-muted)', fontSize: 10, textTransform: 'uppercase', letterSpacing: '0.1em', marginBottom: 7, fontWeight: 850 },
  oddsLine: { display: 'flex', justifyContent: 'space-between', gap: 8, fontSize: 12, color: 'var(--text-secondary)', marginTop: 4 },
  propButton: { marginTop: 12, padding: '8px 11px', fontSize: 12, fontWeight: 800 },
  propsPanel: { marginTop: 12, borderTop: '1px solid var(--border-subtle)', paddingTop: 12 },
  propsGrid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: 10 },
  propCard: { border: '1px solid var(--border-subtle)', borderRadius: 'var(--radius-sm)', padding: 10, background: 'rgba(17, 24, 39, 0.78)' },
  propMarket: { color: 'var(--warning)', fontSize: 11, fontWeight: 850, marginBottom: 5 },
  propName: { color: 'var(--text-primary)', fontSize: 12, fontWeight: 800 },
  propDetails: { color: 'var(--text-muted)', fontSize: 12, marginTop: 3 },
}

function probColor(p) {
  if (p == null) return 'var(--text-muted)'
  if (p >= 0.62) return 'var(--success)'
  if (p >= 0.50) return 'var(--warning)'
  return 'var(--danger)'
}

function formatTime(iso) {
  if (!iso) return null
  try {
    const d = new Date(iso)
    return d.toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit', timeZone: 'America/New_York' }) + ' ET'
  } catch { return null }
}

function weatherLabel(weather) {
  if (!weather) return null
  const pieces = []
  if (weather.temp_f != null) pieces.push(`${weather.temp_f}°F`)
  if (weather.condition) pieces.push(weather.condition)
  if (weather.wind) pieces.push(weather.wind)
  return pieces.length ? pieces.join(' · ') : null
}

function american(v) {
  if (v == null || v === '') return 'Unavailable'
  const n = Number(v)
  if (Number.isNaN(n)) return String(v)
  return n > 0 ? `+${n}` : `${n}`
}

function normalizeTeamName(name) {
  return String(name || '')
    .toLowerCase()
    .replace(/[^a-z0-9]/g, '')
    .replace(/^the/, '')
}

function matchupKey(away, home) {
  return `${normalizeTeamName(away)}@${normalizeTeamName(home)}`
}

function keyFromMatchup(m) {
  return matchupKey(m.away_team_name || m.away_team || m.away_name, m.home_team_name || m.home_team || m.home_name)
}

function keyFromEvent(e) {
  const away = e?.away_team?.name || e?.away_team || ''
  const home = e?.home_team?.name || e?.home_team || ''
  return matchupKey(away, home)
}

function eventMarkets(event) {
  return event?.markets || []
}

function findMarket(event, keys) {
  const wanted = Array.isArray(keys) ? keys : [keys]
  return eventMarkets(event).find(m => wanted.includes(m.market_key) || wanted.includes(m.market_type) || wanted.includes(m.market_name))
}

function OddsMarket({ label, market }) {
  const selections = market?.selections || []
  if (!market || selections.length === 0) {
    return (
      <div style={s.marketCard}>
        <div style={s.marketLabel}>{label}</div>
        <div style={s.oddsLine}><span>Unavailable</span><span>—</span></div>
      </div>
    )
  }
  return (
    <div style={s.marketCard}>
      <div style={s.marketLabel}>{label}</div>
      {selections.slice(0, 2).map((sel, idx) => (
        <div key={`${label}-${idx}`} style={s.oddsLine}>
          <span>{sel.name || sel.description || 'Unavailable'}{sel.line != null ? ` ${sel.line}` : ''}</span>
          <strong style={{ color: 'var(--text-primary)' }}>{american(sel.price)}</strong>
        </div>
      ))}
    </div>
  )
}

function PropPreview({ eventId }) {
  const [open, setOpen] = useState(false)
  const [loading, setLoading] = useState(false)
  const [propsData, setPropsData] = useState(null)
  const [error, setError] = useState(null)

  function toggle(e) {
    e.stopPropagation()
    if (open) {
      setOpen(false)
      return
    }
    setOpen(true)
    if (propsData || loading) return
    setLoading(true)
    setError(null)
    fetch(`${API}/odds/draftkings/event/${eventId}/props`)
      .then(r => r.ok ? r.json() : Promise.reject(r.statusText))
      .then(data => { setPropsData(data); setLoading(false) })
      .catch(err => { setError(String(err)); setLoading(false) })
  }

  const markets = propsData?.markets || propsData?.event?.markets || []
  const cards = markets.flatMap(market =>
    (market.selections || [])
      .filter(sel => sel?.description && sel?.name)
      .slice(0, 8)
      .map(sel => ({ market, sel }))
  ).slice(0, 12)

  return (
    <div onClick={e => e.stopPropagation()}>
      <button type="button" style={s.propButton} onClick={toggle}>
        {open ? 'Hide Player Props' : 'Show Player Props'}
      </button>
      {open && (
        <div style={s.propsPanel}>
          {loading && <div style={s.oddsSubtle}>Loading DraftKings props…</div>}
          {error && <div className="state-panel error" style={{ padding: 12, textAlign: 'left' }}>Props unavailable: {error}</div>}
          {!loading && !error && propsData && cards.length === 0 && (
            <div style={s.oddsSubtle}>No player props returned for this event.</div>
          )}
          <div style={s.propsGrid}>
            {cards.map(({ market, sel }, idx) => (
              <div key={`${market.market_key}-${sel.description}-${sel.name}-${idx}`} style={s.propCard}>
                <div style={s.propMarket}>{String(market.market_name || market.market_key || '').replaceAll('_', ' ')}</div>
                <div style={s.propName}>{sel.description}</div>
                <div style={s.propDetails}>{sel.name} {sel.line} · <strong style={{ color: 'var(--text-primary)' }}>{american(sel.price)}</strong></div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}

function OddsSnapshot({ event }) {
  if (!event) return null
  const moneyline = findMarket(event, 'h2h')
  const spread = findMarket(event, 'spreads')
  const total = findMarket(event, 'totals')
  return (
    <div style={s.oddsBox} onClick={e => e.stopPropagation()}>
      <div style={s.oddsHeader}>
        <div>
          <div style={s.oddsTitle}>DraftKings Market Snapshot</div>
          <div style={s.oddsSubtle}>Moneyline, run line, totals, and available props</div>
        </div>
        <div className="status-badge">Market Context</div>
      </div>
      <div style={s.oddsGrid}>
        <OddsMarket label="Moneyline" market={moneyline} />
        <OddsMarket label="Run Line" market={spread} />
        <OddsMarket label="Total" market={total} />
      </div>
      {event.event_id && <PropPreview eventId={event.event_id} />}
    </div>
  )
}

function ProbBar({ homeProb, awayProb }) {
  const hp = homeProb != null ? Math.round(homeProb * 100) : 50
  const ap = awayProb != null ? Math.round(awayProb * 100) : 100 - hp
  return (
    <div style={{ marginTop: 14 }}>
      <div style={{ display: 'flex', height: 6, borderRadius: 999, overflow: 'hidden', background: 'rgba(148, 163, 184, 0.14)' }}>
        <div style={{ width: `${ap}%`, background: 'linear-gradient(90deg, #5aa7ff, #7bbcff)', transition: 'width 0.4s' }} />
        <div style={{ width: `${hp}%`, background: 'linear-gradient(90deg, #41d695, #8ee8bd)', transition: 'width 0.4s' }} />
      </div>
      <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 11, color: 'var(--text-muted)', marginTop: 5 }}>
        <span>{awayProb != null ? `${ap}% away` : 'Away pending'}</span>
        <span>{homeProb != null ? `${hp}% home` : 'Home pending'}</span>
      </div>
    </div>
  )
}

function statusClass(status) {
  if (!status) return ''
  if (status === 'Final') return ''
  if (String(status).toLowerCase().includes('progress') || String(status).toLowerCase().includes('live')) return 'success'
  return 'warning'
}

export default function HomePage() {
  const today = getMlbLiveDate()
  const [date, setDate] = useState(today)
  const [matchups, setMatchups] = useState([])
  const [oddsEvents, setOddsEvents] = useState([])
  const [loading, setLoading] = useState(false)
  const [oddsLoading, setOddsLoading] = useState(false)
  const [error, setError] = useState(null)
  const [oddsError, setOddsError] = useState(null)
  const navigate = useNavigate()

  useEffect(() => {
    setLoading(true)
    setError(null)
    fetch(`${API}/matchups?date=${date}`)
      .then(r => r.ok ? r.json() : Promise.reject(r.statusText))
      .then(data => { setMatchups(Array.isArray(data) ? data : []); setLoading(false) })
      .catch(e => { setError(String(e)); setLoading(false) })
  }, [date])

  useEffect(() => {
    setOddsLoading(true)
    setOddsError(null)
    fetch(`${API}/odds/draftkings/events?date=${date}`)
      .then(r => r.ok ? r.json() : Promise.reject(r.statusText))
      .then(data => { setOddsEvents(Array.isArray(data?.events) ? data.events : []); setOddsLoading(false) })
      .catch(e => { setOddsError(String(e)); setOddsEvents([]); setOddsLoading(false) })
  }, [date])

  const oddsByMatchup = useMemo(() => {
    const map = new Map()
    oddsEvents.forEach(event => {
      const key = keyFromEvent(event)
      if (key !== '@') map.set(key, event)
    })
    return map
  }, [oddsEvents])

  return (
    <div>
      <header className="page-header">
        <div>
          <p className="page-kicker">Live Slate</p>
          <h1 className="page-title">Daily Matchups</h1>
          <p className="page-subtitle">Validated MLB schedule, probable starters, model win probabilities, weather context, and DraftKings market snapshots in one production slate.</p>
        </div>
        <div className="control-row">
          <span className="status-badge success">Live Data</span>
          <input className="input-control" type="date" value={date} onChange={e => setDate(e.target.value)} />
        </div>
      </header>

      {loading && (
        <div className="state-panel">
          <div className="skeleton-line" style={{ maxWidth: 420, margin: '0 auto 12px' }} />
          <div className="skeleton-line" style={{ maxWidth: 300, margin: '0 auto' }} />
        </div>
      )}
      {error && <div className="state-panel error">Matchup data unavailable: {error}</div>}
      {!loading && !error && oddsError && <div className="state-panel error" style={{ marginBottom: 16 }}>Odds data unavailable: {oddsError}</div>}
      {!loading && !error && !oddsError && oddsLoading && <div className="card-meta" style={{ marginBottom: 14 }}>Loading DraftKings market context…</div>}
      {!loading && !error && matchups.length === 0 && (
        <div className="state-panel">No games are scheduled for {date}.</div>
      )}

      <div style={s.matchupGrid}>
        {matchups.map((m, i) => {
          const oddsEvent = oddsByMatchup.get(keyFromMatchup(m))
          return (
            <article
              key={m.game_pk || i}
              className="pro-card pro-card-hover card-pad"
              style={s.card}
              onClick={() => m.game_pk && navigate(`/matchup/${m.game_pk}`)}
            >
              <div style={s.slateMeta}>
                <div style={s.venue}>
                  <span>{m.venue || 'Venue pending'}</span>
                  {m.game_time && <span>· {formatTime(m.game_time)}</span>}
                  {weatherLabel(m.weather) && <span>· {weatherLabel(m.weather)}</span>}
                </div>
                {m.status && <span className={`status-badge ${statusClass(m.status)}`}>{m.status}</span>}
              </div>

              <div style={s.matchupRow}>
                <div style={s.teamPanel}>
                  <div style={s.teamName}>{m.away_team_name || `Team ${m.away_team_id}`}</div>
                  <div style={s.record}>{m.away_team_record || 'Record pending'}</div>
                  <div style={s.pitcher}>
                    {m.away_pitcher_name
                      ? <Link to={`/pitcher/${m.away_pitcher_id}`} onClick={e => e.stopPropagation()} style={{ color: 'var(--accent)', textDecoration: 'none', fontWeight: 750 }}>
                          {m.away_pitcher_name}
                        </Link>
                      : <span className="status-badge warning">Pitcher Pending</span>}
                  </div>
                  <div style={{ ...s.prob, color: probColor(m.away_win_prob) }}>
                    {m.away_win_prob != null ? `${Math.round(m.away_win_prob * 100)}%` : 'Pending'}
                  </div>
                </div>

                <div style={s.vs}>AT</div>

                <div style={{ ...s.teamPanel, ...s.teamPanelRight }}>
                  <div style={s.teamName}>{m.home_team_name || `Team ${m.home_team_id}`}</div>
                  <div style={s.record}>{m.home_team_record || 'Record pending'}</div>
                  <div style={{ ...s.pitcher, textAlign: 'right' }}>
                    {m.home_pitcher_name
                      ? <Link to={`/pitcher/${m.home_pitcher_id}`} onClick={e => e.stopPropagation()} style={{ color: 'var(--accent)', textDecoration: 'none', fontWeight: 750 }}>
                          {m.home_pitcher_name}
                        </Link>
                      : <span className="status-badge warning">Pitcher Pending</span>}
                  </div>
                  <div style={{ ...s.prob, color: probColor(m.home_win_prob), textAlign: 'right' }}>
                    {m.home_win_prob != null ? `${Math.round(m.home_win_prob * 100)}%` : 'Pending'}
                  </div>
                </div>
              </div>

              <ProbBar homeProb={m.home_win_prob} awayProb={m.away_win_prob} />
              <OddsSnapshot event={oddsEvent} />
            </article>
          )
        })}
      </div>
    </div>
  )
}
