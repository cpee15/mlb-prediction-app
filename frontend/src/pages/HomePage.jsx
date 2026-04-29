import React, { useState, useEffect, useMemo } from 'react'
import { Link, useNavigate } from 'react-router-dom'

const API = import.meta.env.VITE_API_BASE_URL || ''

const s = {
  header: { display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '24px', flexWrap: 'wrap', gap: '12px' },
  title: { fontSize: '24px', fontWeight: '700', color: '#e6edf3' },
  datePicker: {
    background: '#161b22', border: '1px solid #30363d', color: '#e6edf3',
    borderRadius: '6px', padding: '8px 12px', fontSize: '14px', cursor: 'pointer',
  },
  grid: { display: 'grid', gap: '12px' },
  card: {
    background: '#161b22', border: '1px solid #30363d', borderRadius: '10px',
    padding: '16px 20px', cursor: 'pointer', transition: 'border-color 0.15s',
  },
  cardHover: { borderColor: '#58a6ff' },
  meta: { display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '12px', fontSize: '12px', color: '#8b949e' },
  venue: { display: 'flex', gap: '8px', alignItems: 'center', flexWrap: 'wrap' },
  statusBadge: (status) => ({
    display: 'inline-block', borderRadius: '4px', padding: '2px 7px',
    fontSize: '11px', fontWeight: '600',
    background: status === 'Final' ? '#21262d' : status?.includes('Progress') ? '#1f3a1f' : '#21262d',
    color: status === 'Final' ? '#8b949e' : status?.includes('Progress') ? '#3fb950' : '#58a6ff',
  }),
  matchupRow: {
    display: 'grid', gridTemplateColumns: '1fr auto 1fr', alignItems: 'center', gap: '12px',
  },
  team: { display: 'flex', flexDirection: 'column', gap: '3px' },
  teamName: { fontSize: '16px', fontWeight: '600', color: '#e6edf3' },
  record: { fontSize: '12px', color: '#8b949e' },
  pitcher: { fontSize: '12px', color: '#58a6ff' },
  prob: { fontSize: '26px', fontWeight: '700' },
  vs: { textAlign: 'center', fontSize: '13px', color: '#8b949e', fontWeight: '600', letterSpacing: '1px' },
  noData: { color: '#8b949e', fontSize: '14px', textAlign: 'center', padding: '48px' },
  loader: { color: '#8b949e', textAlign: 'center', padding: '48px' },
  error: { color: '#f85149', textAlign: 'center', padding: '24px', background: '#1f1116', borderRadius: '8px' },
  oddsBox: { marginTop: '14px', padding: '12px', border: '1px solid #30363d', borderRadius: '8px', background: '#0d1117' },
  oddsHeader: { display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: '12px', marginBottom: '10px' },
  oddsTitle: { color: '#e6edf3', fontSize: '13px', fontWeight: '700' },
  oddsSubtle: { color: '#8b949e', fontSize: '11px' },
  oddsGrid: { display: 'grid', gridTemplateColumns: 'repeat(3, minmax(0, 1fr))', gap: '8px' },
  marketCard: { border: '1px solid #21262d', borderRadius: '7px', padding: '8px', background: '#161b22' },
  marketLabel: { color: '#8b949e', fontSize: '10px', textTransform: 'uppercase', letterSpacing: '0.7px', marginBottom: '6px', fontWeight: '700' },
  oddsLine: { display: 'flex', justifyContent: 'space-between', gap: '8px', fontSize: '12px', color: '#e6edf3', marginTop: '3px' },
  propButton: { marginTop: '10px', background: '#21262d', border: '1px solid #30363d', color: '#58a6ff', borderRadius: '6px', padding: '7px 10px', fontSize: '12px', cursor: 'pointer', fontWeight: '700' },
  propsPanel: { marginTop: '10px', borderTop: '1px solid #30363d', paddingTop: '10px' },
  propsGrid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: '8px' },
  propCard: { border: '1px solid #21262d', borderRadius: '7px', padding: '8px', background: '#161b22' },
  propMarket: { color: '#d29922', fontSize: '11px', fontWeight: '700', marginBottom: '5px' },
  propName: { color: '#e6edf3', fontSize: '12px', fontWeight: '700' },
  propDetails: { color: '#8b949e', fontSize: '12px', marginTop: '3px' },
}

function probColor(p) {
  if (p == null) return '#8b949e'
  if (p >= 0.62) return '#3fb950'
  if (p >= 0.50) return '#d29922'
  return '#f85149'
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
  if (v == null || v === '') return '—'
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
          <span>{sel.name || sel.description || '—'}{sel.line != null ? ` ${sel.line}` : ''}</span>
          <strong>{american(sel.price)}</strong>
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
        {open ? 'Hide player props' : 'Show player props'}
      </button>
      {open && (
        <div style={s.propsPanel}>
          {loading && <div style={s.oddsSubtle}>Loading DraftKings props…</div>}
          {error && <div style={{ color: '#f85149', fontSize: '12px' }}>Props error: {error}</div>}
          {!loading && !error && propsData && cards.length === 0 && (
            <div style={s.oddsSubtle}>No player props returned for this event.</div>
          )}
          <div style={s.propsGrid}>
            {cards.map(({ market, sel }, idx) => (
              <div key={`${market.market_key}-${sel.description}-${sel.name}-${idx}`} style={s.propCard}>
                <div style={s.propMarket}>{String(market.market_name || market.market_key || '').replaceAll('_', ' ')}</div>
                <div style={s.propName}>{sel.description}</div>
                <div style={s.propDetails}>{sel.name} {sel.line} · <strong style={{ color: '#e6edf3' }}>{american(sel.price)}</strong></div>
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
        <div style={s.oddsTitle}>DraftKings Odds</div>
        <div style={s.oddsSubtle}>{event.event_id ? `Event ${String(event.event_id).slice(0, 8)}` : 'Matched event'}</div>
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
  const ap = 100 - hp
  return (
    <div style={{ marginTop: '12px' }}>
      <div style={{ display: 'flex', height: '5px', borderRadius: '3px', overflow: 'hidden', background: '#21262d' }}>
        <div style={{ width: `${ap}%`, background: '#58a6ff', transition: 'width 0.4s' }} />
        <div style={{ width: `${hp}%`, background: '#3fb950', transition: 'width 0.4s' }} />
      </div>
      <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '11px', color: '#8b949e', marginTop: '3px' }}>
        <span>{ap}% away</span>
        <span>{hp}% home</span>
      </div>
    </div>
  )
}

export default function HomePage() {
  const today = new Date().toISOString().slice(0, 10)
  const [date, setDate] = useState(today)
  const [matchups, setMatchups] = useState([])
  const [oddsEvents, setOddsEvents] = useState([])
  const [loading, setLoading] = useState(false)
  const [oddsLoading, setOddsLoading] = useState(false)
  const [error, setError] = useState(null)
  const [oddsError, setOddsError] = useState(null)
  const [hovered, setHovered] = useState(null)
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
      <div style={s.header}>
        <h1 style={s.title}>Daily Matchups</h1>
        <input type="date" value={date} onChange={e => setDate(e.target.value)} style={s.datePicker} />
      </div>

      {loading && <div style={s.loader}>Loading matchups…</div>}
      {error && <div style={s.error}>Error: {error}</div>}
      {!loading && !error && oddsError && <div style={{ ...s.error, marginBottom: '12px' }}>Odds error: {oddsError}</div>}
      {!loading && !error && !oddsError && oddsLoading && <div style={s.oddsSubtle}>Loading DraftKings odds…</div>}
      {!loading && !error && matchups.length === 0 && (
        <div style={s.noData}>No games scheduled for {date}.</div>
      )}

      <div style={s.grid}>
        {matchups.map((m, i) => {
          const oddsEvent = oddsByMatchup.get(keyFromMatchup(m))
          return (
            <div
              key={m.game_pk || i}
              style={{ ...s.card, ...(hovered === i ? s.cardHover : {}) }}
              onMouseEnter={() => setHovered(i)}
              onMouseLeave={() => setHovered(null)}
              onClick={() => m.game_pk && navigate(`/matchup/${m.game_pk}`)}
            >
              <div style={s.meta}>
                <div style={s.venue}>
                  <span>{m.venue || '—'}</span>
                  {m.game_time && <span>· {formatTime(m.game_time)}</span>}
                  {weatherLabel(m.weather) && <span>· {weatherLabel(m.weather)}</span>}
                </div>
                {m.status && <span style={s.statusBadge(m.status)}>{m.status}</span>}
              </div>

              <div style={s.matchupRow}>
                <div style={s.team}>
                  <div style={s.teamName}>{m.away_team_name || `Team ${m.away_team_id}`}</div>
                  <div style={s.record}>{m.away_team_record || ''}</div>
                  <div style={s.pitcher}>
                    {m.away_pitcher_name
                      ? <Link to={`/pitcher/${m.away_pitcher_id}`} onClick={e => e.stopPropagation()} style={{ color: '#58a6ff', textDecoration: 'none' }}>
                          {m.away_pitcher_name}
                        </Link>
                      : <span style={{ color: '#8b949e' }}>TBD</span>}
                  </div>
                  <div style={{ ...s.prob, color: probColor(m.away_win_prob) }}>
                    {m.away_win_prob != null ? `${Math.round(m.away_win_prob * 100)}%` : '—'}
                  </div>
                </div>

                <div style={s.vs}>@</div>

                <div style={{ ...s.team, textAlign: 'right' }}>
                  <div style={s.teamName}>{m.home_team_name || `Team ${m.home_team_id}`}</div>
                  <div style={s.record}>{m.home_team_record || ''}</div>
                  <div style={{ ...s.pitcher, textAlign: 'right' }}>
                    {m.home_pitcher_name
                      ? <Link to={`/pitcher/${m.home_pitcher_id}`} onClick={e => e.stopPropagation()} style={{ color: '#58a6ff', textDecoration: 'none' }}>
                          {m.home_pitcher_name}
                        </Link>
                      : <span style={{ color: '#8b949e' }}>TBD</span>}
                  </div>
                  <div style={{ ...s.prob, color: probColor(m.home_win_prob), textAlign: 'right' }}>
                    {m.home_win_prob != null ? `${Math.round(m.home_win_prob * 100)}%` : '—'}
                  </div>
                </div>
              </div>

              <ProbBar homeProb={m.home_win_prob} awayProb={m.away_win_prob} />
              <OddsSnapshot event={oddsEvent} />
            </div>
          )
        })}
      </div>
    </div>
  )
}
