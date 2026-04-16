import React, { useState, useEffect } from 'react'
import { Link } from 'react-router-dom'

const API = '/api'

const s = {
  header: { display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '24px' },
  title: { fontSize: '24px', fontWeight: '700', color: '#e6edf3' },
  datePicker: {
    background: '#161b22', border: '1px solid #30363d', color: '#e6edf3',
    borderRadius: '6px', padding: '8px 12px', fontSize: '14px', cursor: 'pointer',
  },
  grid: { display: 'grid', gap: '16px' },
  card: {
    background: '#161b22', border: '1px solid #30363d', borderRadius: '10px',
    padding: '20px 24px', display: 'grid',
    gridTemplateColumns: '1fr auto 1fr', alignItems: 'center', gap: '16px',
  },
  team: { display: 'flex', flexDirection: 'column', gap: '4px' },
  teamName: { fontSize: '18px', fontWeight: '600', color: '#e6edf3' },
  pitcher: { fontSize: '13px', color: '#8b949e' },
  prob: { fontSize: '28px', fontWeight: '700' },
  probWin: { color: '#3fb950' },
  probLose: { color: '#8b949e' },
  vs: { textAlign: 'center' },
  vsText: { fontSize: '13px', color: '#8b949e', fontWeight: '600', letterSpacing: '1px' },
  badge: {
    display: 'inline-block', background: '#1f3a1f', color: '#3fb950',
    borderRadius: '4px', padding: '2px 8px', fontSize: '12px', fontWeight: '600',
    marginTop: '4px',
  },
  noData: { color: '#8b949e', fontSize: '14px', textAlign: 'center', padding: '48px' },
  loader: { color: '#8b949e', textAlign: 'center', padding: '48px' },
  error: { color: '#f85149', textAlign: 'center', padding: '24px', background: '#1f1116', borderRadius: '8px' },
}

function probColor(p) {
  if (p === null || p === undefined) return '#8b949e'
  if (p >= 0.6) return '#3fb950'
  if (p >= 0.5) return '#d29922'
  return '#f85149'
}

function ProbBar({ homeProb, awayProb }) {
  const hp = homeProb != null ? Math.round(homeProb * 100) : 50
  const ap = 100 - hp
  return (
    <div style={{ margin: '12px 0 0', gridColumn: '1/-1' }}>
      <div style={{ display: 'flex', height: '6px', borderRadius: '3px', overflow: 'hidden', background: '#21262d' }}>
        <div style={{ width: `${ap}%`, background: '#58a6ff', transition: 'width 0.4s' }} />
        <div style={{ width: `${hp}%`, background: '#3fb950', transition: 'width 0.4s' }} />
      </div>
      <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '11px', color: '#8b949e', marginTop: '4px' }}>
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
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  useEffect(() => {
    setLoading(true)
    setError(null)
    fetch(`${API}/matchups?date=${date}`)
      .then(r => r.ok ? r.json() : Promise.reject(r.statusText))
      .then(data => { setMatchups(data); setLoading(false) })
      .catch(e => { setError(String(e)); setLoading(false) })
  }, [date])

  return (
    <div>
      <div style={s.header}>
        <h1 style={s.title}>Today's Matchups</h1>
        <input
          type="date"
          value={date}
          onChange={e => setDate(e.target.value)}
          style={s.datePicker}
        />
      </div>

      {loading && <div style={s.loader}>Loading matchups…</div>}
      {error && <div style={s.error}>Error: {error}</div>}

      {!loading && !error && matchups.length === 0 && (
        <div style={s.noData}>No games scheduled for {date}.</div>
      )}

      <div style={s.grid}>
        {matchups.map((m, i) => (
          <div key={i} style={s.card}>
            {/* Away team */}
            <div style={s.team}>
              <div style={s.teamName}>{m.away_team_name || `Team ${m.away_team_id}`}</div>
              <div style={s.pitcher}>
                {m.away_pitcher_name
                  ? <Link to={`/pitcher/${m.away_pitcher_id}`} style={{ color: '#58a6ff', textDecoration: 'none' }}>
                      {m.away_pitcher_name}
                    </Link>
                  : 'TBD'}
              </div>
              <div style={{ ...s.prob, color: probColor(m.away_win_prob) }}>
                {m.away_win_prob != null ? `${Math.round(m.away_win_prob * 100)}%` : '—'}
              </div>
            </div>

            {/* VS */}
            <div style={s.vs}>
              <div style={s.vsText}>@</div>
            </div>

            {/* Home team */}
            <div style={{ ...s.team, textAlign: 'right' }}>
              <div style={s.teamName}>{m.home_team_name || `Team ${m.home_team_id}`}</div>
              <div style={s.pitcher}>
                {m.home_pitcher_name
                  ? <Link to={`/pitcher/${m.home_pitcher_id}`} style={{ color: '#58a6ff', textDecoration: 'none' }}>
                      {m.home_pitcher_name}
                    </Link>
                  : 'TBD'}
              </div>
              <div style={{ ...s.prob, color: probColor(m.home_win_prob) }}>
                {m.home_win_prob != null ? `${Math.round(m.home_win_prob * 100)}%` : '—'}
              </div>
            </div>

            <ProbBar homeProb={m.home_win_prob} awayProb={m.away_win_prob} />
          </div>
        ))}
      </div>
    </div>
  )
}
