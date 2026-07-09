import React, { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { API_BASE } from '../lib/api'

const API = API_BASE
const CALENDAR_SCHEDULE_URL = `${API}/matchups/calendar/schedule`

const card = { background: '#161b22', border: '1px solid #30363d', borderRadius: '10px', padding: '16px', marginBottom: '16px' }

export default function YesterdayTodayPage() {
  const [data, setData] = useState(null)
  const [error, setError] = useState(null)

  async function loadCalendar() {
    const res = await fetch(CALENDAR_SCHEDULE_URL)
    if (!res.ok) throw new Error(`${res.status} ${res.statusText}`)
    return res.json()
  }

  useEffect(() => {
    loadCalendar()
      .then(setData)
      .catch(e => setError(String(e)))
  }, [])

  async function snapshot() {
    await fetch(`${API}/matchups/calendar/snapshot`, { method: 'POST' })
    const refreshed = await loadCalendar()
    setData(refreshed)
  }

  if (error) return <div style={{ color: '#f85149' }}>{error}</div>
  if (!data) return <div style={{ color: '#8b949e' }}>Loading calendar snapshots…</div>

  return (
    <div>
      <h1 style={{ fontSize: '24px', marginBottom: '16px' }}>Matchup Calendar (Yesterday / Today)</h1>
      {['yesterday', 'today', 'tomorrow'].map((k) => (
        <div key={k} style={card}>
          <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '10px' }}>
            <strong style={{ textTransform: 'capitalize' }}>{k}: {data[k].date}</strong>
            <button onClick={snapshot}>Save latest copy</button>
          </div>
          <div style={{ fontSize: '13px', color: '#8b949e', marginBottom: '8px' }}>{data[k].count} games</div>
          {data[k].games.slice(0, 8).map((g) => (
            <div key={g.game_pk} style={{ marginBottom: '6px', fontSize: '14px' }}>
              <Link to={`/matchup/${g.game_pk}`} style={{ color: '#58a6ff', textDecoration: 'none' }}>
                {g.away_team_name} @ {g.home_team_name}
              </Link>
            </div>
          ))}
        </div>
      ))}
    </div>
  )
}
