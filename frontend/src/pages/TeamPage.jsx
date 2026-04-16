import React, { useState } from 'react'

const API = '/api'

const TEAMS = [
  { id: 108, name: 'Los Angeles Angels' },
  { id: 109, name: 'Arizona Diamondbacks' },
  { id: 110, name: 'Baltimore Orioles' },
  { id: 111, name: 'Boston Red Sox' },
  { id: 112, name: 'Chicago Cubs' },
  { id: 113, name: 'Cincinnati Reds' },
  { id: 114, name: 'Cleveland Guardians' },
  { id: 115, name: 'Colorado Rockies' },
  { id: 116, name: 'Detroit Tigers' },
  { id: 117, name: 'Houston Astros' },
  { id: 118, name: 'Kansas City Royals' },
  { id: 119, name: 'Los Angeles Dodgers' },
  { id: 120, name: 'Washington Nationals' },
  { id: 121, name: 'New York Mets' },
  { id: 133, name: 'Oakland Athletics' },
  { id: 134, name: 'Pittsburgh Pirates' },
  { id: 135, name: 'San Diego Padres' },
  { id: 136, name: 'Seattle Mariners' },
  { id: 137, name: 'San Francisco Giants' },
  { id: 138, name: 'St. Louis Cardinals' },
  { id: 139, name: 'Tampa Bay Rays' },
  { id: 140, name: 'Texas Rangers' },
  { id: 141, name: 'Toronto Blue Jays' },
  { id: 142, name: 'Minnesota Twins' },
  { id: 143, name: 'Philadelphia Phillies' },
  { id: 144, name: 'Atlanta Braves' },
  { id: 145, name: 'Chicago White Sox' },
  { id: 146, name: 'Miami Marlins' },
  { id: 147, name: 'New York Yankees' },
  { id: 158, name: 'Milwaukee Brewers' },
]

const s = {
  row: { display: 'flex', gap: '12px', marginBottom: '28px', alignItems: 'center' },
  select: {
    flex: 1, background: '#161b22', border: '1px solid #30363d', color: '#e6edf3',
    borderRadius: '6px', padding: '10px 14px', fontSize: '14px', outline: 'none',
  },
  btn: {
    background: '#238636', color: '#fff', border: 'none', borderRadius: '6px',
    padding: '10px 20px', fontSize: '14px', fontWeight: '600', cursor: 'pointer',
  },
  splitGrid: { display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '16px' },
  splitCard: { background: '#161b22', border: '1px solid #30363d', borderRadius: '8px', padding: '16px' },
  splitTitle: { fontSize: '14px', fontWeight: '600', color: '#58a6ff', marginBottom: '12px' },
  splitRow: { display: 'flex', justifyContent: 'space-between', padding: '6px 0', borderBottom: '1px solid #21262d', fontSize: '13px' },
  splitKey: { color: '#8b949e' },
  splitVal: { color: '#e6edf3', fontWeight: '500' },
  loader: { color: '#8b949e', padding: '48px', textAlign: 'center' },
  error: { color: '#f85149', padding: '24px', background: '#1f1116', borderRadius: '8px' },
  hint: { color: '#8b949e', textAlign: 'center', padding: '48px' },
}

function fmt(val, d = 3) {
  if (val == null) return '—'
  return typeof val === 'number' ? val.toFixed(d) : val
}
function pct(val) {
  if (val == null) return '—'
  return `${(val * 100).toFixed(1)}%`
}

function SplitCard({ title, split }) {
  if (!split) return (
    <div style={s.splitCard}>
      <div style={s.splitTitle}>{title}</div>
      <div style={{ color: '#8b949e', fontSize: '13px' }}>No data in database yet. Run the ETL to populate.</div>
    </div>
  )
  const rows = [
    ['PA', split.pa],
    ['AVG', fmt(split.batting_avg)],
    ['OBP', fmt(split.on_base_pct)],
    ['SLG', fmt(split.slugging_pct)],
    ['HR', split.home_runs],
    ['K%', pct(split.k_pct)],
    ['BB%', pct(split.bb_pct)],
  ]
  return (
    <div style={s.splitCard}>
      <div style={s.splitTitle}>{title}</div>
      {rows.map(([k, v]) => (
        <div key={k} style={s.splitRow}>
          <span style={s.splitKey}>{k}</span>
          <span style={s.splitVal}>{v ?? '—'}</span>
        </div>
      ))}
    </div>
  )
}

export default function TeamPage() {
  const currentYear = new Date().getFullYear()
  const [teamId, setTeamId] = useState(147)
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  function load() {
    setLoading(true)
    setError(null)
    // Team splits live in /matchups context — we fetch directly from the MLB Stats API
    // via our backend. For now fetch today's matchups and find the team.
    const today = new Date().toISOString().slice(0, 10)
    fetch(`${API}/matchups?date=${today}`)
      .then(r => r.ok ? r.json() : Promise.reject(r.statusText))
      .then(matchups => {
        // Find a game that has this team and surface its split data
        const game = matchups.find(m => m.home_team_id === teamId || m.away_team_id === teamId)
        setData(game || null)
        setLoading(false)
      })
      .catch(e => { setError(String(e)); setLoading(false) })
  }

  const teamName = TEAMS.find(t => t.id === teamId)?.name || `Team ${teamId}`

  return (
    <div>
      <h1 style={{ fontSize: '24px', fontWeight: '700', marginBottom: '20px' }}>Team Splits</h1>

      <div style={s.row}>
        <select style={s.select} value={teamId} onChange={e => setTeamId(Number(e.target.value))}>
          {TEAMS.map(t => <option key={t.id} value={t.id}>{t.name}</option>)}
        </select>
        <button style={s.btn} onClick={load}>Load</button>
      </div>

      {loading && <div style={s.loader}>Loading…</div>}
      {error && <div style={s.error}>{error}</div>}

      {!loading && !error && data && (
        <div>
          <h2 style={{ fontSize: '18px', fontWeight: '600', marginBottom: '16px', color: '#8b949e' }}>
            {teamName} — Today's Game Context
          </h2>
          <div style={{ background: '#161b22', border: '1px solid #30363d', borderRadius: '8px', padding: '16px', marginBottom: '16px', fontSize: '14px', color: '#8b949e' }}>
            Team splits are populated via the ETL pipeline. Run <code style={{ background: '#21262d', padding: '2px 6px', borderRadius: '4px', color: '#e6edf3' }}>python seed_db.py</code> to load this season's data.
          </div>
          <div style={s.splitGrid}>
            <SplitCard title="vs Left-Handed Pitchers" split={null} />
            <SplitCard title="vs Right-Handed Pitchers" split={null} />
          </div>
        </div>
      )}

      {!loading && !error && !data && (
        <div style={s.hint}>
          Select a team and click Load to view their splits.<br />
          <span style={{ fontSize: '13px' }}>Data requires the ETL pipeline to have run for this season.</span>
        </div>
      )}
    </div>
  )
}
