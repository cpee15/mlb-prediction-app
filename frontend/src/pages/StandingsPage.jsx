import React, { useState, useEffect } from 'react'
import { Link } from 'react-router-dom'
import { API_BASE } from '../lib/api'

const API = API_BASE

const DIVISIONS = {
  201: 'AL East', 202: 'AL Central', 200: 'AL West',
  204: 'NL East', 205: 'NL Central', 203: 'NL West',
}
const DIVISION_ORDER = [201, 202, 200, 204, 205, 203]

function useIsMobile(breakpoint = 768) {
  const getMatches = () => (typeof window !== 'undefined' ? window.innerWidth <= breakpoint : false)
  const [isMobile, setIsMobile] = useState(getMatches)

  useEffect(() => {
    if (typeof window === 'undefined') return undefined
    const onResize = () => setIsMobile(getMatches())
    window.addEventListener('resize', onResize)
    return () => window.removeEventListener('resize', onResize)
  }, [breakpoint])

  return isMobile
}

const s = {
  page: { minWidth: 0 },
  title: { fontSize: '24px', fontWeight: '700', color: '#e6edf3', marginBottom: '24px', overflowWrap: 'anywhere' },
  grid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(min(340px, 100%), 1fr))', gap: '20px', minWidth: 0 },
  gridMobile: { gridTemplateColumns: '1fr', gap: '14px' },
  divCard: { background: '#161b22', border: '1px solid #30363d', borderRadius: '10px', overflow: 'hidden', minWidth: 0 },
  divHeader: { padding: '12px 16px', borderBottom: '1px solid #21262d', fontSize: '13px', fontWeight: '700', color: '#58a6ff', textTransform: 'uppercase', letterSpacing: '0.5px', overflowWrap: 'anywhere' },
  tableWrap: { width: '100%', maxWidth: '100%', overflowX: 'auto', WebkitOverflowScrolling: 'touch' },
  table: { width: '100%', minWidth: '520px', borderCollapse: 'collapse', fontSize: '13px' },
  tableMobile: { minWidth: '500px', fontSize: '12px' },
  th: { padding: '8px 12px', textAlign: 'right', color: '#8b949e', fontSize: '11px', textTransform: 'uppercase', letterSpacing: '0.4px', borderBottom: '1px solid #21262d', whiteSpace: 'nowrap' },
  thMobile: { padding: '8px 9px' },
  thLeft: { textAlign: 'left' },
  td: { padding: '9px 12px', borderBottom: '1px solid #0d1117', color: '#e6edf3', textAlign: 'right', whiteSpace: 'nowrap' },
  tdMobile: { padding: '9px 9px' },
  tdLeft: { textAlign: 'left' },
  tdTeam: { display: 'flex', alignItems: 'center', gap: '8px', minWidth: 0 },
  rank: { color: '#8b949e', width: '18px', flexShrink: 0, fontSize: '12px' },
  teamLink: { color: '#e6edf3', textDecoration: 'none', minWidth: 0, overflowWrap: 'anywhere', whiteSpace: 'normal', lineHeight: 1.2 },
  streak: (s) => ({
    display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
    fontSize: '11px', fontWeight: '600', padding: '1px 5px', borderRadius: '3px',
    background: s?.startsWith('W') ? '#1f3a1f' : '#3a1f1f',
    color: s?.startsWith('W') ? '#3fb950' : '#f85149',
    whiteSpace: 'nowrap',
  }),
  loader: { color: '#8b949e', padding: '48px', textAlign: 'center' },
  error: { color: '#f85149', padding: '24px', background: '#1f1116', borderRadius: '8px', overflowWrap: 'anywhere' },
  seasonRow: { display: 'flex', gap: '12px', alignItems: 'center', marginBottom: '20px', flexWrap: 'wrap', minWidth: 0 },
  select: {
    background: '#161b22', border: '1px solid #30363d', color: '#e6edf3',
    borderRadius: '6px', padding: '8px 12px', fontSize: '14px', maxWidth: '100%',
  },
  mobileHint: { color: '#8b949e', fontSize: '12px', margin: '-8px 0 14px' },
}

function pct(wins, losses) {
  const total = wins + losses
  if (total === 0) return '.000'
  return (wins / total).toFixed(3).replace(/^0/, '')
}

function parseStreak(teamRecord) {
  const streak = teamRecord?.streak?.streakCode
  return streak || null
}

function parseLast10(teamRecord) {
  const last10 = teamRecord?.records?.splitRecords?.find(r => r.type === 'lastTen')
  if (!last10) return null
  return `${last10.wins}-${last10.losses}`
}

export default function StandingsPage() {
  const isMobile = useIsMobile()
  const currentYear = new Date().getFullYear()
  const [season, setSeason] = useState(currentYear)
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    setLoading(true)
    setError(null)
    fetch(`${API}/standings?season=${season}`)
      .then(r => r.ok ? r.json() : r.json().then(e => Promise.reject(e.detail || r.statusText)))
      .then(d => { setData(d); setLoading(false) })
      .catch(e => { setError(String(e)); setLoading(false) })
  }, [season])

  if (loading) return <div style={s.loader}>Loading standings…</div>
  if (error) return <div style={s.error}>Error: {error}</div>

  // Build division map
  const divMap = {}
  ;(data || []).forEach(record => {
    const divId = record.division?.id
    if (!divMap[divId]) divMap[divId] = []
    ;(record.teamRecords || []).forEach(tr => divMap[divId].push(tr))
  })

  return (
    <div style={s.page}>
      <div style={s.title}>MLB Standings</div>

      <div style={s.seasonRow}>
        <select style={s.select} value={season} onChange={e => setSeason(Number(e.target.value))}>
          {[currentYear, currentYear - 1, currentYear - 2, currentYear - 3].map(y => (
            <option key={y} value={y}>{y}</option>
          ))}
        </select>
      </div>

      {isMobile && <div style={s.mobileHint}>Swipe inside a division table to see all standings columns.</div>}

      <div style={{ ...s.grid, ...(isMobile ? s.gridMobile : {}) }}>
        {DIVISION_ORDER.map(divId => {
          const teams = divMap[divId] || []
          const divName = DIVISIONS[divId] || `Division ${divId}`
          if (teams.length === 0) return null

          return (
            <div key={divId} style={s.divCard}>
              <div style={s.divHeader}>{divName}</div>
              <div style={s.tableWrap}>
                <table style={{ ...s.table, ...(isMobile ? s.tableMobile : {}) }}>
                  <thead>
                    <tr>
                      <th style={{ ...s.th, ...(isMobile ? s.thMobile : {}), ...s.thLeft }}>Team</th>
                      <th style={{ ...s.th, ...(isMobile ? s.thMobile : {}) }}>W</th>
                      <th style={{ ...s.th, ...(isMobile ? s.thMobile : {}) }}>L</th>
                      <th style={{ ...s.th, ...(isMobile ? s.thMobile : {}) }}>PCT</th>
                      <th style={{ ...s.th, ...(isMobile ? s.thMobile : {}) }}>GB</th>
                      <th style={{ ...s.th, ...(isMobile ? s.thMobile : {}) }}>L10</th>
                      <th style={{ ...s.th, ...(isMobile ? s.thMobile : {}) }}>Str</th>
                    </tr>
                  </thead>
                  <tbody>
                    {teams.map((tr, i) => {
                      const team = tr.team || {}
                      const wins = tr.wins || 0
                      const losses = tr.losses || 0
                      const gb = tr.gamesBack === '0' ? '—' : tr.gamesBack
                      const streak = parseStreak(tr)
                      const last10 = parseLast10(tr)

                      return (
                        <tr key={i}>
                          <td style={{ ...s.td, ...(isMobile ? s.tdMobile : {}), ...s.tdLeft }}>
                            <div style={s.tdTeam}>
                              <span style={s.rank}>{i + 1}</span>
                              <Link to={`/team/${team.id}`} style={s.teamLink}>
                                {team.name || `Team ${team.id}`}
                              </Link>
                            </div>
                          </td>
                          <td style={{ ...s.td, ...(isMobile ? s.tdMobile : {}) }}>{wins}</td>
                          <td style={{ ...s.td, ...(isMobile ? s.tdMobile : {}) }}>{losses}</td>
                          <td style={{ ...s.td, ...(isMobile ? s.tdMobile : {}), fontWeight: '600' }}>{pct(wins, losses)}</td>
                          <td style={{ ...s.td, ...(isMobile ? s.tdMobile : {}), color: '#8b949e' }}>{gb}</td>
                          <td style={{ ...s.td, ...(isMobile ? s.tdMobile : {}), color: '#8b949e' }}>{last10 || '—'}</td>
                          <td style={{ ...s.td, ...(isMobile ? s.tdMobile : {}) }}>
                            {streak ? <span style={s.streak(streak)}>{streak}</span> : '—'}
                          </td>
                        </tr>
                      )
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
}
