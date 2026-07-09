import React, { useEffect, useMemo, useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import { API_BASE, fetchJson, getMlbLiveDate } from '../lib/api'
import { fmtPct, fmtDec } from '../utils/formatters'

const API = API_BASE
const DETAIL_TTL_SECONDS = 30 * 60
const SHELL_TTL_SECONDS = 10 * 60
const PROJECTION_TTL_SECONDS = 10 * 60
const COMPETITIVE_TTL_SECONDS = 30 * 60

const t = {
  page: { width: '100%', maxWidth: '100%', overflowX: 'hidden' },
  back: { color: '#58a6ff', textDecoration: 'none', fontSize: 13, display: 'inline-block', marginBottom: 20 },
  panel: { background: '#161b22', border: '1px solid #30363d', borderRadius: 10, padding: '20px 24px', marginBottom: 20 },
  muted: { color: '#8b949e' },
  title: { fontSize: 20, fontWeight: 800, color: '#e6edf3' },
  small: { fontSize: 12, color: '#8b949e' },
  teams: { display: 'flex', alignItems: 'center', gap: 16, flexWrap: 'wrap' },
  team: { textAlign: 'center' },
  at: { color: '#8b949e', fontWeight: 700, fontSize: 20 },
  chip: { display: 'inline-flex', alignItems: 'center', gap: 6, background: '#21262d', color: '#8b949e', border: '1px solid #30363d', borderRadius: 999, padding: '4px 9px', fontSize: 11, fontWeight: 700 },
  goodChip: { background: '#102b1b', color: '#3fb950', border: '1px solid #238636' },
  warnChip: { background: '#2d2308', color: '#d29922', border: '1px solid #5f4700' },
  tabs: { display: 'flex', gap: 0, marginBottom: 16, background: '#0d1117', border: '1px solid #21262d', borderRadius: 6, overflow: 'hidden', width: 'fit-content' },
  tab: active => ({ padding: '8px 16px', fontSize: 13, fontWeight: 700, cursor: 'pointer', background: active ? '#58a6ff' : 'transparent', color: active ? '#0d1117' : '#8b949e', border: 'none' }),
  grid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(min(100%, 420px), 1fr))', gap: 16 },
  row: { display: 'flex', justifyContent: 'space-between', gap: 12, padding: '7px 0', borderBottom: '1px solid #21262d', color: '#e6edf3', fontSize: 13 },
  batterCard: { background: '#0d1117', border: '1px solid #21262d', borderRadius: 10, marginBottom: 10, overflow: 'hidden' },
  batterHeader: { display: 'flex', justifyContent: 'space-between', gap: 12, alignItems: 'center', padding: '11px 14px', cursor: 'pointer' },
  matrixGrid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(230px, 1fr))', gap: 10, padding: '0 14px 14px' },
  pitchCard: edge => ({ background: '#101820', border: `1px solid ${edge > 0.15 ? '#238636' : edge < -0.15 ? '#da3633' : '#30363d'}`, borderRadius: 10, padding: 12 }),
  skeleton: { background: 'linear-gradient(90deg,#161b22,#21262d,#161b22)', borderRadius: 8, minHeight: 18, opacity: 0.75 },
  error: { color: '#f85149', background: '#1f1116', border: '1px solid #3a1f1f', borderRadius: 8, padding: 12, marginBottom: 16 },
}

const pct = (v, d = 1) => fmtPct(v, d)
const dec = (v, d = 3) => fmtDec(v, d)

function normalizeGamePk(value) {
  return String(value || '')
}

function findScheduleGame(calendar, gamePk) {
  const wanted = normalizeGamePk(gamePk)
  for (const bucket of ['today', 'tomorrow', 'yesterday']) {
    const games = calendar?.[bucket]?.games || []
    const found = games.find(g => normalizeGamePk(g.game_pk || g.gamePk || g.id) === wanted)
    if (found) return { ...found, bucket }
  }
  return null
}

function findProjectionGame(payload, gamePk) {
  const wanted = normalizeGamePk(gamePk)
  const games = Array.isArray(payload?.games) ? payload.games : []
  return games.find(g => normalizeGamePk(g.game_pk || g.gamePk || g.id) === wanted) || null
}

function buildShell({ scheduleGame, projectionGame, detail, gamePk }) {
  const away = detail?.away_team || projectionGame?.away_team || scheduleGame?.away_team || {}
  const home = detail?.home_team || projectionGame?.home_team || scheduleGame?.home_team || {}
  const probability = projectionGame?.model_projection_probability || projectionGame?.probability || detail?.model_projection_probability || detail?.probability || null
  const gameDate = detail?.game_date || projectionGame?.game_date || scheduleGame?.game_date || scheduleGame?.start_time || scheduleGame?.game_time || null
  return {
    game_pk: gamePk,
    game_date: gameDate,
    status: detail?.status || projectionGame?.status || scheduleGame?.status || scheduleGame?.game_status || 'Scheduled',
    venue: detail?.venue || projectionGame?.venue || scheduleGame?.venue || scheduleGame?.ballpark || null,
    weather: detail?.weather || projectionGame?.weather || null,
    away: {
      name: away.name || away.team_name || away.abbrev || scheduleGame?.away_name || scheduleGame?.away_team_name || 'Away',
      record: away.record || away.team_record || scheduleGame?.away_record || '',
      pitcher_name: away.pitcher_name || away.probable_pitcher || scheduleGame?.away_pitcher_name || scheduleGame?.away_probable_pitcher || 'TBD',
      pitcher_id: away.pitcher_id || away.probable_pitcher_id || scheduleGame?.away_pitcher_id || null,
      lineup_source: away.lineup_source || projectionGame?.away_lineup_source || detail?.away_lineup_source || null,
      lineup: away.lineup || [],
    },
    home: {
      name: home.name || home.team_name || home.abbrev || scheduleGame?.home_name || scheduleGame?.home_team_name || 'Home',
      record: home.record || home.team_record || scheduleGame?.home_record || '',
      pitcher_name: home.pitcher_name || home.probable_pitcher || scheduleGame?.home_pitcher_name || scheduleGame?.home_probable_pitcher || 'TBD',
      pitcher_id: home.pitcher_id || home.probable_pitcher_id || scheduleGame?.home_pitcher_id || null,
      lineup_source: home.lineup_source || projectionGame?.home_lineup_source || detail?.home_lineup_source || null,
      lineup: home.lineup || [],
    },
    home_win_prob: probability?.home_win_prob ?? probability?.home_win_probability ?? projectionGame?.home_win_prob ?? detail?.home_win_prob ?? null,
    away_win_prob: probability?.away_win_prob ?? probability?.away_win_probability ?? projectionGame?.away_win_prob ?? detail?.away_win_prob ?? null,
    probability_source: probability?.source || projectionGame?.probability_source || detail?.probability_source || null,
  }
}

function formatTime(value) {
  if (!value) return null
  try {
    return new Date(value).toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit', timeZone: 'America/New_York' }) + ' ET'
  } catch {
    return null
  }
}

function lineupLabel(source) {
  if (source === 'official') return 'Official lineup'
  if (source === 'projected') return 'Projected lineup'
  if (source === 'roster') return 'Roster fallback'
  return 'Lineup pending'
}

function bestPitchEdge(matrix) {
  if (!Array.isArray(matrix) || !matrix.length) return null
  return matrix.reduce((best, row) => !best || Number(row.edge_score ?? -999) > Number(best.edge_score ?? -999) ? row : best, null)
}

function PitchMetric({ label, value }) {
  return (
    <div style={{ background: '#0a0f14', border: '1px solid #21262d', borderRadius: 8, padding: 8 }}>
      <div style={{ ...t.small, textTransform: 'uppercase', marginBottom: 4 }}>{label}</div>
      <div style={{ color: '#e6edf3', fontWeight: 800 }}>{value ?? '—'}</div>
    </div>
  )
}

function PitchCard({ pitch }) {
  const bvt = pitch?.batter_vs_type || {}
  const edge = Number(pitch?.edge_score || 0)
  return (
    <div style={t.pitchCard(edge)}>
      <div style={{ display: 'flex', justifyContent: 'space-between', gap: 8, marginBottom: 10 }}>
        <div>
          <div style={{ color: '#e6edf3', fontWeight: 900, fontSize: 16 }}>{pitch?.pitch_type || '—'}</div>
          <div style={t.small}>Usage {pct(pitch?.pitcher_usage_pct)}</div>
        </div>
        <div style={{ color: edge > 0.15 ? '#3fb950' : edge < -0.15 ? '#f85149' : '#8b949e', fontWeight: 900 }}>
          {edge > 0 ? '+' : ''}{edge.toFixed(2)}
        </div>
      </div>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, minmax(0, 1fr))', gap: 8 }}>
        <PitchMetric label="xwOBA" value={dec(bvt.xwoba)} />
        <PitchMetric label="AVG" value={dec(bvt.batting_avg ?? bvt.avg)} />
        <PitchMetric label="Whiff" value={pct(bvt.whiff_pct)} />
        <PitchMetric label="HardHit" value={pct(bvt.hard_hit_pct ?? bvt.hardhit_pct)} />
      </div>
    </div>
  )
}

function BatterRow({ batter, expanded, onToggle }) {
  const matchup = batter?.matchup || {}
  const matrix = matchup.pitch_type_matrix || []
  const best = bestPitchEdge(matrix)
  return (
    <div style={t.batterCard}>
      <div style={t.batterHeader} onClick={onToggle}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          <span style={t.small}>{batter?.batting_order || '—'}</span>
          <span style={{ color: '#e6edf3', fontWeight: 800 }}>{batter?.batter_name || 'Unknown batter'}</span>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          {best && <span style={{ ...t.chip, ...(Number(best.edge_score || 0) > 0.15 ? t.goodChip : {}) }}>{best.pitch_type}: {Number(best.edge_score || 0) > 0 ? '+' : ''}{Number(best.edge_score || 0).toFixed(2)}</span>}
          <span style={t.small}>{expanded ? '▼' : '▶'}</span>
        </div>
      </div>
      {expanded && (
        <div style={t.matrixGrid}>
          {matrix.length ? matrix.map((pitch, idx) => <PitchCard key={`${pitch.pitch_type || 'p'}-${idx}`} pitch={pitch} />) : <div style={t.small}>No arsenal matchup data available yet.</div>}
        </div>
      )}
    </div>
  )
}

function SkeletonRows() {
  return (
    <div style={t.panel}>
      <div style={{ ...t.skeleton, width: '45%', height: 20, marginBottom: 16 }} />
      {[1, 2, 3, 4, 5, 6].map(i => <div key={i} style={{ ...t.skeleton, height: 42, marginBottom: 10 }} />)}
    </div>
  )
}

function Header({ shell, shellLoading }) {
  const hp = shell?.home_win_prob
  const ap = shell?.away_win_prob
  const hPct = hp != null ? Math.round(Number(hp) * 100) : null
  const aPct = ap != null ? Math.round(Number(ap) * 100) : null
  return (
    <div style={t.panel}>
      <div style={{ display: 'flex', justifyContent: 'space-between', gap: 16, flexWrap: 'wrap', alignItems: 'flex-start' }}>
        <div style={t.teams}>
          <div style={t.team}>
            <div style={t.title}>{shell?.away?.name || (shellLoading ? 'Away loading…' : 'Away')}</div>
            <div style={t.small}>{shell?.away?.record || shell?.away?.pitcher_name || ''}</div>
          </div>
          <div style={t.at}>@</div>
          <div style={t.team}>
            <div style={t.title}>{shell?.home?.name || (shellLoading ? 'Home loading…' : 'Home')}</div>
            <div style={t.small}>{shell?.home?.record || shell?.home?.pitcher_name || ''}</div>
          </div>
        </div>
        <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-end', gap: 8 }}>
          <span style={{ ...t.chip, ...(shell?.probability_source === 'model_projections' ? t.goodChip : {}) }}>{shell?.probability_source || 'Probability pending'}</span>
          {shell?.status && <span style={t.chip}>{shell.status}</span>}
        </div>
      </div>
      <div style={{ marginTop: 12, display: 'flex', gap: 12, flexWrap: 'wrap' }}>
        {shell?.venue && <span style={t.small}>📍 {shell.venue}</span>}
        {formatTime(shell?.game_date) && <span style={t.small}>🕐 {formatTime(shell.game_date)}</span>}
        {shell?.away?.pitcher_name && <span style={t.small}>Away SP: {shell.away.pitcher_name}</span>}
        {shell?.home?.pitcher_name && <span style={t.small}>Home SP: {shell.home.pitcher_name}</span>}
      </div>
      {(hPct != null || aPct != null) && (
        <div style={{ marginTop: 16 }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8 }}>
            <strong style={{ color: '#58a6ff' }}>{shell?.away?.name}: {aPct ?? '—'}%</strong>
            <strong style={{ color: '#3fb950' }}>{shell?.home?.name}: {hPct ?? '—'}%</strong>
          </div>
          <div style={{ height: 10, borderRadius: 999, background: '#21262d', overflow: 'hidden', display: 'flex' }}>
            <div style={{ width: `${aPct ?? 50}%`, background: '#58a6ff' }} />
            <div style={{ width: `${hPct ?? 50}%`, background: '#3fb950' }} />
          </div>
        </div>
      )}
    </div>
  )
}

export default function MatchupDetailShellFirstPage() {
  const { game_pk } = useParams()
  const [scheduleGame, setScheduleGame] = useState(null)
  const [projectionGame, setProjectionGame] = useState(null)
  const [detail, setDetail] = useState(null)
  const [competitive, setCompetitive] = useState(null)
  const [activeTab, setActiveTab] = useState('competitive')
  const [expanded, setExpanded] = useState({})
  const [shellLoading, setShellLoading] = useState(true)
  const [competitiveLoading, setCompetitiveLoading] = useState(true)
  const [detailLoading, setDetailLoading] = useState(false)
  const [error, setError] = useState(null)

  const shell = useMemo(() => buildShell({ scheduleGame, projectionGame, detail, gamePk: game_pk }), [scheduleGame, projectionGame, detail, game_pk])

  useEffect(() => {
    let cancelled = false
    setShellLoading(true)
    setCompetitiveLoading(true)
    setDetailLoading(true)
    setError(null)
    setScheduleGame(null)
    setProjectionGame(null)
    setDetail(null)
    setCompetitive(null)
    setExpanded({})

    async function loadShell() {
      try {
        const calendar = await fetchJson(`${API}/matchups/calendar/schedule`, { ttlSeconds: SHELL_TTL_SECONDS })
        if (cancelled) return null
        const sched = findScheduleGame(calendar, game_pk)
        setScheduleGame(sched)
        const date = sched?.game_date?.slice?.(0, 10) || sched?.start_time?.slice?.(0, 10) || sched?.date || getMlbLiveDate()
        fetchJson(`${API}/models/projections?date=${date}`, { ttlSeconds: PROJECTION_TTL_SECONDS })
          .then(payload => {
            if (!cancelled) setProjectionGame(findProjectionGame(payload, game_pk))
          })
          .catch(() => {})
        return sched
      } catch (exc) {
        if (!cancelled) setError(String(exc))
        return null
      } finally {
        if (!cancelled) setShellLoading(false)
      }
    }

    loadShell()

    fetchJson(`${API}/matchup/${game_pk}/competitive`, { ttlSeconds: COMPETITIVE_TTL_SECONDS })
      .then(payload => {
        if (!cancelled) setCompetitive(payload)
      })
      .catch(() => {
        if (!cancelled) setCompetitive(null)
      })
      .finally(() => {
        if (!cancelled) setCompetitiveLoading(false)
      })

    window.setTimeout(() => {
      if (cancelled) return
      fetchJson(`${API}/matchup/${game_pk}`, { ttlSeconds: DETAIL_TTL_SECONDS })
        .then(payload => {
          if (!cancelled) setDetail(payload)
        })
        .catch(() => {})
        .finally(() => {
          if (!cancelled) setDetailLoading(false)
        })
    }, 0)

    return () => { cancelled = true }
  }, [game_pk])

  const awayRows = competitive?.away_lineup_matchups || []
  const homeRows = competitive?.home_lineup_matchups || []
  const hasCompetitive = awayRows.length > 0 || homeRows.length > 0

  function toggle(key) {
    setExpanded(prev => ({ ...prev, [key]: !prev[key] }))
  }

  return (
    <div style={t.page}>
      <Link to="/" style={t.back}>← Back to Matchups</Link>
      {error && !scheduleGame && !detail && <div style={t.error}>{error}</div>}
      <Header shell={shell} shellLoading={shellLoading} />

      <div style={t.tabs}>
        <button style={t.tab(activeTab === 'competitive')} onClick={() => setActiveTab('competitive')}>Batter vs Arsenal</button>
        <button style={t.tab(activeTab === 'overview')} onClick={() => setActiveTab('overview')}>Overview</button>
      </div>

      {activeTab === 'competitive' && (
        <>
          {competitiveLoading && !hasCompetitive ? <SkeletonRows /> : (
            <div style={t.panel}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 12, marginBottom: 16, flexWrap: 'wrap' }}>
                <div>
                  <div style={t.title}>Batter vs Arsenal</div>
                  <div style={t.small}>Default view. Competitive matrix hydrates independently from the shell.</div>
                </div>
                <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
                  <span style={{ ...t.chip, ...(competitiveLoading ? t.warnChip : t.goodChip) }}>{competitiveLoading ? 'Hydrating' : 'Loaded'}</span>
                  <span style={t.chip}>Away: {competitive?.away_lineup_source || lineupLabel(shell?.away?.lineup_source)}</span>
                  <span style={t.chip}>Home: {competitive?.home_lineup_source || lineupLabel(shell?.home?.lineup_source)}</span>
                </div>
              </div>

              {!hasCompetitive && !competitiveLoading && <div style={t.small}>No competitive arsenal matrix returned yet for this matchup.</div>}

              <div style={t.grid}>
                <div>
                  <h3 style={{ color: '#58a6ff', marginTop: 0 }}>{shell?.away?.name || 'Away'}</h3>
                  {awayRows.map((batter, idx) => <BatterRow key={`away-${batter.batter_id || idx}`} batter={batter} expanded={!!expanded[`away-${idx}`]} onToggle={() => toggle(`away-${idx}`)} />)}
                </div>
                <div>
                  <h3 style={{ color: '#3fb950', marginTop: 0 }}>{shell?.home?.name || 'Home'}</h3>
                  {homeRows.map((batter, idx) => <BatterRow key={`home-${batter.batter_id || idx}`} batter={batter} expanded={!!expanded[`home-${idx}`]} onToggle={() => toggle(`home-${idx}`)} />)}
                </div>
              </div>
            </div>
          )}
        </>
      )}

      {activeTab === 'overview' && (
        <div style={t.panel}>
          <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12, alignItems: 'center', marginBottom: 16 }}>
            <div style={t.title}>Overview</div>
            <span style={{ ...t.chip, ...(detailLoading ? t.warnChip : t.goodChip) }}>{detailLoading ? 'Loading detail' : detail ? 'Detail loaded' : 'Shell only'}</span>
          </div>
          <div style={t.grid}>
            <div>
              <h3 style={{ color: '#58a6ff' }}>{shell?.away?.name}</h3>
              <div style={t.row}><span>Pitcher</span><strong>{shell?.away?.pitcher_name || 'TBD'}</strong></div>
              <div style={t.row}><span>Lineup</span><strong>{lineupLabel(shell?.away?.lineup_source)}</strong></div>
              <div style={t.row}><span>Win Prob</span><strong>{shell?.away_win_prob != null ? pct(shell.away_win_prob, 0) : 'Pending'}</strong></div>
            </div>
            <div>
              <h3 style={{ color: '#3fb950' }}>{shell?.home?.name}</h3>
              <div style={t.row}><span>Pitcher</span><strong>{shell?.home?.pitcher_name || 'TBD'}</strong></div>
              <div style={t.row}><span>Lineup</span><strong>{lineupLabel(shell?.home?.lineup_source)}</strong></div>
              <div style={t.row}><span>Win Prob</span><strong>{shell?.home_win_prob != null ? pct(shell.home_win_prob, 0) : 'Pending'}</strong></div>
            </div>
          </div>
          {detail && (
            <div style={{ marginTop: 16 }}>
              <div style={t.small}>Full matchup detail is loaded in the background and cached for repeat visits.</div>
            </div>
          )}
        </div>
      )}
    </div>
  )
}
