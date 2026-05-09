import React, { useState, useEffect } from 'react'
import { useParams, useNavigate, Link } from 'react-router-dom'
import {
  LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip,
  Legend, ResponsiveContainer, ReferenceLine, Cell,
} from 'recharts'

const API = import.meta.env.VITE_API_BASE_URL || ''

const C = {
  green: '#3fb950', blue: '#58a6ff', orange: '#d29922', red: '#f85149',
  text: '#e6edf3', muted: '#8b949e', bg: '#0d1117', card: '#161b22',
  border: '#30363d', elevated: '#21262d',
}

const s = {
  searchRow: { display: 'flex', gap: '12px', marginBottom: '28px' },
  input: {
    flex: 1, background: C.card, border: `1px solid ${C.border}`, color: C.text,
    borderRadius: '6px', padding: '10px 14px', fontSize: '14px', outline: 'none',
  },
  playerHeader: {
    background: C.card, border: `1px solid ${C.border}`, borderRadius: '10px',
    padding: '20px 24px', marginBottom: '20px',
  },
  playerName: { fontSize: '26px', fontWeight: '700', color: C.text, marginBottom: '6px' },
  playerMeta: { display: 'flex', gap: '16px', flexWrap: 'wrap', fontSize: '13px', color: C.muted },
  metaChip: { background: C.elevated, borderRadius: '4px', padding: '2px 8px', color: C.text },
  rollingLink: {
    display: 'inline-block', background: C.elevated, border: `1px solid ${C.border}`,
    color: C.blue, textDecoration: 'none', borderRadius: '6px',
    padding: '7px 16px', fontSize: '13px', fontWeight: '500', marginBottom: '24px',
  },
  qaBox: { background: C.card, border: `1px solid ${C.border}`, borderRadius: '8px', padding: '12px 14px', marginBottom: '20px', fontSize: '13px', color: C.muted },
  qaWarn: { color: C.orange, marginTop: '6px' },
  section: { marginBottom: '32px' },
  sectionTitle: { fontSize: '16px', fontWeight: '600', color: C.text, marginBottom: '14px', borderBottom: `1px solid ${C.elevated}`, paddingBottom: '8px' },
  statsGrid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(140px, 1fr))', gap: '12px' },
  statCard: { background: C.card, border: `1px solid ${C.border}`, borderRadius: '8px', padding: '14px 16px' },
  statLabel: { fontSize: '11px', color: C.muted, marginBottom: '6px', textTransform: 'uppercase', letterSpacing: '0.5px' },
  statVal: { fontSize: '22px', fontWeight: '700', color: C.text },
  tableWrap: { background: C.card, border: `1px solid ${C.border}`, borderRadius: '10px', overflow: 'auto' },
  table: { width: '100%', borderCollapse: 'collapse', fontSize: '13px' },
  th: { padding: '10px 14px', textAlign: 'left', color: C.muted, fontWeight: '500', fontSize: '11px', textTransform: 'uppercase', letterSpacing: '0.4px', borderBottom: `1px solid ${C.elevated}`, whiteSpace: 'nowrap' },
  thR: { textAlign: 'right' },
  td: { padding: '10px 14px', borderBottom: `1px solid ${C.bg}`, color: C.text, whiteSpace: 'nowrap' },
  tdR: { textAlign: 'right' },
  tdMuted: { color: C.muted },
  sourceBadge: { display: 'inline-block', fontSize: '11px', padding: '2px 7px', borderRadius: '3px', background: C.elevated, color: C.muted, marginLeft: '10px', verticalAlign: 'middle', fontWeight: '400' },
  loader: { color: C.muted, padding: '48px', textAlign: 'center' },
  error: { color: C.red, padding: '24px', background: '#1f1116', borderRadius: '8px' },
  hint: { color: C.muted, textAlign: 'center', padding: '48px' },
  chartBox: { background: C.card, border: `1px solid ${C.border}`, borderRadius: '10px', padding: '20px', marginBottom: '0' },
}

const searchDropStyle = {
  position: 'absolute', top: '100%', left: 0, right: 0, zIndex: 100,
  background: C.card, border: `1px solid ${C.border}`, borderRadius: '6px',
  marginTop: '4px', maxHeight: '280px', overflowY: 'auto',
}
const searchItemStyle = (hover) => ({
  padding: '9px 14px', cursor: 'pointer', borderBottom: `1px solid ${C.elevated}`,
  background: hover ? C.elevated : 'transparent',
  display: 'flex', justifyContent: 'space-between', alignItems: 'center',
})

const fmt = (v, d = 1) => v != null ? (typeof v === 'number' ? v.toFixed(d) : v) : '—'
const pct = (v, d = 1) => v != null ? `${(v * 100).toFixed(d)}%` : '—'
const num = (v) => v != null ? v : '—'

// League average baselines (MLB ~2024)
const LEAGUE_AVGS = {
  avg_exit_velocity: 88.5,
  hard_hit_pct: 0.38,
  barrel_pct: 0.08,
  k_pct: 0.225,
  bb_pct: 0.085,
  avg_launch_angle: 12,
}

function StatCard({ label, value }) {
  return (
    <div style={s.statCard}>
      <div style={s.statLabel}>{label}</div>
      <div style={s.statVal}>{value}</div>
    </div>
  )
}

function DataQualityBox({ quality }) {
  if (!quality) return null
  const warnings = quality.warnings || []
  return (
    <div style={s.qaBox}>
      <div>Data Quality: <strong style={{ color: C.text }}>{quality.ordering_quality || 'unknown'}</strong></div>
      <div>Latest Statcast Event: <strong style={{ color: C.text }}>{quality.latest_event_date || '—'}</strong></div>
      {warnings.map((w, i) => <div key={i} style={s.qaWarn}>⚠ {w}</div>)}
    </div>
  )
}

// Horizontal gauge bar like Baseball Savant's percentile display
function GaugeBar({ label, value, leagueAvg, max, unit = '', inverse = false, format }) {
  if (value == null) return null
  const displayVal = format ? format(value) : `${value.toFixed(1)}${unit}`
  const pctFill = Math.min(100, Math.max(0, (value / max) * 100))
  const leaguePct = Math.min(100, Math.max(0, (leagueAvg / max) * 100))
  const isGood = inverse ? value <= leagueAvg : value >= leagueAvg
  const color = isGood ? C.green : C.orange

  return (
    <div style={{ marginBottom: '18px' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '7px', fontSize: '13px' }}>
        <span style={{ color: C.muted }}>{label}</span>
        <div style={{ display: 'flex', gap: '12px', alignItems: 'center' }}>
          <span style={{ color: C.muted, fontSize: '11px' }}>Lg avg: {format ? format(leagueAvg) : `${leagueAvg}${unit}`}</span>
          <span style={{ color, fontWeight: '700' }}>{displayVal}</span>
        </div>
      </div>
      <div style={{ background: C.elevated, borderRadius: '4px', height: '10px', position: 'relative' }}>
        <div style={{ width: `${pctFill}%`, height: '100%', background: color, borderRadius: '4px', opacity: 0.85 }} />
        {/* League average tick */}
        <div style={{
          position: 'absolute', top: '-3px', left: `${leaguePct}%`,
          width: '2px', height: '16px', background: C.muted, opacity: 0.6,
          transform: 'translateX(-50%)',
        }} />
      </div>
    </div>
  )
}

function StatcastGauges({ sc, agg }) {
  const data = sc || agg
  if (!data) return null
  const ev = data.avg_exit_velocity
  const hh = data.hard_hit_pct
  const br = data.barrel_pct
  const kp = data.k_pct
  const bp = data.bb_pct

  return (
    <div style={{ ...s.chartBox, padding: '24px 28px' }}>
      <div style={{ fontSize: '13px', color: C.muted, marginBottom: '20px' }}>
        Bars show player value vs MLB average. Green = above average, orange = below average.
      </div>
      <GaugeBar
        label="Avg Exit Velocity"
        value={ev}
        leagueAvg={LEAGUE_AVGS.avg_exit_velocity}
        max={115}
        unit=" mph"
      />
      <GaugeBar
        label="Hard Hit%"
        value={hh != null ? hh * 100 : null}
        leagueAvg={LEAGUE_AVGS.hard_hit_pct * 100}
        max={80}
        unit="%"
      />
      <GaugeBar
        label="Barrel%"
        value={br != null ? br * 100 : null}
        leagueAvg={LEAGUE_AVGS.barrel_pct * 100}
        max={25}
        unit="%"
      />
      <GaugeBar
        label="K%"
        value={kp != null ? kp * 100 : null}
        leagueAvg={LEAGUE_AVGS.k_pct * 100}
        max={50}
        unit="%"
        inverse
      />
      <GaugeBar
        label="BB%"
        value={bp != null ? bp * 100 : null}
        leagueAvg={LEAGUE_AVGS.bb_pct * 100}
        max={20}
        unit="%"
      />
    </div>
  )
}

const chartTooltipStyle = {
  background: '#1c2128', border: `1px solid ${C.border}`, borderRadius: '6px', fontSize: '12px', color: C.text,
}

function SeasonTrendChart({ yby }) {
  if (!yby || yby.length < 2) return null
  const data = [...yby].sort((a, b) => a.season - b.season).map(row => ({
    season: String(row.season),
    AVG: row.batting_avg != null ? +row.batting_avg.toFixed(3) : null,
    OBP: row.on_base_pct != null ? +row.on_base_pct.toFixed(3) : null,
    SLG: row.slugging_pct != null ? +row.slugging_pct.toFixed(3) : null,
    OPS: row.ops != null ? +row.ops.toFixed(3) : null,
  }))

  return (
    <div style={s.chartBox}>
      <ResponsiveContainer width="100%" height={260}>
        <LineChart data={data} margin={{ top: 8, right: 24, left: 0, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke={C.elevated} />
          <XAxis dataKey="season" tick={{ fill: C.muted, fontSize: 12 }} axisLine={{ stroke: C.border }} tickLine={false} />
          <YAxis domain={['auto', 'auto']} tick={{ fill: C.muted, fontSize: 11 }} axisLine={false} tickLine={false} width={48} />
          <Tooltip contentStyle={chartTooltipStyle} labelStyle={{ color: C.blue, fontWeight: 600 }} />
          <Legend wrapperStyle={{ fontSize: '12px', paddingTop: '12px' }} />
          <ReferenceLine y={0.250} stroke={C.muted} strokeDasharray="4 4" opacity={0.4} />
          <Line type="monotone" dataKey="AVG" stroke={C.green} strokeWidth={2} dot={{ r: 3, fill: C.green }} activeDot={{ r: 5 }} connectNulls />
          <Line type="monotone" dataKey="OBP" stroke={C.blue} strokeWidth={2} dot={{ r: 3, fill: C.blue }} activeDot={{ r: 5 }} connectNulls />
          <Line type="monotone" dataKey="SLG" stroke={C.orange} strokeWidth={2} dot={{ r: 3, fill: C.orange }} activeDot={{ r: 5 }} connectNulls />
          <Line type="monotone" dataKey="OPS" stroke="#bc8cff" strokeWidth={2} dot={{ r: 3, fill: '#bc8cff' }} activeDot={{ r: 5 }} connectNulls />
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}

function HRTrendChart({ yby }) {
  if (!yby || yby.length < 2) return null
  const data = [...yby].sort((a, b) => a.season - b.season).map(row => ({
    season: String(row.season),
    HR: row.hr ?? 0,
    SB: row.sb ?? 0,
    RBI: row.rbi ?? 0,
  }))
  return (
    <div style={s.chartBox}>
      <ResponsiveContainer width="100%" height={220}>
        <BarChart data={data} margin={{ top: 8, right: 24, left: 0, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke={C.elevated} vertical={false} />
          <XAxis dataKey="season" tick={{ fill: C.muted, fontSize: 12 }} axisLine={{ stroke: C.border }} tickLine={false} />
          <YAxis tick={{ fill: C.muted, fontSize: 11 }} axisLine={false} tickLine={false} width={32} />
          <Tooltip contentStyle={chartTooltipStyle} labelStyle={{ color: C.blue, fontWeight: 600 }} />
          <Legend wrapperStyle={{ fontSize: '12px', paddingTop: '12px' }} />
          <Bar dataKey="HR" fill={C.blue} radius={[3, 3, 0, 0]} maxBarSize={36} />
          <Bar dataKey="SB" fill={C.green} radius={[3, 3, 0, 0]} maxBarSize={36} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}

function PlatoonChart({ splits }) {
  if (!splits?.vsL && !splits?.vsR) return null
  const vsL = splits.vsL
  const vsR = splits.vsR

  const metrics = [
    { key: 'batting_avg', label: 'AVG', scale: 1000 },
    { key: 'on_base_pct', label: 'OBP', scale: 1000 },
    { key: 'slugging_pct', label: 'SLG', scale: 1000 },
    { key: 'k_pct', label: 'K%', scale: 100, pct: true },
    { key: 'bb_pct', label: 'BB%', scale: 100, pct: true },
  ]

  const data = metrics.map(m => {
    const lv = vsL?.[m.key]
    const rv = vsR?.[m.key]
    return {
      label: m.label,
      'vs LHP': lv != null ? +(lv * m.scale).toFixed(1) : null,
      'vs RHP': rv != null ? +(rv * m.scale).toFixed(1) : null,
      _pct: m.pct,
    }
  })

  return (
    <div style={s.chartBox}>
      <div style={{ display: 'flex', gap: '16px', fontSize: '13px', color: C.muted, marginBottom: '12px', flexWrap: 'wrap' }}>
        <span>AVG/OBP/SLG shown ×1000 (e.g. 280 = .280)</span>
        <span>K%/BB% shown as percentage</span>
      </div>
      <ResponsiveContainer width="100%" height={200}>
        <BarChart data={data} margin={{ top: 4, right: 16, left: 0, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke={C.elevated} vertical={false} />
          <XAxis dataKey="label" tick={{ fill: C.muted, fontSize: 12 }} axisLine={false} tickLine={false} />
          <YAxis tick={{ fill: C.muted, fontSize: 11 }} axisLine={false} tickLine={false} width={36} />
          <Tooltip contentStyle={chartTooltipStyle} labelStyle={{ color: C.orange, fontWeight: 600 }} />
          <Legend wrapperStyle={{ fontSize: '12px', paddingTop: '8px' }} />
          <Bar dataKey="vs LHP" fill="#bc8cff" radius={[3, 3, 0, 0]} maxBarSize={40} />
          <Bar dataKey="vs RHP" fill={C.blue} radius={[3, 3, 0, 0]} maxBarSize={40} />
        </BarChart>
      </ResponsiveContainer>
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '12px', marginTop: '20px' }}>
        {[['vs LHP', vsL, '#bc8cff'], ['vs RHP', vsR, C.blue]].map(([title, split, color]) => (
          <div key={title} style={{ background: C.elevated, borderRadius: '8px', padding: '14px' }}>
            <div style={{ fontSize: '13px', fontWeight: '600', color, marginBottom: '10px' }}>{title} · {split?.pa ?? '—'} PA</div>
            {split ? [
              ['AVG', fmt(split.batting_avg, 3)],
              ['OBP', fmt(split.on_base_pct, 3)],
              ['SLG', fmt(split.slugging_pct, 3)],
              ['OPS', fmt(split.ops, 3)],
              ['K%', pct(split.k_pct)],
              ['BB%', pct(split.bb_pct)],
            ].map(([k, v]) => (
              <div key={k} style={{ display: 'flex', justifyContent: 'space-between', padding: '4px 0', fontSize: '13px', borderBottom: `1px solid ${C.border}` }}>
                <span style={{ color: C.muted }}>{k}</span>
                <span style={{ color: C.text, fontWeight: '500' }}>{v}</span>
              </div>
            )) : <div style={{ color: C.muted, fontSize: '13px' }}>No data</div>}
          </div>
        ))}
      </div>
    </div>
  )
}

function KBBRadarBars({ data: statData, label }) {
  if (!statData) return null
  const metrics = [
    { key: 'k_pct', label: 'K%', val: statData.k_pct, max: 0.45, good: (v) => v <= LEAGUE_AVGS.k_pct, format: (v) => pct(v) },
    { key: 'bb_pct', label: 'BB%', val: statData.bb_pct, max: 0.20, good: (v) => v >= LEAGUE_AVGS.bb_pct, format: (v) => pct(v) },
    { key: 'hard_hit_pct', label: 'Hard Hit%', val: statData.hard_hit_pct, max: 0.70, good: (v) => v >= LEAGUE_AVGS.hard_hit_pct, format: (v) => pct(v) },
    { key: 'barrel_pct', label: 'Barrel%', val: statData.barrel_pct, max: 0.25, good: (v) => v >= LEAGUE_AVGS.barrel_pct, format: (v) => pct(v) },
  ]
  return (
    <div>
      {metrics.map(m => {
        if (m.val == null) return null
        const fill = Math.min(100, (m.val / m.max) * 100)
        const color = m.good(m.val) ? C.green : C.orange
        return (
          <div key={m.key} style={{ marginBottom: '12px' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '13px', marginBottom: '5px' }}>
              <span style={{ color: C.muted }}>{m.label}</span>
              <span style={{ color, fontWeight: '600' }}>{m.format(m.val)}</span>
            </div>
            <div style={{ background: C.elevated, borderRadius: '4px', height: '8px' }}>
              <div style={{ width: `${fill}%`, height: '100%', background: color, borderRadius: '4px' }} />
            </div>
          </div>
        )
      })}
    </div>
  )
}

export default function BatterPage() {
  const { id } = useParams()
  const navigate = useNavigate()
  const [query, setQuery] = useState('')
  const [results, setResults] = useState([])
  const [searching, setSearching] = useState(false)
  const [hoverIdx, setHoverIdx] = useState(-1)
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const debounceRef = React.useRef(null)

  function load(pid) {
    if (!pid) return
    setLoading(true); setError(null); setResults([])
    fetch(`${API}/batter/${pid}/profile`)
      .then(r => r.ok ? r.json() : r.json().then(e => Promise.reject(e.detail || r.statusText)))
      .then(d => { setData(d); setLoading(false) })
      .catch(e => { setError(String(e)); setLoading(false); setData(null) })
  }

  useEffect(() => { if (id) load(id) }, [id])

  function onQueryChange(e) {
    const val = e.target.value
    setQuery(val); setHoverIdx(-1)
    clearTimeout(debounceRef.current)
    if (val.length < 2) { setResults([]); return }
    debounceRef.current = setTimeout(() => {
      setSearching(true)
      fetch(`${API}/players/search?name=${encodeURIComponent(val)}`)
        .then(r => r.ok ? r.json() : [])
        .then(d => { setResults((d || []).filter(p => p.position_type === 'Batter')); setSearching(false) })
        .catch(() => setSearching(false))
    }, 300)
  }

  function selectPlayer(p) {
    setQuery(p.name); setResults([])
    navigate(`/batter/${p.id}`)
  }

  const info = data?.player_info
  const ss = data?.season_stats
  const sc = data?.statcast
  const splits = data?.splits || {}
  const yby = data?.year_by_year || []
  const agg = data?.aggregate
  const dq = data?.data_quality

  return (
    <div>
      <h1 style={{ fontSize: '24px', fontWeight: '700', marginBottom: '20px' }}>Batter Profile</h1>

      <div style={{ position: 'relative', marginBottom: '28px' }}>
        <div style={s.searchRow}>
          <input
            style={s.input}
            placeholder="Search batter by name (e.g. Aaron Judge)"
            value={query}
            onChange={onQueryChange}
            autoComplete="off"
          />
          {searching && <span style={{ color: C.muted, fontSize: '13px', alignSelf: 'center' }}>Searching…</span>}
        </div>
        {results.length > 0 && (
          <div style={searchDropStyle}>
            {results.slice(0, 10).map((p, i) => (
              <div key={p.id} style={searchItemStyle(i === hoverIdx)}
                onMouseEnter={() => setHoverIdx(i)} onMouseLeave={() => setHoverIdx(-1)}
                onClick={() => selectPlayer(p)}>
                <span style={{ color: C.text }}>{p.name}</span>
                <span style={{ color: C.muted, fontSize: '12px' }}>{p.team || ''}</span>
              </div>
            ))}
          </div>
        )}
      </div>

      {loading && <div style={s.loader}>Loading…</div>}
      {error && <div style={s.error}>{error}</div>}
      {!loading && !error && !data && <div style={s.hint}>Search for a batter by name to view their stats.</div>}

      {data && (
        <>
          {info && (
            <div style={s.playerHeader}>
              <div style={s.playerName}>{info.name}</div>
              <div style={s.playerMeta}>
                {info.position && <span style={s.metaChip}>{info.position}</span>}
                {info.team && <span style={s.metaChip}>{info.team}</span>}
                {info.bats && <span>Bats: <strong style={{ color: C.text }}>{info.bats}</strong></span>}
                {info.throws && <span>Throws: <strong style={{ color: C.text }}>{info.throws}</strong></span>}
                {info.mlb_debut && <span>Debut: <strong style={{ color: C.text }}>{info.mlb_debut?.slice(0, 4)}</strong></span>}
              </div>
            </div>
          )}

          <DataQualityBox quality={dq} />

          {id && (
            <Link to={`/batter/${id}/rolling`} style={s.rollingLink}>
              View Rolling Stats (PA / AB / Games) →
            </Link>
          )}

          {ss && (
            <div style={s.section}>
              <div style={s.sectionTitle}>
                {new Date().getFullYear()} Season Stats
                <span style={s.sourceBadge}>MLB Stats API</span>
              </div>
              <div style={s.statsGrid}>
                <StatCard label="G" value={num(ss.g)} />
                <StatCard label="PA" value={num(ss.pa)} />
                <StatCard label="AB" value={num(ss.ab)} />
                <StatCard label="H" value={num(ss.h)} />
                <StatCard label="HR" value={num(ss.hr)} />
                <StatCard label="RBI" value={num(ss.rbi)} />
                <StatCard label="R" value={num(ss.r)} />
                <StatCard label="SB" value={num(ss.sb)} />
                <StatCard label="BB" value={num(ss.bb)} />
                <StatCard label="K" value={num(ss.k)} />
                <StatCard label="AVG" value={fmt(ss.batting_avg, 3)} />
                <StatCard label="OBP" value={fmt(ss.on_base_pct, 3)} />
                <StatCard label="SLG" value={fmt(ss.slugging_pct, 3)} />
                <StatCard label="OPS" value={fmt(ss.ops, 3)} />
                <StatCard label="K%" value={pct(ss.k_pct)} />
                <StatCard label="BB%" value={pct(ss.bb_pct)} />
              </div>
            </div>
          )}

          {(sc || agg) && (
            <div style={s.section}>
              <div style={s.sectionTitle}>
                Statcast Quality Metrics
                {sc && <span style={s.sourceBadge}>{sc.data_window} · {sc.sample_size} PA</span>}
              </div>
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(280px, 1fr))', gap: '16px' }}>
                {/* Stat cards row */}
                <div style={{ ...s.chartBox, display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '10px', padding: '16px' }}>
                  {sc && <>
                    <StatCard label="Avg Exit Velo" value={`${fmt(sc.avg_exit_velocity)} mph`} />
                    <StatCard label="Max Exit Velo" value={`${fmt(sc.max_exit_velocity)} mph`} />
                    <StatCard label="Avg Launch Angle" value={`${fmt(sc.avg_launch_angle)}°`} />
                    <StatCard label="Hard Hit%" value={pct(sc.hard_hit_pct)} />
                    <StatCard label="Barrel%" value={pct(sc.barrel_pct)} />
                    <StatCard label="Sample" value={`${sc.sample_size ?? '—'} PA`} />
                  </>}
                  {!sc && agg && <>
                    <StatCard label="Exit Velocity" value={`${fmt(agg.avg_exit_velocity)} mph`} />
                    <StatCard label="Launch Angle" value={`${fmt(agg.avg_launch_angle)}°`} />
                    <StatCard label="Hard Hit%" value={pct(agg.hard_hit_pct)} />
                    <StatCard label="Barrel%" value={pct(agg.barrel_pct)} />
                  </>}
                </div>
                {/* Gauge bars */}
                <StatcastGauges sc={sc} agg={agg} />
              </div>
            </div>
          )}

          {(splits.vsL || splits.vsR) && (
            <div style={s.section}>
              <div style={s.sectionTitle}>Platoon Splits — Current Season</div>
              <PlatoonChart splits={splits} />
            </div>
          )}

          {yby.length > 0 && (
            <div style={s.section}>
              <div style={s.sectionTitle}>
                Career Season Trends
                <span style={s.sourceBadge}>MLB Stats API</span>
              </div>
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '16px', marginBottom: '16px' }}>
                <div>
                  <div style={{ fontSize: '13px', color: C.muted, marginBottom: '8px', fontWeight: '500' }}>Rate Stats (AVG / OBP / SLG / OPS)</div>
                  <SeasonTrendChart yby={yby} />
                </div>
                <div>
                  <div style={{ fontSize: '13px', color: C.muted, marginBottom: '8px', fontWeight: '500' }}>Counting Stats (HR / SB)</div>
                  <HRTrendChart yby={yby} />
                </div>
              </div>
            </div>
          )}

          {yby.length > 0 && (
            <div style={s.section}>
              <div style={s.sectionTitle}>
                Year-by-Year
                <span style={s.sourceBadge}>MLB Stats API</span>
              </div>
              <div style={s.tableWrap}>
                <table style={s.table}>
                  <thead>
                    <tr>
                      {['Season','G','PA','H','2B','3B','HR','RBI','SB','BB','K','AVG','OBP','SLG','OPS','K%','BB%'].map(h => (
                        <th key={h} style={h === 'Season' ? s.th : { ...s.th, ...s.thR }}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {yby.map((row, i) => (
                      <tr key={i}>
                        <td style={{ ...s.td, fontWeight: '700', color: C.blue }}>{row.season}</td>
                        <td style={{ ...s.td, ...s.tdR }}>{num(row.g)}</td>
                        <td style={{ ...s.td, ...s.tdR }}>{num(row.pa)}</td>
                        <td style={{ ...s.td, ...s.tdR }}>{num(row.h)}</td>
                        <td style={{ ...s.td, ...s.tdR }}>{num(row.doubles)}</td>
                        <td style={{ ...s.td, ...s.tdR }}>{num(row.triples)}</td>
                        <td style={{ ...s.td, ...s.tdR }}>{num(row.hr)}</td>
                        <td style={{ ...s.td, ...s.tdR }}>{num(row.rbi)}</td>
                        <td style={{ ...s.td, ...s.tdR }}>{num(row.sb)}</td>
                        <td style={{ ...s.td, ...s.tdR }}>{num(row.bb)}</td>
                        <td style={{ ...s.td, ...s.tdR }}>{num(row.k)}</td>
                        <td style={{ ...s.td, ...s.tdR }}>{fmt(row.batting_avg, 3)}</td>
                        <td style={{ ...s.td, ...s.tdR }}>{fmt(row.on_base_pct, 3)}</td>
                        <td style={{ ...s.td, ...s.tdR }}>{fmt(row.slugging_pct, 3)}</td>
                        <td style={{ ...s.td, ...s.tdR }}>{fmt(row.ops, 3)}</td>
                        <td style={{ ...s.td, ...s.tdR }}>{pct(row.k_pct)}</td>
                        <td style={{ ...s.td, ...s.tdR }}>{pct(row.bb_pct)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </>
      )}
    </div>
  )
}
