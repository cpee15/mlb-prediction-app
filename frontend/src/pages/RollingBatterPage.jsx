import React, { useState, useEffect, useCallback } from 'react'
import { useParams, Link } from 'react-router-dom'
import {
  BarChart, Bar, LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip,
  Legend, ResponsiveContainer, ReferenceLine, Cell, ComposedChart, Area,
} from 'recharts'

const API = import.meta.env.VITE_API_BASE_URL || ''

const C = {
  green: '#3fb950', blue: '#58a6ff', orange: '#d29922', red: '#f85149',
  purple: '#bc8cff', teal: '#39d0d8',
  text: '#e6edf3', muted: '#8b949e', bg: '#0d1117', card: '#161b22',
  border: '#30363d', elevated: '#21262d',
}

const RESULT_COLORS = {
  single: C.green, double: C.green, triple: C.green, home_run: C.blue,
  walk: C.orange, intent_walk: C.orange, hit_by_pitch: C.orange,
  strikeout: C.red, strikeout_double_play: C.red,
  field_out: C.muted, grounded_into_double_play: C.muted, double_play: C.muted,
  force_out: C.muted, fielders_choice: C.muted, fielders_choice_out: C.muted,
  sac_fly: C.muted, sac_bunt: C.muted,
}

const RESULT_LABELS = {
  single: '1B', double: '2B', triple: '3B', home_run: 'HR',
  walk: 'BB', intent_walk: 'IBB', hit_by_pitch: 'HBP',
  strikeout: 'K', strikeout_double_play: 'KDP',
  field_out: 'FO', grounded_into_double_play: 'GDP', double_play: 'DP',
  force_out: 'FO', fielders_choice: 'FC', fielders_choice_out: 'FCO',
  sac_fly: 'SF', sac_bunt: 'SB',
}

const s = {
  back: { color: C.blue, textDecoration: 'none', fontSize: '13px', display: 'inline-block', marginBottom: '20px' },
  header: { display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '20px', flexWrap: 'wrap', gap: '12px' },
  title: { fontSize: '22px', fontWeight: '700', color: C.text },
  subtitle: { fontSize: '13px', color: C.muted, marginTop: '4px' },
  tabs: { display: 'flex', gap: '0', marginBottom: '20px', background: C.card, border: `1px solid ${C.border}`, borderRadius: '8px', overflow: 'hidden', width: 'fit-content' },
  tab: (active) => ({
    padding: '8px 18px', fontSize: '13px', fontWeight: '500', cursor: 'pointer',
    background: active ? C.blue : 'transparent', color: active ? C.bg : C.muted, border: 'none', outline: 'none',
  }),
  qaBox: { background: C.card, border: `1px solid ${C.border}`, borderRadius: '8px', padding: '12px 14px', marginBottom: '20px', fontSize: '13px', color: C.muted },
  qaWarn: { color: C.orange, marginTop: '6px' },
  metaGrid: { display: 'flex', flexWrap: 'wrap', gap: '8px', marginTop: '10px' },
  metaChip: { background: C.elevated, border: `1px solid ${C.border}`, borderRadius: '999px', padding: '3px 9px', fontSize: '12px', color: C.muted },
  tableWrap: { background: C.card, border: `1px solid ${C.border}`, borderRadius: '10px', overflow: 'auto', marginBottom: '20px' },
  table: { width: '100%', borderCollapse: 'collapse', fontSize: '13px', minWidth: '600px' },
  th: { padding: '12px 14px', textAlign: 'left', color: C.muted, fontSize: '11px', textTransform: 'uppercase', letterSpacing: '0.5px', borderBottom: `1px solid ${C.elevated}`, whiteSpace: 'nowrap' },
  thRight: { textAlign: 'right' },
  td: { padding: '10px 14px', borderBottom: `1px solid ${C.bg}`, color: C.text, whiteSpace: 'nowrap' },
  tdRight: { textAlign: 'right' },
  tdMuted: { color: C.muted },
  sectionTitle: { fontSize: '15px', fontWeight: '600', color: C.text, marginBottom: '12px', borderBottom: `1px solid ${C.elevated}`, paddingBottom: '8px' },
  chartBox: { background: C.card, border: `1px solid ${C.border}`, borderRadius: '10px', padding: '20px', marginBottom: '20px' },
  chartTitle: { fontSize: '14px', fontWeight: '600', color: C.text, marginBottom: '4px' },
  chartSub: { fontSize: '12px', color: C.muted, marginBottom: '16px' },
  abRow: { display: 'grid', gridTemplateColumns: '90px 56px 56px 70px auto 60px 60px 40px', gap: '4px', padding: '7px 14px', borderBottom: `1px solid ${C.bg}`, fontSize: '13px', alignItems: 'center' },
  abHeader: { display: 'grid', gridTemplateColumns: '90px 56px 56px 70px auto 60px 60px 40px', gap: '4px', padding: '9px 14px', borderBottom: `1px solid ${C.elevated}`, fontSize: '11px', color: C.muted, textTransform: 'uppercase', letterSpacing: '0.5px' },
  pagination: { display: 'flex', gap: '8px', alignItems: 'center', justifyContent: 'center', padding: '16px', fontSize: '13px', color: C.muted },
  pageBtn: (disabled) => ({ background: disabled ? C.elevated : C.border, border: 'none', color: disabled ? '#555' : C.text, borderRadius: '6px', padding: '6px 14px', cursor: disabled ? 'default' : 'pointer', fontSize: '13px' }),
  loader: { color: C.muted, padding: '48px', textAlign: 'center' },
  error: { color: C.red, padding: '24px', background: '#1f1116', borderRadius: '8px' },
}

const tooltipStyle = {
  background: '#1c2128', border: `1px solid ${C.border}`, borderRadius: '6px', fontSize: '12px', color: C.text,
}

const pct = (v) => v != null ? `${(v * 100).toFixed(1)}%` : '—'
const dec = (v, d = 3) => v != null ? Number(v).toFixed(d) : '—'
const mph = v => v != null ? `${Number(v).toFixed(1)}` : '—'
const deg = v => v != null ? `${Number(v).toFixed(1)}°` : '—'
const num = v => v != null ? Number(v).toLocaleString() : '—'

function avgColor(v) { if (v == null) return C.text; if (v >= 0.280) return C.green; if (v >= 0.240) return C.orange; return C.red }
function kColor(v) { if (v == null) return C.text; if (v <= 0.18) return C.green; if (v <= 0.25) return C.orange; return C.red }
function evColor(v) { if (v == null) return C.text; if (v >= 92) return C.green; if (v >= 88) return C.orange; return C.red }

function DataQualityBox({ quality }) {
  if (!quality) return null
  const warnings = quality.warnings || []
  return (
    <div style={s.qaBox}>
      <div>Data Quality: <strong style={{ color: C.text }}>{quality.ordering_quality || 'unknown'}</strong></div>
      <div>Latest Event: <strong style={{ color: quality.is_stale ? C.orange : C.text }}>{quality.latest_event_date || '—'}</strong></div>
      <div style={s.metaGrid}>
        <span style={s.metaChip}>Source: {quality.source || 'postgres_statcast_events'}</span>
        <span style={s.metaChip}>As of: {quality.as_of_date || '—'}</span>
        <span style={s.metaChip}>Days stale: {quality.days_stale ?? '—'}</span>
        <span style={s.metaChip}>Terminal rows: {num(quality.terminal_event_rows)}</span>
        <span style={s.metaChip}>Total rows: {num(quality.total_event_rows)}</span>
      </div>
      {warnings.map((w, i) => <div key={i} style={s.qaWarn}>⚠ {w}</div>)}
    </div>
  )
}

function WindowMeta({ stats }) {
  if (!stats) return null
  return (
    <div style={{ color: C.muted, fontSize: '11px', marginTop: '3px' }}>
      PA {num(stats.actual_pa)} · AB {num(stats.actual_ab)} · BBE {num(stats.batted_ball_count)} · HH {num(stats.hard_hit_count)} · Barrel {num(stats.barrel_count)}
      {stats.invalid_event_count ? ` · Invalid ${num(stats.invalid_event_count)}` : ''}
    </div>
  )
}

// Builds chart data array from rolling windows dict
function buildChartRows(windows) {
  if (!windows) return []
  return Object.entries(windows)
    .filter(([, st]) => st != null)
    .map(([window, st]) => ({
      window,
      AVG: st.batting_avg != null ? +st.batting_avg.toFixed(3) : null,
      OBP: st.on_base_pct != null ? +st.on_base_pct.toFixed(3) : null,
      SLG: st.slugging_pct != null ? +st.slugging_pct.toFixed(3) : null,
      OPS: st.ops != null ? +st.ops.toFixed(3) : null,
      'K%': st.k_pct != null ? +(st.k_pct * 100).toFixed(1) : null,
      'BB%': st.bb_pct != null ? +(st.bb_pct * 100).toFixed(1) : null,
      EV: st.avg_exit_velocity != null ? +st.avg_exit_velocity.toFixed(1) : null,
      'HH%': st.hard_hit_pct != null ? +(st.hard_hit_pct * 100).toFixed(1) : null,
      'Barrel%': st.barrel_pct != null ? +(st.barrel_pct * 100).toFixed(1) : null,
      HR: st.home_runs ?? null,
    }))
}

function RollingRateChart({ rows, view }) {
  if (!rows.length) return null
  const label = view === 'games' ? 'games' : view === 'ab' ? 'AB' : 'PA'
  return (
    <div style={s.chartBox}>
      <div style={s.chartTitle}>Slash Line by Window Size</div>
      <div style={s.chartSub}>How performance changes from recent to larger sample — smaller windows = more recent</div>
      <ResponsiveContainer width="100%" height={240}>
        <LineChart data={rows} margin={{ top: 4, right: 16, left: 0, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke={C.elevated} />
          <XAxis dataKey="window" tick={{ fill: C.muted, fontSize: 12 }}
            label={{ value: `Last N ${label}`, position: 'insideBottom', offset: -2, fill: C.muted, fontSize: 11 }}
            height={36} axisLine={{ stroke: C.border }} tickLine={false} />
          <YAxis domain={[0, 1.2]} tick={{ fill: C.muted, fontSize: 11 }} axisLine={false} tickLine={false} width={44} />
          <Tooltip contentStyle={tooltipStyle} labelStyle={{ color: C.blue, fontWeight: 600 }} />
          <Legend wrapperStyle={{ fontSize: '12px', paddingTop: '8px' }} />
          <ReferenceLine y={0.250} stroke={C.muted} strokeDasharray="4 4" opacity={0.35} label={{ value: '.250', fill: C.muted, fontSize: 10, position: 'right' }} />
          <Line type="monotone" dataKey="AVG" stroke={C.green} strokeWidth={2.5} dot={{ r: 4, fill: C.green }} activeDot={{ r: 6 }} connectNulls />
          <Line type="monotone" dataKey="OBP" stroke={C.blue} strokeWidth={2.5} dot={{ r: 4, fill: C.blue }} activeDot={{ r: 6 }} connectNulls />
          <Line type="monotone" dataKey="SLG" stroke={C.orange} strokeWidth={2.5} dot={{ r: 4, fill: C.orange }} activeDot={{ r: 6 }} connectNulls />
          <Line type="monotone" dataKey="OPS" stroke={C.purple} strokeWidth={2} dot={{ r: 3, fill: C.purple }} activeDot={{ r: 5 }} connectNulls strokeDasharray="5 3" />
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}

function DisciplineChart({ rows, view }) {
  if (!rows.length) return null
  const label = view === 'games' ? 'games' : view === 'ab' ? 'AB' : 'PA'
  return (
    <div style={s.chartBox}>
      <div style={s.chartTitle}>Strikeout & Walk Rate by Window</div>
      <div style={s.chartSub}>K% and BB% — smaller window = more recent plate appearances</div>
      <ResponsiveContainer width="100%" height={220}>
        <ComposedChart data={rows} margin={{ top: 4, right: 16, left: 0, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke={C.elevated} vertical={false} />
          <XAxis dataKey="window" tick={{ fill: C.muted, fontSize: 12 }}
            label={{ value: `Last N ${label}`, position: 'insideBottom', offset: -2, fill: C.muted, fontSize: 11 }}
            height={36} axisLine={{ stroke: C.border }} tickLine={false} />
          <YAxis tick={{ fill: C.muted, fontSize: 11 }} axisLine={false} tickLine={false} width={40}
            tickFormatter={v => `${v}%`} />
          <Tooltip contentStyle={tooltipStyle} labelStyle={{ color: C.orange, fontWeight: 600 }}
            formatter={(val, name) => [`${val}%`, name]} />
          <Legend wrapperStyle={{ fontSize: '12px', paddingTop: '8px' }} />
          <ReferenceLine y={22.5} stroke={C.red} strokeDasharray="4 4" opacity={0.35}
            label={{ value: 'Lg K%', fill: C.red, fontSize: 10, position: 'right' }} />
          <ReferenceLine y={8.5} stroke={C.green} strokeDasharray="4 4" opacity={0.35}
            label={{ value: 'Lg BB%', fill: C.green, fontSize: 10, position: 'right' }} />
          <Bar dataKey="K%" fill={C.red} opacity={0.8} radius={[3, 3, 0, 0]} maxBarSize={40} />
          <Line type="monotone" dataKey="BB%" stroke={C.green} strokeWidth={2.5} dot={{ r: 4, fill: C.green }} activeDot={{ r: 6 }} connectNulls />
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  )
}

function ContactQualityChart({ rows, view }) {
  if (!rows.length) return null
  const label = view === 'games' ? 'games' : view === 'ab' ? 'AB' : 'PA'
  return (
    <div style={s.chartBox}>
      <div style={s.chartTitle}>Contact Quality by Window</div>
      <div style={s.chartSub}>Exit velocity (mph), Hard Hit%, and Barrel% across rolling windows</div>
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '0', height: '220px' }}>
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={rows} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke={C.elevated} />
            <XAxis dataKey="window" tick={{ fill: C.muted, fontSize: 11 }} axisLine={false} tickLine={false} height={28} />
            <YAxis domain={[80, 100]} tick={{ fill: C.muted, fontSize: 11 }} axisLine={false} tickLine={false} width={38}
              tickFormatter={v => `${v}`} />
            <Tooltip contentStyle={tooltipStyle} formatter={(v, name) => [`${v} mph`, name]} />
            <ReferenceLine y={88.5} stroke={C.muted} strokeDasharray="3 3" opacity={0.5}
              label={{ value: 'Lg avg', fill: C.muted, fontSize: 10, position: 'right' }} />
            <Line type="monotone" dataKey="EV" stroke={C.teal} strokeWidth={2.5} dot={{ r: 4, fill: C.teal }} activeDot={{ r: 6 }} connectNulls name="Exit Velo" />
          </LineChart>
        </ResponsiveContainer>
        <ResponsiveContainer width="100%" height="100%">
          <ComposedChart data={rows} margin={{ top: 4, right: 16, left: 0, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke={C.elevated} />
            <XAxis dataKey="window" tick={{ fill: C.muted, fontSize: 11 }} axisLine={false} tickLine={false} height={28} />
            <YAxis tick={{ fill: C.muted, fontSize: 11 }} axisLine={false} tickLine={false} width={36}
              tickFormatter={v => `${v}%`} />
            <Tooltip contentStyle={tooltipStyle} formatter={(v, name) => [`${v}%`, name]} />
            <ReferenceLine y={38} stroke={C.orange} strokeDasharray="3 3" opacity={0.4}
              label={{ value: 'HH lg', fill: C.orange, fontSize: 10, position: 'right' }} />
            <Bar dataKey="HH%" fill={C.orange} opacity={0.75} radius={[3, 3, 0, 0]} maxBarSize={36} />
            <Line type="monotone" dataKey="Barrel%" stroke={C.purple} strokeWidth={2.5} dot={{ r: 4, fill: C.purple }} activeDot={{ r: 6 }} connectNulls />
          </ComposedChart>
        </ResponsiveContainer>
      </div>
    </div>
  )
}

function PlatoonCompareChart({ splits }) {
  if (!splits || !Object.keys(splits).length) return null

  const entries = Object.entries(splits).filter(([, st]) => st != null)
  if (entries.length < 2) return null

  const metrics = ['AVG', 'OBP', 'SLG', 'K%', 'BB%']
  const data = metrics.map(m => {
    const row = { metric: m }
    entries.forEach(([name, st]) => {
      if (m === 'K%') row[name] = st.k_pct != null ? +(st.k_pct * 100).toFixed(1) : null
      else if (m === 'BB%') row[name] = st.bb_pct != null ? +(st.bb_pct * 100).toFixed(1) : null
      else if (m === 'AVG') row[name] = st.batting_avg != null ? +(st.batting_avg * 1000).toFixed(0) : null
      else if (m === 'OBP') row[name] = st.on_base_pct != null ? +(st.on_base_pct * 1000).toFixed(0) : null
      else if (m === 'SLG') row[name] = st.slugging_pct != null ? +(st.slugging_pct * 1000).toFixed(0) : null
    })
    return row
  })

  const SPLIT_COLORS = [C.purple, C.blue]

  return (
    <div style={{ ...s.chartBox, marginBottom: '20px' }}>
      <div style={s.chartTitle}>Platoon Splits Comparison (Last 200 PA)</div>
      <div style={s.chartSub}>AVG/OBP/SLG ×1000 (e.g. 280 = .280) · K%/BB% as percentage</div>
      <ResponsiveContainer width="100%" height={200}>
        <BarChart data={data} margin={{ top: 4, right: 16, left: 0, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke={C.elevated} vertical={false} />
          <XAxis dataKey="metric" tick={{ fill: C.muted, fontSize: 12 }} axisLine={false} tickLine={false} />
          <YAxis tick={{ fill: C.muted, fontSize: 11 }} axisLine={false} tickLine={false} width={36} />
          <Tooltip contentStyle={tooltipStyle} labelStyle={{ color: C.text, fontWeight: 600 }} />
          <Legend wrapperStyle={{ fontSize: '12px', paddingTop: '8px' }} />
          {entries.map(([name], i) => (
            <Bar key={name} dataKey={name} fill={SPLIT_COLORS[i % SPLIT_COLORS.length]} radius={[3, 3, 0, 0]} maxBarSize={48} />
          ))}
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}

export default function RollingBatterPage() {
  const { id } = useParams()
  const [view, setView] = useState('pa')
  const [rolling, setRolling] = useState(null)
  const [splits, setSplits] = useState(null)
  const [abData, setAbData] = useState(null)
  const [abPage, setAbPage] = useState(0)
  const [loading, setLoading] = useState(true)
  const [abLoading, setAbLoading] = useState(false)
  const [error, setError] = useState(null)
  const [chartView, setChartView] = useState('rate')

  const AB_PAGE_SIZE = 50

  useEffect(() => {
    if (!id) return
    fetch(`${API}/batter/${id}/rolling/splits?pa=200`)
      .then(r => r.ok ? r.json() : null)
      .then(d => { if (d) setSplits(d.splits || {}) })
      .catch(() => {})
  }, [id])

  useEffect(() => {
    if (!id) return
    setLoading(true); setError(null)
    const endpoint = view === 'games'
      ? `${API}/batter/${id}/rolling/games?windows=5,10,15,30`
      : view === 'ab'
        ? `${API}/batter/${id}/rolling/ab?windows=10,25,50,100`
        : `${API}/batter/${id}/rolling/pa?windows=10,25,50,100,200,400,1000`
    fetch(endpoint)
      .then(r => r.ok ? r.json() : r.json().then(e => Promise.reject(e.detail || r.statusText)))
      .then(d => { setRolling(d); setLoading(false) })
      .catch(e => { setError(String(e)); setLoading(false) })
  }, [id, view])

  const loadAbs = useCallback((page) => {
    if (!id) return
    setAbLoading(true)
    fetch(`${API}/batter/${id}/at-bats/ordered?limit=${AB_PAGE_SIZE}&offset=${page * AB_PAGE_SIZE}`)
      .then(r => r.ok ? r.json() : r.json().then(e => Promise.reject(e.detail || r.statusText)))
      .then(d => { setAbData(d); setAbLoading(false) })
      .catch(() => setAbLoading(false))
  }, [id])

  useEffect(() => { loadAbs(0) }, [loadAbs])

  const totalEvents = abData?.total || 0
  const totalPages = Math.ceil(totalEvents / AB_PAGE_SIZE)
  const rows = rolling?.windows
    ? Object.entries(rolling.windows).map(([window, stats]) => ({ window, stats }))
    : []
  const chartRows = buildChartRows(rolling?.windows)

  function goPage(p) { setAbPage(p); loadAbs(p) }
  if (error) return <div style={s.error}>{error}</div>

  return (
    <div>
      <Link to={`/batter/${id}`} style={s.back}>← Back to Batter Profile</Link>
      <div style={s.header}>
        <div>
          <div style={s.title}>Rolling Stats — Batter #{id}</div>
          <div style={s.subtitle}>Rolling PA is default. AB mode uses strict official AB filtering.</div>
        </div>
      </div>

      <DataQualityBox quality={rolling?.data_quality} />

      {/* Window type tabs */}
      <div style={s.tabs}>
        <button style={s.tab(view === 'pa')} onClick={() => setView('pa')}>Last N PA</button>
        <button style={s.tab(view === 'ab')} onClick={() => setView('ab')}>Last N AB</button>
        <button style={s.tab(view === 'games')} onClick={() => setView('games')}>Last N Games</button>
      </div>

      {loading ? <div style={s.loader}>Loading…</div> : (
        <>
          {/* Chart type selector */}
          <div style={{ display: 'flex', gap: '8px', marginBottom: '16px', flexWrap: 'wrap' }}>
            {[['rate', 'Slash Line'], ['discipline', 'K% / BB%'], ['contact', 'Contact Quality']].map(([key, label]) => (
              <button
                key={key}
                onClick={() => setChartView(key)}
                style={{
                  padding: '6px 14px', fontSize: '12px', fontWeight: '500', cursor: 'pointer',
                  background: chartView === key ? C.elevated : 'transparent',
                  border: `1px solid ${chartView === key ? C.blue : C.border}`,
                  color: chartView === key ? C.blue : C.muted,
                  borderRadius: '6px', outline: 'none',
                }}
              >
                {label}
              </button>
            ))}
          </div>

          {chartView === 'rate' && <RollingRateChart rows={chartRows} view={view} />}
          {chartView === 'discipline' && <DisciplineChart rows={chartRows} view={view} />}
          {chartView === 'contact' && <ContactQualityChart rows={chartRows} view={view} />}

          {/* Data table */}
          <div style={s.tableWrap}>
            <table style={s.table}>
              <thead>
                <tr>
                  <th style={s.th}>Window</th>
                  <th style={s.th}>{view === 'games' ? 'Games' : view === 'ab' ? 'AB' : 'PA'}</th>
                  <th style={s.th}>Range</th>
                  <th style={{ ...s.th, ...s.thRight }}>AVG</th>
                  <th style={{ ...s.th, ...s.thRight }}>OBP</th>
                  <th style={{ ...s.th, ...s.thRight }}>SLG</th>
                  <th style={{ ...s.th, ...s.thRight }}>HR</th>
                  <th style={{ ...s.th, ...s.thRight }}>K%</th>
                  <th style={{ ...s.th, ...s.thRight }}>BB%</th>
                  <th style={{ ...s.th, ...s.thRight }}>EV</th>
                  <th style={{ ...s.th, ...s.thRight }}>LA</th>
                  <th style={{ ...s.th, ...s.thRight }}>HH%</th>
                  <th style={{ ...s.th, ...s.thRight }}>Barrel%</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((w, i) => {
                  const st = w.stats
                  if (!st) {
                    return <tr key={i}><td style={{ ...s.td, fontWeight: '600' }}>{w.window}</td><td colSpan={12} style={{ ...s.td, ...s.tdMuted }}>Insufficient data</td></tr>
                  }
                  const count = view === 'games' ? (st.actual_games ?? '—') : view === 'ab' ? (st.actual_ab ?? '—') : (st.actual_pa ?? '—')
                  const dateRange = st.start_date && st.end_date ? `${st.start_date.slice(5)} – ${st.end_date.slice(5)}` : '—'
                  return (
                    <tr key={i}>
                      <td style={{ ...s.td, fontWeight: '700', color: C.blue }}>
                        {w.window}
                        <WindowMeta stats={st} />
                      </td>
                      <td style={s.td}>{count}</td>
                      <td style={{ ...s.td, ...s.tdMuted, fontSize: '12px' }}>{dateRange}</td>
                      <td style={{ ...s.td, ...s.tdRight, color: avgColor(st.batting_avg), fontWeight: '600' }}>{dec(st.batting_avg)}</td>
                      <td style={{ ...s.td, ...s.tdRight }}>{dec(st.on_base_pct ?? null)}</td>
                      <td style={{ ...s.td, ...s.tdRight }}>{dec(st.slugging_pct ?? null)}</td>
                      <td style={{ ...s.td, ...s.tdRight }}>{st.home_runs ?? '—'}</td>
                      <td style={{ ...s.td, ...s.tdRight, color: kColor(st.k_pct), fontWeight: '600' }}>{pct(st.k_pct)}</td>
                      <td style={{ ...s.td, ...s.tdRight }}>{pct(st.bb_pct)}</td>
                      <td style={{ ...s.td, ...s.tdRight, color: evColor(st.avg_exit_velocity) }}>{mph(st.avg_exit_velocity)}</td>
                      <td style={{ ...s.td, ...s.tdRight }}>{deg(st.avg_launch_angle)}</td>
                      <td style={{ ...s.td, ...s.tdRight }}>{pct(st.hard_hit_pct)}</td>
                      <td style={{ ...s.td, ...s.tdRight }}>{pct(st.barrel_pct)}</td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        </>
      )}

      {splits && Object.keys(splits).length > 0 && (
        <div style={{ marginBottom: '24px' }}>
          <div style={s.sectionTitle}>Platoon Splits (Last 200 PA)</div>
          <PlatoonCompareChart splits={splits} />
          <div style={s.tableWrap}>
            <table style={s.table}>
              <thead><tr>
                <th style={s.th}>Split</th>
                <th style={{ ...s.th, ...s.thRight }}>PA</th>
                <th style={{ ...s.th, ...s.thRight }}>AVG</th>
                <th style={{ ...s.th, ...s.thRight }}>OBP</th>
                <th style={{ ...s.th, ...s.thRight }}>SLG</th>
                <th style={{ ...s.th, ...s.thRight }}>OPS</th>
                <th style={{ ...s.th, ...s.thRight }}>K%</th>
                <th style={{ ...s.th, ...s.thRight }}>BB%</th>
                <th style={{ ...s.th, ...s.thRight }}>EV</th>
                <th style={{ ...s.th, ...s.thRight }}>HH%</th>
              </tr></thead>
              <tbody>
                {Object.entries(splits).map(([name, st]) => st && (
                  <tr key={name}>
                    <td style={{ ...s.td, fontWeight: '700', color: C.blue }}>
                      {name}
                      <WindowMeta stats={st} />
                    </td>
                    <td style={{ ...s.td, ...s.tdRight }}>{st.actual_pa ?? '—'}</td>
                    <td style={{ ...s.td, ...s.tdRight }}>{dec(st.batting_avg)}</td>
                    <td style={{ ...s.td, ...s.tdRight }}>{dec(st.on_base_pct)}</td>
                    <td style={{ ...s.td, ...s.tdRight }}>{dec(st.slugging_pct)}</td>
                    <td style={{ ...s.td, ...s.tdRight }}>{dec(st.ops)}</td>
                    <td style={{ ...s.td, ...s.tdRight }}>{pct(st.k_pct)}</td>
                    <td style={{ ...s.td, ...s.tdRight }}>{pct(st.bb_pct)}</td>
                    <td style={{ ...s.td, ...s.tdRight }}>{mph(st.avg_exit_velocity)}</td>
                    <td style={{ ...s.td, ...s.tdRight }}>{pct(st.hard_hit_pct)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      <div style={{ marginTop: '28px' }}>
        <div style={s.sectionTitle}>Chronological Plate Appearance Session</div>
        <div style={{ fontSize: '13px', color: C.muted, marginBottom: '14px' }}>
          {abData ? `${totalEvents.toLocaleString()} total terminal PA outcomes on record · showing ${AB_PAGE_SIZE} per page` : ''}
        </div>
        <DataQualityBox quality={abData?.data_quality} />
        {abLoading ? <div style={s.loader}>Loading events…</div> : abData && (
          <div style={s.tableWrap}>
            <div style={s.abHeader}>
              <span>Date</span><span>AB #</span><span>Pitch #</span><span>Pitch</span>
              <span>Result</span><span style={{ textAlign: 'right' }}>EV</span>
              <span style={{ textAlign: 'right' }}>LA</span><span>Stand</span>
            </div>
            {(abData.events || []).map((ab, i) => {
              const resultLabel = RESULT_LABELS[ab.result] || ab.result?.replace(/_/g, ' ') || '?'
              const resultColor = RESULT_COLORS[ab.result] || C.muted
              return (
                <div key={i} style={s.abRow}>
                  <span style={{ color: C.muted, fontSize: '12px' }}>{ab.game_date}</span>
                  <span style={{ color: C.muted, fontSize: '12px' }}>{ab.at_bat_number ?? '—'}</span>
                  <span style={{ color: C.muted, fontSize: '12px' }}>{ab.pitch_number ?? '—'}</span>
                  <span style={{ color: C.muted, fontSize: '12px' }}>{ab.pitch_type || '—'}</span>
                  <span style={{ color: resultColor, fontWeight: '600' }}>{resultLabel}</span>
                  <span style={{ color: ab.exit_velocity ? C.text : C.muted, textAlign: 'right' }}>
                    {ab.exit_velocity ? `${ab.exit_velocity.toFixed(1)}` : '—'}
                  </span>
                  <span style={{ color: C.muted, textAlign: 'right', fontSize: '12px' }}>
                    {ab.launch_angle != null ? `${ab.launch_angle.toFixed(0)}°` : '—'}
                  </span>
                  <span style={{ color: C.muted, fontSize: '12px' }}>{ab.batter_stand || '?'}</span>
                </div>
              )
            })}
            <div style={s.pagination}>
              <button style={s.pageBtn(abPage === 0)} disabled={abPage === 0} onClick={() => goPage(abPage - 1)}>← Prev</button>
              <span>Page {abPage + 1} of {totalPages || 1}</span>
              <button style={s.pageBtn(abPage >= totalPages - 1)} disabled={abPage >= totalPages - 1} onClick={() => goPage(abPage + 1)}>Next →</button>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
