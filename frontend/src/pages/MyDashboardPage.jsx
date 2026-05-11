import React, { useState } from 'react'

const API = import.meta.env.VITE_API_BASE_URL || ''

const COMPONENTS = [
  {
    key: 'hitters',
    title: 'My Top Hitters Today',
    description: 'Unique hitter board from Batter vs Arsenal, pitch usage, damage quality, and model context.',
  },
  {
    key: 'pitchers',
    title: 'My Top Pitchers Today',
    description: 'Pitcher lean board using K profile, contact suppression, opponent offense, and arsenal context.',
  },
  {
    key: 'teams',
    title: 'My Top Teams Today',
    description: 'Team board from model side edge, expected runs, offense profile, and opponent weaknesses.',
  },
  {
    key: 'totals',
    title: 'My Top Totals Today',
    description: 'Game total watchlist from projected runs, run environment, and simulation context.',
  },
  {
    key: 'overall_players',
    title: 'My Top Overall Players Today',
    description: 'Combined unique player board blending hitter and pitcher model-solver scores.',
  },
]

export default function MyDashboardPage() {
  const today = new Date().toISOString().slice(0, 10)
  const [date, setDate] = useState(today)
  const [results, setResults] = useState({})
  const [loading, setLoading] = useState({})
  const [errors, setErrors] = useState({})

  async function runSolver(component) {
    setLoading(prev => ({ ...prev, [component]: true }))
    setErrors(prev => ({ ...prev, [component]: null }))
    try {
      const params = new URLSearchParams({ date, component })
      const res = await fetch(`${API}/my-dashboard/solver?${params.toString()}`)
      const json = await res.json()
      if (!res.ok) throw new Error(JSON.stringify(json.detail || json))
      setResults(prev => ({ ...prev, [component]: json }))
    } catch (err) {
      console.error('My Dashboard solver failed:', err)
      setErrors(prev => ({ ...prev, [component]: err.message || 'Solver failed' }))
    } finally {
      setLoading(prev => ({ ...prev, [component]: false }))
    }
  }

  async function runAll() {
    for (const component of COMPONENTS) {
      // Run sequentially to avoid hammering the backend/model projection cache on first load.
      // eslint-disable-next-line no-await-in-loop
      await runSolver(component.key)
    }
  }

  return (
    <div>
      <section style={heroStyle}>
        <div>
          <div style={eyebrowStyle}>Personal analyst workspace prototype</div>
          <h1 style={titleStyle}>My Dashboard</h1>
          <p style={subtitleStyle}>
            Your daily analyst board for model edges, players, teams, totals, notes, and future saved picks.
          </p>
        </div>
        <div style={dateBoxStyle}>
          <label style={labelStyle}>Board Date</label>
          <input type="date" value={date} onChange={e => setDate(e.target.value)} style={inputStyle} />
          <button onClick={runAll} style={primaryButtonStyle}>Populate All</button>
        </div>
      </section>

      <section style={signupBannerStyle}>
        <div>
          <strong>Coming soon: sign in to save your daily boards, picks, tags, and notes across devices.</strong>
          <p style={{ margin: '6px 0 0', color: '#8b949e', lineHeight: 1.5 }}>
            For now, this page uses today’s app-owned model data and local dashboard actions. Save, Tag, and Add Note are placeholders for the future user-owned object system.
          </p>
        </div>
        <button disabled style={disabledButtonStyle}>Sign-in coming soon</button>
      </section>

      <section style={gridStyle}>
        {COMPONENTS.map(component => (
          <DashboardCard
            key={component.key}
            component={component}
            result={results[component.key]}
            loading={loading[component.key]}
            error={errors[component.key]}
            onRun={() => runSolver(component.key)}
          />
        ))}
      </section>
    </div>
  )
}

function DashboardCard({ component, result, loading, error, onRun }) {
  const items = result?.items || []
  return (
    <div style={cardStyle}>
      <div style={{ display: 'flex', justifyContent: 'space-between', gap: '12px', alignItems: 'flex-start' }}>
        <div>
          <h2 style={cardTitleStyle}>{component.title}</h2>
          <p style={cardDescriptionStyle}>{component.description}</p>
        </div>
        <span style={countBadgeStyle}>{items.length || 0}/10</span>
      </div>

      <button onClick={onRun} disabled={loading} style={solverButtonStyle}>
        {loading ? 'Solving...' : 'Use Model Solver To Populate Top 10 Today'}
      </button>

      {error && <div style={errorStyle}>{error}</div>}

      {!result && !loading && !error && (
        <div style={emptyStyle}>
          No saved results yet. Run the model solver to populate this component from existing app-owned data.
        </div>
      )}

      <div style={{ display: 'grid', gap: '10px', marginTop: '14px' }}>
        {items.map(item => <ResultItem key={item.dedupe_key || `${item.entity_type}-${item.entity_id}-${item.rank}`} item={item} />)}
      </div>

      {result && (
        <details style={{ marginTop: '14px' }}>
          <summary style={summaryStyle}>Data quality and missing data</summary>
          <pre style={preStyle}>{JSON.stringify({ data_quality: result.data_quality, missing_data: result.missing_data }, null, 2)}</pre>
        </details>
      )}
    </div>
  )
}

function ResultItem({ item }) {
  const metrics = item.metrics || {}
  const chart = item.chart_data || { labels: [], values: [] }
  const maxValue = Math.max(...(chart.values || []).map(v => Math.abs(Number(v) || 0)), 1)
  return (
    <div style={itemStyle}>
      <div style={itemHeaderStyle}>
        <div>
          <div style={rankStyle}>#{item.rank} {item.entity_name || item.entity_id}</div>
          <div style={metaStyle}>
            {item.player_type ? `${item.player_type} | ` : ''}{item.team || 'Team missing'} vs {item.opponent || 'Opponent missing'} {item.game_pk ? `| Game ${item.game_pk}` : ''}
          </div>
        </div>
        <div style={{ textAlign: 'right' }}>
          <div style={scoreStyle}>{formatNumber(item.score)}</div>
          <div style={confidenceStyle(item.confidence)}>{item.confidence || 'low'}</div>
        </div>
      </div>

      <p style={reasonStyle}>{item.primary_reason || 'Model solver ranked this item from app-owned data.'}</p>
      <ScoreBar value={Number(item.score) || 0} />

      {chart.labels?.length > 0 && (
        <div style={barsWrapStyle}>
          {chart.labels.map((label, idx) => {
            const value = Number(chart.values[idx]) || 0
            const width = Math.max(8, Math.min(100, Math.abs(value) / maxValue * 100))
            return (
              <div key={`${label}-${idx}`} style={metricRowStyle}>
                <span style={metricLabelStyle}>{label}</span>
                <div style={metricTrackStyle}><div style={{ ...metricFillStyle, width: `${width}%` }} /></div>
                <span style={metricValueStyle}>{formatNumber(value)}</span>
              </div>
            )
          })}
        </div>
      )}

      {item.best_pitch_angles?.length > 0 && (
        <div style={pitchAnglesStyle}>
          <strong>Best pitch angles:</strong>
          {item.best_pitch_angles.map((angle, idx) => (
            <div key={`${angle.pitch_type}-${idx}`} style={smallTextStyle}>• {angle.reason}</div>
          ))}
        </div>
      )}

      <details>
        <summary style={summaryStyle}>View reasoning</summary>
        <ul style={reasonListStyle}>
          {(item.reasoning || []).map((reason, idx) => <li key={idx}>{reason}</li>)}
        </ul>
        <pre style={preStyle}>{JSON.stringify({ metrics, missing_data: item.missing_data || [], source: item.source, dedupe_key: item.dedupe_key }, null, 2)}</pre>
      </details>

      <div style={actionsStyle}>
        <button disabled style={placeholderButtonStyle}>Save</button>
        <button disabled style={placeholderButtonStyle}>Tag</button>
        <button disabled style={placeholderButtonStyle}>Add Note</button>
      </div>
    </div>
  )
}

function ScoreBar({ value }) {
  const width = Math.max(4, Math.min(100, Math.abs(value) * 35))
  return <div style={scoreTrackStyle}><div style={{ ...scoreFillStyle, width: `${width}%` }} /></div>
}

function formatNumber(value) {
  const num = Number(value)
  if (!Number.isFinite(num)) return 'N/A'
  return Math.abs(num) >= 10 ? num.toFixed(1) : num.toFixed(3)
}

const heroStyle = {
  display: 'flex', justifyContent: 'space-between', gap: '20px', flexWrap: 'wrap',
  background: 'linear-gradient(135deg, #161b22, #0d1117)', border: '1px solid #30363d', borderRadius: '16px', padding: '24px', marginBottom: '16px'
}
const eyebrowStyle = { color: '#58a6ff', fontSize: '13px', fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.08em' }
const titleStyle = { margin: '6px 0 8px', fontSize: '34px', color: '#e6edf3' }
const subtitleStyle = { margin: 0, color: '#8b949e', maxWidth: '720px', lineHeight: 1.55 }
const dateBoxStyle = { minWidth: '220px', display: 'grid', gap: '8px', alignContent: 'start' }
const labelStyle = { color: '#8b949e', fontSize: '13px' }
const inputStyle = { background: '#0d1117', color: '#e6edf3', border: '1px solid #30363d', borderRadius: '8px', padding: '10px' }
const primaryButtonStyle = { background: '#238636', color: '#fff', border: 0, borderRadius: '8px', padding: '10px 12px', cursor: 'pointer', fontWeight: 700 }
const signupBannerStyle = { display: 'flex', justifyContent: 'space-between', gap: '16px', alignItems: 'center', flexWrap: 'wrap', background: '#0f1a2e', border: '1px solid #1f6feb55', borderRadius: '14px', padding: '16px', marginBottom: '18px', color: '#e6edf3' }
const disabledButtonStyle = { background: '#21262d', color: '#8b949e', border: '1px solid #30363d', borderRadius: '8px', padding: '10px 12px' }
const gridStyle = { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(360px, 1fr))', gap: '16px' }
const cardStyle = { background: '#161b22', border: '1px solid #30363d', borderRadius: '14px', padding: '16px', color: '#e6edf3' }
const cardTitleStyle = { margin: 0, fontSize: '20px' }
const cardDescriptionStyle = { color: '#8b949e', lineHeight: 1.45, margin: '6px 0 0', fontSize: '13px' }
const countBadgeStyle = { background: '#0d1117', border: '1px solid #30363d', borderRadius: '999px', padding: '4px 8px', color: '#8b949e', fontSize: '12px' }
const solverButtonStyle = { width: '100%', marginTop: '14px', background: '#58a6ff', color: '#07111f', border: 0, borderRadius: '8px', padding: '10px', fontWeight: 800, cursor: 'pointer' }
const errorStyle = { background: '#2d1b1b', border: '1px solid #a33', borderRadius: '8px', padding: '10px', marginTop: '12px', color: '#ffb4b4' }
const emptyStyle = { marginTop: '14px', padding: '14px', background: '#0d1117', border: '1px dashed #30363d', borderRadius: '10px', color: '#8b949e', lineHeight: 1.45 }
const itemStyle = { background: '#0d1117', border: '1px solid #30363d', borderRadius: '10px', padding: '12px' }
const itemHeaderStyle = { display: 'flex', justifyContent: 'space-between', gap: '12px', alignItems: 'start' }
const rankStyle = { fontWeight: 800, color: '#e6edf3' }
const metaStyle = { color: '#8b949e', fontSize: '12px', marginTop: '4px' }
const scoreStyle = { fontWeight: 900, color: '#58a6ff' }
const confidenceStyle = (level) => ({ marginTop: '4px', color: level === 'high' ? '#3fb950' : level === 'medium' ? '#d29922' : '#8b949e', fontSize: '12px', textTransform: 'uppercase', fontWeight: 700 })
const reasonStyle = { color: '#c9d1d9', lineHeight: 1.45, fontSize: '13px' }
const scoreTrackStyle = { height: '8px', background: '#21262d', borderRadius: '999px', overflow: 'hidden', margin: '10px 0' }
const scoreFillStyle = { height: '100%', background: '#58a6ff', borderRadius: '999px' }
const barsWrapStyle = { display: 'grid', gap: '6px', margin: '10px 0' }
const metricRowStyle = { display: 'grid', gridTemplateColumns: '92px 1fr 52px', alignItems: 'center', gap: '8px' }
const metricLabelStyle = { color: '#8b949e', fontSize: '11px' }
const metricTrackStyle = { height: '6px', background: '#21262d', borderRadius: '999px', overflow: 'hidden' }
const metricFillStyle = { height: '100%', background: '#3fb950', borderRadius: '999px' }
const metricValueStyle = { color: '#c9d1d9', fontSize: '11px', textAlign: 'right' }
const pitchAnglesStyle = { margin: '10px 0', color: '#c9d1d9', fontSize: '12px', lineHeight: 1.5 }
const smallTextStyle = { color: '#8b949e', marginTop: '3px' }
const summaryStyle = { cursor: 'pointer', color: '#58a6ff', fontSize: '13px', marginTop: '8px' }
const reasonListStyle = { color: '#c9d1d9', fontSize: '13px', lineHeight: 1.5, paddingLeft: '18px' }
const preStyle = { background: '#010409', border: '1px solid #30363d', borderRadius: '8px', padding: '10px', overflowX: 'auto', color: '#8b949e', fontSize: '11px' }
const actionsStyle = { display: 'flex', gap: '8px', marginTop: '10px' }
const placeholderButtonStyle = { background: '#21262d', color: '#8b949e', border: '1px solid #30363d', borderRadius: '6px', padding: '6px 8px', fontSize: '12px' }
