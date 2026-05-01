import React, { useEffect, useMemo, useState } from 'react'

const API = import.meta.env.VITE_API_BASE_URL || ''

const s = {
  page: { color: '#e6edf3' },
  header: { marginBottom: '24px' },
  title: { margin: 0, color: '#e6edf3', fontSize: '34px', fontWeight: 800 },
  subtitle: { color: '#8b949e', marginTop: '6px', fontSize: '15px' },
  dateInput: {
    background: '#0d1117',
    color: '#e6edf3',
    border: '1px solid #30363d',
    borderRadius: '8px',
    padding: '8px',
    marginLeft: '8px',
  },
  card: {
    background: '#0d1117',
    border: '1px solid #30363d',
    borderRadius: '14px',
    padding: '18px',
    marginBottom: '16px',
  },
  gameCard: {
    background: '#0d1117',
    border: '1px solid #30363d',
    borderRadius: '14px',
    padding: '20px',
    marginBottom: '20px',
  },
  gameHeader: {
    display: 'flex',
    justifyContent: 'space-between',
    gap: '16px',
    alignItems: 'flex-start',
    flexWrap: 'wrap',
    marginBottom: '16px',
  },
  matchupTitle: { margin: 0, color: '#e6edf3', fontSize: '24px', fontWeight: 800 },
  meta: { color: '#8b949e', fontSize: '14px', marginTop: '5px' },
  pill: {
    display: 'inline-block',
    padding: '3px 8px',
    borderRadius: '999px',
    background: '#21262d',
    color: '#c9d1d9',
    fontSize: '12px',
    marginLeft: '8px',
    whiteSpace: 'nowrap',
  },
  grid: {
    display: 'grid',
    gridTemplateColumns: 'repeat(auto-fit, minmax(260px, 1fr))',
    gap: '14px',
  },
  metricCard: {
    background: '#161b22',
    border: '1px solid #30363d',
    borderRadius: '12px',
    padding: '16px',
  },
  metricLabel: {
    color: '#8b949e',
    textTransform: 'uppercase',
    letterSpacing: '0.6px',
    fontSize: '11px',
    fontWeight: 700,
    marginBottom: '7px',
  },
  metricValue: { color: '#58a6ff', fontSize: '30px', fontWeight: 850, lineHeight: 1 },
  metricSub: { color: '#c9d1d9', fontSize: '13px', marginTop: '8px' },
  splitGrid: {
    display: 'grid',
    gridTemplateColumns: 'repeat(auto-fit, minmax(340px, 1fr))',
    gap: '14px',
    marginTop: '14px',
  },
  sectionTitle: {
    color: '#58a6ff',
    fontSize: '18px',
    fontWeight: 800,
    margin: '18px 0 10px',
  },
  row: {
    display: 'flex',
    justifyContent: 'space-between',
    gap: '12px',
    borderBottom: '1px solid #21262d',
    padding: '8px 0',
    fontSize: '14px',
  },
  key: { color: '#8b949e' },
  val: { color: '#e6edf3', fontWeight: 750, textAlign: 'right' },
  details: {
    marginTop: '14px',
    background: '#0a0f14',
    border: '1px solid #21262d',
    borderRadius: '10px',
    padding: '12px',
  },
  summary: { cursor: 'pointer', color: '#c9d1d9', fontWeight: 800 },
  noData: { color: '#8b949e', padding: '18px', textAlign: 'center' },
}

function today() {
  return new Date().toISOString().slice(0, 10)
}

function num(v, digits = 1) {
  if (v === null || v === undefined || v === '') return '—'
  const n = Number(v)
  if (!Number.isFinite(n)) return String(v)
  return n.toFixed(digits)
}

function pct(v, digits = 1) {
  if (v === null || v === undefined || v === '') return '—'
  const n = Number(v)
  if (!Number.isFinite(n)) return String(v)
  return `${(n * 100).toFixed(digits)}%`
}

function label(v) {
  if (v === null || v === undefined || v === '') return '—'
  return String(v).replace(/_/g, ' ')
}

function findModel(team, name) {
  return (team?.models || []).find(m => m?.model_name === name)
}

function isSimulationModel(model) {
  return String(model?.model_name || '').startsWith('Simulation:')
}

function StatRow({ k, v, format = 'text' }) {
  const rendered = format === 'pct' ? pct(v) : format === 'num' ? num(v) : label(v)
  return (
    <div style={s.row}>
      <span style={s.key}>{k}</span>
      <span style={s.val}>{rendered}</span>
    </div>
  )
}

function MetricCard({ labelText, value, sub, format = 'num' }) {
  const rendered = format === 'pct' ? pct(value) : format === 'text' ? label(value) : num(value)
  return (
    <div style={s.metricCard}>
      <div style={s.metricLabel}>{labelText}</div>
      <div style={s.metricValue}>{rendered}</div>
      {sub ? <div style={s.metricSub}>{sub}</div> : null}
    </div>
  )
}

function TeamProjectionPanel({ side, teamName, pitcherName, model }) {
  const inputs = model?.inputs || {}

  return (
    <div style={s.metricCard}>
      <div style={s.metricLabel}>{side} Projection</div>
      <h3 style={{ margin: '0 0 4px', color: '#e6edf3' }}>{teamName || side}</h3>
      <div style={{ color: '#8b949e', fontSize: '13px', marginBottom: '12px' }}>
        {pitcherName || 'No pitcher listed'}
        <span style={s.pill}>{model?.data_confidence || 'unknown'} confidence</span>
      </div>

      <MetricCard
        labelText="Expected Runs"
        value={inputs.expected_runs ?? model?.score}
        sub={`Raw: ${num(inputs.raw_expected_runs)}`}
      />

      <div style={{ marginTop: '12px' }}>
        <StatRow k="Win Probability" v={inputs.win_probability} format="pct" />
        <StatRow k="3+ Runs" v={inputs.team_3_plus_runs} format="pct" />
        <StatRow k="4+ Runs" v={inputs.team_4_plus_runs} format="pct" />
        <StatRow k="5+ Runs" v={inputs.team_5_plus_runs} format="pct" />
        <StatRow k="Offense Source" v={inputs.offense_source} />
        <StatRow k="Opposing Bullpen" v={inputs.opposing_bullpen_quality} />
        <StatRow k="Run Environment Index" v={inputs.run_environment_index} format="num" />
      </div>
    </div>
  )
}

function TotalProjectionPanel({ model }) {
  const inputs = model?.inputs || {}

  return (
    <div style={s.metricCard}>
      <div style={s.metricLabel}>Game Total Projection</div>
      <MetricCard
        labelText="Projected Total Runs"
        value={inputs.total_expected_runs ?? model?.score}
        sub={`Raw: ${num(inputs.raw_total_expected_runs)}`}
      />

      <div style={{ marginTop: '12px' }}>
        <StatRow k="Over 6.5" v={inputs.over_6_5} format="pct" />
        <StatRow k="Over 7.5" v={inputs.over_7_5} format="pct" />
        <StatRow k="Over 8.5" v={inputs.over_8_5} format="pct" />
        <StatRow k="Over 9.5" v={inputs.over_9_5} format="pct" />
        <StatRow k="Under 7.5" v={inputs.under_7_5} format="pct" />
        <StatRow k="Under 8.5" v={inputs.under_8_5} format="pct" />
        <StatRow k="Under 9.5" v={inputs.under_9_5} format="pct" />
        <StatRow k="Tie After Regulation" v={inputs.tie_after_regulation} format="pct" />
        <StatRow k="Environment" v={inputs.environment_label} />
      </div>
    </div>
  )
}

function DiagnosticModelCard({ model }) {
  const inputs = model?.inputs || {}
  return (
    <div style={{ ...s.metricCard, marginBottom: '10px' }}>
      <div style={{ color: '#e6edf3', fontWeight: 800 }}>
        {model?.model_name || 'Model'}
        <span style={s.pill}>{model?.status || 'N/A'}</span>
        <span style={s.pill}>Confidence: {model?.data_confidence || 'N/A'}</span>
      </div>
      <div style={{ color: '#58a6ff', fontSize: '24px', fontWeight: 850, margin: '8px 0' }}>
        {model?.score ?? '—'}
      </div>
      <div style={{ color: '#8b949e', fontSize: '13px', marginBottom: '8px' }}>
        {model?.formula || 'No formula supplied.'}
      </div>
      <details>
        <summary style={s.summary}>Inputs / missing fields</summary>
        <div style={{ ...s.grid, marginTop: '10px' }}>
          {Object.entries(inputs).map(([k, v]) => (
            <div key={k} style={{ background: '#0d1117', border: '1px solid #30363d', borderRadius: '8px', padding: '8px' }}>
              <div style={{ color: '#8b949e', fontSize: '12px' }}>{k}</div>
              <pre style={{ margin: 0, color: '#c9d1d9', whiteSpace: 'pre-wrap', fontFamily: 'inherit' }}>
                {typeof v === 'object' ? JSON.stringify(v, null, 2) : String(v ?? '—')}
              </pre>
            </div>
          ))}
        </div>
        <div style={{ color: '#8b949e', marginTop: '8px' }}>
          Missing: {(model?.missing_inputs || []).length ? model.missing_inputs.join(', ') : 'None'}
        </div>
      </details>
    </div>
  )
}

function GameProjectionCard({ game }) {
  const away = game?.teams?.away || {}
  const home = game?.teams?.home || {}

  const awayRunModel = findModel(away, 'Simulation: Away Team Run/Win Projection')
  const homeRunModel = findModel(home, 'Simulation: Home Team Run/Win Projection')
  const totalModel = findModel(away, 'Simulation: Game Total Projection') || findModel(home, 'Simulation: Game Total Projection')

  const awayInputs = awayRunModel?.inputs || {}
  const homeInputs = homeRunModel?.inputs || {}
  const totalInputs = totalModel?.inputs || {}

  const diagnosticModels = [
    ...(away.models || []),
    ...(home.models || []),
  ].filter(m => !isSimulationModel(m))

  return (
    <article style={s.gameCard}>
      <div style={s.gameHeader}>
        <div>
          <h2 style={s.matchupTitle}>
            {game?.away_team?.name || away?.team_name || 'Away'} @ {game?.home_team?.name || home?.team_name || 'Home'}
          </h2>
          <div style={s.meta}>
            Game PK: {game?.game_pk || '—'} | Time: {game?.game_time || '—'} | Venue: {game?.venue || '—'} | Status: {game?.status || '—'}
          </div>
        </div>
        <div style={{ textAlign: 'right' }}>
          <span style={s.pill}>Simulation Dashboard</span>
          <span style={s.pill}>{totalModel?.data_confidence || 'low'} confidence</span>
        </div>
      </div>

      {!awayRunModel || !homeRunModel || !totalModel ? (
        <div style={s.noData}>Simulation projections are not available for this game yet.</div>
      ) : (
        <>
          <div style={s.grid}>
            <MetricCard labelText="Projected Total" value={totalInputs.total_expected_runs ?? totalModel.score} />
            <MetricCard labelText={`${game?.away_team?.name || away?.team_name || 'Away'} Win`} value={awayInputs.win_probability} format="pct" />
            <MetricCard labelText={`${game?.home_team?.name || home?.team_name || 'Home'} Win`} value={homeInputs.win_probability} format="pct" />
            <MetricCard labelText="Over 8.5" value={totalInputs.over_8_5} format="pct" />
          </div>

          <div style={s.splitGrid}>
            <TeamProjectionPanel
              side="Away"
              teamName={game?.away_team?.name || away?.team_name}
              pitcherName={game?.away_pitcher?.name || away?.pitcher_name}
              model={awayRunModel}
            />
            <TeamProjectionPanel
              side="Home"
              teamName={game?.home_team?.name || home?.team_name}
              pitcherName={game?.home_pitcher?.name || home?.pitcher_name}
              model={homeRunModel}
            />
          </div>

          <div style={{ marginTop: '14px' }}>
            <TotalProjectionPanel model={totalModel} />
          </div>
        </>
      )}

      <details style={s.details}>
        <summary style={s.summary}>Legacy / diagnostic models</summary>
        <div style={{ marginTop: '12px' }}>
          {diagnosticModels.length ? diagnosticModels.map((model, idx) => (
            <DiagnosticModelCard key={`${model?.model_name || 'model'}-${idx}`} model={model} />
          )) : <div style={s.noData}>No diagnostic models available.</div>}
        </div>
      </details>
    </article>
  )
}

export default function ModelProjectionsPage() {
  const [date, setDate] = useState(today())
  const [payload, setPayload] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  useEffect(() => {
    let cancelled = false

    async function load() {
      setLoading(true)
      setError(null)

      try {
        const url = `${API}/models/projections?date=${date}`
        const res = await fetch(url)
        const contentType = res.headers.get('content-type') || ''

        if (!res.ok) {
          const body = await res.text()
          throw new Error(`Request failed: ${res.status} ${res.statusText}. URL: ${url}. Response: ${body.slice(0, 300)}`)
        }

        if (!contentType.includes('application/json')) {
          const body = await res.text()
          throw new Error(`Expected JSON but received ${contentType || 'unknown content type'}. URL: ${url}. Response starts with: ${body.slice(0, 120)}`)
        }

        const json = await res.json()
        if (!cancelled) setPayload(json)
      } catch (err) {
        if (!cancelled) setError(err.message)
      } finally {
        if (!cancelled) setLoading(false)
      }
    }

    load()
    return () => { cancelled = true }
  }, [date])

  const games = useMemo(() => payload?.games || [], [payload])

  return (
    <div style={s.page}>
      <header style={s.header}>
        <h1 style={s.title}>Model Projections</h1>
        <p style={s.subtitle}>
          Simulation-first projections powered by team offense priors, bullpen profiles, environment, and calibrated game outcomes.
        </p>
        <label style={{ color: '#c9d1d9' }}>
          Date:
          <input type="date" value={date} onChange={e => setDate(e.target.value)} style={s.dateInput} />
        </label>
      </header>

      {loading && <div style={s.card}>Loading projections...</div>}
      {error && <div style={{ ...s.card, borderColor: '#f85149', color: '#f85149' }}>{error}</div>}

      {payload?.source_notes?.length ? (
        <div style={s.card}>
          <strong>Source notes:</strong> {payload.source_notes.join(' ')}
        </div>
      ) : null}

      {!loading && payload && !games.length ? (
        <div style={s.card}>No games returned for this date.</div>
      ) : null}

      {games.map(game => (
        <GameProjectionCard key={game.game_pk || `${game.away_team?.name}-${game.home_team?.name}`} game={game} />
      ))}
    </div>
  )
}
