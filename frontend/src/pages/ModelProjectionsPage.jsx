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
  splitGrid: {
    display: 'grid',
    gridTemplateColumns: 'repeat(auto-fit, minmax(340px, 1fr))',
    gap: '14px',
    marginTop: '14px',
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
  tabBar: {
    display: 'flex',
    flexWrap: 'wrap',
    border: '1px solid #30363d',
    borderRadius: '10px',
    overflow: 'hidden',
    margin: '16px 0',
    width: 'fit-content',
    maxWidth: '100%',
  },
  tab: {
    border: 0,
    borderRight: '1px solid #30363d',
    background: '#0d1117',
    color: '#8b949e',
    padding: '10px 14px',
    fontWeight: 800,
    cursor: 'pointer',
  },
  tabActive: {
    background: '#58a6ff',
    color: '#0d1117',
  },
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

function GenericPanel({ title, subtitle, children }) {
  return (
    <div style={s.metricCard}>
      <div style={s.metricLabel}>{title}</div>
      {subtitle ? <h3 style={{ margin: '0 0 12px', color: '#e6edf3' }}>{subtitle}</h3> : null}
      {children}
    </div>
  )
}

function DataSection({ title, data, formatHint = {} }) {
  if (!data || typeof data !== 'object') {
    return <GenericPanel title={title}><div style={s.noData}>No data available.</div></GenericPanel>
  }

  return (
    <GenericPanel title={title}>
      {Object.entries(data).map(([key, value]) => {
        if (value && typeof value === 'object') return null
        const format = formatHint[key] || (typeof value === 'number' && Math.abs(value) <= 1 ? 'pct' : 'text')
        return <StatRow key={key} k={label(key)} v={value} format={format} />
      })}
    </GenericPanel>
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

function OverviewTab({ game, awayRunModel, homeRunModel, totalModel }) {
  const away = game?.teams?.away || {}
  const home = game?.teams?.home || {}
  const awayInputs = awayRunModel?.inputs || {}
  const homeInputs = homeRunModel?.inputs || {}
  const totalInputs = totalModel?.inputs || {}

  return (
    <>
      <div style={s.grid}>
        <MetricCard labelText="Projected Total" value={totalInputs.total_expected_runs ?? totalModel?.score} />
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
  )
}

function PitcherTab({ workspace }) {
  return (
    <div style={s.splitGrid}>
      <PitcherProfilePanel labelText="Away Starting Pitcher" profile={workspace?.awayPitcherProfile} />
      <PitcherProfilePanel labelText="Home Starting Pitcher" profile={workspace?.homePitcherProfile} />
    </div>
  )
}

function PitcherProfilePanel({ labelText, profile }) {
  const metadata = profile?.metadata || {}
  const arsenal = profile?.arsenal || {}

  return (
    <div style={s.metricCard}>
      <div style={s.metricLabel}>{labelText}</div>
      <h3 style={{ margin: '0 0 4px', color: '#e6edf3' }}>{metadata.pitcher_name || 'Unknown pitcher'}</h3>
      <div style={{ color: '#8b949e', fontSize: '13px', marginBottom: '12px' }}>
        {metadata.source_type || 'pitcher profile'}
        <span style={s.pill}>{metadata.data_confidence || 'unknown'} confidence</span>
      </div>

      <div style={s.grid}>
        <DataSection title="Bat Missing" data={profile?.bat_missing} />
        <DataSection title="Command / Control" data={profile?.command_control} />
        <DataSection title="Contact Management" data={profile?.contact_management} />
        <GenericPanel title="Arsenal">
          <StatRow k="Avg Velocity" v={arsenal.avg_velocity} format="num" />
          <StatRow k="Avg Spin Rate" v={arsenal.avg_spin_rate} format="num" />
          <details style={{ marginTop: '10px' }}>
            <summary style={s.summary}>Pitch mix</summary>
            <pre style={{ whiteSpace: 'pre-wrap', color: '#c9d1d9', fontFamily: 'inherit' }}>
              {JSON.stringify(arsenal.pitch_mix || {}, null, 2)}
            </pre>
          </details>
        </GenericPanel>
      </div>
    </div>
  )
}

function BatterTab({ workspace }) {
  return (
    <div style={s.splitGrid}>
      <OffenseProfilePanel labelText="Away Offense" profile={workspace?.awayOffenseProfile} />
      <OffenseProfilePanel labelText="Home Offense" profile={workspace?.homeOffenseProfile} />
    </div>
  )
}

function OffenseProfilePanel({ labelText, profile }) {
  const metadata = profile?.metadata || {}

  return (
    <div style={s.metricCard}>
      <div style={s.metricLabel}>{labelText}</div>
      <h3 style={{ margin: '0 0 4px', color: '#e6edf3' }}>{metadata.team_name || 'Unknown team'}</h3>
      <div style={{ color: '#8b949e', fontSize: '13px', marginBottom: '12px' }}>
        {metadata.source_type || 'offense profile'}
        <span style={s.pill}>{metadata.data_confidence || 'unknown'} confidence</span>
      </div>

      <div style={s.grid}>
        <DataSection title="Contact Skill" data={profile?.contact_skill} />
        <DataSection title="Plate Discipline" data={profile?.plate_discipline} />
        <DataSection title="Power" data={profile?.power} />
        <DataSection title="Run Creation" data={profile?.run_creation} />
      </div>
    </div>
  )
}

function EnvironmentTab({ workspace }) {
  const profile = workspace?.environmentProfile || {}
  const run = profile.run_environment || {}
  const weather = profile.weather || {}
  const metadata = profile.metadata || {}

  return (
    <div style={s.splitGrid}>
      <GenericPanel title="Run Environment" subtitle={label(run.scoring_environment_label)}>
        <StatRow k="Run Scoring Index" v={run.run_scoring_index} format="num" />
        <StatRow k="HR Boost Index" v={run.hr_boost_index} format="num" />
        <StatRow k="Hit Boost Index" v={run.hit_boost_index} format="num" />
        <StatRow k="Weather Impact" v={run.weather_run_impact} />
        <StatRow k="Wind Impact" v={run.wind_run_impact} />
      </GenericPanel>

      <GenericPanel title="Weather">
        <StatRow k="Temperature" v={weather.temperature_f} format="num" />
        <StatRow k="Condition" v={weather.condition} />
        <StatRow k="Wind Speed" v={weather.wind_speed_mph} format="num" />
        <StatRow k="Wind Direction" v={weather.wind_direction} />
      </GenericPanel>

      <DataSection title="Metadata" data={metadata} />
    </div>
  )
}

function MatchupTab({ workspace }) {
  return (
    <div style={s.splitGrid}>
      <MatchupPanel labelText="Away Offense vs Home Pitching" analysis={workspace?.awayMatchupAnalysis} />
      <MatchupPanel labelText="Home Offense vs Away Pitching" analysis={workspace?.homeMatchupAnalysis} />
    </div>
  )
}

function MatchupPanel({ labelText, analysis }) {
  const metadata = analysis?.metadata || {}
  const summary = analysis?.summary || {}
  const plate = analysis?.plate_discipline_matchup || {}
  const arsenal = analysis?.arsenal_matchup || {}

  return (
    <div style={s.metricCard}>
      <div style={s.metricLabel}>{labelText}</div>
      <h3 style={{ margin: '0 0 8px', color: '#e6edf3' }}>
        {metadata.offense_team_name || 'Offense'} vs {metadata.opposing_pitcher_name || 'Pitcher'}
      </h3>
      <StatRow k="Status" v={summary.status} />
      <StatRow k="Biggest Edge" v={summary.biggest_edge} />
      <StatRow k="Confidence" v={summary.confidence} format="pct" />
      <StatRow k="Note" v={summary.note} />

      <div style={s.grid}>
        <DataSection title="Plate Discipline Matchup" data={plate} />
        <GenericPanel title="Arsenal Matchup">
          <StatRow k="Biggest Edge" v={arsenal.biggest_edge} />
          <StatRow k="Pitch Count Used" v={arsenal.pitch_count_used} format="num" />
          <details style={{ marginTop: '10px' }}>
            <summary style={s.summary}>Pitch edges</summary>
            <pre style={{ whiteSpace: 'pre-wrap', color: '#c9d1d9', fontFamily: 'inherit' }}>
              {JSON.stringify(arsenal.pitch_edges || [], null, 2)}
            </pre>
          </details>
        </GenericPanel>
      </div>
    </div>
  )
}

function BullpenTab({ workspace }) {
  return (
    <div style={s.splitGrid}>
      <BullpenProfilePanel labelText="Away Bullpen" profile={workspace?.awayBullpenProfile} />
      <BullpenProfilePanel labelText="Home Bullpen" profile={workspace?.homeBullpenProfile} />
    </div>
  )
}

function BullpenProfilePanel({ labelText, profile }) {
  const metadata = profile?.metadata || {}

  return (
    <div style={s.metricCard}>
      <div style={s.metricLabel}>{labelText}</div>
      <h3 style={{ margin: '0 0 4px', color: '#e6edf3' }}>{metadata.team_name || 'Unknown team'}</h3>
      <div style={{ color: '#8b949e', fontSize: '13px', marginBottom: '12px' }}>
        {metadata.bullpen_profile_version || 'bullpen profile'}
        <span style={s.pill}>{label(metadata.bullpen_quality_label)}</span>
      </div>
      <div style={s.grid}>
        <DataSection title="Bat Missing" data={profile?.bat_missing} />
        <DataSection title="Command / Control" data={profile?.command_control} />
        <DataSection title="Contact Management" data={profile?.contact_management} />
        <DataSection title="Platoon Profile" data={profile?.platoon_profile} />
        <DataSection title="Arsenal" data={profile?.arsenal} />
      </div>
    </div>
  )
}

function SimulationTab({ workspace }) {
  const sim = workspace?.bullpenAdjustedGameSimulation || {}
  const totals = sim.calibrated_total_probabilities || sim.total_probabilities || {}
  const teamTotals = sim.calibrated_team_total_probabilities || sim.team_total_probabilities || {}

  return (
    <div style={s.splitGrid}>
      <GenericPanel title="Game Simulation" subtitle={sim.model_version || 'bullpen adjusted simulation'}>
        <StatRow k="Total Expected Runs" v={sim.total_expected_runs} format="num" />
        <StatRow k="Away Expected Runs" v={sim.away_expected_runs} format="num" />
        <StatRow k="Home Expected Runs" v={sim.home_expected_runs} format="num" />
        <StatRow k="Away Win Probability" v={sim.away_win_probability} format="pct" />
        <StatRow k="Home Win Probability" v={sim.home_win_probability} format="pct" />
        <StatRow k="Tie After Regulation" v={sim.tie_after_regulation_probability} format="pct" />
        <StatRow k="Dynamic Starter Exit" v={sim.dynamic_starter_exit ? 'true' : 'false'} />
      </GenericPanel>

      <GenericPanel title="Game Totals">
        <StatRow k="Over 6.5" v={totals['over_6.5']} format="pct" />
        <StatRow k="Over 7.5" v={totals['over_7.5']} format="pct" />
        <StatRow k="Over 8.5" v={totals['over_8.5']} format="pct" />
        <StatRow k="Over 9.5" v={totals['over_9.5']} format="pct" />
        <StatRow k="Under 8.5" v={totals['under_8.5']} format="pct" />
        <StatRow k="Under 9.5" v={totals['under_9.5']} format="pct" />
      </GenericPanel>

      <GenericPanel title="Team Totals">
        <StatRow k="Away 3+ Runs" v={teamTotals.away_3_plus} format="pct" />
        <StatRow k="Away 4+ Runs" v={teamTotals.away_4_plus} format="pct" />
        <StatRow k="Away 5+ Runs" v={teamTotals.away_5_plus} format="pct" />
        <StatRow k="Home 3+ Runs" v={teamTotals.home_3_plus} format="pct" />
        <StatRow k="Home 4+ Runs" v={teamTotals.home_4_plus} format="pct" />
        <StatRow k="Home 5+ Runs" v={teamTotals.home_5_plus} format="pct" />
      </GenericPanel>
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

function DiagnosticsTab({ game }) {
  const away = game?.teams?.away || {}
  const home = game?.teams?.home || {}
  const diagnosticModels = [
    ...(away.models || []),
    ...(home.models || []),
  ].filter(m => !isSimulationModel(m))

  return (
    <div>
      {diagnosticModels.length ? diagnosticModels.map((model, idx) => (
        <DiagnosticModelCard key={`${model?.model_name || 'model'}-${idx}`} model={model} />
      )) : <div style={s.noData}>No diagnostic models available.</div>}
    </div>
  )
}

const TABS = [
  ['overview', 'Overview'],
  ['pitcher', 'Pitcher'],
  ['batter', 'Batter'],
  ['environment', 'Environment'],
  ['matchup', 'Matchup Analysis'],
  ['bullpen', 'Bullpen'],
  ['simulation', 'Simulation'],
  ['diagnostics', 'Diagnostics'],
]

function GameProjectionCard({ game }) {
  const [activeTab, setActiveTab] = useState('overview')
  const away = game?.teams?.away || {}
  const home = game?.teams?.home || {}
  const workspace = game?.workspace || {}

  const awayRunModel = findModel(away, 'Simulation: Away Team Run/Win Projection')
  const homeRunModel = findModel(home, 'Simulation: Home Team Run/Win Projection')
  const totalModel = findModel(away, 'Simulation: Game Total Projection') || findModel(home, 'Simulation: Game Total Projection')

  function renderTab() {
    if (activeTab === 'overview') return <OverviewTab game={game} awayRunModel={awayRunModel} homeRunModel={homeRunModel} totalModel={totalModel} />
    if (activeTab === 'pitcher') return <PitcherTab workspace={workspace} />
    if (activeTab === 'batter') return <BatterTab workspace={workspace} />
    if (activeTab === 'environment') return <EnvironmentTab workspace={workspace} />
    if (activeTab === 'matchup') return <MatchupTab workspace={workspace} />
    if (activeTab === 'bullpen') return <BullpenTab workspace={workspace} />
    if (activeTab === 'simulation') return <SimulationTab workspace={workspace} />
    if (activeTab === 'diagnostics') return <DiagnosticsTab game={game} />
    return null
  }

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
          <span style={s.pill}>Simulation Workspace</span>
          <span style={s.pill}>{workspace?.metadata?.data_confidence || totalModel?.data_confidence || 'low'} confidence</span>
        </div>
      </div>

      <div style={s.tabBar}>
        {TABS.map(([id, name]) => (
          <button
            key={id}
            type="button"
            onClick={() => setActiveTab(id)}
            style={{
              ...s.tab,
              ...(activeTab === id ? s.tabActive : {}),
            }}
          >
            {name}
          </button>
        ))}
      </div>

      {!awayRunModel || !homeRunModel || !totalModel ? (
        <div style={s.noData}>Simulation projections are not available for this game yet.</div>
      ) : renderTab()}
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
          Full prediction workspace powered by pitcher profiles, offense profiles, environment, matchup analysis, bullpen modeling, and calibrated simulations.
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
