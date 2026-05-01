import React, { useEffect, useState } from 'react'

const card = {
  background: '#161b22',
  border: '1px solid #30363d',
  borderRadius: '12px',
  padding: '18px',
  marginBottom: '16px',
}

const pill = {
  display: 'inline-block',
  padding: '3px 8px',
  borderRadius: '999px',
  background: '#21262d',
  color: '#c9d1d9',
  fontSize: '12px',
  marginLeft: '8px',
}

function today() {
  return new Date().toISOString().slice(0, 10)
}

function value(v) {
  if (v === null || v === undefined || v === '') return 'N/A'
  if (typeof v === 'number') return Number.isInteger(v) ? String(v) : v.toFixed(3).replace(/0+$/, '').replace(/\.$/, '')
  if (typeof v === 'object') return JSON.stringify(v, null, 2)
  return String(v)
}

function ModelCard({ model }) {
  const inputs = model?.inputs || {}
  return (
    <div style={card}>
      <h4 style={{ margin: '0 0 8px', color: '#e6edf3' }}>
        {model?.model_name || 'Model'}
        <span style={pill}>{model?.status || 'N/A'}</span>
        <span style={pill}>Confidence: {model?.data_confidence || 'N/A'}</span>
      </h4>
      <div style={{ fontSize: '28px', fontWeight: 800, color: '#58a6ff', marginBottom: '10px' }}>{value(model?.score)}</div>
      <p style={{ color: '#c9d1d9', margin: '8px 0' }}><strong>Formula:</strong> {model?.formula || 'N/A'}</p>
      <details open>
        <summary style={{ cursor: 'pointer', color: '#e6edf3', fontWeight: 700 }}>Inputs</summary>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(190px, 1fr))', gap: '8px', marginTop: '10px' }}>
          {Object.entries(inputs).map(([k, v]) => (
            <div key={k} style={{ background: '#0d1117', border: '1px solid #30363d', borderRadius: '8px', padding: '8px' }}>
              <div style={{ color: '#8b949e', fontSize: '12px' }}>{k}</div>
              <pre style={{ margin: 0, color: '#c9d1d9', whiteSpace: 'pre-wrap', fontFamily: 'inherit' }}>{value(v)}</pre>
            </div>
          ))}
        </div>
      </details>
      <div style={{ marginTop: '12px', color: '#c9d1d9' }}>
        <strong>Calculation steps:</strong>
        <ol>{(model?.calculation_steps || []).map((step, i) => <li key={i}>{step}</li>)}</ol>
      </div>
      <div style={{ color: '#c9d1d9' }}><strong>Missing inputs:</strong> {(model?.missing_inputs || []).length ? model.missing_inputs.join(', ') : 'None'}</div>
      <div style={{ color: '#8b949e', marginTop: '8px' }}><strong>Source notes:</strong> {(model?.source_notes || []).join(' ') || 'N/A'}</div>
    </div>
  )
}

function TeamBlock({ label, team }) {
  return (
    <section style={{ marginTop: '18px' }}>
      <h3 style={{ color: '#e6edf3', marginBottom: '8px' }}>{label}: {team?.team_name || 'N/A'} <span style={pill}>{team?.pitcher_name || 'No pitcher'}</span></h3>
      {(team?.models || []).map((model, idx) => <ModelCard key={`${label}-${idx}-${model?.model_name}`} model={model} />)}
    </section>
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
        const res = await fetch(`/models/projections?date=${date}`)
        if (!res.ok) throw new Error(`Request failed: ${res.status}`)
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

  return (
    <div>
      <header style={{ marginBottom: '24px' }}>
        <h1 style={{ margin: 0, color: '#e6edf3' }}>Model Projections</h1>
        <p style={{ color: '#8b949e' }}>Every game, every model, with formulas, inputs, calculation steps, missing inputs, confidence, and source notes.</p>
        <label style={{ color: '#c9d1d9' }}>Date: <input type="date" value={date} onChange={e => setDate(e.target.value)} style={{ background: '#0d1117', color: '#e6edf3', border: '1px solid #30363d', borderRadius: '8px', padding: '8px' }} /></label>
      </header>
      {loading && <div style={card}>Loading projections...</div>}
      {error && <div style={{ ...card, borderColor: '#f85149', color: '#f85149' }}>{error}</div>}
      {payload?.source_notes?.length ? <div style={card}><strong>Source notes:</strong> {payload.source_notes.join(' ')}</div> : null}
      {(payload?.games || []).map(game => (
        <article key={game.game_pk || `${game.away_team?.name}-${game.home_team?.name}`} style={{ ...card, background: '#0d1117' }}>
          <h2 style={{ marginTop: 0, color: '#e6edf3' }}>{game.away_team?.name || 'Away'} @ {game.home_team?.name || 'Home'}</h2>
          <div style={{ color: '#8b949e', marginBottom: '12px' }}>Game PK: {value(game.game_pk)} | Time: {value(game.game_time)} | Venue: {value(game.venue)} | Status: {value(game.status)}</div>
          <TeamBlock label="Away" team={game.teams?.away} />
          <TeamBlock label="Home" team={game.teams?.home} />
        </article>
      ))}
      {!loading && payload && !(payload.games || []).length && <div style={card}>No games returned for this date.</div>}
    </div>
  )
}
