import React, { useState } from 'react'

const API = import.meta.env.VITE_API_BASE_URL || ''

const PROMPT_CHIPS = [
  "Summarize today's slate",
  'What is the strongest model edge?',
  'Which games are missing data?',
  'Best Stored 365 hitter matchups',
  'Top pitcher leans',
  'Explain this matchup',
  'What does Daily Odds tell us?',
  'Which game has the cleanest signal?',
  'Show me the best hitter vs pitcher spots',
  'What data is stale or weak?',
]

export default function AIPage() {
  const today = new Date().toISOString().slice(0, 10)
  const [message, setMessage] = useState("Summarize today's slate")
  const [date, setDate] = useState(today)
  const [gamePk, setGamePk] = useState('')
  const [playerId, setPlayerId] = useState('')
  const [response, setResponse] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  async function ask(nextMessage = message) {
    setLoading(true)
    setError(null)

    try {
      const payload = {
        message: nextMessage,
        date: date || null,
        game_pk: gamePk ? Number(gamePk) : null,
        player_id: playerId ? Number(playerId) : null,
      }

      const res = await fetch(`${API}/ai-data-assistant`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      })

      if (!res.ok) {
        const text = await res.text()
        throw new Error(text || `Request failed (${res.status})`)
      }

      const json = await res.json()
      setResponse(json)
    } catch (err) {
      console.error('AI Data Assistant request failed:', err)
      setError(err.message || 'Request failed')
      setResponse(null)
    } finally {
      setLoading(false)
    }
  }

  function runChip(chip) {
    setMessage(chip)
    ask(chip)
  }

  return (
    <div>
      <h1 style={{ fontSize: '28px', marginBottom: '8px' }}>AI Data Assistant</h1>
      <p style={{ color: '#8b949e', marginBottom: '18px', lineHeight: 1.5 }}>
        Ask the app to explain matchups, model edges, Daily Odds, Stored 365 hitter spots, and data quality using only app-owned data.
      </p>

      <div style={{
        background: '#161b22',
        border: '1px solid #30363d',
        borderRadius: '12px',
        padding: '16px',
        marginBottom: '16px'
      }}>
        <div style={{ display: 'flex', gap: '10px', marginBottom: '12px', flexWrap: 'wrap' }}>
          <label style={{ color: '#8b949e', fontSize: '13px' }}>
            Date
            <input
              type="date"
              value={date}
              onChange={e => setDate(e.target.value)}
              style={inputStyle}
            />
          </label>
          <label style={{ color: '#8b949e', fontSize: '13px' }}>
            Game PK
            <input
              value={gamePk}
              onChange={e => setGamePk(e.target.value)}
              placeholder="optional"
              style={{ ...inputStyle, width: '140px' }}
            />
          </label>
          <label style={{ color: '#8b949e', fontSize: '13px' }}>
            Player ID
            <input
              value={playerId}
              onChange={e => setPlayerId(e.target.value)}
              placeholder="optional"
              style={{ ...inputStyle, width: '140px' }}
            />
          </label>
        </div>

        <div style={{ display: 'flex', gap: '8px', marginBottom: '14px' }}>
          <input
            value={message}
            onChange={e => setMessage(e.target.value)}
            onKeyDown={e => { if (e.key === 'Enter') ask() }}
            style={{
              flex: 1,
              background: '#0d1117',
              color: '#e6edf3',
              border: '1px solid #30363d',
              borderRadius: '8px',
              padding: '12px'
            }}
          />
          <button onClick={() => ask()} disabled={loading}>
            {loading ? 'Asking...' : 'Ask'}
          </button>
        </div>

        <div style={{ display: 'flex', gap: '8px', flexWrap: 'wrap' }}>
          {PROMPT_CHIPS.map(chip => (
            <button key={chip} onClick={() => runChip(chip)} disabled={loading} style={chipStyle}>
              {chip}
            </button>
          ))}
        </div>
      </div>

      {error && (
        <div style={{
          background: '#2d1b1b',
          border: '1px solid #a33',
          borderRadius: '8px',
          padding: '14px',
          marginBottom: '12px'
        }}>
          <strong>Error:</strong> {error}
        </div>
      )}

      {response && (
        <div style={{
          background: '#161b22',
          border: '1px solid #30363d',
          borderRadius: '12px',
          padding: '16px'
        }}>
          <div style={{ fontWeight: 700, marginBottom: '8px' }}>Answer</div>
          <div style={{ whiteSpace: 'pre-wrap', lineHeight: 1.55, marginBottom: '16px' }}>
            {response.answer}
          </div>

          <div style={metaGridStyle}>
            <MetaCard title="Intent" value={response.intent || 'unknown'} />
            <MetaCard title="Sources used" value={(response.sources_used || []).join(', ') || 'None'} />
            <MetaCard title="Confidence note" value={response.confidence_note || 'None'} />
          </div>

          <details style={{ marginTop: '14px' }}>
            <summary style={{ cursor: 'pointer', color: '#58a6ff' }}>Data quality and missing data</summary>
            <pre style={preStyle}>{JSON.stringify({ data_quality: response.data_quality, missing_data: response.missing_data }, null, 2)}</pre>
          </details>
        </div>
      )}
    </div>
  )
}

function MetaCard({ title, value }) {
  return (
    <div style={{ background: '#0d1117', border: '1px solid #30363d', borderRadius: '8px', padding: '10px' }}>
      <div style={{ color: '#8b949e', fontSize: '12px', marginBottom: '6px' }}>{title}</div>
      <div style={{ color: '#e6edf3', fontSize: '13px', overflowWrap: 'anywhere' }}>{value}</div>
    </div>
  )
}

const inputStyle = {
  display: 'block',
  marginTop: '6px',
  background: '#0d1117',
  color: '#e6edf3',
  border: '1px solid #30363d',
  borderRadius: '8px',
  padding: '9px'
}

const chipStyle = {
  background: '#0d1117',
  color: '#58a6ff',
  border: '1px solid #30363d',
  borderRadius: '999px',
  padding: '8px 10px',
  cursor: 'pointer'
}

const metaGridStyle = {
  display: 'grid',
  gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))',
  gap: '10px'
}

const preStyle = {
  marginTop: '12px',
  background: '#0d1117',
  borderRadius: '6px',
  padding: '10px',
  overflowX: 'auto',
  fontSize: '12px'
}
