import React, { useState } from 'react'
import { API_BASE, getMlbLiveDate } from '../lib/api'

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
  const today = getMlbLiveDate()
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

      const res = await fetch(`${API_BASE}/ai-data-assistant`, {
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

  const dataUsed = response?.data_used || response?.sources_used || []
  const primaryRecommendations = response?.primary_recommendations || []
  const watchlist = response?.watchlist || []
  const warnings = response?.warnings || []

  return (
    <div>
      <h1 style={{ fontSize: '28px', marginBottom: '8px' }}>AI Data Assistant</h1>
      <p style={{ color: '#8b949e', marginBottom: '18px', lineHeight: 1.5 }}>
        Ask the app to explain DK + model-projection edges, Daily Odds, Stored 365 hitter spots, and data quality using only app-owned data.
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
            <MetaCard title="Data used" value={dataUsed.join(', ') || 'None'} />
            <MetaCard title="Confidence note" value={response.confidence_note || 'None'} />
          </div>

          <SectionList
            title="Primary recommendations"
            items={primaryRecommendations}
            emptyMessage="No primary recommendations are currently supported by the app-owned evidence packet."
          />

          <SectionList
            title="Watchlist"
            items={watchlist}
            emptyMessage="No additional watchlist angles are currently available."
          />

          <SectionList
            title="Warnings"
            items={warnings}
            emptyMessage="No warnings were flagged."
            renderItem={(item) => typeof item === 'string' ? item : JSON.stringify(item)}
          />

          <details style={{ marginTop: '14px' }}>
            <summary style={{ cursor: 'pointer', color: '#58a6ff' }}>Data quality and missing data</summary>
            <pre style={preStyle}>{JSON.stringify({
              data_quality: response.data_quality,
              missing_data: response.missing_data,
              context_preview: response.context_preview,
              trace_logging: response.trace_logging,
            }, null, 2)}</pre>
          </details>
        </div>
      )}
    </div>
  )
}

function SectionList({ title, items, emptyMessage, renderItem }) {
  return (
    <div style={{ marginTop: '16px' }}>
      <div style={{ fontWeight: 700, marginBottom: '8px' }}>{title}</div>
      {!items?.length ? (
        <div style={emptyStateStyle}>{emptyMessage}</div>
      ) : (
        <div style={listStyle}>
          {items.map((item, index) => (
            <div key={`${title}-${index}`} style={listItemStyle}>
              {renderItem ? renderItem(item) : formatStructuredItem(item)}
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

function formatStructuredItem(item) {
  if (!item) return 'Unknown item'
  if (typeof item === 'string') return item

  const pieces = []
  const label = item.label || item.selection || item.player_name || item.team
  if (label) pieces.push(label)
  if (item.market) pieces.push(item.market)
  if (item.score !== undefined && item.score !== null) pieces.push(`score ${formatValue(item.score)}`)
  if (item.confidence_tier) pieces.push(item.confidence_tier)
  if (item.expected_value !== undefined && item.expected_value !== null) pieces.push(`EV ${formatValue(item.expected_value)}`)
  if (item.price !== undefined && item.price !== null && item.price !== '') pieces.push(`price ${item.price}`)

  const reasons = Array.isArray(item.reasons) ? item.reasons.slice(0, 3).join('; ') : ''
  return reasons ? `${pieces.join(' | ')} - ${reasons}` : pieces.join(' | ')
}

function formatValue(value) {
  if (typeof value === 'number') {
    return Number.isInteger(value) ? String(value) : value.toFixed(3).replace(/0+$/, '').replace(/\.$/, '')
  }
  return String(value)
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

const listStyle = {
  display: 'grid',
  gap: '8px'
}

const listItemStyle = {
  background: '#0d1117',
  border: '1px solid #30363d',
  borderRadius: '8px',
  padding: '10px',
  color: '#e6edf3',
  fontSize: '13px',
  lineHeight: 1.5,
  overflowWrap: 'anywhere'
}

const emptyStateStyle = {
  background: '#0d1117',
  border: '1px dashed #30363d',
  borderRadius: '8px',
  padding: '10px',
  color: '#8b949e',
  fontSize: '13px'
}

const preStyle = {
  marginTop: '12px',
  background: '#0d1117',
  borderRadius: '6px',
  padding: '10px',
  overflowX: 'auto',
  fontSize: '12px'
}
