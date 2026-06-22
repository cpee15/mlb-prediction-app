import React, { useEffect, useMemo, useRef, useState } from 'react'
import { API_BASE, getMlbLiveDate } from '../lib/api'

const STORAGE_KEY = 'mlbgpt-ai-data-assistant-chat-v2'
const PROMPT_CHIPS = [
  "What’s the strongest DK/model edge today?",
  'Which game has the cleanest signal?',
  'Give me 3 angles worth watching tonight.',
  'Explain why the model likes this side.',
  'What props look interesting, even if they’re just watchlist spots?',
]

const WELCOME_MESSAGE = {
  id: 'welcome',
  role: 'assistant',
  answer:
    "I’m your MLB analyst for DK + model-projection reads, Daily Odds context, Stored 365 matchup flags, and data-quality checks. Ask me what stands out, what’s thin, or why the model likes a side, and I’ll keep it grounded in the app’s own data.",
  response: {
    confidence_note: 'App-owned data only. I can sound conversational, but I do not invent baseball facts.',
    data_used: ['matchups', 'model_projections', 'daily_odds_models'],
    primary_recommendations: [],
    watchlist: [],
    warnings: [],
    missing_data: [],
  },
  createdAt: Date.now(),
}

function loadStoredMessages() {
  if (typeof window === 'undefined') return [WELCOME_MESSAGE]
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY)
    if (!raw) return [WELCOME_MESSAGE]
    const parsed = JSON.parse(raw)
    if (!Array.isArray(parsed) || parsed.length === 0) return [WELCOME_MESSAGE]
    return parsed
  } catch {
    return [WELCOME_MESSAGE]
  }
}

function sanitizeMessagesForStorage(messages) {
  return messages
    .filter(message => !message.pending)
    .slice(-24)
    .map(message => ({
      id: message.id,
      role: message.role,
      content: message.content,
      answer: message.answer,
      createdAt: message.createdAt,
      llmMode: message.llmMode,
      response: message.response,
    }))
}

function messageText(message) {
  return (message?.role === 'assistant' ? message?.answer : message?.content) || ''
}

function buildConversationPayload(messages) {
  return messages
    .filter(message => !message.pending)
    .slice(-8)
    .map(message => ({
      role: message.role === 'assistant' ? 'assistant' : 'user',
      content: messageText(message),
    }))
    .filter(turn => turn.content)
}

export default function AIPage() {
  const today = getMlbLiveDate()
  const [draft, setDraft] = useState('')
  const [date, setDate] = useState(today)
  const [gamePk, setGamePk] = useState('')
  const [playerId, setPlayerId] = useState('')
  const [messages, setMessages] = useState(() => loadStoredMessages())
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [llmConfigured, setLlmConfigured] = useState(null)
  const [useLlm, setUseLlm] = useState(true)
  const [healthLoaded, setHealthLoaded] = useState(false)
  const chatEndRef = useRef(null)

  useEffect(() => {
    let ignore = false
    async function loadHealth() {
      try {
        const res = await fetch(`${API_BASE}/ai-data-assistant/health`)
        const json = await res.json()
        if (ignore) return
        setLlmConfigured(Boolean(json.llm_configured))
        setUseLlm(Boolean(json.default_use_llm))
      } catch {
        if (!ignore) {
          setLlmConfigured(false)
          setUseLlm(false)
        }
      } finally {
        if (!ignore) setHealthLoaded(true)
      }
    }
    loadHealth()
    return () => {
      ignore = true
    }
  }, [])

  useEffect(() => {
    if (typeof window === 'undefined') return
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(sanitizeMessagesForStorage(messages)))
  }, [messages])

  useEffect(() => {
    chatEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages, loading])

  const llmModeLabel = useMemo(() => {
    if (!healthLoaded) return 'Checking LLM availability…'
    if (!llmConfigured) return 'Deterministic mode only'
    return useLlm ? 'LLM polish on' : 'LLM polish off'
  }, [healthLoaded, llmConfigured, useLlm])

  async function ask(nextMessage = draft) {
    const trimmed = (nextMessage || '').trim()
    if (!trimmed || loading) return

    const userMessage = {
      id: `user-${Date.now()}`,
      role: 'user',
      content: trimmed,
      createdAt: Date.now(),
    }
    const pendingId = `assistant-${Date.now()}`
    const pendingAssistant = {
      id: pendingId,
      role: 'assistant',
      pending: true,
      answer: '',
      createdAt: Date.now(),
    }
    const historyForPayload = buildConversationPayload(messages)

    setLoading(true)
    setError(null)
    setDraft('')
    setMessages(prev => [...prev, userMessage, pendingAssistant])

    try {
      const payload = {
        message: trimmed,
        date: date || null,
        game_pk: gamePk ? Number(gamePk) : null,
        player_id: playerId ? Number(playerId) : null,
        use_llm: Boolean(llmConfigured && useLlm),
        conversation: historyForPayload,
      }

      const res = await fetch(`${API_BASE}/ai-data-assistant`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      })

      const json = await res.json()
      if (!res.ok) {
        throw new Error(typeof json?.detail === 'string' ? json.detail : JSON.stringify(json?.detail || json))
      }

      const assistantMessage = {
        id: pendingId,
        role: 'assistant',
        answer: json.answer || 'No answer returned.',
        response: json,
        llmMode: json.llm_mode,
        createdAt: Date.now(),
      }

      setMessages(prev => prev.map(message => (message.id === pendingId ? assistantMessage : message)))
    } catch (err) {
      const fallbackMessage = {
        id: pendingId,
        role: 'assistant',
        answer: `I hit an error trying to answer that: ${err.message || 'Request failed'}`,
        response: {
          warnings: [err.message || 'Request failed'],
          missing_data: [],
          primary_recommendations: [],
          watchlist: [],
          data_used: [],
          confidence_note: 'No app response was returned for this turn.',
        },
        createdAt: Date.now(),
      }
      setError(err.message || 'Request failed')
      setMessages(prev => prev.map(message => (message.id === pendingId ? fallbackMessage : message)))
    } finally {
      setLoading(false)
    }
  }

  function clearChat() {
    setMessages([WELCOME_MESSAGE])
    setError(null)
  }

  return (
    <div style={pageStyle}>
      <div style={heroStyle}>
        <div>
          <div style={eyebrowStyle}>MLB GPT Analyst Chat</div>
          <h1 style={titleStyle}>AI Data Assistant</h1>
          <p style={subtitleStyle}>
            Real chat UI, DK/model-projection-first reasoning, and app-owned evidence only.
          </p>
        </div>
        <div style={heroBadgeRowStyle}>
          <ModeBadge label={llmModeLabel} active={Boolean(llmConfigured && useLlm)} muted={!llmConfigured} />
          <ModeBadge label={date} active={false} muted={false} />
        </div>
      </div>

      <div style={controlsBarStyle}>
        <label style={controlLabelStyle}>
          Date
          <input type="date" value={date} onChange={e => setDate(e.target.value)} style={controlInputStyle} />
        </label>
        <label style={controlLabelStyle}>
          Game PK
          <input value={gamePk} onChange={e => setGamePk(e.target.value)} placeholder="optional" style={smallInputStyle} />
        </label>
        <label style={controlLabelStyle}>
          Player ID
          <input value={playerId} onChange={e => setPlayerId(e.target.value)} placeholder="optional" style={smallInputStyle} />
        </label>
        <label style={toggleWrapStyle}>
          <input
            type="checkbox"
            checked={Boolean(llmConfigured && useLlm)}
            disabled={!llmConfigured}
            onChange={e => setUseLlm(e.target.checked)}
          />
          <span>Use LLM polish</span>
        </label>
        <button onClick={clearChat} style={secondaryButtonStyle}>Clear chat</button>
      </div>

      <div style={chatShellStyle}>
        <div style={chatHeaderStyle}>
          <div>
            <div style={chatTitleStyle}>Talk to the MLB analyst</div>
            <div style={chatSubtitleStyle}>
              Ask for strongest edges, cleanest signals, watchlist props, or missing-data problems.
            </div>
          </div>
          <div style={statusTextStyle}>
            {error ? `Last error: ${error}` : 'Ready'}
          </div>
        </div>

        <div style={starterPromptWrapStyle}>
          {PROMPT_CHIPS.map(chip => (
            <button key={chip} onClick={() => ask(chip)} disabled={loading} style={starterChipStyle}>
              {chip}
            </button>
          ))}
        </div>

        <div style={chatHistoryStyle}>
          {messages.map(message => (
            <ChatMessage key={message.id} message={message} />
          ))}
          {loading && <TypingBubble />}
          <div ref={chatEndRef} />
        </div>

        <div style={composerStyle}>
          <textarea
            value={draft}
            onChange={e => setDraft(e.target.value)}
            onKeyDown={e => {
              if (e.key === 'Enter' && !e.shiftKey) {
                e.preventDefault()
                ask()
              }
            }}
            placeholder="Ask what stands out, what the model likes, what props are worth a watch, or where the data is thin…"
            style={composerInputStyle}
          />
          <div style={composerFooterStyle}>
            <div style={composerHintStyle}>Enter to send • Shift+Enter for a new line</div>
            <button onClick={() => ask()} disabled={loading || !draft.trim()} style={sendButtonStyle}>
              {loading ? 'Thinking…' : 'Send'}
            </button>
          </div>
        </div>
      </div>
    </div>
  )
}

function ChatMessage({ message }) {
  const isUser = message.role === 'user'
  const response = message.response || {}
  const primaryRecommendations = response.primary_recommendations || []
  const watchlist = response.watchlist || []
  const warnings = response.warnings || []
  const dataUsed = response.data_used || response.sources_used || []
  const missingData = response.missing_data || []
  const llmMode = message.llmMode || response.llm_mode

  return (
    <div style={{ ...messageRowStyle, justifyContent: isUser ? 'flex-end' : 'flex-start' }}>
      <div style={{ ...bubbleStyle, ...(isUser ? userBubbleStyle : assistantBubbleStyle) }}>
        <div style={bubbleMetaStyle}>
          <span style={bubbleRoleStyle}>{isUser ? 'You' : 'AI Data Assistant'}</span>
          {!isUser && llmMode && (
            <span style={bubbleTagStyle}>
              {llmMode.active ? 'LLM polished' : llmMode.configured ? 'Deterministic reply' : 'LLM unavailable'}
            </span>
          )}
        </div>

        <div style={{ whiteSpace: 'pre-wrap', lineHeight: 1.6, color: isUser ? '#f8fafc' : '#dbe7ff' }}>
          {isUser ? message.content : message.answer}
        </div>

        {!isUser && (
          <div style={assistantSectionsWrapStyle}>
            <StructuredSection title="Primary recommendations" items={primaryRecommendations} emptyMessage="No actionable recommendations were surfaced for this turn." />
            <StructuredSection title="Watchlist" items={watchlist} emptyMessage="No extra watchlist angles were surfaced for this turn." />
            <details style={detailsStyle}>
              <summary style={detailsSummaryStyle}>Data used, warnings, and confidence</summary>
              <div style={detailsBodyStyle}>
                <StructuredInlineList title="Data used" items={dataUsed} emptyMessage="No sources listed." />
                <StructuredInlineList title="Warnings" items={warnings} emptyMessage="No warnings flagged." />
                <StructuredInlineList
                  title="Missing data"
                  items={missingData.map(item => (typeof item === 'string' ? item : JSON.stringify(item)))}
                  emptyMessage="No missing-data flags listed."
                />
                <div style={confidenceNoteStyle}>{response.confidence_note || 'No confidence note returned.'}</div>
              </div>
            </details>
          </div>
        )}
      </div>
    </div>
  )
}

function TypingBubble() {
  return (
    <div style={{ ...messageRowStyle, justifyContent: 'flex-start' }}>
      <div style={{ ...bubbleStyle, ...assistantBubbleStyle, maxWidth: 180 }}>
        <div style={bubbleMetaStyle}>
          <span style={bubbleRoleStyle}>AI Data Assistant</span>
        </div>
        <div style={typingWrapStyle}>
          <span style={typingDotStyle}>•</span>
          <span style={typingDotStyle}>•</span>
          <span style={typingDotStyle}>•</span>
        </div>
      </div>
    </div>
  )
}

function StructuredSection({ title, items, emptyMessage }) {
  return (
    <details style={detailsStyle}>
      <summary style={detailsSummaryStyle}>{title}</summary>
      <div style={detailsBodyStyle}>
        {!items?.length ? (
          <div style={emptyCopyStyle}>{emptyMessage}</div>
        ) : (
          <div style={structuredGridStyle}>
            {items.map((item, index) => (
              <div key={`${title}-${index}`} style={structuredCardStyle}>
                {formatStructuredItem(item)}
              </div>
            ))}
          </div>
        )}
      </div>
    </details>
  )
}

function StructuredInlineList({ title, items, emptyMessage }) {
  return (
    <div style={{ marginBottom: 12 }}>
      <div style={inlineTitleStyle}>{title}</div>
      {!items?.length ? (
        <div style={emptyCopyStyle}>{emptyMessage}</div>
      ) : (
        <div style={inlineListStyle}>
          {items.map((item, index) => (
            <span key={`${title}-${index}`} style={inlineChipStyle}>{String(item)}</span>
          ))}
        </div>
      )}
    </div>
  )
}

function ModeBadge({ label, active, muted }) {
  return (
    <div
      style={{
        ...modeBadgeStyle,
        background: active ? 'rgba(34,197,94,0.16)' : muted ? 'rgba(148,163,184,0.12)' : 'rgba(56,189,248,0.12)',
        borderColor: active ? 'rgba(34,197,94,0.36)' : muted ? 'rgba(148,163,184,0.24)' : 'rgba(56,189,248,0.32)',
        color: active ? '#bbf7d0' : muted ? '#94a3b8' : '#bae6fd',
      }}
    >
      {label}
    </div>
  )
}

function formatStructuredItem(item) {
  if (!item) return 'Unknown item'
  if (typeof item === 'string') return item

  const lines = []
  const head = [item.label || item.selection || item.player_name || item.team, item.market].filter(Boolean).join(' • ')
  if (head) lines.push(head)

  const meta = []
  if (item.score !== undefined && item.score !== null) meta.push(`score ${formatValue(item.score)}`)
  if (item.confidence_tier) meta.push(item.confidence_tier)
  if (item.expected_value !== undefined && item.expected_value !== null) meta.push(`EV ${formatValue(item.expected_value)}`)
  if (item.price !== undefined && item.price !== null && item.price !== '') meta.push(`price ${item.price}`)
  if (meta.length) lines.push(meta.join(' | '))

  const reasons = Array.isArray(item.reasons) ? item.reasons.slice(0, 3) : Array.isArray(item.drivers) ? item.drivers.slice(0, 3) : []
  if (reasons.length) lines.push(reasons.join(' • '))

  return lines.join('\n')
}

function formatValue(value) {
  if (typeof value === 'number') {
    return Number.isInteger(value) ? String(value) : value.toFixed(3).replace(/0+$/, '').replace(/\.$/, '')
  }
  return String(value)
}

const pageStyle = {
  minHeight: '100vh',
  background: 'radial-gradient(circle at top, #14213a 0%, #090d18 45%, #060913 100%)',
  color: '#eef4ff',
  padding: '28px 18px 36px',
}

const heroStyle = {
  maxWidth: 1200,
  margin: '0 auto 18px',
  display: 'flex',
  justifyContent: 'space-between',
  gap: 18,
  alignItems: 'flex-start',
  flexWrap: 'wrap',
}

const eyebrowStyle = {
  fontSize: 12,
  letterSpacing: '0.18em',
  textTransform: 'uppercase',
  color: '#7dd3fc',
  marginBottom: 8,
}

const titleStyle = {
  margin: 0,
  fontSize: '34px',
  lineHeight: 1.05,
}

const subtitleStyle = {
  margin: '10px 0 0',
  maxWidth: 760,
  color: '#a8b6d9',
  lineHeight: 1.6,
}

const heroBadgeRowStyle = {
  display: 'flex',
  gap: 10,
  flexWrap: 'wrap',
}

const modeBadgeStyle = {
  padding: '9px 12px',
  borderRadius: 999,
  border: '1px solid transparent',
  fontSize: 12,
  fontWeight: 700,
}

const controlsBarStyle = {
  maxWidth: 1200,
  margin: '0 auto 18px',
  display: 'flex',
  gap: 12,
  flexWrap: 'wrap',
  alignItems: 'flex-end',
  padding: 16,
  borderRadius: 18,
  border: '1px solid rgba(148,163,184,0.16)',
  background: 'rgba(10, 16, 30, 0.78)',
  boxShadow: '0 20px 60px rgba(0,0,0,0.28)',
}

const controlLabelStyle = {
  display: 'grid',
  gap: 6,
  color: '#9fb0d3',
  fontSize: 12,
}

const controlInputStyle = {
  background: '#07101f',
  color: '#ecf4ff',
  border: '1px solid rgba(148,163,184,0.24)',
  borderRadius: 12,
  padding: '10px 12px',
}

const smallInputStyle = {
  ...controlInputStyle,
  width: 120,
}

const toggleWrapStyle = {
  display: 'flex',
  alignItems: 'center',
  gap: 8,
  color: '#d7e3ff',
  fontSize: 13,
  paddingBottom: 10,
}

const secondaryButtonStyle = {
  border: '1px solid rgba(148,163,184,0.24)',
  background: 'rgba(15,23,42,0.82)',
  color: '#dbeafe',
  borderRadius: 12,
  padding: '10px 14px',
  cursor: 'pointer',
}

const chatShellStyle = {
  maxWidth: 1200,
  margin: '0 auto',
  borderRadius: 24,
  border: '1px solid rgba(148,163,184,0.16)',
  background: 'linear-gradient(180deg, rgba(9, 14, 25, 0.94), rgba(5, 9, 17, 0.98))',
  boxShadow: '0 28px 90px rgba(0,0,0,0.34)',
  overflow: 'hidden',
}

const chatHeaderStyle = {
  display: 'flex',
  justifyContent: 'space-between',
  gap: 16,
  alignItems: 'center',
  padding: '20px 22px 14px',
  borderBottom: '1px solid rgba(148,163,184,0.12)',
}

const chatTitleStyle = {
  fontSize: 18,
  fontWeight: 700,
  color: '#f5f9ff',
}

const chatSubtitleStyle = {
  marginTop: 4,
  color: '#8fa4ca',
  fontSize: 13,
}

const statusTextStyle = {
  fontSize: 12,
  color: '#8fa4ca',
}

const starterPromptWrapStyle = {
  display: 'flex',
  gap: 10,
  flexWrap: 'wrap',
  padding: '14px 22px 0',
}

const starterChipStyle = {
  borderRadius: 999,
  border: '1px solid rgba(56,189,248,0.22)',
  background: 'rgba(56,189,248,0.10)',
  color: '#dff7ff',
  padding: '9px 12px',
  cursor: 'pointer',
  fontSize: 13,
}

const chatHistoryStyle = {
  padding: '20px 22px',
  minHeight: '52vh',
  maxHeight: '58vh',
  overflowY: 'auto',
  display: 'grid',
  gap: 14,
}

const messageRowStyle = {
  display: 'flex',
}

const bubbleStyle = {
  maxWidth: '78%',
  borderRadius: 22,
  padding: '14px 16px',
  boxShadow: '0 12px 32px rgba(0,0,0,0.22)',
}

const userBubbleStyle = {
  background: 'linear-gradient(135deg, #1d4ed8, #2563eb)',
  border: '1px solid rgba(147,197,253,0.28)',
}

const assistantBubbleStyle = {
  background: 'linear-gradient(180deg, rgba(12,21,38,0.96), rgba(8,14,28,0.98))',
  border: '1px solid rgba(148,163,184,0.16)',
}

const bubbleMetaStyle = {
  display: 'flex',
  gap: 8,
  alignItems: 'center',
  marginBottom: 8,
  flexWrap: 'wrap',
}

const bubbleRoleStyle = {
  fontSize: 12,
  fontWeight: 700,
  letterSpacing: '0.04em',
  textTransform: 'uppercase',
  color: '#9fb7ff',
}

const bubbleTagStyle = {
  fontSize: 11,
  color: '#dff7ff',
  borderRadius: 999,
  padding: '4px 8px',
  background: 'rgba(56,189,248,0.10)',
  border: '1px solid rgba(56,189,248,0.18)',
}

const assistantSectionsWrapStyle = {
  marginTop: 14,
  display: 'grid',
  gap: 10,
}

const detailsStyle = {
  borderRadius: 16,
  background: 'rgba(255,255,255,0.02)',
  border: '1px solid rgba(148,163,184,0.10)',
}

const detailsSummaryStyle = {
  cursor: 'pointer',
  padding: '10px 12px',
  color: '#dbeafe',
  fontWeight: 600,
}

const detailsBodyStyle = {
  padding: '0 12px 12px',
}

const structuredGridStyle = {
  display: 'grid',
  gap: 8,
}

const structuredCardStyle = {
  background: 'rgba(8,14,28,0.9)',
  border: '1px solid rgba(148,163,184,0.12)',
  borderRadius: 14,
  padding: '10px 12px',
  color: '#dbeafe',
  whiteSpace: 'pre-wrap',
  lineHeight: 1.5,
  fontSize: 13,
}

const inlineTitleStyle = {
  fontSize: 12,
  fontWeight: 700,
  color: '#9fb7ff',
  marginBottom: 6,
  textTransform: 'uppercase',
  letterSpacing: '0.05em',
}

const inlineListStyle = {
  display: 'flex',
  flexWrap: 'wrap',
  gap: 8,
}

const inlineChipStyle = {
  display: 'inline-flex',
  alignItems: 'center',
  borderRadius: 999,
  padding: '6px 10px',
  background: 'rgba(148,163,184,0.10)',
  border: '1px solid rgba(148,163,184,0.16)',
  color: '#dbeafe',
  fontSize: 12,
}

const emptyCopyStyle = {
  color: '#8fa4ca',
  fontSize: 13,
}

const confidenceNoteStyle = {
  marginTop: 8,
  color: '#c4d4f4',
  fontSize: 13,
  lineHeight: 1.5,
}

const typingWrapStyle = {
  display: 'flex',
  gap: 8,
  color: '#9fb7ff',
  fontSize: 20,
}

const typingDotStyle = {
  lineHeight: 1,
}

const composerStyle = {
  borderTop: '1px solid rgba(148,163,184,0.12)',
  padding: '16px 18px 18px',
  background: 'rgba(5, 9, 17, 0.98)',
}

const composerInputStyle = {
  width: '100%',
  minHeight: 86,
  resize: 'vertical',
  borderRadius: 18,
  border: '1px solid rgba(148,163,184,0.16)',
  background: 'rgba(11, 18, 32, 0.98)',
  color: '#eff6ff',
  padding: '14px 16px',
  boxSizing: 'border-box',
  fontSize: 15,
  lineHeight: 1.55,
}

const composerFooterStyle = {
  display: 'flex',
  justifyContent: 'space-between',
  alignItems: 'center',
  gap: 12,
  marginTop: 12,
  flexWrap: 'wrap',
}

const composerHintStyle = {
  color: '#7f93ba',
  fontSize: 12,
}

const sendButtonStyle = {
  borderRadius: 14,
  border: '1px solid rgba(56,189,248,0.26)',
  background: 'linear-gradient(135deg, #38bdf8, #60a5fa)',
  color: '#03121c',
  padding: '11px 16px',
  fontWeight: 800,
  cursor: 'pointer',
}
