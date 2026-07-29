import React, { useEffect, useMemo, useRef, useState } from 'react'
import { API_BASE, getMlbLiveDate } from '../lib/api'
import { dashboardApi } from '../lib/dashboardSession.mjs'

const STORAGE_KEY = 'mlbgpt-ai-data-assistant-chat-v3'
const PROMPT_CHIPS = [
  "What’s the strongest DK/model edge today?",
  'Which game has the cleanest signal?',
  'Give me 3 angles worth watching tonight.',
  'Explain why the model likes this side.',
  'What props look interesting, even if they’re just watchlist spots?',
]

function loadStoredMessages() {
  if (typeof window === 'undefined') return []
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY)
    if (!raw) return []
    const parsed = JSON.parse(raw)
    return Array.isArray(parsed) ? parsed : []
  } catch {
    return []
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
  const [pendingDraftValue, setPendingDraftValue] = useState('')
  const [savedWorkspace, setSavedWorkspace] = useState(null)
  const [savedReportsError, setSavedReportsError] = useState('')
  const [savedReportsLoading, setSavedReportsLoading] = useState(true)
  const [selectedReportIds, setSelectedReportIds] = useState([])
  const chatEndRef = useRef(null)
  const composerRef = useRef(null)

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
    let ignore = false
    dashboardApi('/my-dashboard/workspace')
      .then(json => { if (!ignore) setSavedWorkspace(json) })
      .catch(err => { if (!ignore && err?.status !== 401) setSavedReportsError(err.message || 'Saved reports are unavailable.') })
      .finally(() => { if (!ignore) setSavedReportsLoading(false) })
    return () => { ignore = true }
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

  const hasConversation = messages.length > 0

  async function ask(nextMessage = draft, options = {}) {
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
    setPendingDraftValue(trimmed)
    setDraft(trimmed)
    setMessages(prev => [...prev, userMessage, pendingAssistant])

    try {
      const payload = {
        message: trimmed,
        date: date || null,
        game_pk: gamePk ? Number(gamePk) : null,
        player_id: playerId ? Number(playerId) : null,
        use_llm: Boolean(llmConfigured && useLlm),
        conversation: historyForPayload,
        saved_report_ids: selectedReportIds,
      }

      const json = await dashboardApi('/ai-data-assistant', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      })

      const assistantMessage = {
        id: pendingId,
        role: 'assistant',
        answer: json.answer || 'No answer returned.',
        response: json,
        llmMode: json.llm_mode,
        createdAt: Date.now(),
      }

      setMessages(prev => prev.map(message => (message.id === pendingId ? assistantMessage : message)))
      if (pendingDraftValue === trimmed || options.fromPrompt) {
        setDraft('')
      }
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
      setPendingDraftValue('')
      composerRef.current?.focus()
    }
  }

  function handlePromptClick(chip) {
    setDraft(chip)
    requestAnimationFrame(() => {
      ask(chip, { fromPrompt: true })
    })
  }

  function clearChat() {
    setMessages([])
    setError(null)
    setDraft('')
  }

  return (
    <div style={pageStyle}>
      <div style={shellStyle}>
        <div style={headerStyle}>
          <div>
            <div style={eyebrowStyle}>MLBGPT analyst chat</div>
            <h1 style={titleStyle}>AI Data Assistant</h1>
            <div style={subtitleStyle}>Ask like you’re talking to an analyst, not filling out a report form.</div>
          </div>
          <div style={headerMetaStyle}>
            <ModeBadge label={llmModeLabel} active={Boolean(llmConfigured && useLlm)} muted={!llmConfigured} />
            <button onClick={clearChat} style={secondaryButtonStyle}>Clear</button>
          </div>
        </div>

        <div style={toolbarStyle}>
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
        </div>

        <SavedReportSelector workspace={savedWorkspace} loading={savedReportsLoading} error={savedReportsError} selectedIds={selectedReportIds} setSelectedIds={setSelectedReportIds} />

        {!hasConversation && (
          <div style={emptyStateStyle}>
            <div style={emptyTitleStyle}>Start with one of these</div>
            <div style={emptyCopyStyle}>
              I’ll answer from DK/model projections first, then Daily Odds, Stored 365 matchup flags, and data-quality checks.
            </div>
            <div style={promptWrapStyle}>
              {PROMPT_CHIPS.map(chip => (
                <button key={chip} onClick={() => handlePromptClick(chip)} disabled={loading} style={promptChipStyle}>
                  {chip}
                </button>
              ))}
            </div>
          </div>
        )}

        <div style={chatHistoryStyle}>
          {messages.map(message => (
            <ChatMessage key={message.id} message={message} />
          ))}
          {loading && <TypingBubble prompt={pendingDraftValue} />}
          <div ref={chatEndRef} />
        </div>

        <div style={composerStyle}>
          <textarea
            ref={composerRef}
            value={draft}
            onChange={e => setDraft(e.target.value)}
            onKeyDown={e => {
              if (e.key === 'Enter' && !e.shiftKey) {
                e.preventDefault()
                ask()
              }
            }}
            placeholder="Ask what stands out, why the model likes a side, what props are worth watching, or where the data is thin…"
            style={composerInputStyle}
          />
          <div style={composerFooterStyle}>
            <div style={composerHintStyle}>{error ? `Last error: ${error}` : 'Enter to send • Shift+Enter for a new line'}</div>
            <button onClick={() => ask()} disabled={loading || !draft.trim()} style={sendButtonStyle}>
              {loading ? 'Thinking…' : 'Send'}
            </button>
          </div>
        </div>
      </div>
    </div>
  )
}

function SavedReportSelector({ workspace, loading, error, selectedIds, setSelectedIds }) {
  const folders = Array.isArray(workspace?.folders) ? workspace.folders : []
  const selected = new Set(selectedIds.map(String))
  const reports = folders.flatMap(folder => (Array.isArray(folder?.items) ? folder.items : [])
    .filter(item => ['report_view', 'workbench_view', 'dashboard_report'].includes(item?.source_type))
    .map(item => ({ ...item, folderName: folder.folder_name || 'Saved Reports' })))
  function toggle(id) {
    const key = String(id)
    setSelectedIds(current => current.map(String).includes(key)
      ? current.filter(value => String(value) !== key)
      : current.length < 5 ? [...current, id] : current)
  }
  return <details style={savedSelectorStyle} open={selectedIds.length > 0}>
    <summary style={savedSelectorSummaryStyle}>Saved Reports <span style={savedCountStyle}>{selectedIds.length} selected</span></summary>
    <div style={savedSelectorBodyStyle}>
      {loading ? <div style={savedNoticeStyle}>Loading your saved reports…</div> : null}
      {error ? <div style={{ ...savedNoticeStyle, color: '#fca5a5' }}>{error}</div> : null}
      {!loading && !error && !reports.length ? <div style={savedNoticeStyle}>No saved reports yet. Save one in MyDashboard first.</div> : null}
      {selectedIds.length ? <div style={selectedReportWrapStyle}>{reports.filter(report => selected.has(String(report.id))).map(report => <button key={`selected-${report.id}`} type="button" style={selectedReportChipStyle} onClick={() => toggle(report.id)}>{report.title || 'Saved report'} ×</button>)}</div> : null}
      <div style={savedReportGridStyle}>{reports.map(report => {
        const reportType = report?.payload_json?.definition?.report_type || report.source_type
        const checked = selected.has(String(report.id))
        return <label key={report.id} style={checked ? savedReportActiveStyle : savedReportStyle}><input type="checkbox" checked={checked} disabled={!checked && selectedIds.length >= 5} onChange={() => toggle(report.id)} /><span><strong>{report.title || 'Saved report'}</strong><small>{report.folderName} · {reportType}</small></span></label>
      })}</div>
      <div style={savedNoticeStyle}>Select up to five. The assistant reruns each report with its saved filters, weights, date, and sort.</div>
    </div>
  </details>
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
  const hasMeta = !isUser && (primaryRecommendations.length || watchlist.length || warnings.length || missingData.length || dataUsed.length || response.confidence_note)

  return (
    <div style={{ ...messageRowStyle, justifyContent: isUser ? 'flex-end' : 'flex-start' }}>
      <div style={{ ...bubbleStyle, ...(isUser ? userBubbleStyle : assistantBubbleStyle) }}>
        <div style={bubbleMetaStyle}>
          <span style={bubbleRoleStyle}>{isUser ? 'You' : 'AI Data Assistant'}</span>
          {!isUser && llmMode?.active && <span style={bubbleTagStyle}>LLM</span>}
        </div>

        <div style={{ whiteSpace: 'pre-wrap', lineHeight: 1.65, color: isUser ? '#f8fafc' : '#e8eefc' }}>
          {isUser ? message.content : message.answer}
        </div>

        {hasMeta && !isUser && (
          <div style={assistantSectionsWrapStyle}>
            {primaryRecommendations.length > 0 && <StructuredSection title="Top angles" items={primaryRecommendations} />}
            {watchlist.length > 0 && <StructuredSection title="Watchlist" items={watchlist} />}
            {(warnings.length > 0 || missingData.length > 0 || dataUsed.length > 0 || response.confidence_note) && (
              <details style={detailsStyle}>
                <summary style={detailsSummaryStyle}>Why I said that</summary>
                <div style={detailsBodyStyle}>
                  {dataUsed.length > 0 && <StructuredInlineList title="Data used" items={dataUsed} />}
                  {warnings.length > 0 && <StructuredInlineList title="Warnings" items={warnings} />}
                  {missingData.length > 0 && (
                    <StructuredInlineList
                      title="Missing data"
                      items={missingData.map(item => (typeof item === 'string' ? item : JSON.stringify(item)))}
                    />
                  )}
                  {response.confidence_note && <div style={confidenceNoteStyle}>{response.confidence_note}</div>}
                </div>
              </details>
            )}
          </div>
        )}
      </div>
    </div>
  )
}

function TypingBubble({ prompt }) {
  return (
    <div style={{ ...messageRowStyle, justifyContent: 'flex-start' }}>
      <div style={{ ...bubbleStyle, ...assistantBubbleStyle, maxWidth: 260 }}>
        <div style={bubbleMetaStyle}>
          <span style={bubbleRoleStyle}>AI Data Assistant</span>
        </div>
        <div style={typingLabelStyle}>{prompt ? `Thinking about: ${prompt}` : 'Thinking…'}</div>
        <div style={typingWrapStyle}>
          <span style={typingDotStyle}>•</span>
          <span style={typingDotStyle}>•</span>
          <span style={typingDotStyle}>•</span>
        </div>
      </div>
    </div>
  )
}

function StructuredSection({ title, items }) {
  return (
    <details style={detailsStyle}>
      <summary style={detailsSummaryStyle}>{title}</summary>
      <div style={detailsBodyStyle}>
        <div style={structuredGridStyle}>
          {items.map((item, index) => (
            <div key={`${title}-${index}`} style={structuredCardStyle}>
              {formatStructuredItem(item)}
            </div>
          ))}
        </div>
      </div>
    </details>
  )
}

function StructuredInlineList({ title, items }) {
  return (
    <div style={{ marginBottom: 12 }}>
      <div style={inlineTitleStyle}>{title}</div>
      <div style={inlineListStyle}>
        {items.map((item, index) => (
          <span key={`${title}-${index}`} style={inlineChipStyle}>{String(item)}</span>
        ))}
      </div>
    </div>
  )
}

function ModeBadge({ label, active, muted }) {
  return (
    <div
      style={{
        ...modeBadgeStyle,
        background: active ? 'rgba(34,197,94,0.14)' : muted ? 'rgba(148,163,184,0.08)' : 'rgba(56,189,248,0.10)',
        borderColor: active ? 'rgba(34,197,94,0.28)' : muted ? 'rgba(148,163,184,0.18)' : 'rgba(56,189,248,0.24)',
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
  background: 'linear-gradient(180deg, #07101d 0%, #04070f 100%)',
  color: '#eef4ff',
  padding: '22px 14px 28px',
}

const shellStyle = {
  maxWidth: 980,
  margin: '0 auto',
  borderRadius: 22,
  border: '1px solid rgba(148,163,184,0.14)',
  background: 'rgba(7, 12, 21, 0.96)',
  boxShadow: '0 24px 90px rgba(0,0,0,0.35)',
  overflow: 'hidden',
}

const headerStyle = {
  display: 'flex',
  justifyContent: 'space-between',
  gap: 16,
  alignItems: 'flex-start',
  padding: '20px 20px 14px',
  borderBottom: '1px solid rgba(148,163,184,0.10)',
}

const eyebrowStyle = {
  fontSize: 11,
  letterSpacing: '0.16em',
  textTransform: 'uppercase',
  color: '#7dd3fc',
  marginBottom: 6,
}

const titleStyle = {
  margin: 0,
  fontSize: '28px',
  lineHeight: 1.1,
}

const subtitleStyle = {
  marginTop: 8,
  color: '#9db0d1',
  fontSize: 14,
  lineHeight: 1.5,
}

const headerMetaStyle = {
  display: 'flex',
  gap: 10,
  alignItems: 'center',
  flexWrap: 'wrap',
}

const modeBadgeStyle = {
  padding: '8px 11px',
  borderRadius: 999,
  border: '1px solid transparent',
  fontSize: 12,
  fontWeight: 700,
}

const toolbarStyle = {
  display: 'flex',
  gap: 12,
  flexWrap: 'wrap',
  alignItems: 'flex-end',
  padding: '14px 20px',
  borderBottom: '1px solid rgba(148,163,184,0.10)',
}

const savedSelectorStyle = { margin: '12px 20px 0', border: '1px solid rgba(148,163,184,0.14)', borderRadius: 14, background: 'rgba(255,255,255,0.02)' }
const savedSelectorSummaryStyle = { cursor: 'pointer', padding: '11px 13px', color: '#dbeafe', fontWeight: 700, fontSize: 13 }
const savedCountStyle = { marginLeft: 8, color: '#7dd3fc', fontSize: 11 }
const savedSelectorBodyStyle = { padding: '0 12px 12px' }
const savedReportGridStyle = { display: 'grid', gridTemplateColumns: 'repeat(auto-fit,minmax(220px,1fr))', gap: 8, maxHeight: 230, overflowY: 'auto' }
const savedReportStyle = { display: 'flex', gap: 9, alignItems: 'flex-start', padding: 10, border: '1px solid rgba(148,163,184,0.12)', borderRadius: 10, color: '#dbeafe', background: '#0b1424', fontSize: 12 }
const savedReportActiveStyle = { ...savedReportStyle, border: '1px solid rgba(56,189,248,0.42)', background: 'rgba(56,189,248,0.10)' }
const savedNoticeStyle = { margin: '8px 0', color: '#91a5c8', fontSize: 12, lineHeight: 1.45 }
const selectedReportWrapStyle = { display: 'flex', gap: 7, flexWrap: 'wrap', marginBottom: 10 }
const selectedReportChipStyle = { border: '1px solid rgba(56,189,248,0.24)', borderRadius: 999, background: 'rgba(56,189,248,0.08)', color: '#dff7ff', padding: '6px 9px', fontSize: 11 }

const controlLabelStyle = {
  display: 'grid',
  gap: 6,
  color: '#97abcf',
  fontSize: 12,
}

const controlInputStyle = {
  background: '#0b1424',
  color: '#ecf4ff',
  border: '1px solid rgba(148,163,184,0.20)',
  borderRadius: 10,
  padding: '9px 11px',
}

const smallInputStyle = {
  ...controlInputStyle,
  width: 110,
}

const toggleWrapStyle = {
  display: 'flex',
  alignItems: 'center',
  gap: 8,
  color: '#d7e3ff',
  fontSize: 13,
  paddingBottom: 8,
}

const secondaryButtonStyle = {
  border: '1px solid rgba(148,163,184,0.18)',
  background: 'rgba(15,23,42,0.82)',
  color: '#dbeafe',
  borderRadius: 10,
  padding: '9px 12px',
  cursor: 'pointer',
}

const emptyStateStyle = {
  margin: '18px 20px 6px',
  borderRadius: 16,
  border: '1px solid rgba(148,163,184,0.10)',
  background: 'rgba(255,255,255,0.02)',
  padding: '16px 16px 14px',
}

const emptyTitleStyle = {
  fontSize: 16,
  fontWeight: 700,
  marginBottom: 6,
}

const emptyCopyStyle = {
  color: '#91a5c8',
  fontSize: 14,
  lineHeight: 1.55,
}

const promptWrapStyle = {
  display: 'flex',
  gap: 10,
  flexWrap: 'wrap',
  marginTop: 14,
}

const promptChipStyle = {
  borderRadius: 999,
  border: '1px solid rgba(56,189,248,0.18)',
  background: 'rgba(56,189,248,0.08)',
  color: '#dff7ff',
  padding: '9px 12px',
  cursor: 'pointer',
  fontSize: 13,
}

const chatHistoryStyle = {
  padding: '18px 20px',
  minHeight: '48vh',
  maxHeight: '58vh',
  overflowY: 'auto',
  display: 'grid',
  gap: 14,
}

const messageRowStyle = {
  display: 'flex',
}

const bubbleStyle = {
  maxWidth: '80%',
  borderRadius: 20,
  padding: '14px 15px',
}

const userBubbleStyle = {
  background: 'linear-gradient(135deg, #1d4ed8, #2563eb)',
  border: '1px solid rgba(147,197,253,0.24)',
}

const assistantBubbleStyle = {
  background: 'rgba(11,18,32,0.98)',
  border: '1px solid rgba(148,163,184,0.12)',
}

const bubbleMetaStyle = {
  display: 'flex',
  gap: 8,
  alignItems: 'center',
  marginBottom: 8,
}

const bubbleRoleStyle = {
  fontSize: 11,
  fontWeight: 700,
  letterSpacing: '0.04em',
  textTransform: 'uppercase',
  color: '#9fb7ff',
}

const bubbleTagStyle = {
  fontSize: 10,
  color: '#dff7ff',
  borderRadius: 999,
  padding: '3px 7px',
  background: 'rgba(56,189,248,0.10)',
  border: '1px solid rgba(56,189,248,0.14)',
}

const assistantSectionsWrapStyle = {
  marginTop: 12,
  display: 'grid',
  gap: 10,
}

const detailsStyle = {
  borderRadius: 14,
  background: 'rgba(255,255,255,0.02)',
  border: '1px solid rgba(148,163,184,0.10)',
}

const detailsSummaryStyle = {
  cursor: 'pointer',
  padding: '9px 11px',
  color: '#dbeafe',
  fontWeight: 600,
  fontSize: 13,
}

const detailsBodyStyle = {
  padding: '0 11px 11px',
}

const structuredGridStyle = {
  display: 'grid',
  gap: 8,
}

const structuredCardStyle = {
  background: 'rgba(8,14,28,0.9)',
  border: '1px solid rgba(148,163,184,0.12)',
  borderRadius: 12,
  padding: '10px 11px',
  color: '#dbeafe',
  whiteSpace: 'pre-wrap',
  lineHeight: 1.5,
  fontSize: 13,
}

const inlineTitleStyle = {
  fontSize: 11,
  fontWeight: 700,
  color: '#9fb7ff',
  marginBottom: 6,
  textTransform: 'uppercase',
  letterSpacing: '0.05em',
}

const inlineListStyle = {
  display: 'flex',
  flexWrap: 'wrap',
  gap: 7,
}

const inlineChipStyle = {
  display: 'inline-flex',
  alignItems: 'center',
  borderRadius: 999,
  padding: '6px 9px',
  background: 'rgba(148,163,184,0.10)',
  border: '1px solid rgba(148,163,184,0.14)',
  color: '#dbeafe',
  fontSize: 12,
}

const confidenceNoteStyle = {
  marginTop: 8,
  color: '#c4d4f4',
  fontSize: 13,
  lineHeight: 1.5,
}

const typingLabelStyle = {
  color: '#b8c8e6',
  fontSize: 13,
  marginBottom: 8,
}

const typingWrapStyle = {
  display: 'flex',
  gap: 8,
  color: '#9fb7ff',
  fontSize: 18,
}

const typingDotStyle = {
  lineHeight: 1,
}

const composerStyle = {
  borderTop: '1px solid rgba(148,163,184,0.10)',
  padding: '14px 16px 16px',
  background: 'rgba(5, 9, 17, 0.98)',
}

const composerInputStyle = {
  width: '100%',
  minHeight: 84,
  resize: 'vertical',
  borderRadius: 16,
  border: '1px solid rgba(148,163,184,0.14)',
  background: 'rgba(11, 18, 32, 0.98)',
  color: '#eff6ff',
  padding: '13px 15px',
  boxSizing: 'border-box',
  fontSize: 15,
  lineHeight: 1.55,
}

const composerFooterStyle = {
  display: 'flex',
  justifyContent: 'space-between',
  alignItems: 'center',
  gap: 12,
  marginTop: 10,
  flexWrap: 'wrap',
}

const composerHintStyle = {
  color: '#7f93ba',
  fontSize: 12,
}

const sendButtonStyle = {
  borderRadius: 12,
  border: '1px solid rgba(56,189,248,0.22)',
  background: 'linear-gradient(135deg, #38bdf8, #60a5fa)',
  color: '#03121c',
  padding: '10px 15px',
  fontWeight: 800,
  cursor: 'pointer',
}
