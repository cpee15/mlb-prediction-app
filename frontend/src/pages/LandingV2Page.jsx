import React, { useEffect, useMemo, useState } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import { API_BASE, getMlbLiveDate } from '../lib/api'

const SESSION_KEY = 'mlbgpt_dashboard_session_token'

const COMPONENTS = [
  { key: 'hitters', title: 'My Top Hitters Today', shortTitle: 'Hitters', description: 'Stored 365 hitter board with pitch-type matchup, EV, LA, and arsenal context.' },
  { key: 'pitchers', title: 'My Top Pitchers Today', shortTitle: 'Pitchers', description: 'Pitcher lean board using K profile, contact suppression, and opponent context.' },
  { key: 'teams', title: 'My Top Teams Today', shortTitle: 'Teams', description: 'Team board from model side edge, projected runs, and offense profile.' },
  { key: 'totals', title: 'My Top Totals Today', shortTitle: 'Totals', description: 'Game total board from projected runs, run environment, and simulation context.' },
  { key: 'overall_players', title: 'My Top Overall Players Today', shortTitle: 'Overall', description: 'Combined player board blending hitter and pitcher solver outputs.' },
]

const C = {
  bg: '#070b14',
  panel: '#0d1424',
  panel2: '#101a2e',
  border: '#21304a',
  text: '#eef5ff',
  muted: '#91a1bb',
  green: '#42f58d',
  blue: '#56b7ff',
}

function token() {
  if (typeof window === 'undefined' || !window.localStorage) return ''
  return window.localStorage.getItem(SESSION_KEY) || ''
}

function saveToken(value) {
  if (typeof window !== 'undefined' && value) window.localStorage.setItem(SESSION_KEY, value)
}

function fmt(value) {
  const num = Number(value)
  if (!Number.isFinite(num)) return value == null || value === '' ? '—' : String(value)
  return Math.abs(num) >= 10 ? num.toFixed(1) : num.toFixed(3)
}

function title(item) {
  return item?.entity_name || item?.player || item?.game || item?.teams || 'Dashboard result'
}

function subtitle(item, component) {
  return item?.primary_reason || item?.market || item?.pick || item?.entity_type || component?.description || ''
}

function metrics(item) {
  return Object.entries(item?.metrics || {}).filter(([, value]) => value !== null && value !== undefined && value !== '')
}

function dateLabel(date) {
  try {
    return new Date(`${date}T12:00:00`).toLocaleDateString('en-US', { month: 'short', day: 'numeric' })
  } catch {
    return date
  }
}

function Pill({ children, tone = '' }) {
  if (children === null || children === undefined || children === '') return null
  return <span className={`status-badge ${tone}`}>{children}</span>
}

function Metric({ label, value }) {
  if (value === null || value === undefined || value === '') return null
  return (
    <div style={s.metric}>
      <span style={s.metricLabel}>{label}</span>
      <strong style={s.metricValue}>{value}</strong>
    </div>
  )
}

function useDashboardPreview(date) {
  const [profile, setProfile] = useState(null)
  const [checked, setChecked] = useState(false)
  const [results, setResults] = useState({})
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  useEffect(() => {
    let cancelled = false

    async function load() {
      setError('')
      try {
        const session = token()
        const profileResponse = await fetch(`${API_BASE}/my-dashboard/profile`, {
          credentials: 'include',
          headers: session ? { 'X-Dashboard-Session': session } : {},
        })
        const profileJson = await profileResponse.json().catch(() => ({}))
        if (cancelled) return

        if (profileJson?.session_token) saveToken(profileJson.session_token)
        if (!profileJson?.authenticated) {
          setProfile(null)
          setChecked(true)
          return
        }

        setProfile(profileJson.user || null)
        setChecked(true)
        setLoading(true)

        const keys = COMPONENTS.map(component => component.key)
        const boardResponse = await fetch(`${API_BASE}/my-dashboard/solver/batch`, {
          method: 'POST',
          credentials: 'include',
          headers: {
            'Content-Type': 'application/json',
            ...(token() ? { 'X-Dashboard-Session': token() } : {}),
          },
          body: JSON.stringify({
            date,
            components: keys,
            filters_by_component: Object.fromEntries(keys.map(key => [key, {}])),
            active_lineups: false,
          }),
        })
        const boardJson = await boardResponse.json().catch(() => ({}))
        if (cancelled) return
        if (!boardResponse.ok) throw new Error(typeof boardJson?.detail === 'string' ? boardJson.detail : 'Dashboard solver unavailable')
        setResults(boardJson.results || {})
      } catch (err) {
        if (!cancelled) setError(err.message || 'Dashboard preview unavailable')
      } finally {
        if (!cancelled) {
          setChecked(true)
          setLoading(false)
        }
      }
    }

    load()
    return () => { cancelled = true }
  }, [date])

  return { profile, checked, results, loading, error }
}

function buildSections(results) {
  return COMPONENTS.map(component => ({
    ...component,
    items: Array.isArray(results?.[component.key]?.items) ? results[component.key].items.slice(0, 3) : [],
  })).filter(section => section.items.length > 0)
}

function DashboardCard({ item, component, active, onClick }) {
  const confidence = String(item?.confidence || '').toLowerCase()
  const tone = confidence.includes('high') ? 'success' : confidence.includes('medium') ? 'warning' : confidence.includes('low') ? 'danger' : ''
  return (
    <button type="button" onClick={onClick} style={active ? s.previewCardActive : s.previewCard}>
      <div style={s.cardTop}>
        <div>
          <div style={s.cardTitle}>{title(item)}</div>
          <div style={s.cardMeta}>{subtitle(item, component)}</div>
        </div>
        <div style={s.scoreBox}>
          <span style={s.scoreLabel}>Score</span>
          <strong style={s.score}>{fmt(item?.score)}</strong>
        </div>
      </div>
      <div style={s.pills}>
        <Pill>{item?.entity_type}</Pill>
        <Pill tone="success">{item?.team}</Pill>
        {item?.opponent ? <Pill tone="warning">vs {item.opponent}</Pill> : null}
        <Pill tone={tone}>{item?.confidence}</Pill>
      </div>
    </button>
  )
}

function SelectedBreakdown({ selected, onUnlock }) {
  if (!selected) {
    return (
      <section style={s.section}>
        <div style={s.panelLarge}>
          <p style={s.kicker}>Selected Play Breakdown</p>
          <h2 style={s.h2}>Why this grades well</h2>
          <p style={s.sub}>Select a preview card to inspect its real dashboard fields.</p>
        </div>
      </section>
    )
  }

  const { component, item } = selected
  const itemMetrics = metrics(item)
  const reasoning = Array.isArray(item?.reasoning) ? item.reasoning.filter(Boolean) : []

  return (
    <section style={s.section}>
      <div style={s.panelLarge}>
        <div style={s.sectionHead}>
          <div>
            <p style={s.kicker}>{component.title}</p>
            <h2 style={s.h2}>Why this grades well</h2>
            <p style={s.sub}>{title(item)} — {subtitle(item, component)}</p>
          </div>
          <button type="button" onClick={onUnlock} style={s.primary}>Unlock Today’s Card</button>
        </div>
        <div style={s.metricGrid}>
          <Metric label="Score" value={fmt(item?.score)} />
          <Metric label="Confidence" value={item?.confidence || '—'} />
          <Metric label="Team" value={item?.team} />
          <Metric label="Opponent" value={item?.opponent ? `vs ${item.opponent}` : null} />
        </div>
        {reasoning.length ? (
          <div style={s.reasonBox}>
            <h3 style={s.h3}>Reason / explanation</h3>
            <ul style={s.reasons}>{reasoning.slice(0, 5).map((reason, index) => <li key={`${title(item)}-${index}`}>{reason}</li>)}</ul>
          </div>
        ) : null}
        {itemMetrics.length ? (
          <div style={s.reasonBox}>
            <h3 style={s.h3}>Supporting metrics</h3>
            <div style={s.metricGrid}>{itemMetrics.slice(0, 12).map(([key, value]) => <Metric key={key} label={key} value={fmt(value)} />)}</div>
          </div>
        ) : null}
      </div>
    </section>
  )
}

export default function LandingV2Page() {
  const navigate = useNavigate()
  const date = getMlbLiveDate()
  const { profile, checked, results, loading, error } = useDashboardPreview(date)
  const sections = useMemo(() => buildSections(results), [results])
  const [selected, setSelected] = useState(null)

  useEffect(() => {
    if (!selected && sections[0]?.items?.[0]) setSelected({ component: sections[0], item: sections[0].items[0] })
  }, [sections, selected])

  function unlock() {
    navigate('/my-dashboard')
  }

  const topSection = sections[0]

  return (
    <div style={s.page}>
      <section style={s.hero}>
        <div>
          <div style={s.eyebrow}>● Live MLB Prediction Engine</div>
          <h1 style={s.h1}>Today’s MLB card, ranked by edge.</h1>
          <p style={s.heroText}>MLBGPT scans every matchup using pitcher trends, bullpen form, team offense, weather, odds, and AI model projections to surface the strongest betting angles before first pitch.</p>
          <div style={s.buttons}>
            <button type="button" onClick={unlock} style={s.primary}>Unlock Today’s Card</button>
            <Link to="/" style={s.secondary}>View Matchups</Link>
          </div>
        </div>
        <div style={s.terminal}>
          <div style={s.terminalHead}>
            <strong>{topSection?.title || 'Today’s Best Scores'}</strong>
            <span style={s.green}>{profile ? 'Dashboard Data' : 'Preview Locked'}</span>
            <span style={s.muted}>{dateLabel(date)}</span>
          </div>
          <div style={s.boardRows}>
            {topSection?.items?.length ? topSection.items.map((item, index) => (
              <div key={`${topSection.key}-${item?.entity_id || index}`} style={s.heroRow}>
                <span style={s.rowTitle}>{title(item)}</span>
                <span>{item?.entity_type || topSection.shortTitle}</span>
                <strong style={s.green}>{fmt(item?.score)}</strong>
                <strong style={s.blue}>{item?.confidence || '—'}</strong>
              </div>
            )) : (
              <div style={s.emptyPreview}>Sign in to preview today’s ranked dashboard cards. This page does not show fake plays.</div>
            )}
          </div>
          {selected ? <div style={s.heroMetrics}><Metric label="Selected" value={title(selected.item)} /><Metric label="Score" value={fmt(selected.item?.score)} /><Metric label="Confidence" value={selected.item?.confidence || '—'} /></div> : null}
        </div>
      </section>

      {loading ? <div className="state-panel" style={s.notice}>Loading real dashboard preview…</div> : null}
      {error ? <div className="state-panel error" style={s.notice}>Dashboard preview unavailable: {error}</div> : null}

      <section style={s.section}>
        <div style={s.sectionHead}>
          <div>
            <h2 style={s.h2}>Today’s Best Scores</h2>
            <p style={s.sub}>A public preview layer composed from the same My Dashboard board outputs.</p>
          </div>
          <button type="button" onClick={unlock} style={s.secondary}>Unlock Full Card</button>
        </div>
        {sections.length ? (
          <div style={s.previewGrid}>{sections.map(section => (
            <div key={section.key} style={s.panel}>
              <h3 style={s.h3}>{section.title}</h3>
              <p style={s.cardMeta}>{section.description}</p>
              <div style={s.stack}>{section.items.map((item, index) => <DashboardCard key={`${section.key}-${item?.entity_id || index}`} item={item} component={section} active={selected?.item === item} onClick={() => checked && !profile ? unlock() : setSelected({ component: section, item })} />)}</div>
            </div>
          ))}</div>
        ) : (
          <div className="state-panel" style={{ textAlign: 'left' }}><strong>No fabricated picks are displayed.</strong><p style={{ margin: '8px 0 0' }}>Sign in to run the existing dashboard board engine.</p></div>
        )}
      </section>

      <SelectedBreakdown selected={selected} onUnlock={unlock} />

      <section style={s.section}>
        <h2 style={s.h2}>Built like a sportsbook dashboard. Explained like an analyst.</h2>
        <p style={s.sub}>The preview preserves the existing My Dashboard categories, solver endpoint, score field, confidence field, metrics, and reasoning.</p>
        <div style={s.proofGrid}>
          <div style={s.panel}><h3 style={s.h3}>Dashboard boards</h3>{COMPONENTS.map(component => <div key={component.key} style={component.key === (topSection?.key || 'hitters') ? s.navActive : s.navItem}>{component.shortTitle}</div>)}</div>
          <div style={s.panel}><h3 style={s.h3}>Real output fields</h3><p style={s.sub}>entity_name, primary_reason, score, confidence, entity_type, team, opponent, metrics, reasoning.</p></div>
          <div style={s.panel}><h3 style={s.h3}>No new model logic</h3><p style={s.sub}>This route consumes `/my-dashboard/solver/batch` and does not calculate new scores client-side.</p></div>
        </div>
      </section>

      <section style={s.section}>
        <div style={s.assistant}>
          <div><h2 style={s.h2}>Ask MLBGPT before you bet.</h2><p style={s.sub}>The existing AI Data Assistant is available for slate, matchup, model-edge, and data-quality questions.</p></div>
          <div style={s.prompt}><strong>Prompt:</strong> What is the strongest model edge?<div style={s.answer}>Open the AI Data Assistant to ask this against live app-owned data.</div><Link to="/ai-data-assistant" style={{ ...s.secondary, display: 'inline-flex', marginTop: 14 }}>Open AI Data Assistant</Link></div>
        </div>
      </section>

      <section style={s.final}>
        <h2 style={s.finalTitle}>Stop building your MLB card from scratch.</h2>
        <p style={s.finalText}>Let MLBGPT rank the board, explain the edge, and show the risk before first pitch.</p>
        <button type="button" onClick={unlock} style={s.primary}>Unlock Today’s Card</button>
      </section>
    </div>
  )
}

const s = {
  page: { margin: '-34px calc(50% - 50vw) -56px', padding: '76px 28px 0', minHeight: '100vh', color: C.text, background: `radial-gradient(circle at top left, rgba(66,245,141,.12), transparent 32%), radial-gradient(circle at 75% 10%, rgba(86,183,255,.12), transparent 28%), ${C.bg}` },
  hero: { maxWidth: 1220, margin: '0 auto', paddingBottom: 48, display: 'grid', gridTemplateColumns: 'repeat(auto-fit,minmax(320px,1fr))', gap: 40, alignItems: 'center' },
  eyebrow: { display: 'inline-flex', padding: '8px 12px', border: `1px solid ${C.border}`, borderRadius: 999, background: 'rgba(13,20,36,.72)', color: C.green, fontSize: 13, fontWeight: 800, marginBottom: 22 },
  h1: { fontSize: 'clamp(48px,7vw,78px)', lineHeight: .94, letterSpacing: '-0.075em', margin: '0 0 24px' },
  h2: { fontSize: 'clamp(32px,5vw,42px)', lineHeight: 1, letterSpacing: '-0.05em', margin: '0 0 14px' },
  h3: { margin: '0 0 10px', fontSize: 21, letterSpacing: '-0.03em' },
  heroText: { color: '#b6c4d9', fontSize: 19, lineHeight: 1.55, maxWidth: 600, margin: '0 0 30px' },
  buttons: { display: 'flex', gap: 14, flexWrap: 'wrap', alignItems: 'center' },
  primary: { background: `linear-gradient(135deg, ${C.green}, ${C.blue})`, color: '#04100b', padding: '15px 22px', borderRadius: 14, fontWeight: 900, border: 0, textDecoration: 'none' },
  secondary: { border: `1px solid ${C.border}`, color: C.text, padding: '14px 20px', borderRadius: 14, fontWeight: 800, background: 'rgba(13,20,36,.56)', textDecoration: 'none' },
  terminal: { border: `1px solid ${C.border}`, borderRadius: 26, background: 'linear-gradient(180deg,rgba(16,26,46,.96),rgba(7,11,20,.98))', boxShadow: '0 30px 100px rgba(0,0,0,.38)', overflow: 'hidden' },
  terminalHead: { display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 12, flexWrap: 'wrap', padding: '18px 20px', borderBottom: `1px solid ${C.border}` },
  boardRows: { padding: 18 },
  heroRow: { display: 'grid', gridTemplateColumns: '1fr .6fr .35fr .55fr', gap: 12, alignItems: 'center', padding: '14px', marginBottom: 10, border: '1px solid rgba(255,255,255,.06)', borderRadius: 16, background: 'rgba(255,255,255,.035)' },
  rowTitle: { fontWeight: 850 },
  heroMetrics: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit,minmax(140px,1fr))', gap: 10, padding: '0 18px 18px' },
  emptyPreview: { padding: 18, border: '1px solid rgba(255,255,255,.06)', borderRadius: 16, color: C.muted, background: 'rgba(255,255,255,.035)' },
  section: { maxWidth: 1180, margin: '0 auto', padding: '48px 0' },
  sectionHead: { display: 'flex', justifyContent: 'space-between', gap: 18, alignItems: 'start', flexWrap: 'wrap', marginBottom: 22 },
  sub: { color: C.muted, fontSize: 17, lineHeight: 1.55, margin: 0 },
  previewGrid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit,minmax(300px,1fr))', gap: 18 },
  panel: { border: `1px solid ${C.border}`, borderRadius: 22, padding: 20, background: 'rgba(13,20,36,.72)' },
  panelLarge: { border: `1px solid ${C.border}`, borderRadius: 28, padding: 24, background: '#0a1020', boxShadow: '0 24px 80px rgba(0,0,0,.28)' },
  stack: { display: 'grid', gap: 12, marginTop: 14 },
  previewCard: { width: '100%', border: '1px solid rgba(255,255,255,.08)', borderRadius: 18, padding: 14, background: 'rgba(255,255,255,.035)', color: C.text, textAlign: 'left' },
  previewCardActive: { width: '100%', border: '1px solid rgba(66,245,141,.42)', borderRadius: 18, padding: 14, background: 'rgba(66,245,141,.10)', color: C.text, textAlign: 'left' },
  cardTop: { display: 'flex', justifyContent: 'space-between', gap: 16, marginBottom: 12 },
  cardTitle: { fontSize: 16, fontWeight: 850, color: C.text },
  cardMeta: { color: C.muted, fontSize: 12, lineHeight: 1.5 },
  scoreBox: { textAlign: 'right' },
  scoreLabel: { display: 'block', color: C.muted, fontSize: 10, textTransform: 'uppercase', letterSpacing: '.08em' },
  score: { display: 'block', color: C.green, fontSize: 22 },
  pills: { display: 'flex', gap: 8, flexWrap: 'wrap' },
  metricGrid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit,minmax(140px,1fr))', gap: 10, marginTop: 18 },
  metric: { background: 'rgba(86,183,255,.06)', border: '1px solid rgba(86,183,255,.14)', borderRadius: 16, padding: 14 },
  metricLabel: { display: 'block', color: C.muted, fontSize: 11, textTransform: 'uppercase', letterSpacing: '.07em', marginBottom: 7 },
  metricValue: { display: 'block', color: C.text, fontSize: 19, overflowWrap: 'anywhere' },
  reasonBox: { marginTop: 20 },
  reasons: { color: '#b6c4d9', lineHeight: 1.7, paddingLeft: 20 },
  proofGrid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit,minmax(250px,1fr))', gap: 18, marginTop: 22 },
  navItem: { padding: 12, borderRadius: 12, color: C.muted, marginBottom: 8 },
  navActive: { padding: 12, borderRadius: 12, background: 'rgba(66,245,141,.12)', color: C.green, fontWeight: 800, marginBottom: 8 },
  assistant: { border: `1px solid ${C.border}`, borderRadius: 28, padding: 28, background: 'linear-gradient(135deg,rgba(66,245,141,.08),rgba(86,183,255,.08))', display: 'grid', gridTemplateColumns: 'repeat(auto-fit,minmax(280px,1fr))', gap: 20, alignItems: 'center' },
  prompt: { background: 'rgba(7,11,20,.72)', border: '1px solid rgba(255,255,255,.09)', borderRadius: 18, padding: 18, color: '#dbe9ff', lineHeight: 1.5 },
  answer: { marginTop: 12, borderLeft: `3px solid ${C.green}`, paddingLeft: 14, color: C.muted },
  final: { textAlign: 'center', padding: '76px 28px 90px' },
  finalTitle: { fontSize: 'clamp(38px,6vw,66px)', lineHeight: .95, letterSpacing: '-0.065em', margin: '0 auto 20px', maxWidth: 760 },
  finalText: { color: C.muted, fontSize: 18, marginBottom: 28 },
  notice: { maxWidth: 1180, margin: '0 auto 24px' },
  green: { color: C.green, fontWeight: 850 },
  blue: { color: C.blue, fontWeight: 850 },
  muted: { color: C.muted },
  kicker: { margin: '0 0 8px', color: C.green, fontSize: 12, fontWeight: 850, letterSpacing: '.12em', textTransform: 'uppercase' },
}
