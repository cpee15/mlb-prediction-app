import React, { useEffect, useMemo, useState } from 'react'

const API = import.meta.env.VITE_API_BASE_URL || ''

const COMPONENTS = [
  { key: 'hitters', title: 'My Top Hitters Today', description: 'Unique hitter board from Batter vs Arsenal, pitch usage, damage quality, and model context.' },
  { key: 'pitchers', title: 'My Top Pitchers Today', description: 'Pitcher lean board using K profile, contact suppression, opponent offense, and arsenal context.' },
  { key: 'teams', title: 'My Top Teams Today', description: 'Team board from model side edge, expected runs, offense profile, and opponent weaknesses.' },
  { key: 'totals', title: 'My Top Totals Today', description: 'Game total watchlist from projected runs, run environment, and simulation context.' },
  { key: 'overall_players', title: 'My Top Overall Players Today', description: 'Combined unique player board blending hitter and pitcher model-solver scores.' },
]

const FEATURE_CHOICES = ['Matchups', 'Daily Odds', 'Model Projections', 'News', 'Props', 'Pitchers', 'Batters']

const BASE_FILTERS = {
  search_text: '',
  team: '',
  opponent: '',
  min_score: '',
  min_confidence: '',
}

function cleanFilters(filters) {
  return Object.fromEntries(
    Object.entries(filters || {}).filter(([, value]) => value !== '' && value !== null && value !== undefined)
  )
}

function formatDateTime(value) {
  if (!value) return '—'
  try {
    return new Date(value).toLocaleString()
  } catch {
    return value
  }
}

function formatNumber(value) {
  const num = Number(value)
  if (!Number.isFinite(num)) return '—'
  return Math.abs(num) >= 10 ? num.toFixed(1) : num.toFixed(3)
}

export default function MyDashboardWorkspacePage() {
  const today = new Date().toISOString().slice(0, 10)
  const [authChecked, setAuthChecked] = useState(false)
  const [profile, setProfile] = useState(null)
  const [workspace, setWorkspace] = useState(null)
  const [authError, setAuthError] = useState(null)
  const [savingProfile, setSavingProfile] = useState(false)
  const [results, setResults] = useState({})
  const [runErrors, setRunErrors] = useState({})
  const [loading, setLoading] = useState({})
  const [saveMessage, setSaveMessage] = useState(null)
  const [form, setForm] = useState({
    email: '',
    username: '',
    password: '',
    feature_interests: ['Matchups', 'Model Projections'],
    wants_newsletter: false,
    plan_type: 'free',
  })
  const [filters, setFilters] = useState(
    Object.fromEntries(COMPONENTS.map(component => [component.key, { ...BASE_FILTERS }]))
  )

  useEffect(() => {
    async function bootstrap() {
      try {
        const res = await fetch(`${API}/my-dashboard/profile`, { credentials: 'include' })
        const json = await res.json()
        if (json.authenticated) {
          setProfile(json.user)
          await loadWorkspace()
        }
      } catch (err) {
        console.error('My Dashboard bootstrap failed', err)
      } finally {
        setAuthChecked(true)
      }
    }
    bootstrap()
  }, [])

  async function loadWorkspace() {
    const res = await fetch(`${API}/my-dashboard/workspace`, { credentials: 'include' })
    const json = await res.json()
    if (!res.ok) throw new Error(typeof json.detail === 'string' ? json.detail : JSON.stringify(json.detail || json))
    setWorkspace(json)
    return json
  }

  async function handleProfileSubmit(event) {
    event.preventDefault()
    setSavingProfile(true)
    setAuthError(null)
    try {
      const res = await fetch(`${API}/my-dashboard/profile`, {
        method: 'POST',
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(form),
      })
      const json = await res.json()
      if (!res.ok) throw new Error(typeof json.detail === 'string' ? json.detail : JSON.stringify(json.detail || json))
      setProfile(json.user)
      await loadWorkspace()
    } catch (err) {
      setAuthError(err.message || 'Failed to create dashboard profile')
    } finally {
      setSavingProfile(false)
      setAuthChecked(true)
    }
  }

  function toggleInterest(choice) {
    setForm(prev => ({
      ...prev,
      feature_interests: prev.feature_interests.includes(choice)
        ? prev.feature_interests.filter(item => item !== choice)
        : [...prev.feature_interests, choice],
    }))
  }

  async function runBoard(componentKey) {
    setLoading(prev => ({ ...prev, [componentKey]: true }))
    setRunErrors(prev => ({ ...prev, [componentKey]: null }))
    try {
      const res = await fetch(`${API}/my-dashboard/solver`, {
        method: 'POST',
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          date: today,
          component: componentKey,
          filters: cleanFilters(filters[componentKey] || {}),
        }),
      })
      const json = await res.json()
      if (!res.ok) throw new Error(typeof json.detail === 'string' ? json.detail : JSON.stringify(json.detail || json))
      setResults(prev => ({ ...prev, [componentKey]: json }))
    } catch (err) {
      setRunErrors(prev => ({ ...prev, [componentKey]: err.message || 'Board run failed' }))
    } finally {
      setLoading(prev => ({ ...prev, [componentKey]: false }))
    }
  }

  async function runAllBoards() {
    const keys = COMPONENTS.map(component => component.key)
    setLoading(prev => ({ ...prev, ...Object.fromEntries(keys.map(key => [key, true])) }))
    setRunErrors({})
    try {
      const res = await fetch(`${API}/my-dashboard/solver/batch`, {
        method: 'POST',
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          date: today,
          components: keys,
          filters_by_component: Object.fromEntries(keys.map(key => [key, cleanFilters(filters[key] || {})])),
          active_lineups: false,
        }),
      })
      const json = await res.json()
      if (!res.ok) throw new Error(typeof json.detail === 'string' ? json.detail : JSON.stringify(json.detail || json))
      setResults(prev => ({ ...prev, ...(json.results || {}) }))
    } catch (err) {
      setRunErrors(prev => ({ ...prev, _all: err.message || 'Populate all failed' }))
    } finally {
      setLoading(prev => ({ ...prev, ...Object.fromEntries(keys.map(key => [key, false])) }))
    }
  }

  async function saveItemToToday(component, item) {
    if (!workspace?.today_folder_id) return
    setSaveMessage(null)
    try {
      const res = await fetch(`${API}/my-dashboard/items`, {
        method: 'POST',
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          folder_id: workspace.today_folder_id,
          source_tab: 'my-dashboard',
          source_type: 'solver_result',
          title: `${component.title} | ${item.entity_name || item.title || 'Saved item'}`,
          subtitle: item.primary_reason || component.description,
          payload_json: {
            saved_from_component: component.key,
            saved_on_date: today,
            entity_name: item.entity_name,
            entity_id: item.entity_id,
            entity_type: item.entity_type,
            game_pk: item.game_pk,
            score: item.score,
            confidence: item.confidence,
            metrics: item.metrics || {},
            reasoning: item.reasoning || [],
            available_fields: Object.keys(item.metrics || {}),
            conditions: ['equals', 'contains', 'min', 'max', 'in'],
            max_filters: 5,
            logic_mode: 'AND',
          },
          filter_json: cleanFilters(filters[component.key] || {}),
          sort_json: { by: 'score', direction: 'desc' },
        }),
      })
      const json = await res.json()
      if (!res.ok) throw new Error(typeof json.detail === 'string' ? json.detail : JSON.stringify(json.detail || json))
      await loadWorkspace()
      setSaveMessage(`Saved ${item.entity_name || item.title || 'item'} to today's folder.`)
    } catch (err) {
      setSaveMessage(err.message || 'Failed to save item')
    }
  }

  const folders = workspace?.folders || []
  const defaultFolder = useMemo(() => folders.find(folder => folder.is_default), [folders])
  const todayFolder = useMemo(() => folders.find(folder => folder.id === workspace?.today_folder_id), [folders, workspace])

  if (!authChecked) {
    return <div style={stateStyle}>Loading dashboard workspace…</div>
  }

  if (!profile) {
    return (
      <div style={pageStyle}>
        <section style={heroStyle}>
          <div>
            <div style={eyebrowStyle}>My Dashboard MVP</div>
            <h1 style={titleStyle}>Create your analyst profile</h1>
            <p style={subtitleStyle}>Store daily dashboards, keep folders by date, and prepare for future click-to-save discovery from Matchups, Daily Odds, Model Projections, News, and every endpoint across the app.</p>
          </div>
        </section>

        <section style={layoutStyle}>
          <div style={railStyle}>
            <InfoCard title="Daily folders" body="Every user gets a Default Dashboard plus today’s dated folder so discoveries stay organized and easy to review later." />
            <InfoCard title="Free seeded board" body="Your default dashboard is automatically seeded from the current My Dashboard experience that exists in the app today." />
            <InfoCard title="Future save-anything path" body="The backend item schema is already structured so later we can save discoveries from Matchups, Odds, Models, News, Pitchers, Batters, Teams, and more." />
          </div>

          <form onSubmit={handleProfileSubmit} style={formCardStyle}>
            <div>
              <h2 style={cardTitleStyle}>Sign in or create profile</h2>
              <p style={cardSubtitleStyle}>Required: email and username. Password is optional for this MVP.</p>
            </div>

            <label style={labelStyle}>Email</label>
            <input style={inputStyle} value={form.email} onChange={e => setForm(prev => ({ ...prev, email: e.target.value }))} placeholder="you@example.com" />

            <label style={labelStyle}>Username</label>
            <input style={inputStyle} value={form.username} onChange={e => setForm(prev => ({ ...prev, username: e.target.value }))} placeholder="SharpBettorMike" />

            <label style={labelStyle}>Password (optional)</label>
            <input style={inputStyle} type="password" value={form.password} onChange={e => setForm(prev => ({ ...prev, password: e.target.value }))} placeholder="Optional password" />

            <label style={labelStyle}>Feature interests</label>
            <div style={pillWrapStyle}>
              {FEATURE_CHOICES.map(choice => (
                <button key={choice} type="button" onClick={() => toggleInterest(choice)} style={form.feature_interests.includes(choice) ? activePillStyle : pillStyle}>
                  {choice}
                </button>
              ))}
            </div>

            <label style={checkLabelStyle}><input type="checkbox" checked={form.wants_newsletter} onChange={e => setForm(prev => ({ ...prev, wants_newsletter: e.target.checked }))} /> Interested in newsletter updates</label>
            <label style={checkLabelStyle}><input type="radio" name="plan_type" checked={form.plan_type === 'free'} onChange={() => setForm(prev => ({ ...prev, plan_type: 'free' }))} /> Free dashboard</label>
            <label style={checkLabelStyle}><input type="radio" name="plan_type" checked={form.plan_type === 'newsletter_10_day_trial'} onChange={() => setForm(prev => ({ ...prev, plan_type: 'newsletter_10_day_trial' }))} /> 10 days of picks for $5 interest</label>

            {authError && <div style={errorStyle}>{authError}</div>}
            <button type="submit" style={primaryButtonStyle} disabled={savingProfile}>{savingProfile ? 'Creating profile…' : 'Enter My Dashboard'}</button>
          </form>
        </section>
      </div>
    )
  }

  return (
    <div style={pageStyle}>
      <section style={heroStyle}>
        <div>
          <div style={eyebrowStyle}>My Dashboard</div>
          <h1 style={titleStyle}>Welcome back, {profile.username}</h1>
          <p style={subtitleStyle}>Your signed-in workspace now persists in the app database. Save today’s discovery into folders, keep a seeded default board, and continue using the existing free solver boards below.</p>
        </div>
        <div style={summaryCardStyle}>
          <div style={summaryRowStyle}><span>Email</span><strong>{profile.email}</strong></div>
          <div style={summaryRowStyle}><span>Plan</span><strong>{profile.preferences?.plan_type || 'free'}</strong></div>
          <div style={summaryRowStyle}><span>Newsletter</span><strong>{profile.preferences?.wants_newsletter ? 'Interested' : 'No'}</strong></div>
          <div style={summaryRowStyle}><span>Interests</span><strong>{(profile.preferences?.feature_interests || []).join(', ') || 'None selected'}</strong></div>
        </div>
      </section>

      <section style={layoutStyle}>
        <section style={panelStyle}>
          <div style={panelHeaderStyle}>
            <div>
              <h2 style={cardTitleStyle}>Folders</h2>
              <p style={cardSubtitleStyle}>Project-style cards for your default board and daily saved dashboards.</p>
            </div>
            <button onClick={loadWorkspace} style={secondaryButtonStyle}>Refresh</button>
          </div>
          <div style={folderGridStyle}>
            {folders.map(folder => (
              <div key={folder.id} style={folderCardStyle}>
                <div style={folderBadgeStyle}>{folder.is_default ? 'Default' : 'Daily Folder'}</div>
                <div style={folderNameStyle}>{folder.folder_name}</div>
                <div style={metaStyle}>{folder.folder_date || 'Reusable board'}</div>
                <div style={metaStyle}>{folder.item_count} item{folder.item_count === 1 ? '' : 's'}</div>
              </div>
            ))}
          </div>
        </section>

        <section style={panelStyle}>
          <div style={panelHeaderStyle}>
            <div>
              <h2 style={cardTitleStyle}>Seeded default dashboard</h2>
              <p style={cardSubtitleStyle}>Built from the current My Dashboard concepts so every signed-in user starts with a familiar free board.</p>
            </div>
          </div>
          <div style={seedGridStyle}>
            {(defaultFolder?.items || []).map(item => (
              <div key={item.id} style={seedCardStyle}>
                <div style={seedTitleStyle}>{item.title}</div>
                <div style={seedBodyStyle}>{item.subtitle}</div>
                <div style={metaStyle}>Max filters: {item.payload_json?.save_ready?.max_filters || 5}</div>
              </div>
            ))}
          </div>
        </section>
      </section>

      <section style={panelStyle}>
        <div style={panelHeaderStyle}>
          <div>
            <h2 style={cardTitleStyle}>Today’s folder</h2>
            <p style={cardSubtitleStyle}>Saved discoveries from the boards below land here first. Later this same schema can accept saved insights from every app tab.</p>
          </div>
        </div>
        <div style={todayShellStyle}>
          <div style={folderCardStyle}>
            <div style={folderBadgeStyle}>Today</div>
            <div style={folderNameStyle}>{todayFolder?.folder_name || today}</div>
            <div style={metaStyle}>{todayFolder?.item_count || 0} item{todayFolder?.item_count === 1 ? '' : 's'}</div>
          </div>
          <div style={savedListStyle}>
            {(todayFolder?.items || []).length === 0 ? (
              <div style={emptyStyle}>No saved discoveries yet. Run a board and use “Save to Today”.</div>
            ) : (
              todayFolder.items.map(item => (
                <div key={item.id} style={savedItemStyle}>
                  <div style={savedTitleStyle}>{item.title}</div>
                  <div style={savedBodyStyle}>{item.subtitle || 'Saved dashboard item'}</div>
                  <div style={metaStyle}>Source: {item.source_tab} • {item.source_type}</div>
                </div>
              ))
            )}
          </div>
        </div>
        {saveMessage && <div style={successStyle}>{saveMessage}</div>}
      </section>

      <section style={panelStyle}>
        <div style={panelHeaderStyle}>
          <div>
            <h2 style={cardTitleStyle}>Free daily boards</h2>
            <p style={cardSubtitleStyle}>These still run through the current free solver endpoints. The difference now is that signed-in users can save the discoveries they care about.</p>
          </div>
          <button onClick={runAllBoards} style={primaryButtonStyle}>Populate all</button>
        </div>
        {runErrors._all && <div style={errorStyle}>{runErrors._all}</div>}
        <div style={boardGridStyle}>
          {COMPONENTS.map(component => {
            const result = results[component.key]
            const items = result?.items || []
            return (
              <div key={component.key} style={boardCardStyle}>
                <div style={boardHeaderStyle}>
                  <div>
                    <div style={boardTitleStyle}>{component.title}</div>
                    <div style={boardDescriptionStyle}>{component.description}</div>
                  </div>
                  <div style={countBadgeStyle}>{items.length || 0}/10</div>
                </div>

                <div style={filterGridStyle}>
                  <input style={smallInputStyle} placeholder="Search" value={filters[component.key]?.search_text || ''} onChange={e => setFilters(prev => ({ ...prev, [component.key]: { ...prev[component.key], search_text: e.target.value } }))} />
                  <input style={smallInputStyle} placeholder="Team" value={filters[component.key]?.team || ''} onChange={e => setFilters(prev => ({ ...prev, [component.key]: { ...prev[component.key], team: e.target.value } }))} />
                  <input style={smallInputStyle} placeholder="Opponent" value={filters[component.key]?.opponent || ''} onChange={e => setFilters(prev => ({ ...prev, [component.key]: { ...prev[component.key], opponent: e.target.value } }))} />
                  <input style={smallInputStyle} placeholder="Min score" value={filters[component.key]?.min_score || ''} onChange={e => setFilters(prev => ({ ...prev, [component.key]: { ...prev[component.key], min_score: e.target.value } }))} />
                  <select style={smallInputStyle} value={filters[component.key]?.min_confidence || ''} onChange={e => setFilters(prev => ({ ...prev, [component.key]: { ...prev[component.key], min_confidence: e.target.value } }))}>
                    <option value="">Any confidence</option>
                    <option value="low">Low+</option>
                    <option value="medium">Medium+</option>
                    <option value="high">High only</option>
                  </select>
                </div>

                <button onClick={() => runBoard(component.key)} style={secondaryButtonStyle} disabled={loading[component.key]}>{loading[component.key] ? 'Running…' : 'Run board'}</button>
                {runErrors[component.key] && <div style={errorStyle}>{runErrors[component.key]}</div>}

                <div style={resultGridStyle}>
                  {items.length === 0 ? (
                    <div style={emptyStyle}>No results yet. Run this board to populate fresh discovery for today.</div>
                  ) : (
                    items.slice(0, 5).map((item, idx) => (
                      <div key={`${component.key}-${idx}-${item.entity_id || item.entity_name || idx}`} style={resultCardStyle}>
                        <div style={resultHeaderStyle}>
                          <div>
                            <div style={resultTitleStyle}>{item.entity_name || item.title || 'Ranked item'}</div>
                            <div style={metaStyle}>{item.team || 'Team unavailable'} {item.opponent ? `vs ${item.opponent}` : ''}</div>
                          </div>
                          <div style={scorePillStyle}>{formatNumber(item.score)}</div>
                        </div>
                        <div style={resultBodyStyle}>{item.primary_reason || 'Model solver ranked this item from app-owned data.'}</div>
                        <div style={metaStyle}>Confidence: {item.confidence || 'low'}</div>
                        <div style={resultActionsStyle}>
                          <button onClick={() => saveItemToToday(component, item)} style={miniPrimaryStyle}>Save to Today</button>
                        </div>
                      </div>
                    ))
                  )}
                </div>
              </div>
            )
          })}
        </div>
      </section>
    </div>
  )
}

function InfoCard({ title, body }) {
  return (
    <div style={infoCardStyle}>
      <div style={infoTitleStyle}>{title}</div>
      <div style={infoBodyStyle}>{body}</div>
    </div>
  )
}

const pageStyle = { display: 'grid', gap: '18px' }
const heroStyle = { display: 'flex', justifyContent: 'space-between', gap: '18px', flexWrap: 'wrap', background: 'linear-gradient(135deg, #141925, #0d1117)', border: '1px solid #2a3243', borderRadius: '18px', padding: '24px' }
const eyebrowStyle = { color: '#8ab4ff', fontSize: '12px', fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: '8px' }
const titleStyle = { margin: 0, fontSize: '34px', color: '#e6edf3' }
const subtitleStyle = { margin: '10px 0 0', color: '#97a3b6', maxWidth: '760px', lineHeight: 1.6 }
const layoutStyle = { display: 'grid', gridTemplateColumns: 'minmax(260px, 0.9fr) minmax(320px, 1.1fr)', gap: '18px' }
const railStyle = { display: 'grid', gap: '14px' }
const infoCardStyle = { background: '#161b22', border: '1px solid #30363d', borderRadius: '16px', padding: '18px' }
const infoTitleStyle = { color: '#e6edf3', fontWeight: 700, marginBottom: '8px' }
const infoBodyStyle = { color: '#97a3b6', lineHeight: 1.55 }
const formCardStyle = { background: '#161b22', border: '1px solid #30363d', borderRadius: '18px', padding: '22px', display: 'grid', gap: '12px' }
const panelStyle = { background: '#161b22', border: '1px solid #30363d', borderRadius: '18px', padding: '20px', display: 'grid', gap: '14px' }
const panelHeaderStyle = { display: 'flex', justifyContent: 'space-between', gap: '16px', flexWrap: 'wrap', alignItems: 'flex-start' }
const cardTitleStyle = { margin: 0, color: '#e6edf3', fontSize: '24px' }
const cardSubtitleStyle = { margin: '8px 0 0', color: '#97a3b6', lineHeight: 1.5 }
const labelStyle = { color: '#c9d1d9', fontSize: '13px', fontWeight: 600 }
const inputStyle = { background: '#0d1117', color: '#e6edf3', border: '1px solid #30363d', borderRadius: '10px', padding: '10px 12px' }
const smallInputStyle = { background: '#111827', color: '#e6edf3', border: '1px solid #30363d', borderRadius: '8px', padding: '8px 10px', fontSize: '12px' }
const pillWrapStyle = { display: 'flex', flexWrap: 'wrap', gap: '8px' }
const pillStyle = { background: '#0d1117', color: '#c9d1d9', border: '1px solid #30363d', borderRadius: '999px', padding: '8px 12px', cursor: 'pointer' }
const activePillStyle = { ...pillStyle, background: '#1c365d', borderColor: '#3b82f6' }
const checkLabelStyle = { display: 'flex', gap: '8px', alignItems: 'center', color: '#c9d1d9', fontSize: '13px' }
const primaryButtonStyle = { background: '#7c3aed', color: '#fff', border: 0, borderRadius: '10px', padding: '12px 14px', fontWeight: 700, cursor: 'pointer' }
const secondaryButtonStyle = { background: '#111827', color: '#e6edf3', border: '1px solid #30363d', borderRadius: '10px', padding: '10px 12px', cursor: 'pointer' }
const miniPrimaryStyle = { background: '#2563eb', color: '#fff', border: 0, borderRadius: '8px', padding: '8px 10px', cursor: 'pointer', fontSize: '12px', fontWeight: 700 }
const summaryCardStyle = { minWidth: '320px', background: '#0d1117', border: '1px solid #273042', borderRadius: '14px', padding: '16px', display: 'grid', gap: '10px' }
const summaryRowStyle = { display: 'flex', justifyContent: 'space-between', gap: '10px', color: '#97a3b6', fontSize: '13px' }
const folderGridStyle = { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(210px, 1fr))', gap: '14px' }
const folderCardStyle = { background: 'linear-gradient(180deg, #141b2b, #111827)', border: '1px solid #293246', borderRadius: '16px', padding: '18px', display: 'grid', gap: '8px', minHeight: '120px' }
const folderBadgeStyle = { color: '#a78bfa', fontSize: '11px', fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.08em' }
const folderNameStyle = { color: '#e6edf3', fontSize: '20px', fontWeight: 700 }
const metaStyle = { color: '#8b949e', fontSize: '12px', lineHeight: 1.45 }
const seedGridStyle = { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: '14px' }
const seedCardStyle = { background: '#0d1117', border: '1px solid #263042', borderRadius: '14px', padding: '16px', display: 'grid', gap: '8px' }
const seedTitleStyle = { color: '#e6edf3', fontWeight: 700 }
const seedBodyStyle = { color: '#97a3b6', fontSize: '13px', lineHeight: 1.5 }
const todayShellStyle = { display: 'grid', gap: '14px' }
const savedListStyle = { display: 'grid', gap: '10px' }
const savedItemStyle = { background: '#0d1117', border: '1px solid #263042', borderRadius: '12px', padding: '12px' }
const savedTitleStyle = { color: '#e6edf3', fontWeight: 700, marginBottom: '4px' }
const savedBodyStyle = { color: '#97a3b6', fontSize: '13px', lineHeight: 1.45, marginBottom: '6px' }
const boardGridStyle = { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(320px, 1fr))', gap: '16px' }
const boardCardStyle = { background: '#0d1117', border: '1px solid #263042', borderRadius: '16px', padding: '16px', display: 'grid', gap: '12px' }
const boardHeaderStyle = { display: 'flex', justifyContent: 'space-between', gap: '12px', alignItems: 'flex-start' }
const boardTitleStyle = { color: '#e6edf3', fontWeight: 700 }
const boardDescriptionStyle = { color: '#97a3b6', fontSize: '13px', lineHeight: 1.45, marginTop: '6px' }
const countBadgeStyle = { background: '#1f2937', border: '1px solid #374151', borderRadius: '999px', color: '#e6edf3', padding: '6px 10px', fontSize: '12px', fontWeight: 700 }
const filterGridStyle = { display: 'grid', gridTemplateColumns: 'repeat(2, minmax(0, 1fr))', gap: '8px' }
const resultGridStyle = { display: 'grid', gap: '10px' }
const resultCardStyle = { background: '#111827', border: '1px solid #273042', borderRadius: '12px', padding: '12px', display: 'grid', gap: '8px' }
const resultHeaderStyle = { display: 'flex', justifyContent: 'space-between', gap: '10px' }
const resultTitleStyle = { color: '#e6edf3', fontWeight: 700 }
const resultBodyStyle = { color: '#97a3b6', fontSize: '13px', lineHeight: 1.45 }
const resultActionsStyle = { display: 'flex', justifyContent: 'flex-end' }
const scorePillStyle = { background: '#1d4ed8', color: '#fff', borderRadius: '999px', padding: '6px 10px', fontSize: '12px', fontWeight: 700, height: 'fit-content' }
const emptyStyle = { color: '#8b949e', fontSize: '13px', lineHeight: 1.5, padding: '8px 0' }
const successStyle = { background: '#10261a', border: '1px solid #1f6f43', color: '#7ee787', borderRadius: '10px', padding: '12px' }
const errorStyle = { background: '#2b1014', border: '1px solid #8b1e2d', color: '#ffb4be', borderRadius: '10px', padding: '12px' }
const stateStyle = { color: '#9aa4b2', padding: '36px', textAlign: 'center' }
