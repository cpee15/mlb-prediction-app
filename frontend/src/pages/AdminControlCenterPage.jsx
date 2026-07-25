import React, { useEffect, useMemo, useState } from 'react'
import { adminAccessState, dashboardApi, logoutDashboardSession } from '../lib/dashboardSession.mjs'
import {
  editableUserPayload,
  featureFlagUpdatePayload,
  settingUpdatePayload,
  userEditorValues,
} from '../lib/adminControlCenterState.mjs'

const FRANKLIN = '"Franklin Gothic Medium", "Franklin Gothic", "Arial Narrow", Arial, sans-serif'
const CENTURY = '"Century Gothic", CenturyGothic, AppleGothic, Arial, sans-serif'
const C = {
  bg: '#06101d', panel: '#111827', panel2: '#0b1322', border: 'rgba(148,163,184,.22)',
  text: '#e8eef8', muted: '#94a3b8', blue: '#60a5fa', green: '#34d399', amber: '#fbbf24', red: '#f87171',
}

const SECTIONS = [
  ['overview', 'Overview', 'functional'],
  ['objects', 'Object Manager', 'functional'],
  ['apps', 'Apps', 'functional'],
  ['users', 'Users & Access', 'functional'],
  ['settings', 'Settings', 'functional'],
  ['operations', 'Operations', 'locked'],
  ['workbench', 'Workbench', 'locked'],
  ['audit', 'Audit Log', 'functional'],
]

const ENDPOINTS = {
  overview: '/admin/overview',
  objects: '/admin/objects',
  apps: '/admin/apps',
  users: '/admin/users',
  audit: '/admin/audit-events',
}

async function loadAdminSection(section) {
  if (section === 'users') {
    const [users, profiles] = await Promise.all([
      dashboardApi('/admin/users'),
      dashboardApi('/admin/profiles'),
    ])
    return { ...users, profiles: profiles.profiles || [] }
  }
  if (section === 'settings') {
    const [settings, flags, profiles] = await Promise.all([
      dashboardApi('/admin/settings'),
      dashboardApi('/admin/feature-flags'),
      dashboardApi('/admin/profiles'),
    ])
    return {
      settings: settings.settings || [],
      feature_flags: flags.feature_flags || [],
      profiles: profiles.profiles || [],
    }
  }
  return dashboardApi(ENDPOINTS[section])
}

function titleCase(value) {
  return String(value || '').replace(/[_.-]+/g, ' ').replace(/\s+/g, ' ').trim().replace(/\b\w/g, character => character.toUpperCase())
}

function formatValue(value) {
  if (value == null || value === '') return '—'
  if (typeof value === 'boolean') return value ? 'Yes' : 'No'
  if (Array.isArray(value)) return value.length ? value.join(', ') : 'None'
  if (typeof value === 'object') return JSON.stringify(value)
  return String(value)
}

function Badge({ children, tone = 'blue' }) {
  const color = tone === 'green' ? C.green : tone === 'amber' ? C.amber : tone === 'red' ? C.red : C.blue
  return <span style={{ ...s.badge, color, borderColor: `${color}55`, background: `${color}15` }}>{children}</span>
}

function StatePage({ eyebrow, title, children, tone = 'blue', action }) {
  const color = tone === 'red' ? C.red : tone === 'amber' ? C.amber : C.blue
  return <main style={s.statePage}><section style={{ ...s.stateCard, borderColor: `${color}55` }}>
    <div style={{ ...s.eyebrow, color }}>{eyebrow}</div>
    <h1 style={s.title}>{title}</h1>
    <p style={s.copy}>{children}</p>
    {action || null}
  </section></main>
}

function Metric({ label, value, detail }) {
  return <div style={s.metric}><span style={s.metricLabel}>{label}</span><strong style={s.metricValue}>{formatValue(value)}</strong>{detail ? <small style={s.muted}>{detail}</small> : null}</div>
}

function OverviewPanel({ data }) {
  const counts = data?.counts || {}
  const hydration = data?.operations?.hydration || {}
  return <div style={s.stack}>
    <section style={s.heroCard}>
      <div><div style={s.eyebrow}>Owner Console</div><h2 style={s.sectionTitle}>System overview</h2><p style={s.copy}>Private visibility into registered MLBGPT objects, applications, users, configuration, and operational freshness.</p></div>
      <div style={s.badgeRow}><Badge tone="green">{data?.administrator?.role || 'admin'}</Badge><Badge>{counts.capabilities || 0} capabilities</Badge><Badge tone="amber">Owner only</Badge></div>
    </section>
    <div style={s.metricGrid}>
      <Metric label="Registered objects" value={counts.objects} />
      <Metric label="Queryable objects" value={counts.queryable_objects} />
      <Metric label="Application surfaces" value={counts.application_surfaces} />
      <Metric label="Registered users" value={counts.users} />
    </div>
    <section style={s.card}>
      <div style={s.cardHeader}><div><div style={s.eyebrow}>Freshness</div><h3 style={s.cardTitle}>Dashboard hydration</h3></div><Badge tone={hydration.has_error ? 'red' : hydration.status === 'unknown' ? 'amber' : 'green'}>{titleCase(hydration.status || 'unknown')}</Badge></div>
      <div style={s.detailGrid}>
        <Metric label="Target date" value={hydration.target_date} />
        <Metric label="Completed" value={hydration.completed_at} />
        <Metric label="Components" value={hydration.component_count} />
        <Metric label="Warnings" value={hydration.warning_count} />
      </div>
    </section>
    <section style={s.card}><div style={s.cardHeader}><div><div style={s.eyebrow}>Roadmap Boundary</div><h3 style={s.cardTitle}>Still locked</h3></div><Badge tone="amber">2 areas</Badge></div><div style={s.lockGrid}>{(data?.locked_sections || []).map(section => <div style={s.lockCard} key={section.key}><strong>{section.label}</strong><span>{section.next_phase}</span></div>)}</div></section>
  </div>
}

function ObjectPanel({ data }) {
  const [expanded, setExpanded] = useState('')
  const objects = data?.objects || []
  return <div style={s.stack}>
    <section style={s.sectionHeader}><div><div style={s.eyebrow}>Server Registry</div><h2 style={s.sectionTitle}>Object Manager</h2><p style={s.copy}>These contracts come directly from the report-type registry. Physical database inspection is not exposed.</p></div><div style={s.badgeRow}><Badge>{data?.totalSize || 0} registered</Badge><Badge tone="green">{data?.queryableSize || 0} queryable</Badge></div></section>
    <div style={s.objectList}>{objects.map(object => {
      const isOpen = expanded === object.api_name
      return <article style={s.card} key={object.api_name}>
        <button style={s.objectHeader} onClick={() => setExpanded(isOpen ? '' : object.api_name)} aria-expanded={isOpen}>
          <span><small style={s.apiName}>{object.api_name}</small><strong style={s.objectLabel}>{object.label}</strong><small style={s.muted}>{object.base_object} · UI: {object.ui_object}</small></span>
          <span style={s.badgeRow}><Badge tone={object.queryable ? 'green' : 'amber'}>{object.queryable ? 'Queryable' : 'Registered only'}</Badge><span style={s.disclosure}>{isOpen ? '−' : '+'}</span></span>
        </button>
        {!isOpen ? null : <div style={s.objectBody}>
          <div style={s.detailGrid}>
            <Metric label="Population contract" value={object.population} />
            <Metric label="Relationships" value={object.relationships} />
            <Metric label="Freshness" value={object.freshness} />
            <Metric label="Filtering" value={object.filtering?.supported} detail={`${object.filtering?.field_count || 0} fields · ${(object.filtering?.logic || []).map(titleCase).join(' / ') || 'No logic'}`} />
            <Metric label="Sorting" value={object.sorting?.supported} detail={`${object.sorting?.field_count || 0} fields`} />
          </div>
          <div style={s.tableWrap}><table style={s.table}><thead><tr><th style={s.th}>Field</th><th style={s.th}>API name</th><th style={s.th}>Type</th><th style={s.th}>Operators</th><th style={s.th}>Freshness</th><th style={s.th}>Report</th><th style={s.th}>Filter</th><th style={s.th}>Sort</th></tr></thead><tbody>{(object.fields || []).map(field => <tr key={field.name}><td style={s.td}>{field.label}</td><td style={s.codeCell}>{field.name}</td><td style={s.td}>{field.data_type}</td><td style={s.td}>{formatValue(field.supported_operators)}</td><td style={s.td}>{titleCase(field.freshness)}</td><td style={s.td}>{field.selectable !== false ? 'Yes' : 'No'}</td><td style={s.td}>{field.filterable ? 'Yes' : 'No'}</td><td style={s.td}>{field.sortable ? 'Yes' : 'No'}</td></tr>)}</tbody></table>{!object.fields?.length ? <div style={s.empty}>No queryable field catalog is registered for this object.</div> : null}</div>
        </div>}
      </article>
    })}</div>
  </div>
}

function AppsPanel({ data }) {
  return <div style={s.stack}><section style={s.sectionHeader}><div><div style={s.eyebrow}>Code-owned Registry</div><h2 style={s.sectionTitle}>Applications</h2><p style={s.copy}>User-facing application surfaces and their safe availability classifications.</p></div><Badge>{data?.totalSize || 0} surfaces</Badge></section><div style={s.appGrid}>{(data?.apps || []).map(app => <article style={s.card} key={app.key}><div style={s.cardHeader}><div><small style={s.apiName}>{app.route}</small><h3 style={s.cardTitle}>{app.label}</h3></div><Badge tone={app.visibility === 'admin' ? 'amber' : app.visibility === 'authenticated' ? 'green' : 'blue'}>{titleCase(app.visibility)}</Badge></div><div style={s.detailList}><span><b>Status</b>{titleCase(app.feature_status)}</span><span><b>Health</b>{titleCase(app.health_classification)}</span></div></article>)}</div></div>
}

function UsersPanel({ data, currentUserId, onRefresh }) {
  const [selected, setSelected] = useState(null)
  const [editor, setEditor] = useState({})
  const [saving, setSaving] = useState(false)
  const [message, setMessage] = useState('')

  async function editUser(userId) {
    setMessage('')
    try {
      const payload = await dashboardApi(`/admin/users/${userId}`)
      setSelected(payload.user)
      setEditor(userEditorValues(payload.user))
    } catch (error) {
      setMessage(error.message || 'User profile could not be loaded.')
    }
  }

  async function saveUser(event) {
    event.preventDefault()
    setSaving(true)
    setMessage('')
    try {
      const payload = await dashboardApi(`/admin/users/${selected.id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(editableUserPayload(editor)),
      })
      setSelected(payload.user)
      setEditor(userEditorValues(payload.user))
      setMessage(payload.sessions_revoked ? 'Profile saved. Existing sessions were revoked.' : 'Profile saved.')
      await onRefresh()
    } catch (error) {
      setMessage(error.message || 'Profile could not be saved.')
    } finally {
      setSaving(false)
    }
  }

  return <div style={s.stack}>
    <section style={s.sectionHeader}><div><div style={s.eyebrow}>Safe Directory</div><h2 style={s.sectionTitle}>Users & Access</h2><p style={s.copy}>Edit directory metadata and account state. Roles and capabilities remain server-owned and cannot be changed here.</p></div><Badge>{data?.totalSize || 0} users</Badge></section>
    <div style={s.appGrid}>{(data?.profiles || []).map(profile => <article style={s.card} key={profile.key}><div style={s.cardHeader}><div><small style={s.apiName}>{profile.key}</small><h3 style={s.cardTitle}>{profile.label}</h3></div><Badge tone={profile.role === 'admin' ? 'amber' : 'blue'}>{titleCase(profile.role)}</Badge></div><p style={s.copy}>{profile.description}</p><small style={s.muted}>{profile.capabilities.length} server-owned capabilities · Read only</small></article>)}</div>
    {message ? <div style={message.includes('could not') ? s.error : s.success}>{message}</div> : null}
    {selected ? <form style={s.card} onSubmit={saveUser}>
      <div style={s.cardHeader}><div><div style={s.eyebrow}>Directory record</div><h3 style={s.cardTitle}>{selected.email}</h3></div><button type="button" style={s.secondaryButton} onClick={() => setSelected(null)}>Cancel</button></div>
      <div style={s.formGrid}>{['username', 'first_name', 'last_name', 'display_name', 'alias', 'title', 'company', 'locale', 'language', 'timezone'].map(key => <label style={s.field} key={key}>{titleCase(key)}<input style={s.input} value={editor[key] || ''} onChange={event => setEditor(current => ({ ...current, [key]: event.target.value }))} /></label>)}</div>
      <div style={s.checkRow}><label><input type="checkbox" checked={editor.is_active !== false} disabled={selected.id === currentUserId} onChange={event => setEditor(current => ({ ...current, is_active: event.target.checked }))} /> Active</label><label><input type="checkbox" checked={Boolean(editor.is_locked)} disabled={selected.id === currentUserId} onChange={event => setEditor(current => ({ ...current, is_locked: event.target.checked }))} /> Locked</label><Badge>{titleCase(selected.profile_key)}</Badge></div>
      <button style={s.primaryButton} disabled={saving}>{saving ? 'Saving…' : 'Save Profile'}</button>
    </form> : null}
    <div style={s.tableWrap}><table style={s.table}><thead><tr><th style={s.th}>User</th><th style={s.th}>Role</th><th style={s.th}>Plan</th><th style={s.th}>Capabilities</th><th style={s.th}>Updated</th><th style={s.th}>Action</th></tr></thead><tbody>{(data?.users || []).map(user => <tr key={user.id}><td style={s.td}><strong>{user.username}</strong><small style={s.userEmail}>{user.email}</small></td><td style={s.td}><Badge tone={user.role === 'admin' ? 'amber' : 'blue'}>{titleCase(user.role)}</Badge></td><td style={s.td}>{titleCase(user.plan)}</td><td style={s.td}>{formatValue(user.capabilities)}</td><td style={s.td}>{user.updated_at || '—'}</td><td style={s.td}><button style={s.inlineButton} onClick={() => editUser(user.id)}>Edit</button></td></tr>)}</tbody></table></div>
  </div>
}

function SettingsPanel({ data, onRefresh, isMobile }) {
  const [values, setValues] = useState({})
  const [flagTargets, setFlagTargets] = useState({})
  const [flagEnabled, setFlagEnabled] = useState({})
  const [message, setMessage] = useState('')

  async function saveSetting(setting) {
    setMessage('')
    try {
      await dashboardApi('/admin/settings', {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(settingUpdatePayload(setting, values[`${setting.namespace}.${setting.key}`] ?? setting.value)),
      })
      setMessage(`${titleCase(setting.key)} saved.`)
      await onRefresh()
    } catch (error) { setMessage(error.message || 'Setting could not be saved.') }
  }

  async function saveFlag(flag) {
    setMessage('')
    try {
      await dashboardApi('/admin/feature-flags', {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(featureFlagUpdatePayload(
          flag,
          flagEnabled[flag.key] ?? flag.enabled,
          flagTargets[flag.key] ?? flag.target_profiles,
        )),
      })
      setMessage(`${flag.label} saved.`)
      await onRefresh()
    } catch (error) { setMessage(error.message || 'Feature flag could not be saved.') }
  }

  function toggleTarget(flag, key) {
    const current = flagTargets[flag.key] ?? flag.target_profiles
    const next = current.includes(key) ? current.filter(item => item !== key) : [...current, key]
    setFlagTargets(values => ({ ...values, [flag.key]: next }))
  }

  function toggleFlagEnabled(flag) {
    const current = flagEnabled[flag.key] ?? flag.enabled
    setFlagEnabled(values => ({ ...values, [flag.key]: !current }))
  }

  return <div style={s.stack}>
    <section style={s.sectionHeader}><div><div style={s.eyebrow}>Validated Configuration</div><h2 style={s.sectionTitle}>Settings</h2><p style={s.copy}>Only code-registered keys and values are accepted. These controls cannot grant capabilities, store secrets, or bypass server authorization.</p></div><Badge tone="green">Audited</Badge></section>
    {message ? <div style={message.includes('could not') ? s.error : s.success}>{message}</div> : null}
    <section style={s.card}><div style={s.eyebrow}>Global defaults</div><div style={s.settingList}>{(data?.settings || []).map(setting => { const id = `${setting.namespace}.${setting.key}`; const allowed = setting.validation?.allowed || []; return <div style={{ ...s.settingRow, ...(isMobile ? s.settingRowMobile : {}) }} key={id}><div><strong>{titleCase(setting.key)}</strong><small style={s.muted}>{setting.description}</small><small style={s.apiName}>{id}</small></div>{allowed.length ? <select style={s.input} value={values[id] ?? setting.value} onChange={event => setValues(current => ({ ...current, [id]: event.target.value }))}>{allowed.map(value => <option key={value}>{value}</option>)}</select> : <input style={s.input} value={values[id] ?? setting.value} onChange={event => setValues(current => ({ ...current, [id]: event.target.value }))} />}<button style={s.inlineButton} onClick={() => saveSetting(setting)}>Save</button></div> })}</div></section>
    <section style={s.card}><div style={s.eyebrow}>Feature flags</div><p style={s.copy}>All flags default off. They are foundation records only and are not wired into public behavior in this phase.</p><div style={s.settingList}>{(data?.feature_flags || []).map(flag => { const enabled = flagEnabled[flag.key] ?? flag.enabled; return <div style={{ ...s.settingRow, ...(isMobile ? s.settingRowMobile : {}) }} key={flag.key}><div><strong>{flag.label}</strong><small style={s.muted}>{flag.description}</small><small style={s.apiName}>{flag.key}</small><div style={s.checkRow}><label><input type="checkbox" checked={enabled} onChange={() => toggleFlagEnabled(flag)} /> Enabled</label>{(data?.profiles || []).map(profile => <label key={profile.key}><input type="checkbox" checked={(flagTargets[flag.key] ?? flag.target_profiles).includes(profile.key)} onChange={() => toggleTarget(flag, profile.key)} /> {profile.label}</label>)}</div></div><Badge tone={enabled ? 'green' : 'amber'}>{enabled ? 'Enabled' : 'Disabled'}</Badge><button style={s.inlineButton} onClick={() => saveFlag(flag)}>Save Flag</button></div> })}</div></section>
  </div>
}

function AuditPanel({ data }) {
  return <div style={s.stack}><section style={s.sectionHeader}><div><div style={s.eyebrow}>Immutable History</div><h2 style={s.sectionTitle}>Audit Log</h2><p style={s.copy}>Safe summaries of successful Control Center mutations. Credentials, sessions, secrets, and saved-report content are excluded.</p></div><Badge>{data?.totalSize || 0} events</Badge></section><div style={s.tableWrap}><table style={s.table}><thead><tr><th style={s.th}>Time</th><th style={s.th}>Actor</th><th style={s.th}>Action</th><th style={s.th}>Target</th><th style={s.th}>Before</th><th style={s.th}>After</th></tr></thead><tbody>{(data?.audit_events || []).map(event => <tr key={event.id}><td style={s.td}>{event.created_at}</td><td style={s.td}>{event.actor?.username || event.actor?.id}</td><td style={s.codeCell}>{event.action}</td><td style={s.td}>{event.target_type}: {event.target_identifier}</td><td style={s.td}>{formatValue(event.before)}</td><td style={s.td}>{formatValue(event.after)}</td></tr>)}</tbody></table>{!data?.audit_events?.length ? <div style={s.empty}>No administrative changes have been recorded yet.</div> : null}</div></div>
}

function LockedPanel({ section, overview }) {
  const contract = (overview?.locked_sections || []).find(item => item.key === section)
  return <section style={s.lockedPanel}><div style={s.lockIcon}>◇</div><div style={s.eyebrow}>Phase 2 Boundary</div><h2 style={s.sectionTitle}>{contract?.label || titleCase(section)} is locked</h2><p style={s.copy}>{contract?.next_phase || 'This administrative area is intentionally unavailable in the current foundation.'}</p><Badge tone="amber">Unavailable</Badge></section>
}

export default function AdminControlCenterPage() {
  const [width, setWidth] = useState(() => typeof window === 'undefined' ? 1200 : window.innerWidth)
  const [active, setActive] = useState('overview')
  const [profile, setProfile] = useState(null)
  const [status, setStatus] = useState(null)
  const [loading, setLoading] = useState(true)
  const [sectionLoading, setSectionLoading] = useState(false)
  const [error, setError] = useState('')
  const [data, setData] = useState({})
  const isMobile = width < 760
  const accessState = adminAccessState({ loading, authenticated: Boolean(profile), status })

  useEffect(() => {
    const updateWidth = () => setWidth(window.innerWidth)
    window.addEventListener('resize', updateWidth)
    return () => window.removeEventListener('resize', updateWidth)
  }, [])

  useEffect(() => {
    let cancelled = false
    async function verifyAccess() {
      setLoading(true)
      try {
        const profilePayload = await dashboardApi('/my-dashboard/profile')
        if (cancelled) return
        if (!profilePayload.authenticated) {
          setProfile(null)
          setStatus(401)
          return
        }
        setProfile(profilePayload.user)
        const overview = await dashboardApi('/admin/overview')
        if (cancelled) return
        setData(current => ({ ...current, overview }))
        setStatus(200)
      } catch (requestError) {
        if (cancelled) return
        setStatus(requestError?.status || 500)
        setError(requestError?.message || 'Control Center could not be loaded.')
      } finally {
        if (!cancelled) setLoading(false)
      }
    }
    verifyAccess()
    return () => { cancelled = true }
  }, [])

  useEffect(() => {
    if (accessState !== 'ready' || (!ENDPOINTS[active] && !['users', 'settings'].includes(active)) || data[active]) return
    let cancelled = false
    async function loadSection() {
      setSectionLoading(true)
      setError('')
      try {
        const payload = await loadAdminSection(active)
        if (!cancelled) setData(current => ({ ...current, [active]: payload }))
      } catch (requestError) {
        if (!cancelled) {
          setStatus(requestError?.status || 500)
          setError(requestError?.message || 'This administrative section could not be loaded.')
        }
      } finally {
        if (!cancelled) setSectionLoading(false)
      }
    }
    loadSection()
    return () => { cancelled = true }
  }, [active, accessState, data])

  async function refreshSection(section) {
    const payload = await loadAdminSection(section)
    setData(current => ({ ...current, [section]: payload }))
  }

  const sectionContent = useMemo(() => {
    if (active === 'overview') return <OverviewPanel data={data.overview} />
    if (active === 'objects') return <ObjectPanel data={data.objects} />
    if (active === 'apps') return <AppsPanel data={data.apps} />
    if (active === 'users') return <UsersPanel data={data.users} currentUserId={profile?.id} onRefresh={() => refreshSection('users')} />
    if (active === 'settings') return <SettingsPanel data={data.settings} onRefresh={() => refreshSection('settings')} isMobile={isMobile} />
    if (active === 'audit') return <AuditPanel data={data.audit} />
    return <LockedPanel section={active} overview={data.overview} />
  }, [active, data, profile, isMobile])

  if (accessState === 'loading' && !data.overview) return <StatePage eyebrow="MLBGPT Control Center" title="Verifying administrator access">Checking the active server session.</StatePage>
  if (accessState === 'sign_in_required') return <StatePage eyebrow="Private Administration" title="Sign-in required" action={<a style={s.primaryLink} href="/my-dashboard">Sign in through MyDashboard</a>}>A password-backed MyDashboard session is required before this route can verify administrator access.</StatePage>
  if (accessState === 'access_denied') return <StatePage eyebrow="Private Administration" title="Access denied" tone="red" action={<a style={s.secondaryLink} href="/my-dashboard">Return to MyDashboard</a>}>Your account is authenticated, but the server did not grant Control Center access.</StatePage>
  if (accessState === 'error') return <StatePage eyebrow="Control Center Error" title="Unable to verify access" tone="red" action={<button style={s.secondaryButton} onClick={() => window.location.reload()}>Try Again</button>}>{error || 'The server could not verify this administrative session.'}</StatePage>

  async function signOut() {
    await logoutDashboardSession().catch(() => null)
    window.location.assign('/my-dashboard')
  }

  return <main style={s.page}>
    <header style={s.topbar}><div><div style={s.eyebrow}>MLBGPT</div><h1 style={s.topTitle}>Control Center</h1></div><div style={s.topActions}><span style={s.identity}>{profile?.username}<small>{profile?.email}</small></span><a href="/my-dashboard" style={s.secondaryLink}>MyDashboard</a><button style={s.secondaryButton} onClick={signOut}>Sign Out</button></div></header>
    <div style={{ ...s.shell, ...(isMobile ? s.shellMobile : {}) }}>
      <nav style={{ ...s.sidebar, ...(isMobile ? s.sidebarMobile : {}) }} aria-label="Control Center navigation"><div style={{ ...s.navLabel, ...(isMobile ? s.navLabelMobile : {}) }}>Administration</div>{SECTIONS.map(([key, label, mode]) => <button key={key} style={active === key ? s.navActive : s.navButton} onClick={() => { setActive(key); setError('') }}><span>{label}</span>{mode === 'locked' ? <small>Locked</small> : null}</button>)}</nav>
      <section style={s.content}>{sectionLoading ? <div style={s.loadingBar}>Loading section…</div> : null}{error ? <div style={s.error}>{error}</div> : null}{sectionContent}</section>
    </div>
  </main>
}

const s = {
  page: { minHeight: '100vh', color: C.text, background: `linear-gradient(180deg,${C.bg},#030712)`, fontFamily: CENTURY, fontSize: 13, lineHeight: 1.45 },
  statePage: { minHeight: '100vh', display: 'grid', placeItems: 'center', padding: 18, color: C.text, background: `linear-gradient(180deg,${C.bg},#030712)`, fontFamily: CENTURY },
  stateCard: { width: 'min(560px,100%)', boxSizing: 'border-box', padding: 'clamp(22px,5vw,42px)', background: C.panel, border: `1px solid ${C.border}`, borderRadius: 22 },
  topbar: { display: 'flex', justifyContent: 'space-between', gap: 16, flexWrap: 'wrap', alignItems: 'center', padding: '15px clamp(14px,3vw,32px)', background: 'rgba(6,16,29,.94)', borderBottom: `1px solid ${C.border}` },
  topTitle: { margin: 0, fontFamily: FRANKLIN, fontSize: 25, fontWeight: 500, letterSpacing: '-.01em' },
  topActions: { display: 'flex', gap: 8, alignItems: 'center', flexWrap: 'wrap' },
  identity: { display: 'grid', marginRight: 4, fontFamily: FRANKLIN, fontSize: 12, textAlign: 'right' },
  shell: { display: 'grid', gridTemplateColumns: 'minmax(190px,230px) minmax(0,1fr)', maxWidth: 1600, margin: '0 auto' },
  shellMobile: { gridTemplateColumns: '1fr' },
  sidebar: { position: 'sticky', top: 0, alignSelf: 'start', display: 'grid', gap: 6, minHeight: 'calc(100vh - 78px)', padding: 16, boxSizing: 'border-box', background: C.panel2, borderRight: `1px solid ${C.border}` },
  sidebarMobile: { position: 'static', minHeight: 'auto', gridTemplateColumns: 'repeat(2,minmax(0,1fr))', borderRight: 0, borderBottom: `1px solid ${C.border}` },
  navLabel: { margin: '3px 6px 8px', color: C.muted, fontFamily: FRANKLIN, fontSize: 10, letterSpacing: '.11em', textTransform: 'uppercase' },
  navLabelMobile: { gridColumn: '1 / -1' },
  navButton: { display: 'flex', justifyContent: 'space-between', gap: 8, padding: '10px 11px', color: C.muted, fontFamily: FRANKLIN, fontSize: 12, textAlign: 'left', background: 'transparent', border: '1px solid transparent', borderRadius: 9 },
  navActive: { display: 'flex', justifyContent: 'space-between', gap: 8, padding: '10px 11px', color: C.text, fontFamily: FRANKLIN, fontSize: 12, textAlign: 'left', background: 'rgba(96,165,250,.15)', border: '1px solid rgba(96,165,250,.45)', borderRadius: 9 },
  content: { minWidth: 0, padding: 'clamp(14px,3vw,32px)' }, stack: { display: 'grid', gap: 14 },
  heroCard: { display: 'flex', justifyContent: 'space-between', gap: 18, flexWrap: 'wrap', padding: 'clamp(17px,3vw,27px)', background: C.panel, border: `1px solid ${C.border}`, borderRadius: 18 },
  sectionHeader: { display: 'flex', justifyContent: 'space-between', gap: 16, flexWrap: 'wrap', alignItems: 'end', paddingBottom: 4 },
  title: { margin: '7px 0 10px', fontFamily: FRANKLIN, fontSize: 'clamp(28px,4vw,40px)', fontWeight: 500, lineHeight: 1.05 },
  sectionTitle: { margin: '4px 0 5px', fontFamily: FRANKLIN, fontSize: 'clamp(23px,3vw,31px)', fontWeight: 500, lineHeight: 1.1 },
  cardTitle: { margin: '3px 0', fontFamily: FRANKLIN, fontSize: 18, fontWeight: 500 },
  eyebrow: { color: C.blue, fontFamily: FRANKLIN, fontSize: 10, fontWeight: 500, letterSpacing: '.11em', textTransform: 'uppercase' },
  copy: { maxWidth: 760, margin: '5px 0 14px', color: C.muted, fontSize: 13, lineHeight: 1.55 },
  muted: { display: 'block', color: C.muted, fontSize: 11, fontWeight: 400 },
  card: { minWidth: 0, padding: 15, background: C.panel, border: `1px solid ${C.border}`, borderRadius: 15 },
  cardHeader: { display: 'flex', justifyContent: 'space-between', gap: 12, flexWrap: 'wrap', alignItems: 'start' },
  badgeRow: { display: 'flex', gap: 7, flexWrap: 'wrap', alignItems: 'center' },
  badge: { display: 'inline-flex', padding: '4px 8px', fontFamily: FRANKLIN, fontSize: 10, fontWeight: 500, border: '1px solid', borderRadius: 999 },
  metricGrid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit,minmax(150px,1fr))', gap: 10 },
  detailGrid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit,minmax(150px,1fr))', gap: 9, marginTop: 12 },
  metric: { display: 'grid', gap: 3, minWidth: 0, padding: 12, background: C.panel2, border: `1px solid ${C.border}`, borderRadius: 11 },
  metricLabel: { color: C.muted, fontFamily: FRANKLIN, fontSize: 10, textTransform: 'uppercase', letterSpacing: '.07em' },
  metricValue: { overflowWrap: 'anywhere', fontFamily: CENTURY, fontSize: 17, fontWeight: 500 },
  lockGrid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit,minmax(190px,1fr))', gap: 9, marginTop: 12 },
  lockCard: { display: 'grid', gap: 5, padding: 12, color: C.muted, background: C.panel2, border: `1px solid ${C.border}`, borderRadius: 11 },
  lockedPanel: { display: 'grid', justifyItems: 'start', alignContent: 'center', minHeight: 460, padding: 'clamp(20px,5vw,54px)', background: C.panel, border: `1px solid ${C.border}`, borderRadius: 18 },
  lockIcon: { marginBottom: 15, color: C.amber, fontFamily: FRANKLIN, fontSize: 38 },
  objectList: { display: 'grid', gap: 10 }, objectHeader: { width: '100%', display: 'flex', justifyContent: 'space-between', gap: 12, alignItems: 'center', padding: 0, color: C.text, fontFamily: FRANKLIN, textAlign: 'left', background: 'transparent', border: 0 },
  objectLabel: { display: 'block', margin: '3px 0', fontFamily: FRANKLIN, fontSize: 17, fontWeight: 500 },
  apiName: { display: 'block', color: C.blue, fontFamily: CENTURY, fontSize: 10, overflowWrap: 'anywhere' }, disclosure: { fontFamily: FRANKLIN, fontSize: 22 },
  objectBody: { marginTop: 14, paddingTop: 14, borderTop: `1px solid ${C.border}` },
  tableWrap: { minWidth: 0, marginTop: 12, overflowX: 'auto', background: C.panel, border: `1px solid ${C.border}`, borderRadius: 12 },
  table: { width: '100%', minWidth: 760, borderCollapse: 'collapse', fontFamily: CENTURY, fontSize: 11 },
  th: { padding: '9px 10px', color: C.muted, fontFamily: FRANKLIN, fontSize: 10, fontWeight: 500, textAlign: 'left', textTransform: 'uppercase', letterSpacing: '.05em', background: C.panel2, borderBottom: `1px solid ${C.border}` },
  td: { padding: '8px 10px', verticalAlign: 'top', borderBottom: `1px solid ${C.border}` },
  codeCell: { padding: '8px 10px', color: C.blue, fontFamily: 'ui-monospace, SFMono-Regular, Menlo, monospace', fontSize: 10, borderBottom: `1px solid ${C.border}` },
  empty: { padding: 18, color: C.muted, textAlign: 'center' },
  appGrid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit,minmax(240px,1fr))', gap: 10 },
  detailList: { display: 'grid', gap: 8, marginTop: 12, color: C.muted },
  userEmail: { display: 'block', color: C.muted, fontSize: 10 },
  primaryLink: { display: 'inline-flex', padding: '10px 14px', color: C.text, fontFamily: FRANKLIN, fontSize: 12, textDecoration: 'none', background: 'rgba(96,165,250,.2)', border: '1px solid rgba(96,165,250,.5)', borderRadius: 9 },
  secondaryLink: { display: 'inline-flex', padding: '8px 11px', color: C.text, fontFamily: FRANKLIN, fontSize: 11, textDecoration: 'none', background: C.panel2, border: `1px solid ${C.border}`, borderRadius: 8 },
  secondaryButton: { padding: '8px 11px', color: C.text, fontFamily: FRANKLIN, fontSize: 11, background: C.panel2, border: `1px solid ${C.border}`, borderRadius: 8 },
  loadingBar: { marginBottom: 10, padding: 8, color: C.blue, background: 'rgba(96,165,250,.1)', borderRadius: 8 },
  error: { marginBottom: 10, padding: 10, color: '#fecaca', background: 'rgba(248,113,113,.12)', border: '1px solid rgba(248,113,113,.3)', borderRadius: 9 },
  success: { marginBottom: 10, padding: 10, color: '#a7f3d0', background: 'rgba(52,211,153,.12)', border: '1px solid rgba(52,211,153,.3)', borderRadius: 9 },
  formGrid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit,minmax(180px,1fr))', gap: 10, margin: '14px 0' },
  field: { display: 'grid', gap: 5, color: C.muted, fontSize: 11 },
  input: { minWidth: 0, boxSizing: 'border-box', padding: '8px 9px', color: C.text, fontFamily: CENTURY, fontSize: 12, background: C.panel2, border: `1px solid ${C.border}`, borderRadius: 8 },
  checkRow: { display: 'flex', gap: 14, flexWrap: 'wrap', alignItems: 'center', margin: '10px 0' },
  primaryButton: { padding: '9px 13px', color: C.text, fontFamily: FRANKLIN, fontSize: 12, background: 'rgba(52,211,153,.18)', border: '1px solid rgba(52,211,153,.45)', borderRadius: 8 },
  inlineButton: { padding: '6px 9px', color: C.text, fontFamily: FRANKLIN, fontSize: 10, background: 'rgba(96,165,250,.14)', border: '1px solid rgba(96,165,250,.35)', borderRadius: 7 },
  settingList: { display: 'grid', gap: 8, marginTop: 12 },
  settingRow: { display: 'grid', gridTemplateColumns: 'minmax(220px,1fr) minmax(150px,260px) auto', gap: 10, alignItems: 'center', padding: 11, background: C.panel2, border: `1px solid ${C.border}`, borderRadius: 10 },
  settingRowMobile: { gridTemplateColumns: 'minmax(0,1fr)', alignItems: 'stretch' },
}
