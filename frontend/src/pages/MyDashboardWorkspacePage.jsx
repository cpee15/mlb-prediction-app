import React, { useEffect, useMemo, useState } from 'react'
import { DEFAULT_BUILDER_FIELDS, collectBuilderFieldGroups, getValueByPath } from './myDashboardWorkspaceFieldLibrary'

const API = import.meta.env.VITE_API_BASE_URL || ''
const CACHE_PREFIX = 'my-dashboard:v5:'
const DASHBOARD_SESSION_STORAGE_KEY = 'mlbgpt_dashboard_session_token'
const SHELF_PREF_KEY = 'my-dashboard:v6:saved-shelf-open'
const SURFACE_PREF_KEY = 'my-dashboard:v6:surface'

const COMPONENTS = [
  { key: 'hitters', title: 'My Top Hitters Today', shortTitle: 'Hitters', description: 'Stored 365 hitter board with pitch-type matchup, EV, LA, and arsenal context.' },
  { key: 'pitchers', title: 'My Top Pitchers Today', shortTitle: 'Pitchers', description: 'Pitcher lean board using K profile, contact suppression, and opponent context.' },
  { key: 'teams', title: 'My Top Teams Today', shortTitle: 'Teams', description: 'Team board from model side edge, projected runs, and offense profile.' },
  { key: 'totals', title: 'My Top Totals Today', shortTitle: 'Totals', description: 'Game total board from projected runs, run environment, and simulation context.' },
  { key: 'overall_players', title: 'My Top Overall Players Today', shortTitle: 'Overall', description: 'Combined player board blending hitter and pitcher solver outputs.' },
]

const FEATURE_CHOICES = ['Matchups', 'Daily Odds', 'Model Projections', 'News', 'Props', 'Pitchers', 'Batters']
const BASIC_FILTERS = { search_text: '', team: '', opponent: '', min_score: '', max_score: '', min_confidence: '', category: '', player_type: '', pitch_type: '', source: '' }
const SURFACES = ['boards', 'builder']

const styles = {
  page: { minHeight: '100vh', background: 'linear-gradient(180deg,#0b1020 0%,#09111f 100%)', color: '#e5eefc', padding: 24 },
  hero: { display: 'grid', gridTemplateColumns: '1.2fr 0.8fr', gap: 20, background: 'rgba(17,28,52,0.92)', border: '1px solid rgba(148,163,184,0.18)', borderRadius: 28, padding: 24, marginBottom: 20 },
  shell: { display: 'grid', gridTemplateColumns: '250px 1fr', gap: 18 },
  side: { position: 'sticky', top: 20, alignSelf: 'start', background: 'rgba(15,23,42,0.9)', border: '1px solid rgba(148,163,184,0.18)', borderRadius: 22, padding: 16, display: 'grid', gap: 10 },
  main: { display: 'grid', gap: 18 },
  card: { background: 'rgba(17,28,52,0.88)', border: '1px solid rgba(148,163,184,0.18)', borderRadius: 24, padding: 18, boxShadow: '0 16px 44px rgba(2,6,23,0.22)' },
  boardGrid: { display: 'grid', gridTemplateColumns: '430px 1fr', gap: 18 },
  builderGrid: { display: 'grid', gridTemplateColumns: '1fr 1fr 1.1fr', gap: 18 },
  savedGrid: { display: 'grid', gridTemplateColumns: '320px 1fr', gap: 18, marginTop: 16 },
  folderSplit: { display: 'grid', gridTemplateColumns: '0.9fr 1.1fr', gap: 16 },
  input: { width: '100%', borderRadius: 14, border: '1px solid rgba(148,163,184,0.18)', background: 'rgba(15,23,42,0.72)', color: '#e5eefc', padding: '11px 12px', outline: 'none' },
  textarea: { minHeight: 90, width: '100%', borderRadius: 16, border: '1px solid rgba(148,163,184,0.18)', background: 'rgba(15,23,42,0.72)', color: '#e5eefc', padding: 12, resize: 'vertical' },
  primary: { border: '1px solid rgba(94,162,255,0.36)', background: 'linear-gradient(180deg,rgba(94,162,255,0.24),rgba(94,162,255,0.12))', color: '#e5eefc', borderRadius: 14, padding: '11px 14px', fontWeight: 800, cursor: 'pointer' },
  secondary: { border: '1px solid rgba(148,163,184,0.18)', background: 'rgba(17,28,52,0.74)', color: '#e5eefc', borderRadius: 14, padding: '11px 14px', fontWeight: 700, cursor: 'pointer' },
  ghost: { border: '1px solid rgba(148,163,184,0.14)', background: 'transparent', color: '#9aa9c7', borderRadius: 14, padding: '11px 14px', fontWeight: 700, cursor: 'pointer' },
  row: { display: 'flex', gap: 10, flexWrap: 'wrap' },
  between: { display: 'flex', justifyContent: 'space-between', alignItems: 'start', gap: 16 },
  title: { margin: 0, fontSize: 34, lineHeight: 1.1 },
  eyebrow: { color: '#5ea2ff', fontSize: 12, textTransform: 'uppercase', letterSpacing: '0.14em', fontWeight: 800, marginBottom: 8 },
  sub: { margin: '10px 0 0', color: '#9aa9c7', lineHeight: 1.7, fontSize: 14 },
  sectionTitle: { fontSize: 18, fontWeight: 750 },
  small: { color: '#9aa9c7', fontSize: 12 },
  empty: { borderRadius: 16, border: '1px dashed rgba(148,163,184,0.18)', background: 'rgba(15,23,42,0.34)', padding: 18, color: '#9aa9c7', lineHeight: 1.6 },
  folderList: { display: 'grid', gap: 10, maxHeight: 460, overflowY: 'auto' },
  resultList: { display: 'grid', gap: 14 },
  fieldList: { display: 'grid', gap: 10, maxHeight: 650, overflowY: 'auto' },
}

function todayIso() { return new Date().toISOString().slice(0, 10) }
function clone(value) { return JSON.parse(JSON.stringify(value)) }
function emptyFilters() { return { ...BASIC_FILTERS, metrics: {}, weights: {} } }
function defaultFiltersByComponent() { return Object.fromEntries(COMPONENTS.map(component => [component.key, emptyFilters()])) }
function defaultLineupToggles() { return Object.fromEntries(COMPONENTS.map(component => [component.key, false])) }
function cacheKey(kind, payload) { return `${CACHE_PREFIX}${kind}:${JSON.stringify(payload)}` }
function formatNumber(value) { const num = Number(value); if (!Number.isFinite(num)) return '—'; return Math.abs(num) >= 10 ? num.toFixed(1) : num.toFixed(3) }
function ensureArray(value) { return Array.isArray(value) ? value : [] }
function readSessionCache(key) { if (typeof window === 'undefined' || !window.sessionStorage) return null; try { const raw = window.sessionStorage.getItem(key); return raw ? JSON.parse(raw) : null } catch { return null } }
function writeSessionCache(key, value) { if (typeof window === 'undefined' || !window.sessionStorage) return; try { window.sessionStorage.setItem(key, JSON.stringify(value)) } catch {} }
function readPref(key, fallback) { if (typeof window === 'undefined' || !window.sessionStorage) return fallback; try { const raw = window.sessionStorage.getItem(key); return raw == null ? fallback : JSON.parse(raw) } catch { return fallback } }
function writePref(key, value) { if (typeof window === 'undefined' || !window.sessionStorage) return; try { window.sessionStorage.setItem(key, JSON.stringify(value)) } catch {} }
function getDashboardSessionToken() { if (typeof window === 'undefined' || !window.localStorage) return ''; return window.localStorage.getItem(DASHBOARD_SESSION_STORAGE_KEY) || '' }
function setDashboardSessionToken(token) { if (typeof window === 'undefined' || !window.localStorage) return; if (token) window.localStorage.setItem(DASHBOARD_SESSION_STORAGE_KEY, token); else window.localStorage.removeItem(DASHBOARD_SESSION_STORAGE_KEY) }
function pill(tone) { const map = { blue: ['#5ea2ff','rgba(94,162,255,0.12)'], green: ['#43c59e','rgba(67,197,158,0.12)'], amber: ['#f1b75c','rgba(241,183,92,0.12)'] }; const [color, bg] = map[tone] || map.blue; return { display:'inline-flex', padding:'6px 10px', borderRadius:999, color, background:bg, fontSize:12, fontWeight:700 } }

function cleanFilters(filters) {
  const source = filters || {}
  const cleaned = {}
  for (const [key, value] of Object.entries(source)) {
    if (key === 'metrics' || key === 'weights') continue
    if (value !== '' && value !== null && value !== undefined) cleaned[key] = value
  }
  const metrics = {}
  for (const [metric, rules] of Object.entries(source.metrics || {})) {
    const entry = {}
    if (rules?.min !== '' && rules?.min !== null && rules?.min !== undefined) entry.min = Number(rules.min)
    if (rules?.max !== '' && rules?.max !== null && rules?.max !== undefined) entry.max = Number(rules.max)
    if (Object.keys(entry).length) metrics[metric] = entry
  }
  if (Object.keys(metrics).length) cleaned.metrics = metrics
  const weights = {}
  for (const [metric, value] of Object.entries(source.weights || {})) {
    if (value === '' || value === null || value === undefined) continue
    const num = Number(value)
    if (Number.isFinite(num)) weights[metric] = num
  }
  if (Object.keys(weights).length) cleaned.weights = weights
  return cleaned
}

function mergeFilterState(savedFilters) { return { ...emptyFilters(), ...(savedFilters || {}), metrics: clone(savedFilters?.metrics || {}), weights: clone(savedFilters?.weights || {}) } }
function available(result, componentKey) {
  const defaults = {
    hitters: { pitch_types: [], suggested_metric_filters: ['EV', 'LA', 'Pitches Seen', 'xwOBA', 'HardHit', 'Usage'], suggested_weight_metrics: ['EV', 'LA', 'Pitches Seen', 'xwOBA', 'Usage'] },
    pitchers: { suggested_metric_filters: ['K%', 'xwOBA Allowed', 'HardHit Allowed', 'Opp K%', 'Score'], suggested_weight_metrics: ['K%', 'xwOBA Allowed', 'HardHit Allowed', 'Score'] },
    teams: { suggested_metric_filters: ['Edge Score', 'Win Edge', 'Run Diff', 'ISO', 'OBP'], suggested_weight_metrics: ['Edge Score', 'Win Edge', 'Run Diff', 'ISO'] },
    totals: { suggested_metric_filters: ['Projected Total', 'Raw Total', 'Run Index', 'Score'], suggested_weight_metrics: ['Projected Total', 'Run Index', 'Score'] },
    overall_players: { suggested_metric_filters: ['Score'], suggested_weight_metrics: ['Score'] },
  }
  return { ...(defaults[componentKey] || {}), ...(result?.available_filters || {}) }
}
function emptySaveDraft(folderId = '') { return { folder_id: folderId, title: '', subtitle: '', notes: '' } }
function defaultSaveDrafts(folderId = '') { return Object.fromEntries(COMPONENTS.map(component => [component.key, emptySaveDraft(folderId)])) }
function sourceComponentKey(item) { return item?.payload_json?.saved_from_component || item?.payload_json?.component_key || item?.payload_json?.board_state?.component || null }

export default function MyDashboardWorkspacePage() {
  const today = todayIso()
  const [authChecked, setAuthChecked] = useState(false)
  const [profile, setProfile] = useState(null)
  const [workspace, setWorkspace] = useState(null)
  const [results, setResults] = useState({})
  const [filters, setFilters] = useState(defaultFiltersByComponent)
  const [activeLineupsByComponent, setActiveLineupsByComponent] = useState(defaultLineupToggles)
  const [saveDrafts, setSaveDrafts] = useState(defaultSaveDrafts)
  const [newFolder, setNewFolder] = useState({ folder_name: '', folder_date: today })
  const [loading, setLoading] = useState({})
  const [runErrors, setRunErrors] = useState({})
  const [saveMessage, setSaveMessage] = useState(null)
  const [authError, setAuthError] = useState(null)
  const [savingProfile, setSavingProfile] = useState(false)
  const [creatingFolder, setCreatingFolder] = useState(false)
  const [surface, setSurface] = useState(() => readPref(SURFACE_PREF_KEY, 'boards'))
  const [savedShelfOpen, setSavedShelfOpen] = useState(() => readPref(SHELF_PREF_KEY, true))
  const [activeComponent, setActiveComponent] = useState('hitters')
  const [selectedFolderId, setSelectedFolderId] = useState('')
  const [selectedItemId, setSelectedItemId] = useState('')
  const [builderState, setBuilderState] = useState({ component: 'hitters', search: '', selectedFields: DEFAULT_BUILDER_FIELDS, sortBy: 'score' })
  const [builderDraft, setBuilderDraft] = useState(emptySaveDraft())
  const [form, setForm] = useState({ email: '', username: '', password: '', feature_interests: ['Matchups', 'Model Projections'], wants_newsletter: false, plan_type: 'free' })

  useEffect(() => { writePref(SURFACE_PREF_KEY, surface) }, [surface])
  useEffect(() => { writePref(SHELF_PREF_KEY, savedShelfOpen) }, [savedShelfOpen])

  useEffect(() => {
    async function bootstrap() {
      try {
        const res = await fetch(`${API}/my-dashboard/profile`, { credentials: 'include', headers: getDashboardSessionToken() ? { 'X-Dashboard-Session': getDashboardSessionToken() } : {} })
        const json = await res.json()
        if (json.authenticated) {
          setProfile(json.user)
          await loadWorkspace()
          await runAllBoards({ preferCache: true })
        }
      } finally { setAuthChecked(true) }
    }
    bootstrap()
  }, [])

  useEffect(() => {
    const folderId = workspace?.today_folder_id ? String(workspace.today_folder_id) : ''
    setSaveDrafts(prev => {
      const next = clone(prev)
      for (const component of COMPONENTS) {
        if (!next[component.key]) next[component.key] = emptySaveDraft(folderId)
        if (!next[component.key].folder_id) next[component.key].folder_id = folderId
        if (!next[component.key].title) next[component.key].title = `${component.title} | ${today}`
        if (!next[component.key].subtitle) next[component.key].subtitle = component.description
      }
      return next
    })
    setBuilderDraft(prev => ({ ...emptySaveDraft(folderId), ...prev, folder_id: prev.folder_id || folderId, title: prev.title || `Custom Dashboard | ${today}`, subtitle: prev.subtitle || 'Builder view saved from My Dashboard' }))
    if (!selectedFolderId && folderId) setSelectedFolderId(folderId)
  }, [workspace?.today_folder_id, today, selectedFolderId])

  async function handleUnauthorized(message = 'Dashboard sign-in required') {
    setDashboardSessionToken('')
    setProfile(null)
    setWorkspace(null)
    setAuthError(message)
    setSaveMessage(message)
    setAuthChecked(true)
  }

  async function apiJson(url, options = {}) {
    const token = getDashboardSessionToken()
    const headers = { ...(options.headers || {}) }
    if (token) headers['X-Dashboard-Session'] = token
    const res = await fetch(url, { credentials: 'include', ...options, headers })
    const json = await res.json().catch(() => ({}))
    if (json?.session_token) setDashboardSessionToken(json.session_token)
    if (res.status === 401) {
      await handleUnauthorized(typeof json?.detail === 'string' ? json.detail : 'Dashboard sign-in required')
      throw new Error(typeof json?.detail === 'string' ? json.detail : 'Dashboard sign-in required')
    }
    if (!res.ok) throw new Error(typeof json?.detail === 'string' ? json.detail : JSON.stringify(json.detail || json))
    return json
  }

  async function loadWorkspace() { const json = await apiJson(`${API}/my-dashboard/workspace`); setWorkspace(json); return json }
  async function handleProfileSubmit(event) {
    event.preventDefault()
    setSavingProfile(true)
    setAuthError(null)
    try {
      const signupJson = await apiJson(`${API}/my-dashboard/profile`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(form) })
      if (signupJson?.session_token) setDashboardSessionToken(signupJson.session_token)
      const profileJson = await apiJson(`${API}/my-dashboard/profile`)
      if (!profileJson.authenticated) throw new Error('Dashboard session was not established. Try signing in again.')
      setProfile(profileJson.user)
      await loadWorkspace()
      await runAllBoards({ preferCache: false })
    } catch (err) {
      setAuthError(err.message || 'Failed to create dashboard profile')
      setProfile(null)
    } finally { setSavingProfile(false); setAuthChecked(true) }
  }

  function setBasicFilter(componentKey, key, value) { setFilters(prev => ({ ...prev, [componentKey]: { ...prev[componentKey], [key]: value } })) }
  function setMetricFilter(componentKey, metric, side, value) { setFilters(prev => { const next = clone(prev); const entry = { ...(next[componentKey].metrics?.[metric] || {}) }; entry[side] = value; if ((entry.min || '') === '' && (entry.max || '') === '') delete next[componentKey].metrics[metric]; else next[componentKey].metrics[metric] = entry; return next }) }
  function setWeight(componentKey, metric, value) { setFilters(prev => ({ ...prev, [componentKey]: { ...prev[componentKey], weights: { ...(prev[componentKey].weights || {}), [metric]: value } } })) }
  function setSaveDraft(componentKey, key, value) { setSaveDrafts(prev => ({ ...prev, [componentKey]: { ...prev[componentKey], [key]: value } })) }
  function resetFilters(componentKey) { setFilters(prev => ({ ...prev, [componentKey]: emptyFilters() })) }
  function toggleActiveLineups(componentKey) { setActiveLineupsByComponent(prev => ({ ...prev, [componentKey]: !prev[componentKey] })) }

  async function runBoard(componentKey, options = {}) {
    const activeLineups = options.activeLineups ?? activeLineupsByComponent[componentKey]
    const payload = { date: today, component: componentKey, filters: cleanFilters(filters[componentKey] || {}) }
    const sessionKey = cacheKey(activeLineups ? 'board_active_lineups' : 'board', payload)
    const endpoint = activeLineups ? `${API}/my-dashboard/solver/active-lineups` : `${API}/my-dashboard/solver`
    if (options.preferCache) {
      const cached = readSessionCache(sessionKey)
      if (cached) { setResults(prev => ({ ...prev, [componentKey]: cached })); return }
    }
    setLoading(prev => ({ ...prev, [componentKey]: true }))
    setRunErrors(prev => ({ ...prev, [componentKey]: null }))
    try {
      const json = await apiJson(endpoint, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) })
      writeSessionCache(sessionKey, json)
      setResults(prev => ({ ...prev, [componentKey]: json }))
    } catch (err) { setRunErrors(prev => ({ ...prev, [componentKey]: err.message || 'Board run failed' })) }
    finally { setLoading(prev => ({ ...prev, [componentKey]: false })) }
  }

  async function runAllBoards(options = {}) {
    const keys = COMPONENTS.map(component => component.key)
    const activeLineups = options.activeLineups ?? false
    const payload = { date: today, components: keys, filters_by_component: Object.fromEntries(keys.map(key => [key, cleanFilters(filters[key] || {})])), active_lineups: activeLineups }
    const sessionKey = cacheKey(activeLineups ? 'batch_active_lineups' : 'batch', payload)
    if (options.preferCache) {
      const cached = readSessionCache(sessionKey)
      if (cached?.results) { setResults(prev => ({ ...prev, ...(cached.results || {}) })); return }
    }
    setLoading(Object.fromEntries(keys.map(key => [key, true])))
    setRunErrors({})
    try {
      const json = await apiJson(`${API}/my-dashboard/solver/batch`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) })
      writeSessionCache(sessionKey, json)
      setResults(prev => ({ ...prev, ...(json.results || {}) }))
    } catch (err) { setRunErrors(prev => ({ ...prev, _all: err.message || 'Populate all failed' })) }
    finally { setLoading(Object.fromEntries(keys.map(key => [key, false]))) }
  }

  async function saveItemToToday(component, item) {
    if (!workspace?.today_folder_id) return
    setSaveMessage(null)
    try {
      await apiJson(`${API}/my-dashboard/items`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ folder_id: workspace.today_folder_id, source_tab: 'my-dashboard', source_type: 'solver_result', title: `${component.title} | ${item.entity_name || 'Saved item'}`, subtitle: item.primary_reason || component.description, payload_json: { saved_from_component: component.key, saved_on_date: today, entity_name: item.entity_name, entity_id: item.entity_id, entity_type: item.entity_type, score: item.score, confidence: item.confidence, metrics: item.metrics || {}, reasoning: item.reasoning || [], max_filters: 10 }, filter_json: cleanFilters(filters[component.key] || {}), sort_json: { by: 'score', direction: 'desc' } }) })
      await loadWorkspace()
      setSaveMessage(`Saved ${item.entity_name || 'item'} to Today`)
    } catch (err) { setSaveMessage(err.message || 'Failed to save item') }
  }

  async function saveBoardState(component) {
    const componentKey = component.key
    const boardResult = results[componentKey]
    const draft = saveDrafts[componentKey] || emptySaveDraft(String(workspace?.today_folder_id || ''))
    const folderId = Number(draft.folder_id || workspace?.today_folder_id)
    if (!folderId) { setSaveMessage('Choose a folder before saving this dashboard state.'); return }
    if (!boardResult?.items?.length) { setSaveMessage('Run the board before saving a dashboard state.'); return }
    const title = (draft.title || `${component.title} | ${today}`).trim()
    try {
      await apiJson(`${API}/my-dashboard/items`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ folder_id: folderId, source_tab: 'my-dashboard', source_type: 'solver_board_state', title, subtitle: (draft.subtitle || component.description || '').trim() || null, notes: (draft.notes || '').trim() || null, payload_json: { saved_from_component: componentKey, saved_on_date: today, board_state: boardResult, saved_item_count: boardResult.items.length, active_lineups_only: !!activeLineupsByComponent[componentKey] }, filter_json: cleanFilters(filters[componentKey] || {}), sort_json: { by: 'score', direction: 'desc', component: componentKey, active_lineups: !!activeLineupsByComponent[componentKey] } }) })
      await loadWorkspace()
      setSaveMessage(`Saved dashboard state: ${title}`)
    } catch (err) { setSaveMessage(err.message || 'Failed to save dashboard state') }
  }

  async function saveBuilderView() {
    const componentKey = builderState.component
    const boardResult = results[componentKey]
    const folderId = Number(builderDraft.folder_id || workspace?.today_folder_id)
    if (!folderId) { setSaveMessage('Choose a folder before saving this builder view.'); return }
    if (!boardResult?.items?.length) { setSaveMessage('Run the source board before saving this builder view.'); return }
    const title = (builderDraft.title || `Custom Dashboard | ${today}`).trim()
    try {
      await apiJson(`${API}/my-dashboard/items`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ folder_id: folderId, source_tab: 'my-dashboard', source_type: 'solver_board_state', title, subtitle: builderDraft.subtitle || 'Saved custom dashboard builder view', notes: builderDraft.notes || null, payload_json: { saved_from_component: componentKey, saved_on_date: today, board_state: boardResult, builder_state: builderState, saved_item_count: boardResult.items.length, active_lineups_only: !!activeLineupsByComponent[componentKey] }, filter_json: cleanFilters(filters[componentKey] || {}), sort_json: { by: builderState.sortBy || 'score', direction: 'desc', component: componentKey, active_lineups: !!activeLineupsByComponent[componentKey] } }) })
      await loadWorkspace()
      setSaveMessage(`Saved builder view: ${title}`)
    } catch (err) { setSaveMessage(err.message || 'Failed to save builder view') }
  }

  async function createFolder() {
    if (!newFolder.folder_name.trim()) { setSaveMessage('Folder name is required.'); return }
    setCreatingFolder(true)
    try {
      const json = await apiJson(`${API}/my-dashboard/folders`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ folder_name: newFolder.folder_name.trim(), folder_date: newFolder.folder_date || null, is_default: false }) })
      const folderId = String(json.folder?.id || '')
      setNewFolder({ folder_name: '', folder_date: today })
      await loadWorkspace()
      if (folderId) {
        setSelectedFolderId(folderId)
        setSaveDrafts(prev => Object.fromEntries(Object.entries(prev).map(([key, value]) => [key, { ...value, folder_id: folderId }])))
        setBuilderDraft(prev => ({ ...prev, folder_id: folderId }))
      }
      setSaveMessage(`Created folder: ${json.folder?.folder_name || 'New folder'}`)
    } catch (err) { setSaveMessage(err.message || 'Failed to create folder') }
    finally { setCreatingFolder(false) }
  }

  async function restoreSavedState(item) {
    const componentKey = sourceComponentKey(item)
    if (!componentKey) { setSaveMessage('This saved item does not contain a restorable dashboard state.'); return }
    setActiveComponent(componentKey)
    setFilters(prev => ({ ...prev, [componentKey]: mergeFilterState(item.filter_json || {}) }))
    setActiveLineupsByComponent(prev => ({ ...prev, [componentKey]: !!(item.sort_json?.active_lineups || item.payload_json?.active_lineups_only) }))
    if (item.payload_json?.board_state) setResults(prev => ({ ...prev, [componentKey]: item.payload_json.board_state }))
    if (item.payload_json?.builder_state) {
      setBuilderState(item.payload_json.builder_state)
      setSurface('builder')
      setSaveMessage(`Loaded saved builder view: ${item.title}`)
      return
    }
    setSurface('boards')
    setSaveMessage(`Loaded saved dashboard state: ${item.title}`)
  }

  const folders = workspace?.folders || []
  const todayFolder = useMemo(() => folders.find(folder => folder.id === workspace?.today_folder_id), [folders, workspace?.today_folder_id])
  const activeComponentMeta = COMPONENTS.find(component => component.key === activeComponent) || COMPONENTS[0]
  const activeResult = results[activeComponent] || null
  const activeFilters = filters[activeComponent] || emptyFilters()
  const activeDraft = saveDrafts[activeComponent] || emptySaveDraft(String(workspace?.today_folder_id || ''))
  const activeHelper = available(activeResult, activeComponent)
  const selectedFolder = folders.find(folder => String(folder.id) === String(selectedFolderId)) || folders[0] || null
  const selectedItem = ensureArray(selectedFolder?.items).find(item => String(item.id) === String(selectedItemId)) || ensureArray(selectedFolder?.items)[0] || null
  const fieldGroups = useMemo(() => collectBuilderFieldGroups({ results, workspace }), [results, workspace])
  const builderItems = ensureArray(results?.[builderState.component]?.items)
  const builderPreview = useMemo(() => builderItems.slice(0, 12).map(item => ({ ...Object.fromEntries((builderState.selectedFields || []).map(field => [field, getValueByPath(item, field)])) })), [builderItems, builderState])

  if (!authChecked) return <div style={styles.loading}>Loading dashboard workspace…</div>

  return <div />
}
