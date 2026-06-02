
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
    hitters: {
      pitch_types: [],
      suggested_metric_filters: ['EV', 'LA', 'Pitches Seen', 'xwOBA', 'HardHit', 'Usage'],
      suggested_weight_metrics: ['EV', 'LA', 'Pitches Seen', 'xwOBA', 'Usage'],
    },
    pitchers: {
      suggested_metric_filters: ['K%', 'xwOBA Allowed', 'HardHit Allowed', 'Opp K%', 'Score'],
      suggested_weight_metrics: ['K%', 'xwOBA Allowed', 'HardHit Allowed', 'Score'],
    },
    teams: {
      suggested_metric_filters: ['Edge Score', 'Win Edge', 'Run Diff', 'ISO', 'OBP'],
      suggested_weight_metrics: ['Edge Score', 'Win Edge', 'Run Diff', 'ISO'],
    },
    totals: {
      suggested_metric_filters: ['Projected Total', 'Raw Total', 'Run Index', 'Score'],
      suggested_weightMetrics: ['Projected Total', 'Run Index', 'Score'],
      suggested_weight_metrics: ['Projected Total', 'Run Index', 'Score'],
    },
    overall_players: {
      suggested_metric_filters: ['Score'],
      suggested_weight_metrics: ['Score'],
    },
  }
  return { ...(defaults[componentKey] || {}), ...(result?.available_filters || {}) }
}

function emptySaveDraft(folderId = '') { return { folder_id: folderId, title: '', subtitle: '', notes: '' } }
function defaultSaveDrafts(folderId = '') { return Object.fromEntries(COMPONENTS.map(component => [component.key, emptySaveDraft(folderId)])) }
function sourceComponentKey(item) { return item?.payload_json?.saved_from_component || item?.payload_json?.component_key || item?.payload_json?.board_state?.component || null }
function metricEntries(item) { return Object.entries(item?.metrics || {}) }
function trimText(value) { return typeof value === 'string' ? value.trim() : value }

function compareValues(a, b, direction = 'desc') {
  const left = a == null ? '' : a
  const right = b == null ? '' : b
  if (typeof left === 'number' && typeof right === 'number') return direction === 'asc' ? left - right : right - left
  return direction === 'asc' ? String(left).localeCompare(String(right)) : String(right).localeCompare(String(left))
}

function StatusPill({ children, tone = 'default' }) {
  const toneMap = {
    default: { background: C.blueSoft, color: C.blue },
    success: { background: 'rgba(67, 197, 158, 0.14)', color: C.green },
    warning: { background: 'rgba(241, 183, 92, 0.14)', color: C.amber },
    danger: { background: 'rgba(248, 113, 113, 0.14)', color: C.red },
  }
  const style = toneMap[tone] || toneMap.default
  return <span style={{ ...styles.pill, background: style.background, color: style.color }}>{children}</span>
}

function SectionCard({ title, subtitle, action, children, dense = false }) {
  return (
    <section style={{ ...styles.sectionCard, padding: dense ? 16 : 18 }}>
      <div style={styles.sectionHeader}>
        <div>
          <div style={styles.sectionTitle}>{title}</div>
          {subtitle ? <div style={styles.sectionSubtitle}>{subtitle}</div> : null}
        </div>
        {action}
      </div>
      {children}
    </section>
  )
}

function WorkspaceNav({ activeSurface, setActiveSurface, activeComponent, setActiveComponent, results }) {
  return (
    <aside style={styles.navRail}>
      <div style={styles.brandBlock}>
        <div style={styles.brandEyebrow}>Workspace</div>
        <div style={styles.brandTitle}>My Dashboard</div>
        <div style={styles.brandSubtitle}>Cleaner board control, better saved-state UX, and a report-builder canvas.</div>
      </div>
      <div style={styles.navGroup}>
        {SURFACES.map(surface => (
          <button key={surface.key} type="button" onClick={() => setActiveSurface(surface.key)} style={activeSurface === surface.key ? styles.navButtonActive : styles.navButton}>
            <div style={styles.navButtonTitle}>{surface.label}</div>
            <div style={styles.navButtonSubtitle}>{surface.description}</div>
          </button>
        ))}
      </div>
      <div style={styles.navGroupLabel}>Board sources</div>
      <div style={styles.navGroup}>
        {COMPONENTS.map(component => {
          const count = ensureArray(results?.[component.key]?.items).length
          return (
            <button key={component.key} type="button" onClick={() => { setActiveSurface('boards'); setActiveComponent(component.key) }} style={activeComponent === component.key ? styles.subNavButtonActive : styles.subNavButton}>
              <span>{component.shortTitle}</span>
              <span style={styles.subNavCount}>{count}</span>
            </button>
          )
        })}
      </div>
    </aside>
  )
}

function CommandBar({ profile, folders, todayCount, setActiveSurface, loadWorkspace, runAllBoards, saveMessage }) {
  return (
    <section style={styles.commandBar}>
      <div>
        <div style={styles.eyebrow}>My Dashboard</div>
        <h1 style={styles.pageTitle}>Welcome back, {profile.username}</h1>
        <p style={styles.pageSubtitle}>Enterprise workspace shell, retractable saved boards, cleaner board management, and a customizable builder without touching the solver logic.</p>
      </div>
      <div style={styles.commandRight}>
        <div style={styles.accountMetaCard}>
          <div style={styles.metaLine}>Email: {profile.email}</div>
          <div style={styles.metaLine}>Plan: {profile.preferences?.plan_type || 'free'}</div>
          <div style={styles.metaLine}>Folders: {folders.length}</div>
          <div style={styles.metaLine}>Today saved: {todayCount}</div>
        </div>
        <div style={styles.commandButtons}>
          <button type="button" onClick={loadWorkspace} style={styles.secondaryButton}>Refresh workspace</button>
          <button type="button" onClick={() => runAllBoards()} style={styles.primaryButton}>Populate all</button>
          <button type="button" onClick={() => runAllBoards({ activeLineups: true })} style={styles.secondaryButton}>Confirmed lineups only</button>
          <button type="button" onClick={() => setActiveSurface('builder')} style={styles.ghostButton}>Create dashboard</button>
        </div>
        {saveMessage ? <div style={styles.successBanner}>{saveMessage}</div> : null}
      </div>
    </section>
  )
}

function SavedBoardsShelf({
  open,
  onToggle,
  folders,
  todayCount,
  newFolder,
  setNewFolder,
  createFolder,
  creatingFolder,
  selectedFolderId,
  setSelectedFolderId,
  selectedSavedItemId,
  setSelectedSavedItemId,
  restoreSavedState,
}) {
  const selectedFolder = folders.find(folder => String(folder.id) === String(selectedFolderId)) || folders[0] || null
  const selectedItem = ensureArray(selectedFolder?.items).find(item => String(item.id) === String(selectedSavedItemId)) || ensureArray(selectedFolder?.items)[0] || null

  useEffect(() => {
    if (!selectedFolder && folders[0]) setSelectedFolderId(String(folders[0].id))
  }, [folders, selectedFolder, setSelectedFolderId])

  useEffect(() => {
    if (!selectedItem && selectedFolder?.items?.[0]) setSelectedSavedItemId(String(selectedFolder.items[0].id))
  }, [selectedFolder, selectedItem, setSelectedSavedItemId])

  return (
    <SectionCard
      title="Saved Boards"
      subtitle="A retractable folder-first browser for saved board states and saved items."
      action={<button type="button" onClick={onToggle} style={styles.secondaryButton}>{open ? 'Collapse shelf' : 'Expand shelf'}</button>}
    >
      <div style={styles.shelfSummaryRow}>
        <StatusPill>{folders.length} folders</StatusPill>
        <StatusPill tone="success">{todayCount} saved today</StatusPill>
        <StatusPill tone="warning">{folders.reduce((sum, folder) => sum + (folder.item_count || 0), 0)} total items</StatusPill>
      </div>
      {!open ? null : (
        <div style={styles.shelfContentGrid}>
          <div style={styles.folderColumn}>
            <div style={styles.inlineRow}>
              <input style={styles.input} placeholder="New folder title" value={newFolder.folder_name} onChange={e => setNewFolder(prev => ({ ...prev, folder_name: e.target.value }))} />
              <input style={styles.input} type="date" value={newFolder.folder_date} onChange={e => setNewFolder(prev => ({ ...prev, folder_date: e.target.value }))} />
              <button type="button" onClick={createFolder} style={styles.primaryButton} disabled={creatingFolder}>{creatingFolder ? 'Creating…' : 'Create folder'}</button>
            </div>
            <div style={styles.folderList}>
              {folders.length === 0 ? <div style={styles.emptyState}>No folders yet.</div> : folders.map(folder => {
                const active = String(folder.id) === String(selectedFolderId)
                return (
                  <button key={folder.id} type="button" onClick={() => { setSelectedFolderId(String(folder.id)); setSelectedSavedItemId('') }} style={active ? styles.folderButtonActive : styles.folderButton}>
                    <div>
                      <div style={styles.folderTitle}>📁 {folder.folder_name}</div>
                      <div style={styles.folderMeta}>{folder.folder_date || (folder.is_default ? 'Default dashboard' : 'No date')}</div>
                    </div>
                    <div style={styles.countBadge}>{folder.item_count || 0}</div>
                  </button>
                )
              })}
            </div>
          </div>
          <div style={styles.folderDetailColumn}>
            {!selectedFolder ? (
              <div style={styles.emptyState}>Select a folder to browse saved dashboards.</div>
            ) : (
              <>
                <div style={styles.folderHeader}>
                  <div>
                    <div style={styles.sectionTitle}>{selectedFolder.folder_name}</div>
                    <div style={styles.sectionSubtitle}>{selectedFolder.folder_date || 'No folder date'} • {selectedFolder.item_count || 0} saved items</div>
                  </div>
                </div>
                <div style={styles.folderInspectorGrid}>
                  <div style={styles.savedList}>
                    {ensureArray(selectedFolder.items).length === 0 ? <div style={styles.emptyState}>No saved items in this folder.</div> : ensureArray(selectedFolder.items).map(item => {
                      const active = String(item.id) === String(selectedItem?.id)
                      return (
                        <button key={item.id} type="button" onClick={() => setSelectedSavedItemId(String(item.id))} style={active ? styles.savedItemButtonActive : styles.savedItemButton}>
                          <div style={styles.savedItemTitle}>{item.title}</div>
                          <div style={styles.savedItemMeta}>{item.subtitle || item.source_type}</div>
                          <div style={styles.savedItemMeta}>Source: {item.source_tab} • {item.source_type}</div>
                        </button>
                      )
                    })}
                  </div>
                  <div style={styles.savedInspector}>
                    {!selectedItem ? <div style={styles.emptyState}>Choose a saved item to inspect its details.</div> : (
                      <>
                        <div style={styles.savedInspectorTitle}>{selectedItem.title}</div>
                        <div style={styles.savedInspectorSubtitle}>{selectedItem.subtitle || selectedItem.source_type}</div>
                        {selectedItem.notes ? <div style={styles.notesBox}>{selectedItem.notes}</div> : null}
                        <div style={styles.metaWrap}>
                          <StatusPill>{selectedItem.source_type}</StatusPill>
                          <StatusPill tone="success">{sourceComponentKey(selectedItem) || 'saved'}</StatusPill>
                        </div>
                        <div style={styles.metaWrap}>
                          {selectedItem.filter_json ? Object.keys(selectedItem.filter_json).slice(0, 6).map(key => <span key={`${selectedItem.id}-${key}`} style={styles.metricPill}>{key}</span>) : null}
                        </div>
                        <button type="button" onClick={() => restoreSavedState(selectedItem)} style={styles.primaryButton}>Load saved state</button>
                      </>
                    )}
                  </div>
                </div>
              </>
            )}
          </div>
        </div>
      )}
    </SectionCard>
  )
}

function BoardControls({
  component,
  result,
  filterState,
  helper,
  draft,
  folders,
  activeLineups,
  setBasicFilter,
  setMetricFilter,
  setWeight,
  setSaveDraft,
  resetFilters,
  toggleActiveLineups,
  saveBoardState,
  runBoard,
  loading,
  runError,
}) {
  const metricFilters = ensureArray(helper.suggested_metric_filters).slice(0, 8)
  const weightMetrics = ensureArray(helper.suggested_weight_metrics).slice(0, 6)

  return (
    <div style={styles.boardControlColumn}>
      <SectionCard title="Board controls" subtitle={component.description}>
        <div style={styles.metaWrap}>
          <StatusPill>{ensureArray(result?.items).length} results</StatusPill>
          <StatusPill tone="success">Before {result?.result_count_before_filters ?? '—'}</StatusPill>
          <StatusPill tone="warning">After {result?.result_count_after_filters ?? '—'}</StatusPill>
        </div>
        {result?.filter_warnings?.length ? <div style={styles.warningBanner}>{result.filter_warnings.join(' • ')}</div> : null}
        <label style={styles.checkboxRow}>
          <input type="checkbox" checked={!!activeLineups} onChange={() => toggleActiveLineups(component.key)} />
          Confirmed lineup players only on rerun
        </label>
        <div style={styles.buttonRow}>
          <button type="button" onClick={() => runBoard(component.key)} style={styles.primaryButton} disabled={loading}>{loading ? 'Running…' : 'Run board'}</button>
          <button type="button" onClick={() => runBoard(component.key, { preferCache: true })} style={styles.secondaryButton}>Use cached</button>
          <button type="button" onClick={() => resetFilters(component.key)} style={styles.ghostButton}>Reset filters</button>
        </div>
        {runError ? <div style={styles.errorBanner}>{runError}</div> : null}
      </SectionCard>

      <SectionCard title="Default filters" subtitle="Keep the existing filter model, but present it more cleanly.">
        <div style={styles.filterGrid}>
          <input style={styles.smallInput} placeholder="Search" value={filterState.search_text || ''} onChange={e => setBasicFilter(component.key, 'search_text', e.target.value)} />
          <input style={styles.smallInput} placeholder="Team" value={filterState.team || ''} onChange={e => setBasicFilter(component.key, 'team', e.target.value)} />
          <input style={styles.smallInput} placeholder="Opponent" value={filterState.opponent || ''} onChange={e => setBasicFilter(component.key, 'opponent', e.target.value)} />
          <input style={styles.smallInput} placeholder="Min score" value={filterState.min_score || ''} onChange={e => setBasicFilter(component.key, 'min_score', e.target.value)} />
          <input style={styles.smallInput} placeholder="Max score" value={filterState.max_score || ''} onChange={e => setBasicFilter(component.key, 'max_score', e.target.value)} />
          <select style={styles.smallInput} value={filterState.min_confidence || ''} onChange={e => setBasicFilter(component.key, 'min_confidence', e.target.value)}>
            <option value="">Any confidence</option>
            <option value="low">Low+</option>
            <option value="medium">Medium+</option>
            <option value="high">High only</option>
          </select>
          {component.key === 'hitters' ? (
            <select style={styles.smallInput} value={filterState.pitch_type || ''} onChange={e => setBasicFilter(component.key, 'pitch_type', e.target.value)}>
              <option value="">Any pitch type</option>
              {ensureArray(helper.pitch_types).map(value => <option key={value} value={value}>{value}</option>)}
            </select>
          ) : null}
          {ensureArray(helper.categories).length ? (
            <select style={styles.smallInput} value={filterState.category || ''} onChange={e => setBasicFilter(component.key, 'category', e.target.value)}>
              <option value="">Any category</option>
              {ensureArray(helper.categories).map(value => <option key={value} value={value}>{value}</option>)}
            </select>
          ) : null}
        </div>
      </SectionCard>

      <SectionCard title="Metric thresholds" subtitle="Min/max thresholds still map to the same payload shape.">
        <div style={styles.metricGrid}>
          {metricFilters.map(metric => (
            <div key={`${component.key}-${metric}`} style={styles.metricCard}>
              <div style={styles.metricTitle}>{metric}</div>
              <div style={styles.metricInputRow}>
                <input style={styles.smallInput} placeholder="Min" value={filterState.metrics?.[metric]?.min || ''} onChange={e => setMetricFilter(component.key, metric, 'min', e.target.value)} />
                <input style={styles.smallInput} placeholder="Max" value={filterState.metrics?.[metric]?.max || ''} onChange={e => setMetricFilter(component.key, metric, 'max', e.target.value)} />
              </div>
            </div>
          ))}
        </div>
      </SectionCard>

      <SectionCard title="Weighting" subtitle="Existing weight sliders, cleaner presentation.">
        <div style={styles.sliderGrid}>
          {weightMetrics.map(metric => {
            const value = Number(filterState.weights?.[metric] ?? 1)
            return (
              <div key={`${component.key}-weight-${metric}`} style={styles.metricCard}>
                <div style={styles.metricTitle}>{metric}</div>
                <input type="range" min="0" max="2" step="0.1" value={value} onChange={e => setWeight(component.key, metric, e.target.value)} style={{ width: '100%' }} />
                <div style={styles.sectionSubtitle}>Weight {value.toFixed(1)}</div>
              </div>
            )
          })}
        </div>
      </SectionCard>

      <SectionCard title="Save current board" subtitle="Folder-first save flow with title, subtitle, and notes.">
        <div style={styles.filterGrid}>
          <select style={styles.smallInput} value={draft.folder_id || ''} onChange={e => setSaveDraft(component.key, 'folder_id', e.target.value)}>
            <option value="">Choose folder</option>
            {folders.map(folder => <option key={folder.id} value={String(folder.id)}>{folder.folder_name}</option>)}
          </select>
          <input style={styles.smallInput} placeholder="Dashboard title" value={draft.title || ''} onChange={e => setSaveDraft(component.key, 'title', e.target.value)} />
          <input style={styles.smallInput} placeholder="Subtitle" value={draft.subtitle || ''} onChange={e => setSaveDraft(component.key, 'subtitle', e.target.value)} />
        </div>
        <textarea style={styles.textArea} placeholder="Notes" value={draft.notes || ''} onChange={e => setSaveDraft(component.key, 'notes', e.target.value)} />
        <button type="button" onClick={() => saveBoardState(component)} style={styles.primaryButton}>Save board state</button>
      </SectionCard>
    </div>
  )
}

function ResultList({ component, result, saveItemToToday }) {
  const items = ensureArray(result?.items)

  return (
    <div style={styles.boardResultsColumn}>
      <SectionCard title={component.title} subtitle="Cleaner result cards with the same saved-item workflow.">
        {items.length === 0 ? <div style={styles.emptyState}>No results yet. Run this board to populate fresh discovery for today.</div> : (
          <div style={styles.resultsGrid}>
            {items.map((item, index) => (
              <article key={`${component.key}-${index}-${item.entity_id || item.entity_name || index}`} style={styles.resultCard}>
                <div style={styles.resultTopRow}>
                  <div>
                    <div style={styles.resultTitle}>{item.entity_name || 'Unnamed result'}</div>
                    <div style={styles.resultSubTitle}>{item.primary_reason || component.description}</div>
                  </div>
                  <div style={styles.resultScoreStack}>
                    <div style={styles.scoreLabel}>Score</div>
                    <div style={styles.scoreValue}>{formatNumber(item.score)}</div>
                    <div style={styles.scoreConfidence}>{item.confidence || '—'}</div>
                  </div>
                </div>
                <div style={styles.metaWrap}>
                  {item.entity_type ? <StatusPill>{item.entity_type}</StatusPill> : null}
                  {item.team ? <StatusPill tone="success">{item.team}</StatusPill> : null}
                  {item.opponent ? <StatusPill tone="warning">vs {item.opponent}</StatusPill> : null}
                </div>
                <div style={styles.metricWrap}>
                  {metricEntries(item).slice(0, 8).map(([key, value]) => (
                    <div key={`${item.entity_id || item.entity_name}-${key}`} style={styles.metricChip}>
                      <span style={styles.metricChipLabel}>{key}</span>
                      <span style={styles.metricChipValue}>{formatNumber(value)}</span>
                    </div>
                  ))}
                </div>
                {ensureArray(item.reasoning).length ? (
                  <ul style={styles.reasonList}>
                    {ensureArray(item.reasoning).slice(0, 4).map((reason, idx) => <li key={`${item.entity_id || item.entity_name}-reason-${idx}`}>{reason}</li>)}
                  </ul>
                ) : null}
                <div style={styles.buttonRow}>
                  <button type="button" onClick={() => saveItemToToday(component, item)} style={styles.secondaryButton}>Save item to Today</button>
                </div>
              </article>
            ))}
          </div>
        )}
      </SectionCard>
    </div>
  )
}

function BuilderWorkspace({
  builderState,
  setBuilderState,
  builderDraft,
  setBuilderDraft,
  fieldGroups,
  builderItems,
  builderFilters,
  folders,
  saveBuilderView,
  runBoard,
  loading,
}) {
  const selectedFieldSet = new Set(builderState.selectedFields || [])
  const filteredGroups = fieldGroups
    .map(group => ({
      ...group,
      fields: group.fields.filter(field => {
        const query = trimText(builderState.search || '').toLowerCase()
        if (!query) return true
        return field.label.toLowerCase().includes(query) || field.accessor.toLowerCase().includes(query)
      }),
    }))
    .filter(group => group.fields.length > 0)

  const previewRows = useMemo(() => {
    const rows = ensureArray(builderItems).map(item => {
      const row = {}
      ;(builderState.selectedFields || []).forEach(accessor => {
        row[accessor] = getValueByPath(item, accessor)
      })
      return { __raw: item, ...row }
    })

    if (builderState.sortBy) {
      rows.sort((left, right) => compareValues(left[builderState.sortBy], right[builderState.sortBy], builderState.sortDir))
    }

    return rows.slice(0, 12)
  }, [builderItems, builderState.selectedFields, builderState.sortBy, builderState.sortDir])

  function addField(accessor) {
    if (selectedFieldSet.has(accessor)) return
    setBuilderState(prev => ({ ...prev, selectedFields: [...prev.selectedFields, accessor] }))
  }

  function removeField(accessor) {
    setBuilderState(prev => ({ ...prev, selectedFields: prev.selectedFields.filter(field => field !== accessor) }))
  }

  function moveField(accessor, direction) {
    setBuilderState(prev => {
      const current = [...prev.selectedFields]
      const index = current.indexOf(accessor)
      if (index < 0) return prev
      const nextIndex = direction === 'up' ? index - 1 : index + 1
      if (nextIndex < 0 || nextIndex >= current.length) return prev
      const swap = current[nextIndex]
      current[nextIndex] = accessor
      current[index] = swap
      return { ...prev, selectedFields: current }
    })
  }

  return (
    <div style={styles.builderGrid}>
      <div style={styles.builderColumn}>
        <SectionCard title="Builder source" subtitle="Choose the live board result set you want to shape into a custom dashboard.">
          <div style={styles.filterGrid}>
            <select style={styles.smallInput} value={builderState.component} onChange={e => setBuilderState(prev => ({ ...prev, component: e.target.value }))}>
              {COMPONENTS.map(component => <option key={component.key} value={component.key}>{component.title}</option>)}
            </select>
            <input style={styles.smallInput} placeholder="Search fields" value={builderState.search || ''} onChange={e => setBuilderState(prev => ({ ...prev, search: e.target.value }))} />
            <button type="button" onClick={() => runBoard(builderState.component)} style={styles.secondaryButton} disabled={loading}>{loading ? 'Refreshing…' : 'Refresh source board'}</button>
          </div>
          <div style={styles.metaWrap}>
            <StatusPill>{ensureArray(builderItems).length} source rows</StatusPill>
            <StatusPill tone="success">{(builderState.selectedFields || []).length} selected fields</StatusPill>
          </div>
        </SectionCard>

        <SectionCard title="Field library" subtitle="Built from the live board payloads, saved states, metrics, and identity fields already available in the app.">
          <div style={styles.builderFieldGroups}>
            {filteredGroups.map(group => (
              <div key={group.groupKey} style={styles.fieldGroup}>
                <div style={styles.fieldGroupTitle}>{group.title}</div>
                <div style={styles.fieldList}>
                  {group.fields.map(field => (
                    <div key={field.accessor} style={styles.fieldRow}>
                      <div>
                        <div style={styles.fieldLabel}>{field.label}</div>
                        <div style={styles.fieldAccessor}>{field.accessor}</div>
                      </div>
                      <button type="button" onClick={() => addField(field.accessor)} style={selectedFieldSet.has(field.accessor) ? styles.fieldAddButtonDisabled : styles.fieldAddButton} disabled={selectedFieldSet.has(field.accessor)}>
                        {selectedFieldSet.has(field.accessor) ? 'Added' : 'Add'}
                      </button>
                    </div>
                  ))}
                </div>
              </div>
            ))}
          </div>
        </SectionCard>
      </div>

      <div style={styles.builderColumn}>
        <SectionCard title="Selected fields" subtitle="Reorder the canvas fields and shape the preview without changing solver logic.">
          {(builderState.selectedFields || []).length === 0 ? <div style={styles.emptyState}>No fields selected yet.</div> : (
            <div style={styles.selectedFieldList}>
              {builderState.selectedFields.map((accessor, index) => (
                <div key={accessor} style={styles.selectedFieldRow}>
                  <div>
                    <div style={styles.fieldLabel}>{accessor}</div>
                    <div style={styles.fieldAccessor}>Column {index + 1}</div>
                  </div>
                  <div style={styles.selectedFieldActions}>
                    <button type="button" onClick={() => moveField(accessor, 'up')} style={styles.iconButton}>↑</button>
                    <button type="button" onClick={() => moveField(accessor, 'down')} style={styles.iconButton}>↓</button>
                    <button type="button" onClick={() => removeField(accessor)} style={styles.iconButtonDanger}>✕</button>
                  </div>
                </div>
              ))}
            </div>
          )}
          <div style={styles.filterGrid}>
            <select style={styles.smallInput} value={builderState.sortBy || ''} onChange={e => setBuilderState(prev => ({ ...prev, sortBy: e.target.value }))}>
              <option value="">No sort</option>
              {(builderState.selectedFields || []).map(accessor => <option key={`sort-${accessor}`} value={accessor}>{accessor}</option>)}
            </select>
            <select style={styles.smallInput} value={builderState.sortDir || 'desc'} onChange={e => setBuilderState(prev => ({ ...prev, sortDir: e.target.value }))}>
              <option value="desc">Descending</option>
              <option value="asc">Ascending</option>
            </select>
          </div>
        </SectionCard>

        <SectionCard title="Builder save options" subtitle="Save the current board state plus the builder layout so it can be restored later.">
          <div style={styles.filterGrid}>
            <select style={styles.smallInput} value={builderDraft.folder_id || ''} onChange={e => setBuilderDraft(prev => ({ ...prev, folder_id: e.target.value }))}>
              <option value="">Choose folder</option>
              {folders.map(folder => <option key={folder.id} value={String(folder.id)}>{folder.folder_name}</option>)}
            </select>
            <input style={styles.smallInput} placeholder="Builder title" value={builderDraft.title || ''} onChange={e => setBuilderDraft(prev => ({ ...prev, title: e.target.value }))} />
            <input style={styles.smallInput} placeholder="Subtitle" value={builderDraft.subtitle || ''} onChange={e => setBuilderDraft(prev => ({ ...prev, subtitle: e.target.value }))} />
          </div>
          <textarea style={styles.textArea} placeholder="Notes" value={builderDraft.notes || ''} onChange={e => setBuilderDraft(prev => ({ ...prev, notes: e.target.value }))} />
          <button type="button" onClick={saveBuilderView} style={styles.primaryButton}>Save builder view</button>
        </SectionCard>

        <SectionCard title="Current source filters" subtitle="Builder works on top of the same filter state already powering the source board.">
          <div style={styles.metaWrap}>
            {Object.entries(cleanFilters(builderFilters || {})).length === 0 ? <div style={styles.emptyState}>No active source filters.</div> : Object.entries(cleanFilters(builderFilters || {})).map(([key, value]) => <span key={`filter-${key}`} style={styles.metricPill}>{key}: {typeof value === 'object' ? 'configured' : String(value)}</span>)}
          </div>
        </SectionCard>
      </div>

      <div style={styles.builderPreviewColumn}>
        <SectionCard title="Canvas preview" subtitle="A live table preview of the selected fields from the current source board.">
          {previewRows.length === 0 ? <div style={styles.emptyState}>Run the selected source board and add fields to preview a custom dashboard.</div> : (
            <div style={styles.tableWrap}>
              <table style={styles.table}>
                <thead>
                  <tr>
                    {(builderState.selectedFields || []).map(accessor => <th key={`head-${accessor}`} style={styles.th}>{accessor}</th>)}
                  </tr>
                </thead>
                <tbody>
                  {previewRows.map((row, index) => (
                    <tr key={`preview-${index}`}>
                      {(builderState.selectedFields || []).map(accessor => <td key={`cell-${index}-${accessor}`} style={styles.td}>{String(row[accessor] ?? '—')}</td>)}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </SectionCard>
      </div>
    </div>
  )
}

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

  async function saveBuilderView() {
    const componentKey = builderState.component
    const boardResult = results[componentKey]
    const folderId = Number(builderDraft.folder_id || workspace?.today_folder_id)
    if (!folderId) { setSaveMessage('Choose a folder before saving this builder view.'); return }
    if (!boardResult?.items?.length) { setSaveMessage('Run the source board before saving this builder view.'); return }
    const title = (builderDraft.title || `Custom Dashboard | ${today}`).trim()
    try {
      await apiJson(`${API}/my-dashboard/items`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          folder_id: folderId,
          source_tab: 'my-dashboard',
          source_type: 'solver_board_state',
          title,
          subtitle: trimText(builderDraft.subtitle) || 'Saved custom dashboard builder view',
          notes: trimText(builderDraft.notes) || null,
          payload_json: {
            saved_from_component: componentKey,
            saved_on_date: today,
            board_state: boardResult,
            builder_state: builderState,
            saved_item_count: boardResult.items.length,
            active_lineups_only: !!activeLineupsByComponent[componentKey],
          },
          filter_json: cleanFilters(filters[componentKey] || {}),
          sort_json: { by: builderState.sortBy || 'score', direction: builderState.sortDir || 'desc', component: componentKey, active_lineups: !!activeLineupsByComponent[componentKey] },
        }),
      })
      await loadWorkspace()
      setSaveMessage(`Saved builder view: ${title}`)
    } catch (err) {
      setSaveMessage(err.message || 'Failed to save builder view')
    }
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
