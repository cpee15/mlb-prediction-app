import React, { useEffect, useMemo, useState } from 'react'
import { buildReportCsv, mlbDateIso, safeFilenamePart } from '../lib/dashboardReportUtils.mjs'
import { PAGE_SIZE_OPTIONS, defaultQueryState, normalizeQueryState, queryPayload, resultRange, serverFields } from '../lib/dashboardQueryState.mjs'

const API = import.meta.env.VITE_API_BASE_URL || ''
const SESSION_KEY = 'mlbgpt_dashboard_session_token'
const BUILDER_KEY = 'mlbgpt-report-builder:v4'
const OBJECTS = [
  { key: 'hitters', label: 'Hitters', description: 'Every available hitter matchup, optionally limited to confirmed 1–9 lineups.' },
  { key: 'pitchers', label: 'Pitchers', description: 'Every available projected starter and pitcher lean.' },
  { key: 'teams', label: 'Teams', description: 'Both teams from every available projected game.' },
  { key: 'totals', label: 'Totals', description: 'Every available projected game total.' },
  { key: 'overall_players', label: 'Overall Players', description: 'Combined hitter and pitcher report universe.' },
]
const ACTIVE_LINEUP_OBJECTS = new Set(['hitters', 'overall_players'])
const DEFAULT_FIELDS = ['rank', 'entity_name', 'team', 'opponent', 'score', 'confidence']
const FALLBACK_FIELDS = DEFAULT_FIELDS.map(accessor => ({ accessor, label: accessor.replaceAll('_', ' ').replace(/\b\w/g, c => c.toUpperCase()), group: 'Identity', sortable: true }))
const C = { bg: '#07101d', panel: '#111827', panel2: '#0b1322', panel3: '#182235', border: 'rgba(148,163,184,.22)', text: '#e8eef8', muted: '#94a3b8', blue: '#60a5fa', green: '#34d399', amber: '#fbbf24', red: '#f87171' }

function safeArray(value) { return Array.isArray(value) ? value : [] }
function readJson(key, fallback) { try { return JSON.parse(localStorage.getItem(key)) || fallback } catch { return fallback } }
function writeJson(key, value) { try { localStorage.setItem(key, JSON.stringify(value)) } catch {} }
function readToken() { try { return localStorage.getItem(SESSION_KEY) || '' } catch { return '' } }
function writeToken(value) { try { value ? localStorage.setItem(SESSION_KEY, value) : localStorage.removeItem(SESSION_KEY) } catch {} }
function emptyFilters() { return { search_text: '', team: '', opponent: '', min_score: '', max_score: '', min_confidence: '', category: '', pitch_type: '', metrics: {}, weights: {} } }
function cleanFilters(filters) {
  const out = {}
  Object.entries(filters || {}).forEach(([key, value]) => { if (!['metrics', 'weights'].includes(key) && value !== '' && value != null) out[key] = value })
  const metrics = {}
  Object.entries(filters?.metrics || {}).forEach(([metric, rule]) => {
    const next = {}
    if (rule?.min !== '' && rule?.min != null) next.min = Number(rule.min)
    if (rule?.max !== '' && rule?.max != null) next.max = Number(rule.max)
    if (Object.keys(next).length) metrics[metric] = next
  })
  if (Object.keys(metrics).length) out.metrics = metrics
  const weights = {}
  Object.entries(filters?.weights || {}).forEach(([metric, value]) => { const number = Number(value); if (Number.isFinite(number) && number !== 1) weights[metric] = number })
  if (Object.keys(weights).length) out.weights = weights
  return out
}
function getValue(row, accessor) { return accessor.startsWith('metrics.') ? row?.metrics?.[accessor.slice(8)] : accessor.split('.').reduce((value, key) => value == null ? null : value[key], row) }
function formatCell(value) { if (value == null || value === '') return '—'; if (typeof value === 'number') return Math.abs(value) >= 10 ? value.toFixed(1) : value.toFixed(3); if (typeof value === 'boolean') return value ? 'Yes' : 'No'; if (Array.isArray(value)) return value.slice(0, 4).join(', '); if (typeof value === 'object') return JSON.stringify(value); return String(value) }
function downloadTextFile(filename, contents, mimeType) { const blob = new Blob([contents], { type: mimeType }); const url = URL.createObjectURL(blob); const link = document.createElement('a'); link.href = url; link.download = filename; document.body.appendChild(link); link.click(); link.remove(); URL.revokeObjectURL(url) }
function Pill({ children, tone = 'blue' }) { const color = tone === 'green' ? C.green : tone === 'amber' ? C.amber : tone === 'red' ? C.red : C.blue; return <span style={{ ...s.pill, color, borderColor: `${color}55`, background: `${color}16` }}>{children}</span> }
function StatePanel({ tone = 'empty', title, children, action }) { const style = tone === 'error' ? s.errorState : tone === 'loading' ? s.loadingState : s.empty; return <div style={style}><strong>{title}</strong><div>{children}</div>{action}</div> }

function FieldLibrary({ fields, selected, setSelected }) {
  const selectedSet = new Set(selected)
  const grouped = fields.reduce((map, field) => ({ ...map, [field.group || 'Other']: [...(map[field.group || 'Other'] || []), field] }), {})
  function toggle(accessor) { const next = selectedSet.has(accessor) ? selected.filter(value => value !== accessor) : [...selected, accessor]; setSelected(next.length ? next : DEFAULT_FIELDS) }
  return <section style={s.card}><div style={s.cardHeader}><div><h3 style={s.panelTitle}>Field Library</h3><p style={s.copySmall}>Fields are supplied by the server report-object metadata.</p></div><Pill>{selected.length} selected</Pill></div>{Object.entries(grouped).map(([group, groupFields]) => <div key={group}><div style={s.sectionLabel}>{group}</div><div style={s.fieldGrid}>{groupFields.map(field => <button key={field.accessor} style={selectedSet.has(field.accessor) ? s.fieldActive : s.fieldButton} onClick={() => toggle(field.accessor)}><strong>{field.label}</strong><small>{field.accessor}</small></button>)}</div></div>)}</section>
}

function FilterPanel({ objectKey, filters, fields, setBasic, setMetric }) {
  const metrics = fields.filter(field => field.accessor.startsWith('metrics.') && field.filterable !== false).slice(0, 10)
  return <section style={s.card}><h3 style={s.panelTitle}>Filters</h3><p style={s.copySmall}>Filters run against the full result universe before pagination.</p><div style={s.filterGrid}><input style={s.input} placeholder="Search text" value={filters.search_text || ''} onChange={e => setBasic(objectKey, 'search_text', e.target.value)} /><input style={s.input} placeholder="Team contains" value={filters.team || ''} onChange={e => setBasic(objectKey, 'team', e.target.value)} /><input style={s.input} placeholder="Opponent contains" value={filters.opponent || ''} onChange={e => setBasic(objectKey, 'opponent', e.target.value)} /><select style={s.input} value={filters.min_confidence || ''} onChange={e => setBasic(objectKey, 'min_confidence', e.target.value)}><option value="">Any confidence</option><option value="low">Low+</option><option value="medium">Medium+</option><option value="high">High only</option></select><input style={s.input} placeholder="Minimum score" value={filters.min_score || ''} onChange={e => setBasic(objectKey, 'min_score', e.target.value)} /><input style={s.input} placeholder="Maximum score" value={filters.max_score || ''} onChange={e => setBasic(objectKey, 'max_score', e.target.value)} /><input style={s.input} placeholder="Category" value={filters.category || ''} onChange={e => setBasic(objectKey, 'category', e.target.value)} /><input style={s.input} placeholder="Pitch type" value={filters.pitch_type || ''} onChange={e => setBasic(objectKey, 'pitch_type', e.target.value)} /></div>{metrics.length ? <><div style={s.sectionLabel}>Metric thresholds</div><div style={s.metricGrid}>{metrics.map(field => { const metric = field.accessor.slice(8); return <div style={s.metricCard} key={field.accessor}><strong>{field.label}</strong><div style={s.twoCol}><input style={s.miniInput} placeholder="Min" value={filters.metrics?.[metric]?.min || ''} onChange={e => setMetric(objectKey, metric, 'min', e.target.value)} /><input style={s.miniInput} placeholder="Max" value={filters.metrics?.[metric]?.max || ''} onChange={e => setMetric(objectKey, metric, 'max', e.target.value)} /></div></div> })}</div></> : null}</section>
}

function Pagination({ result, query, loading, onPage, onPageSize }) {
  const page = result?.page_info || {}
  const range = resultRange(page, result?.totalSize ?? result?.total_count)
  return <div style={s.pagination}><span>{range.total ? `Showing ${range.start}–${range.end} of ${range.total}` : 'No results'}</span><label style={s.pageSize}>Rows<select style={s.selectSmall} value={query.page_size} onChange={e => onPageSize(Number(e.target.value))}>{PAGE_SIZE_OPTIONS.map(size => <option key={size} value={size}>{size}</option>)}</select></label><button style={s.secondary} disabled={loading || !page.has_previous} onClick={() => onPage(page.previous_page)}>Previous</button><span>Page {page.page_number || 1} of {page.page_count || 1}</span><button style={s.secondary} disabled={loading || !page.has_next} onClick={() => onPage(page.next_page)}>Next</button></div>
}

function ReportWorkspace({ open, close, objectMeta, result, fields, columns, setColumns, query, loading, onQuery, onSave, onExportAll }) {
  const [hidden, setHidden] = useState([])
  const [exporting, setExporting] = useState(false)
  useEffect(() => setHidden([]), [result, columns])
  if (!open) return null
  const rows = safeArray(result?.records?.length ? result.records : result?.items)
  const visible = columns.filter(accessor => !hidden.includes(accessor))
  const fieldMap = Object.fromEntries(fields.map(field => [field.accessor, field]))
  const range = resultRange(result?.page_info, result?.totalSize ?? result?.total_count)
  function move(accessor, delta) { setColumns(current => { const from = current.indexOf(accessor), to = from + delta; if (from < 0 || to < 0 || to >= current.length) return current; const next = [...current], [item] = next.splice(from, 1); next.splice(to, 0, item); return next }) }
  function sortBy(field) { if (field?.sortable === false) return; onQuery({ ...query, page_number: 1, sort_by: field.accessor, sort_direction: query.sort_by === field.accessor && query.sort_direction === 'desc' ? 'asc' : 'desc' }) }
  function exportPage() { if (!rows.length || !visible.length) return; downloadTextFile(`${safeFilenamePart(objectMeta.label)}-${result?.date || mlbDateIso()}-page-${query.page_number}.csv`, buildReportCsv({ columns: visible, rows, fieldMap, getValue }), 'text/csv;charset=utf-8') }
  async function exportAll() { setExporting(true); try { await onExportAll(visible, fieldMap) } finally { setExporting(false) } }
  return <div style={s.overlay}><section style={s.reportSurface}><header style={s.reportHeader}><div><div style={s.eyebrow}>Report Workspace</div><h2 style={s.reportTitle}>{objectMeta.label} Report</h2><div style={s.copySmall}>MLB date {result?.date || '—'} · {range.total} total results · {visible.length} visible columns</div></div><div style={s.actions}><button style={s.secondary} onClick={exportPage} disabled={!rows.length}>Export page</button><button style={s.secondary} onClick={exportAll} disabled={!range.total || exporting}>{exporting ? 'Exporting all…' : 'Export all'}</button><button style={s.secondary} onClick={onSave} disabled={!rows.length}>Save Report</button><button style={s.primary} onClick={close}>Back to Builder</button></div></header><div style={s.reportBody}><aside style={s.columnPanel}><h3 style={s.panelTitle}>Report Columns</h3><div style={s.columnList}>{columns.map((accessor, index) => { const field = fieldMap[accessor] || { label: accessor }; const isHidden = hidden.includes(accessor); return <div style={s.columnRow} key={accessor}><label><input type="checkbox" checked={!isHidden} onChange={() => setHidden(current => isHidden ? current.filter(v => v !== accessor) : [...current, accessor])} /> {field.label}</label><span><button style={s.iconButton} disabled={index === 0} onClick={() => move(accessor, -1)}>↑</button><button style={s.iconButton} disabled={index === columns.length - 1} onClick={() => move(accessor, 1)}>↓</button></span></div> })}</div></aside><main style={s.gridPanel}>{result?.filter_warnings?.length ? <div style={s.warning}>{result.filter_warnings.join(' • ')}</div> : null}<Pagination result={result} query={query} loading={loading} onPage={page_number => onQuery({ ...query, page_number })} onPageSize={page_size => onQuery({ ...query, page_number: 1, page_size })} />{loading ? <StatePanel tone="loading" title="Loading report page">Applying server sorting and pagination.</StatePanel> : rows.length && visible.length ? <div style={s.dataGridWrap}><table style={s.table}><thead><tr>{visible.map(accessor => { const field = fieldMap[accessor] || { accessor, label: accessor, sortable: true }; const active = query.sort_by === accessor; return <th key={accessor} style={s.th}><button style={s.sortButton} disabled={field.sortable === false} onClick={() => sortBy(field)}>{field.label} {active ? (query.sort_direction === 'desc' ? '↓' : '↑') : field.sortable === false ? '' : '↕'}</button></th> })}</tr></thead><tbody>{rows.map((row, rowIndex) => <tr key={row.entity_id || row.game_pk || rowIndex}>{visible.map(accessor => <td style={s.td} key={`${rowIndex}-${accessor}`}>{formatCell(getValue(row, accessor))}</td>)}</tr>)}</tbody></table></div> : <StatePanel title="No qualifying rows">No rows matched the selected date, filters, and lineup scope.</StatePanel>}<Pagination result={result} query={query} loading={loading} onPage={page_number => onQuery({ ...query, page_number })} onPageSize={page_size => onQuery({ ...query, page_number: 1, page_size })} /></main></div></section></div>
}

export default function MyDashboardReportBuilderV2() {
  const persisted = typeof window === 'undefined' ? {} : readJson(BUILDER_KEY, {})
  const [authChecked, setAuthChecked] = useState(false)
  const [profile, setProfile] = useState(null)
  const [form, setForm] = useState({ email: '', username: '', password: '' })
  const [activeObject, setActiveObject] = useState(persisted.activeObject || 'hitters')
  const [reportDate, setReportDate] = useState(persisted.reportDate || mlbDateIso())
  const [filters, setFilters] = useState(persisted.filters || Object.fromEntries(OBJECTS.map(object => [object.key, emptyFilters()])))
  const [queries, setQueries] = useState(persisted.queries || Object.fromEntries(OBJECTS.map(object => [object.key, defaultQueryState()])))
  const [selectedFields, setSelectedFields] = useState(persisted.selectedFields || DEFAULT_FIELDS)
  const [activeLineupsOnly, setActiveLineupsOnly] = useState(Boolean(persisted.activeLineupsOnly))
  const [results, setResults] = useState({})
  const [reportOpen, setReportOpen] = useState(false)
  const [reportObject, setReportObject] = useState('hitters')
  const [reportColumns, setReportColumns] = useState(DEFAULT_FIELDS)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [saveMessage, setSaveMessage] = useState('')
  const [workspace, setWorkspace] = useState(null)

  const activeMeta = OBJECTS.find(object => object.key === activeObject) || OBJECTS[0]
  const activeResult = results[activeObject]
  const activeFields = useMemo(() => serverFields(activeResult, FALLBACK_FIELDS), [activeResult])
  const activeQuery = normalizeQueryState(queries[activeObject])
  const activeFilters = filters[activeObject] || emptyFilters()
  const reportResult = results[reportObject]
  const reportMeta = OBJECTS.find(object => object.key === reportObject) || activeMeta
  const reportFields = serverFields(reportResult, FALLBACK_FIELDS)
  const folders = safeArray(workspace?.folders)
  const folderId = Number(workspace?.today_folder_id || folders[0]?.id)

  useEffect(() => writeJson(BUILDER_KEY, { activeObject, reportDate, filters, queries, selectedFields, activeLineupsOnly }), [activeObject, reportDate, filters, queries, selectedFields, activeLineupsOnly])

  async function apiJson(url, options = {}) {
    const headers = { ...(options.headers || {}) }, token = readToken()
    if (token) headers['X-Dashboard-Session'] = token
    const response = await fetch(url, { credentials: 'include', ...options, headers })
    const json = await response.json().catch(() => ({}))
    if (json?.session_token) writeToken(json.session_token)
    if (response.status === 401) { writeToken(''); setProfile(null); throw new Error('Dashboard sign-in required') }
    if (!response.ok) throw new Error(typeof json?.detail === 'string' ? json.detail : JSON.stringify(json.detail || json))
    return json
  }
  async function loadWorkspace() { try { const json = await apiJson(`${API}/my-dashboard/workspace`); setWorkspace(json) } catch {} }
  useEffect(() => { let cancelled = false; (async () => { try { const headers = readToken() ? { 'X-Dashboard-Session': readToken() } : {}; const response = await fetch(`${API}/my-dashboard/profile`, { credentials: 'include', headers }); const json = await response.json().catch(() => ({})); if (!cancelled && json.authenticated) { setProfile(json.user); await loadWorkspace() } } finally { if (!cancelled) setAuthChecked(true) } })(); return () => { cancelled = true } }, [])
  async function submitProfile(event) { event.preventDefault(); setError(''); try { const created = await apiJson(`${API}/my-dashboard/profile`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ ...form, feature_interests: ['Matchups', 'Model Projections'], wants_newsletter: false, plan_type: 'free' }) }); if (created?.session_token) writeToken(created.session_token); const current = await apiJson(`${API}/my-dashboard/profile`); setProfile(current.user); await loadWorkspace() } catch (err) { setError(err.message) } }
  function setBasic(objectKey, key, value) { setFilters(current => ({ ...current, [objectKey]: { ...(current[objectKey] || emptyFilters()), [key]: value } })); setQueries(current => ({ ...current, [objectKey]: { ...normalizeQueryState(current[objectKey]), page_number: 1 } })) }
  function setMetric(objectKey, metric, side, value) { setFilters(current => ({ ...current, [objectKey]: { ...(current[objectKey] || emptyFilters()), metrics: { ...(current[objectKey]?.metrics || {}), [metric]: { ...(current[objectKey]?.metrics?.[metric] || {}), [side]: value } } } })); setQueries(current => ({ ...current, [objectKey]: { ...normalizeQueryState(current[objectKey]), page_number: 1 } })) }
  function endpointFor(objectKey) { return activeLineupsOnly && ACTIVE_LINEUP_OBJECTS.has(objectKey) ? `${API}/my-dashboard/solver/active-lineups` : `${API}/my-dashboard/solver` }
  async function runReport(objectKey = activeObject, queryOverride = null, open = true) {
    const query = normalizeQueryState(queryOverride || queries[objectKey])
    setQueries(current => ({ ...current, [objectKey]: query })); setLoading(true); setError(''); setSaveMessage('')
    try {
      const json = await apiJson(endpointFor(objectKey), { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(queryPayload({ date: reportDate, component: objectKey, filters: cleanFilters(filters[objectKey] || {}), query })) })
      setResults(current => ({ ...current, [objectKey]: json })); setReportObject(objectKey); if (open) { setReportColumns(current => current.length ? current : DEFAULT_FIELDS); setReportOpen(true) }; return json
    } catch (err) { setError(err.message || 'Report generation failed'); return null } finally { setLoading(false) }
  }
  async function changeReportQuery(nextQuery) { await runReport(reportObject, nextQuery, false) }
  async function exportAll(columns, fieldMap) {
    const baseQuery = normalizeQueryState(queries[reportObject]); let page = 1; let done = false; const rows = []
    while (!done) {
      const json = await apiJson(endpointFor(reportObject), { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(queryPayload({ date: reportDate, component: reportObject, filters: cleanFilters(filters[reportObject] || {}), query: { ...baseQuery, page_number: page, page_size: 250 }, includeMetadata: page === 1 })) })
      rows.push(...safeArray(json.records?.length ? json.records : json.items)); done = Boolean(json.done) || !json?.page_info?.has_next; page += 1; if (page > 100) throw new Error('Export safety limit reached')
    }
    downloadTextFile(`${safeFilenamePart(reportMeta.label)}-${reportDate}-all.csv`, buildReportCsv({ columns, rows, fieldMap, getValue }), 'text/csv;charset=utf-8')
  }
  async function saveReport() {
    if (!folderId || !reportResult?.items?.length) { setSaveMessage('Populate a report before saving.'); return }
    const query = normalizeQueryState(queries[reportObject])
    const definition = { component: reportObject, selected_fields: reportColumns, filters: cleanFilters(filters[reportObject] || {}), active_lineups_only: activeLineupsOnly, page_size: query.page_size, sort: { by: query.sort_by, direction: query.sort_direction } }
    try { await apiJson(`${API}/my-dashboard/items`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ folder_id: folderId, source_tab: 'my-dashboard', source_type: 'report_view', title: `${reportMeta.label} Report | ${reportDate}`, subtitle: reportMeta.description, payload_json: { schema_version: 2, definition, snapshot: { generated_for_date: reportDate, generated_at: new Date().toISOString(), board_state: reportResult } }, filter_json: definition.filters, sort_json: { ...definition.sort, component: reportObject } }) }); setSaveMessage(`Saved ${reportMeta.label} report.`); await loadWorkspace() } catch (err) { setSaveMessage(err.message || 'Failed to save report.') }
  }

  if (!authChecked) return <div style={s.loadingPage}>Loading Report Builder…</div>
  if (!profile) return <main style={s.authPage}><form style={s.authCard} onSubmit={submitProfile}><div style={s.eyebrow}>MLBGPT Report Builder</div><h1>Create your analyst profile</h1><input required style={s.input} placeholder="Email" value={form.email} onChange={e => setForm(v => ({ ...v, email: e.target.value }))} /><input required style={s.input} placeholder="Username" value={form.username} onChange={e => setForm(v => ({ ...v, username: e.target.value }))} /><input style={s.input} type="password" placeholder="Password" value={form.password} onChange={e => setForm(v => ({ ...v, password: e.target.value }))} />{error ? <div style={s.error}>{error}</div> : null}<button style={s.primary}>Enter Report Builder</button></form></main>

  return <main style={s.page}><section style={s.hero}><div><div style={s.eyebrow}>MLBGPT Report Builder</div><h1 style={s.title}>Query the complete daily baseball universe.</h1><p style={s.copy}>Page size controls what is displayed—not how many hitters, pitchers, teams, or totals qualify.</p><div style={s.pillRow}><Pill>Signed in: {profile.username}</Pill><Pill tone="green">MLB date: {reportDate}</Pill><Pill tone="amber">Server sorted</Pill></div></div><div style={s.actions}><label style={s.dateField}>MLB date<input type="date" style={s.input} value={reportDate} onChange={e => { setReportDate(e.target.value || mlbDateIso()); setQueries(current => Object.fromEntries(Object.entries(current).map(([key, value]) => [key, { ...normalizeQueryState(value), page_number: 1 }]))) }} /></label><button style={s.primary} disabled={loading} onClick={() => runReport()}>{loading ? 'Populating…' : 'Populate Report'}</button></div></section>{saveMessage ? <div style={s.success}>{saveMessage}</div> : null}{error ? <StatePanel tone="error" title="Report could not be generated" action={<button style={s.secondary} onClick={() => runReport()}>Try again</button>}>{error}</StatePanel> : null}<div style={s.builderShell}><aside style={s.objectManager}><h3 style={s.panelTitle}>Report Objects</h3>{OBJECTS.map(object => <button key={object.key} style={activeObject === object.key ? s.objectActive : s.objectButton} onClick={() => { setActiveObject(object.key); setSelectedFields(DEFAULT_FIELDS) }}><span><strong>{object.label}</strong><small>{object.description}</small></span><b>{results[object.key]?.totalSize ?? results[object.key]?.total_count ?? '—'}</b></button>)}</aside><div style={s.mainStack}><section style={s.card}><div style={s.cardHeader}><div><h2 style={s.panelTitle}>{activeMeta.label}</h2><p style={s.copySmall}>{activeMeta.description}</p></div><label style={s.check}><input type="checkbox" checked={activeLineupsOnly} disabled={!ACTIVE_LINEUP_OBJECTS.has(activeObject)} onChange={e => { setActiveLineupsOnly(e.target.checked); setQueries(current => ({ ...current, [activeObject]: { ...activeQuery, page_number: 1 } })) }} /> Confirmed 1–9 only</label></div></section><div style={s.builderColumns}><div style={s.stack}><FilterPanel objectKey={activeObject} filters={activeFilters} fields={activeFields} setBasic={setBasic} setMetric={setMetric} /><FieldLibrary fields={activeFields} selected={selectedFields} setSelected={setSelectedFields} /></div><section style={s.previewCard}><h3 style={s.panelTitle}>Report Preview</h3><div style={s.pillRow}><Pill>{selectedFields.length} columns</Pill><Pill tone="green">{activeResult?.totalSize ?? 0} total results</Pill><Pill tone="amber">{activeQuery.page_size} per page</Pill></div><button style={s.populateWide} disabled={loading} onClick={() => runReport()}>{loading ? 'Populating…' : 'Populate Report'}</button>{activeResult ? <button style={s.secondaryWide} onClick={() => { setReportObject(activeObject); setReportColumns([...selectedFields]); setReportOpen(true) }}>Open Last Report</button> : <StatePanel title="No report generated yet">Choose fields and filters, then populate the report.</StatePanel>}</section></div></div></div><ReportWorkspace open={reportOpen} close={() => setReportOpen(false)} objectMeta={reportMeta} result={reportResult} fields={reportFields} columns={reportColumns} setColumns={setReportColumns} query={normalizeQueryState(queries[reportObject])} loading={loading} onQuery={changeReportQuery} onSave={saveReport} onExportAll={exportAll} /></main>
}

const s = {
  page: { minHeight: '100vh', padding: 'clamp(12px,2.5vw,28px)', boxSizing: 'border-box', color: C.text, background: `linear-gradient(180deg,${C.bg},#030712)` }, loadingPage: { minHeight: '100vh', display: 'grid', placeItems: 'center', color: C.text, background: C.bg }, authPage: { minHeight: '100vh', display: 'grid', placeItems: 'center', padding: 16, color: C.text, background: C.bg }, authCard: { width: '100%', maxWidth: 520, display: 'grid', gap: 12, padding: 24, borderRadius: 24, border: `1px solid ${C.border}`, background: C.panel },
  hero: { display: 'flex', justifyContent: 'space-between', flexWrap: 'wrap', gap: 18, padding: 24, marginBottom: 18, borderRadius: 24, border: `1px solid ${C.border}`, background: C.panel }, eyebrow: { color: C.blue, fontSize: 12, fontWeight: 900, letterSpacing: '.11em', textTransform: 'uppercase' }, title: { margin: '8px 0', fontSize: 'clamp(28px,5vw,46px)' }, copy: { margin: 0, maxWidth: 760, color: C.muted, lineHeight: 1.6 }, copySmall: { margin: '4px 0 0', color: C.muted, fontSize: 13, lineHeight: 1.45 }, actions: { display: 'flex', gap: 10, flexWrap: 'wrap', alignItems: 'flex-end' }, dateField: { minWidth: 170, display: 'grid', gap: 5, color: C.muted, fontSize: 12, fontWeight: 800 },
  builderShell: { display: 'grid', gridTemplateColumns: 'minmax(230px,280px) minmax(0,1fr)', gap: 16, alignItems: 'start' }, objectManager: { position: 'sticky', top: 16, display: 'grid', gap: 8, padding: 15, borderRadius: 22, border: `1px solid ${C.border}`, background: C.panel }, objectButton: { display: 'flex', justifyContent: 'space-between', gap: 10, textAlign: 'left', padding: 12, color: C.text, background: C.panel2, border: `1px solid ${C.border}`, borderRadius: 14, cursor: 'pointer' }, objectActive: { display: 'flex', justifyContent: 'space-between', gap: 10, textAlign: 'left', padding: 12, color: C.text, background: 'rgba(96,165,250,.16)', border: '1px solid rgba(96,165,250,.5)', borderRadius: 14, cursor: 'pointer' }, mainStack: { display: 'grid', gap: 14, minWidth: 0 }, builderColumns: { display: 'grid', gridTemplateColumns: 'minmax(0,1.25fr) minmax(280px,.75fr)', gap: 14 }, stack: { display: 'grid', gap: 14 },
  card: { padding: 16, minWidth: 0, borderRadius: 20, border: `1px solid ${C.border}`, background: C.panel }, previewCard: { padding: 18, display: 'grid', gap: 16, alignContent: 'start', borderRadius: 20, border: `1px solid ${C.border}`, background: C.panel }, cardHeader: { display: 'flex', justifyContent: 'space-between', gap: 12, flexWrap: 'wrap' }, panelTitle: { margin: 0, fontSize: 19, fontWeight: 900 }, input: { width: '100%', boxSizing: 'border-box', padding: '11px 12px', color: C.text, colorScheme: 'dark', background: C.panel2, border: `1px solid ${C.border}`, borderRadius: 11 }, miniInput: { width: '100%', boxSizing: 'border-box', padding: 8, color: C.text, background: C.panel2, border: `1px solid ${C.border}`, borderRadius: 9 }, selectSmall: { padding: 8, color: C.text, background: C.panel2, border: `1px solid ${C.border}`, borderRadius: 9 },
  filterGrid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit,minmax(150px,1fr))', gap: 8 }, metricGrid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit,minmax(145px,1fr))', gap: 8 }, metricCard: { display: 'grid', gap: 7, padding: 10, color: C.muted, background: C.panel2, border: `1px solid ${C.border}`, borderRadius: 12 }, twoCol: { display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 6 }, sectionLabel: { margin: '14px 0 8px', color: C.muted, fontSize: 11, fontWeight: 900, textTransform: 'uppercase' }, fieldGrid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit,minmax(135px,1fr))', gap: 7 }, fieldButton: { display: 'grid', gap: 2, textAlign: 'left', padding: 9, color: C.text, background: C.panel2, border: `1px solid ${C.border}`, borderRadius: 10, cursor: 'pointer' }, fieldActive: { display: 'grid', gap: 2, textAlign: 'left', padding: 9, color: C.text, background: 'rgba(52,211,153,.13)', border: '1px solid rgba(52,211,153,.42)', borderRadius: 10, cursor: 'pointer' },
  primary: { padding: '11px 15px', color: C.text, fontWeight: 900, background: 'rgba(96,165,250,.2)', border: '1px solid rgba(96,165,250,.5)', borderRadius: 12, cursor: 'pointer' }, secondary: { padding: '9px 12px', color: C.text, fontWeight: 800, background: C.panel2, border: `1px solid ${C.border}`, borderRadius: 10, cursor: 'pointer' }, secondaryWide: { width: '100%', padding: 12, color: C.text, fontWeight: 800, background: C.panel2, border: `1px solid ${C.border}`, borderRadius: 12, cursor: 'pointer' }, populateWide: { width: '100%', padding: 14, color: C.text, fontWeight: 900, background: 'rgba(96,165,250,.22)', border: '1px solid rgba(96,165,250,.55)', borderRadius: 13, cursor: 'pointer' }, check: { display: 'inline-flex', gap: 8, alignItems: 'center', color: C.muted }, pillRow: { display: 'flex', gap: 8, flexWrap: 'wrap' }, pill: { display: 'inline-flex', padding: '6px 10px', border: '1px solid', borderRadius: 999, fontSize: 12, fontWeight: 850 },
  empty: { display: 'grid', gap: 8, padding: 14, color: C.muted, border: `1px dashed ${C.border}`, borderRadius: 13, background: C.panel2 }, loadingState: { display: 'grid', gap: 8, padding: 14, color: '#bfdbfe', border: '1px solid rgba(96,165,250,.35)', borderRadius: 13, background: 'rgba(96,165,250,.10)' }, errorState: { display: 'grid', gap: 10, marginBottom: 14, padding: 14, color: '#fecaca', border: '1px solid rgba(248,113,113,.35)', borderRadius: 13, background: 'rgba(248,113,113,.12)' }, success: { marginBottom: 14, padding: 12, color: '#a7f3d0', background: 'rgba(52,211,153,.12)', border: '1px solid rgba(52,211,153,.35)', borderRadius: 12 }, error: { padding: 12, color: '#fecaca', background: 'rgba(248,113,113,.12)', borderRadius: 12 }, warning: { padding: 11, color: '#fde68a', background: 'rgba(251,191,36,.12)', borderRadius: 12 },
  pagination: { display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 10, flexWrap: 'wrap', color: C.muted, fontSize: 13 }, pageSize: { display: 'flex', gap: 7, alignItems: 'center' }, overlay: { position: 'fixed', inset: 0, zIndex: 1000, padding: 'clamp(6px,2vw,22px)', boxSizing: 'border-box', background: 'rgba(2,6,23,.88)' }, reportSurface: { height: '100%', display: 'grid', gridTemplateRows: 'auto minmax(0,1fr)', overflow: 'hidden', color: C.text, background: C.panel, border: `1px solid ${C.border}`, borderRadius: 20 }, reportHeader: { display: 'flex', justifyContent: 'space-between', gap: 16, flexWrap: 'wrap', padding: 18, borderBottom: `1px solid ${C.border}` }, reportTitle: { margin: '5px 0', fontSize: 28 }, reportBody: { minHeight: 0, display: 'grid', gridTemplateColumns: '280px minmax(0,1fr)' }, columnPanel: { minHeight: 0, padding: 14, overflowY: 'auto', borderRight: `1px solid ${C.border}`, background: C.panel2 }, columnList: { display: 'grid', gap: 7, marginTop: 12 }, columnRow: { display: 'flex', justifyContent: 'space-between', gap: 8, alignItems: 'center', padding: 8, border: `1px solid ${C.border}`, borderRadius: 10, background: C.panel }, iconButton: { marginLeft: 4, padding: '4px 7px', color: C.text, background: C.panel3, border: `1px solid ${C.border}`, borderRadius: 7 }, gridPanel: { minWidth: 0, minHeight: 0, padding: 14, display: 'grid', gridTemplateRows: 'auto auto minmax(0,1fr) auto', gap: 10 }, dataGridWrap: { minWidth: 0, minHeight: 0, overflow: 'auto', border: `1px solid ${C.border}`, borderRadius: 12, background: C.panel2 }, table: { width: 'max-content', minWidth: '100%', borderCollapse: 'separate', borderSpacing: 0 }, th: { position: 'sticky', top: 0, padding: 0, background: C.panel3, borderBottom: `1px solid ${C.border}` }, sortButton: { width: '100%', padding: '11px 12px', color: C.text, textAlign: 'left', whiteSpace: 'nowrap', background: 'transparent', border: 0, cursor: 'pointer' }, td: { padding: '10px 12px', color: C.text, whiteSpace: 'nowrap', borderBottom: `1px solid ${C.border}` },
}
