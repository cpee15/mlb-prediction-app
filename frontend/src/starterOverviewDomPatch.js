import { API_BASE, getMlbLiveDate } from './lib/api'

const wanted = new Set(['era','whip','k_pct','bb_pct','k_minus_bb_pct','hr_per_9','innings_pitched','strikeouts','walks','xwoba_allowed','xba_allowed','hard_hit_rate_allowed','barrel_rate_allowed','avg_velocity','avg_spin_rate'])
let cache = null
let cacheDate = null
let cacheTime = 0

function fmt(m) {
  if (!m || m.value == null) return null
  if (m.formatted_value) return m.formatted_value
  const n = Number(m.value)
  if (Number.isNaN(n)) return String(m.value)
  const k = m.key || ''
  if (k.endsWith('_pct')) return (n * 100).toFixed(1) + '%'
  if (k === 'era' || k === 'whip' || k === 'hr_per_9') return n.toFixed(2)
  if (k === 'xwoba_allowed' || k === 'xba_allowed') return n.toFixed(3)
  if (k === 'avg_velocity') return n.toFixed(1)
  if (k === 'avg_spin_rate' || k === 'strikeouts' || k === 'walks') return Math.round(n).toLocaleString()
  return Number.isInteger(n) ? n.toLocaleString() : n.toFixed(2)
}

function rows(overview) {
  return (overview?.display_metrics || [])
    .filter(m => m && m.available && m.value != null && wanted.has(m.key))
    .map(m => ({ label: m.label, value: fmt(m), key: m.key }))
    .filter(m => m.value && !['N/A', 'Pending', 'Unavailable', 'null'].includes(String(m.value)))
    .slice(0, 10)
}

async function loadSlate() {
  const dateInput = document.querySelector('input[type="date"]')
  const date = dateInput?.value || getMlbLiveDate()
  if (cache && cacheDate === date && Date.now() - cacheTime < 300000) return cache
  const res = await fetch(`${API_BASE}/matchups?date=${date}`)
  if (!res.ok) return []
  cache = await res.json()
  cacheDate = date
  cacheTime = Date.now()
  return Array.isArray(cache) ? cache : []
}

function buildGrid(metricRows, align) {
  const grid = document.createElement('div')
  grid.className = 'starter-overview-dom-grid'
  grid.style.cssText = 'display:grid;grid-template-columns:repeat(auto-fit,minmax(74px,1fr));gap:7px;margin-top:12px;min-width:0;'
  metricRows.forEach(m => {
    const box = document.createElement('div')
    box.style.cssText = 'border:1px solid var(--border-subtle);border-radius:var(--radius-sm);background:rgba(17,24,39,.62);padding:7px 8px;min-width:0;'
    const label = document.createElement('div')
    label.textContent = m.label
    label.style.cssText = `color:var(--text-muted);font-size:9px;text-transform:uppercase;letter-spacing:.06em;margin-bottom:4px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;text-align:${align};`
    const value = document.createElement('div')
    value.textContent = m.value
    value.style.cssText = `color:var(--text-primary);font-size:13px;font-weight:850;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;text-align:${align};`
    box.appendChild(label)
    box.appendChild(value)
    grid.appendChild(box)
  })
  return grid
}

function findTextNode(root, text) {
  const walker = document.createTreeWalker(root, NodeFilter.SHOW_ELEMENT)
  let node
  const target = String(text || '').trim().toLowerCase()
  if (!target) return null
  while ((node = walker.nextNode())) {
    if (node.children.length <= 2 && String(node.textContent || '').trim().toLowerCase() === target) return node
  }
  return null
}

function injectForPitcher(name, overview, align) {
  const metricRows = rows(overview)
  if (!metricRows.length || !name) return
  const node = findTextNode(document.body, name)
  if (!node) return
  const panel = node.closest('div')
  if (!panel || panel.querySelector('.starter-overview-dom-grid')) return
  panel.appendChild(buildGrid(metricRows, align))
}

async function renderStarterOverviewMetrics() {
  try {
    const slate = await loadSlate()
    slate.forEach(game => {
      injectForPitcher(game.away_pitcher_name, game.pitcher_overview?.away, 'left')
      injectForPitcher(game.home_pitcher_name, game.pitcher_overview?.home, 'right')
    })
  } catch (_) {}
}

if (typeof window !== 'undefined') {
  const observer = new MutationObserver(() => renderStarterOverviewMetrics())
  window.addEventListener('load', renderStarterOverviewMetrics)
  document.addEventListener('change', e => {
    if (e.target && e.target.matches && e.target.matches('input[type="date"]')) {
      cache = null
      setTimeout(renderStarterOverviewMetrics, 300)
    }
  })
  observer.observe(document.documentElement, { childList: true, subtree: true })
}
