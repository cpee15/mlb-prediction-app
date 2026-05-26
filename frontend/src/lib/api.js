const PROD_API_BASE = 'https://mlb-prediction-app-production-732c.up.railway.app'

export const API_BASE = import.meta.env.VITE_API_BASE_URL || PROD_API_BASE
const JSON_CACHE = new Map()

function nowMs() {
  return Date.now()
}

function cloneJson(value) {
  if (value == null) return value
  try {
    return JSON.parse(JSON.stringify(value))
  } catch {
    return value
  }
}

export function readCachedJson(url, ttlSeconds = 60) {
  const key = String(url || '')
  const record = JSON_CACHE.get(key)
  if (!record) return null
  if (ttlSeconds > 0 && nowMs() - record.createdAt > ttlSeconds * 1000) {
    JSON_CACHE.delete(key)
    return null
  }
  return cloneJson(record.value)
}

export function writeCachedJson(url, value) {
  const key = String(url || '')
  JSON_CACHE.set(key, {
    createdAt: nowMs(),
    value: cloneJson(value),
  })
  return cloneJson(value)
}

export async function fetchJson(url, { ttlSeconds = 60, forceRefresh = false, signal } = {}) {
  if (!forceRefresh) {
    const cached = readCachedJson(url, ttlSeconds)
    if (cached != null) return cached
  }

  const response = await fetch(url, { signal })
  if (!response.ok) {
    const body = await response.text()
    throw new Error(`${response.status} ${response.statusText}: ${body.slice(0, 300)}`)
  }

  const contentType = response.headers.get('content-type') || ''
  if (!contentType.includes('application/json')) {
    const body = await response.text()
    throw new Error(`Expected JSON but received ${contentType || 'unknown content type'}. Response starts with: ${body.slice(0, 120)}`)
  }

  const json = await response.json()
  writeCachedJson(url, json)
  return cloneJson(json)
}

export function getMlbToday() {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/New_York',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).formatToParts(new Date())

  const values = Object.fromEntries(parts.map(part => [part.type, part.value]))
  return `${values.year}-${values.month}-${values.day}`
}

/**
 * Return the current MLB "live" date, accounting for late-night rollover.
 * Games that start before 5 AM ET are still part of the previous day's slate.
 */
export function getMlbLiveDate() {
  const now = new Date()
  const etHour = Number(
    new Intl.DateTimeFormat('en-US', {
      timeZone: 'America/New_York',
      hour: 'numeric',
      hour12: false,
    }).format(now),
  )

  const adjusted = etHour < 5 ? new Date(now.getTime() - 24 * 60 * 60 * 1000) : now

  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/New_York',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).formatToParts(adjusted)

  const values = Object.fromEntries(parts.map(part => [part.type, part.value]))
  return `${values.year}-${values.month}-${values.day}`
}

/**
 * Add (or subtract) days from an ISO date string (YYYY-MM-DD).
 */
export function addIsoDays(dateStr, days) {
  const d = new Date(dateStr + 'T12:00:00')
  d.setDate(d.getDate() + days)
  return d.toISOString().slice(0, 10)
}
