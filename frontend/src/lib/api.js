const PROD_API_BASE = 'https://mlb-prediction-app-production-732c.up.railway.app'

export const API_BASE = import.meta.env.VITE_API_BASE_URL || PROD_API_BASE
const JSON_CACHE = new Map()
const IN_FLIGHT_JSON = new Map()
const STORAGE_PREFIX = 'mlb-json-cache:v2:'
const CACHE_CHANNEL_NAME = 'mlb-json-cache-updates'
const RAW_FETCH_TTL_SECONDS = 30 * 60
const HEAVY_MATCHUPS_IN_FLIGHT = new Map()
let CACHE_CHANNEL = null
let FETCH_CACHE_INSTALLED = false

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

function storageKey(key) {
  return `${STORAGE_PREFIX}${String(key || '')}`
}

function durableStorage() {
  if (typeof window === 'undefined') return null
  try {
    return window.localStorage || null
  } catch {
    return null
  }
}

function sessionFallbackStorage() {
  if (typeof window === 'undefined') return null
  try {
    return window.sessionStorage || null
  } catch {
    return null
  }
}

function readStorageRecord(key) {
  const stores = [durableStorage(), sessionFallbackStorage()].filter(Boolean)
  for (const store of stores) {
    try {
      const raw = store.getItem(storageKey(key))
      if (!raw) continue
      const parsed = JSON.parse(raw)
      if (!parsed || typeof parsed !== 'object') continue
      if (typeof parsed.createdAt !== 'number') continue
      return parsed
    } catch {
    }
  }
  return null
}

function writeStorageRecord(key, value) {
  const record = JSON.stringify({
    createdAt: nowMs(),
    value: cloneJson(value),
  })
  const stores = [durableStorage(), sessionFallbackStorage()].filter(Boolean)
  for (const store of stores) {
    try {
      store.setItem(storageKey(key), record)
      return
    } catch {
    }
  }
}

function deleteStorageRecord(key) {
  const stores = [durableStorage(), sessionFallbackStorage()].filter(Boolean)
  for (const store of stores) {
    try {
      store.removeItem(storageKey(key))
    } catch {
    }
  }
}

function cacheChannel() {
  if (typeof window === 'undefined' || typeof window.BroadcastChannel === 'undefined') return null
  if (CACHE_CHANNEL) return CACHE_CHANNEL
  try {
    CACHE_CHANNEL = new window.BroadcastChannel(CACHE_CHANNEL_NAME)
    CACHE_CHANNEL.onmessage = event => {
      const msg = event?.data || {}
      if (!msg.key) return
      if (msg.type === 'delete') {
        JSON_CACHE.delete(String(msg.key))
        IN_FLIGHT_JSON.delete(String(msg.key))
        return
      }
      if (msg.type === 'write') {
        const record = readStorageRecord(String(msg.key))
        if (record) {
          JSON_CACHE.set(String(msg.key), {
            createdAt: record.createdAt,
            value: cloneJson(record.value),
          })
        }
      }
    }
    return CACHE_CHANNEL
  } catch {
    return null
  }
}

function publishCacheEvent(type, key) {
  const channel = cacheChannel()
  if (!channel) return
  try {
    channel.postMessage({ type, key: String(key || '') })
  } catch {
  }
}

function urlString(input) {
  try {
    if (typeof input === 'string') return input
    if (input?.url) return input.url
    return String(input || '')
  } catch {
    return ''
  }
}

function isCacheableApiGet(input, init = {}) {
  const method = String(init?.method || input?.method || 'GET').toUpperCase()
  if (method !== 'GET') return false
  if (init?.cache === 'no-store' || init?.cache === 'reload') return false
  const url = urlString(input)
  if (!url) return false
  return url.startsWith(API_BASE) || url.startsWith('/')
}

function jsonResponseFromCache(value) {
  return new Response(JSON.stringify(cloneJson(value)), {
    status: 200,
    statusText: 'OK',
    headers: {
      'content-type': 'application/json; charset=utf-8',
      'x-mlbgpt-browser-cache': 'HIT',
    },
  })
}

function dateBucket(date) {
  const today = getMlbLiveDate()
  const todayDate = new Date(`${today}T12:00:00Z`)
  const targetDate = new Date(`${date}T12:00:00Z`)
  const diffDays = Math.round((targetDate - todayDate) / 86400000)
  if (diffDays === -1) return 'yesterday'
  if (diffDays === 0) return 'today'
  if (diffDays === 1) return 'tomorrow'
  return null
}

function scheduleRowToMatchup(g, date) {
  return {
    game_pk: g.game_pk,
    game_date: date,
    game_time: g.game_time || g.game_date,
    venue: g.venue,
    status: g.status,
    home_team_id: g.home_team_id,
    away_team_id: g.away_team_id,
    home_team_name: g.home_team_name,
    away_team_name: g.away_team_name,
    home_pitcher_id: g.home_pitcher?.id,
    away_pitcher_id: g.away_pitcher?.id,
    home_pitcher_name: g.home_pitcher?.name,
    away_pitcher_name: g.away_pitcher?.name,
    home_team_record: 'Record pending',
    away_team_record: 'Record pending',
    home_win_prob: null,
    away_win_prob: null,
    probability_source: 'schedule_calendar_first_paint',
    frontend_fallback_source: '/matchups/calendar/schedule',
  }
}

function matchupsDateFromUrl(url) {
  try {
    const parsed = new URL(url, API_BASE)
    if (parsed.pathname !== '/matchups') return null
    return parsed.searchParams.get('date') || getMlbLiveDate()
  } catch {
    return null
  }
}

async function buildScheduleFirstMatchups(date) {
  const scheduleUrl = `${API_BASE}/matchups/calendar/schedule`
  const calendar = readCachedJson(scheduleUrl, 120) || await fetchJsonUncached(scheduleUrl)
  const bucket = dateBucket(date)
  const source = bucket ? calendar?.[bucket] : null
  if (!source || !Array.isArray(source.games)) return []
  return source.games.map(game => scheduleRowToMatchup(game, source.date || date))
}

function hydrateHeavyMatchupsInBackground(url) {
  const key = String(url || '')
  if (!key || HEAVY_MATCHUPS_IN_FLIGHT.has(key)) return
  const promise = fetchJsonUncached(key)
    .catch(() => null)
    .finally(() => HEAVY_MATCHUPS_IN_FLIGHT.delete(key))
  HEAVY_MATCHUPS_IN_FLIGHT.set(key, promise)
}

function installFetchCache() {
  if (typeof window === 'undefined' || FETCH_CACHE_INSTALLED) return
  if (typeof window.fetch !== 'function' || typeof window.Response !== 'function') return
  const originalFetch = window.fetch.bind(window)
  window.fetch = async (input, init = {}) => {
    const url = urlString(input)
    if (!isCacheableApiGet(input, init)) {
      return originalFetch(input, init)
    }

    const cached = readCachedJson(url, RAW_FETCH_TTL_SECONDS)
    if (cached != null) {
      return jsonResponseFromCache(cached)
    }

    const response = await originalFetch(input, init)
    const contentType = response.headers.get('content-type') || ''
    if (!response.ok || !contentType.includes('application/json')) {
      return response
    }

    try {
      const clone = response.clone()
      const json = await clone.json()
      writeCachedJson(url, json)
    } catch {
    }
    return response
  }
  FETCH_CACHE_INSTALLED = true
}

if (typeof window !== 'undefined') {
  cacheChannel()
  installFetchCache()
  window.addEventListener?.('storage', event => {
    if (!event.key || !event.key.startsWith(STORAGE_PREFIX)) return
    const key = event.key.slice(STORAGE_PREFIX.length)
    if (!event.newValue) {
      JSON_CACHE.delete(key)
      IN_FLIGHT_JSON.delete(key)
      return
    }
    const record = readStorageRecord(key)
    if (record) {
      JSON_CACHE.set(key, {
        createdAt: record.createdAt,
        value: cloneJson(record.value),
      })
    }
  })
}

export function readCachedJson(url, ttlSeconds = 60) {
  const key = String(url || '')
  const memoryRecord = JSON_CACHE.get(key)
  if (memoryRecord) {
    if (ttlSeconds > 0 && nowMs() - memoryRecord.createdAt > ttlSeconds * 1000) {
      JSON_CACHE.delete(key)
      deleteStorageRecord(key)
      return null
    }
    return cloneJson(memoryRecord.value)
  }

  const storageRecord = readStorageRecord(key)
  if (!storageRecord) return null
  if (ttlSeconds > 0 && nowMs() - storageRecord.createdAt > ttlSeconds * 1000) {
    deleteStorageRecord(key)
    return null
  }

  JSON_CACHE.set(key, {
    createdAt: storageRecord.createdAt,
    value: cloneJson(storageRecord.value),
  })
  return cloneJson(storageRecord.value)
}

export function writeCachedJson(url, value) {
  const key = String(url || '')
  const record = {
    createdAt: nowMs(),
    value: cloneJson(value),
  }
  JSON_CACHE.set(key, record)
  writeStorageRecord(key, record.value)
  publishCacheEvent('write', key)
  return cloneJson(value)
}

export function clearCachedJson(url) {
  const key = String(url || '')
  JSON_CACHE.delete(key)
  IN_FLIGHT_JSON.delete(key)
  deleteStorageRecord(key)
  publishCacheEvent('delete', key)
}

async function fetchJsonUncached(url, signal) {
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

export async function fetchJson(url, { ttlSeconds = 60, forceRefresh = false, signal } = {}) {
  const key = String(url || '')
  if (!forceRefresh) {
    const cached = readCachedJson(url, ttlSeconds)
    if (cached != null) return cached
  } else {
    clearCachedJson(url)
  }

  const matchupsDate = !forceRefresh ? matchupsDateFromUrl(key) : null
  if (matchupsDate) {
    try {
      const scheduleMatchups = await buildScheduleFirstMatchups(matchupsDate)
      if (scheduleMatchups.length > 0) {
        writeCachedJson(key, scheduleMatchups)
        hydrateHeavyMatchupsInBackground(key)
        return cloneJson(scheduleMatchups)
      }
    } catch {
      // Fall through to the heavy route if the lightweight schedule is unavailable.
    }
  }

  const canDedupe = !forceRefresh && !signal
  if (canDedupe && IN_FLIGHT_JSON.has(key)) {
    return cloneJson(await IN_FLIGHT_JSON.get(key))
  }

  const requestPromise = fetchJsonUncached(url, signal)
  if (canDedupe) {
    IN_FLIGHT_JSON.set(key, requestPromise)
  }

  try {
    return cloneJson(await requestPromise)
  } finally {
    if (canDedupe) {
      IN_FLIGHT_JSON.delete(key)
    }
  }
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
