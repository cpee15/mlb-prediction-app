const PROD_API_BASE = 'https://mlb-prediction-app-production-732c.up.railway.app'

export const API_BASE = import.meta.env.VITE_API_BASE_URL || PROD_API_BASE
const JSON_CACHE = new Map()
const IN_FLIGHT_JSON = new Map()
const STORAGE_PREFIX = 'mlb-json-cache:v2:'
const CACHE_CHANNEL_NAME = 'mlb-json-cache-updates'
let CACHE_CHANNEL = null

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

if (typeof window !== 'undefined') {
  cacheChannel()
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
