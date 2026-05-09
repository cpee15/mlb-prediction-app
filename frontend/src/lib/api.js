const PROD_API_BASE = 'https://mlb-prediction-app-production-732c.up.railway.app'

export const API_BASE = import.meta.env.VITE_API_BASE_URL || PROD_API_BASE

function getEasternDateParts(date = new Date()) {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/New_York',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    hour12: false,
  }).formatToParts(date)

  const values = Object.fromEntries(parts.map(part => [part.type, part.value]))

  return {
    year: values.year,
    month: values.month,
    day: values.day,
    hour: Number(values.hour),
  }
}

function formatDateParts(values) {
  return `${values.year}-${values.month}-${values.day}`
}

function shiftIsoDate(isoDate, days) {
  const [year, month, day] = isoDate.split('-').map(Number)
  const utc = new Date(Date.UTC(year, month - 1, day + days))
  return utc.toISOString().slice(0, 10)
}

export function getMlbToday(date = new Date()) {
  return formatDateParts(getEasternDateParts(date))
}

export function getMlbLiveDate(date = new Date()) {
  const eastern = getEasternDateParts(date)
  const easternDate = formatDateParts(eastern)

  if (eastern.hour >= 0 && eastern.hour < 6) {
    return shiftIsoDate(easternDate, -1)
  }

  return easternDate
}

export function addIsoDays(isoDate, days) {
  return shiftIsoDate(isoDate, days)
}
