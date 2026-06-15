export const UNIT_SIZE_DOLLARS = 100

export function toNumber(value) {
  if (value === null || value === undefined || value === '') return null
  const n = Number(value)
  return Number.isFinite(n) ? n : null
}

export function americanOddsToProfit(stake = UNIT_SIZE_DOLLARS, odds, result) {
  const stakeValue = toNumber(stake) ?? UNIT_SIZE_DOLLARS
  const price = toNumber(odds)
  const normalized = String(result || '').toLowerCase()
  if (normalized === 'push') return 0
  if (normalized === 'lost' || normalized === 'loss') return -stakeValue
  if (normalized !== 'won' && normalized !== 'win') return null
  if (price === null || price === 0) return null
  if (price < 0) return stakeValue * 100 / Math.abs(price)
  return stakeValue * price / 100
}

export function resultToUnits(profit, stake = UNIT_SIZE_DOLLARS) {
  const p = toNumber(profit)
  const s = toNumber(stake) ?? UNIT_SIZE_DOLLARS
  if (p === null || !s) return null
  return p / s
}

export function isGradedDecision(row) {
  return ['won', 'lost', 'push'].includes(String(row?.grade || '').toLowerCase())
}

export function isPending(row) {
  const grade = String(row?.grade || '').toLowerCase()
  const status = String(row?.result_status || '').toLowerCase()
  return grade === 'pending' || status === 'pending' || status === 'live'
}

export function normalizeStatus(value) {
  return String(value || '').trim().toLowerCase().replace(/\s+/g, '_')
}

export function numericScore(row) {
  const candidates = [row?.confidence, row?.score, row?.model_probability].map(toNumber).filter(v => v !== null)
  return candidates.length ? Math.max(...candidates) : null
}

export function confidenceBand(value) {
  const n = toNumber(value)
  if (n === null) return 'Unavailable'
  if (n >= 0.65) return '.65+'
  if (n >= 0.60) return '.60-.64'
  if (n >= 0.55) return '.55-.59'
  if (n >= 0.50) return '.50-.54'
  return '<.50'
}

export function edgeBand(value) {
  const n = toNumber(value)
  if (n === null) return 'Unavailable'
  if (n >= 0.10) return '+10%+'
  if (n >= 0.05) return '+5% to +9.9%'
  if (n > 0) return '+0% to +4.9%'
  if (n === 0) return '0%'
  return 'Negative'
}

export function getRecommendationStatus(row) {
  const diagnostics = row?.daily_odds_diagnostics || row?.raw_payload?.daily_odds_diagnostics || row?.raw_payload?.diagnostics || {}
  return normalizeStatus(diagnostics.recommendation_status || row?.recommendation_status || row?.bucket || row?.status)
}

export function isRejectedNoBet(row) {
  const status = getRecommendationStatus(row)
  const reason = normalizeStatus(row?.grade_reason || row?.primary_reason)
  return ['no_bet', 'rejected', 'suppressed', 'non_positive_edge'].some(token => status.includes(token) || reason.includes(token))
}

export function rowHasPrice(row) {
  return toNumber(row?.price) !== null || toNumber(row?.provider_price) !== null || toNumber(row?.latest_price) !== null || toNumber(row?.best_price_seen) !== null
}

export function effectivePrice(row) {
  return toNumber(row?.price) ?? toNumber(row?.provider_price) ?? toNumber(row?.latest_price) ?? toNumber(row?.best_price_seen)
}

export function recommendationBucket(row) {
  const status = getRecommendationStatus(row)
  const score = numericScore(row)
  const edge = toNumber(row?.edge)
  const ev = toNumber(row?.expected_value)
  const hasIdentity = Boolean(row?.pick_label || row?.player_name || row?.team_name)
  const hasMetrics = [score, edge, ev, toNumber(row?.model_probability)].some(v => v !== null)
  if (!hasIdentity && !hasMetrics) return 'missing_data'
  if (isRejectedNoBet(row)) return 'rejected'
  if (status.includes('strong') || status.includes('recommended') || (score !== null && score >= 0.55) || (edge !== null && edge > 0) || (ev !== null && ev > 0)) return 'recommended'
  if (row?.grade === 'watchlist_only' || row?.grade === 'ungraded' || (score !== null && score >= 0.50)) return 'lean'
  return 'missing_data'
}

export function recommendationLabel(bucket) {
  return {
    recommended: 'Recommended',
    lean: 'Lean / Watchlist',
    rejected: 'Rejected / No Bet',
    missing_data: 'Missing Data / Ungraded',
  }[bucket] || 'Missing Data / Ungraded'
}

export function gradeProfit(row, unitSize = UNIT_SIZE_DOLLARS) {
  if (!isGradedDecision(row)) return null
  const price = effectivePrice(row)
  if (price === null && String(row?.grade).toLowerCase() === 'won') return null
  return americanOddsToProfit(unitSize, price || -100, row.grade)
}

export function summarizePnl(rows = [], unitSize = UNIT_SIZE_DOLLARS) {
  const ledger = rows.map(row => {
    const stake = unitSize
    const profit = gradeProfit(row, stake)
    return {
      ...row,
      bucket: recommendationBucket(row),
      stake,
      price_for_pnl: effectivePrice(row),
      profit,
      units: resultToUnits(profit, stake),
      confidence_band: confidenceBand(row?.confidence ?? row?.score ?? row?.model_probability),
      edge_band: edgeBand(row?.edge),
    }
  })
  const graded = ledger.filter(row => isGradedDecision(row))
  const priced = graded.filter(row => row.profit !== null)
  const wins = graded.filter(row => row.grade === 'won').length
  const losses = graded.filter(row => row.grade === 'lost').length
  const pushes = graded.filter(row => row.grade === 'push').length
  const risked = priced.filter(row => row.grade !== 'push').length * unitSize
  const profit = priced.reduce((sum, row) => sum + (toNumber(row.profit) ?? 0), 0)
  const units = resultToUnits(profit, unitSize) ?? 0
  const decisions = wins + losses
  return {
    rows: ledger,
    graded_count: graded.length,
    priced_graded_count: priced.length,
    pending_count: ledger.filter(isPending).length,
    wins,
    losses,
    pushes,
    win_rate: decisions ? wins / decisions : null,
    total_risked: risked,
    profit,
    units,
    roi: risked ? profit / risked : null,
  }
}

export function groupProfit(rows = [], groupBy = () => 'Unknown') {
  const map = new Map()
  rows.forEach(row => {
    const key = groupBy(row) || 'Unknown'
    const current = map.get(key) || { label: key, count: 0, profit: 0, units: 0, wins: 0, losses: 0, pushes: 0 }
    current.count += 1
    current.profit += toNumber(row.profit) ?? 0
    current.units += toNumber(row.units) ?? 0
    if (row.grade === 'won') current.wins += 1
    if (row.grade === 'lost') current.losses += 1
    if (row.grade === 'push') current.pushes += 1
    map.set(key, current)
  })
  return Array.from(map.values()).sort((a, b) => b.profit - a.profit)
}

export function dailySeries(rows = []) {
  const map = new Map()
  rows.forEach(row => {
    const date = String(row.snapshot_date || row.date || '').slice(0, 10) || 'Unknown'
    const current = map.get(date) || { date, profit: 0, units: 0, graded: 0, pending: 0, wins: 0, losses: 0 }
    if (row.profit !== null && row.profit !== undefined) {
      current.profit += toNumber(row.profit) ?? 0
      current.units += toNumber(row.units) ?? 0
    }
    if (isGradedDecision(row)) current.graded += 1
    if (isPending(row)) current.pending += 1
    if (row.grade === 'won') current.wins += 1
    if (row.grade === 'lost') current.losses += 1
    map.set(date, current)
  })
  let cumulative = 0
  return Array.from(map.values()).sort((a, b) => a.date.localeCompare(b.date)).map(row => {
    cumulative += row.profit
    const decisions = row.wins + row.losses
    return { ...row, cumulative, win_rate: decisions ? row.wins / decisions : null, roi: row.graded ? row.profit / (row.graded * UNIT_SIZE_DOLLARS) : null }
  })
}

export function maxDrawdown(series = []) {
  let peak = 0
  let worst = 0
  series.forEach(point => {
    const value = toNumber(point.cumulative) ?? 0
    peak = Math.max(peak, value)
    worst = Math.min(worst, value - peak)
  })
  return worst
}

export function comparePeriods(currentRows = [], previousRows = [], unitSize = UNIT_SIZE_DOLLARS) {
  const current = summarizePnl(currentRows, unitSize)
  const previous = summarizePnl(previousRows, unitSize)
  return {
    current,
    previous,
    deltas: {
      profit: current.profit - previous.profit,
      units: current.units - previous.units,
      win_rate: current.win_rate !== null && previous.win_rate !== null ? current.win_rate - previous.win_rate : null,
      roi: current.roi !== null && previous.roi !== null ? current.roi - previous.roi : null,
      graded_count: current.graded_count - previous.graded_count,
      pending_count: current.pending_count - previous.pending_count,
    },
  }
}
