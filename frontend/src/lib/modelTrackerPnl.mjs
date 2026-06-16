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

export function isGradedDecision(row) { return ['won', 'lost', 'push'].includes(String(row?.grade || '').toLowerCase()) }
export function isPending(row) { const grade = String(row?.grade || '').toLowerCase(); const status = String(row?.result_status || '').toLowerCase(); return grade === 'pending' || status === 'pending' || status === 'live' }
export function normalizeStatus(value) { return String(value || '').trim().toLowerCase().replace(/\s+/g, '_') }
function probabilityCandidate(value) { const n = toNumber(value); return n !== null && n >= 0 && n <= 1 ? n : null }
export function numericScore(row) { const candidates = [row?.confidence, row?.score, row?.model_probability].map(probabilityCandidate).filter(v => v !== null); return candidates.length ? Math.max(...candidates) : null }
export function projectionValue(row) { const score = toNumber(row?.score); if (score !== null && score > 1) return score; return toNumber(row?.projection) ?? toNumber(row?.projected_total) ?? toNumber(row?.raw_payload?.projected_total) ?? toNumber(row?.raw_payload?.projection) }
export function signalValue(row) { return numericScore(row) ?? projectionValue(row) ?? toNumber(row?.expected_value) ?? toNumber(row?.edge) }

export function confidenceBand(value) {
  const n = probabilityCandidate(value)
  if (n === null) return null
  if (n >= 0.65) return '.65+'
  if (n >= 0.60) return '.60-.64'
  if (n >= 0.55) return '.55-.59'
  if (n >= 0.50) return '.50-.54'
  return '<.50'
}

export function edgeBand(value) {
  const n = toNumber(value)
  if (n === null) return null
  if (n >= 0.10) return '+10%+'
  if (n >= 0.05) return '+5% to +9.9%'
  if (n > 0) return '+0% to +4.9%'
  if (n === 0) return '0%'
  return 'Negative'
}

export function getRecommendationStatus(row) { const diagnostics = row?.daily_odds_diagnostics || row?.raw_payload?.daily_odds_diagnostics || row?.raw_payload?.diagnostics || {}; return normalizeStatus(diagnostics.recommendation_status || row?.recommendation_status || row?.bucket || row?.status) }
export function isRejectedNoBet(row) { const status = getRecommendationStatus(row); const reason = normalizeStatus(row?.grade_reason || row?.primary_reason); return ['no_bet', 'rejected', 'suppressed', 'non_positive_edge'].some(token => status.includes(token) || reason.includes(token)) }
export function rowHasPrice(row) { return toNumber(row?.price) !== null || toNumber(row?.provider_price) !== null || toNumber(row?.latest_price) !== null || toNumber(row?.best_price_seen) !== null }
export function effectivePrice(row) { return toNumber(row?.price) ?? toNumber(row?.provider_price) ?? toNumber(row?.latest_price) ?? toNumber(row?.best_price_seen) }
export function hasNegativeEconomics(row) { const edge = toNumber(row?.edge); const ev = toNumber(row?.expected_value); return (edge !== null && edge < 0) || (ev !== null && ev < 0) }
export function hasPositiveEconomics(row) { const edge = toNumber(row?.edge); const ev = toNumber(row?.expected_value); return (edge !== null && edge > 0) || (ev !== null && ev > 0) }

export function economicsCoverage(row) {
  const edge = toNumber(row?.edge)
  const ev = toNumber(row?.expected_value)
  const price = effectivePrice(row)
  if (edge === null && ev === null && price === null) return 'No market economics'
  if (edge === null && ev === null) return 'Priced, no EV/edge'
  if (price === null) return 'EV/edge, no price'
  return 'Priced with EV/edge'
}

export function decisionQualityLabel(row) {
  const edge = toNumber(row?.edge)
  const ev = toNumber(row?.expected_value)
  if ((edge !== null && edge < 0) || (ev !== null && ev < 0)) return 'Negative EV / edge'
  if ((edge !== null && edge > 0) || (ev !== null && ev > 0)) return rowHasPrice(row) ? 'Positive EV priced' : 'Positive EV unpriced'
  if (projectionValue(row) !== null) return 'Projection only'
  if (numericScore(row) !== null) return 'Probability only'
  return 'Insufficient economics'
}

export function modelDecision(row) {
  const score = numericScore(row)
  const projection = projectionValue(row)
  const edge = toNumber(row?.edge)
  const ev = toNumber(row?.expected_value)
  const hasPrice = rowHasPrice(row)
  const status = getRecommendationStatus(row)
  const hasIdentity = Boolean(row?.pick_label || row?.player_name || row?.team_name || row?.model_name)
  const hasMetrics = [score, projection, edge, ev].some(v => v !== null)

  if (!hasIdentity && !hasMetrics) return { bucket: 'missing_data', label: 'Incomplete', posture: 'No decision', action: 'Hold until the model output has a selection and usable market context.' }
  if (isRejectedNoBet(row) || hasNegativeEconomics(row)) {
    const reason = edge !== null && edge < 0 ? `negative edge ${edge}` : ev !== null && ev < 0 ? `negative EV ${ev}` : 'model marked no play'
    return { bucket: 'rejected', label: 'No Play', posture: 'Avoid', action: `Pass. ${reason}; this should not be surfaced as a recommendation.` }
  }
  if (projection !== null && !hasPrice && edge === null && ev === null) return { bucket: 'lean', label: 'Model Projection', posture: 'Market intelligence', action: 'Use as the current model number. Wait for a price or market line before action.' }
  if (hasPositiveEconomics(row) && hasPrice && (score === null || score >= 0.52)) return { bucket: 'recommended', label: 'Recommendation', posture: 'Actionable', action: 'Positive economics with market price available. Candidate for the primary card.' }
  if (hasPositiveEconomics(row) && !hasPrice) return { bucket: 'lean', label: 'Lean / Watch', posture: 'Price needed', action: 'Good directional signal, but hold until a usable market price is available.' }
  if (score !== null && score >= 0.57 && hasPrice && status.includes('recommended')) return { bucket: 'lean', label: 'Lean / Watch', posture: 'Needs edge confirmation', action: 'Probability is strong, but edge/EV is missing. Keep it as a watch item until economics are confirmed.' }
  if (projection !== null || score !== null || status.includes('watch')) return { bucket: 'lean', label: projection !== null ? 'Model Projection' : 'Lean / Watch', posture: 'Monitor', action: projection !== null ? 'Model number is available; compare against market before action.' : 'Directional signal only. Keep on watchlist.' }
  return { bucket: 'missing_data', label: 'No Play', posture: 'No decision', action: 'Not enough usable signal to recommend.' }
}

export function recommendationBucket(row) { return modelDecision(row).bucket }
export function recommendationLabel(bucket) { return { recommended: 'Recommended', lean: 'Lean / Watchlist', rejected: 'Rejected / No Bet', missing_data: 'Missing Data / Ungraded' }[bucket] || 'Missing Data / Ungraded' }
export function productionBucketLabel(bucket) { return { recommended: 'Recommendation', lean: 'Lean', rejected: 'Low Confidence / No Play', missing_data: 'Low Confidence / No Play', low_confidence: 'Low Confidence / No Play' }[bucket] || 'Low Confidence / No Play' }
export function productionBucket(row) { const bucket = row?.bucket || recommendationBucket(row); return bucket === 'recommended' || bucket === 'lean' ? bucket : 'low_confidence' }

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
    return { ...row, bucket: recommendationBucket(row), stake, price_for_pnl: effectivePrice(row), profit, units: resultToUnits(profit, stake), confidence_band: confidenceBand(row?.confidence ?? row?.score ?? row?.model_probability), edge_band: edgeBand(row?.edge), decision_quality: decisionQualityLabel(row), economics_coverage: economicsCoverage(row) }
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
  const evRows = ledger.filter(row => toNumber(row.edge) !== null || toNumber(row.expected_value) !== null)
  const pricedRows = ledger.filter(row => rowHasPrice(row))
  return { rows: ledger, graded_count: graded.length, priced_graded_count: priced.length, pending_count: ledger.filter(isPending).length, wins, losses, pushes, win_rate: decisions ? wins / decisions : null, total_risked: risked, profit, units, roi: risked ? profit / risked : null, ev_coverage: ledger.length ? evRows.length / ledger.length : null, price_coverage: ledger.length ? pricedRows.length / ledger.length : null }
}

export function groupProfit(rows = [], groupBy = () => 'Unknown') {
  const map = new Map()
  rows.forEach(row => {
    const key = groupBy(row)
    if (key === null || key === undefined || key === '' || key === 'Unavailable') return
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
    if (row.profit !== null && row.profit !== undefined) { current.profit += toNumber(row.profit) ?? 0; current.units += toNumber(row.units) ?? 0 }
    if (isGradedDecision(row)) current.graded += 1
    if (isPending(row)) current.pending += 1
    if (row.grade === 'won') current.wins += 1
    if (row.grade === 'lost') current.losses += 1
    map.set(date, current)
  })
  let cumulative = 0
  return Array.from(map.values()).sort((a, b) => a.date.localeCompare(b.date)).map(row => { cumulative += row.profit; const decisions = row.wins + row.losses; return { ...row, cumulative, win_rate: decisions ? row.wins / decisions : null, roi: row.graded ? row.profit / (row.graded * UNIT_SIZE_DOLLARS) : null } })
}

export function maxDrawdown(series = []) { let peak = 0; let worst = 0; series.forEach(point => { const value = toNumber(point.cumulative) ?? 0; peak = Math.max(peak, value); worst = Math.min(worst, value - peak) }); return worst }
export function comparePeriods(currentRows = [], previousRows = [], unitSize = UNIT_SIZE_DOLLARS) { const current = summarizePnl(currentRows, unitSize); const previous = summarizePnl(previousRows, unitSize); return { current, previous, deltas: { profit: current.profit - previous.profit, units: current.units - previous.units, win_rate: current.win_rate !== null && previous.win_rate !== null ? current.win_rate - previous.win_rate : null, roi: current.roi !== null && previous.roi !== null ? current.roi - previous.roi : null, graded_count: current.graded_count - previous.graded_count, pending_count: current.pending_count - previous.pending_count } } }

function isoDate(date) { return date.toISOString().slice(0, 10) }
function parseDate(value) { const [year, month, day] = String(value).slice(0, 10).split('-').map(Number); return new Date(Date.UTC(year, month - 1, day)) }
function addDays(value, days) { const date = parseDate(value); date.setUTCDate(date.getUTCDate() + days); return isoDate(date) }
function startOfMonth(value) { const date = parseDate(value); return isoDate(new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), 1))) }
function endOfPreviousMonth(value) { const date = parseDate(value); return isoDate(new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), 0))) }
function startOfPreviousMonth(value) { const date = parseDate(value); return isoDate(new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth() - 1, 1))) }
export function listDatesBetween(startDate, endDate) { const dates = []; let cursor = startDate; while (cursor <= endDate) { dates.push(cursor); cursor = addDays(cursor, 1) } return dates }
export function getPeriodWindows(period, selectedDate) { const date = String(selectedDate).slice(0, 10); if (period === 'dod') return { label: 'DoD', current: { start: date, end: date, label: date }, previous: { start: addDays(date, -1), end: addDays(date, -1), label: addDays(date, -1) } }; if (period === 'wow' || period === 'rolling7') { const currentStart = addDays(date, -6); const previousEnd = addDays(currentStart, -1); return { label: period === 'wow' ? 'WoW' : 'Rolling 7', current: { start: currentStart, end: date, label: `${currentStart} to ${date}` }, previous: { start: addDays(previousEnd, -6), end: previousEnd, label: `${addDays(previousEnd, -6)} to ${previousEnd}` } } }; if (period === 'mom') { const currentStart = startOfMonth(date); const previousStart = startOfPreviousMonth(date); const daysIntoMonth = listDatesBetween(currentStart, date).length - 1; const previousEnd = addDays(previousStart, daysIntoMonth); const previousMonthEnd = endOfPreviousMonth(date); return { label: 'MoM', current: { start: currentStart, end: date, label: `${currentStart} to ${date}` }, previous: { start: previousStart, end: previousEnd > previousMonthEnd ? previousMonthEnd : previousEnd, label: `${previousStart} to ${previousEnd > previousMonthEnd ? previousMonthEnd : previousEnd}` } } }; if (period === 'rolling30') { const currentStart = addDays(date, -29); const previousEnd = addDays(currentStart, -1); return { label: 'Rolling 30', current: { start: currentStart, end: date, label: `${currentStart} to ${date}` }, previous: { start: addDays(previousEnd, -29), end: previousEnd, label: `${addDays(previousEnd, -29)} to ${previousEnd}` } } }; const seasonStart = `${date.slice(0, 4)}-03-01`; return { label: 'Season', current: { start: seasonStart, end: date, label: `${seasonStart} to ${date}` }, previous: null } }
export function periodDateKeys(period, selectedDate) { const windows = getPeriodWindows(period, selectedDate); const keys = listDatesBetween(windows.current.start, windows.current.end); if (windows.previous) keys.push(...listDatesBetween(windows.previous.start, windows.previous.end)); return Array.from(new Set(keys)) }
export function rowsInRange(rows, range) { if (!range) return []; return rows.filter(row => { const rowDate = String(row.snapshot_date || row.date || '').slice(0, 10); return rowDate >= range.start && rowDate <= range.end }) }
export function buildPeriodComparison(rows = [], period, selectedDate, unitSize = UNIT_SIZE_DOLLARS) { const windows = getPeriodWindows(period, selectedDate); const currentRows = rowsInRange(rows, windows.current); const previousRows = rowsInRange(rows, windows.previous); const comparison = comparePeriods(currentRows, previousRows, unitSize); return { windows, currentRows, previousRows, comparison, currentSeries: indexSeries(dailySeries(summarizePnl(currentRows, unitSize).rows.filter(row => row.profit !== null)), 'current'), previousSeries: indexSeries(dailySeries(summarizePnl(previousRows, unitSize).rows.filter(row => row.profit !== null)), 'previous') } }
export function indexSeries(series = [], prefix = 'current') { return series.map((point, index) => ({ ...point, day_index: index + 1, [`${prefix}_cumulative`]: point.cumulative, [`${prefix}_profit`]: point.profit })) }
export function playRank(row) { const decision = modelDecision(row); return { bucket: decision.bucket === 'recommended' ? 3 : decision.bucket === 'lean' ? 2 : 1, projection: projectionValue(row) ?? -Infinity, score: numericScore(row) ?? -Infinity, edge: toNumber(row?.edge) ?? -Infinity, expected_value: toNumber(row?.expected_value) ?? -Infinity, has_price: rowHasPrice(row) ? 1 : 0 } }
export function comparePlayRank(a, b) { const ar = playRank(a); const br = playRank(b); return (br.bucket - ar.bucket) || (br.score - ar.score) || (br.edge - ar.edge) || (br.expected_value - ar.expected_value) || (br.projection - ar.projection) || (br.has_price - ar.has_price) }
export function compareModelOutputRank(a, b) { const ar = playRank(a); const br = playRank(b); return (br.bucket - ar.bucket) || (br.projection - ar.projection) || (br.score - ar.score) || (br.edge - ar.edge) || (br.expected_value - ar.expected_value) || (br.has_price - ar.has_price) }
function normalizeOutputName(row) { return String(row?.pick_label || row?.player_name || row?.team_name || row?.model_name || '').toLowerCase().replace(/\s+/g, ' ').replace(/\d+\.\d+/g, '#').trim() }
export function modelOutputKey(row) { return [row?.game_pk || 'ungrouped', row?.away_team || '', row?.home_team || '', row?.market_type || row?.pick_type || '', normalizeOutputName(row), toNumber(row?.line) ?? ''].join('|').toLowerCase() }
export function uniqueModelOutputs(rows = []) { const map = new Map(); rows.forEach(row => { const key = modelOutputKey(row); const current = map.get(key); if (!current || compareModelOutputRank(row, current) < 0) map.set(key, row) }); return Array.from(map.values()) }
export function gameFilterValue(value) { return String(value || 'ungrouped') }
export function gameLabel(row) { if (row?.away_team || row?.home_team) return `${row.away_team || 'Away'} @ ${row.home_team || 'Home'}`; return row?.game_pk ? `Game ${row.game_pk}` : 'Ungrouped' }
export function groupRowsByGame(rows = [], games = []) { const map = new Map(); games.forEach(game => { const key = gameFilterValue(game.game_pk); map.set(key, { ...game, key, label: gameLabel(game), rows: [], buckets: { recommended: [], lean: [], low_confidence: [] }, sources: [], best_score: null, best_projection: null, best_edge: null, price_count: 0 }) }); uniqueModelOutputs(rows).forEach(row => { const key = gameFilterValue(row.game_pk); const game = map.get(key) || { key, game_pk: row.game_pk, away_team: row.away_team, home_team: row.home_team, game_time: row.game_time || row.start_time || row.game_datetime, game_status: row.game_status || row.status, label: gameLabel(row), rows: [], buckets: { recommended: [], lean: [], low_confidence: [] }, sources: [], best_score: null, best_projection: null, best_edge: null, price_count: 0 }; const bucket = productionBucket(row); const score = numericScore(row); const projection = projectionValue(row); const edge = toNumber(row.edge); game.rows.push(row); game.buckets[bucket].push(row); if (row.source && !game.sources.includes(row.source)) game.sources.push(row.source); if (score !== null) game.best_score = game.best_score === null ? score : Math.max(game.best_score, score); if (projection !== null) game.best_projection = game.best_projection === null ? projection : Math.max(game.best_projection, projection); if (edge !== null) game.best_edge = game.best_edge === null ? edge : Math.max(game.best_edge, edge); if (rowHasPrice(row)) game.price_count += 1; map.set(key, game) }); return Array.from(map.values()).filter(game => game.rows.length > 0).map(game => ({ ...game, buckets: { recommended: game.buckets.recommended.sort(comparePlayRank), lean: game.buckets.lean.sort(compareModelOutputRank), low_confidence: game.buckets.low_confidence.sort(comparePlayRank) }, rows: game.rows.sort(compareModelOutputRank) })).sort((a, b) => String(a.game_time || '').localeCompare(String(b.game_time || '')) || a.label.localeCompare(b.label)) }
export function highestConfidenceRows(rows = []) { return uniqueModelOutputs(rows).filter(row => modelDecision(row).bucket === 'recommended').sort(comparePlayRank) }
export function topModelProjectionRows(rows = []) { return uniqueModelOutputs(rows).filter(row => !['rejected', 'missing_data'].includes(modelDecision(row).bucket)).filter(row => projectionValue(row) !== null || numericScore(row) !== null || hasPositiveEconomics(row)).sort(compareModelOutputRank) }
