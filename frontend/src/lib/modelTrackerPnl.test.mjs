import assert from 'node:assert/strict'
import test from 'node:test'

import {
  americanOddsToProfit,
  buildPeriodComparison,
  comparePeriods,
  confidenceBand,
  dailySeries,
  edgeBand,
  getPeriodWindows,
  groupRowsByGame,
  highestConfidenceRows,
  maxDrawdown,
  periodDateKeys,
  productionBucketLabel,
  recommendationBucket,
  summarizePnl,
} from './modelTrackerPnl.mjs'

test('americanOddsToProfit handles positive odds wins', () => {
  assert.equal(americanOddsToProfit(100, 150, 'won'), 150)
})

test('americanOddsToProfit handles negative odds wins', () => {
  assert.equal(americanOddsToProfit(100, -200, 'won'), 50)
})

test('americanOddsToProfit handles losses and pushes', () => {
  assert.equal(americanOddsToProfit(100, -110, 'lost'), -100)
  assert.equal(americanOddsToProfit(100, -110, 'push'), 0)
})

test('confidenceBand and edgeBand bucket values deterministically', () => {
  assert.equal(confidenceBand(0.57), '.55-.59')
  assert.equal(confidenceBand(0.66), '.65+')
  assert.equal(edgeBand(0.073), '+5% to +9.9%')
  assert.equal(edgeBand(-0.01), 'Negative')
})

test('summarizePnl excludes pending rows from realized P&L', () => {
  const summary = summarizePnl([
    { grade: 'won', price: 100, confidence: 0.57, edge: 0.03, pick_label: 'Cubs ML' },
    { grade: 'lost', price: -110, confidence: 0.52, pick_label: 'Yankees TT over' },
    { grade: 'push', price: -110, confidence: 0.51, pick_label: 'Dodgers total' },
    { grade: 'pending', price: -120, confidence: 0.60, pick_label: 'Mets ML' },
  ])
  assert.equal(summary.graded_count, 3)
  assert.equal(summary.pending_count, 1)
  assert.equal(summary.total_risked, 200)
  assert.equal(summary.profit, 0)
  assert.equal(summary.roi, 0)
})

test('recommendationBucket keeps rejected/no-bet separate', () => {
  assert.equal(recommendationBucket({ pick_label: 'No bet', recommendation_status: 'no_bet', confidence: 0.70 }), 'rejected')
  assert.equal(recommendationBucket({ pick_label: 'Strong play', confidence: 0.56 }), 'recommended')
  assert.equal(recommendationBucket({ pick_label: 'Watchlist', confidence: 0.51, grade: 'watchlist_only' }), 'lean')
})

test('production bucket labels use production language', () => {
  assert.equal(productionBucketLabel('recommended'), 'Recommendation')
  assert.equal(productionBucketLabel('lean'), 'Lean')
  assert.equal(productionBucketLabel('rejected'), 'Low Confidence / No Play')
  assert.equal(productionBucketLabel('missing_data'), 'Low Confidence / No Play')
})

test('dailySeries and maxDrawdown are ordered by date', () => {
  const summary = summarizePnl([
    { snapshot_date: '2026-06-02', grade: 'lost', price: -110, pick_label: 'A' },
    { snapshot_date: '2026-06-01', grade: 'won', price: 100, pick_label: 'B' },
    { snapshot_date: '2026-06-03', grade: 'lost', price: -110, pick_label: 'C' },
  ])
  const series = dailySeries(summary.rows)
  assert.deepEqual(series.map(point => point.date), ['2026-06-01', '2026-06-02', '2026-06-03'])
  assert.equal(series.at(-1).cumulative, -100)
  assert.equal(maxDrawdown(series), -200)
})

test('comparePeriods returns current, previous, and deltas', () => {
  const comparison = comparePeriods(
    [{ grade: 'won', price: 100, pick_label: 'Current' }],
    [{ grade: 'lost', price: -110, pick_label: 'Previous' }],
  )
  assert.equal(comparison.current.profit, 100)
  assert.equal(comparison.previous.profit, -100)
  assert.equal(comparison.deltas.profit, 200)
})

test('groupRowsByGame creates production buckets per game', () => {
  const games = [{ game_pk: 1, away_team: 'Cubs', home_team: 'Brewers', game_time: '2026-06-16T18:10:00Z' }]
  const grouped = groupRowsByGame([
    { game_pk: 1, away_team: 'Cubs', home_team: 'Brewers', pick_label: 'Cubs ML', confidence: 0.61, edge: 0.05, price: 120 },
    { game_pk: 1, away_team: 'Cubs', home_team: 'Brewers', pick_label: 'Brewers F5 lean', confidence: 0.51, grade: 'watchlist_only' },
    { game_pk: 1, away_team: 'Cubs', home_team: 'Brewers', pick_label: 'No play total', recommendation_status: 'no_bet', confidence: 0.70 },
  ], games)
  assert.equal(grouped.length, 1)
  assert.equal(grouped[0].label, 'Cubs @ Brewers')
  assert.equal(grouped[0].buckets.recommended.length, 1)
  assert.equal(grouped[0].buckets.lean.length, 1)
  assert.equal(grouped[0].buckets.low_confidence.length, 1)
  assert.equal(grouped[0].price_count, 1)
})

test('highestConfidenceRows sorts by confidence, edge, expected value, then price availability', () => {
  const rows = highestConfidenceRows([
    { pick_label: 'Lower score', confidence: 0.56, edge: 0.30, expected_value: 2, price: 100 },
    { pick_label: 'Top score', confidence: 0.62, edge: 0.01, expected_value: 1 },
    { pick_label: 'No play', recommendation_status: 'no_bet', confidence: 0.99 },
  ])
  assert.deepEqual(rows.map(row => row.pick_label), ['Top score', 'Lower score'])
})

test('period windows generate correct DoD, WoW, and MoM ranges', () => {
  assert.deepEqual(getPeriodWindows('dod', '2026-06-16').previous, { start: '2026-06-15', end: '2026-06-15', label: '2026-06-15' })
  assert.deepEqual(getPeriodWindows('wow', '2026-06-16').current, { start: '2026-06-10', end: '2026-06-16', label: '2026-06-10 to 2026-06-16' })
  assert.deepEqual(getPeriodWindows('mom', '2026-06-16').previous, { start: '2026-05-01', end: '2026-05-16', label: '2026-05-01 to 2026-05-16' })
})

test('periodDateKeys includes current and previous comparison dates', () => {
  const keys = periodDateKeys('dod', '2026-06-16')
  assert.deepEqual(keys, ['2026-06-16', '2026-06-15'])
})

test('buildPeriodComparison uses previous rows when prior-day data exists', () => {
  const result = buildPeriodComparison([
    { snapshot_date: '2026-06-16', grade: 'won', price: 100, pick_label: 'Today' },
    { snapshot_date: '2026-06-15', grade: 'lost', price: -110, pick_label: 'Yesterday' },
  ], 'dod', '2026-06-16')
  assert.equal(result.currentRows.length, 1)
  assert.equal(result.previousRows.length, 1)
  assert.equal(result.comparison.current.profit, 100)
  assert.equal(result.comparison.previous.profit, -100)
  assert.equal(result.comparison.deltas.profit, 200)
})
