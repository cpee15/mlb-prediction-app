import assert from 'node:assert/strict'
import test from 'node:test'

import {
  americanOddsToProfit,
  comparePeriods,
  confidenceBand,
  dailySeries,
  edgeBand,
  maxDrawdown,
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
