import assert from 'node:assert/strict'
import test from 'node:test'

import {
  buildCanonicalProjectionsViewModel,
} from './canonicalProjectionsViewModel.mjs'

function metric(mean, overrides = {}) {
  return {
    mean,
    median: mean,
    p10: mean - 1,
    p90: mean + 1,
    ...overrides,
  }
}

function payload() {
  return {
    diagnostics: {
      canonical_shadow: {
        authoritative_source: 'legacy',
        player_projections: {
          schema_version: (
            'canonical_player_projection_rows_v1'
          ),
          source_projection_schema_version: (
            'canonical_projection_payload_v1'
          ),
          run_id: 'run-123',
          model_version: 'canonical-v1',
          simulation_count: 25,
          identity_enrichment_applied: false,
          authoritative: false,
          authoritative_source: 'legacy',
          players: [
            {
              player_id: 'b-1',
              full_name: 'Test Batter',
              team_side: 'away',
              player_type: 'batter',
              projected_dfs_points: 10.5,
              dfs_floor: 3,
              dfs_median: 9,
              dfs_ceiling: 20,
              metrics: {
                plate_appearances: metric(4.4),
                singles: metric(0.7),
                doubles: metric(0.3),
                triples: metric(0.1),
                home_runs: metric(0.4),
                runs: metric(0.8),
                rbis: metric(1.1),
                walks: metric(0.5),
                stolen_bases: metric(0.2),
                strikeouts: metric(1.2),
              },
            },
            {
              player_id: 'p-1',
              player_type: 'pitcher',
              team_side: 'home',
              metrics: {
                batters_faced: metric(24),
                outs: metric(18),
                hits_allowed: metric(5),
                walks: metric(2),
                hit_by_pitch: metric(0.3),
                strikeouts: metric(7),
                runs_allowed: metric(2.5),
                earned_runs: metric(2),
                dfs_points: metric(
                  18,
                  {
                    p10: 8,
                    median: 17,
                    p90: 27,
                  },
                ),
              },
            },
          ],
        },
      },
    },
  }
}

test('builds same-run projection metadata', () => {
  const view = (
    buildCanonicalProjectionsViewModel(
      payload(),
    )
  )

  assert.equal(view.available, true)
  assert.equal(view.runId, 'run-123')
  assert.equal(view.modelVersion, 'canonical-v1')
  assert.equal(view.simulationCount, 25)
  assert.equal(view.authoritative, false)
  assert.equal(
    view.authoritativeSource,
    'legacy',
  )
})

test('derives batter hits from component means', () => {
  const view = (
    buildCanonicalProjectionsViewModel(
      payload(),
    )
  )
  const batter = view.batters[0]

  assert.equal(batter.name, 'Test Batter')
  assert.equal(batter.plateAppearances, 4.4)
  assert.equal(batter.hits, 1.5)
  assert.equal(batter.stolenBases, 0.2)
  assert.equal(batter.dfsMean, 10.5)
  assert.equal(batter.dfsFloor, 3)
  assert.equal(batter.dfsMedian, 9)
  assert.equal(batter.dfsCeiling, 20)
})

test('derives pitcher innings from outs', () => {
  const view = (
    buildCanonicalProjectionsViewModel(
      payload(),
    )
  )
  const pitcher = view.pitchers[0]

  assert.equal(pitcher.name, 'p-1')
  assert.equal(pitcher.inningsPitched, 6)
  assert.equal(pitcher.battersFaced, 24)
  assert.equal(pitcher.dfsMean, 18)
  assert.equal(pitcher.dfsFloor, 8)
  assert.equal(pitcher.dfsMedian, 17)
  assert.equal(pitcher.dfsCeiling, 27)
})

test('returns unavailable view without rows', () => {
  const view = (
    buildCanonicalProjectionsViewModel({})
  )

  assert.equal(view.available, false)
  assert.deepEqual(view.batters, [])
  assert.deepEqual(view.pitchers, [])
})

test('leaves stolen bases unavailable when simulation omits metric', () => {
  const source = payload()
  delete (
    source
      .diagnostics
      .canonical_shadow
      .player_projections
      .players[0]
      .metrics
      .stolen_bases
  )

  const view = (
    buildCanonicalProjectionsViewModel(source)
  )

  assert.equal(
    view.batters[0].stolenBases,
    null,
  )
})
