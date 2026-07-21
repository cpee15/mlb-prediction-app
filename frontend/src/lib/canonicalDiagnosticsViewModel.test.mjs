import assert from 'node:assert/strict'
import test from 'node:test'

import {
  CANONICAL_DIAGNOSTICS_VIEW_MODEL_VERSION,
  buildCanonicalDiagnosticsViewModel,
} from './canonicalDiagnosticsViewModel.mjs'

function sharedSimulation() {
  return {
    meta: {
      model_version: 'shared-simulation-v1',
    },
    game_state_realism_diagnostics: {
      base_out_state: true,
      runner_advancement_enabled: true,
      extra_innings: 'enabled',
      ghost_runner_enabled: true,
      walk_off_shortening: true,
      double_play_scoring: true,
      sacrifice_fly_scoring_enabled: true,
      steals_model: 'deferred_not_active',
    },
    diagnostics: {
      canonical_shadow: {
        status: 'complete',
        enabled: true,
        canonical_available: true,
        authoritative_source: 'legacy',
        schema_version: 'canonical_shadow_v1',
        legacy_simulation_count: 3000,
        canonical_simulation_count: 3000,
        pitcher_attribution_complete_rate: 0.98,
        replay_validation_pass_rate: 1,
        coverage: {
          game_validation_pass_rate: 1,
          box_score_reconciliation_pass_rate: 1,
        },
        warnings: [
          'legacy_authority_retained',
        ],
        probability_resolution: {
          schema_version:
            'canonical_probability_diagnostics_shadow_v1',
          diagnostics_version:
            'canonical_probability_diagnostics_v1',
          summary: {
            total_resolutions: 100,
            exact_resolutions: 81,
            fallback_resolutions: 19,
            fallback_rate: 0.19,
          },
          tier_usage: [
            {
              tier: 'exact_matchup',
              count: 81,
            },
            {
              tier: 'batter',
              count: 10,
            },
            {
              tier: 'pitcher',
              count: 5,
            },
            {
              tier: 'global',
              count: 4,
            },
          ],
        },
        input_provenance: {
          schema_version:
            'canonical_shadow_input_provenance_v1',
          assembly_version:
            'canonical_shadow_input_assembly_v1',
          assembly_digest: 'a'.repeat(64),
          matchup: {
            game_pk: 123,
          },
          probability_provider: {
            identity: 'provider:v1:artifact-123',
            provider_name: 'provider',
            provider_version: 'v1',
            artifact_id: 'artifact-123',
          },
          artifacts: {
            exact: {
              artifact_version:
                'canonical_probability_artifact_v1',
              digest: 'b'.repeat(64),
              record_count: 50,
            },
            fallback_catalog: {
              schema_version:
                'canonical_probability_fallback_v1',
              digest: 'c'.repeat(64),
              record_count: 12,
            },
          },
          fallback_policy: {
            policy_version:
              'canonical_probability_fallback_v1',
            tiers: [
              'exact_matchup',
              'batter',
              'pitcher',
              'global',
            ],
          },
          game_config: {
            regulation_innings: 9,
            max_extra_innings: 6,
            automatic_runner_enabled: true,
          },
          dfs_rules: {
            batter_rules_supplied: false,
            pitcher_rules_supplied: false,
          },
          probability_records_exposed: false,
          authoritative_source: 'legacy',
        },
      },
    },
  }
}

test('returns a stable empty view model', () => {
  const view = buildCanonicalDiagnosticsViewModel()

  assert.equal(
    view.viewModelVersion,
    CANONICAL_DIAGNOSTICS_VIEW_MODEL_VERSION,
  )
  assert.equal(view.hasCanonicalShadow, false)
  assert.equal(view.status.state, 'not_run')
  assert.equal(
    view.status.authoritativeSource,
    'legacy',
  )
  assert.match(
    view.status.availabilityReason,
    /not attached/,
  )
  assert.deepEqual(view.warnings, [])
})

test('normalizes canonical status', () => {
  const view = buildCanonicalDiagnosticsViewModel(
    sharedSimulation(),
  )

  assert.equal(view.hasCanonicalShadow, true)
  assert.equal(view.status.state, 'complete')
  assert.equal(view.status.enabled, true)
  assert.equal(
    view.status.canonicalAvailable,
    true,
  )
  assert.equal(
    view.status.canonicalSimulationCount,
    3000,
  )
  assert.equal(
    view.status.authoritativeSource,
    'legacy',
  )
})

test('normalizes probability coverage', () => {
  const view = buildCanonicalDiagnosticsViewModel(
    sharedSimulation(),
  )

  assert.equal(view.coverage.available, true)
  assert.equal(
    view.coverage.totalResolutions,
    100,
  )
  assert.equal(view.coverage.exactRate, 0.81)
  assert.equal(
    view.coverage.fallbackRate,
    0.19,
  )
})

test('preserves ordered fallback tiers', () => {
  const view = buildCanonicalDiagnosticsViewModel(
    sharedSimulation(),
  )

  assert.deepEqual(
    view.coverage.tiers.map(item => item.tier),
    [
      'exact_matchup',
      'batter',
      'pitcher',
      'global',
    ],
  )
})

test('normalizes simulation integrity', () => {
  const view = buildCanonicalDiagnosticsViewModel(
    sharedSimulation(),
  )

  const values = Object.fromEntries(
    view.integrity.metrics.map(metric => [
      metric.key,
      metric.value,
    ]),
  )

  assert.equal(
    values.game_validation,
    1,
  )
  assert.equal(
    values.box_score_reconciliation,
    1,
  )
  assert.equal(
    values.replay_validation,
    1,
  )
  assert.equal(
    values.pitcher_attribution,
    0.98,
  )
})

test('normalizes atomic input provenance', () => {
  const view = buildCanonicalDiagnosticsViewModel(
    sharedSimulation(),
  )

  assert.equal(view.provenance.available, true)
  assert.equal(view.provenance.gamePk, 123)
  assert.equal(
    view.provenance.provider.name,
    'provider',
  )
  assert.equal(
    view.provenance.exactArtifact.recordCount,
    50,
  )
  assert.equal(
    view.provenance.fallbackCatalog.recordCount,
    12,
  )
  assert.equal(
    view.provenance.probabilityRecordsExposed,
    false,
  )
})

test('normalizes realism feature aliases and states', () => {
  const view = buildCanonicalDiagnosticsViewModel(
    sharedSimulation(),
  )

  const features = Object.fromEntries(
    view.realism.features.map(feature => [
      feature.key,
      feature.status,
    ]),
  )

  assert.equal(
    features.base_out_state,
    'enabled',
  )
  assert.equal(
    features.runner_advancement,
    'enabled',
  )
  assert.equal(
    features.automatic_runner,
    'enabled',
  )
  assert.equal(
    features.multi_out_scoring,
    'enabled',
  )
  assert.equal(
    features.stolen_bases,
    'deferred',
  )
})

test('collects and deduplicates warnings', () => {
  const payload = sharedSimulation()

  payload.diagnostics.canonical_shadow.warnings = [
    'legacy_authority_retained',
    'legacy_authority_retained',
  ]

  payload.diagnostics.canonical_shadow
    .probability_resolution = {
      status: 'error',
      error_message: 'probability diagnostics failed',
    }

  const view = buildCanonicalDiagnosticsViewModel(
    payload,
  )

  assert.deepEqual(view.warnings, [
    'legacy_authority_retained',
    'probability diagnostics failed',
  ])
})

test('does not mutate the source payload', () => {
  const payload = sharedSimulation()
  const snapshot = structuredClone(payload)

  buildCanonicalDiagnosticsViewModel(payload)

  assert.deepEqual(payload, snapshot)
})
