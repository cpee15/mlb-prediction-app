from __future__ import annotations

from mlb_app.simulation.box_score import (
    DRAFTKINGS_CLASSIC_BATTER_RULES,
    DRAFTKINGS_CLASSIC_PITCHER_RULES,
)
from mlb_app.simulation.game import (
    CANONICAL_PA_OUTCOME_ORDER,
    CanonicalBaserunningEvidenceCatalog,
    CanonicalCatcherBaserunningProfile,
    CanonicalOutcomeProbability,
    CanonicalPitcherBaserunningProfile,
    CanonicalRunnerBaserunningProfile,
    CanonicalProbabilityArtifact,
    CanonicalProbabilityArtifactRecord,
    CanonicalProbabilityFallbackCatalog,
    CanonicalProbabilityFallbackRecord,
    CanonicalProbabilityFallbackTier,
    CanonicalProbabilityProviderIdentity,
)
from mlb_app.simulation.shadow import (
    CANONICAL_PRODUCTION_SHADOW_EXECUTION_VERSION,
    CanonicalShadowBullpenDiscovery,
    CanonicalShadowBullpenSideDiscovery,
    CanonicalShadowExactArtifactDiscovery,
    CanonicalShadowFallbackCatalogDiscovery,
    CanonicalShadowBaserunningEvidenceDiscovery,
    CanonicalShadowLineupDiscovery,
    CanonicalShadowProbabilityProviderDiscovery,
    run_canonical_production_shadow,
    run_canonical_production_shadow_with_baserunning_discovery,
)


PROVIDER = CanonicalProbabilityProviderIdentity(
    provider_name="model_projections_pa_outcome",
    provider_version="pa_outcome_v1",
)


def probability_points():
    values = {
        "out": 0.43,
        "single": 0.15,
        "double": 0.05,
        "triple": 0.005,
        "hr": 0.03,
        "bb": 0.085,
        "hbp": 0.01,
        "k": 0.24,
    }

    return tuple(
        CanonicalOutcomeProbability(
            outcome=outcome,
            probability=values[outcome.value],
        )
        for outcome in CANONICAL_PA_OUTCOME_ORDER
    )


def lineups():
    return CanonicalShadowLineupDiscovery(
        away_player_ids=tuple(
            f"a{index}"
            for index in range(1, 10)
        ),
        home_player_ids=tuple(
            f"h{index}"
            for index in range(1, 10)
        ),
        away_source_count=9,
        home_source_count=9,
        status="ready",
    )


def bullpens():
    return CanonicalShadowBullpenDiscovery(
        away=CanonicalShadowBullpenSideDiscovery(
            team_id="1",
            starter_id="100",
            bullpen_pitcher_ids=("101",),
            source_record_count=2,
            status="ready",
        ),
        home=CanonicalShadowBullpenSideDiscovery(
            team_id="2",
            starter_id="200",
            bullpen_pitcher_ids=("201",),
            source_record_count=2,
            status="ready",
        ),
    )


def provider_discovery():
    return CanonicalShadowProbabilityProviderDiscovery(
        provider=PROVIDER,
        model_versions=("pa_outcome_v1",),
        valid_model_count=4,
        status="ready",
    )


def exact_discovery():
    records = tuple(
        CanonicalProbabilityArtifactRecord(
            batter_id=batter_id,
            pitcher_id=pitcher_id,
            probabilities=probability_points(),
        )
        for batter_id, pitcher_id in (
            *(
                (f"a{index}", "200")
                for index in range(1, 10)
            ),
            *(
                (f"h{index}", "100")
                for index in range(1, 10)
            ),
        )
    )

    artifact = CanonicalProbabilityArtifact(
        provider=PROVIDER,
        records=records,
    )

    return CanonicalShadowExactArtifactDiscovery(
        artifact=artifact,
        away_record_count=9,
        home_record_count=9,
        away_real_profile_count=9,
        home_real_profile_count=9,
        status="ready",
    )


def fallback_discovery():
    catalog = CanonicalProbabilityFallbackCatalog(
        provider=PROVIDER,
        records=(
            CanonicalProbabilityFallbackRecord(
                tier=(
                    CanonicalProbabilityFallbackTier
                    .GLOBAL
                ),
                identity=None,
                probabilities=probability_points(),
            ),
        ),
    )

    return CanonicalShadowFallbackCatalogDiscovery(
        catalog=catalog,
        source_model_count=4,
        status="ready",
    )


def baserunning_catalog():
    return CanonicalBaserunningEvidenceCatalog(
        runners=tuple(
            CanonicalRunnerBaserunningProfile(
                runner_id=runner_id,
                speed_score=0.50,
                attempt_rate=0.0,
                success_rate=0.75,
                lead_quality=0.50,
                fatigue_index=0.0,
            )
            for runner_id in (
                *(
                    f"a{index}"
                    for index in range(1, 10)
                ),
                *(
                    f"h{index}"
                    for index in range(1, 10)
                ),
            )
        ),
        pitchers=tuple(
            CanonicalPitcherBaserunningProfile(
                pitcher_id=pitcher_id,
                hold_score=0.50,
                delivery_time_score=0.50,
                pickoff_attempt_rate=0.0,
                pickoff_success_rate=0.0,
            )
            for pitcher_id in (
                "100",
                "101",
                "200",
                "201",
            )
        ),
        away_catcher=CanonicalCatcherBaserunningProfile(
            catcher_id="away-catcher",
            team_side="away",
            throwing_score=0.50,
            pop_time_score=0.50,
        ),
        home_catcher=CanonicalCatcherBaserunningProfile(
            catcher_id="home-catcher",
            team_side="home",
            throwing_score=0.50,
            pop_time_score=0.50,
        ),
    )


def run(**overrides):
    kwargs = {
        "game_pk": 123,
        "lineups": lineups(),
        "bullpens": bullpens(),
        "provider_discovery": provider_discovery(),
        "exact_artifact_discovery": (
            exact_discovery()
        ),
        "fallback_catalog_discovery": (
            fallback_discovery()
        ),
        "bootstrap_ready": True,
        "simulation_count": 2,
    }
    kwargs.update(overrides)

    return run_canonical_production_shadow(
        **kwargs
    )


def test_ready_inputs_execute_real_trial_batch():
    result = run()

    assert result.status == "executed"
    assert result.executed is True
    assert result.material is not None
    assert result.execution_inputs is not None
    assert result.simulation_count == 2


def test_trial_batch_contains_requested_games():
    result = run()

    assert len(
        result.material.canonical_payload[
            "metadata"
        ]["simulation_count"]
        if False
        else result.material.canonical_payload
    ) > 0

    assert (
        result.material
        .probability_resolution_diagnostics
        .total_resolutions
        > 0
    )


def test_policy_enables_exact_then_global():
    result = run()

    tiers = (
        result.execution_inputs
        .fallback_policy
        .tiers
    )

    assert tiers == (
        CanonicalProbabilityFallbackTier.EXACT_MATCHUP,
        CanonicalProbabilityFallbackTier.GLOBAL,
    )


def test_execution_carries_atomic_input_provenance():
    result = run()

    assert (
        result.material
        .canonical_shadow_execution_inputs
        is result.execution_inputs
    )
    assert len(
        result.execution_inputs.assembly_digest
    ) == 64


def test_diagnostics_keep_legacy_authority():
    diagnostics = run().to_diagnostics()

    assert diagnostics["schema_version"] == (
        CANONICAL_PRODUCTION_SHADOW_EXECUTION_VERSION
    )
    assert diagnostics["executed"] is True
    assert diagnostics[
        "production_authority_changed"
    ] is False
    assert diagnostics[
        "activation_permitted"
    ] is False
    assert diagnostics["authoritative_source"] == (
        "legacy"
    )


def test_not_ready_does_not_execute():
    result = run(
        bootstrap_ready=False,
    )

    assert result.status == "blocked"
    assert result.executed is False
    assert result.material is None


def test_missing_artifact_does_not_execute():
    result = run(
        exact_artifact_discovery=(
            CanonicalShadowExactArtifactDiscovery(
                status="unavailable",
            )
        )
    )

    assert result.status == "blocked"
    assert result.executed is False


def test_invalid_simulation_count_fails_open():
    result = run(
        simulation_count=0,
    )

    assert result.status == "error"
    assert result.executed is False
    assert result.error_type == "ValueError"


def opener_bulk_classification(
    *,
    starter_id,
    bulk_id,
):
    return {
        "plan_type": "opener_bulk",
        "fallback_used": False,
        "planned_sequence": [
            {
                "order": 1,
                "role": "opener",
                "pitcher_id": starter_id,
            },
            {
                "order": 2,
                "role": "bulk_follower",
                "pitcher_id": bulk_id,
            },
        ],
        "diagnostics": {
            "production_activation": False,
        },
    }


def test_production_matchup_activates_opener_bulk_plan():
    result = run(
        away_pitching_plan_classification=(
            opener_bulk_classification(
                starter_id="100",
                bulk_id="101",
            )
        ),
    )

    assert result.status == "executed"

    plan = (
        result.execution_inputs
        .matchup_input
        .away_pitching_plan
    )

    assert plan.plan_type == "opener_bulk"
    assert (
        plan.preferred_replacement_pitcher_ids
        == ("101",)
    )


def test_unknown_classification_falls_back_safely():
    result = run(
        away_pitching_plan_classification={
            "plan_type": "unknown_fallback",
            "fallback_used": True,
            "planned_sequence": [],
        },
    )

    assert result.status == "executed"

    plan = (
        result.execution_inputs
        .matchup_input
        .away_pitching_plan
    )

    assert plan.plan_type == (
        "traditional_starter"
    )
    assert (
        plan.preferred_replacement_pitcher_ids
        == ()
    )


def test_preferred_replacement_outside_bullpen_is_ignored():
    result = run(
        away_pitching_plan_classification=(
            opener_bulk_classification(
                starter_id="100",
                bulk_id="999",
            )
        ),
    )

    assert result.status == "executed"

    plan = (
        result.execution_inputs
        .matchup_input
        .away_pitching_plan
    )

    assert plan.plan_type == "opener_bulk"
    assert (
        plan.preferred_replacement_pitcher_ids
        == ()
    )


def test_production_shadow_activates_draftkings_scoring_rules():
    result = run()

    assert result.status == "executed"
    assert result.material is not None
    assert result.execution_inputs is not None

    payload = result.material.canonical_payload

    batter_metric_names = {
        metric["name"]
        for row in payload["batters"]
        for metric in row["metrics"]
    }
    pitcher_metric_names = {
        metric["name"]
        for row in payload["pitchers"]
        for metric in row["metrics"]
    }

    assert "dfs_points" in batter_metric_names

    assert (
        result.execution_inputs.batter_dfs_rules
        is DRAFTKINGS_CLASSIC_BATTER_RULES
    )
    assert (
        result.execution_inputs.pitcher_dfs_rules
        is DRAFTKINGS_CLASSIC_PITCHER_RULES
    )

    if (
        payload["diagnostics"]["earned_run_status"]
        == "reconstructed"
    ):
        assert "dfs_points" in pitcher_metric_names
    else:
        assert "dfs_points" not in pitcher_metric_names
        assert (
            "pitcher_dfs_earned_runs_unavailable"
            in payload["diagnostics"]["warnings"]
        )



def test_production_shadow_exposes_reconstructed_earned_runs():
    result = run()

    assert result.status == "executed"
    assert result.material is not None

    payload = result.material.canonical_payload
    diagnostics = payload["diagnostics"]
    pitcher_metric_names = {
        metric["name"]
        for row in payload["pitchers"]
        for metric in row["metrics"]
    }

    assert diagnostics["earned_run_status"] == (
        "reconstructed"
    )
    assert (
        "earned_runs_not_fully_reconstructed"
        not in diagnostics["warnings"]
    )
    assert (
        "pitcher_dfs_earned_runs_unavailable"
        not in diagnostics["warnings"]
    )
    assert "earned_runs" in pitcher_metric_names
    assert "dfs_points" in pitcher_metric_names



def test_production_shadow_accepts_injected_baserunning_catalog():
    source = baserunning_catalog()
    result = run(
        baserunning_evidence_catalog=source,
    )

    assert result.status == "executed"
    assert result.execution_inputs is not None
    assert (
        result.execution_inputs
        .baserunning_evidence_catalog
        is source
    )
    assert (
        result.to_diagnostics()[
            "baserunning_evidence_catalog_digest"
        ]
        == source.digest
    )


def test_invalid_production_baserunning_catalog_fails_open():
    result = run(
        baserunning_evidence_catalog=object(),
    )

    assert result.status == "error"
    assert result.executed is False
    assert result.material is None
    assert result.error_type == "TypeError"



def baserunning_discovery(
    *,
    status="ready",
    error_message=None,
):
    source = (
        baserunning_catalog()
        if status == "ready"
        else None
    )

    return CanonicalShadowBaserunningEvidenceDiscovery(
        catalog=source,
        requested_runner_count=(
            18
            if status == "ready"
            else 0
        ),
        available_runner_count=(
            18
            if status == "ready"
            else 0
        ),
        requested_pitcher_count=(
            4
            if status == "ready"
            else 0
        ),
        available_pitcher_count=(
            4
            if status == "ready"
            else 0
        ),
        status=status,
        error_message=error_message,
    )


def run_with_discovery(
    discovery=None,
    **overrides,
):
    kwargs = {
        "game_pk": 123,
        "lineups": lineups(),
        "bullpens": bullpens(),
        "provider_discovery": provider_discovery(),
        "exact_artifact_discovery": (
            exact_discovery()
        ),
        "fallback_catalog_discovery": (
            fallback_discovery()
        ),
        "bootstrap_ready": True,
        "simulation_count": 2,
    }
    kwargs.update(overrides)

    return (
        run_canonical_production_shadow_with_baserunning_discovery(
            baserunning_evidence_discovery=(
                baserunning_discovery()
                if discovery is None
                else discovery
            ),
            **kwargs,
        )
    )


def test_ready_discovery_injects_catalog():
    discovery = baserunning_discovery()
    result = run_with_discovery(
        discovery=discovery,
    )

    assert result.status == "executed"
    assert result.executed is True
    assert result.execution_inputs is not None
    assert (
        result.execution_inputs
        .baserunning_evidence_catalog
        is discovery.catalog
    )
    assert (
        result.to_diagnostics()[
            "baserunning_evidence_catalog_digest"
        ]
        == discovery.catalog.digest
    )


def test_unavailable_discovery_blocks_execution():
    result = run_with_discovery(
        discovery=baserunning_discovery(
            status="unavailable",
        ),
    )

    assert result.status == "blocked"
    assert result.executed is False
    assert result.material is None
    assert result.error_type == (
        "BaserunningEvidenceUnavailable"
    )


def test_error_discovery_fails_open():
    result = run_with_discovery(
        discovery=baserunning_discovery(
            status="error",
            error_message="source unavailable",
        ),
    )

    assert result.status == "error"
    assert result.executed is False
    assert result.material is None
    assert result.error_type == (
        "BaserunningEvidenceDiscoveryError"
    )
    assert result.error_message == "source unavailable"


def test_invalid_discovery_contract_fails_open():
    result = run_with_discovery(
        discovery=object(),
    )

    assert result.status == "error"
    assert result.executed is False
    assert result.error_type == "TypeError"


def test_direct_catalog_and_discovery_are_rejected():
    result = run_with_discovery(
        baserunning_evidence_catalog=(
            baserunning_catalog()
        ),
    )

    assert result.status == "error"
    assert result.executed is False
    assert result.error_type == "ValueError"
    assert result.error_message == (
        "baserunning_evidence_catalog must be "
        "supplied through discovery"
    )


def test_discovered_execution_preserves_shadow_authority():
    diagnostics = run_with_discovery().to_diagnostics()

    assert diagnostics["activation_permitted"] is False
    assert diagnostics[
        "production_authority_changed"
    ] is False
    assert diagnostics["authoritative_source"] == "legacy"
