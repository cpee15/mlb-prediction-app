from __future__ import annotations

from mlb_app.simulation.game import (
    CANONICAL_PA_OUTCOME_ORDER,
    CanonicalProbabilityProviderIdentity,
)
from mlb_app.simulation.shadow import (
    CANONICAL_SHADOW_EXACT_ARTIFACT_DISCOVERY_VERSION,
    MIN_EXACT_BATTER_RECORDS_PER_SIDE,
    discover_canonical_shadow_exact_artifact,
)


PROVIDER = CanonicalProbabilityProviderIdentity(
    provider_name="model_projections_pa_outcome",
    provider_version="pa_outcome_v1",
)


def batter_row(player_id, real=True):
    return {
        "batter_id": player_id,
        "has_player_split": real,
        "has_batter_aggregate": False,
        "simulation_inputs": {
            "k_pct": 0.22,
            "bb_pct": 0.09,
            "batting_avg": 0.255,
            "on_base_pct": 0.330,
            "slugging_pct": 0.430,
            "iso": 0.175,
            "hard_hit_pct": 0.40,
            "barrel_pct": 0.08,
        },
    }


def context(prefix, pitcher_id):
    return {
        "pitcher_id": pitcher_id,
        "offense_inputs": {
            "lineup": [
                batter_row(f"{prefix}{index}")
                for index in range(1, 10)
            ]
        },
    }


def workspace():
    pitcher_profile = {
        "bat_missing": {
            "k_rate": 0.24,
        },
        "command_control": {
            "bb_rate": 0.08,
        },
        "contact_management": {
            "barrel_rate_allowed": 0.07,
            "hard_hit_rate_allowed": 0.38,
            "xba_allowed": 0.245,
        },
    }

    return {
        "awayPitcherProfile": pitcher_profile,
        "homePitcherProfile": pitcher_profile,
        "environmentProfile": {
            "run_environment": {
                "hr_boost_index": 1.0,
                "hit_boost_index": 1.0,
                "run_scoring_index": 1.0,
            }
        },
    }


def discovery(**overrides):
    kwargs = {
        "away_context": context("1", 100),
        "home_context": context("2", 200),
        "workspace": workspace(),
        "provider": PROVIDER,
    }
    kwargs.update(overrides)

    return discover_canonical_shadow_exact_artifact(
        **kwargs
    )


def test_confirmed_player_profiles_build_exact_artifact():
    result = discovery()

    assert result.status == "ready"
    assert result.ready is True
    assert result.artifact is not None
    assert result.away_record_count == 9
    assert result.home_record_count == 9
    assert len(result.artifact.records) == 18


def test_rows_use_opposing_probable_starter():
    result = discovery()

    away_records = [
        record
        for record in result.artifact.records
        if record.batter_id.startswith("1")
    ]
    home_records = [
        record
        for record in result.artifact.records
        if record.batter_id.startswith("2")
    ]

    assert {
        record.pitcher_id
        for record in away_records
    } == {"200"}

    assert {
        record.pitcher_id
        for record in home_records
    } == {"100"}


def test_records_use_canonical_outcome_order():
    record = discovery().artifact.records[0]

    assert tuple(
        point.outcome
        for point in record.probabilities
    ) == CANONICAL_PA_OUTCOME_ORDER

    assert abs(
        sum(
            point.probability
            for point in record.probabilities
        )
        - 1.0
    ) < 0.000000001


def test_team_fallback_profiles_are_not_exact_rows():
    away = context("1", 100)
    away["offense_inputs"]["lineup"][0] = (
        batter_row("11", real=False)
    )
    away["offense_inputs"]["lineup"][1] = (
        batter_row("12", real=False)
    )

    result = discovery(
        away_context=away,
    )

    assert result.ready is True
    assert result.away_record_count == 7
    assert all(
        record.batter_id not in {"11", "12"}
        for record in result.artifact.records
    )


def test_fewer_than_seven_real_rows_blocks_side():
    away = context("1", 100)

    for index in range(3):
        away["offense_inputs"]["lineup"][index] = (
            batter_row(
                f"1{index + 1}",
                real=False,
            )
        )

    result = discovery(
        away_context=away,
    )

    assert result.ready is False
    assert result.status == "partial"
    assert result.away_record_count == 6
    assert result.blocked_reasons == (
        "insufficient_away_exact_records",
    )


def test_missing_provider_blocks_artifact():
    result = discovery(provider=None)

    assert result.ready is False
    assert result.status == "blocked"
    assert result.blocked_reasons == (
        "missing_provider",
    )


def test_missing_starter_blocks_affected_side():
    home = context("2", None)

    result = discovery(
        home_context=home,
    )

    assert result.ready is False
    assert result.away_record_count == 0
    assert (
        "insufficient_away_exact_records"
        in result.blocked_reasons
    )


def test_readiness_summary_does_not_expose_rows():
    result = discovery()
    summary = result.readiness_workspace_fields()[
        "canonicalExactProbabilityArtifact"
    ]

    assert summary["record_count"] == 18
    assert summary["away_record_count"] == 9
    assert summary["home_record_count"] == 9
    assert len(summary["digest"]) == 64
    assert "records" not in summary


def test_diagnostics_preserve_shadow_authority():
    diagnostics = discovery().to_diagnostics()

    assert diagnostics["schema_version"] == (
        CANONICAL_SHADOW_EXACT_ARTIFACT_DISCOVERY_VERSION
    )
    assert diagnostics[
        "minimum_records_per_side"
    ] == MIN_EXACT_BATTER_RECORDS_PER_SIDE
    assert diagnostics[
        "team_fallback_profiles_included"
    ] is False
    assert diagnostics[
        "bullpen_exact_rows_included"
    ] is False
    assert diagnostics[
        "probability_records_exposed"
    ] is False
    assert diagnostics["activation_permitted"] is False
    assert diagnostics["authoritative_source"] == (
        "legacy"
    )


def test_source_inputs_are_not_mutated():
    away = context("1", 100)
    home = context("2", 200)
    workspace_data = workspace()
    snapshot = (
        repr(away),
        repr(home),
        repr(workspace_data),
    )

    discover_canonical_shadow_exact_artifact(
        away_context=away,
        home_context=home,
        workspace=workspace_data,
        provider=PROVIDER,
    )

    assert (
        repr(away),
        repr(home),
        repr(workspace_data),
    ) == snapshot
