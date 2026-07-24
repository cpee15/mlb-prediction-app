import pytest

from mlb_app.simulation.shadow import (
    CANONICAL_HISTORICAL_SHADOW_REPLAY_INPUT_AUDIT_VERSION,
    CURRENT_ACTIVE_ROSTER_SOURCE,
    HISTORICAL_BULLPEN_SOURCE,
    HISTORICAL_LINEUP_SOURCE,
    CanonicalHistoricalShadowReplayInputEvidence,
    audit_historical_shadow_replay_input_coverage,
)
from mlb_app.simulation.shadow.mlb_play_by_play_baserunning_source import (
    CanonicalMlbPlayByPlayBaserunningGame,
    CanonicalMlbPlayByPlayBaserunningSnapshot,
)


DIGEST_A = "a" * 64
DIGEST_B = "b" * 64
DIGEST_C = "c" * 64
DIGEST_D = "d" * 64
DIGEST_E = "e" * 64
DIGEST_F = "f" * 64


def observed():
    return CanonicalMlbPlayByPlayBaserunningSnapshot(
        window_start="2026-04-20",
        window_end="2026-04-21",
        games=(
            CanonicalMlbPlayByPlayBaserunningGame(
                game_pk=2,
                game_date="2026-04-21",
                stolen_bases=0,
                caught_stealing=1,
            ),
            CanonicalMlbPlayByPlayBaserunningGame(
                game_pk=1,
                game_date="2026-04-20",
                stolen_bases=1,
                caught_stealing=0,
            ),
        ),
        event_count=2,
        stolen_bases=1,
        caught_stealing=1,
        duplicate_event_record_count=0,
        digest=DIGEST_F,
    )


def complete(game_pk, game_date):
    return CanonicalHistoricalShadowReplayInputEvidence(
        game_pk=game_pk,
        game_date=game_date,
        lineup_source=HISTORICAL_LINEUP_SOURCE,
        lineup_snapshot_digest=DIGEST_A,
        bullpen_source=HISTORICAL_BULLPEN_SOURCE,
        bullpen_snapshot_digest=DIGEST_B,
        probability_provider_identity="provider:v1",
        exact_artifact_digest=DIGEST_C,
        fallback_catalog_digest=DIGEST_D,
        baserunning_catalog_digest=DIGEST_E,
    )


def audit(*values):
    return audit_historical_shadow_replay_input_coverage(
        observed=observed(),
        evidence=values,
    )


def test_complete_historical_inputs_are_ready():
    result = audit(
        complete(2, "2026-04-21"),
        complete(1, "2026-04-20"),
    )

    assert result.ready is True
    assert result.discovery.ready_game_count == 2
    assert result.historical_lineup_game_count == 2
    assert result.historical_bullpen_game_count == 2
    assert result.rejected_current_roster_game_count == 0


def test_current_active_roster_is_not_historical_evidence():
    value = complete(1, "2026-04-20")
    arguments = {
        **value.__dict__,
        "bullpen_source": CURRENT_ACTIVE_ROSTER_SOURCE,
    }

    result = audit(
        CanonicalHistoricalShadowReplayInputEvidence(
            **arguments
        ),
        complete(2, "2026-04-21"),
    )

    assert result.ready is False
    assert (
        result.rejected_current_roster_game_count
        == 1
    )
    assert result.discovery.missing_requirement_counts == (
        ("missing_bullpens", 1),
    )


def test_unversioned_lineup_source_is_blocked():
    value = complete(1, "2026-04-20")
    arguments = {
        **value.__dict__,
        "lineup_source": "mlb_stats_boxscore",
    }

    result = audit(
        CanonicalHistoricalShadowReplayInputEvidence(
            **arguments
        ),
        complete(2, "2026-04-21"),
    )

    assert result.ready is False
    assert result.discovery.missing_requirement_counts == (
        ("missing_lineups", 1),
    )


def test_missing_artifact_is_counted():
    value = complete(1, "2026-04-20")
    arguments = {
        **value.__dict__,
        "exact_artifact_digest": None,
    }

    result = audit(
        CanonicalHistoricalShadowReplayInputEvidence(
            **arguments
        ),
        complete(2, "2026-04-21"),
    )

    assert result.discovery.missing_requirement_counts == (
        ("missing_exact_artifact", 1),
    )


def test_game_coverage_must_match_observed_window():
    with pytest.raises(
        ValueError,
        match=(
            "historical replay games must exactly match "
            "observed play-by-play games"
        ),
    ):
        audit(
            complete(1, "2026-04-20"),
        )


def test_official_game_date_must_match():
    with pytest.raises(
        ValueError,
        match=(
            "historical replay game_date must match "
            "observed official game_date"
        ),
    ):
        audit(
            complete(1, "2026-04-21"),
            complete(2, "2026-04-21"),
        )


def test_invalid_digest_is_rejected():
    with pytest.raises(
        ValueError,
        match=(
            "lineup_snapshot_digest must be "
            "a SHA-256 hex digest"
        ),
    ):
        CanonicalHistoricalShadowReplayInputEvidence(
            game_pk=1,
            game_date="2026-04-20",
            lineup_snapshot_digest="invalid",
        )


def test_audit_is_deterministic():
    first = audit(
        complete(2, "2026-04-21"),
        complete(1, "2026-04-20"),
    )
    second = audit(
        complete(1, "2026-04-20"),
        complete(2, "2026-04-21"),
    )

    assert first == second
    assert first.evidence_digest == second.evidence_digest
    assert tuple(
        value.game_pk
        for value in first.discovery.games
    ) == (1, 2)


def test_diagnostics_preserve_shadow_authority():
    diagnostics = audit(
        complete(1, "2026-04-20"),
        complete(2, "2026-04-21"),
    ).to_diagnostics()

    assert diagnostics["ready"] is True
    assert (
        diagnostics[
            "current_active_roster_historical_eligible"
        ]
        is False
    )
    assert (
        diagnostics["calibration_execution_permitted"]
        is False
    )
    assert diagnostics["production_activation"] is False
    assert (
        diagnostics["production_authority_changed"]
        is False
    )
    assert diagnostics["authoritative_source"] == "legacy"


def test_audit_version_is_explicit():
    assert (
        CANONICAL_HISTORICAL_SHADOW_REPLAY_INPUT_AUDIT_VERSION
        == "canonical_historical_shadow_replay_input_audit_v1"
    )
