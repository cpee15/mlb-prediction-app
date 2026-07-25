import pytest

from mlb_app.simulation.shadow import (
    CANONICAL_HISTORICAL_PROBABILITY_ARTIFACT_INVENTORY_VERSION,
    HISTORICAL_PROBABILITY_ARTIFACT_SOURCE,
    CanonicalHistoricalProbabilityArtifactRecord,
    inventory_historical_probability_artifacts,
)
from mlb_app.simulation.shadow.mlb_play_by_play_baserunning_source import (
    CanonicalMlbPlayByPlayBaserunningGame,
    CanonicalMlbPlayByPlayBaserunningSnapshot,
)


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
        digest="f" * 64,
    )


def complete(game_pk, game_date):
    return CanonicalHistoricalProbabilityArtifactRecord(
        game_pk=game_pk,
        game_date=game_date,
        source=HISTORICAL_PROBABILITY_ARTIFACT_SOURCE,
        artifact_as_of_date=game_date,
        provider_identity="provider:v1",
        exact_artifact_digest="a" * 64,
        fallback_catalog_digest="b" * 64,
    )


def inventory(*records):
    return inventory_historical_probability_artifacts(
        observed=observed(),
        artifacts=records,
    )


def test_complete_archive_is_ready():
    result = inventory(
        complete(2, "2026-04-21"),
        complete(1, "2026-04-20"),
    )

    assert result.ready is True
    assert result.game_count == 2
    assert result.provider_ready_game_count == 2
    assert result.exact_artifact_ready_game_count == 2
    assert result.fallback_catalog_ready_game_count == 2
    assert result.ready_game_count == 2


def test_empty_archive_materializes_zero_coverage():
    result = inventory()

    assert result.ready is False
    assert result.game_count == 2
    assert result.provider_ready_game_count == 0
    assert result.exact_artifact_ready_game_count == 0
    assert result.fallback_catalog_ready_game_count == 0
    assert result.ready_game_count == 0
    assert result.missing_requirement_counts == (
        ("missing_exact_artifact", 2),
        ("missing_fallback_catalog", 2),
        ("missing_historical_artifact_source", 2),
        ("missing_probability_provider", 2),
    )


def test_sparse_archive_preserves_exact_game_window():
    result = inventory(
        complete(1, "2026-04-20"),
    )

    assert tuple(
        value.game_pk
        for value in result.games
    ) == (1, 2)
    assert result.ready_game_count == 1


def test_future_dated_artifact_is_rejected():
    value = CanonicalHistoricalProbabilityArtifactRecord(
        game_pk=1,
        game_date="2026-04-20",
        source=HISTORICAL_PROBABILITY_ARTIFACT_SOURCE,
        artifact_as_of_date="2026-04-21",
        provider_identity="provider:v1",
        exact_artifact_digest="a" * 64,
        fallback_catalog_digest="b" * 64,
    )

    assert value.ready is False
    assert value.future_data_rejected is True
    assert value.missing_requirements == (
        "artifact_as_of_date_mismatch",
        "missing_probability_provider",
        "missing_exact_artifact",
        "missing_fallback_catalog",
    )


def test_unknown_game_is_rejected():
    with pytest.raises(
        ValueError,
        match=(
            "historical probability artifacts must "
            "belong to the observed window"
        ),
    ):
        inventory(
            complete(3, "2026-04-22"),
        )


def test_invalid_digest_is_rejected():
    with pytest.raises(
        ValueError,
        match=(
            "exact_artifact_digest must be "
            "a SHA-256 hex digest"
        ),
    ):
        CanonicalHistoricalProbabilityArtifactRecord(
            game_pk=1,
            game_date="2026-04-20",
            exact_artifact_digest="invalid",
        )


def test_inventory_is_deterministic():
    first = inventory(
        complete(2, "2026-04-21"),
        complete(1, "2026-04-20"),
    )
    second = inventory(
        complete(1, "2026-04-20"),
        complete(2, "2026-04-21"),
    )

    assert first == second
    assert (
        first.inventory_digest
        == second.inventory_digest
    )


def test_diagnostics_preserve_shadow_authority():
    diagnostics = inventory().to_diagnostics()

    assert diagnostics["ready"] is False
    assert (
        diagnostics["historical_reconstruction_required"]
        is True
    )
    assert diagnostics["historical_replay_permitted"] is False
    assert diagnostics["historical_replay_executed"] is False
    assert diagnostics["production_activation"] is False
    assert (
        diagnostics["production_authority_changed"]
        is False
    )
    assert diagnostics["authoritative_source"] == "legacy"


def test_inventory_version_is_explicit():
    assert (
        CANONICAL_HISTORICAL_PROBABILITY_ARTIFACT_INVENTORY_VERSION
        == (
            "canonical_historical_probability_"
            "artifact_inventory_v1"
        )
    )
