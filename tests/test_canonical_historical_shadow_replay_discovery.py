import pytest

from mlb_app.simulation.shadow import (
    CANONICAL_HISTORICAL_SHADOW_REPLAY_DISCOVERY_VERSION,
    CANONICAL_MLB_PLAY_BY_PLAY_BASERUNNING_SOURCE_VERSION,
    CanonicalHistoricalShadowReplayInputGame,
    CanonicalMlbPlayByPlayBaserunningGame,
    CanonicalMlbPlayByPlayBaserunningSnapshot,
    discover_historical_shadow_replay_inputs,
)


def observed():
    return CanonicalMlbPlayByPlayBaserunningSnapshot(
        window_start="2026-04-20",
        window_end="2026-05-03",
        games=(
            CanonicalMlbPlayByPlayBaserunningGame(
                game_pk=1,
                game_date="2026-04-20",
                stolen_bases=1,
                caught_stealing=0,
            ),
            CanonicalMlbPlayByPlayBaserunningGame(
                game_pk=2,
                game_date="2026-04-21",
                stolen_bases=0,
                caught_stealing=1,
            ),
        ),
        event_count=2,
        stolen_bases=1,
        caught_stealing=1,
        duplicate_event_record_count=0,
        digest="a" * 64,
        source_version=(
            CANONICAL_MLB_PLAY_BY_PLAY_BASERUNNING_SOURCE_VERSION
        ),
    )


def game(
    *,
    game_pk,
    game_date,
    ready=True,
    **overrides,
):
    arguments = {
        "game_pk": game_pk,
        "game_date": game_date,
        "lineups_ready": ready,
        "bullpens_ready": ready,
        "probability_provider_ready": ready,
        "exact_artifact_ready": ready,
        "fallback_catalog_ready": ready,
        "baserunning_catalog_ready": ready,
        "probability_provider_identity": (
            "provider-v1" if ready else None
        ),
        "exact_artifact_digest": (
            "a" * 64 if ready else None
        ),
        "fallback_catalog_digest": (
            "b" * 64 if ready else None
        ),
        "baserunning_catalog_digest": (
            "c" * 64 if ready else None
        ),
    }
    arguments.update(overrides)

    return CanonicalHistoricalShadowReplayInputGame(
        **arguments
    )


def games():
    return (
        game(
            game_pk=2,
            game_date="2026-04-21",
        ),
        game(
            game_pk=1,
            game_date="2026-04-20",
        ),
    )


def discover(**overrides):
    arguments = {
        "observed": observed(),
        "games": games(),
    }
    arguments.update(overrides)

    return discover_historical_shadow_replay_inputs(
        **arguments
    )


def test_complete_window_is_replay_ready():
    result = discover()

    assert result.ready is True
    assert result.game_count == 2
    assert result.ready_game_count == 2
    assert result.blocked_game_count == 0
    assert result.missing_requirement_counts == ()


def test_missing_inputs_are_counted_by_reason():
    result = discover(
        games=(
            game(
                game_pk=1,
                game_date="2026-04-20",
                ready=False,
                lineups_ready=True,
            ),
            game(
                game_pk=2,
                game_date="2026-04-21",
                ready=False,
            ),
        )
    )

    assert result.ready is False
    assert result.ready_game_count == 0
    assert result.blocked_game_count == 2
    assert dict(
        result.missing_requirement_counts
    )["missing_bullpens"] == 2
    assert dict(
        result.missing_requirement_counts
    )["missing_lineups"] == 1


def test_ready_flags_require_provenance():
    result = discover(
        games=(
            game(
                game_pk=1,
                game_date="2026-04-20",
                exact_artifact_digest=None,
            ),
            game(
                game_pk=2,
                game_date="2026-04-21",
            ),
        )
    )

    assert result.ready is False
    assert result.games[0].missing_requirements == (
        "missing_exact_artifact_digest",
    )


def test_exact_observed_coverage_is_required():
    with pytest.raises(
        ValueError,
        match=(
            "historical replay games must exactly match "
            "observed play-by-play games"
        ),
    ):
        discover(games=(games()[0],))


def test_official_date_alignment_is_required():
    values = list(games())
    values[1] = game(
        game_pk=1,
        game_date="2026-04-22",
    )

    with pytest.raises(
        ValueError,
        match=(
            "historical replay game_date must "
            "match observed official game_date"
        ),
    ):
        discover(games=tuple(values))


def test_duplicate_game_is_rejected():
    value = games()[0]

    with pytest.raises(
        ValueError,
        match=(
            "historical replay game identifiers "
            "must be unique"
        ),
    ):
        discover(games=(value, value))


def test_diagnostics_do_not_expose_input_records():
    diagnostics = discover().to_diagnostics()

    assert diagnostics[
        "calibration_execution_permitted"
    ] is False
    assert diagnostics["production_activation"] is False
    assert diagnostics[
        "production_authority_changed"
    ] is False
    assert diagnostics["authoritative_source"] == "legacy"
    assert all(
        value["lineup_identifiers_exposed"] is False
        for value in diagnostics["games"]
    )
    assert all(
        value["probability_records_exposed"] is False
        for value in diagnostics["games"]
    )


def test_discovery_is_deterministic():
    first = discover()
    second = discover()

    assert first == second
    assert first.to_diagnostics() == second.to_diagnostics()


def test_discovery_version_is_explicit():
    assert (
        CANONICAL_HISTORICAL_SHADOW_REPLAY_DISCOVERY_VERSION
        == "canonical_historical_shadow_replay_discovery_v1"
    )
