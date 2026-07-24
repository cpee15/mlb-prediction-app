import pytest

from mlb_app.simulation.shadow import (
    CANONICAL_HISTORICAL_BASERUNNING_MATERIALIZATION_VERSION,
    CANONICAL_HISTORICAL_BASERUNNING_SHADOW_GAME_VERSION,
    CANONICAL_STATCAST_BASERUNNING_SOURCE_VERSION,
    CanonicalBaserunningOutputValidation,
    CanonicalHistoricalBaserunningShadowGame,
    CanonicalStatcastBaserunningOutcome,
    materialize_historical_baserunning_game_records,
)


def validation(digest="digest"):
    return CanonicalBaserunningOutputValidation(
        status="ready",
        simulation_count=100,
        catalog_digest=digest,
        runner_projection_count=9,
        stolen_base_mean_total=1.0,
        caught_stealing_mean_total=0.25,
    )


def shadow_game(
    *,
    game_pk=1,
    game_date="2026-04-20",
    digest="digest",
):
    return CanonicalHistoricalBaserunningShadowGame(
        game_pk=game_pk,
        game_date=game_date,
        validation=validation(digest),
    )


def outcome(
    *,
    game_pk=1,
    at_bat_number=10,
    pitch_number=3,
    runner_id="runner",
    event_type="stolen_base",
    origin_base="first",
    target_base="second",
    source_version=(
        CANONICAL_STATCAST_BASERUNNING_SOURCE_VERSION
    ),
):
    return CanonicalStatcastBaserunningOutcome(
        runner_id=runner_id,
        event_type=event_type,
        origin_base=origin_base,
        target_base=target_base,
        game_pk=game_pk,
        at_bat_number=at_bat_number,
        pitch_number=pitch_number,
        source_version=source_version,
    )


def materialize(**overrides):
    arguments = {
        "shadow_games": (
            shadow_game(
                game_pk=2,
                game_date="2026-04-21",
                digest="digest-b",
            ),
            shadow_game(
                game_pk=1,
                game_date="2026-04-20",
                digest="digest-a",
            ),
        ),
        "outcomes": (
            outcome(
                game_pk=1,
                runner_id="runner-a",
            ),
            outcome(
                game_pk=1,
                at_bat_number=20,
                runner_id="runner-b",
                event_type="caught_stealing",
            ),
        ),
        "observed_source_version": (
            CANONICAL_STATCAST_BASERUNNING_SOURCE_VERSION
        ),
    }
    arguments.update(overrides)

    return materialize_historical_baserunning_game_records(
        **arguments
    )


def test_materializes_aligned_game_records():
    records = materialize()

    assert tuple(
        value.game_pk
        for value in records
    ) == (1, 2)
    assert records[0].observed_stolen_bases == 1
    assert records[0].observed_caught_stealing == 1
    assert records[1].observed_stolen_bases == 0
    assert records[1].observed_caught_stealing == 0


def test_counts_multiple_runners_on_one_pitch():
    records = materialize(
        shadow_games=(shadow_game(),),
        outcomes=(
            outcome(
                runner_id="runner-a",
            ),
            outcome(
                runner_id="runner-b",
                origin_base="second",
                target_base="third",
            ),
        ),
    )

    assert records[0].observed_stolen_bases == 2
    assert records[0].observed_caught_stealing == 0


def test_zero_activity_game_remains_present():
    records = materialize(
        shadow_games=(shadow_game(),),
        outcomes=(),
    )

    assert len(records) == 1
    assert records[0].observed_stolen_bases == 0
    assert records[0].observed_caught_stealing == 0


def test_duplicate_shadow_games_are_rejected():
    with pytest.raises(
        ValueError,
        match=(
            "historical shadow game identifiers "
            "must be unique"
        ),
    ):
        materialize(
            shadow_games=(
                shadow_game(),
                shadow_game(),
            )
        )


def test_duplicate_outcomes_are_rejected():
    value = outcome()

    with pytest.raises(
        ValueError,
        match=(
            "Statcast outcome identifiers "
            "must be unique"
        ),
    ):
        materialize(
            shadow_games=(shadow_game(),),
            outcomes=(value, value),
        )


def test_unmatched_outcome_game_is_rejected():
    with pytest.raises(
        ValueError,
        match=(
            "Statcast outcome game_pk must match "
            "a historical shadow game"
        ),
    ):
        materialize(
            shadow_games=(shadow_game(),),
            outcomes=(
                outcome(game_pk=2),
            ),
        )


def test_incomplete_outcome_identity_is_rejected():
    with pytest.raises(
        ValueError,
        match=(
            "Statcast outcome requires "
            "at_bat_number"
        ),
    ):
        materialize(
            shadow_games=(shadow_game(),),
            outcomes=(
                outcome(at_bat_number=None),
            ),
        )


def test_invalid_shadow_game_contract_is_rejected():
    with pytest.raises(
        TypeError,
        match=(
            "shadow_games must contain "
            "CanonicalHistoricalBaserunningShadowGame"
        ),
    ):
        materialize(
            shadow_games=(object(),),
        )


def test_invalid_outcome_contract_is_rejected():
    with pytest.raises(
        TypeError,
        match=(
            "outcomes must contain "
            "CanonicalStatcastBaserunningOutcome"
        ),
    ):
        materialize(
            outcomes=(object(),),
        )


def test_unavailable_validation_is_rejected():
    with pytest.raises(
        ValueError,
        match=(
            "historical shadow validation "
            "must be ready"
        ),
    ):
        CanonicalHistoricalBaserunningShadowGame(
            game_pk=1,
            game_date="2026-04-20",
            validation=(
                CanonicalBaserunningOutputValidation()
            ),
        )


def test_materialization_is_deterministic():
    first = materialize()
    second = materialize()

    assert first == second


def test_versions_are_explicit():
    assert (
        CANONICAL_HISTORICAL_BASERUNNING_SHADOW_GAME_VERSION
        == "canonical_historical_baserunning_shadow_game_v1"
    )
    assert (
        CANONICAL_HISTORICAL_BASERUNNING_MATERIALIZATION_VERSION
        == "canonical_historical_baserunning_materialization_v1"
    )
