import pytest

from mlb_app.simulation.shadow import (
    CANONICAL_STATCAST_PICKOFF_SOURCE_VERSION,
    CanonicalStatcastPitcherPickoffCounts,
    aggregate_statcast_pitcher_pickoff_counts,
)


def row(
    *,
    game_pk=1,
    at_bat_number=1,
    pitch_number=1,
    pitcher=100,
    on_1b=200,
    on_2b=None,
    on_3b=None,
    description="ball",
    events=None,
):
    return {
        "game_pk": game_pk,
        "at_bat_number": at_bat_number,
        "pitch_number": pitch_number,
        "pitcher": pitcher,
        "on_1b": on_1b,
        "on_2b": on_2b,
        "on_3b": on_3b,
        "description": description,
        "events": events,
    }


def test_aggregates_exact_pickoff_exposure():
    counts = aggregate_statcast_pitcher_pickoff_counts(
        (
            row(
                pitch_number=1,
                description="ball",
            ),
            row(
                pitch_number=2,
                description="pickoff_1b",
            ),
            row(
                pitch_number=3,
                description="pickoff_1b",
                events="pickoff_1b",
            ),
            row(
                pitch_number=4,
                on_1b=None,
                on_2b=201,
                description="pickoff_2b",
            ),
        )
    )

    assert len(counts) == 1

    value = counts[0]

    assert value.pitcher_id == "100"
    assert value.eligible_opportunities == 4
    assert value.pickoff_attempts == 3
    assert value.successful_pickoffs == 1


def test_success_event_is_also_an_attempt():
    counts = aggregate_statcast_pitcher_pickoff_counts(
        (
            row(
                description=None,
                events="pickoff_1b",
            ),
        )
    )

    assert counts[0].pickoff_attempts == 1
    assert counts[0].successful_pickoffs == 1


def test_normalizes_explicit_pickoff_codes():
    counts = aggregate_statcast_pitcher_pickoff_counts(
        (
            row(
                pitch_number=1,
                description="Pickoff 1B",
            ),
            row(
                pitch_number=2,
                description="pickoff-1b",
            ),
        )
    )

    assert counts[0].eligible_opportunities == 2
    assert counts[0].pickoff_attempts == 2


def test_steal_outcomes_are_not_pickoff_evidence():
    counts = aggregate_statcast_pitcher_pickoff_counts(
        (
            row(
                pitch_number=1,
                description="ball",
                events="stolen_base_2b",
            ),
            row(
                pitch_number=2,
                description="ball",
                events="caught_stealing_2b",
            ),
        )
    )

    assert counts[0].eligible_opportunities == 2
    assert counts[0].pickoff_attempts == 0
    assert counts[0].successful_pickoffs == 0


def test_rows_without_runner_or_attempt_are_ignored():
    counts = aggregate_statcast_pitcher_pickoff_counts(
        (
            row(
                on_1b=None,
                description="ball",
            ),
        )
    )

    assert counts == ()


def test_explicit_attempt_without_runner_is_retained():
    counts = aggregate_statcast_pitcher_pickoff_counts(
        (
            row(
                on_1b=None,
                description="pickoff_1b",
            ),
        )
    )

    assert len(counts) == 1
    assert counts[0].eligible_opportunities == 1
    assert counts[0].pickoff_attempts == 1


def test_duplicate_rows_are_counted_once():
    value = row(
        description="pickoff_1b",
    )

    counts = aggregate_statcast_pitcher_pickoff_counts(
        (value, dict(value))
    )

    assert counts[0].eligible_opportunities == 1
    assert counts[0].pickoff_attempts == 1


def test_pitcher_order_is_first_seen_order():
    counts = aggregate_statcast_pitcher_pickoff_counts(
        (
            row(
                pitcher=200,
                pitch_number=1,
            ),
            row(
                pitcher=100,
                pitch_number=2,
            ),
        )
    )

    assert tuple(
        value.pitcher_id
        for value in counts
    ) == ("200", "100")


def test_missing_pitcher_is_ignored():
    counts = aggregate_statcast_pitcher_pickoff_counts(
        (
            row(
                pitcher=None,
            ),
        )
    )

    assert counts == ()


def test_non_mapping_row_is_rejected():
    with pytest.raises(
        TypeError,
        match="each Statcast row must be a mapping",
    ):
        aggregate_statcast_pitcher_pickoff_counts(
            (object(),)
        )


def test_count_contract_rejects_impossible_values():
    with pytest.raises(
        ValueError,
        match=(
            "successful pickoffs cannot exceed attempts"
        ),
    ):
        CanonicalStatcastPitcherPickoffCounts(
            pitcher_id="pitcher",
            eligible_opportunities=2,
            pickoff_attempts=1,
            successful_pickoffs=2,
        )


def test_source_version_is_explicit():
    value = CanonicalStatcastPitcherPickoffCounts(
        pitcher_id="pitcher",
        eligible_opportunities=2,
        pickoff_attempts=1,
        successful_pickoffs=0,
    )

    assert value.source_version == (
        CANONICAL_STATCAST_PICKOFF_SOURCE_VERSION
    )
