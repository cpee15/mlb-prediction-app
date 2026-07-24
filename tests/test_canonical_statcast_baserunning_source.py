import pytest

from mlb_app.simulation.shadow import (
    CANONICAL_STATCAST_BASERUNNING_SOURCE_VERSION,
    decode_statcast_baserunning_outcomes,
)


def row(**overrides):
    value = {
        "game_pk": 778462,
        "at_bat_number": 17,
        "pitch_number": 5,
        "pitcher": 686613,
        "fielder_2": 605170,
        "on_1b": 650489,
        "on_2b": None,
        "on_3b": None,
        "des": (
            "Ryan Jeffers strikes out swinging. "
            "Willi Castro steals (1) 2nd base."
        ),
    }
    value.update(overrides)
    return value


def test_decodes_stolen_base_with_exact_identities():
    outcomes = decode_statcast_baserunning_outcomes(
        row()
    )

    assert len(outcomes) == 1

    outcome = outcomes[0]

    assert outcome.runner_id == "650489"
    assert outcome.event_type == "stolen_base"
    assert outcome.origin_base == "first"
    assert outcome.target_base == "second"
    assert outcome.game_pk == 778462
    assert outcome.at_bat_number == 17
    assert outcome.pitch_number == 5
    assert outcome.pitcher_id == "686613"
    assert outcome.catcher_id == "605170"


def test_decodes_caught_stealing_from_first():
    outcomes = decode_statcast_baserunning_outcomes(
        row(
            on_1b=663898,
            des=(
                "Cam Smith strikes out swinging and "
                "Brendan Rodgers caught stealing 2nd, "
                "catcher Ryan Jeffers."
            ),
        )
    )

    assert len(outcomes) == 1
    assert outcomes[0].runner_id == "663898"
    assert (
        outcomes[0].event_type
        == "caught_stealing"
    )
    assert outcomes[0].origin_base == "first"
    assert outcomes[0].target_base == "second"


def test_decodes_steal_of_third_from_second():
    outcomes = decode_statcast_baserunning_outcomes(
        row(
            on_1b=None,
            on_2b=592885,
            des=(
                "William Contreras walks. "
                "Christian Yelich steals (2) "
                "3rd base."
            ),
        )
    )

    assert len(outcomes) == 1
    assert outcomes[0].runner_id == "592885"
    assert outcomes[0].origin_base == "second"
    assert outcomes[0].target_base == "third"


def test_decodes_double_steal_as_two_outcomes():
    outcomes = decode_statcast_baserunning_outcomes(
        row(
            on_1b=687363,
            on_2b=691023,
            des=(
                "Masyn Winn strikes out swinging. "
                "Jordan Walker steals (1) 3rd base. "
                "Victor Scott II steals (4) 2nd base."
            ),
        )
    )

    assert tuple(
        (
            outcome.runner_id,
            outcome.origin_base,
            outcome.target_base,
        )
        for outcome in outcomes
    ) == (
        (
            "691023",
            "second",
            "third",
        ),
        (
            "687363",
            "first",
            "second",
        ),
    )


def test_decodes_home_attempt_from_third():
    outcomes = decode_statcast_baserunning_outcomes(
        row(
            on_1b=None,
            on_3b=123456,
            des=(
                "Runner caught stealing home, "
                "catcher to pitcher."
            ),
        )
    )

    assert len(outcomes) == 1
    assert outcomes[0].runner_id == "123456"
    assert (
        outcomes[0].event_type
        == "caught_stealing"
    )
    assert outcomes[0].origin_base == "third"
    assert outcomes[0].target_base == "home"


def test_missing_matching_runner_is_not_fabricated():
    outcomes = decode_statcast_baserunning_outcomes(
        row(
            on_1b=None,
        )
    )

    assert outcomes == ()


def test_non_baserunning_description_returns_empty():
    outcomes = decode_statcast_baserunning_outcomes(
        row(
            des="Batter strikes out swinging.",
        )
    )

    assert outcomes == ()


def test_duplicate_text_is_deduplicated():
    outcomes = decode_statcast_baserunning_outcomes(
        row(
            des=(
                "Runner steals (1) 2nd base. "
                "Runner steals (1) 2nd base."
            ),
        )
    )

    assert len(outcomes) == 1


def test_pandas_missing_value_text_is_not_identity():
    outcomes = decode_statcast_baserunning_outcomes(
        row(
            on_1b="<NA>",
        )
    )

    assert outcomes == ()


def test_non_mapping_input_is_rejected():
    with pytest.raises(
        TypeError,
        match="row must be a mapping",
    ):
        decode_statcast_baserunning_outcomes(
            object()
        )


def test_source_version_is_explicit():
    outcome = decode_statcast_baserunning_outcomes(
        row()
    )[0]

    assert outcome.source_version == (
        CANONICAL_STATCAST_BASERUNNING_SOURCE_VERSION
    )
