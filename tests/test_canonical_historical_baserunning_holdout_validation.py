import pytest

from mlb_app.simulation.shadow import (
    CANONICAL_HISTORICAL_BASERUNNING_HOLDOUT_VERSION,
    CanonicalHistoricalBaserunningHoldoutPlan,
    build_historical_baserunning_holdout_plan,
    filter_historical_baserunning_holdout_schedule,
)


def test_holdout_plan_is_fixed_and_disjoint():
    plan = build_historical_baserunning_holdout_plan()
    diagnostics = plan.to_diagnostics()

    assert plan.selection_window_start == "2026-04-20"
    assert plan.selection_window_end == "2026-05-03"
    assert plan.holdout_window_start == "2026-05-04"
    assert plan.holdout_window_end == "2026-05-17"
    assert plan.minimum_game_count == 150
    assert plan.simulation_count == 100
    assert plan.windows_are_disjoint is True
    assert (
        plan.probability_transform
        .attempt_probability_multiplier
        == 0.52
    )
    assert (
        plan.probability_transform
        .success_rate_adjustment
        == 0.09
    )
    assert (
        diagnostics["candidate_reselected_on_holdout"]
        is False
    )
    assert diagnostics["activation_permitted"] is False
    assert diagnostics["production_activation"] is False
    assert (
        diagnostics["production_authority_changed"]
        is False
    )
    assert diagnostics["authoritative_source"] == "legacy"


def test_overlapping_holdout_is_rejected():
    with pytest.raises(
        ValueError,
        match="must be disjoint",
    ):
        CanonicalHistoricalBaserunningHoldoutPlan(
            selection_window_start="2026-04-20",
            selection_window_end="2026-05-03",
            holdout_window_start="2026-05-01",
            holdout_window_end="2026-05-14",
            minimum_game_count=150,
            simulation_count=100,
        )


def test_underpowered_replay_is_rejected():
    with pytest.raises(
        ValueError,
        match="at least 100",
    ):
        CanonicalHistoricalBaserunningHoldoutPlan(
            selection_window_start="2026-04-20",
            selection_window_end="2026-05-03",
            holdout_window_start="2026-05-04",
            holdout_window_end="2026-05-17",
            minimum_game_count=150,
            simulation_count=99,
        )


def test_holdout_plan_is_deterministic():
    first = build_historical_baserunning_holdout_plan()
    second = build_historical_baserunning_holdout_plan()

    assert first == second
    assert first.digest == second.digest
    assert (
        first.to_diagnostics()
        == second.to_diagnostics()
    )


def test_version_is_explicit():
    assert (
        CANONICAL_HISTORICAL_BASERUNNING_HOLDOUT_VERSION
        == "canonical_historical_baserunning_holdout_v1"
    )



def test_schedule_filter_excludes_postponed_games():
    plan = build_historical_baserunning_holdout_plan()
    schedule = {
        "dates": [
            {
                "date": "2026-05-05",
                "games": [
                    {
                        "gamePk": 1,
                        "officialDate": "2026-05-05",
                        "status": {
                            "detailedState": "Final",
                        },
                    },
                    {
                        "gamePk": 2,
                        "officialDate": "2026-07-07",
                        "rescheduleDate": (
                            "2026-07-07T18:15:00Z"
                        ),
                        "status": {
                            "detailedState": "Final",
                        },
                    },
                ],
            },
            {
                "date": "2026-05-07",
                "games": [
                    {
                        "gamePk": 3,
                        "officialDate": "2026-05-07",
                        "status": {
                            "detailedState": "Final",
                        },
                    },
                ],
            },
        ]
    }

    filtered, excluded = (
        filter_historical_baserunning_holdout_schedule(
            schedule=schedule,
            plan=plan,
        )
    )

    assert [
        game["gamePk"]
        for date_item in filtered["dates"]
        for game in date_item["games"]
    ] == [1, 3]
    assert excluded == (
        {
            "game_pk": 2,
            "schedule_date": "2026-05-05",
            "official_date": "2026-07-07",
            "detailed_state": "Final",
            "reschedule_date": (
                "2026-07-07T18:15:00Z"
            ),
            "reason": (
                "official_date_outside_holdout"
            ),
        },
    )
    assert len(schedule["dates"][0]["games"]) == 2
