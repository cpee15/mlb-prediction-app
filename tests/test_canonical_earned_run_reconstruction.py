from dataclasses import replace

import pytest

from mlb_app.simulation.events import (
    GameState,
    PlayAttribution,
    RunnerMovement,
    build_play_event,
)
from mlb_app.simulation.game import (
    CANONICAL_EARNED_RUN_RECONSTRUCTION_VERSION,
    CanonicalEarnedRunReconstructor,
    CanonicalRunnerResponsibility,
    CanonicalScoredRunResponsibility,
)


def reach_event(
    *,
    sequence=0,
    runner_id="runner_a",
    pitcher_id="starter",
    error_fielder_id=None,
):
    return replace(
        build_play_event(
            sequence=sequence,
            event_type=(
                "reached_on_error"
                if error_fielder_id
                else "single"
            ),
            batter_id=runner_id,
            state_before=GameState(),
            runner_movements=(
                RunnerMovement(
                    runner_id=runner_id,
                    start_base=0,
                    end_base=1,
                ),
            ),
            attribution=PlayAttribution(
                error_fielder_id=error_fielder_id,
                error_type=(
                    "fielding_error"
                    if error_fielder_id
                    else None
                ),
            ),
        ),
        pitcher_id=pitcher_id,
    )


def runner_responsibility(
    *,
    runner_id="runner_a",
    pitcher_id="starter",
    sequence=0,
):
    return CanonicalRunnerResponsibility(
        runner_id=runner_id,
        responsible_pitcher_id=pitcher_id,
        reached_on_event_sequence=sequence,
        reached_on_event_type="single",
    )


def scored_responsibility(
    *,
    runner_id="runner_a",
    responsible_pitcher_id="starter",
    pitcher_on_mound_id="reliever",
    sequence=1,
):
    return CanonicalScoredRunResponsibility(
        runner_id=runner_id,
        responsible_pitcher_id=responsible_pitcher_id,
        pitcher_on_mound_id=pitcher_on_mound_id,
        scoring_event_sequence=sequence,
        scoring_event_type="single",
    )


def test_normal_reach_is_classified_earned():
    value = CanonicalEarnedRunReconstructor()

    value.record_runner_reach(
        responsibility=runner_responsibility(),
        event=reach_event(),
    )

    classification = value.classify_scored_run(
        scored_responsibility()
    )

    assert classification.earned is True
    assert classification.classification_reason == (
        "no_explicit_error_on_reach"
    )
    assert classification.schema_version == (
        CANONICAL_EARNED_RUN_RECONSTRUCTION_VERSION
    )


def test_error_reach_is_classified_unearned():
    value = CanonicalEarnedRunReconstructor()

    value.record_runner_reach(
        responsibility=runner_responsibility(),
        event=reach_event(
            error_fielder_id="shortstop",
        ),
    )

    classification = value.classify_scored_run(
        scored_responsibility()
    )

    assert classification.earned is False
    assert classification.classification_reason == (
        "reached_on_fielding_error"
    )


def test_inherited_run_is_charged_to_responsible_pitcher():
    value = CanonicalEarnedRunReconstructor()

    value.record_runner_reach(
        responsibility=runner_responsibility(
            pitcher_id="starter",
        ),
        event=reach_event(
            pitcher_id="starter",
        ),
    )

    value.classify_scored_run(
        scored_responsibility(
            responsible_pitcher_id="starter",
            pitcher_on_mound_id="reliever",
        )
    )

    lines = value.pitcher_run_lines()

    assert len(lines) == 1
    assert lines[0].pitcher_id == "starter"
    assert lines[0].runs_allowed == 1
    assert lines[0].earned_runs == 1
    assert lines[0].unearned_runs == 0
    assert lines[0].earned_run_status == (
        "reconstructed"
    )


def test_pitcher_lines_aggregate_earned_and_unearned():
    value = CanonicalEarnedRunReconstructor()

    for index, error_fielder_id in enumerate(
        (None, "third_base")
    ):
        runner_id = f"runner_{index}"

        value.record_runner_reach(
            responsibility=runner_responsibility(
                runner_id=runner_id,
                sequence=index,
            ),
            event=reach_event(
                sequence=index,
                runner_id=runner_id,
                error_fielder_id=error_fielder_id,
            ),
        )

        value.classify_scored_run(
            scored_responsibility(
                runner_id=runner_id,
                sequence=index + 2,
            )
        )

    line = value.pitcher_run_lines()[0]

    assert line.runs_allowed == 2
    assert line.earned_runs == 1
    assert line.unearned_runs == 1


def test_retired_runner_is_removed():
    value = CanonicalEarnedRunReconstructor()

    value.record_runner_reach(
        responsibility=runner_responsibility(),
        event=reach_event(),
    )

    value.retire_runner("runner_a")

    with pytest.raises(
        ValueError,
        match="no recorded reach event",
    ):
        value.classify_scored_run(
            scored_responsibility()
        )


def test_duplicate_reach_record_is_rejected():
    value = CanonicalEarnedRunReconstructor()
    responsibility = runner_responsibility()
    event = reach_event()

    value.record_runner_reach(
        responsibility=responsibility,
        event=event,
    )

    with pytest.raises(
        ValueError,
        match="already recorded",
    ):
        value.record_runner_reach(
            responsibility=responsibility,
            event=event,
        )
