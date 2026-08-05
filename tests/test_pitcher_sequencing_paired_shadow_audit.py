from types import SimpleNamespace

import pytest

from mlb_app.simulation.shadow.pitcher_sequencing_paired_shadow_audit import (
    run_paired_pitcher_sequencing_shadow_audit,
)


STABLE_DIAGNOSTICS = {
    "provider_identity": "provider:v1",
    "exact_artifact_digest": "exact",
    "fallback_catalog_digest": "fallback",
    "baserunning_evidence_catalog_digest": (
        "baserunning"
    ),
    "canonical_model_version": "model-v1",
}


def classification(
    *,
    plan_type="opener_bulk",
):
    return {
        "plan_type": plan_type,
        "fallback_used": False,
        "planned_sequence": [
            {
                "order": 1,
                "role": "opener",
                "pitcher_id": "100",
            },
            {
                "order": 2,
                "role": "bulk_follower",
                "pitcher_id": "101",
            },
        ],
    }


def payload(
    *,
    simulation_count=25,
    home_win_probability=0.5,
):
    return {
        "simulation_count": simulation_count,
        "outcomes": {
            "away_win_probability": (
                1.0 - home_win_probability
            ),
            "home_win_probability":
                home_win_probability,
            "tie_probability": 0.0,
            "extra_innings_probability": 0.0,
            "walk_off_probability": 0.0,
            "away_run_distribution": {
                "4": 1.0,
            },
            "home_run_distribution": {
                "4": 1.0,
            },
            "total_run_distribution": {
                "8": 1.0,
            },
        },
        "teams": [],
        "batters": [],
        "pitchers": [],
    }


def sequence_audit(
    *,
    status="observed",
    anomaly_counts=None,
    starter_relief_detected=False,
):
    return {
        "status": status,
        "audited": status == "observed",
        "trial_count": 25,
        "appearance_count": 100,
        "affected_trial_count": (
            1 if anomaly_counts else 0
        ),
        "affected_trial_rate": (
            0.04 if anomaly_counts else 0.0
        ),
        "starter_relief_appearance_count": (
            1 if starter_relief_detected else 0
        ),
        "starter_relief_detected": (
            starter_relief_detected
        ),
        "anomaly_counts": (
            anomaly_counts or {}
        ),
        "role_summaries": {
            "starter": {
                "appearance_count": 50,
                "team_trial_appearance_count": 50,
                "appearance_rate": 1.0,
                "outs_recorded": {
                    "mean": 15.0,
                },
                "innings_equivalent": {
                    "mean": 5.0,
                },
            },
        },
    }


class Execution:
    def __init__(
        self,
        *,
        canonical_payload=None,
        diagnostics=None,
    ):
        self.material = (
            SimpleNamespace(
                canonical_payload=canonical_payload
            )
            if canonical_payload is not None
            else None
        )
        self._diagnostics = diagnostics or {}

    def to_diagnostics(self):
        return dict(self._diagnostics)


def execution(
    *,
    simulation_count=25,
    sequence=None,
    canonical_payload=None,
    diagnostics_overrides=None,
):
    diagnostics = {
        "status": "executed",
        "executed": True,
        "simulation_count": simulation_count,
        "pitcher_appearance_sequence_audit": (
            sequence or sequence_audit()
        ),
        **STABLE_DIAGNOSTICS,
    }
    diagnostics.update(
        diagnostics_overrides or {}
    )

    return Execution(
        canonical_payload=(
            canonical_payload
            if canonical_payload is not None
            else payload(
                simulation_count=simulation_count
            )
        ),
        diagnostics=diagnostics,
    )


def paired_runner(
    baseline=None,
    candidate=None,
):
    calls = []
    results = iter((
        baseline or execution(),
        candidate or execution(
            canonical_payload=payload(
                home_win_probability=0.52,
            )
        ),
    ))

    def runner(**kwargs):
        calls.append(kwargs)
        return next(results)

    return runner, calls


def run(
    *,
    runner=None,
    **kwargs,
):
    if runner is None:
        runner, _ = paired_runner()

    return (
        run_paired_pitcher_sequencing_shadow_audit(
            enabled=True,
            away_pitching_plan_classification=(
                classification()
            ),
            execution_runner=runner,
            bootstrap_ready=True,
            **kwargs,
        )
    )


def test_runs_paired_baseline_and_candidate():
    runner, calls = paired_runner()

    result = run(runner=runner)

    assert result.status == "observed"
    assert result.blockers == ()
    assert len(calls) == 2
    assert calls[0][
        "away_pitching_plan_classification"
    ] is None
    assert calls[0][
        "home_pitching_plan_classification"
    ] is None
    assert calls[1][
        "away_pitching_plan_classification"
    ]["plan_type"] == "opener_bulk"
    assert result.comparison["status"] == "ready"


def test_disabled_does_not_execute():
    called = False

    def runner(**kwargs):
        nonlocal called
        called = True
        return execution()

    result = (
        run_paired_pitcher_sequencing_shadow_audit(
            enabled=False,
            execution_runner=runner,
        )
    )

    assert result.status == "disabled"
    assert result.enabled is False
    assert called is False


def test_requires_candidate_classification():
    result = (
        run_paired_pitcher_sequencing_shadow_audit(
            enabled=True,
            execution_runner=lambda **kwargs: (
                execution()
            ),
        )
    )

    assert result.status == "blocked"
    assert result.blockers == (
        "candidate_plan_classification_unavailable",
    )


def test_routes_both_candidate_classifications():
    runner, calls = paired_runner()
    away = classification(
        plan_type="opener_bulk",
    )
    home = classification(
        plan_type="tandem",
    )

    result = (
        run_paired_pitcher_sequencing_shadow_audit(
            enabled=True,
            away_pitching_plan_classification=away,
            home_pitching_plan_classification=home,
            execution_runner=runner,
            bootstrap_ready=True,
        )
    )

    assert result.status == "observed"
    assert calls[0][
        "away_pitching_plan_classification"
    ] is None
    assert calls[0][
        "home_pitching_plan_classification"
    ] is None
    assert calls[1][
        "away_pitching_plan_classification"
    ] is away
    assert calls[1][
        "home_pitching_plan_classification"
    ] is home


@pytest.mark.parametrize(
    "anomaly_name",
    [
        "planned_starter_not_first",
        "planned_starter_used_in_relief",
        "pitcher_reentry",
        "pitcher_outside_plan",
        "away:preferred_follower_skipped",
    ],
)
def test_blocks_candidate_sequence_anomalies(
    anomaly_name,
):
    runner, _ = paired_runner(
        candidate=execution(
            sequence=sequence_audit(
                anomaly_counts={
                    anomaly_name: 1,
                },
                starter_relief_detected=(
                    anomaly_name
                    == "planned_starter_used_in_relief"
                ),
            )
        )
    )

    result = run(runner=runner)

    expected = anomaly_name.rsplit(
        ":",
        1,
    )[-1]
    assert result.status == "blocked"
    assert expected in result.blockers


def test_blocks_unobserved_candidate_audit():
    runner, _ = paired_runner(
        candidate=execution(
            sequence=sequence_audit(
                status="blocked",
            )
        )
    )

    result = run(runner=runner)

    assert result.status == "blocked"
    assert (
        "candidate_sequence_audit_not_observed"
        in result.blockers
    )


def test_blocks_execution_count_mismatch():
    runner, _ = paired_runner(
        candidate=execution(
            simulation_count=24,
        )
    )

    result = run(runner=runner)

    assert result.status == "blocked"
    assert (
        "paired_execution_count_mismatch"
        in result.blockers
    )


def test_blocks_stable_input_changes():
    runner, _ = paired_runner(
        candidate=execution(
            diagnostics_overrides={
                "provider_identity":
                    "different-provider",
            }
        )
    )

    result = run(runner=runner)

    assert result.status == "blocked"
    assert (
        "production_inputs_changed"
        in result.blockers
    )


def test_blocks_failed_candidate_execution():
    runner, _ = paired_runner(
        candidate=Execution(
            canonical_payload=None,
            diagnostics={
                "status": "error",
                "executed": False,
                "simulation_count": 0,
                **STABLE_DIAGNOSTICS,
            },
        )
    )

    result = run(runner=runner)

    assert result.status == "blocked"
    assert (
        "candidate_execution_not_ready"
        in result.blockers
    )


def test_diagnostics_are_read_only():
    result = run()
    diagnostics = result.to_diagnostics()

    assert diagnostics["status"] == "observed"
    assert diagnostics[
        "database_writes_performed"
    ] is False
    assert diagnostics[
        "production_authority_changed"
    ] is False
    assert diagnostics["decision"][
        "pitcher_sequence_activation_allowed"
    ] is False
    assert diagnostics["decision"][
        "production_activation_allowed"
    ] is False
    assert "records" not in (
        diagnostics["comparison"]
    )


def test_diagnostics_include_workload_summary():
    result = run()
    diagnostics = result.to_diagnostics()

    starter = diagnostics[
        "candidate_sequence"
    ]["role_summaries"]["starter"]

    assert starter["appearance_count"] == 50
    assert starter["outs_recorded"][
        "mean"
    ] == 15.0
    assert starter["innings_equivalent"][
        "mean"
    ] == 5.0


def test_blocks_inverted_opener_bulk_workloads():
    candidate_sequence = sequence_audit()
    candidate_sequence["role_summaries"] = {
        "opener": {
            "appearance_count": 25,
            "team_trial_appearance_count": 25,
            "appearance_rate": 0.5,
            "outs_recorded": {
                "mean": 16.5,
            },
            "innings_equivalent": {
                "mean": 5.5,
            },
        },
        "bulk_follower": {
            "appearance_count": 25,
            "team_trial_appearance_count": 25,
            "appearance_rate": 0.5,
            "outs_recorded": {
                "mean": 10.0,
            },
            "innings_equivalent": {
                "mean": 3.3333333333333335,
            },
        },
    }
    runner, _ = paired_runner(
        candidate=execution(
            sequence=candidate_sequence,
        )
    )

    result = run(runner=runner)

    assert result.status == "blocked"
    assert (
        "opener_bulk_workload_order_invalid"
        in result.blockers
    )
    assert result.to_diagnostics()["decision"][
        "recommended_next_slice"
    ] == (
        "correct_canonical_opener_bulk_"
        "workload_policy"
    )


def test_accepts_shorter_opener_than_bulk_follower():
    candidate_sequence = sequence_audit()
    candidate_sequence["role_summaries"] = {
        "opener": {
            "appearance_count": 25,
            "team_trial_appearance_count": 25,
            "appearance_rate": 0.5,
            "outs_recorded": {
                "mean": 4.0,
            },
            "innings_equivalent": {
                "mean": 1.3333333333333333,
            },
        },
        "bulk_follower": {
            "appearance_count": 25,
            "team_trial_appearance_count": 25,
            "appearance_rate": 0.5,
            "outs_recorded": {
                "mean": 14.0,
            },
            "innings_equivalent": {
                "mean": 4.666666666666667,
            },
        },
    }
    runner, _ = paired_runner(
        candidate=execution(
            sequence=candidate_sequence,
        )
    )

    result = run(runner=runner)

    assert result.status == "observed"
    assert (
        "opener_bulk_workload_order_invalid"
        not in result.blockers
    )
