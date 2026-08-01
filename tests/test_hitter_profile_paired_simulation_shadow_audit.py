from types import SimpleNamespace

import pytest

from mlb_app.simulation.shadow.hitter_profile_paired_simulation_shadow_audit import (
    compare_hitter_profile_simulation_shadow_payloads,
    run_paired_hitter_profile_simulation_shadow_audit,
)


def summary(mean):
    return {
        "count": 10,
        "mean": mean,
        "median": mean,
        "p10": mean,
        "p25": mean,
        "p75": mean,
        "p90": mean,
        "minimum": mean,
        "maximum": mean,
    }


def metric(name, mean):
    return {
        "name": name,
        "summary": summary(mean),
    }


def payload(*, adjustment=0.0):
    return {
        "simulation_count": 10,
        "teams": [
            {
                "team_side": "away",
                "metrics": [
                    metric(
                        "runs",
                        4.0 + adjustment,
                    ),
                ],
            },
            {
                "team_side": "home",
                "metrics": [
                    metric(
                        "runs",
                        5.0 - adjustment,
                    ),
                ],
            },
        ],
        "batters": [
            {
                "player_id": "10",
                "team_side": "away",
                "metrics": [
                    metric(
                        "dfs_points",
                        8.0 + adjustment,
                    ),
                    metric(
                        "walks",
                        0.5 + adjustment,
                    ),
                ],
            },
        ],
        "pitchers": [
            {
                "player_id": "100",
                "team_side": "home",
                "metrics": [
                    metric(
                        "strikeouts",
                        6.0 - adjustment,
                    ),
                ],
            },
        ],
        "outcomes": {
            "simulation_count": 10,
            "away_win_probability":
                0.40 + adjustment,
            "home_win_probability":
                0.60 - adjustment,
            "tie_probability": 0.0,
            "extra_innings_probability": 0.10,
            "walk_off_probability": 0.05,
            "away_run_distribution": {
                "3": 0.5,
                "5": 0.5,
            },
            "home_run_distribution": {
                "4": 0.5,
                "6": 0.5,
            },
            "total_run_distribution": {
                "7": 0.5,
                "11": 0.5,
            },
            "team_total_probabilities": {
                "away_4_plus":
                    0.50 + adjustment,
                "home_5_plus":
                    0.50 - adjustment,
            },
            "total_probabilities": {
                "total_8_plus":
                    0.60 + adjustment,
            },
        },
    }


def materialization():
    return {
        "status": "ready",
        "materialized": True,
        "candidate_results": {
            "10": {
                "status": "ready",
            },
        },
        "candidate_batter_count": 1,
        "database_writes_performed": False,
        "production_inputs_unchanged": True,
        "production_authority_changed": False,
    }


class Execution:
    def __init__(
        self,
        payload_value,
        *,
        overlay=None,
    ):
        self.material = SimpleNamespace(
            canonical_payload=payload_value,
        )
        self.overlay = overlay

    def to_diagnostics(self):
        result = {
            "status": "executed",
            "executed": True,
            "simulation_count": 10,
            "production_authority_changed": False,
        }
        if self.overlay is not None:
            result[
                "hitter_profile_simulation_shadow"
            ] = self.overlay
        return result


def test_comparator_reports_game_team_and_player_deltas():
    result = (
        compare_hitter_profile_simulation_shadow_payloads(
            baseline_payload=payload(),
            candidate_payload=payload(
                adjustment=0.02,
            ),
        )
    )

    assert result["status"] == "ready"
    assert result["simulation_count"] == 10
    assert result["comparison_count"] == len(
        result["records"]
    )
    assert result["comparison_count"] == 22
    assert result[
        "absolute_delta_summary"
    ]["maximum"] == pytest.approx(0.02)
    assert {
        record["scope"]
        for record in result["records"]
    } == {
        "game",
        "game_probability",
        "team",
        "batter",
        "pitcher",
    }
    assert any(
        record["identity"]
        == "total_probabilities"
        and record["metric"]
        == "total_8_plus"
        for record in result["records"]
    )


def test_comparator_rejects_simulation_count_mismatch():
    candidate = payload()
    candidate["simulation_count"] = 11

    result = (
        compare_hitter_profile_simulation_shadow_payloads(
            baseline_payload=payload(),
            candidate_payload=candidate,
        )
    )

    assert result["status"] == "blocked"
    assert result["blockers"] == [
        "paired_simulation_count_mismatch",
    ]


def test_disabled_pair_does_not_execute():
    calls = []

    result = (
        run_paired_hitter_profile_simulation_shadow_audit(
            execution_runner=lambda **kwargs: (
                calls.append(kwargs)
            ),
        )
    )

    assert result.status == "disabled"
    assert calls == []


def test_blocked_materialization_does_not_execute():
    calls = []

    result = (
        run_paired_hitter_profile_simulation_shadow_audit(
            enabled=True,
            candidate_materialization={
                "status": "fallback",
            },
            execution_runner=lambda **kwargs: (
                calls.append(kwargs)
            ),
        )
    )

    assert result.status == "blocked"
    assert calls == []


def test_pair_uses_identical_inputs_and_candidate_only_overlay():
    calls = []

    def runner(**kwargs):
        calls.append(kwargs)
        enabled = kwargs[
            "hitter_profile_shadow_enabled"
        ]
        return Execution(
            payload(
                adjustment=(
                    0.02 if enabled else 0.0
                )
            ),
            overlay=(
                {
                    "overlay_applied": True,
                }
                if enabled
                else None
            ),
        )

    result = (
        run_paired_hitter_profile_simulation_shadow_audit(
            enabled=True,
            acceptance_gate={
                "gate_passed": True,
            },
            candidate_materialization=(
                materialization()
            ),
            execution_runner=runner,
            game_pk=123,
            simulation_count=10,
            bootstrap_ready=True,
        )
    )

    assert result.status == "observed"
    assert len(calls) == 2
    assert calls[0]["game_pk"] == 123
    assert calls[1]["game_pk"] == 123
    assert calls[0]["simulation_count"] == 10
    assert calls[1]["simulation_count"] == 10
    assert (
        calls[0]["hitter_profile_shadow_enabled"]
        is False
    )
    assert (
        calls[1]["hitter_profile_shadow_enabled"]
        is True
    )
    assert (
        calls[1][
            "hitter_profile_candidate_results"
        ]
        == materialization()[
            "candidate_results"
        ]
    )
    assert (
        result.production_execution
        is result.baseline_execution
    )

    diagnostics = result.to_diagnostics()
    assert (
        diagnostics["safety_checks"][
            "candidate_overlay_applied"
        ]
        is True
    )
    assert (
        diagnostics[
            "production_activation_allowed"
        ]
        is False
    )
    assert (
        diagnostics[
            "production_authority_changed"
        ]
        is False
    )


def test_candidate_execution_failure_blocks_observation():
    calls = []

    def runner(**kwargs):
        calls.append(kwargs)
        if kwargs[
            "hitter_profile_shadow_enabled"
        ]:
            return Execution(None)
        return Execution(payload())

    result = (
        run_paired_hitter_profile_simulation_shadow_audit(
            enabled=True,
            acceptance_gate={
                "gate_passed": True,
            },
            candidate_materialization=(
                materialization()
            ),
            execution_runner=runner,
            game_pk=123,
            simulation_count=10,
        )
    )

    assert result.status == "blocked"
    assert result.blockers == (
        "candidate_execution_not_ready",
    )


def test_reserved_overlay_arguments_are_rejected():
    with pytest.raises(
        ValueError,
        match="owns hitter-profile",
    ):
        run_paired_hitter_profile_simulation_shadow_audit(
            enabled=True,
            candidate_materialization=(
                materialization()
            ),
            execution_runner=lambda **kwargs: None,
            hitter_profile_shadow_enabled=True,
        )

def test_candidate_overlay_must_actually_apply():
    def runner(**kwargs):
        return Execution(
            payload(),
            overlay=None,
        )

    result = (
        run_paired_hitter_profile_simulation_shadow_audit(
            enabled=True,
            acceptance_gate={
                "gate_passed": True,
            },
            candidate_materialization=(
                materialization()
            ),
            execution_runner=runner,
            game_pk=123,
            simulation_count=10,
        )
    )

    assert result.status == "blocked"
    assert result.blockers == (
        "candidate_overlay_not_applied",
    )
