from mlb_app.simulation.shadow.hitter_profile_simulation_shadow_acceptance_gate import (
    evaluate_hitter_profile_simulation_shadow_acceptance,
)


def summary(
    *,
    p95,
    maximum,
):
    return {
        "count": 100,
        "minimum": 0.0,
        "mean": p95 / 2,
        "median": p95 / 2,
        "p95": p95,
        "maximum": maximum,
    }


def audit_payload():
    records = [
        {
            "game_pk": game_pk,
            "status": "observed",
            "materialization_status": "ready",
            "baseline_execution": {
                "status": "executed",
            },
            "candidate_execution": {
                "status": "executed",
            },
        }
        for game_pk in range(1, 11)
    ]
    records.extend({
        "game_pk": game_pk,
        "status": "blocked",
        "materialization_status": "blocked",
        "baseline_execution": {},
        "candidate_execution": {},
    } for game_pk in range(11, 16))

    return {
        "status": "observed",
        "audited_game_count": 15,
        "observed_game_count": 10,
        "observation_rate": 10 / 15,
        "comparison_count": 3000,
        "simulation_count": 1000,
        "records": records,
        "absolute_delta_by_scope": {
            "game_probability":
                summary(p95=0.02, maximum=0.04),
            "game":
                summary(p95=0.20, maximum=0.30),
            "team":
                summary(p95=0.25, maximum=0.35),
            "batter":
                summary(p95=0.10, maximum=1.00),
            "pitcher":
                summary(p95=0.10, maximum=0.40),
        },
        "absolute_delta_by_metric": {
            "home_win_probability":
                summary(p95=0.02, maximum=0.04),
            "away_win_probability":
                summary(p95=0.02, maximum=0.04),
            "total_run_distribution_mean":
                summary(p95=0.25, maximum=0.30),
            "dfs_points":
                summary(p95=0.60, maximum=1.00),
        },
        "safety_checks": {
            "all_production_authority_unchanged":
                True,
            "all_production_inputs_unchanged":
                True,
            "all_simulation_counts_match": True,
            "database_writes_performed": False,
        },
        "database_writes_performed": False,
        "production_authority_changed": False,
    }


def test_accepts_scope_specific_shadow_evidence():
    result = (
        evaluate_hitter_profile_simulation_shadow_acceptance(
            audit_payload()
        )
    )

    assert result["gate_passed"] is True
    assert (
        result["status"]
        == "accepted_for_extended_shadow_evaluation"
    )
    assert result["blockers"] == []
    assert result["decision"][
        "extended_shadow_evaluation_allowed"
    ] is True
    assert result["decision"][
        "production_activation_allowed"
    ] is False
    assert result["evaluation_scope"][
        "scope_specific_thresholds"
    ] is True


def test_live_6sj_window_remains_coverage_blocked():
    payload = audit_payload()
    payload["observed_game_count"] = 5
    payload["observation_rate"] = 5 / 15
    payload["records"] = (
        payload["records"][:5]
        + [
            {
                "game_pk": game_pk,
                "status": "blocked",
                "materialization_status":
                    "blocked",
                "baseline_execution": {},
                "candidate_execution": {},
            }
            for game_pk in range(6, 16)
        ]
    )

    result = (
        evaluate_hitter_profile_simulation_shadow_acceptance(
            payload
        )
    )

    assert result["gate_passed"] is False
    assert "minimum_observed_games" in (
        result["blockers"]
    )
    assert "minimum_observation_rate" in (
        result["blockers"]
    )
    assert result["decision"][
        "production_activation_allowed"
    ] is False


def test_blocks_scope_specific_delta_breach():
    payload = audit_payload()
    payload["absolute_delta_by_scope"][
        "game_probability"
    ]["p95"] = 0.031
    payload["absolute_delta_by_metric"][
        "dfs_points"
    ]["maximum"] = 1.26

    result = (
        evaluate_hitter_profile_simulation_shadow_acceptance(
            payload
        )
    )

    assert result["gate_passed"] is False
    assert (
        "game_probability_p95_delta"
        in result["blockers"]
    )
    assert (
        "dfs_points_maximum_delta"
        in result["blockers"]
    )


def test_blocks_safety_and_shared_execution_failures():
    payload = audit_payload()
    payload["safety_checks"][
        "all_production_inputs_unchanged"
    ] = False

    for index in (0, 1):
        payload["records"][index][
            "baseline_execution"
        ]["status"] = "error"
        payload["records"][index][
            "candidate_execution"
        ]["status"] = "error"

    result = (
        evaluate_hitter_profile_simulation_shadow_acceptance(
            payload
        )
    )

    assert result["gate_passed"] is False
    assert (
        "production_inputs_unchanged"
        in result["blockers"]
    )
    assert (
        "maximum_shared_execution_error_rate"
        in result["blockers"]
    )


def test_missing_evidence_fails_closed():
    result = (
        evaluate_hitter_profile_simulation_shadow_acceptance(
            {}
        )
    )

    assert result["gate_passed"] is False
    assert result["blockers"]
    assert result["decision"][
        "production_activation_allowed"
    ] is False
    assert (
        result["production_authority_changed"]
        is False
    )

def test_blocks_incomplete_or_unreconciled_records():
    payload = audit_payload()
    payload["records"].pop(0)

    result = (
        evaluate_hitter_profile_simulation_shadow_acceptance(
            payload
        )
    )

    assert result["gate_passed"] is False
    assert "complete_game_records" in (
        result["blockers"]
    )
    assert "observed_game_count_reconciles" in (
        result["blockers"]
    )
    assert result["decision"][
        "production_activation_allowed"
    ] is False
