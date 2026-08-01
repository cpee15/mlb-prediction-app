from mlb_app.simulation.shadow.hitter_profile_canary_acceptance_gate import (
    evaluate_hitter_profile_canary_acceptance,
)


def audit_payload():
    return {
        "status": "observed",
        "audited_player_split_count": 250,
        "executed_player_split_count": 61,
        "execution_rate": 0.244,
        "state_counts": {
            "executed": 61,
            "signal_evidence_blocked": 177,
            "production_profile_blocked": 12,
        },
        "fallback_telemetry": {
            "fallback_rate": 0.0,
        },
        "maximum_absolute_probability_delta": {
            "median": 0.0334,
            "p95": 0.0761,
            "maximum": 0.0918,
        },
        "absolute_probability_delta_by_outcome": {
            "bb": {"p95": 0.0418},
            "hr": {"p95": 0.0215},
            "k": {"p95": 0.0439},
            "out": {"p95": 0.0761},
        },
        "safety_checks": {
            "all_production_inputs_unchanged":
                True,
            "all_production_authority_unchanged":
                True,
            "all_candidate_probabilities_normalized":
                True,
            "database_writes_performed": False,
        },
        "production_authority_changed": False,
    }


def test_accepts_observed_top_250_evidence():
    result = (
        evaluate_hitter_profile_canary_acceptance(
            audit_payload()
        )
    )

    assert result["gate_passed"] is True
    assert (
        result["status"]
        == "accepted_for_feature_flag_integration"
    )
    assert result["blockers"] == []
    assert result["decision"][
        "feature_flag_integration_allowed"
    ] is True
    assert result["decision"][
        "production_activation_allowed"
    ] is False
    assert result["activation_scope"][
        "eligible_player_splits_only"
    ] is True
    assert result["activation_scope"][
        "production_fallback_required"
    ] is True
    assert (
        result["production_authority_changed"]
        is False
    )


def test_blocks_insufficient_evidence():
    payload = audit_payload()
    payload["audited_player_split_count"] = 100
    payload["executed_player_split_count"] = 31

    result = (
        evaluate_hitter_profile_canary_acceptance(
            payload
        )
    )

    assert result["gate_passed"] is False
    assert (
        "minimum_audited_player_splits"
        in result["blockers"]
    )
    assert (
        "minimum_executed_player_splits"
        in result["blockers"]
    )


def test_blocks_probability_delta_breach():
    payload = audit_payload()
    payload[
        "maximum_absolute_probability_delta"
    ]["p95"] = 0.081

    result = (
        evaluate_hitter_profile_canary_acceptance(
            payload
        )
    )

    assert result["gate_passed"] is False
    assert (
        "maximum_p95_probability_delta"
        in result["blockers"]
    )


def test_blocks_outcome_specific_breach():
    payload = audit_payload()
    payload[
        "absolute_probability_delta_by_outcome"
    ]["hr"]["p95"] = 0.031

    result = (
        evaluate_hitter_profile_canary_acceptance(
            payload
        )
    )

    assert result["gate_passed"] is False
    assert (
        "hr_p95_probability_delta"
        in result["blockers"]
    )


def test_blocks_safety_or_fallback_failure():
    payload = audit_payload()
    payload["fallback_telemetry"][
        "fallback_rate"
    ] = 0.051
    payload["safety_checks"][
        "all_production_inputs_unchanged"
    ] = False

    result = (
        evaluate_hitter_profile_canary_acceptance(
            payload
        )
    )

    assert result["gate_passed"] is False
    assert (
        "maximum_fallback_rate"
        in result["blockers"]
    )
    assert (
        "production_inputs_unchanged"
        in result["blockers"]
    )


def test_missing_metrics_fail_closed():
    result = (
        evaluate_hitter_profile_canary_acceptance(
            {}
        )
    )

    assert result["gate_passed"] is False
    assert result["blockers"]
    assert result["decision"][
        "production_activation_allowed"
    ] is False
