from scripts.audit_shadow_hitter_profile_canary import (
    percentile,
    summarize_canary_records,
)


def canary(
    maximum_delta,
    *,
    fallback=False,
    normalized=True,
):
    return {
        "production_inputs_unchanged": True,
        "production_authority_changed": False,
        "candidate_probability_sum": (
            1.0 if normalized else 1.1
        ),
        "maximum_absolute_probability_delta":
            maximum_delta,
        "probability_deltas": {
            "bb": -0.01,
            "k": 0.02,
        },
        "fallback_telemetry": {
            "by_signal": {
                "walk_skill": {
                    "fallback_used": fallback,
                },
                "strikeout_skill": {
                    "fallback_used": False,
                },
                "power_skill": {
                    "fallback_used": False,
                },
                "hit_type_allocation": {
                    "fallback_used": False,
                },
            },
        },
    }


def test_percentile_interpolates():
    result = percentile(
        [0.01, 0.03],
        0.50,
    )

    assert result is not None
    assert abs(result - 0.02) < 1e-15
    assert percentile([], 0.95) is None


def test_summarizes_population_canary():
    records = [
        {
            "state": "executed",
            "executed": True,
            "blockers": [],
            "canary": canary(0.04),
        },
        {
            "state": "executed",
            "executed": True,
            "blockers": [],
            "canary": canary(
                0.02,
                fallback=True,
            ),
        },
        {
            "state": "signal_evidence_blocked",
            "executed": False,
            "blockers": [
                "insufficient_expected_coverage",
            ],
        },
    ]

    result = summarize_canary_records(
        records,
        season=2026,
        as_of_date="2026-05-03",
        candidate_count=10,
        limit=3,
    )

    assert result["status"] == "observed"
    assert (
        result["executed_player_split_count"]
        == 2
    )
    assert result["state_counts"] == {
        "executed": 2,
        "signal_evidence_blocked": 1,
    }
    assert result["blocker_counts"] == {
        "insufficient_expected_coverage": 1,
    }
    assert result[
        "fallback_telemetry"
    ]["fallback_count"] == 1
    assert result[
        "maximum_absolute_probability_delta"
    ]["median"] == 0.03
    assert result["safety_checks"][
        "all_production_inputs_unchanged"
    ] is True
    assert result["safety_checks"][
        "all_candidate_probabilities_normalized"
    ] is True
    assert result["decision"][
        "production_activation_allowed"
    ] is False
    assert (
        result["production_authority_changed"]
        is False
    )


def test_detects_probability_normalization_failure():
    result = summarize_canary_records(
        [
            {
                "state": "executed",
                "executed": True,
                "blockers": [],
                "canary": canary(
                    0.04,
                    normalized=False,
                ),
            },
        ],
        season=2026,
        as_of_date="2026-05-03",
        candidate_count=1,
        limit=1,
    )

    assert result["safety_checks"][
        "all_candidate_probabilities_normalized"
    ] is False
