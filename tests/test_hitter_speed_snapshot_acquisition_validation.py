from copy import deepcopy

from mlb_app.simulation.shadow.hitter_speed_snapshot_acquisition_validation import (
    evaluate_hitter_speed_snapshot_acquisition_contract,
)


FIELDS = [
    "last_name, first_name",
    "player_id",
    "team_id",
    "team",
    "position",
    "age",
    "competitive_runs",
    "bolts",
    "hp_to_1b",
    "sprint_speed",
]


def observation(
    season,
    semantic_sha256,
):
    return {
        "season_requested": season,
        "http_status": 200,
        "content_type":
            "text/csv; charset=utf-8",
        "fieldnames": list(FIELDS),
        "row_count": 500,
        "unique_player_count": 500,
        "invalid_player_id_count": 0,
        "duplicate_player_id_count": 0,
        "underqualified_row_count": 0,
        "invalid_sprint_speed_count": 0,
        "raw_replay_identical": True,
        "semantic_replay_identical": True,
        "semantic_sha256":
            semantic_sha256,
        "season_field_present": False,
        "freshness_field_present": False,
    }


def evidence():
    return [
        observation(2024, "a" * 64),
        observation(2025, "b" * 64),
        observation(2026, "c" * 64),
    ]


def evaluate(rows=None, **overrides):
    values = {
        "historical_as_of_query_supported":
            False,
    }
    values.update(overrides)
    return (
        evaluate_hitter_speed_snapshot_acquisition_contract(
            evidence() if rows is None else rows,
            **values,
        )
    )


def test_confirms_acquisition_contract():
    result = evaluate()

    assert result["status"] == "ready"
    assert (
        result["acquisition_supported"]
        is True
    )
    assert (
        result["response_contract_ready"]
        is True
    )
    assert (
        result[
            "cross_season_responses_distinct"
        ]
        is True
    )
    assert result["observed_seasons"] == [
        2024,
        2025,
        2026,
    ]


def test_allows_prospective_collection():
    result = evaluate()

    assert (
        result[
            "prospective_collection_allowed"
        ]
        is True
    )
    assert (
        result["decision"][
            "begin_prospective_collection"
        ]
        is True
    )
    assert result["recommended_next_slice"] == (
        "begin_prospective_hitter_speed_"
        "snapshot_collection"
    )


def test_blocks_retrospective_evaluation():
    result = evaluate()

    assert (
        result[
            "retrospective_predictive_"
            "evaluation_allowed"
        ]
        is False
    )
    assert (
        "historical_as_of_query_unsupported"
        in result[
            "retrospective_temporal_blockers"
        ]
    )
    assert (
        "historical_capture_precedes_"
        "outcomes_unverified"
        in result[
            "retrospective_temporal_blockers"
        ]
    )
    assert (
        result["decision"][
            "run_retrospective_speed_audit"
        ]
        is False
    )


def test_requires_external_capture_metadata():
    result = evaluate()

    assert (
        result[
            "season_metadata_external_required"
        ]
        is True
    )
    assert (
        result[
            "freshness_metadata_external_required"
        ]
        is True
    )


def test_missing_required_field_blocks():
    rows = evidence()
    rows[0]["fieldnames"].remove(
        "sprint_speed"
    )

    result = evaluate(rows)

    assert result["status"] == "blocked"
    assert (
        result["acquisition_supported"]
        is False
    )
    assert "required_csv_fields_missing" in (
        result["blockers"]
    )


def test_duplicate_identity_blocks():
    rows = evidence()
    rows[1][
        "duplicate_player_id_count"
    ] = 1

    result = evaluate(rows)

    assert result["status"] == "blocked"
    assert (
        "player_identity_contract_failed"
        in result["blockers"]
    )


def test_non_deterministic_replay_blocks():
    rows = evidence()
    rows[2]["raw_replay_identical"] = False

    result = evaluate(rows)

    assert result["status"] == "blocked"
    assert (
        "response_replay_not_deterministic"
        in result["blockers"]
    )


def test_cross_season_collision_blocks():
    rows = evidence()
    rows[2]["semantic_sha256"] = (
        rows[1]["semantic_sha256"]
    )

    result = evaluate(rows)

    assert result["status"] == "blocked"
    assert (
        "cross_season_responses_not_distinct"
        in result["blockers"]
    )


def test_input_order_does_not_change_result():
    rows = evidence()

    first = evaluate(rows)
    second = evaluate(
        list(reversed(deepcopy(rows)))
    )

    assert first == second


def test_no_production_authority():
    result = evaluate()

    assert result["parameter_selected"] is False
    assert (
        result["production_authority_changed"]
        is False
    )
    assert result["shadow_only"] is True

    impact = result["production_impact"]
    assert (
        impact["external_fetch_performed"]
        is False
    )
    assert (
        impact["database_writes_performed"]
        is False
    )
    assert (
        impact["production_model_modified"]
        is False
    )
    assert (
        impact["simulation_authority_changed"]
        is False
    )
