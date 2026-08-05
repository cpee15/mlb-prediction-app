from copy import deepcopy

import pytest

from mlb_app.simulation.shadow.pregame_pitching_plan_materialization import (
    materialize_canonical_pregame_pitching_plan,
)


def classification(
    *,
    plan_type="opener_bulk",
    starter_id="100",
    follower_ids=("101",),
):
    roles = {
        "opener_bulk": (
            "opener",
            "bulk_follower",
        ),
        "tandem": (
            "tandem_primary",
            "tandem_secondary",
        ),
        "bullpen_game": (
            "opener",
            "reliever",
        ),
    }
    starter_role, follower_role = roles.get(
        plan_type,
        ("starter", "reliever"),
    )

    planned_sequence = [{
        "order": 1,
        "role": starter_role,
        "pitcher_id": starter_id,
    }]

    planned_sequence.extend(
        {
            "order": index + 2,
            "role": follower_role,
            "pitcher_id": pitcher_id,
        }
        for index, pitcher_id in enumerate(
            follower_ids
        )
    )

    return {
        "plan_type": plan_type,
        "fallback_used": False,
        "planned_sequence": planned_sequence,
    }


def materialize(
    *,
    team_side="away",
    starter_id="100",
    bullpen_pitcher_ids=(
        "101",
        "102",
        "103",
    ),
    plan_classification=None,
):
    if plan_classification is None:
        plan_classification = classification()

    return (
        materialize_canonical_pregame_pitching_plan(
            team_side=team_side,
            starter_id=starter_id,
            bullpen_pitcher_ids=(
                bullpen_pitcher_ids
            ),
            classification=(
                plan_classification
            ),
        )
    )


def test_materializes_opener_bulk_sequence():
    result = materialize()

    assert result.status == "ready"
    assert result.ready is True
    assert result.fallback_used is False
    assert result.blockers == ()
    assert result.pitching_plan.plan_type == (
        "opener_bulk"
    )
    assert (
        result.pitching_plan
        .preferred_replacement_pitcher_ids
        == ("101",)
    )


def test_materializes_tandem_sequence():
    result = materialize(
        plan_classification=classification(
            plan_type="tandem",
        )
    )

    assert result.pitching_plan.plan_type == (
        "tandem"
    )
    assert (
        result.pitching_plan
        .preferred_replacement_pitcher_ids
        == ("101",)
    )


def test_materializes_bullpen_game_sequence():
    result = materialize(
        plan_classification=classification(
            plan_type="bullpen_game",
            follower_ids=("101", "102"),
        )
    )

    assert result.pitching_plan.plan_type == (
        "bullpen_game"
    )
    assert (
        result.pitching_plan
        .preferred_replacement_pitcher_ids
        == ("101", "102")
    )


def test_traditional_plan_has_no_preferred_follower():
    result = materialize(
        plan_classification=classification(
            plan_type="traditional_starter",
            follower_ids=("101",),
        )
    )

    assert result.pitching_plan.plan_type == (
        "traditional_starter"
    )
    assert (
        result.pitching_plan
        .preferred_replacement_pitcher_ids
        == ()
    )


def test_workload_capped_plan_has_no_fixed_follower():
    result = materialize(
        plan_classification=classification(
            plan_type=(
                "workload_capped_starter"
            ),
            follower_ids=("101",),
        )
    )

    assert result.pitching_plan.plan_type == (
        "workload_capped_starter"
    )
    assert (
        result.pitching_plan
        .preferred_replacement_pitcher_ids
        == ()
    )


def test_missing_classification_falls_back_safely():
    result = (
        materialize_canonical_pregame_pitching_plan(
            team_side="away",
            starter_id="100",
            bullpen_pitcher_ids=("101",),
            classification=None,
        )
    )

    assert result.fallback_used is True
    assert result.pitching_plan.plan_type == (
        "traditional_starter"
    )
    assert result.blockers == (
        "classification_unavailable",
    )


def test_unknown_classification_falls_back_safely():
    result = materialize(
        plan_classification={
            "plan_type": "unknown_fallback",
            "fallback_used": True,
            "planned_sequence": [],
        }
    )

    assert result.fallback_used is True
    assert result.pitching_plan.plan_type == (
        "traditional_starter"
    )
    assert result.blockers == (
        "classification_fallback_used",
    )


def test_unsupported_type_falls_back_safely():
    result = materialize(
        plan_classification={
            "plan_type": "invented_plan",
            "fallback_used": False,
            "planned_sequence": [],
        }
    )

    assert result.fallback_used is True
    assert result.pitching_plan.plan_type == (
        "traditional_starter"
    )
    assert result.blockers == (
        "unsupported_plan_type",
    )


def test_unavailable_follower_is_not_materialized():
    result = materialize(
        plan_classification=classification(
            follower_ids=("999",),
        )
    )

    assert result.pitching_plan.plan_type == (
        "opener_bulk"
    )
    assert (
        result.pitching_plan
        .preferred_replacement_pitcher_ids
        == ()
    )
    assert result.blockers == (
        "planned_pitcher_not_in_bullpen",
    )
    assert result.diagnostics[
        "unavailable_planned_pitcher_count"
    ] == 1


def test_starter_is_not_repeated_as_follower():
    result = materialize(
        plan_classification=classification(
            follower_ids=("100", "101"),
        )
    )

    assert (
        result.pitching_plan
        .preferred_replacement_pitcher_ids
        == ("101",)
    )


def test_duplicate_followers_are_deduplicated():
    result = materialize(
        plan_classification=classification(
            follower_ids=(
                "101",
                "101",
                "102",
            ),
        )
    )

    assert (
        result.pitching_plan
        .preferred_replacement_pitcher_ids
        == ("101", "102")
    )


def test_invalid_sequence_records_are_diagnostic():
    value = classification()
    value["planned_sequence"].append(None)
    value["planned_sequence"].append({
        "order": 3,
        "role": "reliever",
        "pitcher_id": "",
    })

    result = materialize(
        plan_classification=value
    )

    assert result.blockers == (
        "invalid_planned_sequence_record",
    )
    assert result.diagnostics[
        "invalid_sequence_record_count"
    ] == 2


def test_materialization_is_deterministic():
    value = classification(
        follower_ids=("102", "101"),
    )

    first = materialize(
        plan_classification=deepcopy(value)
    )
    second = materialize(
        plan_classification=deepcopy(value)
    )

    assert first == second
    assert (
        first.pitching_plan
        .preferred_replacement_pitcher_ids
        == ("102", "101")
    )


def test_diagnostics_do_not_expose_pitcher_ids():
    result = materialize()
    diagnostics = str(result.diagnostics)

    assert "100" not in diagnostics
    assert "101" not in diagnostics
    assert "102" not in diagnostics


def test_is_read_only_and_non_authoritative():
    result = materialize()

    assert (
        result.database_writes_performed
        is False
    )
    assert (
        result.production_authority_changed
        is False
    )
    assert result.diagnostics[
        "database_writes_performed"
    ] is False
    assert result.diagnostics[
        "production_authority_changed"
    ] is False


def test_requires_starter_identity():
    with pytest.raises(
        ValueError,
        match="starter_id",
    ):
        materialize(
            starter_id=None,
        )


def test_validates_team_side_through_contract():
    with pytest.raises(
        ValueError,
        match="team_side",
    ):
        materialize(
            team_side="neutral",
        )


def test_materializes_correct_home_team_side():
    result = materialize(
        team_side="home",
        starter_id="200",
        bullpen_pitcher_ids=(
            "201",
            "202",
        ),
        plan_classification=classification(
            starter_id="200",
            follower_ids=("201",),
        ),
    )

    assert result.pitching_plan.team_side == "home"
    assert result.pitching_plan.starter_id == "200"
    assert (
        result.pitching_plan
        .preferred_replacement_pitcher_ids
        == ("201",)
    )
