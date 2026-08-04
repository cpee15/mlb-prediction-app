from dataclasses import replace

import pytest

from mlb_app.simulation.game.matchup_input import (
    CanonicalPitchingPlan,
)
from mlb_app.simulation.shadow.canonical_pitcher_role_and_innings_attribution_audit import (
    audit_canonical_pitcher_role_and_innings_attribution,
)


def summary(
    *,
    mean,
    p10=None,
    p90=None,
    maximum=None,
):
    return {
        "count": 1000,
        "minimum": 0.0,
        "p10": p10,
        "median": mean,
        "mean": mean,
        "p90": p90,
        "p95": p90,
        "maximum": maximum,
    }


def pitcher(
    player_id,
    team_side,
    *,
    mean_outs,
):
    return {
        "player_id": player_id,
        "team_side": team_side,
        "metrics": [
            {
                "name": "outs_recorded",
                "summary": summary(
                    mean=mean_outs,
                    p10=mean_outs - 3,
                    p90=mean_outs + 3,
                    maximum=mean_outs + 6,
                ),
            },
        ],
    }


def traditional_plan(team_side):
    return CanonicalPitchingPlan(
        team_side=team_side,
        starter_id=f"{team_side}_starter",
        bullpen_pitcher_ids=(
            f"{team_side}_reliever",
        ),
    )


def opener_plan(team_side):
    return CanonicalPitchingPlan(
        team_side=team_side,
        starter_id=f"{team_side}_opener",
        bullpen_pitcher_ids=(
            f"{team_side}_bulk",
            f"{team_side}_reliever",
        ),
        plan_type="opener_bulk",
        preferred_replacement_pitcher_ids=(
            f"{team_side}_bulk",
        ),
    )


def payload():
    return {
        "schema_version":
            "canonical_projection_payload_v1",
        "simulation_count": 1000,
        "pitchers": [
            pitcher(
                "away_opener",
                "away",
                mean_outs=5.0,
            ),
            pitcher(
                "away_bulk",
                "away",
                mean_outs=13.0,
            ),
            pitcher(
                "away_reliever",
                "away",
                mean_outs=5.0,
            ),
            pitcher(
                "home_starter",
                "home",
                mean_outs=17.0,
            ),
            pitcher(
                "home_reliever",
                "home",
                mean_outs=7.0,
            ),
        ],
    }


def record(result, player_id):
    return next(
        value
        for value in result["records"]
        if value["player_id"] == player_id
    )


def test_attributes_opener_bulk_and_traditional_roles():
    result = (
        audit_canonical_pitcher_role_and_innings_attribution(
            projection_payload=payload(),
            away_pitching_plan=opener_plan(
                "away"
            ),
            home_pitching_plan=traditional_plan(
                "home"
            ),
        )
    )

    assert result["status"] == "observed"
    assert result[
        "role_attribution_complete_rate"
    ] == 1.0
    assert record(
        result,
        "away_opener",
    )["role"] == "opener"
    assert record(
        result,
        "away_bulk",
    )["role"] == "bulk_follower"
    assert record(
        result,
        "away_reliever",
    )["role"] == "reliever"
    assert record(
        result,
        "home_starter",
    )["role"] == "starter"


def test_converts_outs_distribution_to_innings():
    result = (
        audit_canonical_pitcher_role_and_innings_attribution(
            projection_payload=payload(),
            away_pitching_plan=opener_plan(
                "away"
            ),
            home_pitching_plan=traditional_plan(
                "home"
            ),
        )
    )

    bulk = record(result, "away_bulk")

    assert bulk["outs_recorded"]["mean"] == 13.0
    assert bulk["innings_pitched"]["mean"] == (
        13.0 / 3.0
    )
    assert bulk["innings_pitched"]["p90"] == (
        16.0 / 3.0
    )


def test_reports_pitcher_projected_outside_plan():
    projection_payload = payload()
    projection_payload["pitchers"].append(
        pitcher(
            "home_unplanned",
            "home",
            mean_outs=3.0,
        )
    )

    result = (
        audit_canonical_pitcher_role_and_innings_attribution(
            projection_payload=projection_payload,
            away_pitching_plan=opener_plan(
                "away"
            ),
            home_pitching_plan=traditional_plan(
                "home"
            ),
        )
    )

    unexpected = record(
        result,
        "home_unplanned",
    )

    assert unexpected["role"] == (
        "unexpected_pitcher"
    )
    assert (
        "projected_pitcher_outside_plan"
        in unexpected["anomalies"]
    )
    assert result["anomaly_counts"][
        "projected_pitcher_outside_plan"
    ] == 1


def test_reports_missing_starter_and_bulk_follower():
    projection_payload = payload()
    projection_payload["pitchers"] = [
        value
        for value in projection_payload[
            "pitchers"
        ]
        if value["player_id"] not in {
            "away_opener",
            "away_bulk",
        }
    ]

    result = (
        audit_canonical_pitcher_role_and_innings_attribution(
            projection_payload=projection_payload,
            away_pitching_plan=opener_plan(
                "away"
            ),
            home_pitching_plan=traditional_plan(
                "home"
            ),
        )
    )

    assert result["anomaly_counts"][
        "planned_starter_not_projected"
    ] == 1
    assert result["anomaly_counts"][
        "preferred_replacement_not_projected"
    ] == 1


def test_reports_external_role_conflict():
    result = (
        audit_canonical_pitcher_role_and_innings_attribution(
            projection_payload=payload(),
            away_pitching_plan=opener_plan(
                "away"
            ),
            home_pitching_plan=traditional_plan(
                "home"
            ),
            expected_roles_by_id={
                "home_starter": "reliever",
            },
        )
    )

    starter = record(result, "home_starter")

    assert starter["expected_role"] == (
        "reliever"
    )
    assert "expected_role_mismatch" in (
        starter["anomalies"]
    )


def test_attributes_tandem_roles():
    tandem = replace(
        opener_plan("away"),
        plan_type="tandem",
    )

    result = (
        audit_canonical_pitcher_role_and_innings_attribution(
            projection_payload=payload(),
            away_pitching_plan=tandem,
            home_pitching_plan=traditional_plan(
                "home"
            ),
        )
    )

    assert record(
        result,
        "away_opener",
    )["role"] == "tandem_primary"
    assert record(
        result,
        "away_bulk",
    )["role"] == "tandem_secondary"


def test_missing_outs_metric_is_diagnostic():
    projection_payload = payload()
    projection_payload["pitchers"][0][
        "metrics"
    ] = []

    result = (
        audit_canonical_pitcher_role_and_innings_attribution(
            projection_payload=projection_payload,
            away_pitching_plan=opener_plan(
                "away"
            ),
            home_pitching_plan=traditional_plan(
                "home"
            ),
        )
    )

    opener = record(result, "away_opener")

    assert "outs_projection_unavailable" in (
        opener["anomalies"]
    )
    assert opener["innings_pitched"][
        "mean"
    ] is None


def test_audit_is_non_authoritative_and_read_only():
    result = (
        audit_canonical_pitcher_role_and_innings_attribution(
            projection_payload=payload(),
            away_pitching_plan=opener_plan(
                "away"
            ),
            home_pitching_plan=traditional_plan(
                "home"
            ),
        )
    )

    assert (
        result["database_writes_performed"]
        is False
    )
    assert (
        result["production_authority_changed"]
        is False
    )
    assert result["decision"][
        "production_activation_allowed"
    ] is False
    assert result["decision"][
        "sequencing_activation_allowed"
    ] is False


def test_rejects_invalid_inputs():
    with pytest.raises(
        TypeError,
        match="projection_payload",
    ):
        audit_canonical_pitcher_role_and_innings_attribution(
            projection_payload=object(),
            away_pitching_plan=opener_plan(
                "away"
            ),
            home_pitching_plan=traditional_plan(
                "home"
            ),
        )

    with pytest.raises(
        ValueError,
        match="team_side",
    ):
        audit_canonical_pitcher_role_and_innings_attribution(
            projection_payload=payload(),
            away_pitching_plan=traditional_plan(
                "home"
            ),
            home_pitching_plan=traditional_plan(
                "home"
            ),
        )
