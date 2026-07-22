import pytest

from mlb_app.simulation.projections import (
    CANONICAL_PLAYER_PROJECTION_ROWS_VERSION,
    canonical_player_projection_rows,
)


def summary(
    *,
    mean,
    median=None,
    p10=0.0,
    p25=0.0,
    p75=0.0,
    p90=0.0,
    minimum=0.0,
    maximum=0.0,
    count=2,
):
    return {
        "count": count,
        "mean": mean,
        "median": mean if median is None else median,
        "p10": p10,
        "p25": p25,
        "p75": p75,
        "p90": p90,
        "minimum": minimum,
        "maximum": maximum,
    }


def metric(name, value):
    return {
        "name": name,
        "summary": value,
    }


def payload():
    return {
        "schema_version": (
            "canonical_projection_payload_v1"
        ),
        "run_id": "run-123",
        "model_version": "canonical-event-model-v1",
        "simulation_count": 2,
        "batters": [
            {
                "player_id": "away_batter",
                "team_side": "away",
                "metrics": [
                    metric(
                        "runs",
                        summary(mean=0.5),
                    ),
                    metric(
                        "dfs_points",
                        summary(
                            mean=9.5,
                            median=8.0,
                            p10=2.0,
                            p90=18.0,
                        ),
                    ),
                ],
            },
        ],
        "pitchers": [
            {
                "player_id": "home_pitcher",
                "team_side": "home",
                "metrics": [
                    metric(
                        "strikeouts",
                        summary(mean=6.0),
                    ),
                ],
            },
        ],
        "diagnostics": {
            "warnings": [],
            "pitcher_attribution_complete_rate": 1.0,
            "replay_validation_pass_rate": 1.0,
        },
    }


def test_rows_flatten_existing_canonical_payload():
    value = canonical_player_projection_rows(
        payload()
    )

    assert value["schema_version"] == (
        CANONICAL_PLAYER_PROJECTION_ROWS_VERSION
    )
    assert value["simulation_count"] == 2
    assert len(value["players"]) == 2

    batter = value["players"][0]

    assert batter["player_id"] == "away_batter"
    assert batter["player_type"] == "batter"
    assert batter["projected_dfs_points"] == 9.5
    assert batter["dfs_floor"] == 2.0
    assert batter["dfs_median"] == 8.0
    assert batter["dfs_ceiling"] == 18.0
    assert batter["metrics"]["runs"]["mean"] == 0.5


def test_pitcher_without_dfs_metric_remains_displayable():
    value = canonical_player_projection_rows(
        payload()
    )

    pitcher = value["players"][1]

    assert pitcher["player_id"] == "home_pitcher"
    assert pitcher["player_type"] == "pitcher"
    assert pitcher["projected_dfs_points"] is None
    assert (
        pitcher["metrics"]["strikeouts"]["mean"]
        == 6.0
    )


def test_rows_are_deterministically_ordered():
    value = payload()
    value["batters"].append(
        {
            "player_id": "home_batter",
            "team_side": "home",
            "metrics": [
                metric(
                    "runs",
                    summary(mean=0.25),
                ),
            ],
        }
    )

    rows = canonical_player_projection_rows(
        value
    )["players"]

    assert [
        (
            row["team_side"],
            row["player_type"],
            row["player_id"],
        )
        for row in rows
    ] == [
        ("away", "batter", "away_batter"),
        ("home", "batter", "home_batter"),
        ("home", "pitcher", "home_pitcher"),
    ]


def test_metric_count_must_match_simulation_count():
    value = payload()
    value["batters"][0]["metrics"][0][
        "summary"
    ]["count"] = 1

    with pytest.raises(
        ValueError,
        match="metric count must match",
    ):
        canonical_player_projection_rows(
            value
        )


def test_invalid_team_side_is_rejected():
    value = payload()
    value["batters"][0]["team_side"] = (
        "neutral"
    )

    with pytest.raises(
        ValueError,
        match="team_side",
    ):
        canonical_player_projection_rows(
            value
        )
