from mlb_app.simulation.projections import (
    DRAFTKINGS_PROJECTION_MATCH_SCHEMA_VERSION,
    ingest_draftkings_salary_csv,
    match_canonical_projections_to_draftkings,
)


HEADER = (
    "Position,Name + ID,Name,ID,"
    "Roster Position,Salary,Game Info,"
    "TeamAbbrev,AvgPointsPerGame,"
    "Status,Starting\n"
)


def slate():
    return ingest_draftkings_salary_csv(
        HEADER
        + 'OF,"José Ramírez (1)",'
        "José Ramírez,1,3B/UTIL,6000,"
        "CLE@DET 07/22/2026 07:10PM ET,"
        "CLE,10.0,,Yes\n"
        + 'SP,"Pitcher One (2)",'
        "Pitcher One,2,P,9000,"
        "CLE@DET 07/22/2026 07:10PM ET,"
        "DET,18.0,,Yes\n"
        + 'OF,"Missing Player (3)",'
        "Missing Player,3,OF/UTIL,4000,"
        "CLE@DET 07/22/2026 07:10PM ET,"
        "CLE,5.0,,No\n"
    )


def projections():
    return {
        "schema_version": (
            "canonical_player_projection_rows_v1"
        ),
        "simulation_count": 1000,
        "players": [
            {
                "player_id": "100",
                "mlb_player_id": 100,
                "full_name": "Jose Ramirez",
                "player_type": "batter",
                "team_name": "CLE",
                "projected_dfs_points": 12.0,
                "dfs_floor": 4.0,
                "dfs_median": 10.0,
                "dfs_ceiling": 24.0,
                "metrics": {"runs": {"mean": 0.8}},
            },
            {
                "player_id": "200",
                "mlb_player_id": 200,
                "full_name": "Pitcher One",
                "player_type": "pitcher",
                "team_name": "DET",
                "projected_dfs_points": 21.0,
                "dfs_floor": 8.0,
                "dfs_median": 19.0,
                "dfs_ceiling": 34.0,
                "metrics": {
                    "strikeouts": {"mean": 7.0}
                },
            },
            {
                "player_id": "300",
                "mlb_player_id": 300,
                "full_name": "Off Slate",
                "player_type": "batter",
                "team_name": "CHC",
                "projected_dfs_points": 9.0,
                "dfs_floor": 2.0,
                "dfs_median": 7.0,
                "dfs_ceiling": 18.0,
                "metrics": {},
            },
        ],
        "identity_enrichment_applied": True,
        "authoritative": False,
        "authoritative_source": "legacy",
    }


def test_exact_normalized_name_team_type_matching():
    value = match_canonical_projections_to_draftkings(
        projection_payload=projections(),
        slate=slate(),
    )

    assert value["schema_version"] == (
        DRAFTKINGS_PROJECTION_MATCH_SCHEMA_VERSION
    )

    jose = next(
        row
        for row in value["players"]
        if row["dk_player_id"] == "1"
    )

    assert jose["match_status"] == "matched"
    assert jose["mlb_player_id"] == 100
    assert jose["position"] == "OF"
    assert jose["roster_positions"] == [
        "3B",
        "UTIL",
    ]
    assert jose["salary"] == 6000
    assert jose["projected_dfs_points"] == 12.0
    assert jose["value_per_1000"] == 2.0
    assert jose["floor_value_per_1000"] == (
        0.666667
    )
    assert jose["median_value_per_1000"] == (
        1.666667
    )
    assert jose["ceiling_value_per_1000"] == 4.0


def test_pitcher_type_must_be_compatible():
    value = match_canonical_projections_to_draftkings(
        projection_payload=projections(),
        slate=slate(),
    )

    pitcher = next(
        row
        for row in value["players"]
        if row["dk_player_id"] == "2"
    )

    assert pitcher["match_status"] == "matched"
    assert pitcher["player_type"] == "pitcher"
    assert pitcher["mlb_player_id"] == 200


def test_unmatched_draftkings_rows_are_preserved():
    value = match_canonical_projections_to_draftkings(
        projection_payload=projections(),
        slate=slate(),
    )

    missing = next(
        row
        for row in value["players"]
        if row["dk_player_id"] == "3"
    )

    assert missing["match_status"] == "unmatched"
    assert missing["salary"] == 4000
    assert missing["projected_dfs_points"] is None
    assert missing["value_per_1000"] is None


def test_diagnostics_report_unmatched_canonical_rows():
    value = match_canonical_projections_to_draftkings(
        projection_payload=projections(),
        slate=slate(),
    )

    diagnostics = value["diagnostics"]

    assert diagnostics["matched_player_count"] == 2
    assert (
        diagnostics[
            "unmatched_draftkings_player_count"
        ]
        == 1
    )
    assert (
        diagnostics[
            "unmatched_canonical_player_count"
        ]
        == 1
    )
    assert diagnostics["fuzzy_matching_used"] is False
    assert diagnostics[
        "unmatched_canonical_players"
    ][0]["full_name"] == "Off Slate"


def test_ambiguous_matches_are_not_selected():
    duplicate = projections()
    duplicate["players"].append(
        {
            "player_id": "101",
            "mlb_player_id": 101,
            "full_name": "José Ramírez",
            "player_type": "batter",
            "team_name": "CLE",
            "projected_dfs_points": 11.0,
            "dfs_floor": 3.0,
            "dfs_median": 9.0,
            "dfs_ceiling": 20.0,
            "metrics": {},
        }
    )

    value = match_canonical_projections_to_draftkings(
        projection_payload=duplicate,
        slate=slate(),
    )

    jose = next(
        row
        for row in value["players"]
        if row["dk_player_id"] == "1"
    )

    assert jose["match_status"] == "ambiguous"
    assert jose["match_candidate_count"] == 2
    assert jose["projected_dfs_points"] is None
