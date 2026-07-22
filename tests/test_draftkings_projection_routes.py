import pytest

pytest.importorskip("fastapi")

from fastapi import FastAPI
from fastapi.testclient import TestClient

from mlb_app.draftkings_projection_routes import (
    router,
)


HEADER = (
    "Position,Name + ID,Name,ID,"
    "Roster Position,Salary,Game Info,"
    "TeamAbbrev,AvgPointsPerGame,"
    "Status,Starting\n"
)


def client():
    app = FastAPI()
    app.include_router(router)
    return TestClient(app)


def projection_payload():
    return {
        "schema_version": (
            "canonical_player_projection_rows_v1"
        ),
        "simulation_count": 100,
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
                "metrics": {},
            }
        ],
        "identity_enrichment_applied": True,
        "authoritative": False,
        "authoritative_source": "legacy",
    }


def salary_csv():
    return (
        HEADER
        + 'OF,"José Ramírez (1)",'
        "José Ramírez,1,3B/UTIL,6000,"
        "CLE@DET 07/22/2026 07:10PM ET,"
        "CLE,10.0,,Yes\n"
    )


def test_preview_route_returns_matched_rows():
    response = client().post(
        "/dfs/draftkings/projections/preview",
        json={
            "projection_payload": (
                projection_payload()
            ),
            "salary_csv": salary_csv(),
            "source_filename": "DKSalaries.csv",
        },
    )

    assert response.status_code == 200

    payload = response.json()

    assert payload["schema_version"] == (
        "draftkings_projection_match_v1"
    )
    assert payload["preview"] is True
    assert payload["persistent"] is False
    assert (
        payload["lineup_generation_applied"]
        is False
    )

    player = payload["players"][0]

    assert player["match_status"] == "matched"
    assert player["salary"] == 6000
    assert player["projected_dfs_points"] == 12.0
    assert player["value_per_1000"] == 2.0


def test_preview_route_preserves_diagnostics():
    response = client().post(
        "/dfs/draftkings/projections/preview",
        json={
            "projection_payload": (
                projection_payload()
            ),
            "salary_csv": salary_csv(),
        },
    )

    diagnostics = response.json()["diagnostics"]

    assert diagnostics["matched_player_count"] == 1
    assert (
        diagnostics[
            "unmatched_draftkings_player_count"
        ]
        == 0
    )
    assert diagnostics["fuzzy_matching_used"] is False


def test_preview_route_rejects_invalid_csv():
    response = client().post(
        "/dfs/draftkings/projections/preview",
        json={
            "projection_payload": (
                projection_payload()
            ),
            "salary_csv": (
                "Name,ID\n"
                "Player,1\n"
            ),
        },
    )

    assert response.status_code == 422
    assert (
        "missing required DraftKings columns"
        in response.json()["detail"]
    )


def test_preview_route_rejects_invalid_projection_shape():
    response = client().post(
        "/dfs/draftkings/projections/preview",
        json={
            "projection_payload": {
                "players": "invalid"
            },
            "salary_csv": salary_csv(),
        },
    )

    assert response.status_code == 422
    assert (
        response.json()["detail"]
        == (
            "projection players must be "
            "a list or tuple"
        )
    )
