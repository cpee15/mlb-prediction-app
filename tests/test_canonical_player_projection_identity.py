import datetime as dt

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from mlb_app.dashboard_object_models import (
    DashboardPlayerCurrent,
)
from mlb_app.database import Base
from mlb_app.simulation.projections import (
    CANONICAL_PLAYER_IDENTITY_ENRICHMENT_VERSION,
    enrich_canonical_player_projection_rows,
)


NOW = dt.datetime(2026, 7, 15, 12, 0, 0)


def make_session():
    engine = create_engine(
        "sqlite:///:memory:",
        future=True,
    )
    Base.metadata.create_all(engine)

    session = sessionmaker(
        bind=engine,
        future=True,
    )()

    session.add_all(
        [
            DashboardPlayerCurrent(
                mlb_player_id=100,
                snapshot_id=1,
                player_type="hitter",
                full_name="Away Batter",
                team_id=10,
                team_name="Away Club",
                primary_position="RF",
                is_active=True,
                metrics_json={},
                projection_version="projection-v1",
                source_freshness_json={},
                provenance_json={},
                promoted_at=NOW,
                updated_at=NOW,
            ),
            DashboardPlayerCurrent(
                mlb_player_id=200,
                snapshot_id=2,
                player_type="pitcher",
                full_name="Home Pitcher",
                team_id=20,
                team_name="Home Club",
                primary_position="SP",
                is_active=False,
                metrics_json={},
                projection_version="projection-v1",
                source_freshness_json={},
                provenance_json={},
                promoted_at=NOW,
                updated_at=NOW,
            ),
        ]
    )
    session.commit()

    return session


def payload():
    return {
        "schema_version": (
            "canonical_player_projection_rows_v1"
        ),
        "simulation_count": 25,
        "players": [
            {
                "player_id": "100",
                "player_type": "batter",
                "team_side": "away",
                "projected_dfs_points": 9.5,
                "metrics": {},
            },
            {
                "player_id": "200",
                "player_type": "pitcher",
                "team_side": "home",
                "projected_dfs_points": 16.0,
                "metrics": {},
            },
            {
                "player_id": "synthetic_batter",
                "player_type": "batter",
                "team_side": "away",
                "projected_dfs_points": 4.0,
                "metrics": {},
            },
        ],
        "identity_enrichment_applied": False,
        "authoritative": False,
        "authoritative_source": "legacy",
    }


def test_identity_enrichment_resolves_dashboard_players():
    value = enrich_canonical_player_projection_rows(
        session=make_session(),
        payload=payload(),
    )

    batter = value["players"][0]
    pitcher = value["players"][1]

    assert batter["mlb_player_id"] == 100
    assert batter["full_name"] == "Away Batter"
    assert batter["team_name"] == "Away Club"
    assert batter["primary_position"] == "RF"
    assert batter["is_active"] is True

    assert pitcher["full_name"] == "Home Pitcher"
    assert pitcher["identity_player_type"] == (
        "pitcher"
    )
    assert pitcher["is_active"] is False


def test_unresolved_rows_are_preserved():
    value = enrich_canonical_player_projection_rows(
        session=make_session(),
        payload=payload(),
    )

    unresolved = value["players"][2]

    assert unresolved["player_id"] == (
        "synthetic_batter"
    )
    assert unresolved["full_name"] is None
    assert unresolved[
        "identity_resolution_status"
    ] == "unresolved"
    assert unresolved["projected_dfs_points"] == 4.0


def test_enrichment_diagnostics_are_explicit():
    value = enrich_canonical_player_projection_rows(
        session=make_session(),
        payload=payload(),
    )

    diagnostics = value[
        "identity_enrichment"
    ]

    assert diagnostics["schema_version"] == (
        CANONICAL_PLAYER_IDENTITY_ENRICHMENT_VERSION
    )
    assert diagnostics["requested_player_count"] == 3
    assert diagnostics["numeric_player_id_count"] == 2
    assert diagnostics["resolved_player_count"] == 2
    assert diagnostics["unresolved_player_count"] == 1
    assert diagnostics["inactive_player_count"] == 1
    assert diagnostics["unresolved_player_ids"] == [
        "synthetic_batter"
    ]


def test_input_payload_is_not_mutated():
    original = payload()

    enrich_canonical_player_projection_rows(
        session=make_session(),
        payload=original,
    )

    assert original[
        "identity_enrichment_applied"
    ] is False
    assert "full_name" not in original["players"][0]
