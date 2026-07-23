from copy import deepcopy
from types import SimpleNamespace

from mlb_app import model_projections


class QueryStub:
    def __init__(self, rows):
        self.rows = rows

    def filter(self, *args, **kwargs):
        return self

    def order_by(self, *args, **kwargs):
        return self

    def all(self):
        return list(self.rows)


class SessionStub:
    def __init__(self, rows):
        self.rows = rows

    def query(self, model):
        return QueryStub(self.rows)


def shared_simulation():
    return {
        "status": "ok",
        "diagnostics": {
            "canonical_shadow": {
                "status": "available",
                "authoritative_source": "legacy",
                "player_projections": {
                    "schema_version": (
                        "canonical_player_projection_rows_v1"
                    ),
                    "source_schema_version": (
                        "canonical_projection_payload_v1"
                    ),
                    "run_id": "run-123",
                    "model_version": "canonical-v1",
                    "simulation_count": 25,
                    "players": [
                        {
                            "player_id": "101",
                            "player_type": "batter",
                            "team_side": "away",
                            "projected_dfs_points": 10.5,
                            "metrics": {},
                        },
                        {
                            "player_id": (
                                "synthetic_pitcher"
                            ),
                            "player_type": "pitcher",
                            "team_side": "home",
                            "projected_dfs_points": 4.0,
                            "metrics": {},
                        },
                    ],
                    "identity_enrichment_applied": False,
                    "authoritative": False,
                    "authoritative_source": "legacy",
                },
            },
        },
    }


def identity():
    return SimpleNamespace(
        mlb_player_id=101,
        full_name="Resolved Batter",
        team_id=12,
        team_name="Away Club",
        primary_position="3B",
        player_type="batter",
        is_active=True,
    )


def test_enriches_attached_player_rows_from_same_run():
    source = shared_simulation()

    result = (
        model_projections
        ._enrich_game_workspace_player_projections(
            session=SessionStub([identity()]),
            shared_simulation=source,
        )
    )

    projections = result["diagnostics"][
        "canonical_shadow"
    ]["player_projections"]

    assert projections["run_id"] == "run-123"
    assert projections["model_version"] == (
        "canonical-v1"
    )
    assert projections["simulation_count"] == 25
    assert (
        projections[
            "identity_enrichment_applied"
        ]
        is True
    )

    resolved = projections["players"][0]

    assert resolved["mlb_player_id"] == 101
    assert resolved["full_name"] == (
        "Resolved Batter"
    )
    assert resolved["team_name"] == "Away Club"
    assert (
        resolved["projected_dfs_points"]
        == 10.5
    )


def test_preserves_unresolved_rows():
    result = (
        model_projections
        ._enrich_game_workspace_player_projections(
            session=SessionStub([identity()]),
            shared_simulation=shared_simulation(),
        )
    )

    unresolved = result["diagnostics"][
        "canonical_shadow"
    ]["player_projections"]["players"][1]

    assert unresolved["player_id"] == (
        "synthetic_pitcher"
    )
    assert unresolved["full_name"] is None
    assert unresolved[
        "identity_resolution_status"
    ] == "unresolved"
    assert (
        unresolved["projected_dfs_points"]
        == 4.0
    )


def test_missing_projection_attachment_is_unchanged():
    source = {
        "status": "ok",
        "diagnostics": {
            "canonical_shadow": {
                "status": "unavailable",
            },
        },
    }
    original = deepcopy(source)

    result = (
        model_projections
        ._enrich_game_workspace_player_projections(
            session=SessionStub([]),
            shared_simulation=source,
        )
    )

    assert result == original


def test_identity_failure_fails_open(monkeypatch):
    source = shared_simulation()

    def fail(**kwargs):
        raise RuntimeError("identity lookup failed")

    monkeypatch.setattr(
        model_projections,
        "enrich_canonical_player_projection_rows",
        fail,
    )

    result = (
        model_projections
        ._enrich_game_workspace_player_projections(
            session=SessionStub([]),
            shared_simulation=source,
        )
    )

    projections = result["diagnostics"][
        "canonical_shadow"
    ]["player_projections"]

    assert len(projections["players"]) == 2
    assert (
        projections[
            "identity_enrichment_applied"
        ]
        is False
    )

    diagnostics = projections[
        "identity_enrichment"
    ]

    assert diagnostics["status"] == "error"
    assert diagnostics["error_type"] == (
        "RuntimeError"
    )
    assert diagnostics["error_message"] == (
        "identity lookup failed"
    )


def test_non_dictionary_shared_simulation_is_preserved():
    value = (
        model_projections
        ._enrich_game_workspace_player_projections(
            session=SessionStub([]),
            shared_simulation=None,
        )
    )

    assert value is None
