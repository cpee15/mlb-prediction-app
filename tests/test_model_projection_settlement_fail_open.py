from contextlib import contextmanager

from mlb_app import model_projections


class Session:
    @contextmanager
    def begin_nested(self):
        yield


def test_settlement_diagnostics_are_ready_when_storage_loads(
    monkeypatch,
):
    monkeypatch.setattr(
        model_projections,
        "load_canonical_baserunning_production_settlements",
        lambda session: (),
    )

    result = (
        model_projections
        ._load_production_settlement_diagnostics(
            Session()
        )
    )

    assert result["status"] == "ready"
    assert result["storage_available"] is True
    assert result["settled_game_count"] == 0
    assert result["error_type"] is None


def test_settlement_storage_failure_is_diagnostic_only(
    monkeypatch,
):
    def unavailable(session):
        raise RuntimeError(
            "settlement table is unavailable"
        )

    monkeypatch.setattr(
        model_projections,
        "load_canonical_baserunning_production_settlements",
        unavailable,
    )

    result = (
        model_projections
        ._load_production_settlement_diagnostics(
            Session()
         )
    )

    assert result["status"] == "unavailable"
    assert result["storage_available"] is False
    assert result["settled_game_count"] == 0
    assert result["remaining_game_count"] == 100
    assert result["error_type"] == "RuntimeError"
    assert result["error_message"] == (
        "settlement table is unavailable"
    )
    assert (
        result["production_authority_changed"]
        is False
    )
