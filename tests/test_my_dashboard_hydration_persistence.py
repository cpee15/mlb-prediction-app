from __future__ import annotations

from contextlib import contextmanager

from mlb_app import my_dashboard_hydration_persistence as persistence
from mlb_app import my_dashboard_observability as observability


class FakeSession:
    pass


@contextmanager
def fake_session_context():
    yield FakeSession()


class FakeFactory:
    def __call__(self):
        return fake_session_context()


def test_persist_hydration_payload_promotes_existing_component_results(monkeypatch):
    calls = []
    monkeypatch.setattr(persistence, "_session_factory", lambda: FakeFactory())
    monkeypatch.setattr(
        persistence,
        "dashboard_dataset_status",
        lambda **kwargs: {"ready": False, "dataset_row_count": 0},
    )

    def fake_hydrate(**kwargs):
        calls.append(kwargs)
        payload = kwargs["payload_builder"]()
        return {
            "hydrated": True,
            "dataset_row_count": len(payload["items"]),
            "dataset_version": "v1",
        }

    monkeypatch.setattr(persistence, "hydrate_dashboard_dataset", fake_hydrate)

    result = persistence.persist_hydration_payload(
        {
            "target_date": "2026-07-14",
            "active_lineups": True,
            "force_requested": True,
        },
        {
            "date": "2026-07-14",
            "results": {
                "hitters": {"items": [{"entity_id": "1"}, {"entity_id": "2"}]},
            },
        },
    )

    assert result["dataset_source"] == "my_dashboard_records"
    assert result["dataset_mode"] == "active_lineups"
    assert result["hydrated_component_count"] == 1
    assert result["dataset_row_count"] == 2
    assert calls[0]["active_lineups"] is True
    assert calls[0]["force"] is True
    assert calls[0]["payload_builder"]()["items"][0]["entity_id"] == "1"


def test_empty_hydration_payload_never_replaces_previous_dataset(monkeypatch):
    monkeypatch.setattr(persistence, "_session_factory", lambda: FakeFactory())
    monkeypatch.setattr(
        persistence,
        "dashboard_dataset_status",
        lambda **kwargs: {
            "ready": True,
            "dataset_version": "previous",
            "dataset_row_count": 12,
        },
    )
    monkeypatch.setattr(
        persistence,
        "hydrate_dashboard_dataset",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("empty payload must not hydrate")),
    )

    result = persistence.persist_hydration_payload(
        {"target_date": "2026-07-14", "active_lineups": False, "force_requested": True},
        {"results": {"pitchers": {"items": []}}},
    )

    component = result["components"]["pitchers"]
    assert component["dataset_version"] == "previous"
    assert component["skipped"] is True
    assert component["reason"] == "empty_payload_preserved_previous_dataset"


def test_nonforced_hydration_reuses_ready_historical_dataset(monkeypatch):
    monkeypatch.setattr(persistence, "_session_factory", lambda: FakeFactory())
    monkeypatch.setattr(
        persistence,
        "dashboard_dataset_status",
        lambda **kwargs: {
            "ready": True,
            "dataset_version": "current",
            "dataset_row_count": 4,
        },
    )
    monkeypatch.setattr(
        persistence,
        "hydrate_dashboard_dataset",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("ready dataset must be reused")),
    )

    result = persistence.persist_hydration_payload(
        {"target_date": "2026-07-14", "active_lineups": False, "force_requested": False},
        {"results": {"teams": {"items": [{"entity_id": "CHC"}]}}},
    )

    assert result["hydrated_component_count"] == 0
    assert result["skipped_component_count"] == 1
    assert result["components"]["teams"]["reason"] == "current_dataset_already_ready"


def test_complete_hydration_includes_dataset_persistence_metadata(monkeypatch):
    monkeypatch.setattr(
        observability,
        "persist_hydration_payload",
        lambda run, payload: {
            "dataset_source": "my_dashboard_records",
            "hydrated_component_count": 2,
            "dataset_row_count": 18,
        },
    )
    run = observability.begin_hydration("2026-07-14", ["hitters", "pitchers"], True, True)
    payload = {
        "results": {
            "hitters": {"items": [{"entity_id": "1"}]},
            "pitchers": {"items": [{"entity_id": "2"}]},
        }
    }

    completed = observability.complete_hydration(run, payload, cache_mode="forced_refresh")

    assert completed["status"] == "success"
    assert completed["dataset_hydration"]["hydrated_component_count"] == 2
    assert payload["dataset_hydration"]["dataset_row_count"] == 18
