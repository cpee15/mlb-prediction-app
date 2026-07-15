from contextlib import contextmanager

from mlb_app import my_dashboard_routes as routes


class FakeSession:
    pass


@contextmanager
def fake_session_context():
    yield FakeSession()


class FakeFactory:
    def __call__(self):
        return fake_session_context()


def _dataset_result(**kwargs):
    return {
        "execution_path": "my_dashboard_dataset_sql_query",
        "items": [],
        "records": [],
        "totalSize": 0,
        "page_info": {"page_number": kwargs["page_number"], "page_size": kwargs["page_size"]},
    }


def test_filtered_current_date_standard_route_uses_dataset_query(monkeypatch):
    captured = {}
    monkeypatch.setattr(routes, "install_dashboard_context_cache", lambda: None)
    monkeypatch.setattr(
        routes,
        "_normalize_request",
        lambda date, component, filters: {
            "target_date": "2026-07-15",
            "component": "hitters",
            "filters": {"team": "CHC"},
        },
    )
    monkeypatch.setattr(routes, "should_use_dataset_query", lambda **kwargs: True)
    monkeypatch.setattr(routes, "session_factory", lambda: FakeFactory())

    def fake_run_dataset_query(**kwargs):
        captured.update(kwargs)
        payload = kwargs["payload_builder"]()
        assert payload == {"items": [{"entity_id": "1"}]}
        return _dataset_result(**kwargs)

    monkeypatch.setattr(routes, "run_dataset_query", fake_run_dataset_query)
    monkeypatch.setattr(
        routes.dashboard_solver,
        "build_dashboard_solver_payload",
        lambda **kwargs: {"items": [{"entity_id": "1"}]} if kwargs["filters"] == {} else (_ for _ in ()).throw(AssertionError("hydration must be unfiltered")),
    )
    monkeypatch.setattr(
        routes,
        "_legacy_solver_response",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("filtered current-date request must not use legacy solver")),
    )

    result = routes._run_solver(
        date="2026-07-15",
        component="hitters",
        filters={"team": "CHC"},
        page_size=25,
        page_number=2,
        sort_by="metrics.xwOBA",
        sort_direction="asc",
        include_metadata=True,
    )

    assert result["execution_path"] == "my_dashboard_dataset_sql_query"
    assert captured["filters"] == {"team": "CHC"}
    assert captured["page_size"] == 25
    assert captured["page_number"] == 2
    assert captured["sort_by"] == "metrics.xwOBA"
    assert captured["sort_direction"] == "asc"
    assert captured["active_lineups"] is False


def test_filtered_current_date_active_lineup_route_uses_isolated_dataset(monkeypatch):
    captured = {}
    monkeypatch.setattr(routes, "install_dashboard_context_cache", lambda: None)
    monkeypatch.setattr(
        routes,
        "_normalize_request",
        lambda date, component, filters: {
            "target_date": "2026-07-15",
            "component": "hitters",
            "filters": {"min_score": 0.8},
        },
    )
    monkeypatch.setattr(routes, "should_use_dataset_query", lambda **kwargs: True)
    monkeypatch.setattr(routes, "session_factory", lambda: FakeFactory())

    def fake_run_dataset_query(**kwargs):
        captured.update(kwargs)
        assert kwargs["payload_builder"]() == {"items": [{"entity_id": "2", "lineup_verified": True}]}
        return _dataset_result(**kwargs)

    monkeypatch.setattr(routes, "run_dataset_query", fake_run_dataset_query)
    monkeypatch.setattr(
        routes,
        "build_active_lineup_solver_payload",
        lambda **kwargs: {"items": [{"entity_id": "2", "lineup_verified": True}]} if kwargs["filters"] == {} else (_ for _ in ()).throw(AssertionError("hydration must be unfiltered")),
    )
    monkeypatch.setattr(
        routes,
        "_legacy_solver_response",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("filtered active-lineup request must not use legacy solver")),
    )

    result = routes._run_active_lineup_solver(
        date="2026-07-15",
        component="hitters",
        filters={"min_score": 0.8},
    )

    assert result["execution_path"] == "my_dashboard_dataset_sql_query"
    assert captured["active_lineups"] is True
    assert captured["filters"] == {"min_score": 0.8}


def test_weight_only_current_date_request_uses_dataset_query(monkeypatch):
    monkeypatch.setattr(routes, "install_dashboard_context_cache", lambda: None)
    monkeypatch.setattr(
        routes,
        "_normalize_request",
        lambda date, component, filters: {
            "target_date": "2026-07-15",
            "component": "hitters",
            "filters": {"weights": {"EV": 1.5}},
        },
    )
    monkeypatch.setattr(routes, "should_use_dataset_query", lambda **kwargs: True)
    monkeypatch.setattr(routes, "session_factory", lambda: FakeFactory())
    monkeypatch.setattr(routes, "run_dataset_query", lambda **kwargs: _dataset_result(**kwargs))
    monkeypatch.setattr(
        routes,
        "_legacy_solver_response",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("weight-only current-date request must use dataset query")),
    )

    result = routes._run_solver(date="2026-07-15", component="hitters", filters={"weights": {"EV": 1.5}})

    assert result["execution_path"] == "my_dashboard_dataset_sql_query"


def test_historical_and_unfiltered_requests_preserve_legacy_compatibility(monkeypatch):
    calls = []
    monkeypatch.setattr(routes, "install_dashboard_context_cache", lambda: None)
    monkeypatch.setattr(
        routes,
        "_normalize_request",
        lambda date, component, filters: {
            "target_date": str(date),
            "component": "hitters",
            "filters": filters or {},
        },
    )
    monkeypatch.setattr(routes, "should_use_dataset_query", lambda **kwargs: False)
    monkeypatch.setattr(
        routes,
        "run_dataset_query",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("compatibility request must not use dataset query")),
    )

    def fake_legacy(**kwargs):
        calls.append(kwargs)
        return {"execution_path": "legacy_in_memory_solver", "items": []}

    monkeypatch.setattr(routes, "_legacy_solver_response", fake_legacy)

    historical = routes._run_solver(date="2026-07-14", component="hitters", filters={"team": "CHC"})
    unfiltered = routes._run_solver(date="2026-07-15", component="hitters", filters={})

    assert historical["execution_path"] == "legacy_in_memory_solver"
    assert unfiltered["execution_path"] == "legacy_in_memory_solver"
    assert calls[0]["target_date"] == "2026-07-14"
    assert calls[0]["filters"] == {"team": "CHC"}
    assert calls[1]["target_date"] == "2026-07-15"
    assert calls[1]["filters"] == {}
