import datetime as dt

from mlb_app import my_dashboard_dataset_runtime as runtime


def test_substantive_filter_detection():
    assert runtime.has_substantive_filters({"team": "CHC"}) is True
    assert runtime.has_substantive_filters({"metrics": {"EV": {"min": 90}}}) is True
    assert runtime.has_substantive_filters({"metrics": {"EV": {"min": "", "max": ""}}}) is False
    assert runtime.has_substantive_filters({"weights": {"EV": 1.5}}) is True
    assert runtime.has_substantive_filters({"weights": {"EV": 1.0}}) is False
    assert runtime.has_substantive_filters({}) is False


def test_weight_overrides_use_dataset_path(monkeypatch):
    monkeypatch.setattr(runtime, "mlb_business_date", lambda now=None: dt.date(2026, 7, 15))
    assert runtime.should_use_dataset_query(date="2026-07-15", filters={"team": "CHC"}) is True
    assert runtime.should_use_dataset_query(date="2026-07-15", filters={"team": "CHC", "weights": {"EV": 1.5}}) is True
    assert runtime.should_use_dataset_query(date="2026-07-15", filters={"weights": {"EV": 1.5}}) is True
    assert runtime.should_use_dataset_query(date="2026-07-14", filters={"team": "CHC"}) is False
    assert runtime.should_use_dataset_query(date="2026-07-15", filters={}) is False


def test_mlb_business_date_uses_eastern_boundary():
    before_midnight_eastern = dt.datetime(2026, 7, 15, 3, 30, tzinfo=dt.timezone.utc)
    after_midnight_eastern = dt.datetime(2026, 7, 15, 4, 30, tzinfo=dt.timezone.utc)
    assert runtime.mlb_business_date(before_midnight_eastern) == dt.date(2026, 7, 14)
    assert runtime.mlb_business_date(after_midnight_eastern) == dt.date(2026, 7, 15)


def test_runtime_returns_not_ready_without_hydrating(monkeypatch):
    monkeypatch.setattr(runtime, "dashboard_dataset_status", lambda **kwargs: {"ready": False, "stale": False})
    monkeypatch.setattr(
        runtime,
        "query_dashboard_dataset",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("unexpected query")),
    )

    result = runtime.run_dataset_query(
        session=object(),
        date="2026-07-15",
        component="hitters",
        filters={"team": "CHC", "weights": {"EV": 1.5}},
        page_size=50,
        page_number=1,
        sort_by="score",
        sort_direction="desc",
        include_metadata=True,
        payload_builder=lambda: {"items": [{"entity_id": "1"}, {"entity_id": "2"}]},
        active_lineups=False,
    )

    assert result["execution_path"] == "my_dashboard_dataset_sql_query"
    assert result["dataset_hydrated_for_request"] is False
    assert result["data_status"] == "not_ready"


def test_runtime_serves_stale_dataset_without_refresh(monkeypatch):
    monkeypatch.setattr(runtime, "dashboard_dataset_status", lambda **kwargs: {"ready": True, "stale": True, "dataset_version": "old"})
    monkeypatch.setattr(runtime, "query_dashboard_dataset", lambda **kwargs: {"items": [], "records": [], "totalSize": 0})

    result = runtime.run_dataset_query(
        session=object(),
        date="2026-07-15",
        component="hitters",
        filters={"team": "CHC"},
        page_size=50,
        page_number=1,
        sort_by="score",
        sort_direction="desc",
        include_metadata=True,
        payload_builder=lambda: {"items": []},
        active_lineups=False,
    )

    assert result["served_stale_dataset"] is True
    assert result["data_status"] == "stale"
    assert "scheduled refresh is required" in result["filter_warnings"][0]
