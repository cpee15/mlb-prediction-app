import datetime as dt

from mlb_app import my_dashboard_routes as routes


class SessionContext:
    def __enter__(self):
        return object()

    def __exit__(self, *_args):
        return False


def test_canonical_report_query_bootstraps_for_requested_mlb_date(monkeypatch):
    calls = {}
    monkeypatch.setattr(routes, "session_factory", lambda: lambda: SessionContext())

    def ensure(_session, *, target_date):
        calls["target_date"] = target_date
        return {"status": "populated", "current_count": 300}

    def query(_session, report_type, **kwargs):
        calls["report_type"] = report_type
        calls["query"] = kwargs
        return {"report_type": report_type, "records": [], "totalSize": 300}

    monkeypatch.setattr(routes, "ensure_canonical_projection", ensure)
    monkeypatch.setattr(routes, "query_player_report", query)

    payload = routes.DashboardPlayerReportRequest(
        report_type="all_active_hitters",
        as_of_date=dt.date(2026, 7, 20),
        page_size=50,
        page_number=1,
    )
    result = routes.my_dashboard_player_report_query(payload)

    assert calls["target_date"] == dt.date(2026, 7, 20)
    assert calls["report_type"] == "all_active_hitters"
    assert result["totalSize"] == 300
    assert result["population_bootstrap"] == {
        "status": "populated",
        "current_count": 300,
    }


def test_noncanonical_related_report_does_not_auto_bootstrap(monkeypatch):
    monkeypatch.setattr(routes, "session_factory", lambda: lambda: SessionContext())
    monkeypatch.setattr(
        routes,
        "ensure_canonical_projection",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("unexpected bootstrap")),
    )
    monkeypatch.setattr(
        routes,
        "query_related_report",
        lambda _session, report_type, **_kwargs: {
            "report_type": report_type,
            "records": [],
            "totalSize": 0,
        },
    )

    result = routes.my_dashboard_player_report_query(
        routes.DashboardPlayerReportRequest(report_type="players_lineup_history")
    )
    assert result["report_type"] == "players_lineup_history"
    assert "population_bootstrap" not in result
