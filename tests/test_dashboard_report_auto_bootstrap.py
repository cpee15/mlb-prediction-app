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

    def ensure(_session, *, target_date, required_player_type):
        calls["target_date"] = target_date
        calls["required_player_type"] = required_player_type
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
    assert calls["required_player_type"] == "hitter"
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


def test_confirmed_hitters_query_full_canonical_id_population(monkeypatch):
    calls = {}
    monkeypatch.setattr(routes, "session_factory", lambda: lambda: SessionContext())
    monkeypatch.setattr(
        routes,
        "ensure_canonical_projection",
        lambda *_args, **_kwargs: {"status": "already_available", "current_count": 379},
    )
    monkeypatch.setattr(
        routes,
        "build_confirmed_lineup_index",
        lambda _session, target_date: {
            "confirmed_ids": {str(value) for value in range(1, 73)},
            "confirmed_names": set(),
            "metadata": {
                "lineup_status": "partial",
                "confirmed_lineup_date": target_date,
                "confirmed_batter_count": 72,
                "source": "matchups_boxscore_lineups",
                "lineup_revision": "revision-72",
                "model_state": "lineup_building",
            },
        },
    )

    def query(_session, report_type, **kwargs):
        calls["report_type"] = report_type
        calls["population_player_ids"] = kwargs["population_player_ids"]
        calls["population_mode"] = kwargs["population_mode"]
        return {
            "report_type": report_type,
            "records": [{"mlb_player_id": value} for value in range(1, 51)],
            "items": [{"mlb_player_id": value} for value in range(1, 51)],
            "totalSize": 72,
            "population": {
                "mode": "confirmed_lineup",
                "candidate_id_count": 72,
                "matched_current_count": 72,
                "filtered_count": 72,
            },
        }

    monkeypatch.setattr(routes, "query_player_report", query)
    result = routes.my_dashboard_player_report_query(
        routes.DashboardPlayerReportRequest(
            report_type="all_active_hitters",
            as_of_date=dt.date(2026, 7, 23),
            confirmed_lineups_only=True,
            page_size=50,
        )
    )

    assert calls["report_type"] == "all_active_hitters"
    assert calls["population_mode"] == "confirmed_lineup"
    assert len(calls["population_player_ids"]) == 72
    assert result["totalSize"] == 72
    assert result["lineup_filter"]["matched_current_count"] == 72
    assert result["lineup_filter"]["removed_unconfirmed_count"] == 0
    assert all(row["lineup_verified"] for row in result["records"])
