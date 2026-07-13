from __future__ import annotations

from mlb_app.my_dashboard_observability import (
    begin_hydration,
    complete_hydration,
    cron_configuration,
    fail_hydration,
    latest_hydration_status,
    summarize_hydration_payload,
)


def sample_payload():
    return {
        "results": {
            "hitters": {
                "candidate_universe_count": 420,
                "deduped_universe_count": 178,
                "result_count_before_filters": 178,
                "result_count_after_filters": 178,
                "result_count_after_lineup_filter": 141,
                "lineup_filter": {
                    "lineup_status": "partial",
                    "confirmed_batter_count": 141,
                    "games_checked": 15,
                    "games_with_lineups": 9,
                    "teams_with_lineups": 18,
                    "warnings": ["Six games do not have confirmed lineups yet."],
                },
            },
            "teams": {
                "candidate_universe_count": 30,
                "deduped_universe_count": 30,
                "result_count_after_filters": 30,
                "lineup_filter": {"lineup_status": "not_applicable", "warnings": []},
            },
        }
    }


def test_hydration_summary_exposes_component_and_lineup_counts():
    summary = summarize_hydration_payload(sample_payload())
    assert summary["component_count"] == 2
    assert summary["components"]["hitters"]["candidate_universe_count"] == 420
    assert summary["components"]["hitters"]["result_count_after_lineup_filter"] == 141
    assert summary["games_checked"] == 15
    assert summary["games_with_lineups"] == 9
    assert summary["confirmed_batter_count"] == 141
    assert summary["warnings"] == ["Six games do not have confirmed lineups yet."]


def test_completed_run_is_published_as_latest_status():
    run = begin_hydration("2026-07-12", ["hitters", "teams"], True, True)
    status = complete_hydration(run, sample_payload(), cache_mode="forced_refresh")
    latest = latest_hydration_status()
    assert status["status"] == "success"
    assert status["target_date"] == "2026-07-12"
    assert status["cache_mode"] == "forced_refresh"
    assert status["duration_ms"] >= 0
    assert latest["run_id"] == status["run_id"]


def test_failed_run_is_published_without_raising_again():
    run = begin_hydration("2026-07-12", ["hitters"], True, False)
    status = fail_hydration(run, RuntimeError("database unavailable"))
    assert status["status"] == "failed"
    assert status["error"] == "database unavailable"
    assert latest_hydration_status()["run_id"] == status["run_id"]


def test_cron_configuration_is_explicit(monkeypatch):
    monkeypatch.setenv("MY_DASHBOARD_HYDRATION_CRON_SCHEDULE", "0 10 * * *")
    monkeypatch.setenv("MY_DASHBOARD_HYDRATION_TIMEZONE", "America/New_York")
    monkeypatch.setenv("MY_DASHBOARD_HYDRATION_PRODUCTION_VERIFIED", "true")
    config = cron_configuration()
    assert config["recommended_method"] == "POST"
    assert config["recommended_force"] is True
    assert config["configured_schedule"] == "0 10 * * *"
    assert config["production_verified"] is True
