#!/usr/bin/env python3
"""Smoke-test production-critical backend paths.

Usage:
    python scripts/smoke_test_production_paths.py \
        --base-url https://mlb-prediction-app-production-732c.up.railway.app \
        --date 2026-05-12

The script intentionally treats empty odds as non-fatal. It fails on HTTP errors,
invalid JSON, or structurally broken responses for the production-critical paths.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Callable


DEFAULT_BACKEND_BASE_URL = "https://mlb-prediction-app-production-732c.up.railway.app"


class SmokeFailure(Exception):
    pass


def _today_eastern_iso() -> str:
    return dt.datetime.utcnow().date().isoformat()


def _request_json(base_url: str, path: str, timeout: int = 45, method: str = "GET") -> tuple[int, Any]:
    url = f"{base_url.rstrip('/')}{path}"
    request = urllib.request.Request(url=url, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            status = int(response.status)
            body = response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
        raise SmokeFailure(f"HTTP {exc.code} for {method} {path}: {detail[:500]}") from exc
    except urllib.error.URLError as exc:
        raise SmokeFailure(f"Network error for {method} {path}: {exc}") from exc

    if status >= 400:
        raise SmokeFailure(f"HTTP {status} for {method} {path}")

    try:
        return status, json.loads(body) if body else None
    except json.JSONDecodeError as exc:
        raise SmokeFailure(f"Invalid JSON for {method} {path}: {body[:500]}") from exc


def _expect_dict(data: Any, path: str) -> dict[str, Any]:
    if not isinstance(data, dict):
        raise SmokeFailure(f"Expected object for {path}, got {type(data).__name__}")
    return data


def _check_health(data: Any, path: str) -> None:
    payload = _expect_dict(data, path)
    if not payload:
        raise SmokeFailure(f"Empty health payload for {path}")


def _check_matchups(data: Any, path: str) -> None:
    if not isinstance(data, list):
        raise SmokeFailure(f"Expected matchup list for {path}, got {type(data).__name__}")


def _check_daily_odds_fast(data: Any, path: str) -> None:
    payload = _expect_dict(data, path)
    missing = [key for key in ["date", "errors"] if key not in payload]
    if missing:
        raise SmokeFailure(f"Daily Odds fast payload missing {missing} for {path}")
    if not any(key in payload for key in ["models", "games", "model_games"]):
        raise SmokeFailure("Daily Odds fast payload missing models/games/model_games container")
    if payload.get("unified_loaded") is not False:
        raise SmokeFailure("Daily Odds fast payload should not load unified model/dashboard sections by default")


def _check_daily_odds_unified(data: Any, path: str) -> None:
    payload = _expect_dict(data, path)
    enhanced_keys = [
        "daily_recap",
        "model_projection_summary",
        "dashboard_solver_summary",
        "data_quality",
        "sources_used",
        "missing_inputs",
        "fallbacks_used",
    ]
    missing_enhanced = [key for key in enhanced_keys if key not in payload]
    if missing_enhanced:
        raise SmokeFailure(f"Daily Odds unified payload missing {missing_enhanced}")
    if payload.get("unified_loaded") is not True:
        raise SmokeFailure("Daily Odds unified payload should set unified_loaded=true")


def _check_model_projections(data: Any, path: str) -> None:
    payload = _expect_dict(data, path)
    if not any(key in payload for key in ["games", "models", "count"]):
        raise SmokeFailure("Model Projections payload missing games/models/count")


def _check_dashboard_solver(data: Any, path: str) -> None:
    payload = _expect_dict(data, path)
    missing = [key for key in ["items", "component", "date"] if key not in payload]
    if missing:
        raise SmokeFailure(f"My Dashboard solver payload missing {missing} for {path}")
    if not isinstance(payload.get("items"), list):
        raise SmokeFailure("My Dashboard solver items must be a list")


def _check_tracker_health(data: Any, path: str) -> None:
    payload = _expect_dict(data, path)
    if payload.get("component") != "model_tracker" or payload.get("status") != "ok":
        raise SmokeFailure(f"Unexpected Model Tracker health payload for {path}: {payload}")


def _check_tracker_snapshot(data: Any, path: str) -> None:
    payload = _expect_dict(data, path)
    for key in ["date", "rows_collected", "upsert", "errors"]:
        if key not in payload:
            raise SmokeFailure(f"Model Tracker snapshot payload missing {key}")
    if not isinstance(payload.get("errors"), list):
        raise SmokeFailure("Model Tracker snapshot errors must be a list")
    if payload.get("errors"):
        raise SmokeFailure(f"Model Tracker snapshot returned source errors: {payload.get('errors')[:3]}")
    if int(payload.get("rows_collected") or 0) <= 0:
        raise SmokeFailure(f"Model Tracker snapshot collected zero rows: {payload}")


def _check_tracker_list(data: Any, path: str) -> None:
    payload = _expect_dict(data, path)
    for key in ["date", "rows", "games", "summary", "errors"]:
        if key not in payload:
            raise SmokeFailure(f"Model Tracker list payload missing {key}")
    if not isinstance(payload.get("rows"), list):
        raise SmokeFailure("Model Tracker rows must be a list")
    if not isinstance(payload.get("games"), list):
        raise SmokeFailure("Model Tracker games must be a list")


def _check_tracker_list_nonempty(data: Any, path: str) -> None:
    _check_tracker_list(data, path)
    payload = _expect_dict(data, path)
    rows = payload.get("rows") or []
    summary = payload.get("summary") or {}
    if len(rows) <= 0:
        raise SmokeFailure(f"Model Tracker list returned zero rows after snapshot for {path}: {summary}")


def _run_check(base_url: str, label: str, path: str, validator: Callable[[Any, str], None], method: str = "GET") -> bool:
    try:
        status, data = _request_json(base_url, path, method=method)
        validator(data, path)
        print(f"PASS {label}: HTTP {status} {method} {path}")
        return True
    except SmokeFailure as exc:
        print(f"FAIL {label}: {exc}")
        return False


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke-test production-critical MLB backend paths.")
    parser.add_argument("--base-url", default=os.getenv("BACKEND_BASE_URL", DEFAULT_BACKEND_BASE_URL))
    parser.add_argument("--date", default=os.getenv("SMOKE_TEST_DATE", _today_eastern_iso()))
    parser.add_argument("--include-mutating-tracker-checks", action="store_true", help="Also POST a Model Tracker snapshot/results refresh.")
    args = parser.parse_args()

    date = args.date[:10]
    query_date = urllib.parse.urlencode({"date": date})
    unified_query = urllib.parse.urlencode({"date": date, "include_unified": "true"})
    dashboard_hitter_query = urllib.parse.urlencode({"date": date, "component": "hitters"})
    dashboard_pitcher_query = urllib.parse.urlencode({"date": date, "component": "pitchers"})

    checks: list[tuple[str, str, Callable[[Any, str], None], str]] = [
        ("health", "/health", _check_health, "GET"),
        ("matchups", f"/matchups?{query_date}", _check_matchups, "GET"),
        ("daily_odds_models_fast", f"/daily-odds/models?{query_date}", _check_daily_odds_fast, "GET"),
        ("daily_odds_models_unified", f"/daily-odds/models?{unified_query}", _check_daily_odds_unified, "GET"),
        ("model_projections", f"/models/projections?{query_date}", _check_model_projections, "GET"),
        ("ai_data_assistant_health", "/ai-data-assistant/health", _check_health, "GET"),
        ("my_dashboard_health", "/my-dashboard/health", _check_health, "GET"),
        ("my_dashboard_solver_hitters", f"/my-dashboard/solver?{dashboard_hitter_query}", _check_dashboard_solver, "GET"),
        ("my_dashboard_solver_pitchers", f"/my-dashboard/solver?{dashboard_pitcher_query}", _check_dashboard_solver, "GET"),
        ("model_tracker_health", "/model-tracker/health", _check_tracker_health, "GET"),
        ("model_tracker_list", f"/model-tracker?{query_date}", _check_tracker_list, "GET"),
    ]
    if args.include_mutating_tracker_checks:
        checks.append(("model_tracker_snapshot", f"/model-tracker/snapshot?{query_date}", _check_tracker_snapshot, "POST"))
        checks.append(("model_tracker_list_after_snapshot", f"/model-tracker?{query_date}", _check_tracker_list_nonempty, "GET"))

    print(f"Backend base URL: {args.base_url.rstrip('/')}")
    print(f"Smoke-test date: {date}")

    passed = 0
    for label, path, validator, method in checks:
        if _run_check(args.base_url, label, path, validator, method=method):
            passed += 1

    failed = len(checks) - passed
    print(f"Summary: {passed} passed, {failed} failed")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
