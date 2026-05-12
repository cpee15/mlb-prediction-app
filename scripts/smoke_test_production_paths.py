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
    # Avoid external dependencies; this is sufficient for a smoke-test default.
    return dt.datetime.utcnow().date().isoformat()


def _request_json(base_url: str, path: str, timeout: int = 45) -> tuple[int, Any]:
    url = f"{base_url.rstrip('/')}{path}"
    request = urllib.request.Request(url=url, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            status = int(response.status)
            body = response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
        raise SmokeFailure(f"HTTP {exc.code} for {path}: {detail[:500]}") from exc
    except urllib.error.URLError as exc:
        raise SmokeFailure(f"Network error for {path}: {exc}") from exc

    if status >= 400:
        raise SmokeFailure(f"HTTP {status} for {path}")

    try:
        return status, json.loads(body) if body else None
    except json.JSONDecodeError as exc:
        raise SmokeFailure(f"Invalid JSON for {path}: {body[:500]}") from exc


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


def _check_daily_odds(data: Any, path: str) -> None:
    payload = _expect_dict(data, path)
    missing = [key for key in ["date", "errors"] if key not in payload]
    if missing:
        raise SmokeFailure(f"Daily Odds payload missing {missing} for {path}")
    if not any(key in payload for key in ["models", "games", "model_games"]):
        raise SmokeFailure("Daily Odds payload missing models/games/model_games container")


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


def _run_check(
    base_url: str,
    label: str,
    path: str,
    validator: Callable[[Any, str], None],
) -> bool:
    try:
        status, data = _request_json(base_url, path)
        validator(data, path)
        print(f"PASS {label}: HTTP {status} {path}")
        return True
    except SmokeFailure as exc:
        print(f"FAIL {label}: {exc}")
        return False


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke-test production-critical MLB backend paths.")
    parser.add_argument("--base-url", default=os.getenv("BACKEND_BASE_URL", DEFAULT_BACKEND_BASE_URL))
    parser.add_argument("--date", default=os.getenv("SMOKE_TEST_DATE", _today_eastern_iso()))
    args = parser.parse_args()

    date = args.date[:10]
    query_date = urllib.parse.urlencode({"date": date})
    dashboard_query = urllib.parse.urlencode({"date": date, "component": "hitters"})

    checks: list[tuple[str, str, Callable[[Any, str], None]]] = [
        ("health", "/health", _check_health),
        ("matchups", f"/matchups?{query_date}", _check_matchups),
        ("daily_odds_models", f"/daily-odds/models?{query_date}", _check_daily_odds),
        ("model_projections", f"/models/projections?{query_date}", _check_model_projections),
        ("ai_data_assistant_health", "/ai-data-assistant/health", _check_health),
        ("my_dashboard_health", "/my-dashboard/health", _check_health),
        ("my_dashboard_solver", f"/my-dashboard/solver?{dashboard_query}", _check_dashboard_solver),
    ]

    print(f"Backend base URL: {args.base_url.rstrip('/')}")
    print(f"Smoke-test date: {date}")

    passed = 0
    for label, path, validator in checks:
        if _run_check(args.base_url, label, path, validator):
            passed += 1

    failed = len(checks) - passed
    print(f"Summary: {passed} passed, {failed} failed")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
