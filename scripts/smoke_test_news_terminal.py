#!/usr/bin/env python3
"""Smoke-test News terminal endpoints.

Empty or provider_not_configured responses are non-fatal. This fails on HTTP 500,
invalid JSON, or missing stable response keys.
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
from typing import Any

DEFAULT_BACKEND_BASE_URL = "https://mlb-prediction-app-production-732c.up.railway.app"
BUCKET_KEYS = ["hourly", "daily", "weekly", "monthly", "beat", "betting"]


class SmokeFailure(Exception):
    pass


def _today_iso() -> str:
    return dt.datetime.utcnow().date().isoformat()


def _request_json(base_url: str, path: str, timeout: int = 45) -> tuple[int, Any]:
    url = f"{base_url.rstrip('/')}{path}"
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            status = int(response.status)
            body = response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
        raise SmokeFailure(f"HTTP {exc.code} for GET {path}: {detail[:500]}") from exc
    except urllib.error.URLError as exc:
        raise SmokeFailure(f"Network error for GET {path}: {exc}") from exc
    if status >= 400:
        raise SmokeFailure(f"HTTP {status} for GET {path}")
    try:
        return status, json.loads(body) if body else None
    except json.JSONDecodeError as exc:
        raise SmokeFailure(f"Invalid JSON for GET {path}: {body[:500]}") from exc


def _expect_dict(data: Any, path: str) -> dict[str, Any]:
    if not isinstance(data, dict):
        raise SmokeFailure(f"Expected object for {path}, got {type(data).__name__}")
    return data


def _check_health(data: Any, path: str) -> None:
    payload = _expect_dict(data, path)
    if payload.get("component") != "news_terminal" or payload.get("status") != "ok":
        raise SmokeFailure(f"Unexpected News health payload for {path}: {payload}")
    env_vars = payload.get("env_vars_supported") or []
    for key in ["NEWS_PROVIDER", "NEWS_ENABLE_RSS_PROVIDER", "NEWS_CACHE_TTL_SECONDS", "NEWS_MAX_ITEMS_PER_BUCKET", "NEWS_DEFAULT_LOOKBACK_HOURS", "NEWS_API_KEY", "NEWS_ENABLE_X_PROVIDER", "X_BEARER_TOKEN"]:
        if key not in env_vars:
            raise SmokeFailure(f"News health missing supported env var {key}")
    if payload.get("rss_supported") is not True:
        raise SmokeFailure("News health must indicate RSS support")
    if payload.get("active_provider") in {"rss", "rss_network", "rss_feed_network"} and payload.get("requires_api_key") is not False:
        raise SmokeFailure("RSS provider must not require NEWS_API_KEY")


def _check_all(data: Any, path: str) -> None:
    payload = _expect_dict(data, path)
    for key in ["date", "generated_at", "provider", "status", "buckets", "ticker", "team_intel", "sources", "errors", "cache_hit"]:
        if key not in payload:
            raise SmokeFailure(f"News all payload missing {key}")
    buckets = payload.get("buckets")
    if not isinstance(buckets, dict):
        raise SmokeFailure("News buckets must be an object")
    missing = [key for key in BUCKET_KEYS if key not in buckets]
    if missing:
        raise SmokeFailure(f"News buckets missing {missing}")
    if not isinstance(payload.get("ticker"), list):
        raise SmokeFailure("News ticker must be an array")
    if not isinstance(payload.get("errors"), list):
        raise SmokeFailure("News errors must be an array")
    if payload.get("status") == "provider_not_configured" and payload.get("provider") in {"rss", "rss_network", "rss_feed_network"}:
        raise SmokeFailure("RSS provider returned provider_not_configured")


def _check_ticker(data: Any, path: str) -> None:
    payload = _expect_dict(data, path)
    if "ticker" not in payload or not isinstance(payload.get("ticker"), list):
        raise SmokeFailure("Ticker payload missing ticker array")
    if not isinstance(payload.get("errors", []), list):
        raise SmokeFailure("Ticker payload errors must be an array")


def _check_items(data: Any, path: str) -> None:
    payload = _expect_dict(data, path)
    if "items" not in payload or not isinstance(payload.get("items"), list):
        raise SmokeFailure(f"Expected items array for {path}")
    if not isinstance(payload.get("errors", []), list):
        raise SmokeFailure(f"Expected errors array for {path}")


def _check_team_intel(data: Any, path: str) -> None:
    payload = _expect_dict(data, path)
    if "team_intel" not in payload or not isinstance(payload.get("team_intel"), dict):
        raise SmokeFailure("Team intel payload missing team_intel object")
    if not isinstance(payload.get("errors", []), list):
        raise SmokeFailure("Team intel errors must be an array")


def _check_sources(data: Any, path: str) -> None:
    payload = _expect_dict(data, path)
    if "sources" not in payload or not isinstance(payload.get("sources"), list):
        raise SmokeFailure("Sources payload missing sources array")
    if "rss_sources" not in payload or not isinstance(payload.get("rss_sources"), list):
        raise SmokeFailure("Sources payload missing rss_sources array")
    if payload["sources"]:
        first = payload["sources"][0]
        if "rss_sources" not in first:
            raise SmokeFailure("Team source missing rss_sources")


def _run(base_url: str, label: str, path: str, validator) -> bool:
    try:
        status, data = _request_json(base_url, path)
        validator(data, path)
        print(f"PASS {label}: HTTP {status} GET {path}")
        return True
    except SmokeFailure as exc:
        print(f"FAIL {label}: {exc}")
        return False


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke-test News terminal endpoints.")
    parser.add_argument("--base-url", default=os.getenv("BACKEND_BASE_URL", DEFAULT_BACKEND_BASE_URL))
    parser.add_argument("--date", default=os.getenv("SMOKE_TEST_DATE", _today_iso()))
    args = parser.parse_args()
    date = args.date[:10]
    q = urllib.parse.urlencode({"date": date})
    q_team = urllib.parse.urlencode({"date": date, "team": "CHC"})
    checks = [
        ("news_health", "/news/health", _check_health),
        ("news_all", f"/news/all?{q}", _check_all),
        ("news_ticker", f"/news/ticker?{q}", _check_ticker),
        ("news_beat", f"/news/beat?{q_team}", _check_items),
        ("news_betting", f"/news/betting?{q}", _check_items),
        ("news_team_intel", f"/news/team-intel?{q_team}", _check_team_intel),
        ("news_sources", "/news/sources?team=CHC", _check_sources),
    ]
    print(f"Backend base URL: {args.base_url.rstrip('/')}")
    print(f"Smoke-test date: {date}")
    passed = sum(1 for label, path, validator in checks if _run(args.base_url, label, path, validator))
    failed = len(checks) - passed
    print(f"Summary: {passed} passed, {failed} failed")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
