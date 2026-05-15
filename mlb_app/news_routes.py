from __future__ import annotations

import datetime as dt
import os
from typing import Optional

from fastapi import APIRouter, Query

from .news_cache import get_cache, set_cache, ttl_for
from .news_provider import BUCKETS, build_news_payload, build_team_intel, empty_response, get_provider
from .news_sources import get_global_rss_sources, get_news_sources

router = APIRouter()


def _date(value: Optional[str]) -> str:
    return (value or dt.date.today().isoformat())[:10]


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _cached(key: str, ttl_key: str, builder):
    cached = get_cache(key)
    if cached is not None:
        if isinstance(cached, dict):
            out = dict(cached)
            out["cache_hit"] = True
            return out
        return cached
    data = builder()
    ttl = int(os.getenv("NEWS_CACHE_TTL_SECONDS", str(ttl_for(ttl_key))))
    set_cache(key, data, ttl)
    return data


@router.get("/news/health")
def news_health():
    provider = get_provider()
    rss_enabled = os.getenv("NEWS_PROVIDER", "").strip().lower() in {"rss", "rss_network", "rss_feed_network"} or _env_bool("NEWS_ENABLE_RSS_PROVIDER")
    return {
        "component": "news_terminal",
        "status": "ok",
        "provider_configured": provider.configured(),
        "active_provider": provider.name,
        "rss_enabled": rss_enabled,
        "rss_supported": True,
        "requires_api_key": bool(getattr(provider, "requires_api_key", False)),
        "env_vars_supported": [
            "NEWS_PROVIDER",
            "NEWS_ENABLE_RSS_PROVIDER",
            "NEWS_CACHE_TTL_SECONDS",
            "NEWS_MAX_ITEMS_PER_BUCKET",
            "NEWS_DEFAULT_LOOKBACK_HOURS",
            "NEWS_API_KEY",
            "NEWS_ENABLE_X_PROVIDER",
            "X_BEARER_TOKEN",
            "NEWS_DEFAULT_QUERY",
            "RAPIDAPI_KEY",
            "RAPIDAPI_NEWS_HOST",
            "X_API_KEY",
            "X_API_SECRET",
            "X_ACCESS_TOKEN",
            "X_ACCESS_TOKEN_SECRET",
            "NEWS_ENABLE_BETTING_WIRE",
            "NEWS_ENABLE_LOCAL_SOURCES",
        ],
    }


@router.get("/news")
def news_bucket(bucket: str = Query("daily"), date: Optional[str] = None, team: Optional[str] = None):
    target_date = _date(date)
    if bucket not in BUCKETS:
        return empty_response(target_date, "news_terminal", "provider_error", [f"Unsupported bucket: {bucket}"])
    payload = _cached(f"news:{bucket}:{target_date}:{team or 'all'}", bucket, lambda: build_news_payload(target_date, bucket=bucket, team=team))
    slim = dict(payload)
    slim["buckets"] = {name: (payload.get("buckets") or {}).get(name, []) if name == bucket else [] for name in BUCKETS}
    return slim


@router.get("/news/all")
def news_all(date: Optional[str] = None):
    target_date = _date(date)
    return _cached(f"news:all:{target_date}", "all", lambda: build_news_payload(target_date))


@router.get("/news/ticker")
def news_ticker(date: Optional[str] = None):
    target_date = _date(date)
    payload = _cached(f"news:ticker:{target_date}", "ticker", lambda: build_news_payload(target_date))
    return {
        "date": target_date,
        "generated_at": payload.get("generated_at"),
        "provider": payload.get("provider"),
        "status": payload.get("status"),
        "ticker": payload.get("ticker") or [],
        "errors": payload.get("errors") or [],
        "cache_hit": payload.get("cache_hit", False),
    }


@router.get("/news/beat")
def news_beat(team: Optional[str] = None, date: Optional[str] = None):
    target_date = _date(date)
    payload = _cached(f"news:beat:{target_date}:{team or 'all'}", "beat", lambda: build_news_payload(target_date, bucket="beat", team=team))
    return {
        "date": target_date,
        "generated_at": payload.get("generated_at"),
        "provider": payload.get("provider"),
        "status": payload.get("status"),
        "items": (payload.get("buckets") or {}).get("beat", []),
        "errors": payload.get("errors") or [],
        "cache_hit": payload.get("cache_hit", False),
    }


@router.get("/news/betting")
def news_betting(date: Optional[str] = None):
    target_date = _date(date)
    payload = _cached(f"news:betting:{target_date}", "betting", lambda: build_news_payload(target_date, bucket="betting"))
    return {
        "date": target_date,
        "generated_at": payload.get("generated_at"),
        "provider": payload.get("provider"),
        "status": payload.get("status"),
        "items": (payload.get("buckets") or {}).get("betting", []),
        "errors": payload.get("errors") or [],
        "cache_hit": payload.get("cache_hit", False),
    }


@router.get("/news/team-intel")
def news_team_intel(team: Optional[str] = None, date: Optional[str] = None):
    target_date = _date(date)
    payload = _cached(f"news:team-intel:{target_date}:{team or 'all'}", "team_intel", lambda: build_news_payload(target_date, team=team))
    return {
        "date": target_date,
        "generated_at": payload.get("generated_at"),
        "provider": payload.get("provider"),
        "status": payload.get("status"),
        "team_intel": build_team_intel(target_date, team=team, payload=payload),
        "errors": payload.get("errors") or [],
        "cache_hit": payload.get("cache_hit", False),
    }


@router.get("/news/sources")
def news_sources(team: Optional[str] = None):
    return {
        "status": "ok",
        "team": team,
        "sources": get_news_sources(team),
        "rss_sources": get_global_rss_sources(),
    }
