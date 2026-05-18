from __future__ import annotations

import copy
import hashlib
import json
import os
import time
from typing import Any, Callable, Dict, Optional, Tuple

CacheRecord = Tuple[float, Any]
_CACHE: Dict[str, CacheRecord] = {}

DEFAULT_TTLS = {
    "MODEL_PROJECTION_CACHE_TTL_SECONDS": 600,
    "DAILY_CONTEXT_CACHE_TTL_SECONDS": 300,
    "NEWS_CACHE_TTL_SECONDS": 300,
    "TWITTER_X_CACHE_TTL_SECONDS": 120,
    "DASHBOARD_SOLVER_CACHE_TTL_SECONDS": 300,
    "DASHBOARD_CONTEXT_CACHE_TTL_SECONDS": 300,
    "AI_DATA_ASSISTANT_RESPONSE_CACHE_TTL_SECONDS": 180,
    "MATCHUPS_CACHE_TTL_SECONDS": 300,
    "MATCHUP_DETAIL_CACHE_TTL_SECONDS": 300,
    "PITCHER_PROFILE_CACHE_TTL_SECONDS": 21600,
    "PITCHER_ARSENAL_CACHE_TTL_SECONDS": 21600,
}


def _now() -> float:
    return time.monotonic()


def env_ttl(name: str) -> int:
    default = DEFAULT_TTLS.get(name, 300)
    try:
        return max(0, int(os.getenv(name, str(default))))
    except Exception:
        return default


def stable_hash(value: Any) -> str:
    try:
        raw = json.dumps(value or {}, sort_keys=True, default=str, separators=(",", ":"))
    except Exception:
        raw = str(value or {})
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def make_cache_key(*parts: Any) -> str:
    return ":".join(str(part) for part in parts if part is not None)


def get_cache(key: str, ttl_seconds: int) -> Optional[Any]:
    record = _CACHE.get(key)
    if not record:
        return None
    created_at, value = record
    if ttl_seconds <= 0 or _now() - created_at > ttl_seconds:
        _CACHE.pop(key, None)
        return None
    return copy.deepcopy(value)


def set_cache(key: str, value: Any) -> Any:
    _CACHE[key] = (_now(), copy.deepcopy(value))
    return copy.deepcopy(value)


def get_or_set(key: str, ttl_seconds: int, builder: Callable[[], Any]) -> Any:
    cached = get_cache(key, ttl_seconds)
    if cached is not None:
        if isinstance(cached, dict):
            cached["cache_hit"] = True
            cached.setdefault("cache_key", key)
            cached.setdefault("ttl_seconds", ttl_seconds)
        return cached
    started_at = _now()
    value = builder()
    built_ms = int(round((_now() - started_at) * 1000))
    stored = set_cache(key, value)
    if isinstance(stored, dict):
        stored.setdefault("cache_hit", False)
        stored.setdefault("cache_key", key)
        stored.setdefault("ttl_seconds", ttl_seconds)
        stored.setdefault("built_ms", built_ms)
    return stored


def clear_shared_payload_cache(prefix: Optional[str] = None) -> Dict[str, Any]:
    if not prefix:
        count = len(_CACHE)
        _CACHE.clear()
        return {"cleared": True, "entries": count}
    keys = [key for key in _CACHE if key.startswith(prefix)]
    for key in keys:
        _CACHE.pop(key, None)
    return {"cleared": True, "prefix": prefix, "entries": len(keys)}