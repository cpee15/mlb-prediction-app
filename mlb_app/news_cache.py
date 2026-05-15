from __future__ import annotations

import time
from typing import Any, Dict, Optional, Tuple

_CACHE: Dict[str, Tuple[float, Any]] = {}

DEFAULT_TTLS = {
    "ticker": 120,
    "hourly": 300,
    "beat": 300,
    "betting": 300,
    "daily": 900,
    "weekly": 3600,
    "monthly": 3600,
    "sources": 86400,
    "all": 300,
    "team_intel": 300,
}


def get_cache(key: str) -> Optional[Any]:
    entry = _CACHE.get(key)
    if not entry:
        return None
    expires_at, data = entry
    if time.time() >= expires_at:
        _CACHE.pop(key, None)
        return None
    return data


def set_cache(key: str, data: Any, ttl: int) -> None:
    _CACHE[key] = (time.time() + max(1, int(ttl)), data)


def ttl_for(bucket: str, fallback: int = 300) -> int:
    return DEFAULT_TTLS.get(bucket, fallback)


def clear_cache() -> None:
    _CACHE.clear()
