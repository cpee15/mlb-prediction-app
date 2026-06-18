from __future__ import annotations

import copy
import time
from dataclasses import dataclass
from threading import RLock
from typing import Any, Dict, Optional


@dataclass
class CacheEntry:
    value: Any
    created_at: float
    expires_at: float
    stale_expires_at: Optional[float] = None


_CACHE: Dict[str, CacheEntry] = {}
_LOCK = RLock()


def _now() -> float:
    return time.monotonic()


def _clone(value: Any) -> Any:
    try:
        return copy.deepcopy(value)
    except Exception:
        return value


def cache_key(*parts: Any) -> str:
    """Build a stable colon-delimited cache key from non-empty key parts."""
    return ":".join(str(part) for part in parts if part is not None and str(part) != "")


def ttl_cache_set(
    key: str,
    value: Any,
    ttl_seconds: int,
    stale_ttl_seconds: Optional[int] = None,
) -> Any:
    """Store a value with a fresh TTL and optional stale-if-error window."""
    now = _now()
    fresh_ttl = max(0, int(ttl_seconds or 0))
    stale_ttl = max(0, int(stale_ttl_seconds or 0)) if stale_ttl_seconds is not None else fresh_ttl
    entry = CacheEntry(
        value=_clone(value),
        created_at=now,
        expires_at=now + fresh_ttl,
        stale_expires_at=now + fresh_ttl + stale_ttl if stale_ttl > 0 else None,
    )
    with _LOCK:
        _CACHE[key] = entry
    return _clone(value)


def ttl_cache_get(key: str) -> Optional[Any]:
    """Return a fresh cached value, or None on miss/expiry."""
    now = _now()
    with _LOCK:
        entry = _CACHE.get(key)
        if entry is None:
            return None
        if now > entry.expires_at:
            return None
        return _clone(entry.value)


def ttl_cache_get_stale(key: str) -> Optional[Any]:
    """Return a stale cached value if it is still inside the stale window."""
    now = _now()
    with _LOCK:
        entry = _CACHE.get(key)
        if entry is None:
            return None
        if entry.stale_expires_at is None or now > entry.stale_expires_at:
            _CACHE.pop(key, None)
            return None
        return _clone(entry.value)


def ttl_cache_delete(key: str) -> bool:
    with _LOCK:
        return _CACHE.pop(key, None) is not None


def ttl_cache_clear(prefix: Optional[str] = None) -> int:
    """Clear all entries or entries matching a prefix. Returns removed count."""
    with _LOCK:
        if prefix is None:
            count = len(_CACHE)
            _CACHE.clear()
            return count
        keys = [key for key in _CACHE if key.startswith(prefix)]
        for key in keys:
            _CACHE.pop(key, None)
        return len(keys)


def ttl_cache_snapshot() -> Dict[str, Dict[str, Any]]:
    """Return non-sensitive cache metadata for diagnostics."""
    now = _now()
    with _LOCK:
        return {
            key: {
                "age_seconds": round(now - entry.created_at, 3),
                "fresh_seconds_remaining": round(max(entry.expires_at - now, 0), 3),
                "stale_seconds_remaining": round(max((entry.stale_expires_at or entry.expires_at) - now, 0), 3),
            }
            for key, entry in _CACHE.items()
        }
