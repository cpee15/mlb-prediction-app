from __future__ import annotations

import contextvars
import math
import time
from collections import defaultdict, deque
from typing import Any, Deque, Dict, Iterable, List, Optional

try:
    import fastapi as _fastapi
except Exception:  # pragma: no cover - FastAPI may be absent in narrow unit tests.
    _fastapi = None


_MAX_SAMPLES = 1000
_SAMPLES: Deque[Dict[str, Any]] = deque(maxlen=_MAX_SAMPLES)
_CACHE_STATUS: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "mlb_cache_status",
    default=None,
)
_PATCHED = False
_ORIGINAL_FASTAPI = None


def record_cache_status(status: Optional[str]) -> None:
    """Record cache status for the current request context."""
    if status:
        _CACHE_STATUS.set(str(status).upper())


def _percentile(values: List[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = int(math.ceil((percentile / 100.0) * len(ordered))) - 1
    index = min(max(index, 0), len(ordered) - 1)
    return round(ordered[index], 3)


def _route_label(request: Any) -> str:
    route = request.scope.get("route") if getattr(request, "scope", None) else None
    route_path = getattr(route, "path", None)
    return route_path or getattr(getattr(request, "url", None), "path", None) or "unknown"


def _response_size_bytes(response: Any) -> Optional[int]:
    try:
        content_length = response.headers.get("content-length")
        return int(content_length) if content_length is not None else None
    except Exception:
        return None


def record_request_sample(sample: Dict[str, Any]) -> None:
    _SAMPLES.append(sample)


def performance_snapshot() -> Dict[str, Any]:
    """Return bounded, non-sensitive route timing diagnostics."""
    samples = list(_SAMPLES)
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for sample in samples:
        grouped[str(sample.get("route") or sample.get("path") or "unknown")].append(sample)

    routes: Dict[str, Dict[str, Any]] = {}
    for route, rows in grouped.items():
        durations = [float(row.get("duration_ms") or 0) for row in rows]
        routes[route] = {
            "count": len(rows),
            "p50_ms": _percentile(durations, 50),
            "p95_ms": _percentile(durations, 95),
            "max_ms": round(max(durations), 3) if durations else 0.0,
            "cache": dict(defaultdict(int, ((str(row.get("cache_status") or "NONE"), 0) for row in []))),
        }
        cache_counts: Dict[str, int] = defaultdict(int)
        for row in rows:
            cache_counts[str(row.get("cache_status") or "NONE")] += 1
        routes[route]["cache"] = dict(cache_counts)

    slow_requests = sorted(
        samples,
        key=lambda row: float(row.get("duration_ms") or 0),
        reverse=True,
    )[:20]

    return {
        "status": "ok",
        "sample_count": len(samples),
        "sample_limit": _MAX_SAMPLES,
        "routes": routes,
        "slow_requests": slow_requests,
    }


def install_fastapi_performance_patch() -> bool:
    """Patch fastapi.FastAPI so app.py gets timing middleware without a large app.py rewrite.

    This is intentionally small and reversible. It only adds response timing
    headers, bounded in-process samples, and /debug/performance. It does not
    inspect request bodies or headers.
    """
    global _PATCHED, _ORIGINAL_FASTAPI
    if _PATCHED or _fastapi is None:
        return _PATCHED

    original_fastapi = _fastapi.FastAPI
    _ORIGINAL_FASTAPI = original_fastapi

    class InstrumentedFastAPI(original_fastapi):  # type: ignore[misc, valid-type]
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)

            @self.middleware("http")
            async def _performance_middleware(request: Any, call_next: Any) -> Any:
                token = _CACHE_STATUS.set(None)
                started = time.perf_counter()
                status_code = 500
                response = None
                try:
                    response = await call_next(request)
                    status_code = getattr(response, "status_code", 500)
                    return response
                finally:
                    duration_ms = round((time.perf_counter() - started) * 1000, 3)
                    cache_status = _CACHE_STATUS.get()
                    route = _route_label(request)
                    sample = {
                        "method": getattr(request, "method", None),
                        "path": getattr(getattr(request, "url", None), "path", None),
                        "route": route,
                        "status_code": status_code,
                        "duration_ms": duration_ms,
                        "response_size_bytes": _response_size_bytes(response),
                        "cache_status": cache_status,
                    }
                    record_request_sample(sample)
                    if response is not None:
                        response.headers["X-Response-Time-ms"] = str(duration_ms)
                        if cache_status:
                            response.headers["X-Cache"] = cache_status
                    _CACHE_STATUS.reset(token)

            async def _debug_performance() -> Dict[str, Any]:
                return performance_snapshot()

            self.add_api_route(
                "/debug/performance",
                _debug_performance,
                methods=["GET"],
                include_in_schema=False,
            )

    _fastapi.FastAPI = InstrumentedFastAPI
    _PATCHED = True
    return True


__all__ = [
    "install_fastapi_performance_patch",
    "performance_snapshot",
    "record_cache_status",
    "record_request_sample",
]
