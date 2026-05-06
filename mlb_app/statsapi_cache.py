from __future__ import annotations

import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any, Dict, Optional

import requests


VALID_CACHE_MODES = {"live_then_cache", "cache_only", "refresh"}


class StatsApiCacheMiss(RuntimeError):
    pass


def get_cache_mode() -> str:
    mode = os.getenv("STATSAPI_CACHE_MODE", "live_then_cache").strip().lower()
    if mode not in VALID_CACHE_MODES:
        raise ValueError(
            f"Invalid STATSAPI_CACHE_MODE={mode!r}. "
            f"Expected one of {sorted(VALID_CACHE_MODES)}"
        )
    return mode


def get_cache_dir() -> Path:
    return Path(os.getenv("STATSAPI_CACHE_DIR", "tmp/statsapi_cache"))


def _safe_part(value: Any) -> str:
    text = str(value)
    keep = []
    for char in text:
        if char.isalnum() or char in {"-", "_", "."}:
            keep.append(char)
        else:
            keep.append("_")
    return "".join(keep).strip("_") or "unknown"


def make_cache_key(endpoint: str, identifier: Any, params: Optional[Dict[str, Any]] = None) -> str:
    endpoint_part = _safe_part(endpoint)
    identifier_part = _safe_part(identifier)

    params = params or {}
    if params:
        params_json = json.dumps(params, sort_keys=True, separators=(",", ":"))
        params_hash = hashlib.sha1(params_json.encode("utf-8")).hexdigest()[:12]
        return f"{endpoint_part}/{identifier_part}_{params_hash}"

    return f"{endpoint_part}/{identifier_part}"


def cache_path_for_key(cache_key: str) -> Path:
    normalized = cache_key.strip().strip("/")
    return get_cache_dir() / f"{normalized}.json"


def get_cached_json(cache_key: str) -> Optional[Dict[str, Any]]:
    path = cache_path_for_key(cache_key)
    if not path.exists():
        return None
    return json.loads(path.read_text())


def set_cached_json(cache_key: str, payload: Dict[str, Any]) -> Path:
    path = cache_path_for_key(cache_key)
    path.parent.mkdir(parents=True, exist_ok=True)

    fd, tmp_name = tempfile.mkstemp(prefix=path.name, suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
        os.replace(tmp_name, path)
    finally:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)

    return path


def fetch_json_with_cache(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    cache_key: Optional[str] = None,
    timeout: int = 30,
) -> Dict[str, Any]:
    if not cache_key:
        cache_key = make_cache_key("generic", url, params)

    mode = get_cache_mode()
    path = cache_path_for_key(cache_key)

    cached = get_cached_json(cache_key)
    if mode == "cache_only":
        if cached is None:
            raise StatsApiCacheMiss(
                f"StatsAPI cache miss for key={cache_key!r} path={path}. "
                "Run once with STATSAPI_CACHE_MODE=refresh or live_then_cache to warm the cache."
            )
        return cached

    if mode == "live_then_cache" and cached is not None:
        return cached

    try:
        resp = requests.get(url, params=params, timeout=timeout)
        resp.raise_for_status()
        payload = resp.json()
        set_cached_json(cache_key, payload)
        return payload
    except Exception:
        # In refresh mode, fail loudly: caller explicitly asked to refresh.
        if mode == "refresh":
            raise

        # In live_then_cache, fallback to cache if it exists and live fetch failed.
        if cached is not None:
            return cached

        raise


def fetch_json_with_cache_diagnostics(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    cache_key: Optional[str] = None,
    timeout: int = 30,
) -> Dict[str, Any]:
    if not cache_key:
        cache_key = make_cache_key("generic", url, params)

    mode = get_cache_mode()
    path = cache_path_for_key(cache_key)
    cached = get_cached_json(cache_key)

    diagnostics = {
        "cache_hit": False,
        "cache_path": str(path),
        "cache_mode": mode,
        "fetched_live": False,
        "fetch_error": None,
        "payload": None,
    }

    if mode == "cache_only":
        if cached is None:
            diagnostics["fetch_error"] = (
                f"StatsAPI cache miss for key={cache_key!r} path={path}"
            )
            raise StatsApiCacheMiss(diagnostics["fetch_error"])
        diagnostics["cache_hit"] = True
        diagnostics["payload"] = cached
        return diagnostics

    if mode == "live_then_cache" and cached is not None:
        diagnostics["cache_hit"] = True
        diagnostics["payload"] = cached
        return diagnostics

    try:
        resp = requests.get(url, params=params, timeout=timeout)
        resp.raise_for_status()
        payload = resp.json()
        set_cached_json(cache_key, payload)
        diagnostics["fetched_live"] = True
        diagnostics["payload"] = payload
        return diagnostics
    except Exception as exc:
        diagnostics["fetch_error"] = str(exc)
        if mode != "refresh" and cached is not None:
            diagnostics["cache_hit"] = True
            diagnostics["payload"] = cached
            return diagnostics
        raise
