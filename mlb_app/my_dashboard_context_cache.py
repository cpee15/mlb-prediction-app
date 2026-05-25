from __future__ import annotations

from typing import Any, Dict

from .shared_payload_cache import env_ttl, get_or_set, make_cache_key

_installed = False
_original_stored_365 = None
_original_pitcher_lean = None


def install_dashboard_context_cache() -> None:
    """Install shared-cache wrappers around expensive My Dashboard context builders.

    `my_dashboard_solver.py` imports `build_stored_365_sweep_context` and
    `build_pitcher_lean_context` as module globals. Patching those globals here
    keeps the existing solver logic intact while preventing hitters, pitchers,
    overall_players, and active-lineup paths from rebuilding the same expensive
    contexts repeatedly for the same date.
    """
    global _installed, _original_stored_365, _original_pitcher_lean
    if _installed:
        return

    from . import my_dashboard_solver as solver

    _original_stored_365 = solver.build_stored_365_sweep_context
    _original_pitcher_lean = solver.build_pitcher_lean_context

    def cached_stored_365_sweep_context(session, date: str) -> Dict[str, Any]:
        target_date = str(date or "")[:10]
        cache_key = make_cache_key("dashboard_context", "stored_365_sweep", target_date)
        ttl_seconds = env_ttl("DASHBOARD_CONTEXT_CACHE_TTL_SECONDS")
        return get_or_set(cache_key, ttl_seconds, lambda: _original_stored_365(session, target_date))

    def cached_pitcher_lean_context(session, date: str) -> Dict[str, Any]:
        target_date = str(date or "")[:10]
        cache_key = make_cache_key("dashboard_context", "pitcher_lean", target_date)
        ttl_seconds = env_ttl("DASHBOARD_CONTEXT_CACHE_TTL_SECONDS")
        return get_or_set(cache_key, ttl_seconds, lambda: _original_pitcher_lean(session, target_date))

    solver.build_stored_365_sweep_context = cached_stored_365_sweep_context
    solver.build_pitcher_lean_context = cached_pitcher_lean_context
    _installed = True


__all__ = ["install_dashboard_context_cache"]
