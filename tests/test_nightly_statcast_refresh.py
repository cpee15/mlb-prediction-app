from __future__ import annotations

import datetime as dt
import importlib.util
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "nightly_statcast_refresh.py"
spec = importlib.util.spec_from_file_location("nightly_statcast_refresh", MODULE_PATH)
nightly = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(nightly)


def test_resolve_target_dates_single_date():
    assert nightly.resolve_target_dates("2026-05-19", days=2, include_today=True) == ["2026-05-19"]


def test_resolve_target_dates_rejects_bad_date():
    try:
        nightly.resolve_target_dates("2026-99-99", days=2, include_today=True)
    except ValueError:
        return
    raise AssertionError("Expected invalid date to raise ValueError")


def test_database_url_requires_explicit_local_fallback(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    try:
        nightly._database_url(allow_sqlite_local=False)
    except RuntimeError as exc:
        assert "DATABASE_URL is required" in str(exc)
        return
    raise AssertionError("Expected missing DATABASE_URL to raise RuntimeError")


def test_database_url_allows_sqlite_for_local_validation(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    assert nightly._database_url(allow_sqlite_local=True) == "sqlite:///mlb.db"
