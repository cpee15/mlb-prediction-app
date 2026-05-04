"""Transparent starting pitcher arsenal refresh helpers.

This module builds UI-ready pitcher arsenal rows while preserving the legacy
``pitch_arsenal`` table contract. It is intentionally isolated from app route
wiring so matchup endpoints can adopt it behind a safe fallback boundary.
"""

from __future__ import annotations

import datetime as dt
from typing import Any, Dict, Iterable, List, Optional, Sequence

import pandas as pd

from .database import PitchArsenal
from .statcast_utils import fetch_pitch_arsenal_leaderboard, fetch_statcast_pitcher_data

LEGACY_FIELDS = [
    "pitch_type",
    "pitch_name",
    "pitch_count",
    "usage_pct",
    "whiff_pct",
    "strikeout_pct",
    "rv_per_100",
    "xwoba",
    "hard_hit_pct",
]

TRANSPARENCY_FIELDS = [
    "swings",
    "whiffs",
    "pa_ended",
    "strikeouts",
    "batted_ball_count",
    "hard_hit_count",
    "xwoba_sample_count",
    "source",
    "source_window",
    "season",
    "refreshed_at",
    "quality_flags",
]

SWING_PATTERN = r"swing|foul|hit_into_play"
WHIFF_PATTERN = r"swinging_strike"
STRIKEOUT_EVENTS = {"strikeout", "strikeout_double_play"}


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        parsed = float(value)
        if pd.isna(parsed):
            return None
        return parsed
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value is None or value == "":
            return None
        parsed = int(float(value))
        return parsed
    except (TypeError, ValueError):
        return None


def _first_present(row: Any, keys: Sequence[str]) -> Any:
    for key in keys:
        try:
            value = row.get(key)
        except AttributeError:
            value = None
        if value is not None and not (isinstance(value, float) and pd.isna(value)):
            return value
    return None


def normalize_rate(value: Any) -> Optional[float]:
    """Normalize percentage-like values to decimal rates.

    Baseball Savant leaderboards may return either ``32.5`` or ``0.325``. The
    UI expects decimals so formatters can render percentages consistently.
    """
    parsed = _safe_float(value)
    if parsed is None:
        return None
    if parsed > 1:
        return round(parsed / 100.0, 4)
    return round(parsed, 4)


def quality_flags_for_pitch(row: Dict[str, Any]) -> List[str]:
    """Return sample and source warnings for one UI-ready arsenal row."""
    flags: List[str] = []
    pitch_count = _safe_int(row.get("pitch_count")) or 0
    pa_ended = _safe_int(row.get("pa_ended")) or 0
    batted_ball_count = _safe_int(row.get("batted_ball_count")) or 0
    xwoba_sample_count = _safe_int(row.get("xwoba_sample_count")) or 0

    if row.get("source") == "raw_statcast_aggregated":
        flags.append("raw_statcast_fallback")
    if pitch_count < 50:
        flags.append("small_pitch_sample")
    if pa_ended < 10:
        flags.append("unstable_pa_end_k_rate")
    if xwoba_sample_count == 0:
        flags.append("no_xwoba_sample")
    if 0 < batted_ball_count < 10:
        flags.append("low_batted_ball_sample")

    return flags


def _base_payload(
    *,
    pitcher_id: int,
    season: int,
    pitch_type: Any,
    pitch_name: Any,
    pitch_count: Any,
    usage_pct: Any,
    whiff_pct: Any,
    strikeout_pct: Any,
    rv_per_100: Any,
    xwoba: Any,
    hard_hit_pct: Any,
    source: str,
    source_window: str,
    refreshed_at: Optional[str] = None,
    swings: Any = None,
    whiffs: Any = None,
    pa_ended: Any = None,
    strikeouts: Any = None,
    batted_ball_count: Any = None,
    hard_hit_count: Any = None,
    xwoba_sample_count: Any = None,
) -> Dict[str, Any]:
    row = {
        "pitcher_id": pitcher_id,
        "season": season,
        "pitch_type": str(pitch_type) if pitch_type is not None else None,
        "pitch_name": str(pitch_name) if pitch_name is not None else (str(pitch_type) if pitch_type is not None else None),
        "pitch_count": _safe_int(pitch_count),
        "usage_pct": normalize_rate(usage_pct),
        "whiff_pct": normalize_rate(whiff_pct),
        "strikeout_pct": normalize_rate(strikeout_pct),
        "rv_per_100": _safe_float(rv_per_100),
        "xwoba": _safe_float(xwoba),
        "hard_hit_pct": normalize_rate(hard_hit_pct),
        "swings": _safe_int(swings),
        "whiffs": _safe_int(whiffs),
        "pa_ended": _safe_int(pa_ended),
        "strikeouts": _safe_int(strikeouts),
        "batted_ball_count": _safe_int(batted_ball_count),
        "hard_hit_count": _safe_int(hard_hit_count),
        "xwoba_sample_count": _safe_int(xwoba_sample_count),
        "source": source,
        "source_window": source_window,
        "refreshed_at": refreshed_at or dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "quality_flags": [],
    }
    row["quality_flags"] = quality_flags_for_pitch(row)
    return row


def aggregate_pitch_rows_to_arsenal(
    df: pd.DataFrame,
    pitcher_id: int,
    season: int,
    source_window: str,
) -> List[Dict[str, Any]]:
    """Aggregate raw Statcast pitch rows into transparent arsenal rows."""
    if df is None or df.empty or "pitch_type" not in df.columns:
        return []

    work = df.copy()
    work["pitch_type"] = work["pitch_type"].fillna("").astype(str)
    work["description"] = work.get("description", pd.Series(dtype=str)).fillna("").astype(str)
    work["events"] = work.get("events", pd.Series(dtype=str)).fillna("").astype(str)
    work["launch_speed"] = pd.to_numeric(work.get("launch_speed", pd.Series(dtype=float)), errors="coerce")
    work["estimated_woba_using_speedangle"] = pd.to_numeric(
        work.get("estimated_woba_using_speedangle", pd.Series(dtype=float)), errors="coerce"
    )

    total_pitches = int((work["pitch_type"].str.strip() != "").sum())
    if total_pitches <= 0:
        return []

    rows: List[Dict[str, Any]] = []
    for pitch_type, group in work.groupby("pitch_type"):
        pitch_type = str(pitch_type).strip()
        if not pitch_type:
            continue

        pitch_count = len(group)
        descriptions = group["description"].fillna("").astype(str)
        events = group["events"].fillna("").astype(str)
        swings = int(descriptions.str.contains(SWING_PATTERN, case=False, regex=True).sum())
        whiffs = int(descriptions.str.contains(WHIFF_PATTERN, case=False, regex=True).sum())
        pa_ended = int((events.str.strip() != "").sum())
        strikeouts = int(events.isin(STRIKEOUT_EVENTS).sum())
        batted_ball_count = int(group["launch_speed"].notna().sum())
        hard_hit_count = int((group["launch_speed"] >= 95).sum())
        xwoba_values = group["estimated_woba_using_speedangle"].dropna()
        xwoba_sample_count = int(xwoba_values.shape[0])

        rows.append(
            _base_payload(
                pitcher_id=pitcher_id,
                season=season,
                pitch_type=pitch_type,
                pitch_name=pitch_type,
                pitch_count=pitch_count,
                usage_pct=pitch_count / total_pitches,
                whiff_pct=(whiffs / swings) if swings > 0 else None,
                strikeout_pct=(strikeouts / pa_ended) if pa_ended > 0 else None,
                rv_per_100=None,
                xwoba=float(xwoba_values.mean()) if xwoba_sample_count > 0 else None,
                hard_hit_pct=(hard_hit_count / batted_ball_count) if batted_ball_count > 0 else None,
                source="raw_statcast_aggregated",
                source_window=source_window,
                swings=swings,
                whiffs=whiffs,
                pa_ended=pa_ended,
                strikeouts=strikeouts,
                batted_ball_count=batted_ball_count,
                hard_hit_count=hard_hit_count,
                xwoba_sample_count=xwoba_sample_count,
            )
        )

    rows.sort(key=lambda row: row.get("usage_pct") or 0, reverse=True)
    return rows


def build_arsenal_payload_from_pitch_arsenal_rows(
    rows: Iterable[Any],
    season: int,
    source: str,
    source_window: str,
) -> List[Dict[str, Any]]:
    """Convert legacy ORM rows into transparent UI-ready payload rows."""
    payload: List[Dict[str, Any]] = []
    for raw in rows or []:
        pitcher_id = getattr(raw, "pitcher_id", None)
        payload.append(
            _base_payload(
                pitcher_id=int(pitcher_id) if pitcher_id is not None else 0,
                season=season,
                pitch_type=getattr(raw, "pitch_type", None),
                pitch_name=getattr(raw, "pitch_name", None),
                pitch_count=getattr(raw, "pitch_count", None),
                usage_pct=getattr(raw, "usage_pct", None),
                whiff_pct=getattr(raw, "whiff_pct", None),
                strikeout_pct=getattr(raw, "strikeout_pct", None),
                rv_per_100=getattr(raw, "rv_per_100", None),
                xwoba=getattr(raw, "xwoba", None),
                hard_hit_pct=getattr(raw, "hard_hit_pct", None),
                source=source,
                source_window=source_window,
                xwoba_sample_count=None if getattr(raw, "xwoba", None) is None else 1,
            )
        )
    payload.sort(key=lambda row: row.get("usage_pct") or 0, reverse=True)
    return payload


def _leaderboard_rows_to_payload(df: pd.DataFrame, pitcher_id: int, season: int) -> List[Dict[str, Any]]:
    if df is None or df.empty:
        return []
    pid_col = next((c for c in ["pitcher", "player_id", "mlbam_id", "pitcher_id"] if c in df.columns), None)
    if not pid_col:
        return []

    matched = df[df[pid_col].astype(str) == str(pitcher_id)]
    if matched.empty:
        return []

    payload: List[Dict[str, Any]] = []
    for _, row in matched.iterrows():
        pitch_type = _first_present(row, ["pitch_type"])
        if not pitch_type:
            continue
        payload.append(
            _base_payload(
                pitcher_id=pitcher_id,
                season=season,
                pitch_type=pitch_type,
                pitch_name=_first_present(row, ["pitch_name"]),
                pitch_count=_first_present(row, ["pitches", "pitch_count"]),
                usage_pct=_first_present(row, ["pitch_usage", "usage_pct"]),
                whiff_pct=_first_present(row, ["whiff_percent", "whiff_pct"]),
                strikeout_pct=_first_present(row, ["k_percent", "strikeout_pct"]),
                rv_per_100=_first_present(row, ["run_value_per_100", "rv_per_100"]),
                xwoba=_first_present(row, ["est_woba", "xwoba"]),
                hard_hit_pct=_first_present(row, ["hard_hit_percent", "hard_hit_pct"]),
                source="savant_arsenal_leaderboard",
                source_window=f"{season} season leaderboard",
                xwoba_sample_count=1 if _first_present(row, ["est_woba", "xwoba"]) is not None else 0,
            )
        )
    payload.sort(key=lambda row: row.get("usage_pct") or 0, reverse=True)
    return payload


def _persist_legacy_rows(session: Any, rows: List[Dict[str, Any]]) -> None:
    if session is None or not rows:
        return
    try:
        for row in rows:
            pitch_type = row.get("pitch_type")
            if not pitch_type:
                continue
            existing = (
                session.query(PitchArsenal)
                .filter_by(
                    season=row.get("season"),
                    pitcher_id=row.get("pitcher_id"),
                    pitch_type=pitch_type,
                )
                .first()
            )
            target = existing or PitchArsenal(
                season=row.get("season"),
                pitcher_id=row.get("pitcher_id"),
                pitch_type=pitch_type,
            )
            if existing is None:
                session.add(target)
            for field in ["pitch_name", "pitch_count", "usage_pct", "whiff_pct", "strikeout_pct", "rv_per_100", "xwoba", "hard_hit_pct"]:
                if hasattr(target, field):
                    setattr(target, field, row.get(field))
        session.commit()
    except Exception:
        try:
            session.rollback()
        except Exception:
            pass


def _parse_target_date(target_date: Any) -> dt.date:
    try:
        return dt.date.fromisoformat(str(target_date)[:10])
    except Exception:
        return dt.date.today()


def refresh_starting_pitcher_arsenal(
    session: Any,
    pitcher_id: int,
    season: int,
    target_date: Any,
    window_days: int = 365,
) -> List[Dict[str, Any]]:
    """Return transparent arsenal rows, preferring Savant leaderboard data.

    The function never raises for provider failures; it returns an empty list if
    neither leaderboard nor raw Statcast data can produce rows.
    """
    pitcher_id = int(pitcher_id)
    season = int(season)

    try:
        leaderboard = fetch_pitch_arsenal_leaderboard(season, min_pitches=1)
        payload = _leaderboard_rows_to_payload(leaderboard, pitcher_id, season)
        if payload:
            _persist_legacy_rows(session, payload)
            return payload
    except Exception:
        pass

    end_date = _parse_target_date(target_date)
    start_date = end_date - dt.timedelta(days=window_days)
    source_window = f"{start_date.isoformat()} to {end_date.isoformat()}"

    try:
        raw_df = fetch_statcast_pitcher_data(pitcher_id, start_date.isoformat(), end_date.isoformat())
        payload = aggregate_pitch_rows_to_arsenal(raw_df, pitcher_id, season, source_window)
        if payload:
            _persist_legacy_rows(session, payload)
            return payload
    except Exception:
        return []

    return []


__all__ = [
    "normalize_rate",
    "quality_flags_for_pitch",
    "aggregate_pitch_rows_to_arsenal",
    "build_arsenal_payload_from_pitch_arsenal_rows",
    "refresh_starting_pitcher_arsenal",
]
