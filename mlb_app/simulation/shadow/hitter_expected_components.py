"""Shadow-only hitter expected-component evidence from persisted Statcast."""

from __future__ import annotations

import datetime as dt
from collections import OrderedDict
from typing import Any, Iterable, Mapping, Optional


SHADOW_HITTER_EXPECTED_COMPONENTS_VERSION = (
    "shadow_hitter_expected_components_v1"
)

WINDOW_PRIORS = OrderedDict(
    (
        ("current_season", 0.50),
        ("prior_season", 0.30),
        ("career_pre_prior", 0.20),
    )
)

WOBA_DENOM_EVENTS = {
    "field_out",
    "strikeout",
    "single",
    "walk",
    "double",
    "home_run",
    "force_out",
    "grounded_into_double_play",
    "hit_by_pitch",
    "sac_fly",
    "field_error",
    "triple",
    "fielders_choice",
    "double_play",
    "fielders_choice_out",
    "strikeout_double_play",
    "sac_fly_double_play",
    "triple_play",
}

AB_EVENTS = {
    "field_out",
    "strikeout",
    "single",
    "double",
    "home_run",
    "force_out",
    "grounded_into_double_play",
    "field_error",
    "triple",
    "fielders_choice",
    "double_play",
    "fielders_choice_out",
    "strikeout_double_play",
    "triple_play",
}

STRIKEOUT_EVENTS = {"strikeout", "strikeout_double_play"}


def _value(row: Any, field: str) -> Any:
    if isinstance(row, Mapping):
        return row.get(field)
    return getattr(row, field, None)


def _float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        parsed = float(value)
        return parsed if parsed == parsed else None
    except (TypeError, ValueError):
        return None


def _date(value: Any) -> Optional[dt.date]:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    try:
        return dt.date.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None


def _terminal_rows(rows: Iterable[Any]) -> list[Any]:
    """Return one final terminal row for each plate appearance."""

    selected: dict[tuple[Any, ...], Any] = {}
    for row in rows:
        event = str(_value(row, "events") or "").strip().lower()
        if event not in WOBA_DENOM_EVENTS:
            continue
        key = (
            _value(row, "game_pk"),
            _value(row, "at_bat_number"),
        )
        if key == (None, None):
            key = ("row", _value(row, "id"))
        current = selected.get(key)
        order = (
            _value(row, "pitch_number") or -1,
            _value(row, "id") or -1,
        )
        current_order = (
            _value(current, "pitch_number") or -1,
            _value(current, "id") or -1,
        ) if current is not None else (-2, -2)
        if order >= current_order:
            selected[key] = row
    return list(selected.values())


def _window_summary(
    rows: list[Any],
    *,
    minimum_coverage: float,
) -> dict[str, Any]:
    woba_rows = [
        row for row in rows
        if str(_value(row, "events") or "").lower() in WOBA_DENOM_EVENTS
    ]
    xwoba_values = [
        value
        for value in (
            _float(_value(row, "estimated_woba_using_speedangle"))
            for row in woba_rows
        )
        if value is not None
    ]

    ab_rows = [
        row for row in rows
        if str(_value(row, "events") or "").lower() in AB_EVENTS
    ]
    contact_ab_rows = [
        row for row in ab_rows
        if str(_value(row, "events") or "").lower() not in STRIKEOUT_EVENTS
    ]
    xba_contact_values = [
        value
        for value in (
            _float(_value(row, "estimated_ba_using_speedangle"))
            for row in contact_ab_rows
        )
        if value is not None
    ]

    pa = len(woba_rows)
    ab = len(ab_rows)
    xwoba_coverage = len(xwoba_values) / pa if pa else 0.0
    xba_contact_coverage = (
        len(xba_contact_values) / len(contact_ab_rows)
        if contact_ab_rows
        else 0.0
    )
    xwoba = (
        sum(xwoba_values) / len(xwoba_values)
        if xwoba_values and xwoba_coverage >= minimum_coverage
        else None
    )
    # Statcast expected BA is contact-only. Strikeout ABs contribute zero.
    xba = (
        sum(xba_contact_values) / ab
        if ab and xba_contact_coverage >= minimum_coverage
        else None
    )
    dates = [_date(_value(row, "game_date")) for row in rows]
    dates = [value for value in dates if value is not None]

    return {
        "pa": pa,
        "ab": ab,
        "contact_ab": len(contact_ab_rows),
        "xwoba_sample": len(xwoba_values),
        "xba_contact_sample": len(xba_contact_values),
        "xwoba_coverage": round(xwoba_coverage, 6),
        "xba_contact_coverage": round(xba_contact_coverage, 6),
        "xwoba": round(xwoba, 6) if xwoba is not None else None,
        "xba": round(xba, 6) if xba is not None else None,
        "start_date": min(dates).isoformat() if dates else None,
        "end_date": max(dates).isoformat() if dates else None,
    }


def build_shadow_hitter_expected_components(
    *,
    player_id: int,
    season: int,
    split: str,
    statcast_events: Iterable[Any],
    as_of_date: dt.date,
    source_latest_date: Optional[dt.date] = None,
    minimum_current_pa: int = 25,
    minimum_coverage: float = 0.80,
    maximum_source_age_days: int = 14,
) -> dict[str, Any]:
    """Build disjoint, freshness-gated expected evidence without authority."""

    required_throw = {"vsR": "R", "vsL": "L"}.get(split)
    filtered = []
    future_rows_excluded = 0
    for row in statcast_events:
        game_date = _date(_value(row, "game_date"))
        if game_date is None:
            continue
        if game_date > as_of_date:
            future_rows_excluded += 1
            continue
        if required_throw and _value(row, "p_throws") != required_throw:
            continue
        filtered.append(row)

    terminals = _terminal_rows(filtered)
    grouped = {
        "current_season": [
            row for row in terminals
            if _date(_value(row, "game_date")).year == season
        ],
        "prior_season": [
            row for row in terminals
            if _date(_value(row, "game_date")).year == season - 1
        ],
        "career_pre_prior": [
            row for row in terminals
            if _date(_value(row, "game_date")).year < season - 1
        ],
    }
    windows = {
        name: _window_summary(rows, minimum_coverage=minimum_coverage)
        for name, rows in grouped.items()
    }

    latest_dates = [
        _date(_value(row, "game_date")) for row in terminals
        if _date(_value(row, "game_date")) is not None
    ]
    player_latest_date = max(latest_dates) if latest_dates else None
    latest_date = source_latest_date or player_latest_date
    source_age_days = (
        (as_of_date - latest_date).days if latest_date is not None else None
    )

    blockers = []
    warnings = []
    if windows["current_season"]["pa"] < minimum_current_pa:
        blockers.append("insufficient_current_season_pa")
    if windows["current_season"]["xwoba"] is None:
        blockers.append("insufficient_current_xwoba_coverage")
    if windows["current_season"]["xba"] is None:
        blockers.append("insufficient_current_xba_coverage")
    if source_age_days is None:
        blockers.append("missing_statcast_source")
    elif source_age_days > maximum_source_age_days:
        blockers.append("stale_statcast_source")
    if sum(window["xwoba"] is not None for window in windows.values()) < 2:
        blockers.append("insufficient_xwoba_windows")
    if sum(window["xba"] is not None for window in windows.values()) < 2:
        blockers.append("insufficient_xba_windows")
    if future_rows_excluded:
        warnings.append("future_rows_excluded")

    effective_weights = {}
    for name, prior in WINDOW_PRIORS.items():
        pa = windows[name]["pa"]
        effective_weights[name] = prior * min(1.0, pa / 200.0)
    total_weight = sum(effective_weights.values())
    normalized_weights = {
        name: (value / total_weight if total_weight else 0.0)
        for name, value in effective_weights.items()
    }
    for name, weight in normalized_weights.items():
        windows[name]["normalized_weight"] = round(weight, 6)

    def blended(metric: str) -> Optional[float]:
        available = [
            (windows[name][metric], normalized_weights[name])
            for name in WINDOW_PRIORS
            if windows[name][metric] is not None
        ]
        denominator = sum(weight for _, weight in available)
        if not denominator:
            return None
        return round(
            sum(value * weight for value, weight in available) / denominator,
            6,
        )

    return {
        "schema_version": SHADOW_HITTER_EXPECTED_COMPONENTS_VERSION,
        "status": "blocked" if blockers else "ready",
        "shadow_only": True,
        "production_authority_changed": False,
        "player_id": int(player_id),
        "season": int(season),
        "split": split,
        "as_of_date": as_of_date.isoformat(),
        "source_latest_date": (
            latest_date.isoformat() if latest_date is not None else None
        ),
        "player_latest_date": (
            player_latest_date.isoformat()
            if player_latest_date is not None
            else None
        ),
        "source_age_days": source_age_days,
        "future_rows_excluded": future_rows_excluded,
        "windows": windows,
        "blended_expected_metrics": {
            "xwoba": blended("xwoba"),
            "xba": blended("xba"),
        },
        "blockers": blockers,
        "warnings": warnings,
        "expected_adjustment_applied": False,
    }


def load_shadow_hitter_expected_components(
    session: Any,
    *,
    player_id: int,
    season: int,
    split: str,
    as_of_date: dt.date,
    career_start_season: Optional[int] = None,
) -> dict[str, Any]:
    """Load persisted Statcast rows with an explicit historical cutoff."""

    from sqlalchemy import func

    from mlb_app.database import StatcastEvent

    start_season = career_start_season or max(2008, season - 10)
    query = session.query(StatcastEvent).filter(
        StatcastEvent.batter_id == int(player_id),
        StatcastEvent.game_date >= dt.date(start_season, 1, 1),
        StatcastEvent.game_date <= as_of_date,
        StatcastEvent.events.isnot(None),
    )
    required_throw = {"vsR": "R", "vsL": "L"}.get(split)
    if required_throw:
        query = query.filter(StatcastEvent.p_throws == required_throw)
    rows = query.order_by(
        StatcastEvent.game_date,
        StatcastEvent.game_pk,
        StatcastEvent.at_bat_number,
        StatcastEvent.pitch_number,
        StatcastEvent.id,
    ).all()
    source_latest_date = (
        session.query(func.max(StatcastEvent.game_date))
        .filter(StatcastEvent.game_date <= as_of_date)
        .scalar()
    )
    result = build_shadow_hitter_expected_components(
        player_id=player_id,
        season=season,
        split=split,
        statcast_events=rows,
        as_of_date=as_of_date,
        source_latest_date=source_latest_date,
    )
    result["storage_evidence"] = {
        "raw_row_count": len(rows),
        "career_start_season": start_season,
    }
    return result
