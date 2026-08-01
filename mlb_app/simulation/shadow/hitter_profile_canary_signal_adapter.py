"""Cutoff-safe Statcast signal adapter for the hitter canary."""

from __future__ import annotations

import datetime as dt
import math
import statistics
from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Any


SIGNAL_ADAPTER_SCHEMA_VERSION = (
    "hitter_profile_canary_signal_adapter_v1"
)

AB_EVENTS = {
    "field_out",
    "force_out",
    "grounded_into_double_play",
    "field_error",
    "fielders_choice",
    "double_play",
    "fielders_choice_out",
    "triple_play",
    "single",
    "double",
    "triple",
    "home_run",
    "strikeout",
    "strikeout_double_play",
}
BBE_EVENTS = AB_EVENTS - {
    "strikeout",
    "strikeout_double_play",
}
HIT_EVENTS = {
    "single",
    "double",
    "triple",
    "home_run",
}
SWING_DESCRIPTIONS = {
    "swinging_strike",
    "swinging_strike_blocked",
    "missed_bunt",
    "foul",
    "foul_tip",
    "foul_bunt",
    "bunt_foul_tip",
    "hit_into_play",
}
WHIFF_DESCRIPTIONS = {
    "swinging_strike",
    "swinging_strike_blocked",
    "missed_bunt",
}
CALLED_BALL_DESCRIPTIONS = {
    "ball",
    "blocked_ball",
    "pitchout",
    "automatic_ball",
}
SINGLE_WEIGHT = 0.90


def _value(
    row: Any,
    key: str,
) -> Any:
    if isinstance(row, Mapping):
        return row.get(key)
    return getattr(row, key, None)


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return (
        result
        if math.isfinite(result)
        else None
    )


def _date(value: Any) -> dt.date | None:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    try:
        return dt.date.fromisoformat(
            str(value)
        )
    except (TypeError, ValueError):
        return None


def _terminal_rows(
    rows: Sequence[Any],
) -> list[Any]:
    deduped = {}

    for index, row in enumerate(rows):
        game_pk = _value(row, "game_pk")
        at_bat = _value(
            row,
            "at_bat_number",
        )
        row_id = _value(row, "id")

        if (
            game_pk is not None
            and at_bat is not None
        ):
            key = (
                int(game_pk),
                int(at_bat),
            )
        else:
            key = (
                "row",
                int(row_id or index),
            )

        order = (
            _number(
                _value(row, "pitch_number")
            )
            or 0.0,
            _number(row_id) or float(index),
        )
        previous = deduped.get(key)

        if (
            previous is None
            or order > previous[0]
        ):
            deduped[key] = (
                order,
                row,
            )

    return [
        value[1]
        for value in deduped.values()
    ]


def build_hitter_profile_canary_signals(
    rows: Sequence[Any],
    *,
    player_id: int,
    season: int,
    split: str,
    as_of_date: Any,
    minimum_pitches: int = 100,
    minimum_swings: int = 50,
    minimum_ab: int = 50,
    minimum_expected_bbe: int = 20,
    minimum_expected_coverage: float = 0.80,
    minimum_hits: int = 15,
) -> dict[str, Any]:
    """Build pre-cutoff canary signals from pitch rows."""

    cutoff = _date(as_of_date)
    expected_hand = (
        "R"
        if split == "vsR"
        else "L"
        if split == "vsL"
        else None
    )

    filtered = []
    for row in rows:
        row_date = _date(
            _value(row, "game_date")
        )
        batter_id = _value(
            row,
            "batter_id",
        )
        hand = _value(row, "p_throws")

        if cutoff is None or row_date is None:
            continue
        if row_date > cutoff:
            continue
        if row_date.year != int(season):
            continue
        if (
            batter_id is None
            or int(batter_id)
            != int(player_id)
        ):
            continue
        if (
            expected_hand is not None
            and hand != expected_hand
        ):
            continue
        filtered.append(row)

    descriptions = [
        str(
            _value(row, "description")
            or ""
        )
        for row in filtered
    ]
    pitch_count = len(filtered)
    swing_count = sum(
        description
        in SWING_DESCRIPTIONS
        for description in descriptions
    )
    whiff_count = sum(
        description
        in WHIFF_DESCRIPTIONS
        for description in descriptions
    )
    called_ball_count = sum(
        description
        in CALLED_BALL_DESCRIPTIONS
        for description in descriptions
    )

    terminal = [
        row
        for row in _terminal_rows(filtered)
        if _value(row, "events")
        in AB_EVENTS
    ]
    ab_count = len(terminal)
    bbe = [
        row
        for row in terminal
        if _value(row, "events")
        in BBE_EVENTS
    ]
    expected_rows = [
        row
        for row in bbe
        if (
            _number(
                _value(
                    row,
                    "estimated_ba_using_speedangle",
                )
            )
            is not None
            and _number(
                _value(
                    row,
                    "estimated_woba_using_speedangle",
                )
            )
            is not None
        )
    ]
    expected_coverage = (
        len(expected_rows) / len(bbe)
        if bbe
        else 0.0
    )

    expected_damage = [
        max(
            _number(
                _value(
                    row,
                    "estimated_woba_using_speedangle",
                )
            )
            - SINGLE_WEIGHT
            * _number(
                _value(
                    row,
                    "estimated_ba_using_speedangle",
                )
            ),
            0.0,
        )
        for row in expected_rows
    ]
    expected_damage_per_bbe = (
        statistics.fmean(
            expected_damage
        )
        if expected_damage
        else None
    )
    expected_damage_per_ab = (
        expected_damage_per_bbe
        * len(bbe)
        / ab_count
        if (
            expected_damage_per_bbe
            is not None
            and ab_count > 0
        )
        else None
    )

    hit_counts = Counter(
        _value(row, "events")
        for row in terminal
        if _value(row, "events")
        in HIT_EVENTS
    )
    hit_count = sum(
        hit_counts.values()
    )
    actual_allocation = (
        {
            hit_type:
                hit_counts[hit_type]
                / hit_count
            for hit_type in (
                "single",
                "double",
                "triple",
                "home_run",
            )
        }
        if hit_count > 0
        else None
    )

    blockers = []
    if cutoff is None:
        blockers.append(
            "invalid_as_of_date"
        )
    if expected_hand is None:
        blockers.append(
            "invalid_split"
        )
    if pitch_count < minimum_pitches:
        blockers.append(
            "insufficient_pre_cutoff_pitches"
        )
    if swing_count < minimum_swings:
        blockers.append(
            "insufficient_pre_cutoff_swings"
        )
    if ab_count < minimum_ab:
        blockers.append(
            "insufficient_pre_cutoff_ab"
        )
    if (
        len(expected_rows)
        < minimum_expected_bbe
    ):
        blockers.append(
            "insufficient_expected_bbe"
        )
    if (
        expected_coverage
        < minimum_expected_coverage
    ):
        blockers.append(
            "insufficient_expected_coverage"
        )
    if hit_count < minimum_hits:
        blockers.append(
            "insufficient_pre_cutoff_hits"
        )

    signals = {
        "called_ball_rate": (
            called_ball_count
            / pitch_count
            if pitch_count
            else None
        ),
        "whiff_rate": (
            whiff_count
            / swing_count
            if swing_count
            else None
        ),
        "expected_damage_per_ab":
            expected_damage_per_ab,
        "expected_damage_per_bbe":
            expected_damage_per_bbe,
        "actual_allocation":
            actual_allocation,
    }

    return {
        "schema_version":
            SIGNAL_ADAPTER_SCHEMA_VERSION,
        "status": (
            "blocked"
            if blockers
            else "ready"
        ),
        "shadow_only": True,
        "production_authority_changed":
            False,
        "cutoff_safe": True,
        "player_id": int(player_id),
        "season": int(season),
        "split": split,
        "as_of_date": (
            cutoff.isoformat()
            if cutoff
            else None
        ),
        "signals": signals,
        "coverage": {
            "pitch_count": pitch_count,
            "swing_count": swing_count,
            "whiff_count": whiff_count,
            "called_ball_count":
                called_ball_count,
            "ab_count": ab_count,
            "bbe_count": len(bbe),
            "expected_bbe_count":
                len(expected_rows),
            "expected_coverage":
                expected_coverage,
            "hit_count": hit_count,
        },
        "blockers": sorted(
            set(blockers)
        ),
    }


def load_hitter_profile_canary_signals(
    session: Any,
    *,
    player_id: int,
    season: int,
    split: str,
    as_of_date: Any,
) -> dict[str, Any]:
    """Load cutoff-safe Statcast rows and build signals."""

    from mlb_app.database import (
        StatcastEvent,
    )

    cutoff = _date(as_of_date)
    if cutoff is None:
        return (
            build_hitter_profile_canary_signals(
                [],
                player_id=player_id,
                season=season,
                split=split,
                as_of_date=as_of_date,
            )
        )

    hand = (
        "R"
        if split == "vsR"
        else "L"
        if split == "vsL"
        else ""
    )

    rows = (
        session.query(
            StatcastEvent.id,
            StatcastEvent.game_date,
            StatcastEvent.game_pk,
            StatcastEvent.at_bat_number,
            StatcastEvent.pitch_number,
            StatcastEvent.batter_id,
            StatcastEvent.p_throws,
            StatcastEvent.description,
            StatcastEvent.events,
            StatcastEvent.estimated_ba_using_speedangle,
            StatcastEvent.estimated_woba_using_speedangle,
        )
        .filter(
            StatcastEvent.game_date
            >= dt.date(int(season), 1, 1),
            StatcastEvent.game_date
            <= cutoff,
            StatcastEvent.batter_id
            == int(player_id),
            StatcastEvent.p_throws == hand,
        )
        .order_by(
            StatcastEvent.game_date,
            StatcastEvent.game_pk,
            StatcastEvent.at_bat_number,
            StatcastEvent.pitch_number,
            StatcastEvent.id,
        )
        .all()
    )

    fieldnames = (
        "id",
        "game_date",
        "game_pk",
        "at_bat_number",
        "pitch_number",
        "batter_id",
        "p_throws",
        "description",
        "events",
        "estimated_ba_using_speedangle",
        "estimated_woba_using_speedangle",
    )
    mappings = [
        dict(zip(fieldnames, row))
        for row in rows
    ]

    result = (
        build_hitter_profile_canary_signals(
            mappings,
            player_id=player_id,
            season=season,
            split=split,
            as_of_date=cutoff,
        )
    )
    result["storage_evidence"] = {
        "raw_row_count": len(mappings),
        "source":
            "statcast_events",
        "query_cutoff":
            cutoff.isoformat(),
    }
    return result
