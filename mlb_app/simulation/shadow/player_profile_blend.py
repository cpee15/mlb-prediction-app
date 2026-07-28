"""Diagnostic-only hitter profile blending for shadow evaluation."""

from __future__ import annotations

import datetime as dt
from typing import Any, Dict, Iterable, Mapping, Optional


SHADOW_PLAYER_PROFILE_BLEND_VERSION = "shadow_player_profile_blend_v1"

WINDOW_PRIORS = {
    "current_season": 0.50,
    "prior_season": 0.30,
    "career_pre_prior": 0.20,
}
RELIABILITY_PA = 200.0
AGGREGATE_FRESHNESS_DAYS = 14


def _number(value: Any) -> Optional[float]:
    try:
        if value is None or isinstance(value, bool):
            return None
        return float(value)
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


def _combine_rows(rows: Iterable[Mapping[str, Any]]) -> Dict[str, Any]:
    rows = list(rows)
    count_fields = (
        "pa",
        "hits",
        "doubles",
        "triples",
        "home_runs",
        "walks",
        "strikeouts",
    )
    totals = {
        key: sum(_number(row.get(key)) or 0.0 for row in rows)
        for key in count_fields
    }
    pa = totals["pa"]

    def rate(field: str, numerator: str) -> Optional[float]:
        weighted_numerator = totals[numerator]
        if pa > 0:
            return weighted_numerator / pa
        values = [
            (_number(row.get(field)), _number(row.get("pa")))
            for row in rows
        ]
        values = [(value, weight) for value, weight in values if value is not None]
        denominator = sum(weight or 1.0 for _, weight in values)
        if not values or denominator <= 0:
            return None
        return sum(value * (weight or 1.0) for value, weight in values) / denominator

    batting_values = [
        (_number(row.get("batting_avg")), _number(row.get("pa")))
        for row in rows
        if _number(row.get("batting_avg")) is not None
    ]
    slugging_values = [
        (_number(row.get("slugging_pct")), _number(row.get("pa")))
        for row in rows
        if _number(row.get("slugging_pct")) is not None
    ]

    def weighted(values):
        denominator = sum(weight or 1.0 for _, weight in values)
        if not values or denominator <= 0:
            return None
        return sum(value * (weight or 1.0) for value, weight in values) / denominator

    return {
        **{key: int(value) for key, value in totals.items()},
        "k_pct": rate("k_pct", "strikeouts"),
        "bb_pct": rate("bb_pct", "walks"),
        "batting_avg": weighted(batting_values),
        "slugging_pct": weighted(slugging_values),
    }


def build_shadow_hitter_profile_blend(
    *,
    player_id: int,
    season: int,
    split: str,
    player_splits: Iterable[Mapping[str, Any]],
    batter_aggregate: Optional[Mapping[str, Any]] = None,
    as_of_date: Optional[dt.date] = None,
) -> Dict[str, Any]:
    """Blend disjoint season windows without changing production inputs."""

    as_of_date = as_of_date or dt.date.today()
    matching = [
        dict(row)
        for row in player_splits
        if int(row.get("player_id") or player_id) == int(player_id)
        and row.get("split") == split
    ]
    grouped = {
        "current_season": [
            row for row in matching if int(row.get("season") or 0) == season
        ],
        "prior_season": [
            row for row in matching if int(row.get("season") or 0) == season - 1
        ],
        "career_pre_prior": [
            row for row in matching if int(row.get("season") or 0) <= season - 2
        ],
    }

    windows: Dict[str, Dict[str, Any]] = {}
    raw_weights: Dict[str, float] = {}
    for name, rows in grouped.items():
        if not rows:
            continue
        combined = _combine_rows(rows)
        pa = float(combined.get("pa") or 0)
        reliability = min(1.0, pa / RELIABILITY_PA)
        raw_weight = WINDOW_PRIORS[name] * reliability
        windows[name] = {
            "seasons": sorted({int(row["season"]) for row in rows}),
            "pa": int(pa),
            "reliability": round(reliability, 6),
            "raw_weight": round(raw_weight, 6),
            "metrics": combined,
        }
        raw_weights[name] = raw_weight

    denominator = sum(raw_weights.values())
    normalized_weights = {
        name: weight / denominator
        for name, weight in raw_weights.items()
        if denominator > 0
    }
    for name, weight in normalized_weights.items():
        windows[name]["normalized_weight"] = round(weight, 6)

    metric_fields = ("k_pct", "bb_pct", "batting_avg", "slugging_pct")
    blended: Dict[str, Optional[float]] = {}
    for field in metric_fields:
        values = [
            (windows[name]["metrics"].get(field), weight)
            for name, weight in normalized_weights.items()
            if windows[name]["metrics"].get(field) is not None
        ]
        field_weight = sum(weight for _, weight in values)
        blended[field] = (
            round(sum(float(value) * weight for value, weight in values) / field_weight, 6)
            if field_weight > 0
            else None
        )
    if blended["batting_avg"] is not None and blended["slugging_pct"] is not None:
        blended["iso"] = round(
            blended["slugging_pct"] - blended["batting_avg"],
            6,
        )
    else:
        blended["iso"] = None

    blockers = []
    warnings = ["player_split_freshness_unverifiable"]
    if "current_season" not in windows:
        blockers.append("missing_current_season_split")
    if len(windows) < 2:
        blockers.append("insufficient_disjoint_windows")
    if denominator <= 0:
        blockers.append("no_reliable_sample")

    aggregate = dict(batter_aggregate or {})
    aggregate_end = _date(aggregate.get("end_date"))
    aggregate_age = (
        (as_of_date - aggregate_end).days
        if aggregate_end is not None
        else None
    )
    if not aggregate:
        warnings.append("missing_batter_contact_aggregate")
    elif aggregate_age is None:
        warnings.append("batter_contact_aggregate_freshness_unknown")
    elif aggregate_age > AGGREGATE_FRESHNESS_DAYS:
        warnings.append("stale_batter_contact_aggregate")

    expected_fields = {
        "xwoba": _number(aggregate.get("xwoba")),
        "xba": _number(aggregate.get("xba")),
    }
    expected_available = any(value is not None for value in expected_fields.values())
    if not expected_available:
        warnings.append("unsupported_expected_components")

    return {
        "schema_version": SHADOW_PLAYER_PROFILE_BLEND_VERSION,
        "status": "blocked" if blockers else "ready",
        "shadow_only": True,
        "production_authority_changed": False,
        "player_id": int(player_id),
        "season": int(season),
        "split": split,
        "as_of_date": as_of_date.isoformat(),
        "window_policy": "disjoint_current_prior_career_pre_prior",
        "windows": windows,
        "blended_actual_metrics": blended,
        "contact_quality_context": {
            "end_date": aggregate_end.isoformat() if aggregate_end else None,
            "age_days": aggregate_age,
            "avg_exit_velocity": _number(aggregate.get("avg_exit_velocity")),
            "avg_launch_angle": _number(aggregate.get("avg_launch_angle")),
            "hard_hit_pct": _number(aggregate.get("hard_hit_pct")),
            "barrel_pct": _number(aggregate.get("barrel_pct")),
        },
        "expected_component_adjustment": {
            "status": "available" if expected_available else "unsupported_source_schema",
            "applied": False,
            "fields": expected_fields,
            "reason": (
                "shadow_v1_reports_expected evidence but does not alter probabilities"
                if expected_available
                else "batter_aggregates does not store batter xwOBA or xBA"
            ),
        },
        "blockers": blockers,
        "warnings": sorted(set(warnings)),
    }


def load_shadow_hitter_profile_blend(
    session,
    *,
    player_id: int,
    season: int,
    split: str,
    as_of_date: Optional[dt.date] = None,
) -> Dict[str, Any]:
    """Load persisted evidence and build the diagnostic shadow blend."""

    from mlb_app.database import BatterAggregate, PlayerSplit

    as_of_date = as_of_date or dt.date.today()
    split_rows = (
        session.query(PlayerSplit)
        .filter(
            PlayerSplit.player_id == int(player_id),
            PlayerSplit.season <= int(season),
            PlayerSplit.split == split,
        )
        .order_by(PlayerSplit.season.desc())
        .all()
    )
    split_fields = (
        "player_id",
        "season",
        "split",
        "pa",
        "hits",
        "doubles",
        "triples",
        "home_runs",
        "walks",
        "strikeouts",
        "batting_avg",
        "on_base_pct",
        "slugging_pct",
        "iso",
        "k_pct",
        "bb_pct",
    )
    player_splits = [
        {field: getattr(row, field, None) for field in split_fields}
        for row in split_rows
    ]

    aggregate_row = (
        session.query(BatterAggregate)
        .filter(
            BatterAggregate.batter_id == int(player_id),
            BatterAggregate.end_date <= as_of_date,
        )
        .order_by(BatterAggregate.end_date.desc())
        .first()
    )
    aggregate_fields = (
        "batter_id",
        "window",
        "end_date",
        "avg_exit_velocity",
        "avg_launch_angle",
        "hard_hit_pct",
        "barrel_pct",
        "k_pct",
        "bb_pct",
        "batting_avg",
        "xwoba",
        "xba",
    )
    batter_aggregate = (
        {
            field: getattr(aggregate_row, field, None)
            for field in aggregate_fields
        }
        if aggregate_row is not None
        else None
    )

    result = build_shadow_hitter_profile_blend(
        player_id=player_id,
        season=season,
        split=split,
        player_splits=player_splits,
        batter_aggregate=batter_aggregate,
        as_of_date=as_of_date,
    )
    result["storage_evidence"] = {
        "player_split_row_count": len(player_splits),
        "player_split_seasons": sorted(
            {int(row["season"]) for row in player_splits}
        ),
        "batter_aggregate_found": aggregate_row is not None,
        "batter_aggregate_window": (
            getattr(aggregate_row, "window", None)
            if aggregate_row is not None
            else None
        ),
    }
    return result


def build_shadow_candidate_batter_profile(
    blend: Mapping[str, Any],
) -> Dict[str, Any]:
    """Translate ready blended evidence into PA-model input semantics."""

    if blend.get("status") != "ready":
        return {
            "status": "blocked",
            "reason": "shadow_profile_blend_not_ready",
            "profile": None,
        }

    actual = dict(blend.get("blended_actual_metrics") or {})
    contact = dict(blend.get("contact_quality_context") or {})
    warnings = set(blend.get("warnings") or ())
    contact_is_fresh = (
        "stale_batter_contact_aggregate" not in warnings
        and "missing_batter_contact_aggregate" not in warnings
        and "batter_contact_aggregate_freshness_unknown" not in warnings
    )
    profile = {
        "contact_skill": {
            "k_rate": _number(actual.get("k_pct")),
            "contact_rate": None,
        },
        "plate_discipline": {
            "bb_rate": _number(actual.get("bb_pct")),
        },
        "power": {
            "iso": _number(actual.get("iso")),
            "barrel_rate": (
                _number(contact.get("barrel_pct"))
                if contact_is_fresh
                else None
            ),
            "hard_hit_rate": (
                _number(contact.get("hard_hit_pct"))
                if contact_is_fresh
                else None
            ),
        },
        "metadata": {
            "source_type": "shadow_player_profile_blend",
            "profile_granularity": "individual_hitter",
            "player_id": blend.get("player_id"),
            "sample_window": blend.get("window_policy"),
            "sample_size": sum(
                int(window.get("pa") or 0)
                for window in (blend.get("windows") or {}).values()
            ),
            "shadow_only": True,
            "production_authority_changed": False,
            "blend_schema_version": blend.get("schema_version"),
        },
    }
    return {
        "status": "ready",
        "reason": None,
        "profile": profile,
    }


def compare_shadow_hitter_pa_outcomes(
    *,
    blend: Mapping[str, Any],
    production_batter_profile: Optional[Mapping[str, Any]],
    pitcher_profile: Optional[Mapping[str, Any]],
    environment_profile: Optional[Mapping[str, Any]],
) -> Dict[str, Any]:
    """Compare paired PA distributions without changing production authority."""

    from mlb_app.simulation.pa_outcome_model import (
        build_pa_outcome_probabilities,
    )

    candidate = build_shadow_candidate_batter_profile(blend)
    if candidate["status"] != "ready":
        return {
            "schema_version": "shadow_hitter_pa_comparison_v1",
            "status": "blocked",
            "shadow_only": True,
            "production_authority_changed": False,
            "reason": candidate["reason"],
        }

    production = build_pa_outcome_probabilities(
        dict(production_batter_profile or {}),
        dict(pitcher_profile or {}),
        dict(environment_profile or {}),
    )
    shadow = build_pa_outcome_probabilities(
        candidate["profile"],
        dict(pitcher_profile or {}),
        dict(environment_profile or {}),
    )
    production_probabilities = dict(production.get("probabilities") or {})
    shadow_probabilities = dict(shadow.get("probabilities") or {})
    keys = sorted(set(production_probabilities) | set(shadow_probabilities))
    deltas = {
        key: round(
            float(shadow_probabilities.get(key) or 0.0)
            - float(production_probabilities.get(key) or 0.0),
            6,
        )
        for key in keys
    }
    absolute_deltas = {
        key: round(abs(value), 6)
        for key, value in deltas.items()
    }

    return {
        "schema_version": "shadow_hitter_pa_comparison_v1",
        "status": "ready",
        "shadow_only": True,
        "production_authority_changed": False,
        "production_model_version": production.get("model_version"),
        "shadow_model_version": shadow.get("model_version"),
        "production_probabilities": production_probabilities,
        "shadow_probabilities": shadow_probabilities,
        "probability_deltas": deltas,
        "absolute_probability_deltas": absolute_deltas,
        "maximum_absolute_probability_delta": (
            max(absolute_deltas.values())
            if absolute_deltas
            else 0.0
        ),
        "candidate_profile": candidate["profile"],
        "blend_status": blend.get("status"),
        "blend_warnings": list(blend.get("warnings") or ()),
    }
