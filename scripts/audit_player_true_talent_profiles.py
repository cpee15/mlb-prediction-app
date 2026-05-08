from __future__ import annotations

import csv
import datetime as dt
import json
import os
from pathlib import Path
from statistics import mean, median
from typing import Any, Dict, Iterable, List, Optional, Tuple

from mlb_app.database import (
    PlayerSplit,
    StatcastEvent,
    TeamSplit,
    create_tables,
    get_engine,
    get_session,
)
from mlb_app.db_utils import _calculate_batter_stats
from mlb_app.lineup_profile import fetch_boxscore_lineup
from mlb_app.matchup_generator import _format_team_offense_inputs, generate_matchups_for_date


METRICS = [
    "batting_avg",
    "on_base_pct",
    "slugging_pct",
    "iso",
    "k_pct",
    "bb_pct",
]

COUNT_METRICS = [
    "pa",
    "hits",
    "doubles",
    "triples",
    "home_runs",
    "walks",
    "strikeouts",
]

ALL_OUTPUT_METRICS = METRICS + COUNT_METRICS

# Candidate weights before availability/PA filtering.
# Missing or too-small windows are skipped and remaining weights are renormalized.
DEFAULT_COMPONENT_WEIGHTS = {
    "season_2023": 0.12,
    "season_2024": 0.18,
    "season_2025": 0.25,
    "season_current": 0.25,
    "last90": 0.12,
    "last30": 0.08,
}

DEFAULT_MIN_PA_BY_COMPONENT = {
    "season_2023": 50,
    "season_2024": 50,
    "season_2025": 50,
    "season_current": 25,
    "last90": 25,
    "last30": 10,
}


def _date_range(start: str, end: str) -> Iterable[str]:
    current = dt.date.fromisoformat(start)
    stop = dt.date.fromisoformat(end)
    while current <= stop:
        yield current.isoformat()
        current += dt.timedelta(days=1)


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None or isinstance(value, bool):
            return None
        return float(value)
    except Exception:
        return None


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value is None or isinstance(value, bool):
            return None
        return int(value)
    except Exception:
        return None


def _round(value: Any, ndigits: int = 4) -> Optional[float]:
    number = _safe_float(value)
    if number is None:
        return None
    return round(number, ndigits)


def _distribution(values: List[Any]) -> Dict[str, Optional[float]]:
    clean = sorted(float(v) for v in values if v is not None)
    if not clean:
        return {"min": None, "median": None, "avg": None, "max": None}
    return {
        "min": round(clean[0], 4),
        "median": round(float(median(clean)), 4),
        "avg": round(float(mean(clean)), 4),
        "max": round(clean[-1], 4),
    }


def _season_window(season: int, target_date: dt.date) -> Tuple[dt.date, dt.date]:
    start = dt.date(season, 3, 1)
    end = dt.date(season, 12, 31)
    if season == target_date.year:
        end = target_date
    return start, end


def _component_windows(target_date: dt.date) -> Dict[str, Tuple[dt.date, dt.date]]:
    current_season = target_date.year
    return {
        "season_2023": _season_window(2023, target_date),
        "season_2024": _season_window(2024, target_date),
        "season_2025": _season_window(2025, target_date),
        "season_current": _season_window(current_season, target_date),
        "last90": (target_date - dt.timedelta(days=90), target_date),
        "last30": (target_date - dt.timedelta(days=30), target_date),
    }


def _events_for_window(
    session,
    player_id: int,
    start_date: dt.date,
    end_date: dt.date,
    split: Optional[str],
) -> List[StatcastEvent]:
    query = (
        session.query(StatcastEvent)
        .filter(
            StatcastEvent.batter_id == player_id,
            StatcastEvent.game_date >= start_date,
            StatcastEvent.game_date <= end_date,
            StatcastEvent.events.isnot(None),
        )
        .order_by(
            StatcastEvent.game_date.asc(),
            StatcastEvent.game_pk.asc().nullslast(),
            StatcastEvent.at_bat_number.asc().nullslast(),
            StatcastEvent.pitch_number.asc().nullslast(),
            StatcastEvent.id.asc(),
        )
    )

    if split == "vsR":
        query = query.filter(StatcastEvent.p_throws == "R")
    elif split == "vsL":
        query = query.filter(StatcastEvent.p_throws == "L")

    return list(query.all())


def _statcast_component_profile(
    session,
    player_id: int,
    component: str,
    start_date: dt.date,
    end_date: dt.date,
    split: str,
) -> Dict[str, Any]:
    events = _events_for_window(session, player_id, start_date, end_date, split)
    stats = _calculate_batter_stats(events, raw_event_count=len(events))

    pa = _safe_int(stats.get("actual_pa"))
    batting_avg = _safe_float(stats.get("batting_avg"))
    slugging_pct = _safe_float(stats.get("slugging_pct"))

    return {
        "component": component,
        "source": "statcast_events",
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "pa": pa,
        "hits": _safe_int(stats.get("hits")),
        "doubles": None,
        "triples": None,
        "home_runs": _safe_int(stats.get("home_runs")),
        "walks": _safe_int(stats.get("walks")),
        "strikeouts": _safe_int(stats.get("strikeouts")),
        "batting_avg": batting_avg,
        "on_base_pct": _safe_float(stats.get("on_base_pct")),
        "slugging_pct": slugging_pct,
        "iso": (
            round(max(slugging_pct - batting_avg, 0.0), 4)
            if slugging_pct is not None and batting_avg is not None
            else None
        ),
        "k_pct": _safe_float(stats.get("k_pct")),
        "bb_pct": _safe_float(stats.get("bb_pct")),
    }


def _player_split_component_profile(
    session,
    player_id: int,
    season: int,
    split: str,
    component: str,
) -> Dict[str, Any]:
    row = (
        session.query(PlayerSplit)
        .filter(
            PlayerSplit.player_id == player_id,
            PlayerSplit.season == season,
            PlayerSplit.split == split,
        )
        .one_or_none()
    )

    if not row:
        return {"component": component, "source": "player_splits_missing", "pa": 0}

    batting_avg = _safe_float(row.batting_avg)
    slugging_pct = _safe_float(row.slugging_pct)
    explicit_iso = _safe_float(row.iso)
    if explicit_iso is not None and 0.0 <= explicit_iso <= 0.45:
        iso = explicit_iso
    elif batting_avg is not None and slugging_pct is not None:
        iso = max(slugging_pct - batting_avg, 0.0)
    else:
        iso = None

    return {
        "component": component,
        "source": "player_splits",
        "season": season,
        "split": split,
        "pa": _safe_int(row.pa),
        "hits": _safe_int(row.hits),
        "doubles": _safe_int(row.doubles),
        "triples": _safe_int(row.triples),
        "home_runs": _safe_int(row.home_runs),
        "walks": _safe_int(row.walks),
        "strikeouts": _safe_int(row.strikeouts),
        "batting_avg": batting_avg,
        "on_base_pct": _safe_float(row.on_base_pct),
        "slugging_pct": slugging_pct,
        "iso": _round(iso),
        "k_pct": _safe_float(row.k_pct),
        "bb_pct": _safe_float(row.bb_pct),
    }


def _component_profile(
    session,
    player_id: int,
    target_date: dt.date,
    component: str,
    split: str,
) -> Dict[str, Any]:
    if component == "season_2023":
        return _player_split_component_profile(session, player_id, 2023, split, component)
    if component == "season_2024":
        return _player_split_component_profile(session, player_id, 2024, split, component)
    if component == "season_2025":
        return _player_split_component_profile(session, player_id, 2025, split, component)
    if component == "season_current":
        return _player_split_component_profile(session, player_id, target_date.year, split, component)

    start_date, end_date = _component_windows(target_date)[component]
    return _statcast_component_profile(session, player_id, component, start_date, end_date, split)


def _renormalized_weights(
    components: List[Dict[str, Any]],
    base_weights: Dict[str, float],
    min_pa_by_component: Dict[str, int],
) -> Tuple[Dict[str, float], List[Dict[str, Any]]]:
    usable: List[Dict[str, Any]] = []

    for component in components:
        name = component["component"]
        pa = _safe_int(component.get("pa")) or 0
        min_pa = min_pa_by_component.get(name, 0)

        reason = None
        if base_weights.get(name, 0.0) <= 0:
            reason = "zero_base_weight"
        elif pa < min_pa:
            reason = "below_min_pa"
        elif all(_safe_float(component.get(metric)) is None for metric in METRICS):
            reason = "no_rate_metrics"

        component["min_pa"] = min_pa
        component["is_usable"] = reason is None
        component["skip_reason"] = reason

        if reason is None:
            usable.append(component)

    weight_total = sum(base_weights.get(c["component"], 0.0) for c in usable)
    if weight_total <= 0:
        return {}, components

    weights = {
        c["component"]: round(base_weights.get(c["component"], 0.0) / weight_total, 6)
        for c in usable
    }
    return weights, components


def _weighted_rate(
    components: List[Dict[str, Any]],
    weights: Dict[str, float],
    metric: str,
) -> Optional[float]:
    numerator = 0.0
    denominator = 0.0

    for component in components:
        name = component["component"]
        weight = weights.get(name)
        value = _safe_float(component.get(metric))
        if weight is None or value is None:
            continue
        numerator += value * weight
        denominator += weight

    if denominator <= 0:
        return None
    return round(numerator / denominator, 4)


def _weighted_count_proxy(
    components: List[Dict[str, Any]],
    weights: Dict[str, float],
    metric: str,
) -> Optional[int]:
    value = _weighted_rate(components, weights, metric)
    if value is None:
        return None
    return int(round(value))


def _build_player_true_talent_profile(
    session,
    player_id: int,
    player_name: Optional[str],
    target_date: dt.date,
    split: str,
    base_weights: Dict[str, float],
    min_pa_by_component: Dict[str, int],
) -> Dict[str, Any]:
    components = [
        _component_profile(session, player_id, target_date, component, split)
        for component in base_weights.keys()
    ]
    weights, components = _renormalized_weights(components, base_weights, min_pa_by_component)

    profile: Dict[str, Any] = {
        "player_id": player_id,
        "player_name": player_name,
        "split": split,
        "source": "audit_player_true_talent",
        "usable_component_count": len(weights),
        "used_components": sorted(weights.keys()),
        "component_weights": weights,
        "components": components,
        "has_usable_true_talent_profile": bool(weights),
    }

    for metric in METRICS:
        profile[metric] = _weighted_rate(components, weights, metric)

    for metric in COUNT_METRICS:
        profile[metric] = _weighted_count_proxy(components, weights, metric)

    profile["pa"] = sum(
        _safe_int(c.get("pa")) or 0
        for c in components
        if c.get("component") in weights
    )

    return profile


def _team_split_row(session, team_id: int, season: int, split: str) -> Optional[TeamSplit]:
    return (
        session.query(TeamSplit)
        .filter(
            TeamSplit.team_id == team_id,
            TeamSplit.season == season,
            TeamSplit.split == split,
        )
        .one_or_none()
    )


def _team_inputs(session, team_id: int, season: int, split: str) -> Dict[str, Any]:
    return _format_team_offense_inputs(session, team_id, season, split)


def _avg_metric(profiles: List[Dict[str, Any]], metric: str) -> Optional[float]:
    values = [_safe_float(p.get(metric)) for p in profiles if _safe_float(p.get(metric)) is not None]
    if not values:
        return None
    return round(sum(values) / len(values), 4)


def _sum_metric(profiles: List[Dict[str, Any]], metric: str) -> Optional[int]:
    values = [_safe_int(p.get(metric)) for p in profiles if _safe_int(p.get(metric)) is not None]
    if not values:
        return None
    return int(sum(values))


def _aggregate_lineup_profiles(
    profiles: List[Dict[str, Any]],
    lineup_count: int,
    min_usable_hitters: int,
) -> Dict[str, Any]:
    usable = [p for p in profiles if p.get("has_usable_true_talent_profile")]

    out: Dict[str, Any] = {
        "source": "audit_confirmed_lineup_true_talent",
        "starting_lineup_count": lineup_count,
        "usable_hitter_profile_count": len(usable),
        "min_usable_hitters": min_usable_hitters,
        "would_activate": len(usable) >= min_usable_hitters,
    }

    for metric in METRICS:
        out[metric] = _avg_metric(usable, metric)

    for metric in COUNT_METRICS:
        out[metric] = _sum_metric(usable, metric)

    return out


def _metric_deltas(team_inputs: Dict[str, Any], lineup_inputs: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for metric in ALL_OUTPUT_METRICS:
        team_value = _safe_float(team_inputs.get(metric))
        lineup_value = _safe_float(lineup_inputs.get(metric))
        delta = None
        if team_value is not None and lineup_value is not None:
            delta = round(lineup_value - team_value, 4)

        out[f"{metric}_team"] = team_value
        out[f"{metric}_true_talent_lineup"] = lineup_value
        out[f"{metric}_delta"] = delta

    return out


def _is_extreme_delta(row: Dict[str, Any]) -> bool:
    return (
        abs(_safe_float(row.get("iso_delta")) or 0.0) > 0.030
        or abs(_safe_float(row.get("slugging_pct_delta")) or 0.0) > 0.050
        or abs(_safe_float(row.get("k_pct_delta")) or 0.0) > 0.050
        or abs(_safe_float(row.get("bb_pct_delta")) or 0.0) > 0.030
    )


def _side_context(matchup: Dict[str, Any], side: str) -> Dict[str, Any]:
    offense = matchup.get(f"{side}_offense_inputs") or {}
    return {
        "game_pk": matchup.get("game_pk"),
        "team_id": matchup.get(f"{side}_team_id"),
        "team_name": matchup.get(f"{side}_team_name"),
        "split": offense.get("split") or "vsR",
    }


def _audit_side(
    session,
    date_str: str,
    matchup: Dict[str, Any],
    side: str,
    base_weights: Dict[str, float],
    min_pa_by_component: Dict[str, int],
    min_usable_hitters: int,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    target_date = dt.date.fromisoformat(date_str)
    season = target_date.year
    ctx = _side_context(matchup, side)
    game_pk = _safe_int(ctx.get("game_pk"))
    team_id = _safe_int(ctx.get("team_id"))
    split = ctx.get("split") or "vsR"

    if not game_pk or not team_id:
        row = {
            "date": date_str,
            "game_pk": game_pk,
            "side": side,
            "team_id": team_id,
            "team_name": ctx.get("team_name"),
            "split": split,
            "skip_reason": "missing_game_or_team_id",
        }
        return row, []

    lineups = fetch_boxscore_lineup(game_pk)
    lineup = lineups.get(side) or []
    team_inputs = _team_inputs(session, team_id, season, split)

    player_profiles: List[Dict[str, Any]] = []
    hitter_rows: List[Dict[str, Any]] = []

    for player in lineup:
        player_id = _safe_int(player.get("batter_id"))
        player_name = player.get("name")

        if not player_id:
            continue

        profile = _build_player_true_talent_profile(
            session=session,
            player_id=player_id,
            player_name=player_name,
            target_date=target_date,
            split=split,
            base_weights=base_weights,
            min_pa_by_component=min_pa_by_component,
        )
        player_profiles.append(profile)

        component_pa = {
            f"{component['component']}_pa": _safe_int(component.get("pa")) or 0
            for component in profile.get("components", [])
        }
        component_used = {
            f"{component['component']}_used": bool(component.get("is_usable"))
            for component in profile.get("components", [])
        }

        hitter_rows.append({
            "date": date_str,
            "game_pk": game_pk,
            "side": side,
            "team_id": team_id,
            "team_name": ctx.get("team_name"),
            "split": split,
            "player_id": player_id,
            "player_name": player_name,
            "lineup_slot": player.get("lineup_slot"),
            "usable_component_count": profile.get("usable_component_count"),
            "used_components": ",".join(profile.get("used_components") or []),
            "has_usable_true_talent_profile": profile.get("has_usable_true_talent_profile"),
            "pa": profile.get("pa"),
            "batting_avg": profile.get("batting_avg"),
            "on_base_pct": profile.get("on_base_pct"),
            "slugging_pct": profile.get("slugging_pct"),
            "iso": profile.get("iso"),
            "k_pct": profile.get("k_pct"),
            "bb_pct": profile.get("bb_pct"),
            **component_pa,
            **component_used,
        })

    lineup_inputs = _aggregate_lineup_profiles(
        player_profiles,
        lineup_count=len(lineup),
        min_usable_hitters=min_usable_hitters,
    )

    row = {
        "date": date_str,
        "game_pk": game_pk,
        "side": side,
        "team_id": team_id,
        "team_name": ctx.get("team_name"),
        "split": split,
        "starting_lineup_count": len(lineup),
        "usable_true_talent_hitter_count": lineup_inputs.get("usable_hitter_profile_count"),
        "min_usable_hitters": min_usable_hitters,
        "would_activate": lineup_inputs.get("would_activate"),
        "team_source": team_inputs.get("source"),
        "true_talent_source": lineup_inputs.get("source"),
        **_metric_deltas(team_inputs, lineup_inputs),
    }
    row["extreme_delta"] = _is_extreme_delta(row)

    return row, hitter_rows


def _summarize(rows: List[Dict[str, Any]], hitter_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    component_names = list(DEFAULT_COMPONENT_WEIGHTS.keys())

    component_availability: Dict[str, Dict[str, Any]] = {}
    for component in component_names:
        pa_key = f"{component}_pa"
        used_key = f"{component}_used"
        component_availability[component] = {
            "hitters_with_any_pa": sum(1 for row in hitter_rows if (_safe_int(row.get(pa_key)) or 0) > 0),
            "hitters_used": sum(1 for row in hitter_rows if bool(row.get(used_key))),
            "pa_distribution": _distribution([row.get(pa_key) for row in hitter_rows]),
        }

    return {
        "team_sides": len(rows),
        "lineup_sides_would_activate": sum(1 for row in rows if row.get("would_activate")),
        "extreme_delta_sides": sum(1 for row in rows if row.get("extreme_delta")),
        "usable_true_talent_hitter_count_distribution": _distribution(
            [row.get("usable_true_talent_hitter_count") for row in rows]
        ),
        "delta_distributions": {
            metric: _distribution([row.get(f"{metric}_delta") for row in rows])
            for metric in METRICS
        },
        "component_availability": component_availability,
        "hitters": {
            "rows": len(hitter_rows),
            "with_usable_true_talent_profile": sum(
                1 for row in hitter_rows if row.get("has_usable_true_talent_profile")
            ),
            "usable_component_count_distribution": _distribution(
                [row.get("usable_component_count") for row in hitter_rows]
            ),
        },
        "recommendation": (
            "Audit only. Do not patch production lineup offense yet. "
            "Use this report to compare true-talent deltas against prior raw lineup/team deltas."
        ),
    }


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        path.write_text("")
        return

    fieldnames = sorted({key for row in rows for key in row.keys()})
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    start = os.getenv("BACKTEST_START", "2026-04-20")
    end = os.getenv("BACKTEST_END", "2026-05-03")
    database_url = os.getenv("DATABASE_URL", "sqlite:///mlb.db")
    min_usable_hitters = int(os.getenv("MIN_USABLE_HITTERS", "7"))

    base_weights = dict(DEFAULT_COMPONENT_WEIGHTS)
    min_pa_by_component = dict(DEFAULT_MIN_PA_BY_COMPONENT)

    engine = get_engine(database_url)
    create_tables(engine)
    SessionLocal = get_session(engine)

    side_rows: List[Dict[str, Any]] = []
    hitter_rows: List[Dict[str, Any]] = []

    with SessionLocal() as session:
        for date_str in _date_range(start, end):
            matchups = generate_matchups_for_date(session, date_str)
            for matchup in matchups:
                for side in ("away", "home"):
                    side_row, side_hitter_rows = _audit_side(
                        session=session,
                        date_str=date_str,
                        matchup=matchup,
                        side=side,
                        base_weights=base_weights,
                        min_pa_by_component=min_pa_by_component,
                        min_usable_hitters=min_usable_hitters,
                    )
                    side_rows.append(side_row)
                    hitter_rows.extend(side_hitter_rows)

    summary = _summarize(side_rows, hitter_rows)

    output_dir = Path("tmp")
    output_dir.mkdir(parents=True, exist_ok=True)

    prefix = f"player_true_talent_profile_audit_{start}_to_{end}"
    json_path = output_dir / f"{prefix}.json"
    side_csv_path = output_dir / f"{prefix}_team_sides.csv"
    hitter_csv_path = output_dir / f"{prefix}_hitters.csv"

    payload = {
        "start": start,
        "end": end,
        "database_url": database_url,
        "component_weights": base_weights,
        "min_pa_by_component": min_pa_by_component,
        "summary": summary,
        "rows": side_rows,
    }

    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True))
    _write_csv(side_csv_path, side_rows)
    _write_csv(hitter_csv_path, hitter_rows)

    print(json.dumps(summary, indent=2, sort_keys=True))
    print(f"Wrote {json_path}")
    print(f"Wrote {side_csv_path}")
    print(f"Wrote {hitter_csv_path}")


if __name__ == "__main__":
    main()
