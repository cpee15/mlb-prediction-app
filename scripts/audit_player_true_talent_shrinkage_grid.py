from __future__ import annotations

import csv
import datetime as dt
import json
import os
from pathlib import Path
from statistics import mean, median
from typing import Any, Dict, Iterable, List, Optional, Tuple

from mlb_app.database import create_tables, get_engine, get_session
from mlb_app.lineup_profile import fetch_boxscore_lineup
from mlb_app.matchup_generator import generate_matchups_for_date

from scripts.audit_player_true_talent_profiles import (
    COUNT_METRICS,
    DEFAULT_COMPONENT_WEIGHTS,
    DEFAULT_MIN_PA_BY_COMPONENT,
    METRICS,
    _aggregate_lineup_profiles,
    _build_player_true_talent_profile,
    _date_range,
    _metric_deltas,
    _safe_float,
    _safe_int,
    _side_context,
    _team_inputs,
)


BLEND_METRICS = list(METRICS) + list(COUNT_METRICS)

DEFAULT_BLEND_WEIGHTS = [0.0, 0.10, 0.25, 0.40, 0.50, 0.65, 0.80, 1.0]
DEFAULT_SHRINKAGE_PA_GRID = [150, 300, 600, 1000, 1500]


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


def _parse_float_list_env(name: str, default: List[float]) -> List[float]:
    raw = os.getenv(name)
    if not raw:
        return list(default)

    values: List[float] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        values.append(float(part))
    return values


def _parse_int_list_env(name: str, default: List[int]) -> List[int]:
    raw = os.getenv(name)
    if not raw:
        return list(default)

    values: List[int] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        values.append(int(part))
    return values


def _blend_value(team_value: Any, true_talent_value: Any, weight: float) -> Any:
    team_float = _safe_float(team_value)
    true_talent_float = _safe_float(true_talent_value)

    if team_float is None and true_talent_float is None:
        return None
    if team_float is None:
        return true_talent_float
    if true_talent_float is None:
        return team_float

    value = team_float + weight * (true_talent_float - team_float)

    if isinstance(team_value, int) and isinstance(true_talent_value, int):
        return int(round(value))
    return round(value, 4)


def _apply_fixed_blend(
    team_inputs: Dict[str, Any],
    true_talent_inputs: Dict[str, Any],
    weight: float,
) -> Dict[str, Any]:
    blended = dict(true_talent_inputs or {})
    blended["source"] = f"audit_true_talent_fixed_blend_{weight:.2f}"
    blended["shrinkage_mode"] = "fixed_blend"
    blended["blend_weight"] = weight

    for metric in BLEND_METRICS:
        blended[metric] = _blend_value(
            (team_inputs or {}).get(metric),
            (true_talent_inputs or {}).get(metric),
            weight,
        )

    return blended


def _lineup_reliability(lineup_inputs: Dict[str, Any], shrinkage_pa: int) -> float:
    pa = _safe_float((lineup_inputs or {}).get("pa")) or 0.0
    denominator = pa + float(shrinkage_pa)
    if denominator <= 0:
        return 0.0
    return round(pa / denominator, 6)


def _apply_pa_shrinkage(
    team_inputs: Dict[str, Any],
    true_talent_inputs: Dict[str, Any],
    shrinkage_pa: int,
) -> Dict[str, Any]:
    reliability = _lineup_reliability(true_talent_inputs, shrinkage_pa)
    blended = _apply_fixed_blend(team_inputs, true_talent_inputs, reliability)
    blended["source"] = f"audit_true_talent_pa_shrinkage_{shrinkage_pa}"
    blended["shrinkage_mode"] = "pa_shrinkage"
    blended["shrinkage_pa"] = shrinkage_pa
    blended["reliability"] = reliability
    return blended


def _is_extreme_delta(row: Dict[str, Any]) -> bool:
    return (
        abs(_safe_float(row.get("iso_delta")) or 0.0) > 0.030
        or abs(_safe_float(row.get("slugging_pct_delta")) or 0.0) > 0.050
        or abs(_safe_float(row.get("k_pct_delta")) or 0.0) > 0.050
        or abs(_safe_float(row.get("bb_pct_delta")) or 0.0) > 0.030
    )


def _abs_adjustments(row: Dict[str, Any]) -> Dict[str, Optional[float]]:
    out: Dict[str, Optional[float]] = {}
    for metric in METRICS:
        delta = _safe_float(row.get(f"{metric}_delta"))
        out[f"{metric}_abs_adjustment"] = round(abs(delta), 4) if delta is not None else None
    return out


def _audit_side_base(
    session,
    date_str: str,
    matchup: Dict[str, Any],
    side: str,
    min_usable_hitters: int,
) -> Optional[Dict[str, Any]]:
    target_date = dt.date.fromisoformat(date_str)
    season = target_date.year
    ctx = _side_context(matchup, side)

    game_pk = _safe_int(ctx.get("game_pk"))
    team_id = _safe_int(ctx.get("team_id"))
    split = ctx.get("split") or "vsR"

    if not game_pk or not team_id:
        return None

    lineups = fetch_boxscore_lineup(game_pk)
    lineup = lineups.get(side) or []
    team_inputs = _team_inputs(session, team_id, season, split)

    player_profiles: List[Dict[str, Any]] = []
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
            base_weights=dict(DEFAULT_COMPONENT_WEIGHTS),
            min_pa_by_component=dict(DEFAULT_MIN_PA_BY_COMPONENT),
        )
        player_profiles.append(profile)

    true_talent_inputs = _aggregate_lineup_profiles(
        player_profiles,
        lineup_count=len(lineup),
        min_usable_hitters=min_usable_hitters,
    )

    raw_row = {
        "date": date_str,
        "game_pk": game_pk,
        "side": side,
        "team_id": team_id,
        "team_name": ctx.get("team_name"),
        "split": split,
        "starting_lineup_count": len(lineup),
        "usable_true_talent_hitter_count": true_talent_inputs.get("usable_hitter_profile_count"),
        "min_usable_hitters": min_usable_hitters,
        "would_activate": true_talent_inputs.get("would_activate"),
        "team_source": team_inputs.get("source"),
        "true_talent_source": true_talent_inputs.get("source"),
        "true_talent_pa": true_talent_inputs.get("pa"),
        **_metric_deltas(team_inputs, true_talent_inputs),
    }
    raw_row["extreme_delta"] = _is_extreme_delta(raw_row)

    return {
        "context": raw_row,
        "team_inputs": team_inputs,
        "true_talent_inputs": true_talent_inputs,
    }


def _setting_row(
    base: Dict[str, Any],
    setting_name: str,
    shrinkage_mode: str,
    shrunk_inputs: Dict[str, Any],
) -> Dict[str, Any]:
    context = base["context"]
    team_inputs = base["team_inputs"]

    row = {
        "setting": setting_name,
        "shrinkage_mode": shrinkage_mode,
        "date": context.get("date"),
        "game_pk": context.get("game_pk"),
        "side": context.get("side"),
        "team_id": context.get("team_id"),
        "team_name": context.get("team_name"),
        "split": context.get("split"),
        "starting_lineup_count": context.get("starting_lineup_count"),
        "usable_true_talent_hitter_count": context.get("usable_true_talent_hitter_count"),
        "min_usable_hitters": context.get("min_usable_hitters"),
        "would_activate": context.get("would_activate"),
        "true_talent_pa": context.get("true_talent_pa"),
        "blend_weight": shrunk_inputs.get("blend_weight"),
        "shrinkage_pa": shrunk_inputs.get("shrinkage_pa"),
        "reliability": shrunk_inputs.get("reliability"),
        **_metric_deltas(team_inputs, shrunk_inputs),
    }
    row["extreme_delta"] = _is_extreme_delta(row)
    row.update(_abs_adjustments(row))
    return row


def _summarize_setting(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    team_sides = len(rows)
    extreme_delta_sides = sum(1 for row in rows if row.get("extreme_delta"))

    summary: Dict[str, Any] = {
        "team_sides": team_sides,
        "lineup_sides_would_activate": sum(1 for row in rows if row.get("would_activate")),
        "extreme_delta_sides": extreme_delta_sides,
        "extreme_delta_rate": round(extreme_delta_sides / team_sides, 4) if team_sides else None,
        "usable_true_talent_hitter_count_distribution": _distribution(
            [row.get("usable_true_talent_hitter_count") for row in rows]
        ),
        "true_talent_pa_distribution": _distribution([row.get("true_talent_pa") for row in rows]),
        "delta_distributions": {
            metric: _distribution([row.get(f"{metric}_delta") for row in rows])
            for metric in METRICS
        },
        "avg_abs_adjustment": {},
        "max_abs_adjustment": {},
    }

    for metric in METRICS:
        key = f"{metric}_abs_adjustment"
        values = [_safe_float(row.get(key)) for row in rows if _safe_float(row.get(key)) is not None]
        summary["avg_abs_adjustment"][metric] = round(mean(values), 4) if values else None
        summary["max_abs_adjustment"][metric] = round(max(values), 4) if values else None

    blend_weights = sorted({_safe_float(row.get("blend_weight")) for row in rows if row.get("blend_weight") is not None})
    shrinkage_pas = sorted({_safe_int(row.get("shrinkage_pa")) for row in rows if row.get("shrinkage_pa") is not None})
    reliabilities = [row.get("reliability") for row in rows if row.get("reliability") is not None]

    if blend_weights:
        summary["blend_weights"] = blend_weights
    if shrinkage_pas:
        summary["shrinkage_pas"] = shrinkage_pas
    if reliabilities:
        summary["reliability_distribution"] = _distribution(reliabilities)

    return summary


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

    blend_weights = _parse_float_list_env("TRUE_TALENT_BLEND_WEIGHTS", DEFAULT_BLEND_WEIGHTS)
    shrinkage_pa_grid = _parse_int_list_env("SHRINKAGE_PA_GRID", DEFAULT_SHRINKAGE_PA_GRID)

    engine = get_engine(database_url)
    create_tables(engine)
    SessionLocal = get_session(engine)

    base_rows: List[Dict[str, Any]] = []
    grid_rows: List[Dict[str, Any]] = []

    with SessionLocal() as session:
        for date_str in _date_range(start, end):
            matchups = generate_matchups_for_date(session, date_str)
            for matchup in matchups:
                for side in ("away", "home"):
                    base = _audit_side_base(
                        session=session,
                        date_str=date_str,
                        matchup=matchup,
                        side=side,
                        min_usable_hitters=min_usable_hitters,
                    )
                    if base is None:
                        continue

                    base_rows.append(base["context"])

                    for weight in blend_weights:
                        shrunk = _apply_fixed_blend(base["team_inputs"], base["true_talent_inputs"], weight)
                        grid_rows.append(
                            _setting_row(
                                base=base,
                                setting_name=f"fixed_blend_{weight:.2f}",
                                shrinkage_mode="fixed_blend",
                                shrunk_inputs=shrunk,
                            )
                        )

                    for shrinkage_pa in shrinkage_pa_grid:
                        shrunk = _apply_pa_shrinkage(
                            base["team_inputs"],
                            base["true_talent_inputs"],
                            shrinkage_pa,
                        )
                        grid_rows.append(
                            _setting_row(
                                base=base,
                                setting_name=f"pa_shrinkage_{shrinkage_pa}",
                                shrinkage_mode="pa_shrinkage",
                                shrunk_inputs=shrunk,
                            )
                        )

    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in grid_rows:
        grouped.setdefault(str(row["setting"]), []).append(row)

    setting_summaries = {
        setting: _summarize_setting(rows)
        for setting, rows in sorted(grouped.items())
    }

    ranked_settings = sorted(
        [
            {
                "setting": setting,
                "shrinkage_mode": rows[0].get("shrinkage_mode") if rows else None,
                "extreme_delta_sides": summary.get("extreme_delta_sides"),
                "extreme_delta_rate": summary.get("extreme_delta_rate"),
                "avg_abs_iso_adjustment": summary.get("avg_abs_adjustment", {}).get("iso"),
                "avg_abs_slugging_pct_adjustment": summary.get("avg_abs_adjustment", {}).get("slugging_pct"),
                "avg_abs_k_pct_adjustment": summary.get("avg_abs_adjustment", {}).get("k_pct"),
                "avg_abs_bb_pct_adjustment": summary.get("avg_abs_adjustment", {}).get("bb_pct"),
            }
            for setting, summary in setting_summaries.items()
            for rows in [grouped.get(setting, [])]
        ],
        key=lambda row: (
            row["extreme_delta_sides"] if row["extreme_delta_sides"] is not None else 999999,
            row["avg_abs_iso_adjustment"] if row["avg_abs_iso_adjustment"] is not None else 999999,
        ),
    )

    raw_summary = {
        "team_sides": len(base_rows),
        "lineup_sides_would_activate": sum(1 for row in base_rows if row.get("would_activate")),
        "extreme_delta_sides": sum(1 for row in base_rows if row.get("extreme_delta")),
        "extreme_delta_rate": (
            round(sum(1 for row in base_rows if row.get("extreme_delta")) / len(base_rows), 4)
            if base_rows
            else None
        ),
        "delta_distributions": {
            metric: _distribution([row.get(f"{metric}_delta") for row in base_rows])
            for metric in METRICS
        },
    }

    payload = {
        "start": start,
        "end": end,
        "database_url": database_url,
        "blend_weights": blend_weights,
        "shrinkage_pa_grid": shrinkage_pa_grid,
        "raw_true_talent_summary": raw_summary,
        "ranked_settings": ranked_settings,
        "setting_summaries": setting_summaries,
        "recommendation": (
            "Audit only. Do not patch production lineup offense yet. "
            "Use this grid to choose a stable shrinkage setting for a later production candidate."
        ),
    }

    output_dir = Path("tmp")
    output_dir.mkdir(parents=True, exist_ok=True)

    prefix = f"player_true_talent_shrinkage_grid_{start}_to_{end}"
    json_path = output_dir / f"{prefix}.json"
    grid_csv_path = output_dir / f"{prefix}.csv"
    ranked_csv_path = output_dir / f"{prefix}_ranked_settings.csv"

    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True))
    _write_csv(grid_csv_path, grid_rows)
    _write_csv(ranked_csv_path, ranked_settings)

    print(json.dumps(
        {
            "raw_true_talent_summary": raw_summary,
            "top_ranked_settings": ranked_settings[:10],
            "recommendation": payload["recommendation"],
        },
        indent=2,
        sort_keys=True,
    ))
    print(f"Wrote {json_path}")
    print(f"Wrote {grid_csv_path}")
    print(f"Wrote {ranked_csv_path}")


if __name__ == "__main__":
    main()
