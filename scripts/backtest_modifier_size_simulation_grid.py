from __future__ import annotations

import copy
import csv
import json
import os
from collections import defaultdict
from pathlib import Path
from statistics import mean
from typing import Any, Dict, List, Tuple

from mlb_app.database import create_tables, get_engine, get_session
from mlb_app.matchup_generator import generate_matchups_for_date
from mlb_app.simulation.game_engine_v2 import run_full_game_simulation


BACKTEST_START = os.getenv("BACKTEST_START", "2026-04-20")
BACKTEST_END = os.getenv("BACKTEST_END", "2026-05-03")
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///mlb.db")
SIMS_PER_GAME = int(os.getenv("SIMS_PER_GAME", "500"))
SEED = int(os.getenv("SEED", "42"))

SIZE_GRID = {
    "tiny": 0.25,
    "small": 0.50,
    "medium": 1.00,
    "large": 2.00,
    "sentinel": 6.00,
}

FAMILIES = [
    "hitter_expected_contact",
    "hitter_recent",
    "pitcher_stuff",
    "combined_expected_contact_and_stuff",
    "combined_recent_and_stuff",
]


def daterange(start: str, end: str) -> List[str]:
    import datetime as dt

    current = dt.date.fromisoformat(start)
    stop = dt.date.fromisoformat(end)

    out = []
    while current <= stop:
        out.append(current.isoformat())
        current += dt.timedelta(days=1)

    return out


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def flatten(obj: Any, prefix: str = "") -> Dict[str, Any]:
    out: Dict[str, Any] = {}

    if isinstance(obj, dict):
        for key, value in obj.items():
            path = f"{prefix}.{key}" if prefix else key
            out[path] = value
            if isinstance(value, (dict, list)):
                out.update(flatten(value, path))

    elif isinstance(obj, list):
        for idx, value in enumerate(obj[:5]):
            path = f"{prefix}[{idx}]"
            out[path] = value
            if isinstance(value, (dict, list)):
                out.update(flatten(value, path))

    return out


def extract_pa_summary(result: Dict[str, Any]) -> Dict[str, float]:
    flat = flatten(result.get("pa_models", {}) or {})

    summary = {
        "away_k_rate": None,
        "away_bb_rate": None,
        "away_hit_rate": None,
        "away_hr_rate": None,
        "home_k_rate": None,
        "home_bb_rate": None,
        "home_hit_rate": None,
        "home_hr_rate": None,
    }

    for key in list(summary.keys()):
        side, metric = key.split("_", 1)

        candidates = [
            f"{side}.lineup_average_summary.{metric}",
            f"{side}_starter.lineup_average_summary.{metric}",
            f"{side}_pa_model.lineup_average_summary.{metric}",
        ]

        for candidate in candidates:
            if candidate in flat:
                summary[key] = safe_float(flat[candidate])
                break

    # Fallback path search.
    for path, value in flat.items():
        lower = path.lower()
        for side in ["away", "home"]:
            for metric in ["k_rate", "bb_rate", "hit_rate", "hr_rate"]:
                key = f"{side}_{metric}"
                if summary[key] is None and side in lower and metric in lower:
                    if isinstance(value, (int, float)):
                        summary[key] = safe_float(value)

    return {
        key: safe_float(value)
        for key, value in summary.items()
        if value is not None
    }


def extract_outputs(result: Dict[str, Any]) -> Dict[str, float]:
    flat = flatten(result)

    keys = {
        "total_runs": [
            "derived_outputs.mean_total_runs",
            "derived_outputs.expected_total_runs",
            "derived_outputs.total_runs.mean",
            "derived_outputs.game_summary.mean_total_runs",
        ],
        "away_runs": [
            "derived_outputs.away_mean_runs",
            "derived_outputs.expected_away_runs",
            "derived_outputs.away_runs.mean",
        ],
        "home_runs": [
            "derived_outputs.home_mean_runs",
            "derived_outputs.expected_home_runs",
            "derived_outputs.home_runs.mean",
        ],
        "home_win_probability": [
            "derived_outputs.home_win_probability",
            "derived_outputs.home_win_prob",
            "derived_outputs.home_win_pct",
            "derived_outputs.win_probability.home",
        ],
    }

    output = {}

    for label, candidates in keys.items():
        output[label] = 0.0
        for candidate in candidates:
            if candidate in flat:
                output[label] = safe_float(flat[candidate])
                break

    return output


def output_delta_count(baseline: Dict[str, Any], candidate: Dict[str, Any]) -> int:
    bflat = flatten(baseline)
    cflat = flatten(candidate)

    count = 0
    for key, cvalue in cflat.items():
        if key not in bflat:
            continue
        bvalue = bflat[key]
        if isinstance(cvalue, (int, float, str, bool)) and bvalue != cvalue:
            count += 1
    return count


def apply_modifier(
    matchup: Dict[str, Any],
    family: str,
    size: float,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    copied = copy.deepcopy(matchup)
    rows: List[Dict[str, Any]] = []

    apply_hitter = family in {
        "hitter_expected_contact",
        "hitter_recent",
        "combined_expected_contact_and_stuff",
        "combined_recent_and_stuff",
    }
    apply_pitcher = family in {
        "pitcher_stuff",
        "combined_expected_contact_and_stuff",
        "combined_recent_and_stuff",
    }

    hitter_base = 1.0
    if family in {"hitter_recent", "combined_recent_and_stuff"}:
        hitter_base = 0.6

    for side in ["away", "home"]:
        offense_key = f"{side}_offense_inputs"
        pitcher_key = f"{side}_pitcher_features"

        if apply_hitter and isinstance(copied.get(offense_key), dict):
            offense = copied[offense_key]

            ev = safe_float(offense.get("avg_exit_velocity"), 88.0)
            barrel = safe_float(offense.get("barrel_pct"), 0.08)
            hard_hit = safe_float(offense.get("hard_hit_pct"), 0.38)

            contact_signal = clamp(
                ((ev - 88.0) / 10.0)
                + ((barrel - 0.08) * 3.0)
                + ((hard_hit - 0.38) * 2.0),
                -0.20,
                0.20,
            )

            deltas = {
                "batting_avg": contact_signal * 0.050 * size * hitter_base,
                "slugging_pct": contact_signal * 0.120 * size * hitter_base,
                "iso": contact_signal * 0.080 * size * hitter_base,
                "k_pct": -contact_signal * 0.040 * size * hitter_base,
                "bb_pct": contact_signal * 0.020 * size * hitter_base,
            }

            for metric, delta in deltas.items():
                if metric not in offense:
                    continue

                original = safe_float(offense.get(metric))
                adjusted = original + delta

                if metric in {"batting_avg", "k_pct", "bb_pct"}:
                    adjusted = clamp(adjusted, 0.01, 0.50)
                elif metric == "slugging_pct":
                    adjusted = clamp(adjusted, 0.15, 0.90)
                elif metric == "iso":
                    adjusted = clamp(adjusted, 0.00, 0.55)

                offense[metric] = round(adjusted, 6)

                rows.append(
                    {
                        "side": side,
                        "section": offense_key,
                        "metric": metric,
                        "original": original,
                        "adjusted": adjusted,
                        "delta": adjusted - original,
                    }
                )

            offense["_audit_modifier_size"] = size
            offense["_audit_modifier_family"] = family

        if apply_pitcher and isinstance(copied.get(pitcher_key), dict):
            pitcher = copied[pitcher_key]

            velocity = safe_float(pitcher.get("avg_velocity"), 93.0)
            k_pct = safe_float(pitcher.get("k_pct"), 0.22)
            hard_hit = safe_float(pitcher.get("hard_hit_pct"), 0.38)

            stuff_signal = clamp(
                ((velocity - 93.0) / 6.0)
                + ((k_pct - 0.22) * 2.5)
                - ((hard_hit - 0.38) * 2.0),
                -0.20,
                0.20,
            )

            deltas = {
                "xba": -stuff_signal * 0.040 * size,
                "xwoba": -stuff_signal * 0.060 * size,
                "hard_hit_pct": -stuff_signal * 0.050 * size,
                "k_pct": stuff_signal * 0.050 * size,
                "bb_pct": -stuff_signal * 0.020 * size,
                "avg_velocity": stuff_signal * 0.80 * size,
            }

            for metric, delta in deltas.items():
                if metric not in pitcher:
                    continue

                original = safe_float(pitcher.get(metric))
                adjusted = original + delta

                if metric in {"xba", "xwoba", "hard_hit_pct", "k_pct", "bb_pct"}:
                    adjusted = clamp(adjusted, 0.01, 0.70)
                elif metric == "avg_velocity":
                    adjusted = clamp(adjusted, 80.0, 105.0)

                pitcher[metric] = round(adjusted, 6)

                rows.append(
                    {
                        "side": side,
                        "section": pitcher_key,
                        "metric": metric,
                        "original": original,
                        "adjusted": adjusted,
                        "delta": adjusted - original,
                    }
                )

            pitcher["_audit_modifier_size"] = size
            pitcher["_audit_modifier_family"] = family

    copied["_audit_modifier_family"] = family
    copied["_audit_modifier_size"] = size

    return copied, rows


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        path.write_text("")
        return

    fieldnames = sorted({key for row in rows for key in row.keys()})
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    engine = get_engine(DATABASE_URL)
    create_tables(engine)
    SessionLocal = get_session(engine)

    game_rows: List[Dict[str, Any]] = []
    override_rows: List[Dict[str, Any]] = []
    output_key_rows: List[Dict[str, Any]] = []

    grouped = defaultdict(list)

    with SessionLocal() as session:
        for game_date in daterange(BACKTEST_START, BACKTEST_END):
            matchups = generate_matchups_for_date(session, game_date)

            for matchup in matchups:
                game_pk = matchup.get("game_pk")
                if not game_pk:
                    continue

                baseline_result = run_full_game_simulation(
                    int(game_pk),
                    config={
                        "simulation_count": SIMS_PER_GAME,
                        "seed": SEED,
                        "matchup": {
                            "raw": copy.deepcopy(matchup),
                            "game_date": game_date,
                        },
                    },
                )

                baseline_pa = extract_pa_summary(baseline_result)
                baseline_out = extract_outputs(baseline_result)

                for family in FAMILIES:
                    for size_name, size in SIZE_GRID.items():
                        config_name = f"{family}__{size_name}"

                        injected, changes = apply_modifier(matchup, family, size)

                        try:
                            result = run_full_game_simulation(
                                int(game_pk),
                                config={
                                    "simulation_count": SIMS_PER_GAME,
                                    "seed": SEED,
                                    "matchup": {
                                        "raw": injected,
                                        "game_date": game_date,
                                    },
                                },
                            )
                            success = result.get("status") == "ok"
                        except Exception as exc:
                            result = {"status": "error", "error": str(exc)}
                            success = False

                        cand_pa = extract_pa_summary(result)
                        cand_out = extract_outputs(result)

                        row = {
                            "game_pk": game_pk,
                            "game_date": game_date,
                            "family": family,
                            "size_name": size_name,
                            "size_multiplier": size,
                            "config": config_name,
                            "success": success,
                            "output_delta_count": output_delta_count(baseline_result, result),
                            "total_run_delta": cand_out.get("total_runs", 0.0) - baseline_out.get("total_runs", 0.0),
                            "home_win_probability_delta": cand_out.get("home_win_probability", 0.0) - baseline_out.get("home_win_probability", 0.0),
                        }

                        for metric in [
                            "away_k_rate",
                            "away_bb_rate",
                            "away_hit_rate",
                            "away_hr_rate",
                            "home_k_rate",
                            "home_bb_rate",
                            "home_hit_rate",
                            "home_hr_rate",
                        ]:
                            row[f"{metric}_delta"] = cand_pa.get(metric, 0.0) - baseline_pa.get(metric, 0.0)

                        game_rows.append(row)
                        grouped[config_name].append(row)

                        for change in changes:
                            override_rows.append(
                                {
                                    "game_pk": game_pk,
                                    "game_date": game_date,
                                    "config": config_name,
                                    "family": family,
                                    "size_name": size_name,
                                    **change,
                                }
                            )

                        for path in list(flatten(result).keys())[:300]:
                            output_key_rows.append(
                                {
                                    "config": config_name,
                                    "path": path,
                                }
                            )

    config_rows: List[Dict[str, Any]] = []

    for config_name, rows in grouped.items():
        success_rate = mean([1.0 if row["success"] else 0.0 for row in rows]) if rows else 0.0

        pa_k_delta = mean(
            [
                abs(row["away_k_rate_delta"]) + abs(row["home_k_rate_delta"])
                for row in rows
            ]
        ) if rows else 0.0

        pa_bb_delta = mean(
            [
                abs(row["away_bb_rate_delta"]) + abs(row["home_bb_rate_delta"])
                for row in rows
            ]
        ) if rows else 0.0

        pa_hit_delta = mean(
            [
                abs(row["away_hit_rate_delta"]) + abs(row["home_hit_rate_delta"])
                for row in rows
            ]
        ) if rows else 0.0

        pa_hr_delta = mean(
            [
                abs(row["away_hr_rate_delta"]) + abs(row["home_hr_rate_delta"])
                for row in rows
            ]
        ) if rows else 0.0

        total_delta = mean([abs(row["total_run_delta"]) for row in rows]) if rows else 0.0
        win_delta = mean([abs(row["home_win_probability_delta"]) for row in rows]) if rows else 0.0
        output_delta = mean([row["output_delta_count"] for row in rows]) if rows else 0.0

        aggressiveness = "ok"
        if total_delta > 1.25 or win_delta > 0.10:
            aggressiveness = "too_aggressive"
        elif pa_hit_delta < 0.001 and total_delta < 0.01:
            aggressiveness = "too_small"

        config_rows.append(
            {
                "config": config_name,
                "family": config_name.rsplit("__", 1)[0],
                "size_name": config_name.rsplit("__", 1)[1],
                "games_evaluated": len(rows),
                "success_rate": round(success_rate, 6),
                "avg_abs_pa_k_delta": round(pa_k_delta, 6),
                "avg_abs_pa_bb_delta": round(pa_bb_delta, 6),
                "avg_abs_pa_hit_delta": round(pa_hit_delta, 6),
                "avg_abs_pa_hr_delta": round(pa_hr_delta, 6),
                "avg_abs_total_run_delta": round(total_delta, 6),
                "avg_abs_home_win_probability_delta": round(win_delta, 6),
                "avg_output_delta_count": round(output_delta, 4),
                "aggressiveness": aggressiveness,
            }
        )

    rankings = sorted(
        config_rows,
        key=lambda row: (
            row["aggressiveness"] != "ok",
            abs(row["avg_abs_total_run_delta"] - 0.15),
            abs(row["avg_abs_pa_hit_delta"] - 0.005),
        ),
    )

    ok_configs = [row for row in config_rows if row["aggressiveness"] == "ok"]
    too_small = [row for row in config_rows if row["aggressiveness"] == "too_small"]
    too_aggressive = [row for row in config_rows if row["aggressiveness"] == "too_aggressive"]

    if ok_configs:
        diagnosis = "modifier_size_band_identified"
        recommended = "Carry ok-sized configs into actual-score calibration audit."
    elif len(too_small) == len(config_rows):
        diagnosis = "modifiers_still_too_small"
        recommended = "Increase modifier caps or override deeper PA model inputs."
    elif len(too_aggressive) == len(config_rows):
        diagnosis = "modifiers_too_aggressive"
        recommended = "Shrink modifier sizes before calibration."
    else:
        diagnosis = "profile_injection_grid_needs_actual_scores"
        recommended = "Proceed to actual result extraction/calibration."

    payload = {
        "backtest_start": BACKTEST_START,
        "backtest_end": BACKTEST_END,
        "sims_per_game": SIMS_PER_GAME,
        "games_evaluated": len(set(row["game_pk"] for row in game_rows)),
        "configs_evaluated": len(config_rows),
        "diagnosis": diagnosis,
        "recommended_next_step": recommended,
        "top_ranked_configs": rankings[:10],
        "summary_counts": {
            "ok_configs": len(ok_configs),
            "too_small_configs": len(too_small),
            "too_aggressive_configs": len(too_aggressive),
        },
    }

    out_dir = Path("tmp")
    out_dir.mkdir(parents=True, exist_ok=True)

    prefix = f"modifier_size_simulation_grid_{BACKTEST_START}_to_{BACKTEST_END}"

    json_path = out_dir / f"{prefix}.json"
    games_csv = out_dir / f"{prefix}_games.csv"
    configs_csv = out_dir / f"{prefix}_configs.csv"
    overrides_csv = out_dir / f"{prefix}_overrides.csv"
    output_keys_csv = out_dir / f"{prefix}_output_keys.csv"
    rankings_csv = out_dir / f"{prefix}_rankings.csv"

    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True))
    write_csv(games_csv, game_rows)
    write_csv(configs_csv, config_rows)
    write_csv(overrides_csv, override_rows)
    write_csv(output_keys_csv, output_key_rows)
    write_csv(rankings_csv, rankings)

    print(json.dumps(payload, indent=2, sort_keys=True))
    print(f"Wrote {json_path}")
    print(f"Wrote {games_csv}")
    print(f"Wrote {configs_csv}")
    print(f"Wrote {overrides_csv}")
    print(f"Wrote {output_keys_csv}")
    print(f"Wrote {rankings_csv}")


if __name__ == "__main__":
    main()
