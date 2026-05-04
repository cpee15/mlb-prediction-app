from __future__ import annotations

import csv
import datetime as dt
import json
import os
from statistics import mean
from typing import Any, Dict, List, Optional

from mlb_app.database import create_tables, get_engine, get_session
from mlb_app.model_projections import build_model_projection_payload


PA_KEYS = [
    "strikeout",
    "walk",
    "hit_by_pitch",
    "single",
    "double",
    "triple",
    "home_run",
    "reached_on_error",
    "out",
]

PA_MODEL_KEYS = [
    "away_vs_home_starter",
    "home_vs_away_starter",
    "away_vs_home_bullpen",
    "home_vs_away_bullpen",
]


def safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def pct(value: Optional[float]) -> str:
    if value is None:
        return "None"
    return f"{value * 100:.1f}%"


def nested_get(obj: Dict[str, Any], *path: str) -> Any:
    cur: Any = obj
    for key in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur


def summarize_pa_model(game: Dict[str, Any], model_key: str, model: Dict[str, Any]) -> Dict[str, Any]:
    probs = model.get("lineup_average_probabilities") or {}
    summary = model.get("lineup_average_summary") or {}
    direct = model.get("direct_inputs") or {}
    offense = direct.get("offense") or {}
    pitcher = direct.get("pitcher") or {}
    env = direct.get("environment") or {}
    weights = direct.get("role_weighting") or {}

    non_hr_hit = (
        safe_float(probs.get("single")) or 0.0
    ) + (
        safe_float(probs.get("double")) or 0.0
    ) + (
        safe_float(probs.get("triple")) or 0.0
    )

    total_hit = non_hr_hit + (safe_float(probs.get("home_run")) or 0.0)

    return {
        "game_pk": game.get("game_pk"),
        "matchup": f"{nested_get(game, 'away_team', 'name')} @ {nested_get(game, 'home_team', 'name')}",
        "model_key": model_key,
        "side": model.get("side"),
        "model_version": model.get("model_version"),
        "pitcher_role": weights.get("pitcher_role"),
        "strikeout": safe_float(probs.get("strikeout")),
        "walk": safe_float(probs.get("walk")),
        "hit_by_pitch": safe_float(probs.get("hit_by_pitch")),
        "single": safe_float(probs.get("single")),
        "double": safe_float(probs.get("double")),
        "triple": safe_float(probs.get("triple")),
        "home_run": safe_float(probs.get("home_run")),
        "reached_on_error": safe_float(probs.get("reached_on_error")),
        "out": safe_float(probs.get("out")),
        "hit_rate": safe_float(summary.get("hit_rate")) if summary else total_hit,
        "non_hr_hit_rate": non_hr_hit,
        "xbh_rate": safe_float(summary.get("xbh_rate")),
        "hr_rate": safe_float(summary.get("hr_rate")),
        "contact_out_rate": safe_float(probs.get("out")),
        "out_rate": safe_float(summary.get("out_rate")),
        "total_out_rate": (safe_float(probs.get("strikeout")) or 0.0) + (safe_float(probs.get("out")) or 0.0),
        "non_out_rate": 1.0 - ((safe_float(probs.get("strikeout")) or 0.0) + (safe_float(probs.get("out")) or 0.0)),
        "prob_sum": round(sum(safe_float(probs.get(k)) or 0.0 for k in PA_KEYS), 6),
        "off_k_rate": safe_float(offense.get("k_rate")),
        "off_bb_rate": safe_float(offense.get("bb_rate")),
        "off_batting_avg": safe_float(offense.get("batting_avg")),
        "off_iso": safe_float(offense.get("iso")),
        "off_slugging_pct": safe_float(offense.get("slugging_pct")),
        "pit_k_rate": safe_float(pitcher.get("k_rate")),
        "pit_bb_rate": safe_float(pitcher.get("bb_rate")),
        "pit_xba_allowed": safe_float(pitcher.get("xba_allowed")),
        "pit_xwoba_allowed": safe_float(pitcher.get("xwoba_allowed")),
        "pit_hard_hit_allowed": safe_float(pitcher.get("hard_hit_rate_allowed")),
        "pit_hr_rate": safe_float(pitcher.get("hr_rate")),
        "run_scoring_index": safe_float(env.get("run_scoring_index")),
        "hr_boost_index": safe_float(env.get("hr_boost_index")),
        "hit_boost_index": safe_float(env.get("hit_boost_index")),
        "k_weights": weights.get("k_weights"),
        "bb_weights": weights.get("bb_weights"),
        "hit_weights": weights.get("hit_weights"),
        "power_weights": weights.get("power_weights"),
        "raw_model": model,
    }


def bucket_flags(row: Dict[str, Any]) -> List[str]:
    flags = []

    k = row.get("strikeout")
    bb = row.get("walk")
    hit = row.get("hit_rate")
    hr = row.get("home_run")
    contact_out = row.get("out")
    total_out = row.get("total_out_rate")
    non_out = row.get("non_out_rate")
    prob_sum = row.get("prob_sum")

    if prob_sum is not None and abs(prob_sum - 1.0) > 0.001:
        flags.append("prob_sum_not_1")

    if k is not None and (k < 0.14 or k > 0.32):
        flags.append("k_extreme")
    if bb is not None and (bb < 0.055 or bb > 0.13):
        flags.append("bb_extreme")
    if hit is not None and hit > 0.34:
        flags.append("hit_high")
    if hit is not None and hit < 0.20:
        flags.append("hit_low")
    if hr is not None and hr > 0.045:
        flags.append("hr_high")
    if hr is not None and hr < 0.015:
        flags.append("hr_low")
    if total_out is not None and total_out < 0.62:
        flags.append("total_out_low")
    if total_out is not None and total_out > 0.74:
        flags.append("total_out_high")
    if non_out is not None and non_out > 0.38:
        flags.append("non_out_high")

    return flags


def compact_print(row: Dict[str, Any]) -> None:
    flags = bucket_flags(row)
    print(f"\n[{row['model_key']}] {row['side']} ({row['pitcher_role']})")
    print(
        "PA probs: "
        f"K={pct(row['strikeout'])}, "
        f"BB={pct(row['walk'])}, "
        f"HBP={pct(row['hit_by_pitch'])}, "
        f"1B={pct(row['single'])}, "
        f"2B={pct(row['double'])}, "
        f"3B={pct(row['triple'])}, "
        f"HR={pct(row['home_run'])}, "
        f"ROE={pct(row['reached_on_error'])}, "
        f"CONTACT_OUT={pct(row['out'])}, "
        f"TOTAL_OUT={pct(row['total_out_rate'])}, "
        f"NON_OUT={pct(row['non_out_rate'])}, "
        f"SUM={row['prob_sum']}"
    )
    print(
        "Inputs: "
        f"off_k={pct(row['off_k_rate'])}, "
        f"off_bb={pct(row['off_bb_rate'])}, "
        f"off_avg={row['off_batting_avg']}, "
        f"off_iso={row['off_iso']}, "
        f"pit_k={pct(row['pit_k_rate'])}, "
        f"pit_bb={pct(row['pit_bb_rate'])}, "
        f"pit_xba={row['pit_xba_allowed']}, "
        f"pit_xwoba={row['pit_xwoba_allowed']}, "
        f"pit_hard_hit={pct(row['pit_hard_hit_allowed'])}, "
        f"env(run/hr/hit)=({row['run_scoring_index']}, {row['hr_boost_index']}, {row['hit_boost_index']})"
    )
    if flags:
        print(f"FLAGS: {', '.join(flags)}")


def summarize_rows(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    def vals(key: str) -> List[float]:
        return [row[key] for row in rows if isinstance(row.get(key), (int, float))]

    summary = {}
    for key in [
        "strikeout",
        "walk",
        "hit_rate",
        "non_hr_hit_rate",
        "home_run",
        "out",
        "contact_out_rate",
        "total_out_rate",
        "non_out_rate",
        "run_scoring_index",
        "hr_boost_index",
        "hit_boost_index",
    ]:
        values = vals(key)
        if values:
            summary[key] = {
                "min": round(min(values), 4),
                "avg": round(mean(values), 4),
                "max": round(max(values), 4),
            }

    flag_counts: Dict[str, int] = {}
    for row in rows:
        for flag in bucket_flags(row):
            flag_counts[flag] = flag_counts.get(flag, 0) + 1

    summary["flag_counts"] = flag_counts
    return summary


def csv_safe_row(row: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(row)
    out.pop("raw_model", None)

    for key in ["k_weights", "bb_weights", "hit_weights", "power_weights"]:
        out[key] = json.dumps(out.get(key), sort_keys=True)

    out["flags"] = ",".join(bucket_flags(row))
    return out


def main() -> None:
    target_date = os.getenv("AUDIT_DATE") or dt.date.today().isoformat()
    database_url = os.getenv("DATABASE_URL", "sqlite:///mlb.db")

    engine = get_engine(database_url)
    create_tables(engine)
    Session = get_session(engine)

    with Session() as session:
        payload = build_model_projection_payload(session, target_date)

    games = payload.get("games") or []
    rows: List[Dict[str, Any]] = []

    print("\n=== PA MODEL AUDIT ===")
    print(f"date: {payload.get('date')}")
    print(f"games: {len(games)}")
    print(f"errors: {json.dumps(payload.get('errors') or [], indent=2)}")

    for game in games:
        print("\n" + "=" * 92)
        print(
            f"GAME {game.get('game_pk')}: "
            f"{nested_get(game, 'away_team', 'name')} @ {nested_get(game, 'home_team', 'name')}"
        )

        shared = game.get("sharedSimulation") or {}
        pa_models = shared.get("pa_models") or {}

        for model_key in PA_MODEL_KEYS:
            model = pa_models.get(model_key) or {}
            row = summarize_pa_model(game, model_key, model)
            rows.append(row)
            compact_print(row)

    summary = summarize_rows(rows)

    print("\n" + "=" * 92)
    print("=== SLATE SUMMARY ===")
    print(json.dumps(summary, indent=2, default=str))

    os.makedirs("tmp", exist_ok=True)

    json_path = f"tmp/pa_model_audit_{target_date}.json"
    csv_path = f"tmp/pa_model_audit_{target_date}.csv"

    with open(json_path, "w") as f:
        json.dump(
            {
                "date": target_date,
                "summary": summary,
                "rows": rows,
            },
            f,
            indent=2,
            default=str,
        )

    csv_rows = [csv_safe_row(row) for row in rows]
    if csv_rows:
        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(csv_rows[0].keys()))
            writer.writeheader()
            writer.writerows(csv_rows)

    print(f"\nWrote full JSON audit to {json_path}")
    print(f"Wrote CSV audit to {csv_path}")


if __name__ == "__main__":
    main()
