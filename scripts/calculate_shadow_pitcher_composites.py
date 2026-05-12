from __future__ import annotations

import csv
import json
import os
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from mlb_app.matchup_generator import generate_matchups_for_date


DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///mlb.db")
BACKTEST_START = os.getenv("BACKTEST_START", "2026-04-20")
MAX_GAMES = int(os.getenv("MAX_GAMES", "5"))


PITCH_TYPE_MAP = {
    "FF": "four_seam",
    "FA": "four_seam",
    "SI": "sinker",
    "FT": "sinker",
    "SL": "slider",
    "CU": "curveball",
    "CH": "changeup",
    "KC": "knuckle_curve",
    "FC": "cutter",
}


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        path.write_text("")
        return

    fields = sorted({k for row in rows for k in row.keys()})

    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def normalize_percent(value: Any):
    if value is None:
        return None, []

    try:
        val = float(value)
    except (TypeError, ValueError):
        return None, ["non_numeric"]

    if 0 <= val <= 1:
        return val, []

    if 1 < val <= 100:
        return val / 100.0, ["converted_percent_scale"]

    return None, ["invalid_scale"]


def normalize_pitch_type(raw_pitch_type):
    if raw_pitch_type is None:
        return "", ["missing_pitch_type"]

    raw = str(raw_pitch_type).upper()

    if raw in PITCH_TYPE_MAP:
        return PITCH_TYPE_MAP[raw], []

    return raw.lower(), ["unknown_pitch_type"]


def normalize_row(
    matchup,
    team_side,
    raw_pitch_type,
    metrics,
):
    row = {
        "game_pk": matchup.get("game_pk"),
        "game_date": matchup.get("game_date"),
        "team_side": team_side,
        "raw_pitch_type": raw_pitch_type,
        "validation_flags": [],
    }

    canonical_pitch, flags = normalize_pitch_type(
        raw_pitch_type
    )

    row["canonical_pitch_type"] = canonical_pitch
    row["validation_flags"].extend(flags)

    metric_fields = {
        "usage_pct": "canonical_usage_pct",
        "whiff_pct": "canonical_whiff_pct",
        "strikeout_pct": "canonical_strikeout_pct",
        "xwoba": "canonical_xwoba",
        "xba": "canonical_xba",
        "hard_hit_pct": "canonical_hard_hit_pct",
    }

    for raw_metric, target in metric_fields.items():
        val, flags = normalize_percent(
            metrics.get(raw_metric)
        )

        row[target] = val
        row["validation_flags"].extend(flags)

    rv = metrics.get("rv_per_100")

    try:
        row["canonical_rv_per_100"] = (
            float(rv)
            if rv is not None
            else None
        )
    except (TypeError, ValueError):
        row["canonical_rv_per_100"] = None
        row["validation_flags"].append("invalid_rv")

    row["canonical_pitch_mix_weight"] = (
        row.get("canonical_usage_pct")
    )

    damage_components = []

    for field in [
        "canonical_xwoba",
        "canonical_xba",
        "canonical_hard_hit_pct",
    ]:
        val = row.get(field)

        if val is not None:
            damage_components.append(val)

    rv_val = row.get("canonical_rv_per_100")

    if rv_val is not None:
        damage_components.append(rv_val / 10.0)

    row["canonical_damage_proxy"] = (
        sum(damage_components) / len(damage_components)
        if damage_components
        else None
    )

    row["validation_flags"] = "|".join(
        sorted(set(row["validation_flags"]))
    )

    return row


def load_matchups():
    engine = create_engine(DATABASE_URL)
    SessionLocal = sessionmaker(bind=engine)

    with SessionLocal() as session:
        matchups = generate_matchups_for_date(
            session,
            BACKTEST_START,
        )

    if not matchups:
        raise RuntimeError(
            "No real matchups returned"
        )

    sliced = matchups[:MAX_GAMES]

    for matchup in sliced:
        if "_fallback_error" in matchup:
            raise RuntimeError(
                "Fallback payload detected"
            )

    return sliced


def extract_rows(matchups):
    rows = []

    for matchup in matchups:
        for team_side in ["home", "away"]:

            arsenal_key = (
                f"{team_side}_pitch_arsenal"
            )

            arsenal = (
                matchup.get(arsenal_key) or {}
            )

            if not isinstance(arsenal, dict):
                continue

            for raw_pitch_type, metrics in arsenal.items():

                if not isinstance(metrics, dict):
                    continue

                rows.append(
                    normalize_row(
                        matchup,
                        team_side,
                        raw_pitch_type,
                        metrics,
                    )
                )

    return rows


def weighted_average(rows, value_field):
    numerator = 0.0
    denominator = 0.0
    missing_required = 0

    for row in rows:
        value = row.get(value_field)
        weight = row.get(
            "canonical_pitch_mix_weight"
        )

        if value is None:
            missing_required += 1
            continue

        if weight is None:
            weight = 1.0

        numerator += value * weight
        denominator += weight

    if denominator <= 0:
        return None, denominator, missing_required

    return (
        numerator / denominator,
        denominator,
        missing_required,
    )


def calculate_shadow_composites(
    matchup,
    team_side,
    rows,
):
    game_pk = matchup.get("game_pk")
    game_date = matchup.get("game_date")

    relevant_rows = [
        row
        for row in rows
        if row["game_pk"] == game_pk
        and row["team_side"] == team_side
    ]

    k_shadow, k_weight, k_missing = (
        weighted_average(
            relevant_rows,
            "canonical_whiff_pct",
        )
    )

    k2_shadow, _, _ = weighted_average(
        relevant_rows,
        "canonical_strikeout_pct",
    )

    if (
        k_shadow is not None
        and k2_shadow is not None
    ):
        pit_k_shadow = (
            k_shadow + k2_shadow
        ) / 2.0
    else:
        pit_k_shadow = (
            k_shadow or k2_shadow
        )

    xwoba_shadow, xwoba_weight, xwoba_missing = (
        weighted_average(
            relevant_rows,
            "canonical_xwoba",
        )
    )

    hard_hit_shadow, hh_weight, hh_missing = (
        weighted_average(
            relevant_rows,
            "canonical_hard_hit_pct",
        )
    )

    if (
        hard_hit_shadow is None
        and xwoba_shadow is not None
    ):
        hard_hit_shadow = xwoba_shadow

    hr_shadow, hr_weight, hr_missing = (
        weighted_average(
            relevant_rows,
            "canonical_damage_proxy",
        )
    )

    aggregate_key = (
        f"{team_side}_pitcher_features"
    )

    aggregate = (
        matchup.get(aggregate_key) or {}
    )

    return {
        "game_pk": game_pk,
        "game_date": game_date,
        "team_side": team_side,
        "pit_k_shadow": pit_k_shadow,
        "pit_xwoba_shadow": xwoba_shadow,
        "pit_hard_hit_shadow": hard_hit_shadow,
        "pit_hr_shadow": hr_shadow,
        "row_count": len(relevant_rows),
        "usage_weight_sum": round(
            max(
                k_weight,
                xwoba_weight,
                hh_weight,
                hr_weight,
            ),
            4,
        ),
        "missing_required_count": (
            k_missing
            + xwoba_missing
            + hh_missing
            + hr_missing
        ),
        "validation_flags": "|".join(
            sorted(
                set(
                    flag
                    for row in relevant_rows
                    for flag in str(
                        row.get(
                            "validation_flags",
                            "",
                        )
                    ).split("|")
                    if flag
                )
            )
        ),
        "baseline_xwoba": aggregate.get(
            "xwoba"
        ),
        "baseline_xba": aggregate.get(
            "xba"
        ),
        "baseline_hard_hit_pct": aggregate.get(
            "hard_hit_pct"
        ),
    }


def summarize(composites):
    ready = sum(
        1
        for row in composites
        if row.get("pit_k_shadow") is not None
        and row.get("pit_xwoba_shadow")
        is not None
        and row.get("pit_hr_shadow")
        is not None
    )

    if ready == len(composites) and ready > 0:
        diagnosis = (
            "shadow_pitcher_composites_calculated_research_only"
        )
    elif ready > 0:
        diagnosis = (
            "shadow_pitcher_composites_partial_needs_coverage_work"
        )
    else:
        diagnosis = (
            "shadow_pitcher_composites_failed_real_payloads"
        )

    return {
        "diagnosis": diagnosis,
        "games_scanned": len(
            {
                row["game_pk"]
                for row in composites
            }
        ),
        "composites_generated": len(
            composites
        ),
    }


def main():
    matchups = load_matchups()

    rows = extract_rows(matchups)

    composites = []

    for matchup in matchups:
        for team_side in ["home", "away"]:
            composites.append(
                calculate_shadow_composites(
                    matchup,
                    team_side,
                    rows,
                )
            )

    summary = summarize(composites)

    output = {
        **summary,
        "shadow_composites": composites,
    }

    out_dir = Path("tmp")
    out_dir.mkdir(exist_ok=True)

    json_path = (
        out_dir
        / "shadow_pitcher_composites.json"
    )

    composite_csv = (
        out_dir
        / "shadow_pitcher_composites.csv"
    )

    comparison_csv = (
        out_dir
        / "shadow_pitcher_composite_comparisons.csv"
    )

    json_path.write_text(
        json.dumps(
            output,
            indent=2,
        )
    )

    write_csv(
        composite_csv,
        composites,
    )

    comparison_rows = []

    for row in composites:
        comparison_rows.append(
            {
                "game_pk": row["game_pk"],
                "team_side": row["team_side"],
                "pit_xwoba_shadow":
                    row["pit_xwoba_shadow"],
                "baseline_xwoba":
                    row["baseline_xwoba"],
                "pit_hard_hit_shadow":
                    row["pit_hard_hit_shadow"],
                "baseline_hard_hit_pct":
                    row["baseline_hard_hit_pct"],
            }
        )

    write_csv(
        comparison_csv,
        comparison_rows,
    )

    print(
        json.dumps(
            output,
            indent=2,
        )
    )

    print(f"Wrote {json_path}")
    print(f"Wrote {composite_csv}")
    print(f"Wrote {comparison_csv}")


if __name__ == "__main__":
    main()
