from __future__ import annotations

import csv
import json
import math
import os
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from statistics import mean, pstdev

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from mlb_app.matchup_generator import generate_matchups_for_date


DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "sqlite:///mlb.db",
)

BACKTEST_START = os.getenv(
    "BACKTEST_START",
    "2026-04-20",
)

BACKTEST_END = os.getenv(
    "BACKTEST_END",
)

MAX_GAMES = int(
    os.getenv("MAX_GAMES", "50")
)


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


def write_csv(path, rows):
    if not rows:
        path.write_text("")
        return

    fields = sorted(
        {
            k
            for row in rows
            for k in row.keys()
        }
    )

    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=fields,
        )
        writer.writeheader()
        writer.writerows(rows)


def daterange(start, end):
    cur = start

    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def normalize_percent(value):
    flags = []

    if value is None:
        return None, flags

    try:
        val = float(value)
    except Exception:
        return None, ["non_numeric"]

    if 0 <= val <= 1:
        return val, flags

    if 1 < val <= 100:
        return (
            val / 100.0,
            ["converted_percent_scale"],
        )

    return None, ["invalid_scale"]


def normalize_pitch_type(raw):
    if raw is None:
        return "", ["missing_pitch_type"]

    raw = str(raw).upper()

    if raw in PITCH_TYPE_MAP:
        return PITCH_TYPE_MAP[raw], []

    return raw.lower(), ["unknown_pitch_type"]


def load_matchups_for_date(target_date):
    engine = create_engine(DATABASE_URL)
    SessionLocal = sessionmaker(bind=engine)

    with SessionLocal() as session:
        matchups = generate_matchups_for_date(
            session,
            target_date,
        )

    if not matchups:
        raise RuntimeError(
            f"No real matchups for {target_date}"
        )

    sliced = matchups[:MAX_GAMES]

    for matchup in sliced:
        if "_fallback_error" in matchup:
            raise RuntimeError(
                "Fallback payload detected"
            )

    return sliced


def extract_rows(matchup, team_side):
    arsenal_key = (
        f"{team_side}_pitch_arsenal"
    )

    arsenal = matchup.get(arsenal_key) or {}

    rows = []

    for raw_pitch_type, metrics in arsenal.items():

        if not isinstance(metrics, dict):
            continue

        pitch_type, flags = (
            normalize_pitch_type(
                raw_pitch_type
            )
        )

        row = {
            "game_pk": matchup.get("game_pk"),
            "game_date": matchup.get("game_date"),
            "team_side": team_side,
            "canonical_pitch_type":
                pitch_type,
            "validation_flags": flags[:],
        }

        metric_map = {
            "usage_pct":
                "canonical_usage_pct",
            "whiff_pct":
                "canonical_whiff_pct",
            "strikeout_pct":
                "canonical_strikeout_pct",
            "xwoba":
                "canonical_xwoba",
            "xba":
                "canonical_xba",
            "hard_hit_pct":
                "canonical_hard_hit_pct",
        }

        for src, dest in (
            metric_map.items()
        ):
            val, extra_flags = (
                normalize_percent(
                    metrics.get(src)
                )
            )

            row[dest] = val
            row["validation_flags"].extend(
                extra_flags
            )

        rv = metrics.get("rv_per_100")

        try:
            row[
                "canonical_rv_per_100"
            ] = (
                float(rv)
                if rv is not None
                else None
            )
        except Exception:
            row[
                "canonical_rv_per_100"
            ] = None

        row[
            "canonical_pitch_mix_weight"
        ] = row.get(
            "canonical_usage_pct"
        )

        damage_vals = []

        for field in [
            "canonical_xwoba",
            "canonical_xba",
            "canonical_hard_hit_pct",
        ]:
            val = row.get(field)

            if val is not None:
                damage_vals.append(val)

        rv_val = row.get(
            "canonical_rv_per_100"
        )

        if rv_val is not None:
            damage_vals.append(
                rv_val / 10.0
            )

        row[
            "canonical_damage_proxy"
        ] = (
            sum(damage_vals)
            / len(damage_vals)
            if damage_vals
            else None
        )

        rows.append(row)

    return rows


def weighted_average(rows, value_field):
    numerator = 0.0
    denominator = 0.0
    missing = 0

    for row in rows:
        val = row.get(value_field)

        if val is None:
            missing += 1
            continue

        weight = row.get(
            "canonical_pitch_mix_weight"
        )

        if weight is None:
            weight = 1.0

        numerator += val * weight
        denominator += weight

    if denominator <= 0:
        return None, denominator, missing

    return (
        numerator / denominator,
        denominator,
        missing,
    )


def calculate_shadow(
    matchup,
    team_side,
):
    rows = extract_rows(
        matchup,
        team_side,
    )

    k1, weight, missing1 = (
        weighted_average(
            rows,
            "canonical_whiff_pct",
        )
    )

    k2, _, missing2 = (
        weighted_average(
            rows,
            "canonical_strikeout_pct",
        )
    )

    pit_k_shadow = None

    if (
        k1 is not None
        and k2 is not None
    ):
        pit_k_shadow = (
            k1 + k2
        ) / 2.0
    else:
        pit_k_shadow = k1 or k2

    xwoba_shadow, _, missing3 = (
        weighted_average(
            rows,
            "canonical_xwoba",
        )
    )

    hh_shadow, _, missing4 = (
        weighted_average(
            rows,
            "canonical_hard_hit_pct",
        )
    )

    if (
        hh_shadow is None
        and xwoba_shadow is not None
    ):
        hh_shadow = xwoba_shadow

    hr_shadow, _, missing5 = (
        weighted_average(
            rows,
            "canonical_damage_proxy",
        )
    )

    aggregate = matchup.get(
        f"{team_side}_pitcher_features",
        {},
    )

    return {
        "game_pk": matchup.get("game_pk"),
        "game_date":
            matchup.get("game_date"),
        "team_side": team_side,
        "row_count": len(rows),
        "pit_k_shadow":
            pit_k_shadow,
        "pit_xwoba_shadow":
            xwoba_shadow,
        "pit_hard_hit_shadow":
            hh_shadow,
        "pit_hr_shadow":
            hr_shadow,
        "baseline_xwoba":
            aggregate.get("xwoba"),
        "baseline_xba":
            aggregate.get("xba"),
        "baseline_hard_hit_pct":
            aggregate.get(
                "hard_hit_pct"
            ),
        "missing_required_count":
            (
                missing1
                + missing2
                + missing3
                + missing4
                + missing5
            ),
        "validation_flags":
            "|".join(
                sorted(
                    {
                        flag
                        for row in rows
                        for flag in row[
                            "validation_flags"
                        ]
                    }
                )
            ),
    }


def summarize_stability(values):
    non_null = [
        v for v in values
        if v is not None
    ]

    if not non_null:
        return {
            "count": 0,
            "missing_rate": 1.0,
        }

    return {
        "count": len(non_null),
        "mean": round(
            mean(non_null),
            4,
        ),
        "min": round(
            min(non_null),
            4,
        ),
        "max": round(
            max(non_null),
            4,
        ),
        "stddev": round(
            pstdev(non_null),
            4,
        )
        if len(non_null) > 1
        else 0.0,
        "missing_rate": round(
            1
            - (
                len(non_null)
                / len(values)
            ),
            4,
        ),
    }


def main():
    start = datetime.strptime(
        BACKTEST_START,
        "%Y-%m-%d",
    ).date()

    end = (
        datetime.strptime(
            BACKTEST_END,
            "%Y-%m-%d",
        ).date()
        if BACKTEST_END
        else start
    )

    all_results = []

    for day in daterange(start, end):
        matchups = load_matchups_for_date(
            str(day)
        )

        for matchup in matchups:
            for side in [
                "home",
                "away",
            ]:
                all_results.append(
                    calculate_shadow(
                        matchup,
                        side,
                    )
                )

    coverage_by_date = defaultdict(
        lambda: {
            "team_side_rows": 0,
            "non_null_rows": 0,
        }
    )

    composite_counts = defaultdict(
        lambda: {
            "non_null": 0,
            "null": 0,
        }
    )

    validation_counter = Counter()

    for row in all_results:
        coverage_by_date[
            row["game_date"]
        ]["team_side_rows"] += 1

        non_null_any = any(
            row.get(field)
            is not None
            for field in [
                "pit_k_shadow",
                "pit_xwoba_shadow",
                "pit_hard_hit_shadow",
                "pit_hr_shadow",
            ]
        )

        if non_null_any:
            coverage_by_date[
                row["game_date"]
            ]["non_null_rows"] += 1

        for field in [
            "pit_k_shadow",
            "pit_xwoba_shadow",
            "pit_hard_hit_shadow",
            "pit_hr_shadow",
        ]:
            if row.get(field) is not None:
                composite_counts[field][
                    "non_null"
                ] += 1
            else:
                composite_counts[field][
                    "null"
                ] += 1

        for flag in (
            row.get(
                "validation_flags",
                "",
            ).split("|")
        ):
            if flag:
                validation_counter[
                    flag
                ] += 1

    total_rows = len(all_results)

    stability = {}

    for field in [
        "pit_k_shadow",
        "pit_xwoba_shadow",
        "pit_hard_hit_shadow",
        "pit_hr_shadow",
    ]:
        stability[field] = (
            summarize_stability(
                [
                    row.get(field)
                    for row in all_results
                ]
            )
        )

    coverage_rates = {}

    for field, counts in (
        composite_counts.items()
    ):
        coverage_rates[field] = round(
            counts["non_null"]
            / total_rows,
            4,
        )

    avg_coverage = mean(
        coverage_rates.values()
    )

    if avg_coverage >= 0.7:
        diagnosis = (
            "shadow_composite_coverage_stable_enough_for_research_grid"
        )
    elif avg_coverage >= 0.3:
        diagnosis = (
            "shadow_composite_coverage_partial_needs_mapping_work"
        )
    else:
        diagnosis = (
            "shadow_composite_coverage_too_sparse"
        )

    output = {
        "diagnosis": diagnosis,
        "games_scanned":
            len(
                {
                    r["game_pk"]
                    for r in all_results
                }
            ),
        "team_side_rows":
            total_rows,
        "coverage_rates":
            coverage_rates,
        "stability":
            stability,
        "validation_flags":
            dict(validation_counter),
    }

    out_dir = Path("tmp")
    out_dir.mkdir(exist_ok=True)

    json_path = (
        out_dir
        / "shadow_composite_coverage_stability.json"
    )

    by_date_csv = (
        out_dir
        / "shadow_composite_coverage_by_date.csv"
    )

    by_comp_csv = (
        out_dir
        / "shadow_composite_coverage_by_composite.csv"
    )

    baseline_csv = (
        out_dir
        / "shadow_composite_baseline_comparisons.csv"
    )

    json_path.write_text(
        json.dumps(
            output,
            indent=2,
        )
    )

    date_rows = []

    for date_key, vals in (
        coverage_by_date.items()
    ):
        rate = (
            vals["non_null_rows"]
            / vals["team_side_rows"]
            if vals[
                "team_side_rows"
            ] > 0
            else 0
        )

        date_rows.append(
            {
                "game_date":
                    date_key,
                "team_side_rows":
                    vals[
                        "team_side_rows"
                    ],
                "non_null_rows":
                    vals[
                        "non_null_rows"
                    ],
                "coverage_rate":
                    round(rate, 4),
            }
        )

    write_csv(
        by_date_csv,
        date_rows,
    )

    comp_rows = []

    for field, counts in (
        composite_counts.items()
    ):
        comp_rows.append(
            {
                "composite":
                    field,
                "non_null":
                    counts[
                        "non_null"
                    ],
                "null":
                    counts["null"],
                "coverage_rate":
                    coverage_rates[
                        field
                    ],
                **stability[field],
            }
        )

    write_csv(
        by_comp_csv,
        comp_rows,
    )

    baseline_rows = []

    for row in all_results:
        baseline_rows.append(
            {
                "game_pk":
                    row["game_pk"],
                "game_date":
                    row["game_date"],
                "team_side":
                    row["team_side"],
                "pit_xwoba_shadow":
                    row[
                        "pit_xwoba_shadow"
                    ],
                "baseline_xwoba":
                    row[
                        "baseline_xwoba"
                    ],
                "pit_hard_hit_shadow":
                    row[
                        "pit_hard_hit_shadow"
                    ],
                "baseline_hard_hit_pct":
                    row[
                        "baseline_hard_hit_pct"
                    ],
            }
        )

    write_csv(
        baseline_csv,
        baseline_rows,
    )

    print(
        json.dumps(
            output,
            indent=2,
        )
    )

    print(f"Wrote {json_path}")
    print(f"Wrote {by_date_csv}")
    print(f"Wrote {by_comp_csv}")
    print(f"Wrote {baseline_csv}")


if __name__ == "__main__":
    main()
