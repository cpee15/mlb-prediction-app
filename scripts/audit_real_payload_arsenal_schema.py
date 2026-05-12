from __future__ import annotations

import csv
import json
import os
from collections import Counter, defaultdict
from pathlib import Path


TARGET_FIELDS = {
    "whiff_pct",
    "usage_pct",
    "rv_per_100",
    "arsenal",
    "pitch_mix",
    "strikeout_pct",
    "hard_hit_pct",
    "xwoba",
    "xba",
    "hr_rate",
}


def infer_role(path: str) -> str:

    lower = path.lower()

    if "home" in lower and "starter" in lower:
        return "home_starter"

    if "away" in lower and "starter" in lower:
        return "away_starter"

    if "home" in lower and "bullpen" in lower:
        return "home_bullpen"

    if "away" in lower and "bullpen" in lower:
        return "away_bullpen"

    if (
        "lineup" in lower
        or "hitter" in lower
        or "offense" in lower
    ):
        return "offense"

    return "unknown"


def flatten(obj, prefix=""):

    rows = []

    if isinstance(obj, dict):

        for key, value in obj.items():

            new_prefix = (
                f"{prefix}.{key}"
                if prefix
                else str(key)
            )

            rows.extend(
                flatten(
                    value,
                    new_prefix,
                )
            )

    elif isinstance(obj, list):

        for idx, value in enumerate(obj):

            new_prefix = (
                f"{prefix}[{idx}]"
            )

            rows.extend(
                flatten(
                    value,
                    new_prefix,
                )
            )

    else:

        rows.append(
            {
                "path": prefix,
                "value": obj,
                "type": type(obj).__name__,
            }
        )

    return rows


def write_csv(path, rows):

    if not rows:
        path.write_text("")
        return

    fields = sorted(
        {
            key
            for row in rows
            for key in row.keys()
        }
    )

    with path.open(
        "w",
        newline="",
    ) as handle:

        writer = csv.DictWriter(
            handle,
            fieldnames=fields,
        )

        writer.writeheader()
        writer.writerows(rows)



def load_real_payloads():

    start = os.environ.get(
        "BACKTEST_START",
        "2026-04-20",
    )

    max_games = int(
        os.environ.get(
            "MAX_GAMES",
            "5",
        )
    )

    database_url = os.environ.get(
        "DATABASE_URL",
        "sqlite:///mlb.db",
    )

    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    from mlb_app.matchup_generator import (
        generate_matchups_for_date,
    )

    engine = create_engine(database_url)
    SessionLocal = sessionmaker(bind=engine)

    with SessionLocal() as session:
        matchups = generate_matchups_for_date(
            session,
            start,
        )

    if not matchups:
        raise RuntimeError(
            "generate_matchups_for_date returned zero matchups"
        )

    sliced = matchups[:max_games]

    for payload in sliced:
        if isinstance(payload, dict) and "_fallback_error" in payload:
            raise RuntimeError(
                "fallback payload detected; refusing to continue"
            )

    return sliced


def classify_feasibility(field_stats):

    def stable(field):

        stats = field_stats.get(field)

        if not stats:
            return False

        return (
            stats["numeric_rate"] >= 0.7
            and stats["existence_rate"] >= 0.7
        )

    results = []

    k_status = (
        "feasible_from_real_payloads"
        if stable("whiff_pct")
        else
        "partially_feasible_real_payloads_missing_fields"
    )

    xwoba_status = (
        "feasible_from_real_payloads"
        if stable("rv_per_100")
        else
        "schema_fragmented_needs_normalization"
    )

    hh_status = (
        "feasible_from_real_payloads"
        if stable("pitch_mix")
        else
        "schema_fragmented_needs_normalization"
    )

    hr_status = (
        "feasible_from_real_payloads"
        if stable("hr_rate")
        else
        "partially_feasible_real_payloads_missing_fields"
    )

    results.append(
        {
            "shadow_composite":
                "pit_k_shadow",
            "classification":
                k_status,
        }
    )

    results.append(
        {
            "shadow_composite":
                "pit_xwoba_shadow",
            "classification":
                xwoba_status,
        }
    )

    results.append(
        {
            "shadow_composite":
                "pit_hard_hit_shadow",
            "classification":
                hh_status,
        }
    )

    results.append(
        {
            "shadow_composite":
                "pit_hr_shadow",
            "classification":
                hr_status,
        }
    )

    return results


def main():

    payloads = load_real_payloads()

    flattened_rows = []
    field_counters = defaultdict(Counter)
    field_examples = defaultdict(list)
    field_paths = defaultdict(set)

    for payload in payloads:

        rows = flatten(payload)

        for row in rows:

            flattened_rows.append(row)

            path = row["path"]
            value = row["value"]

            for field in TARGET_FIELDS:

                if field in path:

                    field_counters[field]["seen"] += 1

                    if value is None:

                        field_counters[field]["null"] += 1

                    if isinstance(
                        value,
                        (int, float),
                    ):

                        field_counters[field]["numeric"] += 1

                    field_examples[field].append(
                        str(value)
                    )

                    field_paths[field].add(path)

    total_payloads = max(
        len(payloads),
        1,
    )

    schema_rows = []
    field_stats = {}

    for field in sorted(TARGET_FIELDS):

        counts = field_counters[field]

        existence_rate = (
            counts["seen"]
            / total_payloads
        )

        null_rate = (
            counts["null"]
            / max(counts["seen"], 1)
        )

        numeric_rate = (
            counts["numeric"]
            / max(counts["seen"], 1)
        )

        example_paths = list(
            field_paths[field]
        )[:3]

        example_values = (
            field_examples[field][:3]
        )

        field_stats[field] = {
            "existence_rate":
                round(existence_rate, 3),
            "null_rate":
                round(null_rate, 3),
            "numeric_rate":
                round(numeric_rate, 3),
            "schema_diversity":
                len(field_paths[field]),
            "example_paths":
                example_paths,
            "example_values":
                example_values,
        }

        for path in example_paths:

            schema_rows.append(
                {
                    "field": field,
                    "path": path,
                    "role":
                        infer_role(path),
                    "numeric_rate":
                        round(numeric_rate, 3),
                    "example_values":
                        "; ".join(example_values),
                }
            )

    composite_results = (
        classify_feasibility(field_stats)
    )

    classifications = {
        row["classification"]
        for row in composite_results
    }

    if all(
        x == "feasible_from_real_payloads"
        for x in classifications
    ):

        diagnosis = (
            "real_payload_shadow_composites_feasible"
        )

    elif (
        "schema_fragmented_needs_normalization"
        in classifications
    ):

        diagnosis = (
            "real_payload_shadow_composites_need_schema_normalization"
        )

    elif (
        "partially_feasible_real_payloads_missing_fields"
        in classifications
    ):

        diagnosis = (
            "real_payload_shadow_composites_partially_feasible"
        )

    else:

        diagnosis = (
            "real_payload_shadow_composites_not_feasible"
        )

    game_pks = [
        p.get("game_pk")
        for p in payloads
        if isinstance(p, dict)
    ]

    game_dates = [
        p.get("game_date")
        for p in payloads
        if isinstance(p, dict)
    ]

    payload = {
        "diagnosis": diagnosis,
        "loader_mode": "real_generate_matchups_for_date",
        "used_fallback": False,
        "payloads_scanned":
            len(payloads),
        "game_pks": game_pks,
        "game_dates": game_dates,
        "target_fields":
            sorted(TARGET_FIELDS),
        "field_stats":
            field_stats,
        "shadow_composites":
            composite_results,
    }

    out_dir = Path("tmp")
    out_dir.mkdir(exist_ok=True)

    json_path = (
        out_dir
        / "real_payload_arsenal_schema.json"
    )

    paths_csv = (
        out_dir
        / "real_payload_arsenal_paths.csv"
    )

    composite_csv = (
        out_dir
        / "real_payload_shadow_composite_feasibility.csv"
    )

    json_path.write_text(
        json.dumps(
            payload,
            indent=2,
        )
    )

    write_csv(
        paths_csv,
        schema_rows,
    )

    write_csv(
        composite_csv,
        composite_results,
    )

    print(
        json.dumps(
            payload,
            indent=2,
        )
    )

    print(f"Wrote {json_path}")
    print(f"Wrote {paths_csv}")
    print(f"Wrote {composite_csv}")


if __name__ == "__main__":
    main()
