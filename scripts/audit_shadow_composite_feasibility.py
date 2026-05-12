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
}


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


def fake_matchup_payloads():

    return [
        {
            "pitcher": {
                "strikeout_pct": 0.27,
                "hard_hit_pct": 0.33,
                "xwoba": 0.289,
                "arsenal": {
                    "slider": {
                        "whiff_pct": 0.41,
                        "usage_pct": 0.34,
                        "rv_per_100": 1.8,
                    },
                    "fastball": {
                        "usage_pct": 0.42,
                        "rv_per_100": -0.3,
                    },
                },
                "pitch_mix": {
                    "slider": 0.34,
                    "fastball": 0.42,
                },
            }
        },
        {
            "pitcher": {
                "strikeout_pct": 0.21,
                "hard_hit_pct": None,
                "xwoba": 0.317,
                "arsenal": None,
                "pitch_mix": {},
            }
        },
    ]


def classify_feasibility(field_stats):

    def usable(field):

        stats = field_stats.get(field)

        if not stats:
            return False

        return (
            stats["numeric_rate"] > 0.5
            and stats["existence_rate"] > 0.5
        )

    composite_results = []

    if usable("whiff_pct"):

        k_status = "feasible_now"

    else:

        k_status = (
            "partially_feasible_missing_fields"
        )

    composite_results.append(
        {
            "shadow_composite": "pit_k_shadow",
            "classification": k_status,
        }
    )

    if usable("rv_per_100"):

        xwoba_status = "feasible_now"

    else:

        xwoba_status = (
            "partially_feasible_missing_fields"
        )

    composite_results.append(
        {
            "shadow_composite":
                "pit_xwoba_shadow",
            "classification":
                xwoba_status,
        }
    )

    if usable("pitch_mix"):

        hh_status = "feasible_now"

    else:

        hh_status = (
            "schema_inconsistent"
        )

    composite_results.append(
        {
            "shadow_composite":
                "pit_hard_hit_shadow",
            "classification":
                hh_status,
        }
    )

    if usable("xwoba"):

        hr_status = "feasible_now"

    else:

        hr_status = (
            "partially_feasible_missing_fields"
        )

    composite_results.append(
        {
            "shadow_composite":
                "pit_hr_shadow",
            "classification":
                hr_status,
        }
    )

    return composite_results


def main():

    payloads = fake_matchup_payloads()

    flattened_rows = []
    field_counters = defaultdict(Counter)
    field_examples = defaultdict(list)

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

    field_stats = {}

    total_payloads = max(
        len(payloads),
        1,
    )

    arsenal_rows = []

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

        examples = (
            field_examples[field][:3]
        )

        field_stats[field] = {
            "existence_rate":
                round(existence_rate, 3),
            "null_rate":
                round(null_rate, 3),
            "numeric_rate":
                round(numeric_rate, 3),
            "example_values":
                examples,
        }

        arsenal_rows.append(
            {
                "field": field,
                "existence_rate":
                    round(existence_rate, 3),
                "null_rate":
                    round(null_rate, 3),
                "numeric_rate":
                    round(numeric_rate, 3),
                "example_values":
                    "; ".join(examples),
            }
        )

    composite_results = (
        classify_feasibility(field_stats)
    )

    classifications = {
        row["classification"]
        for row in composite_results
    }

    if classifications == {"feasible_now"}:

        diagnosis = (
            "shadow_composites_feasible_for_research_prototype"
        )

    elif (
        "partially_feasible_missing_fields"
        in classifications
    ):

        diagnosis = (
            "shadow_composites_partially_feasible_needs_schema_work"
        )

    elif (
        "schema_inconsistent"
        in classifications
    ):

        diagnosis = (
            "shadow_composite_feasibility_unclear_needs_manual_review"
        )

    else:

        diagnosis = (
            "shadow_composites_not_feasible_current_data"
        )

    payload = {
        "diagnosis": diagnosis,
        "payloads_scanned": len(payloads),
        "target_fields": sorted(
            TARGET_FIELDS
        ),
        "field_stats": field_stats,
        "shadow_composites": composite_results,
    }

    out_dir = Path("tmp")
    out_dir.mkdir(exist_ok=True)

    json_path = (
        out_dir
        / "shadow_composite_feasibility.json"
    )

    paths_csv = (
        out_dir
        / "shadow_composite_arsenal_paths.csv"
    )

    composite_csv = (
        out_dir
        / "shadow_composite_feasibility_by_composite.csv"
    )

    json_path.write_text(
        json.dumps(
            payload,
            indent=2,
        )
    )

    write_csv(
        paths_csv,
        arsenal_rows,
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
