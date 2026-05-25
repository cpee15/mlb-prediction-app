from __future__ import annotations

import csv
import json
from pathlib import Path


CANONICAL_FIELDS = [
    {
        "canonical_field": "canonical_pitch_type",
        "description": "normalized pitch label",
    },
    {
        "canonical_field": "canonical_usage_pct",
        "description": "normalized usage rate",
    },
    {
        "canonical_field": "canonical_whiff_pct",
        "description": "normalized whiff rate",
    },
    {
        "canonical_field": "canonical_strikeout_pct",
        "description": "normalized strikeout metric",
    },
    {
        "canonical_field": "canonical_rv_per_100",
        "description": "normalized run value metric",
    },
    {
        "canonical_field": "canonical_xwoba",
        "description": "normalized contact quality metric",
    },
    {
        "canonical_field": "canonical_xba",
        "description": "normalized batting average quality",
    },
    {
        "canonical_field": "canonical_hard_hit_pct",
        "description": "normalized hard-hit metric",
    },
    {
        "canonical_field": "canonical_pitch_mix_weight",
        "description": "derived usage-based pitch mix proxy",
    },
    {
        "canonical_field": "canonical_damage_proxy",
        "description": "derived damage realism proxy",
    },
]


SOURCE_MAPPINGS = [
    {
        "source_pattern": "home_pitch_arsenal.<pitch>.<metric>",
        "target": "canonical arsenal fields",
        "role": "home starter",
    },
    {
        "source_pattern": "away_pitch_arsenal.<pitch>.<metric>",
        "target": "canonical arsenal fields",
        "role": "away starter",
    },
    {
        "source_pattern": "home_pitcher_features.<metric>",
        "target": "aggregate pitcher features",
        "role": "home starter",
    },
    {
        "source_pattern": "away_pitcher_features.<metric>",
        "target": "aggregate pitcher features",
        "role": "away starter",
    },
]


VALIDATION_RULES = [
    {
        "shadow_composite": "pit_k_shadow",
        "required_fields": "whiff_pct;usage_pct;strikeout_pct",
        "minimum_numeric_rate": 0.70,
    },
    {
        "shadow_composite": "pit_xwoba_shadow",
        "required_fields": "xwoba;rv_per_100",
        "minimum_numeric_rate": 0.70,
    },
    {
        "shadow_composite": "pit_hard_hit_shadow",
        "required_fields": "hard_hit_pct OR xwoba proxy",
        "minimum_numeric_rate": 0.70,
    },
    {
        "shadow_composite": "pit_hr_shadow",
        "required_fields": "damage_proxy",
        "minimum_numeric_rate": 0.70,
    },
]


def write_csv(path, rows):
    if not rows:
        path.write_text("")
        return

    fields = sorted({k for row in rows for k in row.keys()})

    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def main():
    payload = {
        "diagnosis": "arsenal_schema_normalization_design_ready",
        "canonical_fields": CANONICAL_FIELDS,
        "source_mappings": SOURCE_MAPPINGS,
        "validation_rules": VALIDATION_RULES,
        "design_constraints": [
            "no production simulation mutation",
            "no realism activation",
            "no shadow composite integration",
            "no edge detection",
        ],
    }

    out_dir = Path("tmp")
    out_dir.mkdir(exist_ok=True)

    json_path = out_dir / "arsenal_schema_normalization_design.json"
    canonical_csv = out_dir / "arsenal_schema_canonical_fields.csv"
    mappings_csv = out_dir / "arsenal_schema_source_mappings.csv"
    validation_csv = out_dir / "arsenal_schema_validation_rules.csv"

    json_path.write_text(json.dumps(payload, indent=2))

    write_csv(canonical_csv, CANONICAL_FIELDS)
    write_csv(mappings_csv, SOURCE_MAPPINGS)
    write_csv(validation_csv, VALIDATION_RULES)

    print(json.dumps(payload, indent=2))
    print(f"Wrote {json_path}")
    print(f"Wrote {canonical_csv}")
    print(f"Wrote {mappings_csv}")
    print(f"Wrote {validation_csv}")


if __name__ == "__main__":
    main()
