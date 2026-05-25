from __future__ import annotations

import csv
import json
from pathlib import Path


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


def main():

    formulas = [
        {
            "shadow_composite": "pit_k_shadow",
            "candidate_inputs": "whiff_pct,usage_pct",
            "prototype_strategy": "shrinkage_residual_only",
            "risk_level": "very_high",
        },
        {
            "shadow_composite": "pit_xwoba_shadow",
            "candidate_inputs": "rv_per_100",
            "prototype_strategy": "residual_blend",
            "risk_level": "high",
        },
        {
            "shadow_composite": "pit_hard_hit_shadow",
            "candidate_inputs": "pitch_mix",
            "prototype_strategy": "bounded_blend",
            "risk_level": "moderate",
        },
        {
            "shadow_composite": "pit_hr_shadow",
            "candidate_inputs": "barrel_suppression",
            "prototype_strategy": "bounded_blend",
            "risk_level": "moderate",
        },
    ]

    guardrails = [
        {
            "guardrail":
                "research_only",
            "description":
                "Shadow composites are not production composites.",
        },
        {
            "guardrail":
                "no_build_pa_model_mutation",
            "description":
                "No runtime probability mutation allowed.",
        },
        {
            "guardrail":
                "double_count_protection",
            "description":
                "Use shrinkage and residualization only.",
        },
        {
            "guardrail":
                "future_recalibration_required",
            "description":
                "Layer 4 and 5 recalibration required before activation.",
        },
    ]

    payload = {
        "diagnosis":
            "shadow_composite_designs_ready_for_research_only_phase",
        "shadow_composites": [
            "pit_k_shadow",
            "pit_xwoba_shadow",
            "pit_hard_hit_shadow",
            "pit_hr_shadow",
        ],
        "dormant_arsenal_fields": [
            "whiff_pct",
            "usage_pct",
            "rv_per_100",
            "arsenal",
            "pitch_mix",
        ],
        "recommended_future_layer":
            "layer_8_pitch_arsenal_matchup_realism",
    }

    out_dir = Path("tmp")
    out_dir.mkdir(exist_ok=True)

    json_path = (
        out_dir
        / "shadow_pitcher_composite_designs.json"
    )

    formulas_csv = (
        out_dir
        / "shadow_pitcher_composite_formulas.csv"
    )

    guardrails_csv = (
        out_dir
        / "shadow_pitcher_composite_guardrails.csv"
    )

    json_path.write_text(
        json.dumps(
            payload,
            indent=2,
        )
    )

    write_csv(
        formulas_csv,
        formulas,
    )

    write_csv(
        guardrails_csv,
        guardrails,
    )

    print(
        json.dumps(
            payload,
            indent=2,
        )
    )

    print(f"Wrote {json_path}")
    print(f"Wrote {formulas_csv}")
    print(f"Wrote {guardrails_csv}")


if __name__ == "__main__":
    main()
