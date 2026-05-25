from __future__ import annotations

import csv
import json
from pathlib import Path


def write_csv(path, rows):

    if not rows:
        path.write_text('')
        return

    fields = sorted(
        {
            key
            for row in rows
            for key in row.keys()
        }
    )

    with path.open(
        'w',
        newline='',
    ) as handle:

        writer = csv.DictWriter(
            handle,
            fieldnames=fields,
        )

        writer.writeheader()
        writer.writerows(rows)


def main():

    candidate_mappings = [
        {
            'dormant_field': 'whiff_pct',
            'candidate_composite': 'pit_k',
            'double_count_risk': 'very_high',
            'recommended_strategy': 'shrinkage_residual_only',
        },
        {
            'dormant_field': 'rv_per_100',
            'candidate_composite': 'pit_xwoba',
            'double_count_risk': 'high',
            'recommended_strategy': 'residual_adjustment',
        },
        {
            'dormant_field': 'pitch_mix',
            'candidate_composite': 'pit_hard_hit',
            'double_count_risk': 'moderate',
            'recommended_strategy': 'bounded_blending',
        },
    ]

    guardrails = [
        {
            'guardrail': 'research_flag_required',
            'description': 'Arsenal realism disabled by default.',
        },
        {
            'guardrail': 'no_probability_overrides',
            'description': 'No late-stage probability mutation.',
        },
        {
            'guardrail': 'mandatory_recalibration',
            'description': 'Layer 4 and 5 recalibration required.',
        },
    ]

    payload = {
        'diagnosis':
            'arsenal_realism_design_ready_for_research_phase',
        'recommended_insertion_layer':
            'composite_construction',
        'dormant_arsenal_fields': [
            'whiff_pct',
            'usage_pct',
            'rv_per_100',
            'arsenal',
            'pitch_mix',
        ],
    }

    out_dir = Path('tmp')
    out_dir.mkdir(exist_ok=True)

    json_path = (
        out_dir
        / 'arsenal_composite_design_summary.json'
    )

    mappings_csv = (
        out_dir
        / 'arsenal_composite_candidate_mappings.csv'
    )

    guardrails_csv = (
        out_dir
        / 'arsenal_realism_guardrails.csv'
    )

    json_path.write_text(
        json.dumps(
            payload,
            indent=2,
        )
    )

    write_csv(
        mappings_csv,
        candidate_mappings,
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

    print(f'Wrote {json_path}')
    print(f'Wrote {mappings_csv}')
    print(f'Wrote {guardrails_csv}')


if __name__ == '__main__':
    main()
