from __future__ import annotations

import ast
import csv
import json
from pathlib import Path
from typing import Dict, List


TARGET_FILES = [
    "mlb_app/simulation/game_engine_v2.py",
    "mlb_app/matchup_generator.py",
    "mlb_app/lineup_profile.py",
]

AUTO_GLOBS = [
    "mlb_app/**/*pa*.py",
    "mlb_app/**/*prob*.py",
    "mlb_app/**/*simulation*.py",
    "mlb_app/**/*matchup*.py",
]

CONSUMED_PITCHER_TERMS = [
    "pitcher",
    "k_rate",
    "bb_rate",
    "xba_allowed",
    "xwoba_allowed",
    "hard_hit_rate_allowed",
]

ARSENAL_TERMS = [
    "arsenal",
    "pitch_mix",
    "whiff_pct",
    "strikeout_pct",
    "usage_pct",
    "rv_per_100",
]

PROBABILITY_TERMS = [
    "lineup_average_probabilities",
    "simulate_plate_appearance",
    "plate_appearance",
    "expected_runs",
    "win_probability",
]

DESIGN_RECOMMENDATIONS = [
    {
        "candidate_insertion_point":
            "simulate_plate_appearance",
        "candidate_adjustment_target":
            "strikeout_probability",
        "design_note":
            (
                "Allow arsenal whiff/strikeout "
                "metrics to softly influence "
                "existing strikeout probability "
                "rather than replacing baseline math."
            ),
        "guardrail":
            (
                "Clamp total strikeout adjustment "
                "magnitude to prevent calibration drift."
            ),
    },
    {
        "candidate_insertion_point":
            "lineup_average_probabilities",
        "candidate_adjustment_target":
            "contact_quality",
        "design_note":
            (
                "Blend arsenal xwOBA and hard-hit "
                "signals into contact-quality "
                "distribution after existing "
                "pitcher quality calculations."
            ),
        "guardrail":
            (
                "Require full Layer 4/5 "
                "recalibration before production use."
            ),
    },
    {
        "candidate_insertion_point":
            "derived_outputs aggregation",
        "candidate_adjustment_target":
            "run_environment",
        "design_note":
            (
                "Use arsenal realism only as "
                "secondary environmental weighting "
                "until scalar calibration is proven."
            ),
        "guardrail":
            (
                "Research-flag only. "
                "No production activation."
            ),
    },
]


def discover_files():
    discovered = set()

    for path in TARGET_FILES:
        p = Path(path)

        if p.exists():
            discovered.add(p)

    for pattern in AUTO_GLOBS:
        for path in Path(".").glob(pattern):
            if path.is_file():
                discovered.add(path)

    return sorted(discovered)


def detect_context(
    tree,
    line_number,
):
    context = None

    for node in ast.walk(tree):
        if isinstance(
            node,
            (
                ast.FunctionDef,
                ast.AsyncFunctionDef,
                ast.ClassDef,
            ),
        ):
            if (
                hasattr(node, "lineno")
                and hasattr(node, "end_lineno")
            ):
                if (
                    node.lineno
                    <= line_number
                    <= node.end_lineno
                ):
                    context = getattr(
                        node,
                        "name",
                        None,
                    )

    return context


def line_contains_any(
    line: str,
    terms: List[str],
):
    line_lower = line.lower()

    return any(
        term.lower() in line_lower
        for term in terms
    )


def main():
    consumed_fields = set()
    reporting_pitcher_fields = set()
    reporting_arsenal_fields = set()
    dormant_arsenal_fields = set()

    references = []

    files = discover_files()

    for file_path in files:
        try:
            text = file_path.read_text()

        except Exception:
            continue

        lines = text.splitlines()

        try:
            tree = ast.parse(text)

        except Exception:
            tree = None

        for idx, line in enumerate(
            lines,
            start=1,
        ):
            line_lower = line.lower()

            context = None

            if tree:
                context = detect_context(
                    tree,
                    idx,
                )

            probability_related = (
                line_contains_any(
                    line,
                    PROBABILITY_TERMS,
                )
            )

            pitcher_related = (
                line_contains_any(
                    line,
                    CONSUMED_PITCHER_TERMS,
                )
            )

            arsenal_related = (
                line_contains_any(
                    line,
                    ARSENAL_TERMS,
                )
            )

            if (
                probability_related
                and pitcher_related
            ):
                for term in CONSUMED_PITCHER_TERMS:
                    if term.lower() in line_lower:
                        consumed_fields.add(term)

            if (
                pitcher_related
                and not probability_related
            ):
                for term in CONSUMED_PITCHER_TERMS:
                    if term.lower() in line_lower:
                        reporting_pitcher_fields.add(
                            term
                        )

            if arsenal_related:
                for term in ARSENAL_TERMS:
                    if term.lower() in line_lower:
                        reporting_arsenal_fields.add(
                            term
                        )

                        if not probability_related:
                            dormant_arsenal_fields.add(
                                term
                            )

            matched_terms = []

            for term in (
                CONSUMED_PITCHER_TERMS
                + ARSENAL_TERMS
                + PROBABILITY_TERMS
            ):
                if term.lower() in line_lower:
                    matched_terms.append(term)

            for term in matched_terms:
                references.append(
                    {
                        "file":
                            str(file_path),
                        "line_number":
                            idx,
                        "function_or_class_context":
                            context,
                        "matched_term":
                            term,
                        "source_line":
                            line.strip(),
                    }
                )

    summary_rows = []

    for field in sorted(consumed_fields):
        summary_rows.append(
            {
                "field":
                    field,
                "classification":
                    "currently_consumed_pitcher_field",
            }
        )

    for field in sorted(
        reporting_pitcher_fields
    ):
        summary_rows.append(
            {
                "field":
                    field,
                "classification":
                    "assembled_or_reporting_only_pitcher_field",
            }
        )

    for field in sorted(
        reporting_arsenal_fields
    ):
        summary_rows.append(
            {
                "field":
                    field,
                "classification":
                    "assembled_or_reporting_only_arsenal_field",
            }
        )

    for field in sorted(
        dormant_arsenal_fields
    ):
        summary_rows.append(
            {
                "field":
                    field,
                "classification":
                    "candidate_dormant_arsenal_field",
            }
        )

    if dormant_arsenal_fields:
        diagnosis = (
            "arsenal_fields_reporting_only"
        )

    elif consumed_fields:
        diagnosis = (
            "pitcher_probability_dependencies_identified"
        )

    else:
        diagnosis = (
            "source_dependency_unclear_needs_manual_review"
        )

    payload = {
        "diagnosis": diagnosis,
        "files_scanned": len(files),
        "currently_consumed_pitcher_fields":
            sorted(
                list(consumed_fields)
            ),
        "reporting_only_pitcher_fields":
            sorted(
                list(
                    reporting_pitcher_fields
                )
            ),
        "reporting_only_arsenal_fields":
            sorted(
                list(
                    reporting_arsenal_fields
                )
            ),
        "candidate_dormant_arsenal_fields":
            sorted(
                list(
                    dormant_arsenal_fields
                )
            ),
        "warnings": [
            (
                "No production engine behavior "
                "changes in this layer."
            ),
            (
                "Any future arsenal activation "
                "must remain behind research flags."
            ),
            (
                "Any realism activation requires "
                "full Layer 4/5 recalibration."
            ),
        ],
    }

    out_dir = Path("tmp")
    out_dir.mkdir(exist_ok=True)

    json_path = (
        out_dir
        / "consumed_pitcher_fields_and_arsenal_design.json"
    )

    summary_csv = (
        out_dir
        / "consumed_pitcher_fields_summary.csv"
    )

    design_csv = (
        out_dir
        / "arsenal_activation_design_recommendations.csv"
    )

    json_path.write_text(
        json.dumps(
            payload,
            indent=2,
        )
    )

    if summary_rows:
        fields = sorted(
            {
                key
                for row in summary_rows
                for key in row.keys()
            }
        )

        with summary_csv.open(
            "w",
            newline="",
        ) as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=fields,
            )

            writer.writeheader()
            writer.writerows(summary_rows)

    if DESIGN_RECOMMENDATIONS:
        fields = sorted(
            {
                key
                for row
                in DESIGN_RECOMMENDATIONS
                for key in row.keys()
            }
        )

        with design_csv.open(
            "w",
            newline="",
        ) as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=fields,
            )

            writer.writeheader()
            writer.writerows(
                DESIGN_RECOMMENDATIONS
            )

    print(
        json.dumps(
            payload,
            indent=2,
        )
    )

    print(f"Wrote {json_path}")
    print(f"Wrote {summary_csv}")
    print(f"Wrote {design_csv}")


if __name__ == "__main__":
    main()
