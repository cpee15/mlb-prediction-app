from __future__ import annotations

import ast
import csv
import json
from pathlib import Path
from typing import Dict, List


SEARCH_TERMS = [
    "lineup_average_probabilities",
    "lineup_average_summary",
    "direct_inputs",
    "pitcher",
    "k_rate",
    "bb_rate",
    "xba_allowed",
    "xwoba_allowed",
    "hard_hit_rate_allowed",
    "whiff_pct",
    "strikeout_pct",
    "arsenal",
    "pitch_mix",
    "simulate_plate_appearance",
    "plate_appearance",
    "expected_runs",
]

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


CATEGORY_RULES = {
    "probability_generation": [
        "probability",
        "simulate_plate_appearance",
        "plate_appearance",
        "win_probability",
        "lineup_average_probabilities",
    ],
    "direct_input_assembly": [
        "direct_inputs",
        "assemble",
        "build_inputs",
    ],
    "reporting_only": [
        "summary",
        "report",
        "print",
        "export",
        "serialize",
    ],
    "simulation_loop": [
        "simulation",
        "inning",
        "game_loop",
        "run_game",
    ],
    "derived_output_aggregation": [
        "derived_outputs",
        "expected_runs",
        "aggregation",
    ],
}


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


def classify_reference(
    source_line: str,
):
    line_lower = source_line.lower()

    for category, terms in CATEGORY_RULES.items():
        if any(
            term.lower() in line_lower
            for term in terms
        ):
            return category

    return "unknown"


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


def main():
    references = []

    consumed_summary = {
        "probability_generation_terms": set(),
        "direct_input_terms": set(),
        "arsenal_terms": set(),
        "reporting_only_terms": set(),
    }

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

            matched_terms = [
                term
                for term in SEARCH_TERMS
                if term.lower() in line_lower
            ]

            if not matched_terms:
                continue

            category = classify_reference(
                line
            )

            context = None

            if tree:
                context = detect_context(
                    tree,
                    idx,
                )

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
                        "dependency_category":
                            category,
                    }
                )

                if (
                    category
                    == "probability_generation"
                ):
                    consumed_summary[
                        "probability_generation_terms"
                    ].add(term)

                if (
                    category
                    == "direct_input_assembly"
                ):
                    consumed_summary[
                        "direct_input_terms"
                    ].add(term)

                if (
                    "arsenal"
                    in term.lower()
                    or "whiff"
                    in term.lower()
                    or "strikeout"
                    in term.lower()
                ):
                    consumed_summary[
                        "arsenal_terms"
                    ].add(term)

                if (
                    category
                    == "reporting_only"
                ):
                    consumed_summary[
                        "reporting_only_terms"
                    ].add(term)

    refs_sorted = sorted(
        references,
        key=lambda r: (
            r["dependency_category"],
            r["matched_term"],
            r["file"],
            r["line_number"],
        ),
    )

    probability_terms = (
        consumed_summary[
            "probability_generation_terms"
        ]
    )

    arsenal_terms = (
        consumed_summary[
            "arsenal_terms"
        ]
    )

    if (
        arsenal_terms
        and not any(
            term in probability_terms
            for term in arsenal_terms
        )
    ):
        diagnosis = (
            "arsenal_fields_reporting_only"
        )

    elif any(
        term in probability_terms
        for term in arsenal_terms
    ):
        diagnosis = (
            "arsenal_fields_consumed_by_probability_generation"
        )

    elif probability_terms:
        diagnosis = (
            "pitcher_probability_dependencies_identified"
        )

    else:
        diagnosis = (
            "source_dependency_unclear_needs_manual_review"
        )

    summary_rows = []

    for category_name, terms in consumed_summary.items():
        for term in sorted(terms):
            summary_rows.append(
                {
                    "summary_category":
                        category_name,
                    "term":
                        term,
                }
            )

    payload = {
        "diagnosis": diagnosis,
        "files_scanned": len(files),
        "references_found": len(refs_sorted),
        "probability_generation_terms":
            sorted(
                list(
                    consumed_summary[
                        "probability_generation_terms"
                    ]
                )
            ),
        "arsenal_terms":
            sorted(
                list(
                    consumed_summary[
                        "arsenal_terms"
                    ]
                )
            ),
    }

    out_dir = Path("tmp")
    out_dir.mkdir(exist_ok=True)

    json_path = (
        out_dir
        / "pa_model_source_dependencies.json"
    )

    refs_csv = (
        out_dir
        / "pa_model_source_dependency_references.csv"
    )

    summary_csv = (
        out_dir
        / "pa_model_consumed_field_summary.csv"
    )

    json_path.write_text(
        json.dumps(
            payload,
            indent=2,
        )
    )

    if refs_sorted:
        fields = sorted(
            {
                key
                for row in refs_sorted
                for key in row.keys()
            }
        )

        with refs_csv.open(
            "w",
            newline="",
        ) as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=fields,
            )

            writer.writeheader()
            writer.writerows(refs_sorted)

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

    print(
        json.dumps(
            payload,
            indent=2,
        )
    )

    print(f"Wrote {json_path}")
    print(f"Wrote {refs_csv}")
    print(f"Wrote {summary_csv}")


if __name__ == "__main__":
    main()
