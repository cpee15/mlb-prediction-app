from __future__ import annotations

import ast
import csv
import json
from pathlib import Path


TARGET_FILE = Path(
    "mlb_app/simulation/game_engine_v2.py"
)

TARGET_FUNCTION = "_build_pa_model"

COMPOSITE_TERMS = [
    "off_k",
    "pit_k",
    "pit_bb",
    "pit_hr",
    "pit_xba",
    "pit_xwoba",
    "pit_hard_hit",
]

PROBABILITY_TERMS = [
    "k_prob",
    "bb_prob",
    "hit_prob",
    "hr_prob",
    "single_prob",
    "double_prob",
    "triple_prob",
    "contact_out_prob",
]

OUTPUT_TERMS = [
    "probabilities",
    "summary",
]

ARSENAL_TERMS = [
    "strikeout_pct",
    "arsenal",
    "pitch_mix",
    "whiff_pct",
    "usage_pct",
    "rv_per_100",
]

WINDOW_RADIUS = 5


def numbered_lines(lines, start, end):

    out = []

    for i in range(start, end + 1):

        if i < 1 or i > len(lines):
            continue

        out.append(
            f"{i:04d}: {lines[i - 1]}"
        )

    return "\n".join(out)


def find_function(tree):

    for node in ast.walk(tree):

        if isinstance(
            node,
            (
                ast.FunctionDef,
                ast.AsyncFunctionDef,
            ),
        ):
            if node.name == TARGET_FUNCTION:
                return node

    return None


def find_term_windows(
    lines,
    fn_start,
    fn_end,
    term,
):

    windows = []

    for idx in range(
        fn_start - 1,
        fn_end,
    ):

        line = lines[idx]

        if term in line:

            start = max(
                1,
                idx + 1 - WINDOW_RADIUS,
            )

            end = min(
                len(lines),
                idx + 1 + WINDOW_RADIUS,
            )

            windows.append(
                {
                    "term": term,
                    "line": idx + 1,
                    "excerpt":
                        numbered_lines(
                            lines,
                            start,
                            end,
                        ),
                }
            )

    return windows


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

    if not TARGET_FILE.exists():

        payload = {
            "diagnosis":
                "build_pa_model_function_not_found"
        }

        print(
            json.dumps(
                payload,
                indent=2,
            )
        )

        return

    text = TARGET_FILE.read_text()

    lines = text.splitlines()

    tree = ast.parse(text)

    fn = find_function(tree)

    if fn is None:

        payload = {
            "diagnosis":
                "build_pa_model_function_not_found"
        }

        print(
            json.dumps(
                payload,
                indent=2,
            )
        )

        return

    fn_start = fn.lineno
    fn_end = getattr(
        fn,
        "end_lineno",
        fn.lineno,
    )

    full_excerpt = numbered_lines(
        lines,
        fn_start,
        fn_end,
    )

    windows = []

    for term in (
        COMPOSITE_TERMS
        + PROBABILITY_TERMS
        + OUTPUT_TERMS
        + ARSENAL_TERMS
    ):

        windows.extend(
            find_term_windows(
                lines,
                fn_start,
                fn_end,
                term,
            )
        )

    arsenal_hits = [
        w
        for w in windows
        if w["term"]
        in ARSENAL_TERMS
    ]

    md_lines = []

    md_lines.append(
        "# _build_pa_model Source Excerpt Audit"
    )

    md_lines.append("")

    md_lines.append(
        "## Diagnosis"
    )

    md_lines.append("")

    md_lines.append(
        "`source_excerpt_ready_for_manual_review`"
    )

    md_lines.append("")

    md_lines.append(
        "## Full Function Excerpt"
    )

    md_lines.append("")

    md_lines.append("```python")
    md_lines.append(full_excerpt)
    md_lines.append("```")
    md_lines.append("")

    md_lines.append(
        "## Focused Composite Windows"
    )

    md_lines.append("")

    for row in windows:

        if (
            row["term"]
            not in COMPOSITE_TERMS
        ):
            continue

        md_lines.append(
            f"### {row['term']} "
            f"(line {row['line']})"
        )

        md_lines.append("")

        md_lines.append("```python")
        md_lines.append(
            row["excerpt"]
        )
        md_lines.append("```")
        md_lines.append("")

    md_lines.append(
        "## Focused Probability Windows"
    )

    md_lines.append("")

    for row in windows:

        if (
            row["term"]
            not in PROBABILITY_TERMS
        ):
            continue

        md_lines.append(
            f"### {row['term']} "
            f"(line {row['line']})"
        )

        md_lines.append("")

        md_lines.append("```python")
        md_lines.append(
            row["excerpt"]
        )
        md_lines.append("```")
        md_lines.append("")

    md_lines.append(
        "## Output Construction Windows"
    )

    md_lines.append("")

    for row in windows:

        if (
            row["term"]
            not in OUTPUT_TERMS
        ):
            continue

        md_lines.append(
            f"### {row['term']} "
            f"(line {row['line']})"
        )

        md_lines.append("")

        md_lines.append("```python")
        md_lines.append(
            row["excerpt"]
        )
        md_lines.append("```")
        md_lines.append("")

    md_lines.append(
        "## Arsenal / Strikeout Context"
    )

    md_lines.append("")

    if arsenal_hits:

        for row in arsenal_hits:

            md_lines.append(
                f"### {row['term']} "
                f"(line {row['line']})"
            )

            md_lines.append("")

            md_lines.append("```python")
            md_lines.append(
                row["excerpt"]
            )
            md_lines.append("```")
            md_lines.append("")

    else:

        md_lines.append(
            "No arsenal-related terms found."
        )

    md_lines.append("")

    md_lines.append(
        "## Manual Review Questions"
    )

    md_lines.append("")

    questions = [
        (
            "Is `strikeout_pct` sourced "
            "from generic pitcher/offense "
            "profiles or arsenal/pitch-mix data?"
        ),
        (
            "Does `pit_k` directly "
            "influence `k_prob`?"
        ),
        (
            "Do `pit_xba`, `pit_xwoba`, "
            "and `pit_hard_hit` influence "
            "actual hit/contact probabilities "
            "or only summaries?"
        ),
        (
            "Where could `whiff_pct`, "
            "`usage_pct`, or `rv_per_100` "
            "safely blend into composites?"
        ),
        (
            "What guardrails are needed "
            "to prevent double counting "
            "with existing `pit_k` logic?"
        ),
    ]

    for q in questions:

        md_lines.append(
            f"- {q}"
        )

    md_text = "\n".join(
        md_lines
    )

    payload = {
        "diagnosis":
            "source_excerpt_ready_for_manual_review",
        "target_function":
            TARGET_FUNCTION,
        "file":
            str(TARGET_FILE),
        "function_start_line":
            fn_start,
        "function_end_line":
            fn_end,
        "windows_found":
            len(windows),
        "arsenal_hits_found":
            len(arsenal_hits),
    }

    out_dir = Path("tmp")
    out_dir.mkdir(exist_ok=True)

    md_path = (
        out_dir
        / "build_pa_model_source_excerpt.md"
    )

    json_path = (
        out_dir
        / "build_pa_model_source_excerpt.json"
    )

    csv_path = (
        out_dir
        / "build_pa_model_source_windows.csv"
    )

    md_path.write_text(
        md_text
    )

    json_path.write_text(
        json.dumps(
            payload,
            indent=2,
        )
    )

    write_csv(
        csv_path,
        windows,
    )

    print(
        json.dumps(
            payload,
            indent=2,
        )
    )

    print(f"Wrote {md_path}")
    print(f"Wrote {json_path}")
    print(f"Wrote {csv_path}")


if __name__ == "__main__":
    main()
