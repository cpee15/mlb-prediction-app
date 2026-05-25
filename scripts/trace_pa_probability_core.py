from __future__ import annotations

import ast
import csv
import json
from collections import defaultdict
from pathlib import Path


SEARCH_TERMS = [
    "lineup_average_probabilities",
    "lineup_average_summary",
    "simulate_plate_appearance",
    "plate_appearance",
    "direct_inputs",
    "probabilities",
    "outcome_probabilities",
    "strikeout",
    "walk",
    "single",
    "double",
    "home_run",
    "expected_runs",
]

TARGET_GLOBS = [
    "mlb_app/**/*.py",
]

PROBABILITY_HINT_TERMS = [
    "probabilities",
    "probability",
    "plate_appearance",
    "lineup_average_probabilities",
    "outcome",
    "strikeout",
    "walk",
    "single",
    "double",
    "home_run",
]


def discover_files():
    paths = set()

    for pattern in TARGET_GLOBS:
        for path in Path(".").glob(pattern):
            if path.is_file():
                paths.add(path)

    return sorted(paths)


def get_source_segment(lines, start, end):
    return "\n".join(lines[start - 1:end])


class FunctionVisitor(ast.NodeVisitor):

    def __init__(self, file_path, lines):
        self.file_path = file_path
        self.lines = lines

        self.functions = []
        self.edges = []

    def visit_FunctionDef(self, node):
        self.process_function(node)
        self.generic_visit(node)

    def visit_AsyncFunctionDef(self, node):
        self.process_function(node)
        self.generic_visit(node)

    def process_function(self, node):

        start = node.lineno
        end = getattr(node, "end_lineno", start)

        source = get_source_segment(
            self.lines,
            start,
            end,
        )

        source_lower = source.lower()

        matched_terms = sorted(
            {
                term
                for term in SEARCH_TERMS
                if term.lower() in source_lower
            }
        )

        referenced_keys = set()
        called_functions = set()
        assigned_outputs = set()

        for child in ast.walk(node):

            if isinstance(child, ast.Subscript):

                try:
                    slice_node = child.slice

                    if isinstance(slice_node, ast.Constant):
                        key = slice_node.value

                        if isinstance(key, str):
                            referenced_keys.add(key)

                    elif hasattr(ast, "Index") and isinstance(slice_node, ast.Index):
                        val = slice_node.value

                        if isinstance(val, ast.Constant):
                            key = val.value

                            if isinstance(key, str):
                                referenced_keys.add(key)

                except Exception:
                    pass

            elif isinstance(child, ast.Call):

                try:
                    if isinstance(child.func, ast.Name):
                        called_functions.add(child.func.id)

                    elif isinstance(child.func, ast.Attribute):
                        called_functions.add(child.func.attr)

                except Exception:
                    pass

            elif isinstance(child, ast.Assign):

                for target in child.targets:

                    try:
                        if isinstance(target, ast.Name):
                            assigned_outputs.add(target.id)

                        elif isinstance(target, ast.Subscript):

                            slice_node = target.slice

                            if isinstance(slice_node, ast.Constant):
                                val = slice_node.value

                                if isinstance(val, str):
                                    assigned_outputs.add(val)

                    except Exception:
                        pass

        probability_score = 0

        for term in PROBABILITY_HINT_TERMS:
            if term in source_lower:
                probability_score += 1

        if "direct_inputs" in source_lower:
            probability_score += 2

        if "lineup_average_probabilities" in source_lower:
            probability_score += 3

        function_row = {
            "file": str(self.file_path),
            "function_name": node.name,
            "start_line": start,
            "end_line": end,
            "matched_terms": "|".join(matched_terms),
            "referenced_keys": "|".join(sorted(referenced_keys)),
            "called_functions": "|".join(sorted(called_functions)),
            "assigned_outputs": "|".join(sorted(assigned_outputs)),
            "probability_score": probability_score,
        }

        self.functions.append(function_row)

        for callee in called_functions:
            self.edges.append(
                {
                    "edge_type": "caller_to_callee",
                    "source": node.name,
                    "target": callee,
                }
            )

        for key in referenced_keys:
            self.edges.append(
                {
                    "edge_type": "function_to_field",
                    "source": node.name,
                    "target": key,
                }
            )

        for output in assigned_outputs:
            self.edges.append(
                {
                    "edge_type": "function_to_output",
                    "source": node.name,
                    "target": output,
                }
            )


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

    with path.open("w", newline="") as handle:

        writer = csv.DictWriter(
            handle,
            fieldnames=fields,
        )

        writer.writeheader()
        writer.writerows(rows)


def classify_pitcher_consumption(functions):

    high_score_functions = [
        f
        for f in functions
        if f["probability_score"] >= 4
    ]

    direct_pitcher_hits = []

    indirect_pitcher_hits = []

    for row in high_score_functions:

        refs = row["referenced_keys"]

        if any(
            key in refs
            for key in [
                "pitcher",
                "k_rate",
                "bb_rate",
                "xwoba_allowed",
                "xba_allowed",
                "hard_hit_rate_allowed",
            ]
        ):
            direct_pitcher_hits.append(row)

        elif "probabilities" in row["matched_terms"]:
            indirect_pitcher_hits.append(row)

    if direct_pitcher_hits:
        return (
            "probability_core_identified_pitcher_directly_consumed",
            direct_pitcher_hits,
        )

    if indirect_pitcher_hits:
        return (
            "probability_core_identified_pitcher_indirectly_consumed",
            indirect_pitcher_hits,
        )

    if high_score_functions:
        return (
            "probability_core_identified_pitcher_not_consumed",
            high_score_functions,
        )

    return (
        "probability_core_unclear_needs_manual_review",
        [],
    )


def build_recommendations(diagnosis):

    rows = []

    if diagnosis == "probability_core_identified_pitcher_not_consumed":

        rows.append(
            {
                "recommendation":
                    (
                        "Proceed to Layer 5C-G "
                        "audit-gated realism "
                        "activation design."
                    )
            }
        )

    elif diagnosis == "probability_core_identified_pitcher_indirectly_consumed":

        rows.append(
            {
                "recommendation":
                    (
                        "Trace upstream composite "
                        "construction before any "
                        "realism insertion proposal."
                    )
            }
        )

    elif diagnosis == "probability_core_identified_pitcher_directly_consumed":

        rows.append(
            {
                "recommendation":
                    (
                        "Use consumed-field-only "
                        "mutation audits before "
                        "arsenal integration."
                    )
            }
        )

    else:

        rows.append(
            {
                "recommendation":
                    (
                        "Manual inspection required "
                        "before realism design."
                    )
            }
        )

    return rows


def main():

    all_functions = []
    all_edges = []

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
            continue

        visitor = FunctionVisitor(
            file_path=file_path,
            lines=lines,
        )

        visitor.visit(tree)

        all_functions.extend(visitor.functions)
        all_edges.extend(visitor.edges)

    diagnosis, candidate_functions = (
        classify_pitcher_consumption(
            all_functions
        )
    )

    top_functions = sorted(
        all_functions,
        key=lambda x: x["probability_score"],
        reverse=True,
    )[:15]

    recommendations = build_recommendations(
        diagnosis
    )

    payload = {
        "diagnosis": diagnosis,
        "files_scanned": len(files),
        "functions_found": len(all_functions),
        "edges_found": len(all_edges),
        "top_probability_functions": top_functions,
    }

    out_dir = Path("tmp")
    out_dir.mkdir(exist_ok=True)

    json_path = (
        out_dir
        / "pa_probability_core_trace.json"
    )

    functions_csv = (
        out_dir
        / "pa_probability_core_functions.csv"
    )

    edges_csv = (
        out_dir
        / "pa_probability_core_edges.csv"
    )

    recommendations_csv = (
        out_dir
        / "pa_probability_core_recommendations.csv"
    )

    json_path.write_text(
        json.dumps(
            payload,
            indent=2,
        )
    )

    write_csv(
        functions_csv,
        all_functions,
    )

    write_csv(
        edges_csv,
        all_edges,
    )

    write_csv(
        recommendations_csv,
        recommendations,
    )

    print(
        json.dumps(
            payload,
            indent=2,
        )
    )

    print(f"Wrote {json_path}")
    print(f"Wrote {functions_csv}")
    print(f"Wrote {edges_csv}")
    print(f"Wrote {recommendations_csv}")


if __name__ == "__main__":
    main()
