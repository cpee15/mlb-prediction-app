from __future__ import annotations

import ast
import csv
import json
from pathlib import Path


TARGET_FILE = Path(
    "mlb_app/simulation/game_engine_v2.py"
)

TARGET_FUNCTION = "_build_pa_model"

PITCHER_COMPOSITES = [
    "pit_k",
    "pit_bb",
    "pit_hr",
    "pit_xba",
    "pit_xwoba",
    "pit_hard_hit",
]

OFFENSIVE_COMPOSITES = [
    "off_k",
    "off_bb",
    "off_iso",
    "off_slg",
    "off_avg",
]

ENVIRONMENT_COMPOSITES = [
    "run_env",
    "hit_env",
    "hr_env",
]

PROBABILITY_VARIABLES = [
    "k_prob",
    "bb_prob",
    "hit_prob",
    "hr_prob",
    "single_prob",
    "double_prob",
    "triple_prob",
    "contact_out_prob",
    "probabilities",
    "summary",
]

ARSENAL_TERMS = [
    "arsenal",
    "whiff_pct",
    "usage_pct",
    "strikeout_pct",
    "rv_per_100",
    "pitch_mix",
]

ALL_TARGETS = (
    PITCHER_COMPOSITES
    + OFFENSIVE_COMPOSITES
    + ENVIRONMENT_COMPOSITES
    + PROBABILITY_VARIABLES
)


def get_source_segment(lines, start, end):
    return "\n".join(lines[start - 1:end])


def extract_names(node):

    names = set()

    for child in ast.walk(node):

        if isinstance(child, ast.Name):
            names.add(child.id)

    return sorted(names)


def extract_subscript_keys(node):

    keys = set()

    for child in ast.walk(node):

        if isinstance(child, ast.Subscript):

            try:

                slice_node = child.slice

                if isinstance(
                    slice_node,
                    ast.Constant,
                ):
                    val = slice_node.value

                    if isinstance(val, str):
                        keys.add(val)

                elif (
                    hasattr(ast, "Index")
                    and isinstance(
                        slice_node,
                        ast.Index,
                    )
                ):
                    val = slice_node.value

                    if isinstance(
                        val,
                        ast.Constant,
                    ):
                        key = val.value

                        if isinstance(
                            key,
                            str,
                        ):
                            keys.add(key)

            except Exception:
                pass

    return sorted(keys)


def extract_calls(node):

    calls = set()

    for child in ast.walk(node):

        if isinstance(child, ast.Call):

            try:

                if isinstance(
                    child.func,
                    ast.Name,
                ):
                    calls.add(child.func.id)

                elif isinstance(
                    child.func,
                    ast.Attribute,
                ):
                    calls.add(child.func.attr)

            except Exception:
                pass

    return sorted(calls)


def node_to_source(node, lines):

    try:
        start = node.lineno
        end = getattr(
            node,
            "end_lineno",
            start,
        )

        return get_source_segment(
            lines,
            start,
            end,
        ).strip()

    except Exception:
        return ""


def find_target_function(tree):

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


def build_edges(
    assignment_rows,
):
    edges = []

    for row in assignment_rows:

        target = row["assignment_target"]

        for source in (
            row["referenced_variables"]
            .split("|")
        ):
            if source:

                edges.append(
                    {
                        "edge_type":
                            "source_to_composite",
                        "source":
                            source,
                        "target":
                            target,
                    }
                )

        if target in PROBABILITY_VARIABLES:

            for composite in (
                PITCHER_COMPOSITES
                + OFFENSIVE_COMPOSITES
                + ENVIRONMENT_COMPOSITES
            ):

                if (
                    composite
                    in row[
                        "referenced_variables"
                    ]
                ):
                    edges.append(
                        {
                            "edge_type":
                                "composite_to_probability",
                            "source":
                                composite,
                            "target":
                                target,
                        }
                    )

    return edges


def main():

    assert TARGET_FILE.exists(), TARGET_FILE

    text = TARGET_FILE.read_text()

    lines = text.splitlines()

    tree = ast.parse(text)

    fn = find_target_function(tree)

    assert fn is not None

    assignment_rows = []

    arsenal_hits = []

    probability_inputs = set()

    for node in ast.walk(fn):

        if not isinstance(node, ast.Assign):
            continue

        rhs_source = node_to_source(
            node.value,
            lines,
        )

        referenced_names = (
            extract_names(node.value)
        )

        referenced_keys = (
            extract_subscript_keys(
                node.value
            )
        )

        helper_calls = extract_calls(
            node.value
        )

        for target in node.targets:

            if not isinstance(
                target,
                ast.Name,
            ):
                continue

            target_name = target.id

            if (
                target_name
                not in ALL_TARGETS
            ):
                continue

            row = {
                "line_number":
                    node.lineno,
                "assignment_target":
                    target_name,
                "rhs_expression":
                    rhs_source,
                "referenced_variables":
                    "|".join(
                        sorted(
                            referenced_names
                        )
                    ),
                "referenced_subscript_keys":
                    "|".join(
                        sorted(
                            referenced_keys
                        )
                    ),
                "called_helper_functions":
                    "|".join(
                        sorted(
                            helper_calls
                        )
                    ),
            }

            assignment_rows.append(
                row
            )

            if (
                target_name
                in PROBABILITY_VARIABLES
            ):
                for composite in (
                    PITCHER_COMPOSITES
                    + OFFENSIVE_COMPOSITES
                    + ENVIRONMENT_COMPOSITES
                ):
                    if (
                        composite
                        in referenced_names
                    ):
                        probability_inputs.add(
                            composite
                        )

            rhs_lower = rhs_source.lower()

            for term in ARSENAL_TERMS:
                if term in rhs_lower:

                    arsenal_hits.append(
                        {
                            "term":
                                term,
                            "target":
                                target_name,
                            "line":
                                node.lineno,
                        }
                    )

    edges = build_edges(
        assignment_rows
    )

    recommendations = []

    if probability_inputs:

        recommendations.append(
            {
                "recommendation":
                    (
                        "Pitcher composites appear "
                        "to feed probability "
                        "variables directly."
                    )
            }
        )

    if not arsenal_hits:

        recommendations.append(
            {
                "recommendation":
                    (
                        "Arsenal fields appear "
                        "absent from composite "
                        "construction. "
                        "Layer 5C-H should design "
                        "audit-gated arsenal-to-"
                        "composite blending."
                    )
            }
        )

    if (
        probability_inputs
        and not arsenal_hits
    ):
        diagnosis = [
            "pitcher_composites_confirmed_probability_inputs",
            "arsenal_fields_absent_from_composites",
        ]

    elif not probability_inputs:

        diagnosis = [
            "pitcher_composites_exist_but_not_used"
        ]

    else:

        diagnosis = [
            "composite_dependency_unclear_needs_manual_review"
        ]

    payload = {
        "diagnosis": diagnosis,
        "target_function":
            TARGET_FUNCTION,
        "file":
            str(TARGET_FILE),
        "composite_assignments_found":
            len(assignment_rows),
        "probability_input_composites":
            sorted(
                list(
                    probability_inputs
                )
            ),
        "arsenal_hits":
            arsenal_hits,
    }

    out_dir = Path("tmp")
    out_dir.mkdir(exist_ok=True)

    json_path = (
        out_dir
        / "build_pa_model_composite_trace.json"
    )

    assignments_csv = (
        out_dir
        / "build_pa_model_composite_assignments.csv"
    )

    edges_csv = (
        out_dir
        / "build_pa_model_composite_edges.csv"
    )

    recommendations_csv = (
        out_dir
        / "build_pa_model_composite_recommendations.csv"
    )

    json_path.write_text(
        json.dumps(
            payload,
            indent=2,
        )
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

    write_csv(
        assignments_csv,
        assignment_rows,
    )

    write_csv(
        edges_csv,
        edges,
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
    print(f"Wrote {assignments_csv}")
    print(f"Wrote {edges_csv}")
    print(f"Wrote {recommendations_csv}")


if __name__ == "__main__":
    main()
