from __future__ import annotations

import ast
import csv
import inspect
import json
from pathlib import Path
from typing import Any, Dict, List

import mlb_app.simulation.inning_simulator as inning_simulator


OUTPUT_DIR = Path("tmp")
OUTPUT_DIR.mkdir(exist_ok=True)

ROOT = Path(".")


TARGET_FUNCTION = "simulate_half_inning"

TARGET_FILES = [
    "mlb_app/simulation/inning_simulator.py",
    "mlb_app/simulation/game_engine_v2.py",
    "mlb_app/simulation/game_simulator.py",
    "mlb_app/simulation/game_rules.py",
    "scripts/backtest_extras_walkoff_calibration.py",
    "scripts/backtest_extras_walkoff_hybrid_pairing.py",
]


def safe_write_json(path: Path, payload: Dict[str, Any]) -> None:
    with path.open("w") as handle:
        json.dump(payload, handle, indent=2)


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:

    if not rows:
        path.write_text("")
        return

    fieldnames = sorted(
        {
            key
            for row in rows
            for key in row.keys()
        }
    )

    with path.open("w", newline="") as handle:

        writer = csv.DictWriter(
            handle,
            fieldnames=fieldnames,
        )

        writer.writeheader()
        writer.writerows(rows)


def inspect_signature():

    func = getattr(
        inning_simulator,
        TARGET_FUNCTION,
    )

    signature = inspect.signature(func)

    return {
        "signature":
            str(signature),

        "parameters":
            [
                {
                    "name": param.name,
                    "kind": str(param.kind),
                    "default":
                        (
                            None
                            if param.default
                            is inspect._empty
                            else repr(param.default)
                        ),
                }
                for param
                in signature.parameters.values()
            ],
    }


def inspect_source_patterns():

    source = inspect.getsource(
        getattr(
            inning_simulator,
            TARGET_FUNCTION,
        )
    )

    findings = {
        "initializes_bases":
            (
                "bases ="
                in source
            ),

        "initializes_outs":
            (
                "outs ="
                in source
            ),

        "returns_state_payload":
            (
                "bases"
                in source
                and
                "return"
                in source
            ),

        "contains_runner_logic":
            (
                "runner"
                in source.lower()
                or
                "advance"
                in source.lower()
            ),

        "contains_double_play_logic":
            (
                "double"
                in source.lower()
            ),

        "contains_stolen_base_logic":
            (
                "steal"
                in source.lower()
            ),

        "contains_walkoff_logic":
            (
                "walkoff"
                in source.lower()
            ),
    }

    return findings


def inspect_callers():

    caller_rows = []

    total_calls = 0

    positional_calls = 0
    keyword_calls = 0

    for relative_path in TARGET_FILES:

        path = ROOT / relative_path

        if not path.exists():
            continue

        try:

            source = path.read_text()

            tree = ast.parse(source)

        except Exception as exc:

            caller_rows.append(
                {
                    "file":
                        relative_path,

                    "status":
                        "parse_failed",

                    "risk_level":
                        "moderate",

                    "notes":
                        str(exc),

                    "recommended_action":
                        "manual_review",
                }
            )

            continue

        for node in ast.walk(tree):

            if not isinstance(node, ast.Call):
                continue

            func_name = None

            if isinstance(node.func, ast.Name):
                func_name = node.func.id

            elif isinstance(node.func, ast.Attribute):
                func_name = node.func.attr

            if func_name != TARGET_FUNCTION:
                continue

            total_calls += 1

            has_keywords = len(node.keywords) > 0

            if has_keywords:
                keyword_calls += 1
            else:
                positional_calls += 1

            caller_rows.append(
                {
                    "file":
                        relative_path,

                    "status":
                        "caller_found",

                    "risk_level":
                        (
                            "low"
                            if has_keywords
                            else "moderate"
                        ),

                    "notes":
                        (
                            "keyword_call"
                            if has_keywords
                            else "positional_call"
                        ),

                    "recommended_action":
                        (
                            "safe_to_add_optional_kwargs"
                            if has_keywords
                            else "verify_positional_compatibility"
                        ),
                }
            )

    return {
        "rows":
            caller_rows,

        "summary":
            {
                "total_calls":
                    total_calls,

                "keyword_calls":
                    keyword_calls,

                "positional_calls":
                    positional_calls,
            },
    }


def evaluate_feasibility(
    signature_info,
    source_findings,
    caller_info,
):

    total_calls = (
        caller_info["summary"]["total_calls"]
    )

    positional_calls = (
        caller_info["summary"]["positional_calls"]
    )

    low_risk = (
        source_findings["initializes_bases"]
        and
        source_findings["initializes_outs"]
    )

    optional_kwargs_safe = (
        positional_calls == 0
    )

    if low_risk and optional_kwargs_safe:

        diagnosis = (
            "base_out_state_api_feasible_with_backward_compatible_defaults"
        )

        risk_level = "low"

    elif low_risk:

        diagnosis = (
            "base_out_state_api_requires_refactor"
        )

        risk_level = "moderate"

    else:

        diagnosis = (
            "base_out_state_api_high_risk"
        )

        risk_level = "high"

    unlocked_mechanics = [
        "ghost_runner",
        "double_plays",
        "runner_advancement",
        "steals",
        "caught_stealing",
        "sac_flies",
        "tagging_up",
        "wild_pitches",
        "passed_balls",
        "balks",
        "pinch_runners",
        "walkoff_state_inheritance",
    ]

    minimal_patch = {
        "proposed_signature":
            (
                "simulate_half_inning("
                "probabilities, "
                "rng=None, "
                "initial_bases=(False, False, False), "
                "initial_outs=0)"
            ),

        "recommended_initialization":
            [
                "bases = tuple(initial_bases)",
                "outs = int(initial_outs)",
            ],

        "compatibility_strategy":
            (
                "optional_kwargs_only"
            ),
    }

    return {
        "diagnosis":
            diagnosis,

        "risk_level":
            risk_level,

        "total_call_sites":
            total_calls,

        "positional_call_sites":
            positional_calls,

        "unlocked_mechanics":
            unlocked_mechanics,

        "minimal_future_patch":
            minimal_patch,
    }


signature_info = inspect_signature()

source_findings = inspect_source_patterns()

caller_info = inspect_callers()

feasibility = evaluate_feasibility(
    signature_info,
    source_findings,
    caller_info,
)


checklist_rows = [
    {
        "component":
            "simulate_half_inning_signature",

        "status":
            "inspected",

        "risk_level":
            feasibility["risk_level"],

        "notes":
            signature_info["signature"],

        "recommended_action":
            "add_optional_initial_state_kwargs",
    },

    {
        "component":
            "bases_initialization",

        "status":
            (
                "found"
                if source_findings["initializes_bases"]
                else "missing"
            ),

        "risk_level":
            "low",

        "notes":
            "bases initialized inside inning simulator",

        "recommended_action":
            "replace_with_initial_bases_default",
    },

    {
        "component":
            "outs_initialization",

        "status":
            (
                "found"
                if source_findings["initializes_outs"]
                else "missing"
            ),

        "risk_level":
            "low",

        "notes":
            "outs initialized inside inning simulator",

        "recommended_action":
            "replace_with_initial_outs_default",
    },

    {
        "component":
            "caller_compatibility",

        "status":
            "audited",

        "risk_level":
            feasibility["risk_level"],

        "notes":
            (
                f"total_calls="
                f"{caller_info['summary']['total_calls']}, "
                f"positional_calls="
                f"{caller_info['summary']['positional_calls']}"
            ),

        "recommended_action":
            "preserve_backward_compatible_defaults",
    },

    {
        "component":
            "future_mechanics",

        "status":
            "identified",

        "risk_level":
            "low",

        "notes":
            ", ".join(
                feasibility["unlocked_mechanics"]
            ),

        "recommended_action":
            "implement_only_after_api_patch",
    },
]


summary = {
    "diagnosis":
        feasibility["diagnosis"],

    "risk_level":
        feasibility["risk_level"],

    "signature":
        signature_info,

    "source_findings":
        source_findings,

    "caller_summary":
        caller_info["summary"],

    "minimal_future_patch":
        feasibility["minimal_future_patch"],

    "unlocked_mechanics":
        feasibility["unlocked_mechanics"],

    "recommended_next_step":
        (
            "layer_6f_initial_state_prototype"
            if (
                feasibility["diagnosis"]
                ==
                "base_out_state_api_feasible_with_backward_compatible_defaults"
            )
            else
            "layer_6f_refactor_audit"
        ),
}


safe_write_json(
    OUTPUT_DIR
    /
    "base_out_state_feasibility.json",
    summary,
)

write_csv(
    OUTPUT_DIR
    /
    "base_out_state_feasibility_checklist.csv",
    checklist_rows,
)

print(
    json.dumps(
        summary,
        indent=2,
    )
)
