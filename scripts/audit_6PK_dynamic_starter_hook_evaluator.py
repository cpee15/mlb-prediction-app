#!/usr/bin/env python3
"""
Layer 6PK
Dynamic Starter-Hook Evaluator Independent Audit

Independently verifies the merged 6PJ evaluator for:

- state-contract enforcement;
- output-contract enforcement;
- deterministic behavior;
- input immutability;
- monotonic workload response;
- incomplete-state fallback;
- optional-field non-authority;
- production non-reachability;
- absence of simulation and probability authority.

This audit does not modify production behavior.
"""

from __future__ import annotations

import ast
import csv
import importlib
import json
import subprocess
import sys
from copy import deepcopy
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "6PK"
LAYER_NAME = (
    "dynamic_starter_hook_"
    "evaluator_independent_audit"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = ROOT / (
    "tmp/layer_6PK_dynamic_starter_hook_"
    "evaluator_independent_audit"
)

MODULE_PATH = (
    ROOT
    / "mlb_app/simulation/"
    "starter_hook_evaluator.py"
)

IMPLEMENTATION_SCRIPT = (
    ROOT
    / "scripts/implement_6PJ_"
    "dynamic_starter_hook_state_contract_"
    "and_evaluator.py"
)

PRODUCTION_PATHS = [
    ROOT
    / "mlb_app/simulation/"
    "game_engine_v2.py",
    ROOT
    / "mlb_app/simulation/"
    "game_simulator.py",
    ROOT
    / "mlb_app/simulation/"
    "game_simulation_builder.py",
]

PROHIBITED_IMPORTS = {
    "random",
    "numpy",
    "pandas",
    "sqlalchemy",
}

PROHIBITED_AUTHORITIES = [
    "production_starter_hook_change",
    "starter_exit_distribution_change",
    "starter_innings_change",
    "bullpen_transition_change",
    "bullpen_sequence_change",
    "plate_appearance_probability_change",
    "simulation_parameter_change",
    "simulation_score_change",
    "win_probability_change",
    "canonical_probability_replacement",
    "pitching_plan_production_activation",
    "backend_response_change",
    "frontend_behavior_change",
    "historical_outcome_join",
    "accuracy_metric_generation",
    "parameter_tuning",
    "backtest_execution",
    "pricing",
    "edge_detection",
    "bet_recommendation",
    "broad_layer6_exit",
]


def write_csv(
    path: Path,
    fieldnames: list[str],
    rows: Iterable[dict[str, Any]],
) -> None:
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with path.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=fieldnames,
        )
        writer.writeheader()
        writer.writerows(rows)


def write_json(
    path: Path,
    payload: Any,
) -> None:
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    path.write_text(
        json.dumps(payload, indent=2) + "\n",
        encoding="utf-8",
    )


def read_text(path: Path) -> str:
    if not path.exists():
        return ""

    return path.read_text(
        encoding="utf-8",
        errors="ignore",
    )


def parse_last_json_object(
    text: str,
) -> dict[str, Any]:
    positions = [
        index
        for index, character in enumerate(text)
        if character == "{"
    ]

    for index in reversed(positions):
        candidate = text[index:].strip()

        try:
            payload = json.loads(candidate)
        except json.JSONDecodeError:
            continue

        if isinstance(payload, dict):
            return payload

    return {}


def base_state() -> dict[str, Any]:
    return {
        "inning": 4,
        "outs": 0,
        "base_state": {
            "first": False,
            "second": False,
            "third": False,
        },
        "batters_faced": 16,
        "pitch_count_estimate": 60.0,
        "times_through_order": 1.8,
        "runs_allowed": 1,
        "recent_traffic_index": 0.20,
        "score_margin": 1,
        "leverage_proxy": 0.45,
        "starter_quality_score": 0.0,
        "expected_starter_innings": 5.6,
        "fatigue_index": 0.30,
        "bullpen_availability": {},
        "pitching_plan": {},
    }


def evaluate_case(
    module: Any,
    case_id: str,
    scenario: str,
    state: Any,
    expected_decision: str,
    expected_reason: str | None = None,
) -> dict[str, Any]:
    original = deepcopy(state)

    first = module.evaluate_starter_hook(
        state
    )

    second = module.evaluate_starter_hook(
        deepcopy(state)
    )

    output_validation = (
        module
        .validate_starter_hook_evaluation(
            first
        )
    )

    reasons = first.get(
        "trigger_reasons"
    ) or []

    passed = all(
        [
            first.get("decision")
            == expected_decision,
            (
                expected_reason in reasons
                if expected_reason
                else True
            ),
            first == second,
            state == original,
            output_validation.get("valid")
            is True,
            first.get(
                "behavioral_effect"
            )
            == "none",
            first.get(
                "production_activation"
            )
            is False,
            first.get(
                "canonical_probability_"
                "authority_changed"
            )
            is False,
        ]
    )

    return {
        "case_id": case_id,
        "scenario": scenario,
        "expected_decision": (
            expected_decision
        ),
        "actual_decision": first.get(
            "decision"
        ),
        "expected_reason": (
            expected_reason
        ),
        "actual_reasons": reasons,
        "pull_probability": first.get(
            "pull_probability"
        ),
        "deterministic": first == second,
        "input_unchanged": (
            state == original
        ),
        "output_valid": (
            output_validation.get("valid")
            is True
        ),
        "passed": passed,
        "evaluation": first,
    }


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    required_files_exist = all(
        path.exists()
        for path in [
            MODULE_PATH,
            IMPLEMENTATION_SCRIPT,
            *PRODUCTION_PATHS,
        ]
    )

    implementation_run = subprocess.run(
        [
            sys.executable,
            str(IMPLEMENTATION_SCRIPT),
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    implementation_payload = (
        parse_last_json_object(
            implementation_run.stdout
        )
    )

    implementation_contract_passed = all(
        [
            implementation_run.returncode == 0,
            implementation_payload.get(
                "all_checks_passed"
            )
            is True,
            implementation_payload.get(
                "implementation_checks_passed"
            )
            == 13,
            implementation_payload.get(
                "implementation_checks_required"
            )
            == 13,
            implementation_payload.get(
                "fixtures_passed"
            )
            == 10,
            implementation_payload.get(
                "production_reference_count"
            )
            == 0,
            implementation_payload.get(
                "evaluator_is_pure"
            )
            is True,
        ]
    )

    module = importlib.import_module(
        "mlb_app.simulation."
        "starter_hook_evaluator"
    )

    independent_cases: list[
        dict[str, Any]
    ] = []

    state = base_state()

    independent_cases.append(
        evaluate_case(
            module,
            "PK-C01",
            "neutral_baseline",
            state,
            "keep",
            "no_pull_threshold_reached",
        )
    )

    state = base_state()
    state["pitch_count_estimate"] = 110.0

    independent_cases.append(
        evaluate_case(
            module,
            "PK-C02",
            "critical_pitch_count",
            state,
            "pull",
            "critical_pitch_count",
        )
    )

    state = base_state()
    state["times_through_order"] = 3.1
    state["batters_faced"] = 28
    state["inning"] = 6

    independent_cases.append(
        evaluate_case(
            module,
            "PK-C03",
            "third_time_through_order",
            state,
            "pull",
            "third_time_through_order",
        )
    )

    state = base_state()
    state["runs_allowed"] = 5
    state["recent_traffic_index"] = 0.85

    independent_cases.append(
        evaluate_case(
            module,
            "PK-C04",
            "poor_outing_with_traffic",
            state,
            "pull",
            "five_plus_runs_allowed",
        )
    )

    state = base_state()
    state.update(
        {
            "inning": 7,
            "score_margin": 1,
            "leverage_proxy": 0.95,
            "pitch_count_estimate": 90.0,
            "fatigue_index": 0.60,
        }
    )

    independent_cases.append(
        evaluate_case(
            module,
            "PK-C05",
            "late_close_high_leverage",
            state,
            "pull",
            (
                "late_high_leverage_"
                "close_game"
            ),
        )
    )

    state = base_state()
    state.update(
        {
            "inning": 7,
            "score_margin": 8,
            "leverage_proxy": 0.10,
            "starter_quality_score": 0.8,
            "pitch_count_estimate": 75.0,
            "fatigue_index": 0.35,
            "expected_starter_innings": 7.2,
        }
    )

    independent_cases.append(
        evaluate_case(
            module,
            "PK-C06",
            "late_blowout_extension",
            state,
            "keep",
            (
                "low_leverage_blowout_"
                "extension"
            ),
        )
    )

    state = base_state()
    state.pop(
        "pitch_count_estimate"
    )

    independent_cases.append(
        evaluate_case(
            module,
            "PK-C07",
            "missing_required_field",
            state,
            "insufficient_state",
            "insufficient_state",
        )
    )

    independent_cases.append(
        evaluate_case(
            module,
            "PK-C08",
            "invalid_state_type",
            [],
            "insufficient_state",
            "insufficient_state",
        )
    )

    state = base_state()
    state["bullpen_availability"] = {
        "closer_available": False,
    }
    state["pitching_plan"] = {
        "plan_type": "opener_bulk",
    }

    optional_result = (
        module.evaluate_starter_hook(
            state
        )
    )

    state_without_optional = (
        base_state()
    )

    baseline_result = (
        module.evaluate_starter_hook(
            state_without_optional
        )
    )

    optional_fields_non_authoritative = (
        optional_result
        == baseline_result
    )

    low_workload = base_state()

    medium_workload = base_state()
    medium_workload.update(
        {
            "pitch_count_estimate": 88.0,
            "fatigue_index": 0.55,
        }
    )

    high_workload = base_state()
    high_workload.update(
        {
            "pitch_count_estimate": 108.0,
            "fatigue_index": 0.88,
        }
    )

    low_result = (
        module.evaluate_starter_hook(
            low_workload
        )
    )

    medium_result = (
        module.evaluate_starter_hook(
            medium_workload
        )
    )

    high_result = (
        module.evaluate_starter_hook(
            high_workload
        )
    )

    workload_monotonicity = (
        low_result[
            "pull_probability"
        ]
        <= medium_result[
            "pull_probability"
        ]
        <= high_result[
            "pull_probability"
        ]
    )

    strong_state = base_state()
    strong_state.update(
        {
            "starter_quality_score": 0.8,
            "pitch_count_estimate": 82.0,
            "fatigue_index": 0.45,
        }
    )

    weak_state = deepcopy(
        strong_state
    )

    weak_state[
        "starter_quality_score"
    ] = -0.8

    strong_result = (
        module.evaluate_starter_hook(
            strong_state
        )
    )

    weak_result = (
        module.evaluate_starter_hook(
            weak_state
        )
    )

    quality_directionality = (
        strong_result[
            "pull_probability"
        ]
        <= weak_result[
            "pull_probability"
        ]
    )

    module_tree = ast.parse(
        read_text(MODULE_PATH),
        filename=str(MODULE_PATH),
    )

    forbidden_imports: list[str] = []

    for node in ast.walk(module_tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name in (
                    PROHIBITED_IMPORTS
                ):
                    forbidden_imports.append(
                        alias.name
                    )

        elif isinstance(
            node,
            ast.ImportFrom,
        ):
            if (
                node.module
                in PROHIBITED_IMPORTS
            ):
                forbidden_imports.append(
                    str(node.module)
                )

    production_reference_rows: list[
        dict[str, Any]
    ] = []

    for path in PRODUCTION_PATHS:
        text = read_text(path)

        references_evaluator = any(
            token in text
            for token in [
                "starter_hook_evaluator",
                "evaluate_starter_hook",
                (
                    "validate_starter_hook_"
                    "state"
                ),
            ]
        )

        production_reference_rows.append(
            {
                "path": str(
                    path.relative_to(ROOT)
                ),
                "references_evaluator": (
                    references_evaluator
                ),
            }
        )

    production_reference_count = sum(
        1
        for row in production_reference_rows
        if row[
            "references_evaluator"
        ]
    )

    independent_cases_passed = sum(
        1
        for row in independent_cases
        if row["passed"]
    )

    checks = [
        {
            "check": (
                "required_files_exist"
            ),
            "actual": required_files_exist,
            "expected": True,
            "passed": required_files_exist,
        },
        {
            "check": (
                "six_pj_implementation_contract_passes"
            ),
            "actual": (
                implementation_contract_passed
            ),
            "expected": True,
            "passed": (
                implementation_contract_passed
            ),
        },
        {
            "check": (
                "independent_cases_pass"
            ),
            "actual": (
                independent_cases_passed
            ),
            "expected": len(
                independent_cases
            ),
            "passed": (
                independent_cases_passed
                == len(independent_cases)
            ),
        },
        {
            "check": (
                "optional_fields_non_authoritative"
            ),
            "actual": (
                optional_fields_non_authoritative
            ),
            "expected": True,
            "passed": (
                optional_fields_non_authoritative
            ),
        },
        {
            "check": (
                "workload_response_monotonic"
            ),
            "actual": [
                low_result[
                    "pull_probability"
                ],
                medium_result[
                    "pull_probability"
                ],
                high_result[
                    "pull_probability"
                ],
            ],
            "expected": (
                "non_decreasing"
            ),
            "passed": (
                workload_monotonicity
            ),
        },
        {
            "check": (
                "quality_directionality"
            ),
            "actual": {
                "strong": strong_result[
                    "pull_probability"
                ],
                "weak": weak_result[
                    "pull_probability"
                ],
            },
            "expected": (
                "strong_less_than_or_equal_to_weak"
            ),
            "passed": (
                quality_directionality
            ),
        },
        {
            "check": (
                "zero_forbidden_imports"
            ),
            "actual": forbidden_imports,
            "expected": [],
            "passed": (
                forbidden_imports == []
            ),
        },
        {
            "check": (
                "zero_production_references"
            ),
            "actual": (
                production_reference_count
            ),
            "expected": 0,
            "passed": (
                production_reference_count
                == 0
            ),
        },
        {
            "check": (
                "all_cases_preserve_inputs"
            ),
            "actual": all(
                row[
                    "input_unchanged"
                ]
                for row in independent_cases
            ),
            "expected": True,
            "passed": all(
                row[
                    "input_unchanged"
                ]
                for row in independent_cases
            ),
        },
        {
            "check": (
                "all_cases_deterministic"
            ),
            "actual": all(
                row[
                    "deterministic"
                ]
                for row in independent_cases
            ),
            "expected": True,
            "passed": all(
                row[
                    "deterministic"
                ]
                for row in independent_cases
            ),
        },
        {
            "check": (
                "all_cases_output_valid"
            ),
            "actual": all(
                row[
                    "output_valid"
                ]
                for row in independent_cases
            ),
            "expected": True,
            "passed": all(
                row[
                    "output_valid"
                ]
                for row in independent_cases
            ),
        },
        {
            "check": (
                "production_authority_remains_false"
            ),
            "actual": all(
                row["evaluation"].get(
                    "production_activation"
                )
                is False
                for row in independent_cases
            ),
            "expected": True,
            "passed": all(
                row["evaluation"].get(
                    "production_activation"
                )
                is False
                for row in independent_cases
            ),
        },
        {
            "check": (
                "canonical_probability_authority_unchanged"
            ),
            "actual": all(
                row["evaluation"].get(
                    "canonical_probability_"
                    "authority_changed"
                )
                is False
                for row in independent_cases
            ),
            "expected": True,
            "passed": all(
                row["evaluation"].get(
                    "canonical_probability_"
                    "authority_changed"
                )
                is False
                for row in independent_cases
            ),
        },
    ]

    all_checks_passed = all(
        row["passed"]
        for row in checks
    )

    authority_rows = [
        {
            "authority": authority,
            "granted": False,
            "reason": (
                "6PK is an independent audit only."
            ),
        }
        for authority in PROHIBITED_AUTHORITIES
    ]

    authority_rows.extend(
        [
            {
                "authority": (
                    "diagnostic_integration_planning"
                ),
                "granted": (
                    all_checks_passed
                ),
                "reason": (
                    "Planning only; no production "
                    "simulation wiring."
                ),
            },
            {
                "authority": (
                    "diagnostic_integration_implementation"
                ),
                "granted": False,
                "reason": (
                    "Requires a separate approved plan."
                ),
            },
            {
                "authority": (
                    "production_behavior_integration"
                ),
                "granted": False,
                "reason": (
                    "Historical validation and explicit "
                    "authorization remain absent."
                ),
            },
        ]
    )

    recommended_next_layer = (
        "6PL_dynamic_starter_hook_"
        "diagnostic_integration_plan"
        if all_checks_passed
        else
        "6PL_dynamic_starter_hook_"
        "evaluator_remediation"
    )

    write_csv(
        OUTPUT_DIR / "audit_checks.csv",
        [
            "check",
            "actual",
            "expected",
            "passed",
        ],
        checks,
    )

    write_csv(
        OUTPUT_DIR / "independent_cases.csv",
        [
            "case_id",
            "scenario",
            "expected_decision",
            "actual_decision",
            "expected_reason",
            "actual_reasons",
            "pull_probability",
            "deterministic",
            "input_unchanged",
            "output_valid",
            "passed",
        ],
        [
            {
                key: row[key]
                for key in [
                    "case_id",
                    "scenario",
                    "expected_decision",
                    "actual_decision",
                    "expected_reason",
                    "actual_reasons",
                    "pull_probability",
                    "deterministic",
                    "input_unchanged",
                    "output_valid",
                    "passed",
                ]
            }
            for row in independent_cases
        ],
    )

    write_csv(
        OUTPUT_DIR / "production_reference_scan.csv",
        [
            "path",
            "references_evaluator",
        ],
        production_reference_rows,
    )

    write_csv(
        OUTPUT_DIR / "authority_boundaries.csv",
        [
            "authority",
            "granted",
            "reason",
        ],
        authority_rows,
    )

    write_csv(
        OUTPUT_DIR / "recommended_path.csv",
        [
            "recommended_next_layer",
            "recommended_action",
            "entry_condition",
            "passed",
        ],
        [
            {
                "recommended_next_layer": (
                    recommended_next_layer
                ),
                "recommended_action": (
                    "Plan disabled-by-default diagnostic "
                    "integration while preserving exact "
                    "simulation equivalence."
                    if all_checks_passed
                    else
                    "Remediate failed independent "
                    "evaluator audit checks."
                ),
                "entry_condition": (
                    "All 6PK audit checks pass."
                ),
                "passed": all_checks_passed,
            }
        ],
    )

    write_json(
        OUTPUT_DIR
        / "independent_case_payloads.json",
        independent_cases,
    )

    write_json(
        OUTPUT_DIR
        / "implementation_contract.json",
        {
            "returncode": (
                implementation_run.returncode
            ),
            "contract_passed": (
                implementation_contract_passed
            ),
            "diagnosis": (
                implementation_payload
            ),
            "stderr": (
                implementation_run.stderr
            ),
        },
    )

    audit_summary = {
        "audit_checks_required": len(
            checks
        ),
        "audit_checks_passed": sum(
            1
            for row in checks
            if row["passed"]
        ),
        "independent_cases_required": len(
            independent_cases
        ),
        "independent_cases_passed": (
            independent_cases_passed
        ),
        "optional_fields_non_authoritative": (
            optional_fields_non_authoritative
        ),
        "workload_response_monotonic": (
            workload_monotonicity
        ),
        "quality_directionality": (
            quality_directionality
        ),
        "production_reference_count": (
            production_reference_count
        ),
        "forbidden_imports": (
            forbidden_imports
        ),
        "production_behavior_changed": False,
        "simulation_behavior_changed": False,
        "canonical_probability_authority_changed": (
            False
        ),
        "new_authority_granted": False,
    }

    write_json(
        OUTPUT_DIR / "audit_summary.json",
        audit_summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": (
            "dynamic_starter_hook_evaluator_"
            "independent_audit_passed"
            if all_checks_passed
            else
            "dynamic_starter_hook_evaluator_"
            "independent_audit_failed"
        ),
        "all_checks_passed": (
            all_checks_passed
        ),
        "audit_checks_passed": sum(
            1
            for row in checks
            if row["passed"]
        ),
        "audit_checks_required": len(
            checks
        ),
        "independent_cases_passed": (
            independent_cases_passed
        ),
        "independent_cases_required": len(
            independent_cases
        ),
        "six_pj_implementation_contract_passed": (
            implementation_contract_passed
        ),
        "optional_fields_non_authoritative": (
            optional_fields_non_authoritative
        ),
        "workload_response_monotonic": (
            workload_monotonicity
        ),
        "quality_directionality": (
            quality_directionality
        ),
        "inputs_unchanged": all(
            row["input_unchanged"]
            for row in independent_cases
        ),
        "deterministic_replay_passed": all(
            row["deterministic"]
            for row in independent_cases
        ),
        "all_outputs_valid": all(
            row["output_valid"]
            for row in independent_cases
        ),
        "forbidden_import_count": len(
            forbidden_imports
        ),
        "production_reference_count": (
            production_reference_count
        ),
        "evaluator_is_pure": (
            len(forbidden_imports) == 0
            and production_reference_count == 0
        ),
        "production_starter_hook_changed": False,
        "simulation_behavior_changed": False,
        "canonical_probability_authority_changed": (
            False
        ),
        "production_activation": False,
        "broad_layer6_exit_paused": True,
        "layer6_exit_recommended": False,
        "layer6_exit_finalized": False,
        "new_authority_granted": False,
        "backend_behavior_change_allowed_next": False,
        "frontend_behavior_change_allowed_next": False,
        "simulation_parameter_change_allowed_next": False,
        "final_probability_replacement_allowed_next": False,
        "historical_validation_allowed_next": False,
        "tuning_allowed_next": False,
        "prediction_join_execution_allowed_next": False,
        "accuracy_metrics_allowed_next": False,
        "backtests_allowed_next": False,
        "pricing_allowed_next": False,
        "edge_detection_allowed_next": False,
        "diagnostic_integration_planning_allowed_next": (
            all_checks_passed
        ),
        "diagnostic_integration_allowed_next": False,
        "production_behavior_integration_allowed_next": False,
        "recommended_next_layer": (
            recommended_next_layer
        ),
        "generated_csv_artifacts": [
            str(
                OUTPUT_DIR / "audit_checks.csv"
            ),
            str(
                OUTPUT_DIR
                / "independent_cases.csv"
            ),
            str(
                OUTPUT_DIR
                / "production_reference_scan.csv"
            ),
            str(
                OUTPUT_DIR
                / "authority_boundaries.csv"
            ),
            str(
                OUTPUT_DIR
                / "recommended_path.csv"
            ),
        ],
        "generated_json_artifacts": [
            str(
                OUTPUT_DIR
                / "independent_case_payloads.json"
            ),
            str(
                OUTPUT_DIR
                / "implementation_contract.json"
            ),
            str(
                OUTPUT_DIR / "audit_summary.json"
            ),
            str(
                OUTPUT_DIR / "diagnosis.json"
            ),
        ],
    }

    write_json(
        OUTPUT_DIR / "diagnosis.json",
        diagnosis,
    )

    print(json.dumps(diagnosis, indent=2))

    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
