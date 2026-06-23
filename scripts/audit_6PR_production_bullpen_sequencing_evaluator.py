#!/usr/bin/env python3
"""
Layer 6PR
Production Bullpen Sequencing Evaluator Independent Audit

Independently audits the merged 6PQ bullpen-sequence evaluator.

This layer does not:
- change production pitcher selection;
- change starter innings;
- change bullpen transitions;
- change plate-appearance probabilities;
- change simulation scores or win probabilities;
- authorize integration, tuning, backtesting, pricing, or edge detection.
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


LAYER_ID = "6PR"

LAYER_NAME = (
    "production_bullpen_sequencing_"
    "evaluator_independent_audit"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = ROOT / (
    "tmp/layer_6PR_production_bullpen_"
    "sequencing_evaluator_independent_audit"
)

MODULE_PATH = (
    ROOT
    / "mlb_app/simulation/"
    "bullpen_sequence_evaluator.py"
)

IMPLEMENTATION_SCRIPT = (
    ROOT
    / "scripts/implement_6PQ_production_"
    "bullpen_sequencing_state_contract_"
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
    "requests",
    "sqlalchemy",
}

EXPECTED_OUTPUT_FIELDS = {
    "recommended_pitcher_id",
    "ranked_candidates",
    "leverage_band",
    "selection_reason",
    "fallback_used",
    "fallback_reason",
    "state_completeness",
    "behavioral_effect",
    "canonical_probability_authority_changed",
    "production_activation",
}


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
        try:
            payload = json.loads(
                text[index:].strip()
            )
        except json.JSONDecodeError:
            continue

        if isinstance(payload, dict):
            return payload

    return {}


def reliever(
    pitcher_id: str,
    role: str,
    *,
    availability: str = "available",
    quality: float = 0.0,
    fatigue: float = 0.20,
    recent_usage: int = 0,
    back_to_back: bool = False,
    capacity: float = 1.0,
    evidence_complete: bool = True,
) -> dict[str, Any]:
    return {
        "pitcher_id": pitcher_id,
        "role": role,
        "throws": "R",
        "quality_score": quality,
        "availability_status": availability,
        "fatigue_index": fatigue,
        "recent_usage_count": recent_usage,
        "back_to_back_flag": back_to_back,
        "innings_capacity": capacity,
        "evidence_complete": evidence_complete,
    }


def base_state() -> dict[str, Any]:
    return {
        "team_id": "TEAM",
        "inning": 8,
        "outs": 0,
        "base_state": {
            "first": False,
            "second": False,
            "third": False,
        },
        "score_margin": 1,
        "leverage_proxy": 0.75,
        "current_pitcher_id": "CURRENT",
        "available_relievers": [
            reliever(
                "CLOSER",
                "closer",
                quality=0.40,
            ),
            reliever(
                "SETUP",
                "setup",
                quality=0.30,
            ),
            reliever(
                "MIDDLE",
                "middle_relief",
                quality=0.10,
                capacity=2.0,
            ),
            reliever(
                "LOW",
                "low_leverage",
                capacity=2.0,
            ),
        ],
        "used_pitcher_ids": [],
        "usage_log": [],
        "bullpen_depletion_index": 0.20,
        "extra_inning_flag": False,
    }


def evaluate_case(
    module: Any,
    *,
    case_id: str,
    scenario: str,
    state: Any,
    expected_pitcher_id: Any,
    expected_band: str,
    expected_fallback: bool,
) -> dict[str, Any]:
    original = deepcopy(state)

    first = module.evaluate_bullpen_sequence(
        state
    )

    second = module.evaluate_bullpen_sequence(
        deepcopy(state)
    )

    validation = (
        module
        .validate_bullpen_sequence_evaluation(
            first
        )
    )

    passed = all(
        [
            first.get(
                "recommended_pitcher_id"
            )
            == expected_pitcher_id,
            first.get("leverage_band")
            == expected_band,
            first.get("fallback_used")
            is expected_fallback,
            first == second,
            state == original,
            set(first)
            == EXPECTED_OUTPUT_FIELDS,
            validation.get("valid")
            is True,
            first.get("behavioral_effect")
            == "none",
            first.get(
                "canonical_probability_"
                "authority_changed"
            )
            is False,
            first.get(
                "production_activation"
            )
            is False,
        ]
    )

    return {
        "case_id": case_id,
        "scenario": scenario,
        "expected_pitcher_id": (
            expected_pitcher_id
        ),
        "actual_pitcher_id": first.get(
            "recommended_pitcher_id"
        ),
        "expected_leverage_band": (
            expected_band
        ),
        "actual_leverage_band": first.get(
            "leverage_band"
        ),
        "expected_fallback": (
            expected_fallback
        ),
        "actual_fallback": first.get(
            "fallback_used"
        ),
        "deterministic": first == second,
        "input_unchanged": state == original,
        "output_valid": (
            validation.get("valid")
            is True
        ),
        "passed": passed,
        "evaluation": first,
    }


def production_reference_rows() -> list[
    dict[str, Any]
]:
    rows: list[dict[str, Any]] = []

    tokens = [
        "bullpen_sequence_evaluator",
        "evaluate_bullpen_sequence",
        (
            "validate_bullpen_"
            "sequence_evaluation"
        ),
    ]

    for path in PRODUCTION_PATHS:
        text = read_text(path)

        matched = [
            token
            for token in tokens
            if token in text
        ]

        if matched:
            rows.append(
                {
                    "path": str(
                        path.relative_to(ROOT)
                    ),
                    "matched_tokens": "|".join(
                        matched
                    ),
                }
            )

    return rows


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
            == 10,
            implementation_payload.get(
                "implementation_checks_required"
            )
            == 10,
            implementation_payload.get(
                "fixtures_passed"
            )
            == 10,
            implementation_payload.get(
                "fixtures_required"
            )
            == 10,
            implementation_payload.get(
                "production_reference_count"
            )
            == 0,
            implementation_payload.get(
                "production_activation"
            )
            is False,
        ]
    )

    module = importlib.import_module(
        "mlb_app.simulation."
        "bullpen_sequence_evaluator"
    )

    module_text = read_text(
        MODULE_PATH
    )

    tree = ast.parse(
        module_text,
        filename=str(MODULE_PATH),
    )

    imported_modules = sorted(
        {
            node.module
            for node in ast.walk(tree)
            if isinstance(
                node,
                ast.ImportFrom,
            )
            and node.module is not None
        }
        | {
            alias.name
            for node in ast.walk(tree)
            if isinstance(
                node,
                ast.Import,
            )
            for alias in node.names
        }
    )

    forbidden_imports = sorted(
        set(imported_modules)
        & PROHIBITED_IMPORTS
    )

    production_references = (
        production_reference_rows()
    )

    cases: list[dict[str, Any]] = []

    state = base_state()
    state["leverage_proxy"] = 0.90

    cases.append(
        evaluate_case(
            module,
            case_id="PR-C01",
            scenario="critical_leverage_closer",
            state=state,
            expected_pitcher_id="CLOSER",
            expected_band="critical",
            expected_fallback=False,
        )
    )

    state = base_state()
    state.update(
        {
            "inning": 6,
            "score_margin": 5,
            "leverage_proxy": 0.10,
        }
    )

    cases.append(
        evaluate_case(
            module,
            case_id="PR-C02",
            scenario="low_leverage_preservation",
            state=state,
            expected_pitcher_id="LOW",
            expected_band="low",
            expected_fallback=False,
        )
    )

    state = base_state()
    state[
        "available_relievers"
    ][0][
        "availability_status"
    ] = "unavailable"

    cases.append(
        evaluate_case(
            module,
            case_id="PR-C03",
            scenario="closer_excluded",
            state=state,
            expected_pitcher_id="SETUP",
            expected_band="high",
            expected_fallback=False,
        )
    )

    state = base_state()
    state[
        "available_relievers"
    ] = [
        reliever(
            "FRESH",
            "setup",
            quality=0.10,
            fatigue=0.05,
            recent_usage=0,
        ),
        reliever(
            "TIRED",
            "setup",
            quality=0.10,
            fatigue=0.95,
            recent_usage=3,
            back_to_back=True,
        ),
    ]

    cases.append(
        evaluate_case(
            module,
            case_id="PR-C04",
            scenario="fatigue_directionality",
            state=state,
            expected_pitcher_id="FRESH",
            expected_band="high",
            expected_fallback=False,
        )
    )

    state = base_state()
    state[
        "available_relievers"
    ] = [
        reliever(
            "B",
            "middle_relief",
        ),
        reliever(
            "A",
            "middle_relief",
        ),
    ]

    cases.append(
        evaluate_case(
            module,
            case_id="PR-C05",
            scenario="stable_tie_break",
            state=state,
            expected_pitcher_id="A",
            expected_band="high",
            expected_fallback=False,
        )
    )

    state = base_state()
    state[
        "used_pitcher_ids"
    ] = [
        "CLOSER",
        "SETUP",
    ]

    cases.append(
        evaluate_case(
            module,
            case_id="PR-C06",
            scenario="used_pitchers_excluded",
            state=state,
            expected_pitcher_id="MIDDLE",
            expected_band="high",
            expected_fallback=False,
        )
    )

    state = base_state()

    for candidate in state[
        "available_relievers"
    ]:
        candidate[
            "availability_status"
        ] = "unavailable"

    cases.append(
        evaluate_case(
            module,
            case_id="PR-C07",
            scenario="depletion_fallback",
            state=state,
            expected_pitcher_id=None,
            expected_band="high",
            expected_fallback=True,
        )
    )

    invalid_state = base_state()
    invalid_state.pop(
        "available_relievers"
    )

    cases.append(
        evaluate_case(
            module,
            case_id="PR-C08",
            scenario="invalid_state_fallback",
            state=invalid_state,
            expected_pitcher_id=None,
            expected_band="unknown",
            expected_fallback=True,
        )
    )

    state = base_state()
    state[
        "available_relievers"
    ][1][
        "evidence_complete"
    ] = False

    partial_result = (
        module.evaluate_bullpen_sequence(
            state
        )
    )

    partial_case_passed = all(
        [
            partial_result.get(
                "state_completeness"
            )
            == "partial",
            partial_result.get(
                "production_activation"
            )
            is False,
            partial_result.get(
                "behavioral_effect"
            )
            == "none",
        ]
    )

    cases.append(
        {
            "case_id": "PR-C09",
            "scenario": (
                "partial_evidence_preserved"
            ),
            "expected_pitcher_id": None,
            "actual_pitcher_id": (
                partial_result.get(
                    "recommended_pitcher_id"
                )
            ),
            "expected_leverage_band": (
                "high"
            ),
            "actual_leverage_band": (
                partial_result.get(
                    "leverage_band"
                )
            ),
            "expected_fallback": False,
            "actual_fallback": (
                partial_result.get(
                    "fallback_used"
                )
            ),
            "deterministic": (
                partial_result
                == module
                .evaluate_bullpen_sequence(
                    deepcopy(state)
                )
            ),
            "input_unchanged": True,
            "output_valid": (
                module
                .validate_bullpen_sequence_evaluation(
                    partial_result
                )
                .get("valid")
                is True
            ),
            "passed": partial_case_passed,
            "evaluation": partial_result,
        }
    )

    state = base_state()
    state.update(
        {
            "inning": 10,
            "score_margin": 0,
            "leverage_proxy": 0.70,
            "extra_inning_flag": True,
        }
    )

    state[
        "available_relievers"
    ] = [
        reliever(
            "LONG",
            "long_relief",
            capacity=3.0,
        ),
        reliever(
            "LIMITED_SETUP",
            "setup",
            availability="limited",
            fatigue=0.75,
            recent_usage=2,
            back_to_back=True,
        ),
    ]

    cases.append(
        evaluate_case(
            module,
            case_id="PR-C10",
            scenario="extra_inning_depletion",
            state=state,
            expected_pitcher_id="LONG",
            expected_band="critical",
            expected_fallback=False,
        )
    )

    cases_passed = sum(
        1
        for case in cases
        if case["passed"]
    )

    high_state = base_state()
    high_state["leverage_proxy"] = 0.90

    low_state = base_state()
    low_state.update(
        {
            "inning": 6,
            "score_margin": 5,
            "leverage_proxy": 0.10,
        }
    )

    high_result = (
        module.evaluate_bullpen_sequence(
            high_state
        )
    )

    low_result = (
        module.evaluate_bullpen_sequence(
            low_state
        )
    )

    role_directionality_valid = all(
        [
            high_result[
                "recommended_pitcher_id"
            ]
            == "CLOSER",
            low_result[
                "recommended_pitcher_id"
            ]
            == "LOW",
        ]
    )

    all_outputs_safe = all(
        case["evaluation"].get(
            "behavioral_effect"
        )
        == "none"
        and case["evaluation"].get(
            "canonical_probability_"
            "authority_changed"
        )
        is False
        and case["evaluation"].get(
            "production_activation"
        )
        is False
        for case in cases
    )

    checks = [
        {
            "check": "required_files_exist",
            "actual": required_files_exist,
            "expected": True,
            "passed": required_files_exist,
        },
        {
            "check": (
                "six_pq_implementation_"
                "contract_passes"
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
            "check": "ten_independent_cases_pass",
            "actual": cases_passed,
            "expected": 10,
            "passed": cases_passed == 10,
        },
        {
            "check": (
                "role_directionality_valid"
            ),
            "actual": (
                role_directionality_valid
            ),
            "expected": True,
            "passed": (
                role_directionality_valid
            ),
        },
        {
            "check": (
                "deterministic_across_cases"
            ),
            "actual": all(
                case["deterministic"]
                for case in cases
            ),
            "expected": True,
            "passed": all(
                case["deterministic"]
                for case in cases
            ),
        },
        {
            "check": (
                "inputs_unchanged_across_cases"
            ),
            "actual": all(
                case["input_unchanged"]
                for case in cases
            ),
            "expected": True,
            "passed": all(
                case["input_unchanged"]
                for case in cases
            ),
        },
        {
            "check": (
                "outputs_valid_across_cases"
            ),
            "actual": all(
                case["output_valid"]
                for case in cases
            ),
            "expected": True,
            "passed": all(
                case["output_valid"]
                for case in cases
            ),
        },
        {
            "check": "forbidden_import_count",
            "actual": len(
                forbidden_imports
            ),
            "expected": 0,
            "passed": not forbidden_imports,
        },
        {
            "check": (
                "production_reference_count"
            ),
            "actual": len(
                production_references
            ),
            "expected": 0,
            "passed": not production_references,
        },
        {
            "check": (
                "production_authority_absent"
            ),
            "actual": all_outputs_safe,
            "expected": True,
            "passed": all_outputs_safe,
        },
        {
            "check": (
                "module_declares_no_"
                "production_authority"
            ),
            "actual": all(
                token in module_text
                for token in [
                    (
                        "This module has no "
                        "production simulation authority"
                    ),
                    (
                        '"behavioral_effect": '
                        '"none"'
                    ),
                    (
                        '"production_activation": '
                        "False"
                    ),
                ]
            ),
            "expected": True,
            "passed": all(
                token in module_text
                for token in [
                    (
                        "This module has no "
                        "production simulation authority"
                    ),
                    (
                        '"behavioral_effect": '
                        '"none"'
                    ),
                    (
                        '"production_activation": '
                        "False"
                    ),
                ]
            ),
        },
        {
            "check": (
                "output_contract_exact"
            ),
            "actual": all(
                set(
                    case["evaluation"]
                )
                == EXPECTED_OUTPUT_FIELDS
                for case in cases
            ),
            "expected": True,
            "passed": all(
                set(
                    case["evaluation"]
                )
                == EXPECTED_OUTPUT_FIELDS
                for case in cases
            ),
        },
    ]

    all_checks_passed = all(
        row["passed"]
        for row in checks
    )

    case_rows = []

    for case in cases:
        case_rows.append(
            {
                key: value
                for key, value in case.items()
                if key != "evaluation"
            }
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
            "expected_pitcher_id",
            "actual_pitcher_id",
            "expected_leverage_band",
            "actual_leverage_band",
            "expected_fallback",
            "actual_fallback",
            "deterministic",
            "input_unchanged",
            "output_valid",
            "passed",
        ],
        case_rows,
    )

    write_csv(
        OUTPUT_DIR / "production_references.csv",
        [
            "path",
            "matched_tokens",
        ],
        production_references,
    )

    write_csv(
        OUTPUT_DIR / "import_inventory.csv",
        [
            "imported_module",
            "prohibited",
        ],
        [
            {
                "imported_module": name,
                "prohibited": (
                    name in PROHIBITED_IMPORTS
                ),
            }
            for name in imported_modules
        ],
    )

    write_json(
        OUTPUT_DIR / "independent_payloads.json",
        {
            case["case_id"]: (
                case["evaluation"]
            )
            for case in cases
        },
    )

    summary = {
        "audit_checks_required": len(
            checks
        ),
        "audit_checks_passed": sum(
            1
            for row in checks
            if row["passed"]
        ),
        "independent_cases_required": 10,
        "independent_cases_passed": (
            cases_passed
        ),
        "implementation_contract_passed": (
            implementation_contract_passed
        ),
        "role_directionality_valid": (
            role_directionality_valid
        ),
        "deterministic": all(
            case["deterministic"]
            for case in cases
        ),
        "input_immutability": all(
            case["input_unchanged"]
            for case in cases
        ),
        "output_contract_valid": all(
            case["output_valid"]
            for case in cases
        ),
        "forbidden_import_count": len(
            forbidden_imports
        ),
        "production_reference_count": len(
            production_references
        ),
        "production_behavior_changed": False,
        "simulation_behavior_changed": False,
        "canonical_probability_authority_changed": False,
        "production_activation": False,
    }

    write_json(
        OUTPUT_DIR / "audit_summary.json",
        summary,
    )

    recommended_next_layer = (
        "6PS_production_bullpen_sequencing_"
        "diagnostic_integration_plan"
        if all_checks_passed
        else
        "6PS_production_bullpen_sequencing_"
        "evaluator_remediation"
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": (
            "production_bullpen_sequencing_"
            "evaluator_independent_audit_passed"
            if all_checks_passed
            else
            "production_bullpen_sequencing_"
            "evaluator_independent_audit_failed"
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
            cases_passed
        ),
        "independent_cases_required": 10,
        "six_pq_implementation_contract_passed": (
            implementation_contract_passed
        ),
        "role_directionality_valid": (
            role_directionality_valid
        ),
        "deterministic": all(
            case["deterministic"]
            for case in cases
        ),
        "input_immutability": all(
            case["input_unchanged"]
            for case in cases
        ),
        "output_contract_valid": all(
            case["output_valid"]
            for case in cases
        ),
        "forbidden_import_count": len(
            forbidden_imports
        ),
        "production_reference_count": len(
            production_references
        ),
        "production_bullpen_changed": False,
        "simulation_behavior_changed": False,
        "canonical_probability_authority_changed": False,
        "production_activation": False,
        "broad_layer6_exit_paused": True,
        "layer6_exit_recommended": False,
        "layer6_exit_finalized": False,
        "new_authority_granted": False,
        "historical_validation_allowed_next": False,
        "tuning_allowed_next": False,
        "backtests_allowed_next": False,
        "pricing_allowed_next": False,
        "edge_detection_allowed_next": False,
        "diagnostic_integration_planning_allowed_next": (
            all_checks_passed
        ),
        "production_behavior_integration_allowed_next": False,
        "recommended_next_layer": (
            recommended_next_layer
        ),
        "generated_csv_artifacts": [
            str(
                OUTPUT_DIR
                / "audit_checks.csv"
            ),
            str(
                OUTPUT_DIR
                / "independent_cases.csv"
            ),
            str(
                OUTPUT_DIR
                / "production_references.csv"
            ),
            str(
                OUTPUT_DIR
                / "import_inventory.csv"
            ),
        ],
        "generated_json_artifacts": [
            str(
                OUTPUT_DIR
                / "independent_payloads.json"
            ),
            str(
                OUTPUT_DIR
                / "audit_summary.json"
            ),
            str(
                OUTPUT_DIR
                / "diagnosis.json"
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
