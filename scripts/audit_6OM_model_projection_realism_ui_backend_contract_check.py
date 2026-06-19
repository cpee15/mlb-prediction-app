#!/usr/bin/env python3
"""
Layer 6OM
Model Projection Realism UI ↔ Backend Contract Check Audit

Audits the merged backend/frontend contract for game_state_realism.

This layer does not alter:
- simulation behavior
- simulation parameters
- canonical probabilities
- final probability selection
- historical validation
- tuning
- pricing
- edge detection
"""

from __future__ import annotations

import ast
import csv
import importlib
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "6OM"
LAYER_NAME = (
    "layer6_model_projection_realism_ui_backend_contract_check_audit"
)

ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / (
    "tmp/layer_6OM_model_projection_realism_ui_backend_contract_check_audit"
)

PLAN_PATH = (
    ROOT
    / "scripts/plan_6OL_model_projection_realism_ui_backend_contract_check.py"
)
PLAN_DIAGNOSIS_PATH = (
    ROOT
    / "tmp/layer_6OL_model_projection_realism_ui_backend_contract_check_plan"
    / "diagnosis.json"
)

BACKEND_PATH = ROOT / "mlb_app/model_projections.py"
FRONTEND_PATH = ROOT / "frontend/src/pages/ModelProjectionsPage.jsx"
FRONTEND_PACKAGE_PATH = ROOT / "frontend/package.json"

BUILD_LOG_PATH = OUTPUT_DIR / "frontend_build.log"
BUILD_EXIT_PATH = OUTPUT_DIR / "frontend_build_exit_code.txt"

REQUIRED_FIELDS = [
    ("base_out_state_enabled", bool),
    ("runner_advancement_enabled", bool),
    ("extras_enabled", bool),
    ("ghost_runner_enabled", bool),
    ("walkoff_shortening_enabled", bool),
    ("double_play_enabled", bool),
    ("sac_fly_enabled", bool),
    ("steals_model_status", str),
]

UNSAFE_RENDER_REFERENCES = [
    "projection?.game_state_realism",
    "row?.game_state_realism",
    "item?.game_state_realism",
]

PROHIBITED_ACTIONS = [
    "backend_behavior_change",
    "simulation_parameter_change",
    "final_probability_replacement",
    "historical_validation",
    "parameter_tuning",
    "prediction_join_execution",
    "accuracy_metric_generation",
    "backtest_execution",
    "pricing",
    "edge_detection",
    "bet_recommendation",
]


def write_csv(
    path: Path,
    fieldnames: list[str],
    rows: Iterable[dict[str, Any]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_backend_module():
    """
    Import through the mlb_app package so relative imports resolve using
    the same package context as production.
    """
    return importlib.import_module("mlb_app.model_projections")


def inspect_backend_ast(
    backend_text: str,
) -> tuple[int, int, bool, int]:
    tree = ast.parse(backend_text)

    helper_definition_count = 0
    helper_call_count = 0
    per_game_payload_entry_count = 0
    payload_entry_inside_games_append = False

    for node in ast.walk(tree):
        if (
            isinstance(node, ast.FunctionDef)
            and node.name == "_build_game_state_realism_diagnostics"
        ):
            helper_definition_count += 1

        if isinstance(node, ast.Call):
            if (
                isinstance(node.func, ast.Name)
                and node.func.id
                == "_build_game_state_realism_diagnostics"
            ):
                helper_call_count += 1

            is_games_append = (
                isinstance(node.func, ast.Attribute)
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == "games"
                and node.func.attr == "append"
            )

            if not is_games_append or len(node.args) != 1:
                continue

            payload = node.args[0]

            if not isinstance(payload, ast.Dict):
                continue

            for key, value in zip(payload.keys, payload.values):
                is_expected_key = (
                    isinstance(key, ast.Constant)
                    and key.value == "game_state_realism"
                )
                is_expected_value = (
                    isinstance(value, ast.Call)
                    and isinstance(value.func, ast.Name)
                    and value.func.id
                    == "_build_game_state_realism_diagnostics"
                )

                if is_expected_key and is_expected_value:
                    per_game_payload_entry_count += 1
                    payload_entry_inside_games_append = True

    return (
        helper_definition_count,
        helper_call_count,
        payload_entry_inside_games_append,
        per_game_payload_entry_count,
    )


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    source_paths_exist = all(
        path.exists()
        for path in [
            PLAN_PATH,
            BACKEND_PATH,
            FRONTEND_PATH,
            FRONTEND_PACKAGE_PATH,
        ]
    )

    predecessor_execution_passed = False
    predecessor_diagnosis_passed = False
    predecessor_return_code: int | None = None
    predecessor_diagnosis: dict[str, Any] = {}

    if PLAN_PATH.exists():
        predecessor = subprocess.run(
            [sys.executable, str(PLAN_PATH)],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        predecessor_return_code = predecessor.returncode
        predecessor_execution_passed = predecessor.returncode == 0

    if PLAN_DIAGNOSIS_PATH.exists():
        predecessor_diagnosis = read_json(PLAN_DIAGNOSIS_PATH)
        predecessor_diagnosis_passed = (
            predecessor_diagnosis.get("diagnosis")
            == (
                "layer_6_model_projection_realism_ui_backend_"
                "contract_check_plan_complete"
            )
            and predecessor_diagnosis.get("all_checks_passed") is True
        )

    backend_text = (
        BACKEND_PATH.read_text(encoding="utf-8", errors="ignore")
        if BACKEND_PATH.exists()
        else ""
    )
    frontend_text = (
        FRONTEND_PATH.read_text(encoding="utf-8", errors="ignore")
        if FRONTEND_PATH.exists()
        else ""
    )

    package_json = (
        read_json(FRONTEND_PACKAGE_PATH)
        if FRONTEND_PACKAGE_PATH.exists()
        else {}
    )
    official_build_command_present = (
        package_json.get("scripts", {}).get("build") == "vite build"
    )

    (
        helper_definition_count,
        helper_call_count,
        payload_entry_inside_games_append,
        per_game_payload_entry_count,
    ) = inspect_backend_ast(backend_text)

    representative_payload: dict[str, Any] = {}
    representative_payload_error: str | None = None

    try:
        backend_module = load_backend_module()
        helper = getattr(
            backend_module,
            "_build_game_state_realism_diagnostics",
        )
        representative_payload = helper()
    except Exception as exc:
        representative_payload_error = (
            f"{type(exc).__name__}: {exc}"
        )

    json_serializable = False

    try:
        json.dumps(representative_payload)
        json_serializable = True
    except (TypeError, ValueError):
        json_serializable = False

    field_rows: list[dict[str, Any]] = []

    for field, expected_type in REQUIRED_FIELDS:
        value = representative_payload.get(field)
        present = field in representative_payload
        expected_type_match = (
            present and type(value) is expected_type
        )

        field_rows.append(
            {
                "field": field,
                "present": present,
                "expected_type": expected_type.__name__,
                "actual_type": (
                    type(value).__name__ if present else "missing"
                ),
                "value": json.dumps(value),
                "passed": present and expected_type_match,
            }
        )

    all_fields_passed = all(
        bool(row["passed"]) for row in field_rows
    )

    safe_render = (
        "function GameProjectionCard({ game })" in frontend_text
        and frontend_text.count(
            "renderGameStateRealismDiagnostics("
            "game?.game_state_realism)"
        )
        == 1
    )

    unsafe_references_absent = all(
        token not in frontend_text
        for token in UNSAFE_RENDER_REFERENCES
    )

    missing_group_guard = (
        "if (!gameStateRealism) return null" in frontend_text
    )

    missing_field_fallback = all(
        token in frontend_text
        for token in [
            'value === null || value === undefined || value === ""',
            'return "Unavailable"',
        ]
    )

    diagnostic_disclaimer = (
        "Diagnostic-only. Does not replace final projection probability."
        in frontend_text
    )

    frontend_field_list_matches = all(
        field in frontend_text
        for field, _ in REQUIRED_FIELDS
    )

    build_exit_code: int | None = None

    if BUILD_EXIT_PATH.exists():
        try:
            build_exit_code = int(
                BUILD_EXIT_PATH.read_text(
                    encoding="utf-8"
                ).strip()
            )
        except ValueError:
            build_exit_code = None

    frontend_build_passed = (
        build_exit_code == 0
        and BUILD_LOG_PATH.exists()
        and BUILD_LOG_PATH.stat().st_size > 0
    )

    runtime_confirmation = os.environ.get(
        "MODEL_PROJECTIONS_RUNTIME_CONFIRMED",
        "",
    ).strip().upper()

    manual_runtime_confirmed = runtime_confirmation == "YES"

    backend_checks = [
        {
            "check": "helper_definition_count_is_one",
            "actual": helper_definition_count,
            "expected": 1,
            "passed": helper_definition_count == 1,
        },
        {
            "check": "helper_runtime_call_count_is_one",
            "actual": helper_call_count,
            "expected": 1,
            "passed": helper_call_count == 1,
        },
        {
            "check": "payload_entry_inside_games_append",
            "actual": payload_entry_inside_games_append,
            "expected": True,
            "passed": payload_entry_inside_games_append,
        },
        {
            "check": "per_game_payload_entry_count_is_one",
            "actual": per_game_payload_entry_count,
            "expected": 1,
            "passed": per_game_payload_entry_count == 1,
        },
        {
            "check": "representative_payload_created",
            "actual": representative_payload_error or "created",
            "expected": "created",
            "passed": representative_payload_error is None,
        },
        {
            "check": "representative_payload_json_serializable",
            "actual": json_serializable,
            "expected": True,
            "passed": json_serializable,
        },
        {
            "check": "required_field_contract",
            "actual": sum(
                1 for row in field_rows if row["passed"]
            ),
            "expected": len(REQUIRED_FIELDS),
            "passed": all_fields_passed,
        },
    ]

    frontend_checks = [
        {
            "check": "safe_game_scope_render",
            "actual": safe_render,
            "expected": True,
            "passed": safe_render,
        },
        {
            "check": "unsafe_page_scope_references_absent",
            "actual": unsafe_references_absent,
            "expected": True,
            "passed": unsafe_references_absent,
        },
        {
            "check": "missing_group_guard",
            "actual": missing_group_guard,
            "expected": True,
            "passed": missing_group_guard,
        },
        {
            "check": "missing_field_fallback",
            "actual": missing_field_fallback,
            "expected": True,
            "passed": missing_field_fallback,
        },
        {
            "check": "diagnostic_only_disclaimer",
            "actual": diagnostic_disclaimer,
            "expected": True,
            "passed": diagnostic_disclaimer,
        },
        {
            "check": "frontend_required_field_list",
            "actual": frontend_field_list_matches,
            "expected": True,
            "passed": frontend_field_list_matches,
        },
        {
            "check": "official_vite_build_command",
            "actual": package_json.get(
                "scripts", {}
            ).get("build"),
            "expected": "vite build",
            "passed": official_build_command_present,
        },
        {
            "check": "frontend_production_build",
            "actual": build_exit_code,
            "expected": 0,
            "passed": frontend_build_passed,
        },
        {
            "check": "manual_runtime_smoke",
            "actual": runtime_confirmation or "not_confirmed",
            "expected": "YES",
            "passed": manual_runtime_confirmed,
        },
    ]

    probability_guard_checks = [
        {
            "check": "diagnostic_status_marker",
            "passed": (
                "diagnostic_only_not_final_probability"
                in backend_text
            ),
            "evidence": (
                "sharedSimulationDiagnostics.status remains "
                "diagnostic_only_not_final_probability"
            ),
        },
        {
            "check": "canonical_probability_assignment_preserved",
            "passed": (
                "canonical_probabilities = "
                "_canonical_probability_payload"
                in backend_text
            ),
            "evidence": (
                "Canonical probability payload remains the final "
                "projection probability path."
            ),
        },
        {
            "check": "diagnostic_group_not_probability_source",
            "passed": (
                '"game_state_realism": '
                "_build_game_state_realism_diagnostics()"
                in backend_text
                and (
                    "canonical_probabilities = "
                    "_build_game_state_realism_diagnostics"
                )
                not in backend_text
            ),
            "evidence": (
                "game_state_realism is attached as a separate "
                "diagnostic payload group."
            ),
        },
    ]

    predecessor_checks = [
        {
            "check": "source_paths_exist",
            "actual": source_paths_exist,
            "expected": True,
            "passed": source_paths_exist,
        },
        {
            "check": "6ol_plan_executes",
            "actual": predecessor_return_code,
            "expected": 0,
            "passed": predecessor_execution_passed,
        },
        {
            "check": "6ol_plan_diagnosis_complete",
            "actual": predecessor_diagnosis.get("diagnosis"),
            "expected": (
                "layer_6_model_projection_realism_ui_backend_"
                "contract_check_plan_complete"
            ),
            "passed": predecessor_diagnosis_passed,
        },
    ]

    safety_rows = [
        {
            "boundary": boundary,
            "changed_or_executed": False,
            "passed": True,
        }
        for boundary in PROHIBITED_ACTIONS
    ]

    safety_rows.extend(
        [
            {
                "boundary": "contract_audit_only",
                "changed_or_executed": True,
                "passed": True,
            },
            {
                "boundary": "frontend_build_validation",
                "changed_or_executed": True,
                "passed": frontend_build_passed,
            },
            {
                "boundary": "manual_runtime_smoke",
                "changed_or_executed": True,
                "passed": manual_runtime_confirmed,
            },
        ]
    )

    checks = [
        {
            "check": "predecessor_audit",
            "passed": all(
                bool(row["passed"])
                for row in predecessor_checks
            ),
        },
        {
            "check": "backend_contract_audit",
            "passed": all(
                bool(row["passed"])
                for row in backend_checks
            ),
        },
        {
            "check": "frontend_contract_audit",
            "passed": all(
                bool(row["passed"])
                for row in frontend_checks
            ),
        },
        {
            "check": "probability_guard_audit",
            "passed": all(
                bool(row["passed"])
                for row in probability_guard_checks
            ),
        },
        {
            "check": "safety_audit",
            "passed": all(
                bool(row["passed"])
                for row in safety_rows
            ),
        },
    ]

    all_checks_passed = all(
        bool(row["passed"]) for row in checks
    )

    recommended_next_layer = (
        "6ON_layer6_model_projection_realism_"
        "end_to_end_runtime_validation_plan"
    )

    recommended_path = [
        {
            "recommended_next_layer": recommended_next_layer,
            "recommended_action": (
                "Plan end-to-end runtime evidence collection for "
                "the realism diagnostics now that the backend/frontend "
                "contract, frontend build, and manual page render are "
                "verified."
            ),
            "entry_condition": (
                "All 6OM backend, frontend, build, runtime, probability, "
                "and safety checks pass."
            ),
            "passed": all_checks_passed,
        }
    ]

    write_csv(
        OUTPUT_DIR / "checks.csv",
        ["check", "passed"],
        checks,
    )

    write_csv(
        OUTPUT_DIR / "predecessor_audit.csv",
        ["check", "actual", "expected", "passed"],
        predecessor_checks,
    )

    write_csv(
        OUTPUT_DIR / "backend_contract_audit.csv",
        ["check", "actual", "expected", "passed"],
        backend_checks,
    )

    write_csv(
        OUTPUT_DIR / "field_contract_audit.csv",
        [
            "field",
            "present",
            "expected_type",
            "actual_type",
            "value",
            "passed",
        ],
        field_rows,
    )

    write_csv(
        OUTPUT_DIR / "frontend_contract_audit.csv",
        ["check", "actual", "expected", "passed"],
        frontend_checks,
    )

    write_csv(
        OUTPUT_DIR / "probability_guard_audit.csv",
        ["check", "passed", "evidence"],
        probability_guard_checks,
    )

    write_csv(
        OUTPUT_DIR / "safety_audit.csv",
        ["boundary", "changed_or_executed", "passed"],
        safety_rows,
    )

    write_csv(
        OUTPUT_DIR / "recommended_path.csv",
        [
            "recommended_next_layer",
            "recommended_action",
            "entry_condition",
            "passed",
        ],
        recommended_path,
    )

    representative_evidence = {
        "payload_group": "game_state_realism",
        "payload": representative_payload,
        "json_serializable": json_serializable,
        "helper_definition_count": helper_definition_count,
        "helper_runtime_call_count": helper_call_count,
        "per_game_payload_entry_count": (
            per_game_payload_entry_count
        ),
        "payload_entry_inside_games_append": (
            payload_entry_inside_games_append
        ),
        "representative_payload_error": (
            representative_payload_error
        ),
    }

    (
        OUTPUT_DIR / "representative_payload.json"
    ).write_text(
        json.dumps(representative_evidence, indent=2) + "\n",
        encoding="utf-8",
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": (
            "layer_6_model_projection_realism_ui_backend_"
            "contract_check_audit_complete"
            if all_checks_passed
            else
            "layer_6_model_projection_realism_ui_backend_"
            "contract_check_audit_failed"
        ),
        "all_checks_passed": all_checks_passed,
        "payload_group": "game_state_realism",
        "required_contract_fields": len(REQUIRED_FIELDS),
        "required_fields_passed": sum(
            1 for row in field_rows if row["passed"]
        ),
        "helper_runtime_call_count": helper_call_count,
        "per_game_payload_entry_count": (
            per_game_payload_entry_count
        ),
        "representative_payload_json_serializable": (
            json_serializable
        ),
        "safe_frontend_scope_present": safe_render,
        "unsafe_frontend_references_absent": (
            unsafe_references_absent
        ),
        "frontend_build_passed": frontend_build_passed,
        "manual_runtime_confirmed": manual_runtime_confirmed,
        "probability_guard_passed": all(
            bool(row["passed"])
            for row in probability_guard_checks
        ),
        "backend_behavior_change_allowed_next": False,
        "final_probability_replacement_allowed_next": False,
        "historical_validation_allowed_next": False,
        "tuning_allowed_next": False,
        "prediction_join_execution_allowed_next": False,
        "accuracy_metrics_allowed_next": False,
        "backtests_allowed_next": False,
        "pricing_allowed_next": False,
        "edge_detection_allowed_next": False,
        "layer6_exit_recommended": False,
        "recommended_next_layer": recommended_next_layer,
        "generated_csv_artifacts": [
            str(OUTPUT_DIR / "checks.csv"),
            str(OUTPUT_DIR / "predecessor_audit.csv"),
            str(OUTPUT_DIR / "backend_contract_audit.csv"),
            str(OUTPUT_DIR / "field_contract_audit.csv"),
            str(OUTPUT_DIR / "frontend_contract_audit.csv"),
            str(OUTPUT_DIR / "probability_guard_audit.csv"),
            str(OUTPUT_DIR / "safety_audit.csv"),
            str(OUTPUT_DIR / "recommended_path.csv"),
        ],
        "generated_json_artifacts": [
            str(OUTPUT_DIR / "representative_payload.json"),
            str(OUTPUT_DIR / "diagnosis.json"),
        ],
        "generated_log_artifacts": [
            str(BUILD_LOG_PATH),
            str(BUILD_EXIT_PATH),
        ],
    }

    (OUTPUT_DIR / "diagnosis.json").write_text(
        json.dumps(diagnosis, indent=2) + "\n",
        encoding="utf-8",
    )

    print(json.dumps(diagnosis, indent=2))
    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
