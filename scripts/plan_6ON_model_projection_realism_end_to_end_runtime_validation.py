#!/usr/bin/env python3
"""
Layer 6ON
Model Projection Realism End-to-End Runtime Validation Plan

Planning-only layer.

Defines the evidence required to validate the complete runtime path:

    simulation realism mechanics
        -> diagnostic helper
        -> per-game backend payload
        -> serialized endpoint response
        -> frontend game object
        -> GameProjectionCard diagnostics panel

No production behavior is changed.
No simulation parameter is changed.
No final projection probability is replaced.
No historical outcome validation, tuning, pricing, or edge detection is run.
"""

from __future__ import annotations

import ast
import csv
import json
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "6ON"
LAYER_NAME = (
    "layer6_model_projection_realism_end_to_end_runtime_validation_plan"
)

OUTPUT_DIR = Path(
    "tmp/layer_6ON_model_projection_realism_"
    "end_to_end_runtime_validation_plan"
)

PREDECESSOR_PATH = Path(
    "scripts/audit_6OM_model_projection_realism_ui_"
    "backend_contract_check.py"
)
BACKEND_PATH = Path("mlb_app/model_projections.py")
FRONTEND_PATH = Path(
    "frontend/src/pages/ModelProjectionsPage.jsx"
)
PACKAGE_PATH = Path("frontend/package.json")

PAYLOAD_GROUP = "game_state_realism"

REQUIRED_FIELDS = [
    "base_out_state_enabled",
    "runner_advancement_enabled",
    "extras_enabled",
    "ghost_runner_enabled",
    "walkoff_shortening_enabled",
    "double_play_enabled",
    "sac_fly_enabled",
    "steals_model_status",
]

DIAGNOSTIC_DETAIL_FIELDS = [
    "base_out_transition_model_status",
    "base_out_simulation_summary",
    "runner_advancement_model_status",
    "runner_advancement_summary",
    "extras_walkoff_model_status",
    "double_play_rate_source",
    "double_play_transition_summary",
    "sac_fly_rate_source",
    "sac_fly_transition_summary",
    "steals_projection_wiring_status",
]

PROHIBITED_ACTIONS = [
    "backend_behavior_change",
    "frontend_behavior_change",
    "simulation_parameter_change",
    "simulation_probability_change",
    "canonical_probability_replacement",
    "historical_outcome_join",
    "accuracy_metric_generation",
    "parameter_tuning",
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


def inspect_backend_payload_wiring(
    backend_text: str,
) -> dict[str, Any]:
    tree = ast.parse(backend_text)

    helper_definitions = 0
    helper_calls = 0
    per_game_payload_entries = 0
    entry_inside_games_append = False

    for node in ast.walk(tree):
        if (
            isinstance(node, ast.FunctionDef)
            and node.name
            == "_build_game_state_realism_diagnostics"
        ):
            helper_definitions += 1

        if isinstance(node, ast.Call):
            if (
                isinstance(node.func, ast.Name)
                and node.func.id
                == "_build_game_state_realism_diagnostics"
            ):
                helper_calls += 1

            is_games_append = (
                isinstance(node.func, ast.Attribute)
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == "games"
                and node.func.attr == "append"
                and len(node.args) == 1
                and isinstance(node.args[0], ast.Dict)
            )

            if not is_games_append:
                continue

            payload = node.args[0]

            for key, value in zip(payload.keys, payload.values):
                key_matches = (
                    isinstance(key, ast.Constant)
                    and key.value == PAYLOAD_GROUP
                )
                value_matches = (
                    isinstance(value, ast.Call)
                    and isinstance(value.func, ast.Name)
                    and value.func.id
                    == "_build_game_state_realism_diagnostics"
                )

                if key_matches and value_matches:
                    per_game_payload_entries += 1
                    entry_inside_games_append = True

    return {
        "helper_definition_count": helper_definitions,
        "helper_runtime_call_count": helper_calls,
        "per_game_payload_entry_count": (
            per_game_payload_entries
        ),
        "entry_inside_games_append": (
            entry_inside_games_append
        ),
    }


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    paths = [
        PREDECESSOR_PATH,
        BACKEND_PATH,
        FRONTEND_PATH,
        PACKAGE_PATH,
    ]

    sources_exist = all(path.exists() for path in paths)

    predecessor_text = (
        PREDECESSOR_PATH.read_text(
            encoding="utf-8",
            errors="ignore",
        )
        if PREDECESSOR_PATH.exists()
        else ""
    )
    backend_text = (
        BACKEND_PATH.read_text(
            encoding="utf-8",
            errors="ignore",
        )
        if BACKEND_PATH.exists()
        else ""
    )
    frontend_text = (
        FRONTEND_PATH.read_text(
            encoding="utf-8",
            errors="ignore",
        )
        if FRONTEND_PATH.exists()
        else ""
    )

    package_json = (
        json.loads(PACKAGE_PATH.read_text(encoding="utf-8"))
        if PACKAGE_PATH.exists()
        else {}
    )

    wiring = (
        inspect_backend_payload_wiring(backend_text)
        if backend_text
        else {
            "helper_definition_count": 0,
            "helper_runtime_call_count": 0,
            "per_game_payload_entry_count": 0,
            "entry_inside_games_append": False,
        }
    )

    predecessor_complete_contract_present = False

    if predecessor_text:
        predecessor_tree = ast.parse(predecessor_text)
        predecessor_string_constants = {
            node.value
            for node in ast.walk(predecessor_tree)
            if isinstance(node, ast.Constant)
            and isinstance(node.value, str)
        }

        predecessor_complete_contract_present = all(
            token in predecessor_string_constants
            for token in [
                "frontend_build_passed",
                "manual_runtime_confirmed",
                "probability_guard_passed",
                "recommended_next_layer",
                (
                    "layer_6_model_projection_realism_ui_backend_"
                    "contract_check_audit_complete"
                ),
                (
                    "6ON_layer6_model_projection_realism_"
                    "end_to_end_runtime_validation_plan"
                ),
            ]
        )

    backend_required_fields_present = all(
        field in backend_text
        for field in REQUIRED_FIELDS
    )

    frontend_required_fields_present = all(
        field in frontend_text
        for field in REQUIRED_FIELDS
    )

    backend_detail_fields_present = all(
        field in backend_text
        for field in DIAGNOSTIC_DETAIL_FIELDS
    )

    frontend_safe_consumer_present = all(
        token in frontend_text
        for token in [
            "function GameProjectionCard({ game })",
            (
                "renderGameStateRealismDiagnostics("
                "game?.game_state_realism)"
            ),
            "if (!gameStateRealism) return null",
            'return "Unavailable"',
            (
                "Diagnostic-only. Does not replace final "
                "projection probability."
            ),
        ]
    )

    official_build_command_present = (
        package_json.get("scripts", {}).get("build")
        == "vite build"
    )

    probability_guard_present = all(
        token in backend_text
        for token in [
            (
                "canonical_probabilities = "
                "_canonical_probability_payload"
            ),
            "diagnostic_only_not_final_probability",
            (
                '"game_state_realism": '
                "_build_game_state_realism_diagnostics()"
            ),
        ]
    )

    checks = [
        {
            "check": "required_source_files_exist",
            "passed": sources_exist,
            "evidence": ",".join(str(path) for path in paths),
        },
        {
            "check": "6om_predecessor_contract_present",
            "passed": predecessor_complete_contract_present,
            "evidence": str(PREDECESSOR_PATH),
        },
        {
            "check": "backend_helper_definition_count",
            "passed": (
                wiring["helper_definition_count"] == 1
            ),
            "evidence": wiring["helper_definition_count"],
        },
        {
            "check": "backend_helper_runtime_call_count",
            "passed": (
                wiring["helper_runtime_call_count"] == 1
            ),
            "evidence": wiring["helper_runtime_call_count"],
        },
        {
            "check": "per_game_payload_entry_count",
            "passed": (
                wiring["per_game_payload_entry_count"] == 1
            ),
            "evidence": wiring["per_game_payload_entry_count"],
        },
        {
            "check": "payload_entry_inside_games_append",
            "passed": wiring["entry_inside_games_append"],
            "evidence": "games.append({...})",
        },
        {
            "check": "backend_required_fields_present",
            "passed": backend_required_fields_present,
            "evidence": ",".join(REQUIRED_FIELDS),
        },
        {
            "check": "backend_detail_fields_present",
            "passed": backend_detail_fields_present,
            "evidence": ",".join(DIAGNOSTIC_DETAIL_FIELDS),
        },
        {
            "check": "frontend_required_fields_present",
            "passed": frontend_required_fields_present,
            "evidence": ",".join(REQUIRED_FIELDS),
        },
        {
            "check": "frontend_safe_consumer_present",
            "passed": frontend_safe_consumer_present,
            "evidence": (
                "GameProjectionCard game prop and safe renderer"
            ),
        },
        {
            "check": "official_frontend_build_command_present",
            "passed": official_build_command_present,
            "evidence": (
                package_json.get("scripts", {}).get("build")
            ),
        },
        {
            "check": "probability_guard_present",
            "passed": probability_guard_present,
            "evidence": (
                "canonical probability path remains separate "
                "from diagnostics"
            ),
        },
    ]

    all_checks_passed = all(
        bool(row["passed"]) for row in checks
    )

    evidence_sources = [
        {
            "stage": "mechanic_runtime",
            "source": (
                "shared simulation derived outputs and installed "
                "game-state transition logic"
            ),
            "required_evidence": (
                "Runtime evidence that enabled mechanics are reached "
                "during representative simulation execution."
            ),
            "collection_method": (
                "Capture structured runtime diagnostics from a "
                "representative model-projection request."
            ),
        },
        {
            "stage": "diagnostic_helper",
            "source": (
                "mlb_app.model_projections."
                "_build_game_state_realism_diagnostics"
            ),
            "required_evidence": (
                "JSON-serializable helper output containing all "
                "required fields."
            ),
            "collection_method": (
                "Import through the mlb_app package and execute helper."
            ),
        },
        {
            "stage": "per_game_payload",
            "source": (
                "build_model_projection_payload games list"
            ),
            "required_evidence": (
                "Every successfully produced game contains the "
                "game_state_realism group."
            ),
            "collection_method": (
                "Execute payload construction with a controlled "
                "session or fixture."
            ),
        },
        {
            "stage": "endpoint_serialization",
            "source": (
                "Model Projections API response"
            ),
            "required_evidence": (
                "Serialized response preserves the group and values "
                "without type loss."
            ),
            "collection_method": (
                "Capture endpoint JSON from the repository-supported "
                "runtime or test client."
            ),
        },
        {
            "stage": "frontend_consumption",
            "source": (
                "GameProjectionCard({ game })"
            ),
            "required_evidence": (
                "The exact serialized game object reaches the safe "
                "frontend renderer."
            ),
            "collection_method": (
                "Use fixture rendering or browser/runtime inspection."
            ),
        },
        {
            "stage": "visible_ui",
            "source": (
                "Game-State Realism Diagnostics panel"
            ),
            "required_evidence": (
                "Required values render with the diagnostic-only "
                "disclaimer."
            ),
            "collection_method": (
                "Frontend component test, browser smoke, or captured "
                "runtime evidence."
            ),
        },
    ]

    runtime_cases = [
        {
            "case_id": "complete_payload",
            "input_condition": (
                "Game payload contains the full game_state_realism "
                "group and all required fields."
            ),
            "backend_expectation": (
                "Group serializes without error."
            ),
            "frontend_expectation": (
                "Panel renders all eight required values."
            ),
            "blocking": True,
        },
        {
            "case_id": "missing_group",
            "input_condition": (
                "Game payload does not contain game_state_realism."
            ),
            "backend_expectation": (
                "No unrelated payload behavior changes."
            ),
            "frontend_expectation": (
                "Renderer returns null and page remains usable."
            ),
            "blocking": True,
        },
        {
            "case_id": "partial_payload",
            "input_condition": (
                "One or more required fields are absent."
            ),
            "backend_expectation": (
                "Partial payload remains JSON serializable."
            ),
            "frontend_expectation": (
                "Missing fields display as Unavailable."
            ),
            "blocking": True,
        },
        {
            "case_id": "false_boolean_values",
            "input_condition": (
                "One or more boolean diagnostic fields are false."
            ),
            "backend_expectation": (
                "False values remain booleans after serialization."
            ),
            "frontend_expectation": (
                "False renders as Disabled rather than Unavailable."
            ),
            "blocking": True,
        },
        {
            "case_id": "steals_deferred_status",
            "input_condition": (
                "steals_model_status is deferred_not_active."
            ),
            "backend_expectation": (
                "Status is preserved as a string."
            ),
            "frontend_expectation": (
                "Status is shown without implying active steal logic."
            ),
            "blocking": True,
        },
        {
            "case_id": "multiple_games",
            "input_condition": (
                "Response contains multiple successfully generated games."
            ),
            "backend_expectation": (
                "Each game receives an independent diagnostics group."
            ),
            "frontend_expectation": (
                "Each populated game card may render its own panel."
            ),
            "blocking": True,
        },
        {
            "case_id": "game_generation_error",
            "input_condition": (
                "One matchup fails while another succeeds."
            ),
            "backend_expectation": (
                "Successful games retain diagnostics and errors remain "
                "in the existing errors collection."
            ),
            "frontend_expectation": (
                "Successful cards remain renderable."
            ),
            "blocking": True,
        },
    ]

    payload_assertions = [
        {
            "assertion": "payload_group_name",
            "expected": PAYLOAD_GROUP,
            "scope": "each successful game object",
            "blocking": True,
        },
        {
            "assertion": "required_field_count",
            "expected": len(REQUIRED_FIELDS),
            "scope": PAYLOAD_GROUP,
            "blocking": True,
        },
        {
            "assertion": "boolean_field_types",
            "expected": (
                "Seven required enabled fields are JSON booleans."
            ),
            "scope": PAYLOAD_GROUP,
            "blocking": True,
        },
        {
            "assertion": "steals_status_type",
            "expected": (
                "steals_model_status is a JSON string."
            ),
            "scope": PAYLOAD_GROUP,
            "blocking": True,
        },
        {
            "assertion": "diagnostic_detail_preservation",
            "expected": (
                "Nested diagnostic summaries remain JSON serializable."
            ),
            "scope": PAYLOAD_GROUP,
            "blocking": True,
        },
        {
            "assertion": "canonical_probability_independence",
            "expected": (
                "Canonical side probabilities are unchanged by "
                "game_state_realism serialization."
            ),
            "scope": "per-game projection payload",
            "blocking": True,
        },
    ]

    ui_assertions = [
        {
            "assertion": "safe_prop_scope",
            "expected": (
                "Renderer consumes game?.game_state_realism only."
            ),
            "blocking": True,
        },
        {
            "assertion": "complete_field_render",
            "expected": (
                "All eight required fields appear for complete payloads."
            ),
            "blocking": True,
        },
        {
            "assertion": "missing_group_no_crash",
            "expected": (
                "Absent group produces no panel and no page error."
            ),
            "blocking": True,
        },
        {
            "assertion": "missing_field_fallback",
            "expected": (
                "Absent, null, or empty field displays Unavailable."
            ),
            "blocking": True,
        },
        {
            "assertion": "false_boolean_format",
            "expected": (
                "False boolean displays Disabled."
            ),
            "blocking": True,
        },
        {
            "assertion": "true_boolean_format",
            "expected": (
                "True boolean displays Enabled."
            ),
            "blocking": True,
        },
        {
            "assertion": "diagnostic_disclaimer",
            "expected": (
                "Panel explicitly states it does not replace final "
                "projection probability."
            ),
            "blocking": True,
        },
        {
            "assertion": "production_build",
            "expected": (
                "npm run build exits successfully after dependency "
                "installation."
            ),
            "blocking": True,
        },
    ]

    observability_plan = [
        {
            "signal": "request_target_date",
            "stage": "backend_request",
            "required": True,
            "purpose": (
                "Identify the runtime request used for evidence."
            ),
        },
        {
            "signal": "game_pk",
            "stage": "per_game_payload",
            "required": True,
            "purpose": (
                "Correlate backend payload, endpoint JSON, and UI card."
            ),
        },
        {
            "signal": PAYLOAD_GROUP,
            "stage": "endpoint_response",
            "required": True,
            "purpose": (
                "Prove group survives serialization."
            ),
        },
        {
            "signal": "required_field_values",
            "stage": "endpoint_response",
            "required": True,
            "purpose": (
                "Prove exact field values and JSON types."
            ),
        },
        {
            "signal": "canonical_probabilities_before_after",
            "stage": "probability_guard",
            "required": True,
            "purpose": (
                "Prove diagnostics do not alter canonical probabilities."
            ),
        },
        {
            "signal": "frontend_render_result",
            "stage": "frontend_runtime",
            "required": True,
            "purpose": (
                "Prove panel visibility or safe omission."
            ),
        },
        {
            "signal": "frontend_build_exit_code",
            "stage": "frontend_build",
            "required": True,
            "purpose": (
                "Prove production compilation succeeds."
            ),
        },
    ]

    execution_sequence = [
        {
            "step": 1,
            "action": (
                "Install frontend dependencies using the repository "
                "lockfile."
            ),
            "success_criterion": (
                "Required packages, including recharts, resolve locally."
            ),
        },
        {
            "step": 2,
            "action": (
                "Run syntax compilation without writing pyc files."
            ),
            "success_criterion": (
                "All Python sources compile."
            ),
        },
        {
            "step": 3,
            "action": (
                "Execute the diagnostic helper through the mlb_app "
                "package."
            ),
            "success_criterion": (
                "All required fields and expected JSON types pass."
            ),
        },
        {
            "step": 4,
            "action": (
                "Construct or capture at least one representative "
                "per-game payload."
            ),
            "success_criterion": (
                "game_state_realism is present on each successful game."
            ),
        },
        {
            "step": 5,
            "action": (
                "Capture serialized endpoint or test-client JSON."
            ),
            "success_criterion": (
                "Payload group and field types survive serialization."
            ),
        },
        {
            "step": 6,
            "action": (
                "Exercise complete, missing, partial, false-value, "
                "multi-game, and error-isolation cases."
            ),
            "success_criterion": (
                "All backend and frontend expectations pass."
            ),
        },
        {
            "step": 7,
            "action": (
                "Run the official frontend production build."
            ),
            "success_criterion": (
                "npm run build exits with code 0."
            ),
        },
        {
            "step": 8,
            "action": (
                "Confirm the deployed or local runtime page renders "
                "the expected diagnostics."
            ),
            "success_criterion": (
                "No blank page; cards remain usable; disclaimer visible."
            ),
        },
        {
            "step": 9,
            "action": (
                "Compare canonical probabilities before and after "
                "diagnostic payload inclusion."
            ),
            "success_criterion": (
                "No probability value or selection path changes."
            ),
        },
    ]

    safety_rows = [
        {
            "boundary": action,
            "allowed_in_6ON": False,
            "reason": (
                "6ON defines validation evidence only and may not "
                "change or evaluate downstream model performance."
            ),
        }
        for action in PROHIBITED_ACTIONS
    ]

    safety_rows.extend(
        [
            {
                "boundary": "runtime_validation_design",
                "allowed_in_6ON": True,
                "reason": (
                    "Designing the end-to-end evidence procedure is "
                    "the purpose of this layer."
                ),
            },
            {
                "boundary": "fixture_case_definition",
                "allowed_in_6ON": True,
                "reason": (
                    "Defining controlled runtime cases is planning-only."
                ),
            },
            {
                "boundary": "observability_definition",
                "allowed_in_6ON": True,
                "reason": (
                    "Defining required evidence does not alter behavior."
                ),
            },
        ]
    )

    recommended_next_layer = (
        "6OO_layer6_model_projection_realism_"
        "end_to_end_runtime_validation_implementation"
    )

    recommended_path = [
        {
            "recommended_next_layer": recommended_next_layer,
            "recommended_action": (
                "Implement the controlled end-to-end runtime validation "
                "harness and produce backend, serialization, frontend, "
                "build, runtime, and probability-guard evidence."
            ),
            "entry_condition": (
                "All 6ON planning checks pass and no production "
                "behavior changes are introduced."
            ),
            "passed": all_checks_passed,
        }
    ]

    write_csv(
        OUTPUT_DIR / "checks.csv",
        ["check", "passed", "evidence"],
        checks,
    )

    write_csv(
        OUTPUT_DIR / "evidence_sources.csv",
        [
            "stage",
            "source",
            "required_evidence",
            "collection_method",
        ],
        evidence_sources,
    )

    write_csv(
        OUTPUT_DIR / "runtime_cases.csv",
        [
            "case_id",
            "input_condition",
            "backend_expectation",
            "frontend_expectation",
            "blocking",
        ],
        runtime_cases,
    )

    write_csv(
        OUTPUT_DIR / "payload_assertions.csv",
        [
            "assertion",
            "expected",
            "scope",
            "blocking",
        ],
        payload_assertions,
    )

    write_csv(
        OUTPUT_DIR / "ui_assertions.csv",
        [
            "assertion",
            "expected",
            "blocking",
        ],
        ui_assertions,
    )

    write_csv(
        OUTPUT_DIR / "observability_plan.csv",
        [
            "signal",
            "stage",
            "required",
            "purpose",
        ],
        observability_plan,
    )

    write_csv(
        OUTPUT_DIR / "execution_sequence.csv",
        [
            "step",
            "action",
            "success_criterion",
        ],
        execution_sequence,
    )

    write_csv(
        OUTPUT_DIR / "safety_boundaries.csv",
        [
            "boundary",
            "allowed_in_6ON",
            "reason",
        ],
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

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": (
            "layer_6_model_projection_realism_"
            "end_to_end_runtime_validation_plan_complete"
            if all_checks_passed
            else
            "layer_6_model_projection_realism_"
            "end_to_end_runtime_validation_plan_failed"
        ),
        "all_checks_passed": all_checks_passed,
        "payload_group": PAYLOAD_GROUP,
        "required_contract_fields": len(REQUIRED_FIELDS),
        "diagnostic_detail_fields": len(
            DIAGNOSTIC_DETAIL_FIELDS
        ),
        "runtime_cases_planned": len(runtime_cases),
        "payload_assertions_planned": len(
            payload_assertions
        ),
        "ui_assertions_planned": len(ui_assertions),
        "observability_signals_planned": len(
            observability_plan
        ),
        "execution_steps_planned": len(
            execution_sequence
        ),
        "helper_runtime_call_count": wiring[
            "helper_runtime_call_count"
        ],
        "per_game_payload_entry_count": wiring[
            "per_game_payload_entry_count"
        ],
        "end_to_end_runtime_validation_allowed_next": (
            all_checks_passed
        ),
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
        "layer6_exit_recommended": False,
        "recommended_next_layer": recommended_next_layer,
        "generated_csv_artifacts": [
            str(OUTPUT_DIR / "checks.csv"),
            str(OUTPUT_DIR / "evidence_sources.csv"),
            str(OUTPUT_DIR / "runtime_cases.csv"),
            str(OUTPUT_DIR / "payload_assertions.csv"),
            str(OUTPUT_DIR / "ui_assertions.csv"),
            str(OUTPUT_DIR / "observability_plan.csv"),
            str(OUTPUT_DIR / "execution_sequence.csv"),
            str(OUTPUT_DIR / "safety_boundaries.csv"),
            str(OUTPUT_DIR / "recommended_path.csv"),
        ],
        "generated_json_artifacts": [
            str(OUTPUT_DIR / "diagnosis.json"),
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
