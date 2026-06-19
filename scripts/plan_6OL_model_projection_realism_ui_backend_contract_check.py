#!/usr/bin/env python3
"""
Layer 6OL
Model Projection Realism UI ↔ Backend Contract Check Plan

Planning-only layer.

This script documents the contract that a later audit must verify between:

    mlb_app/model_projections.py
        -> game_state_realism payload
        -> frontend/src/pages/ModelProjectionsPage.jsx
        -> GameProjectionCard({ game })
        -> renderGameStateRealismDiagnostics(game?.game_state_realism)

No production code is changed.
No simulation behavior is changed.
No probability output is replaced.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "6OL"
LAYER_NAME = "layer6_model_projection_realism_ui_backend_contract_check_plan"

OUTPUT_DIR = Path(
    "tmp/layer_6OL_model_projection_realism_ui_backend_contract_check_plan"
)

BACKEND_PATH = Path("mlb_app/model_projections.py")
FRONTEND_PATH = Path("frontend/src/pages/ModelProjectionsPage.jsx")

REQUIRED_FIELDS = [
    ("base_out_state_enabled", "boolean", "Base/out-state simulation is active."),
    (
        "runner_advancement_enabled",
        "boolean",
        "Probabilistic runner advancement is active.",
    ),
    ("extras_enabled", "boolean", "Extra-inning simulation is active."),
    (
        "ghost_runner_enabled",
        "boolean",
        "Automatic extra-inning runner behavior is active.",
    ),
    (
        "walkoff_shortening_enabled",
        "boolean",
        "Home-team walkoff inning shortening is active.",
    ),
    (
        "double_play_enabled",
        "boolean",
        "Double-play state transition logic is active.",
    ),
    (
        "sac_fly_enabled",
        "boolean",
        "Sacrifice-fly state transition logic is active.",
    ),
    (
        "steals_model_status",
        "string",
        "Current steal/caught-stealing model status.",
    ),
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


def contains_all(text: str, tokens: Iterable[str]) -> bool:
    return all(token in text for token in tokens)


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    backend_exists = BACKEND_PATH.exists()
    frontend_exists = FRONTEND_PATH.exists()

    backend_text = (
        BACKEND_PATH.read_text(encoding="utf-8", errors="ignore")
        if backend_exists
        else ""
    )
    frontend_text = (
        FRONTEND_PATH.read_text(encoding="utf-8", errors="ignore")
        if frontend_exists
        else ""
    )

    field_names = [field for field, _, _ in REQUIRED_FIELDS]

    backend_group_present = '"game_state_realism"' in backend_text or (
        "'game_state_realism'" in backend_text
    )
    frontend_group_present = "game?.game_state_realism" in frontend_text
    frontend_renderer_present = (
        "renderGameStateRealismDiagnostics" in frontend_text
    )
    safe_scope_present = (
        "function GameProjectionCard({ game })" in frontend_text
        and "renderGameStateRealismDiagnostics(game?.game_state_realism)"
        in frontend_text
    )

    backend_fields_present = contains_all(backend_text, field_names)
    frontend_fields_present = contains_all(frontend_text, field_names)

    checks = [
        {
            "check": "backend_source_exists",
            "passed": backend_exists,
            "evidence": str(BACKEND_PATH),
        },
        {
            "check": "frontend_source_exists",
            "passed": frontend_exists,
            "evidence": str(FRONTEND_PATH),
        },
        {
            "check": "backend_payload_group_present",
            "passed": backend_group_present,
            "evidence": "game_state_realism",
        },
        {
            "check": "frontend_payload_group_present",
            "passed": frontend_group_present,
            "evidence": "game?.game_state_realism",
        },
        {
            "check": "frontend_renderer_present",
            "passed": frontend_renderer_present,
            "evidence": "renderGameStateRealismDiagnostics",
        },
        {
            "check": "frontend_safe_game_scope_present",
            "passed": safe_scope_present,
            "evidence": (
                "GameProjectionCard({ game }) -> "
                "renderGameStateRealismDiagnostics("
                "game?.game_state_realism)"
            ),
        },
        {
            "check": "backend_required_fields_present",
            "passed": backend_fields_present,
            "evidence": ",".join(field_names),
        },
        {
            "check": "frontend_required_fields_present",
            "passed": frontend_fields_present,
            "evidence": ",".join(field_names),
        },
    ]

    all_checks_passed = all(bool(row["passed"]) for row in checks)

    contract_rows = []
    for field, expected_type, meaning in REQUIRED_FIELDS:
        contract_rows.append(
            {
                "payload_group": "game_state_realism",
                "field": field,
                "expected_type": expected_type,
                "required_for_contract": True,
                "backend_must_emit": True,
                "frontend_must_tolerate_missing": True,
                "frontend_display_fallback": "Unavailable",
                "meaning": meaning,
            }
        )

    frontend_expectations = [
        {
            "expectation": "consumer_scope",
            "required_behavior": (
                "Consume the payload only from the in-scope "
                "GameProjectionCard game prop."
            ),
            "evidence_target": (
                "renderGameStateRealismDiagnostics("
                "game?.game_state_realism)"
            ),
        },
        {
            "expectation": "undeclared_identifier_guard",
            "required_behavior": (
                "Do not reference projection, row, item, or another "
                "undeclared page-scope identifier."
            ),
            "evidence_target": (
                "No unsafe projection?.game_state_realism, "
                "row?.game_state_realism, or item?.game_state_realism "
                "render invocation."
            ),
        },
        {
            "expectation": "missing_group_behavior",
            "required_behavior": (
                "Return null and render no diagnostics panel when the "
                "game_state_realism group is absent."
            ),
            "evidence_target": (
                "if (!gameStateRealism) return null"
            ),
        },
        {
            "expectation": "missing_field_behavior",
            "required_behavior": (
                "Render Unavailable for null, undefined, or empty values."
            ),
            "evidence_target": (
                "formatGameStateRealismValue fallback"
            ),
        },
        {
            "expectation": "diagnostic_only_disclaimer",
            "required_behavior": (
                "Clearly state that the panel does not replace final "
                "projection probability."
            ),
            "evidence_target": (
                "Diagnostic-only. Does not replace final projection "
                "probability."
            ),
        },
        {
            "expectation": "runtime_render_safety",
            "required_behavior": (
                "The Model Projections page must render without a "
                "ReferenceError or blank page."
            ),
            "evidence_target": (
                "frontend build plus manual page-load confirmation"
            ),
        },
    ]

    backend_expectations = [
        {
            "expectation": "payload_location",
            "required_behavior": (
                "Attach game_state_realism to each game object consumed "
                "by GameProjectionCard."
            ),
            "evidence_target": (
                "Serialized game payload returned by model projections "
                "endpoint."
            ),
        },
        {
            "expectation": "stable_group_name",
            "required_behavior": (
                "Use the exact top-level key game_state_realism."
            ),
            "evidence_target": "game_state_realism",
        },
        {
            "expectation": "stable_field_names",
            "required_behavior": (
                "Emit the exact eight documented diagnostic field names."
            ),
            "evidence_target": ",".join(field_names),
        },
        {
            "expectation": "json_serializable_values",
            "required_behavior": (
                "Emit booleans, strings, nulls, or other JSON-safe "
                "diagnostic values."
            ),
            "evidence_target": (
                "Endpoint serialization succeeds without custom frontend "
                "coercion."
            ),
        },
        {
            "expectation": "diagnostic_only",
            "required_behavior": (
                "Do not use game_state_realism to replace the final "
                "projection probability."
            ),
            "evidence_target": (
                "Probability guard remains unchanged."
            ),
        },
        {
            "expectation": "per_game_consistency",
            "required_behavior": (
                "Use the same payload shape for every game, including "
                "games with incomplete projection inputs."
            ),
            "evidence_target": (
                "Representative complete and incomplete game payloads."
            ),
        },
    ]

    validation_plan = [
        {
            "step": 1,
            "validation_type": "static_backend_contract",
            "action": (
                "Locate the exact backend return object that feeds the "
                "Model Projections endpoint."
            ),
            "success_criterion": (
                "game_state_realism is attached at the game-object level."
            ),
        },
        {
            "step": 2,
            "validation_type": "static_frontend_contract",
            "action": (
                "Confirm GameProjectionCard consumes "
                "game?.game_state_realism."
            ),
            "success_criterion": (
                "No undeclared variable is used by the render invocation."
            ),
        },
        {
            "step": 3,
            "validation_type": "field_contract",
            "action": (
                "Compare backend and frontend field sets."
            ),
            "success_criterion": (
                "All eight required names match exactly."
            ),
        },
        {
            "step": 4,
            "validation_type": "representative_payload",
            "action": (
                "Capture at least one real or fixture endpoint payload."
            ),
            "success_criterion": (
                "The game object contains a JSON-serializable "
                "game_state_realism group."
            ),
        },
        {
            "step": 5,
            "validation_type": "missing_payload_case",
            "action": (
                "Exercise a game payload without game_state_realism."
            ),
            "success_criterion": (
                "The page renders and the diagnostics panel is omitted."
            ),
        },
        {
            "step": 6,
            "validation_type": "partial_payload_case",
            "action": (
                "Exercise a payload with one or more missing diagnostic "
                "fields."
            ),
            "success_criterion": (
                "Missing values display as Unavailable without crashing."
            ),
        },
        {
            "step": 7,
            "validation_type": "frontend_build",
            "action": (
                "Run the repository-supported frontend build command "
                "identified from package.json."
            ),
            "success_criterion": (
                "Build exits successfully."
            ),
        },
        {
            "step": 8,
            "validation_type": "manual_runtime_smoke",
            "action": (
                "Load the Model Projections page with populated data."
            ),
            "success_criterion": (
                "Page renders, game cards remain usable, and diagnostics "
                "appear only when payload data exists."
            ),
        },
        {
            "step": 9,
            "validation_type": "probability_guard",
            "action": (
                "Verify diagnostic fields do not replace or modify final "
                "projection probability."
            ),
            "success_criterion": (
                "Existing projection probability path is unchanged."
            ),
        },
    ]

    safety_rows = [
        {
            "boundary": action,
            "allowed_in_6OL": False,
            "reason": (
                "6OL is planning-only and may not execute downstream "
                "model, validation, pricing, or edge work."
            ),
        }
        for action in PROHIBITED_ACTIONS
    ]

    safety_rows.extend(
        [
            {
                "boundary": "source_inventory",
                "allowed_in_6OL": True,
                "reason": (
                    "Static source inventory is required to construct the "
                    "contract-check plan."
                ),
            },
            {
                "boundary": "contract_definition",
                "allowed_in_6OL": True,
                "reason": (
                    "Defining expected payload shape is the purpose of "
                    "this planning layer."
                ),
            },
            {
                "boundary": "validation_procedure_design",
                "allowed_in_6OL": True,
                "reason": (
                    "The layer may specify, but not yet execute, runtime "
                    "contract validation."
                ),
            },
        ]
    )

    recommended_path = [
        {
            "recommended_next_layer": (
                "6OM_layer6_model_projection_realism_ui_backend_"
                "contract_check_audit"
            ),
            "recommended_action": (
                "Execute the contract audit using representative backend "
                "payload evidence, exact frontend field matching, "
                "frontend build validation, and manual runtime smoke "
                "confirmation."
            ),
            "entry_condition": (
                "6OL plan checks pass and no production behavior changes "
                "are introduced."
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
        OUTPUT_DIR / "contract_fields.csv",
        [
            "payload_group",
            "field",
            "expected_type",
            "required_for_contract",
            "backend_must_emit",
            "frontend_must_tolerate_missing",
            "frontend_display_fallback",
            "meaning",
        ],
        contract_rows,
    )

    write_csv(
        OUTPUT_DIR / "frontend_expectations.csv",
        ["expectation", "required_behavior", "evidence_target"],
        frontend_expectations,
    )

    write_csv(
        OUTPUT_DIR / "backend_expectations.csv",
        ["expectation", "required_behavior", "evidence_target"],
        backend_expectations,
    )

    write_csv(
        OUTPUT_DIR / "validation_plan.csv",
        [
            "step",
            "validation_type",
            "action",
            "success_criterion",
        ],
        validation_plan,
    )

    write_csv(
        OUTPUT_DIR / "safety_boundaries.csv",
        ["boundary", "allowed_in_6OL", "reason"],
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
            "layer_6_model_projection_realism_ui_backend_contract_"
            "check_plan_complete"
            if all_checks_passed
            else
            "layer_6_model_projection_realism_ui_backend_contract_"
            "check_plan_failed"
        ),
        "all_checks_passed": all_checks_passed,
        "payload_group": "game_state_realism",
        "required_contract_fields": len(REQUIRED_FIELDS),
        "backend_fields_present": sum(
            1 for field, _, _ in REQUIRED_FIELDS if field in backend_text
        ),
        "frontend_fields_present": sum(
            1 for field, _, _ in REQUIRED_FIELDS if field in frontend_text
        ),
        "safe_frontend_scope_present": safe_scope_present,
        "runtime_contract_audit_allowed_next": all_checks_passed,
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
        "recommended_next_layer": (
            "6OM_layer6_model_projection_realism_ui_backend_"
            "contract_check_audit"
        ),
        "generated_csv_artifacts": [
            str(OUTPUT_DIR / "checks.csv"),
            str(OUTPUT_DIR / "contract_fields.csv"),
            str(OUTPUT_DIR / "frontend_expectations.csv"),
            str(OUTPUT_DIR / "backend_expectations.csv"),
            str(OUTPUT_DIR / "validation_plan.csv"),
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
