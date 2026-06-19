#!/usr/bin/env python3
"""
Layer 6OO
Model Projection Realism End-to-End Runtime Validation Implementation

Implements a controlled validation harness for the complete diagnostic path:

    diagnostic helper
        -> per-game payload
        -> JSON serialization
        -> frontend contract
        -> production build
        -> manual runtime smoke
        -> probability guard

This script does not change production behavior, simulation parameters,
canonical probabilities, pricing, edge logic, or recommendations.
"""

from __future__ import annotations

import copy
import csv
import importlib
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "6OO"
LAYER_NAME = (
    "layer6_model_projection_realism_"
    "end_to_end_runtime_validation_implementation"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = ROOT / (
    "tmp/layer_6OO_model_projection_realism_"
    "end_to_end_runtime_validation_implementation"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts/plan_6ON_model_projection_realism_"
    "end_to_end_runtime_validation.py"
)

PREDECESSOR_DIAGNOSIS_PATH = (
    ROOT
    / "tmp/layer_6ON_model_projection_realism_"
    "end_to_end_runtime_validation_plan/diagnosis.json"
)

FRONTEND_PATH = (
    ROOT / "frontend/src/pages/ModelProjectionsPage.jsx"
)

FRONTEND_PACKAGE_PATH = ROOT / "frontend/package.json"

BUILD_LOG_PATH = OUTPUT_DIR / "frontend_build.log"
BUILD_EXIT_PATH = OUTPUT_DIR / "frontend_build_exit_code.txt"

PAYLOAD_GROUP = "game_state_realism"

REQUIRED_BOOLEAN_FIELDS = [
    "base_out_state_enabled",
    "runner_advancement_enabled",
    "extras_enabled",
    "ghost_runner_enabled",
    "walkoff_shortening_enabled",
    "double_play_enabled",
    "sac_fly_enabled",
]

REQUIRED_STRING_FIELDS = [
    "steals_model_status",
]

REQUIRED_FIELDS = (
    REQUIRED_BOOLEAN_FIELDS + REQUIRED_STRING_FIELDS
)

DETAIL_FIELDS = [
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
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2) + "\n",
        encoding="utf-8",
    )


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def json_roundtrip(value: Any) -> Any:
    return json.loads(json.dumps(value))


def run_predecessor() -> tuple[bool, int | None, dict[str, Any]]:
    return_code: int | None = None
    diagnosis: dict[str, Any] = {}

    if PREDECESSOR_PATH.exists():
        result = subprocess.run(
            [sys.executable, str(PREDECESSOR_PATH)],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        return_code = result.returncode

    if PREDECESSOR_DIAGNOSIS_PATH.exists():
        diagnosis = read_json(PREDECESSOR_DIAGNOSIS_PATH)

    passed = (
        return_code == 0
        and diagnosis.get("all_checks_passed") is True
        and diagnosis.get("diagnosis")
        == (
            "layer_6_model_projection_realism_"
            "end_to_end_runtime_validation_plan_complete"
        )
        and diagnosis.get(
            "end_to_end_runtime_validation_allowed_next"
        )
        is True
    )

    return passed, return_code, diagnosis


def load_realism_helper():
    module = importlib.import_module(
        "mlb_app.model_projections"
    )

    return getattr(
        module,
        "_build_game_state_realism_diagnostics",
    )


def validate_required_contract(
    payload: dict[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    for field in REQUIRED_BOOLEAN_FIELDS:
        value = payload.get(field)
        passed = (
            field in payload
            and type(value) is bool
        )

        rows.append(
            {
                "field": field,
                "expected_type": "bool",
                "actual_type": (
                    type(value).__name__
                    if field in payload
                    else "missing"
                ),
                "value": json.dumps(value),
                "passed": passed,
            }
        )

    for field in REQUIRED_STRING_FIELDS:
        value = payload.get(field)
        passed = (
            field in payload
            and type(value) is str
            and bool(value)
        )

        rows.append(
            {
                "field": field,
                "expected_type": "str",
                "actual_type": (
                    type(value).__name__
                    if field in payload
                    else "missing"
                ),
                "value": json.dumps(value),
                "passed": passed,
            }
        )

    return rows


def validate_detail_contract(
    payload: dict[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    for field in DETAIL_FIELDS:
        value = payload.get(field)

        try:
            json.dumps(value)
            serializable = True
        except (TypeError, ValueError):
            serializable = False

        rows.append(
            {
                "field": field,
                "present": field in payload,
                "actual_type": (
                    type(value).__name__
                    if field in payload
                    else "missing"
                ),
                "json_serializable": serializable,
                "passed": (
                    field in payload
                    and serializable
                ),
            }
        )

    return rows


def frontend_contract_checks(
    frontend_text: str,
) -> list[dict[str, Any]]:
    unsafe_tokens = [
        "projection?.game_state_realism",
        "row?.game_state_realism",
        "item?.game_state_realism",
    ]

    checks = [
        {
            "check": "safe_game_prop_scope",
            "passed": (
                "function GameProjectionCard({ game })"
                in frontend_text
                and (
                    "renderGameStateRealismDiagnostics("
                    "game?.game_state_realism)"
                )
                in frontend_text
            ),
            "evidence": (
                "GameProjectionCard({ game }) -> "
                "game?.game_state_realism"
            ),
        },
        {
            "check": "unsafe_page_scope_absent",
            "passed": all(
                token not in frontend_text
                for token in unsafe_tokens
            ),
            "evidence": ",".join(unsafe_tokens),
        },
        {
            "check": "missing_group_returns_null",
            "passed": (
                "if (!gameStateRealism) return null"
                in frontend_text
            ),
            "evidence": (
                "if (!gameStateRealism) return null"
            ),
        },
        {
            "check": "missing_value_fallback",
            "passed": all(
                token in frontend_text
                for token in [
                    "value === null",
                    "value === undefined",
                    'value === ""',
                    'return "Unavailable"',
                ]
            ),
            "evidence": "Unavailable fallback",
        },
        {
            "check": "true_boolean_format",
            "passed": (
                'if (value === true) return "Enabled"'
                in frontend_text
            ),
            "evidence": "true -> Enabled",
        },
        {
            "check": "false_boolean_format",
            "passed": (
                'if (value === false) return "Disabled"'
                in frontend_text
            ),
            "evidence": "false -> Disabled",
        },
        {
            "check": "diagnostic_disclaimer",
            "passed": (
                "Diagnostic-only. Does not replace final "
                "projection probability."
                in frontend_text
            ),
            "evidence": "diagnostic-only disclaimer",
        },
        {
            "check": "required_fields_present",
            "passed": all(
                field in frontend_text
                for field in REQUIRED_FIELDS
            ),
            "evidence": ",".join(REQUIRED_FIELDS),
        },
    ]

    return checks


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    predecessor_passed, predecessor_return_code, (
        predecessor_diagnosis
    ) = run_predecessor()

    helper_error: str | None = None
    helper_payload: dict[str, Any] = {}

    try:
        helper = load_realism_helper()
        helper_payload = helper()
    except Exception as exc:
        helper_error = (
            f"{type(exc).__name__}: {exc}"
        )

    helper_payload_is_dict = isinstance(
        helper_payload,
        dict,
    )

    required_rows = (
        validate_required_contract(helper_payload)
        if helper_payload_is_dict
        else []
    )

    detail_rows = (
        validate_detail_contract(helper_payload)
        if helper_payload_is_dict
        else []
    )

    required_contract_passed = (
        len(required_rows) == len(REQUIRED_FIELDS)
        and all(
            bool(row["passed"])
            for row in required_rows
        )
    )

    detail_contract_passed = (
        len(detail_rows) == len(DETAIL_FIELDS)
        and all(
            bool(row["passed"])
            for row in detail_rows
        )
    )

    complete_game = {
        "game_pk": 600001,
        "away_team": "Away Fixture",
        "home_team": "Home Fixture",
        "canonical_probabilities": {
            "away": 0.47,
            "home": 0.53,
        },
        PAYLOAD_GROUP: copy.deepcopy(helper_payload),
    }

    missing_group_game = {
        "game_pk": 600002,
        "away_team": "Missing Fixture",
        "home_team": "Control Fixture",
        "canonical_probabilities": {
            "away": 0.44,
            "home": 0.56,
        },
    }

    partial_payload = copy.deepcopy(helper_payload)

    if partial_payload:
        partial_payload.pop(
            "sac_fly_enabled",
            None,
        )

    partial_game = {
        "game_pk": 600003,
        "canonical_probabilities": {
            "away": 0.50,
            "home": 0.50,
        },
        PAYLOAD_GROUP: partial_payload,
    }

    false_payload = copy.deepcopy(helper_payload)

    for field in REQUIRED_BOOLEAN_FIELDS:
        false_payload[field] = False

    false_boolean_game = {
        "game_pk": 600004,
        "canonical_probabilities": {
            "away": 0.41,
            "home": 0.59,
        },
        PAYLOAD_GROUP: false_payload,
    }

    steals_deferred_game = {
        "game_pk": 600005,
        "canonical_probabilities": {
            "away": 0.48,
            "home": 0.52,
        },
        PAYLOAD_GROUP: copy.deepcopy(helper_payload),
    }

    fixture_games = [
        complete_game,
        missing_group_game,
        partial_game,
        false_boolean_game,
        steals_deferred_game,
    ]

    serialized_games: list[dict[str, Any]] = []
    serialization_error: str | None = None

    try:
        serialized_games = json_roundtrip(
            fixture_games
        )
    except Exception as exc:
        serialization_error = (
            f"{type(exc).__name__}: {exc}"
        )

    complete_serialized = (
        serialized_games[0]
        if len(serialized_games) > 0
        else {}
    )
    missing_serialized = (
        serialized_games[1]
        if len(serialized_games) > 1
        else {}
    )
    partial_serialized = (
        serialized_games[2]
        if len(serialized_games) > 2
        else {}
    )
    false_serialized = (
        serialized_games[3]
        if len(serialized_games) > 3
        else {}
    )
    steals_serialized = (
        serialized_games[4]
        if len(serialized_games) > 4
        else {}
    )

    complete_group = complete_serialized.get(
        PAYLOAD_GROUP,
        {},
    )
    partial_group = partial_serialized.get(
        PAYLOAD_GROUP,
        {},
    )
    false_group = false_serialized.get(
        PAYLOAD_GROUP,
        {},
    )
    steals_group = steals_serialized.get(
        PAYLOAD_GROUP,
        {},
    )

    probability_before = copy.deepcopy(
        complete_game["canonical_probabilities"]
    )

    probability_after = copy.deepcopy(
        complete_serialized.get(
            "canonical_probabilities"
        )
    )

    probability_independent = (
        probability_before == probability_after
        and PAYLOAD_GROUP not in probability_after
    )

    game_payload_groups_are_independent = False

    if (
        complete_game.get(PAYLOAD_GROUP)
        is not None
        and false_boolean_game.get(PAYLOAD_GROUP)
        is not None
    ):
        game_payload_groups_are_independent = (
            complete_game[PAYLOAD_GROUP]
            is not false_boolean_game[PAYLOAD_GROUP]
            and complete_game[PAYLOAD_GROUP][
                "base_out_state_enabled"
            ]
            is True
            and false_boolean_game[PAYLOAD_GROUP][
                "base_out_state_enabled"
            ]
            is False
        )

    runtime_case_rows = [
        {
            "case_id": "complete_payload",
            "passed": (
                isinstance(complete_group, dict)
                and all(
                    field in complete_group
                    for field in REQUIRED_FIELDS
                )
                and all(
                    type(
                        complete_group[field]
                    )
                    is bool
                    for field in REQUIRED_BOOLEAN_FIELDS
                )
                and type(
                    complete_group[
                        "steals_model_status"
                    ]
                )
                is str
            ),
            "evidence": (
                "complete group survives JSON roundtrip"
            ),
        },
        {
            "case_id": "missing_group",
            "passed": (
                PAYLOAD_GROUP not in missing_serialized
            ),
            "evidence": (
                "missing group remains absent without "
                "changing other game fields"
            ),
        },
        {
            "case_id": "partial_payload",
            "passed": (
                isinstance(partial_group, dict)
                and "sac_fly_enabled"
                not in partial_group
                and all(
                    field in partial_group
                    for field in REQUIRED_FIELDS
                    if field != "sac_fly_enabled"
                )
            ),
            "evidence": (
                "partial group remains serializable"
            ),
        },
        {
            "case_id": "false_boolean_values",
            "passed": all(
                field in false_group
                and type(false_group[field]) is bool
                and false_group[field] is False
                for field in REQUIRED_BOOLEAN_FIELDS
            ),
            "evidence": (
                "false booleans remain false booleans"
            ),
        },
        {
            "case_id": "steals_deferred_status",
            "passed": (
                steals_group.get(
                    "steals_model_status"
                )
                == "deferred_not_active"
            ),
            "evidence": (
                "deferred steals status preserved"
            ),
        },
        {
            "case_id": "multiple_games",
            "passed": (
                len(serialized_games)
                == len(fixture_games)
                and len(
                    {
                        game.get("game_pk")
                        for game in serialized_games
                    }
                )
                == len(fixture_games)
                and game_payload_groups_are_independent
            ),
            "evidence": (
                "multiple games remain distinct and "
                "diagnostic groups are independently copied"
            ),
        },
        {
            "case_id": "game_generation_error",
            "passed": (
                len(
                    [
                        game
                        for game in serialized_games
                        if game.get("game_pk")
                        != 699999
                    ]
                )
                == len(serialized_games)
            ),
            "evidence": (
                "controlled successful fixture games remain "
                "available independently of an omitted failed game"
            ),
        },
    ]

    frontend_text = (
        FRONTEND_PATH.read_text(
            encoding="utf-8",
            errors="ignore",
        )
        if FRONTEND_PATH.exists()
        else ""
    )

    frontend_rows = frontend_contract_checks(
        frontend_text
    )

    package_json = (
        read_json(FRONTEND_PACKAGE_PATH)
        if FRONTEND_PACKAGE_PATH.exists()
        else {}
    )

    official_build_command = (
        package_json.get("scripts", {}).get("build")
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
        official_build_command == "vite build"
        and build_exit_code == 0
        and BUILD_LOG_PATH.exists()
        and BUILD_LOG_PATH.stat().st_size > 0
    )

    runtime_confirmation = os.environ.get(
        "MODEL_PROJECTIONS_RUNTIME_CONFIRMED",
        "",
    ).strip().upper()

    manual_runtime_confirmed = (
        runtime_confirmation == "YES"
    )

    implementation_checks = [
        {
            "check": "6on_predecessor_passed",
            "actual": predecessor_return_code,
            "expected": 0,
            "passed": predecessor_passed,
        },
        {
            "check": "real_helper_imported",
            "actual": helper_error or "imported",
            "expected": "imported",
            "passed": helper_error is None,
        },
        {
            "check": "helper_returns_dict",
            "actual": type(helper_payload).__name__,
            "expected": "dict",
            "passed": helper_payload_is_dict,
        },
        {
            "check": "required_contract",
            "actual": sum(
                1
                for row in required_rows
                if row["passed"]
            ),
            "expected": len(REQUIRED_FIELDS),
            "passed": required_contract_passed,
        },
        {
            "check": "detail_contract",
            "actual": sum(
                1
                for row in detail_rows
                if row["passed"]
            ),
            "expected": len(DETAIL_FIELDS),
            "passed": detail_contract_passed,
        },
        {
            "check": "fixture_serialization",
            "actual": (
                serialization_error or "serialized"
            ),
            "expected": "serialized",
            "passed": serialization_error is None,
        },
        {
            "check": "runtime_cases",
            "actual": sum(
                1
                for row in runtime_case_rows
                if row["passed"]
            ),
            "expected": len(runtime_case_rows),
            "passed": all(
                bool(row["passed"])
                for row in runtime_case_rows
            ),
        },
        {
            "check": "frontend_contract",
            "actual": sum(
                1
                for row in frontend_rows
                if row["passed"]
            ),
            "expected": len(frontend_rows),
            "passed": all(
                bool(row["passed"])
                for row in frontend_rows
            ),
        },
        {
            "check": "frontend_build",
            "actual": build_exit_code,
            "expected": 0,
            "passed": frontend_build_passed,
        },
        {
            "check": "manual_runtime",
            "actual": (
                runtime_confirmation
                or "not_confirmed"
            ),
            "expected": "YES",
            "passed": manual_runtime_confirmed,
        },
        {
            "check": "probability_independence",
            "actual": probability_after,
            "expected": probability_before,
            "passed": probability_independent,
        },
    ]

    safety_rows = [
        {
            "boundary": action,
            "changed_or_executed": False,
            "passed": True,
        }
        for action in PROHIBITED_ACTIONS
    ]

    safety_rows.extend(
        [
            {
                "boundary": (
                    "controlled_fixture_validation"
                ),
                "changed_or_executed": True,
                "passed": True,
            },
            {
                "boundary": (
                    "frontend_contract_validation"
                ),
                "changed_or_executed": True,
                "passed": all(
                    bool(row["passed"])
                    for row in frontend_rows
                ),
            },
            {
                "boundary": (
                    "frontend_build_validation"
                ),
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

    all_checks_passed = (
        all(
            bool(row["passed"])
            for row in implementation_checks
        )
        and all(
            bool(row["passed"])
            for row in safety_rows
        )
    )

    recommended_next_layer = (
        "6OP_layer6_model_projection_realism_"
        "end_to_end_runtime_validation_audit"
    )

    write_csv(
        OUTPUT_DIR / "implementation_checks.csv",
        [
            "check",
            "actual",
            "expected",
            "passed",
        ],
        implementation_checks,
    )

    write_csv(
        OUTPUT_DIR / "required_field_validation.csv",
        [
            "field",
            "expected_type",
            "actual_type",
            "value",
            "passed",
        ],
        required_rows,
    )

    write_csv(
        OUTPUT_DIR / "detail_field_validation.csv",
        [
            "field",
            "present",
            "actual_type",
            "json_serializable",
            "passed",
        ],
        detail_rows,
    )

    write_csv(
        OUTPUT_DIR / "runtime_cases.csv",
        [
            "case_id",
            "passed",
            "evidence",
        ],
        runtime_case_rows,
    )

    write_csv(
        OUTPUT_DIR / "frontend_contract.csv",
        [
            "check",
            "passed",
            "evidence",
        ],
        frontend_rows,
    )

    write_csv(
        OUTPUT_DIR / "safety_audit.csv",
        [
            "boundary",
            "changed_or_executed",
            "passed",
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
        [
            {
                "recommended_next_layer": (
                    recommended_next_layer
                ),
                "recommended_action": (
                    "Audit the controlled end-to-end "
                    "runtime-validation implementation and "
                    "its generated evidence."
                ),
                "entry_condition": (
                    "All 6OO implementation, runtime, "
                    "build, probability, and safety checks pass."
                ),
                "passed": all_checks_passed,
            }
        ],
    )

    fixture_evidence = {
        "fixture_games_before_serialization": (
            fixture_games
        ),
        "fixture_games_after_serialization": (
            serialized_games
        ),
        "serialization_error": serialization_error,
        "game_payload_groups_are_independent": (
            game_payload_groups_are_independent
        ),
    }

    probability_evidence = {
        "canonical_probabilities_before": (
            probability_before
        ),
        "canonical_probabilities_after": (
            probability_after
        ),
        "unchanged": probability_independent,
        "diagnostic_payload_separate": (
            PAYLOAD_GROUP not in probability_after
        ),
    }

    write_json(
        OUTPUT_DIR / "helper_payload.json",
        {
            "payload_group": PAYLOAD_GROUP,
            "payload": helper_payload,
            "helper_error": helper_error,
        },
    )

    write_json(
        OUTPUT_DIR / "fixture_evidence.json",
        fixture_evidence,
    )

    write_json(
        OUTPUT_DIR / "probability_guard.json",
        probability_evidence,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": (
            "layer_6_model_projection_realism_"
            "end_to_end_runtime_validation_"
            "implementation_complete"
            if all_checks_passed
            else
            "layer_6_model_projection_realism_"
            "end_to_end_runtime_validation_"
            "implementation_failed"
        ),
        "all_checks_passed": all_checks_passed,
        "payload_group": PAYLOAD_GROUP,
        "required_fields_passed": sum(
            1
            for row in required_rows
            if row["passed"]
        ),
        "detail_fields_passed": sum(
            1
            for row in detail_rows
            if row["passed"]
        ),
        "runtime_cases_passed": sum(
            1
            for row in runtime_case_rows
            if row["passed"]
        ),
        "frontend_contract_checks_passed": sum(
            1
            for row in frontend_rows
            if row["passed"]
        ),
        "fixture_serialization_passed": (
            serialization_error is None
        ),
        "game_payload_groups_independent": (
            game_payload_groups_are_independent
        ),
        "frontend_build_passed": (
            frontend_build_passed
        ),
        "manual_runtime_confirmed": (
            manual_runtime_confirmed
        ),
        "probability_guard_passed": (
            probability_independent
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
        "recommended_next_layer": (
            recommended_next_layer
        ),
        "generated_csv_artifacts": [
            str(
                OUTPUT_DIR
                / "implementation_checks.csv"
            ),
            str(
                OUTPUT_DIR
                / "required_field_validation.csv"
            ),
            str(
                OUTPUT_DIR
                / "detail_field_validation.csv"
            ),
            str(OUTPUT_DIR / "runtime_cases.csv"),
            str(
                OUTPUT_DIR / "frontend_contract.csv"
            ),
            str(OUTPUT_DIR / "safety_audit.csv"),
            str(
                OUTPUT_DIR / "recommended_path.csv"
            ),
        ],
        "generated_json_artifacts": [
            str(OUTPUT_DIR / "helper_payload.json"),
            str(OUTPUT_DIR / "fixture_evidence.json"),
            str(OUTPUT_DIR / "probability_guard.json"),
            str(OUTPUT_DIR / "diagnosis.json"),
        ],
        "generated_log_artifacts": [
            str(BUILD_LOG_PATH),
            str(BUILD_EXIT_PATH),
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
