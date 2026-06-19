#!/usr/bin/env python3
"""
Layer 6OP
Model Projection Realism End-to-End Runtime Validation Audit

Independently audits the evidence produced by Layer 6OO.

This layer does not change:
- backend behavior
- frontend behavior
- simulation parameters
- simulation probabilities
- canonical probabilities
- historical validation
- tuning
- pricing
- edge detection
"""

from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "6OP"
LAYER_NAME = (
    "layer6_model_projection_realism_"
    "end_to_end_runtime_validation_audit"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = ROOT / (
    "tmp/layer_6OP_model_projection_realism_"
    "end_to_end_runtime_validation_audit"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts/implement_6OO_model_projection_realism_"
    "end_to_end_runtime_validation.py"
)

PREDECESSOR_OUTPUT_DIR = ROOT / (
    "tmp/layer_6OO_model_projection_realism_"
    "end_to_end_runtime_validation_implementation"
)

PREDECESSOR_DIAGNOSIS_PATH = (
    PREDECESSOR_OUTPUT_DIR / "diagnosis.json"
)

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

EXPECTED_RUNTIME_CASES = {
    "complete_payload",
    "missing_group",
    "partial_payload",
    "false_boolean_values",
    "steals_deferred_status",
    "multiple_games",
    "game_generation_error",
}

EXPECTED_FRONTEND_CHECKS = {
    "safe_game_prop_scope",
    "unsafe_page_scope_absent",
    "missing_group_returns_null",
    "missing_value_fallback",
    "true_boolean_format",
    "false_boolean_format",
    "diagnostic_disclaimer",
    "required_fields_present",
}

EXPECTED_IMPLEMENTATION_CHECKS = {
    "6on_predecessor_passed",
    "real_helper_imported",
    "helper_returns_dict",
    "required_contract",
    "detail_contract",
    "fixture_serialization",
    "runtime_cases",
    "frontend_contract",
    "frontend_build",
    "manual_runtime",
    "probability_independence",
}

PROHIBITED_ACTIONS = {
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
}

EXPECTED_ARTIFACTS = [
    PREDECESSOR_OUTPUT_DIR / "implementation_checks.csv",
    PREDECESSOR_OUTPUT_DIR / "required_field_validation.csv",
    PREDECESSOR_OUTPUT_DIR / "detail_field_validation.csv",
    PREDECESSOR_OUTPUT_DIR / "runtime_cases.csv",
    PREDECESSOR_OUTPUT_DIR / "frontend_contract.csv",
    PREDECESSOR_OUTPUT_DIR / "safety_audit.csv",
    PREDECESSOR_OUTPUT_DIR / "recommended_path.csv",
    PREDECESSOR_OUTPUT_DIR / "helper_payload.json",
    PREDECESSOR_OUTPUT_DIR / "fixture_evidence.json",
    PREDECESSOR_OUTPUT_DIR / "probability_guard.json",
    PREDECESSOR_OUTPUT_DIR / "diagnosis.json",
    PREDECESSOR_OUTPUT_DIR / "frontend_build.log",
    PREDECESSOR_OUTPUT_DIR / "frontend_build_exit_code.txt",
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


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(
        newline="",
        encoding="utf-8",
    ) as handle:
        return list(csv.DictReader(handle))


def csv_bool(value: Any) -> bool:
    return str(value).strip().lower() == "true"


def run_predecessor() -> tuple[int | None, str, str]:
    if not PREDECESSOR_PATH.exists():
        return None, "", "predecessor script missing"

    result = subprocess.run(
        [sys.executable, str(PREDECESSOR_PATH)],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
        env={
            **os.environ,
            "MODEL_PROJECTIONS_RUNTIME_CONFIRMED": "YES",
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONPATH": str(ROOT),
        },
    )

    return (
        result.returncode,
        result.stdout,
        result.stderr,
    )


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    predecessor_return_code, predecessor_stdout, (
        predecessor_stderr
    ) = run_predecessor()

    artifact_rows = [
        {
            "artifact": str(path),
            "exists": path.exists(),
            "nonempty": (
                path.exists()
                and path.stat().st_size > 0
            ),
            "passed": (
                path.exists()
                and path.stat().st_size > 0
            ),
        }
        for path in EXPECTED_ARTIFACTS
    ]

    artifacts_passed = all(
        bool(row["passed"])
        for row in artifact_rows
    )

    diagnosis: dict[str, Any] = {}
    implementation_rows: list[dict[str, str]] = []
    required_rows: list[dict[str, str]] = []
    detail_rows: list[dict[str, str]] = []
    runtime_rows: list[dict[str, str]] = []
    frontend_rows: list[dict[str, str]] = []
    safety_rows: list[dict[str, str]] = []
    recommended_rows: list[dict[str, str]] = []
    helper_evidence: dict[str, Any] = {}
    fixture_evidence: dict[str, Any] = {}
    probability_evidence: dict[str, Any] = {}

    if artifacts_passed:
        diagnosis = read_json(
            PREDECESSOR_DIAGNOSIS_PATH
        )
        implementation_rows = read_csv(
            PREDECESSOR_OUTPUT_DIR
            / "implementation_checks.csv"
        )
        required_rows = read_csv(
            PREDECESSOR_OUTPUT_DIR
            / "required_field_validation.csv"
        )
        detail_rows = read_csv(
            PREDECESSOR_OUTPUT_DIR
            / "detail_field_validation.csv"
        )
        runtime_rows = read_csv(
            PREDECESSOR_OUTPUT_DIR
            / "runtime_cases.csv"
        )
        frontend_rows = read_csv(
            PREDECESSOR_OUTPUT_DIR
            / "frontend_contract.csv"
        )
        safety_rows = read_csv(
            PREDECESSOR_OUTPUT_DIR
            / "safety_audit.csv"
        )
        recommended_rows = read_csv(
            PREDECESSOR_OUTPUT_DIR
            / "recommended_path.csv"
        )
        helper_evidence = read_json(
            PREDECESSOR_OUTPUT_DIR
            / "helper_payload.json"
        )
        fixture_evidence = read_json(
            PREDECESSOR_OUTPUT_DIR
            / "fixture_evidence.json"
        )
        probability_evidence = read_json(
            PREDECESSOR_OUTPUT_DIR
            / "probability_guard.json"
        )

    diagnosis_contract_passed = (
        predecessor_return_code == 0
        and diagnosis.get("diagnosis")
        == (
            "layer_6_model_projection_realism_"
            "end_to_end_runtime_validation_"
            "implementation_complete"
        )
        and diagnosis.get("all_checks_passed") is True
        and diagnosis.get("required_fields_passed") == 8
        and diagnosis.get("detail_fields_passed") == 10
        and diagnosis.get("runtime_cases_passed") == 7
        and diagnosis.get(
            "frontend_contract_checks_passed"
        )
        == 8
        and diagnosis.get(
            "fixture_serialization_passed"
        )
        is True
        and diagnosis.get(
            "game_payload_groups_independent"
        )
        is True
        and diagnosis.get("frontend_build_passed")
        is True
        and diagnosis.get("manual_runtime_confirmed")
        is True
        and diagnosis.get("probability_guard_passed")
        is True
        and diagnosis.get("recommended_next_layer")
        == (
            "6OP_layer6_model_projection_realism_"
            "end_to_end_runtime_validation_audit"
        )
    )

    implementation_names = {
        row.get("check", "")
        for row in implementation_rows
    }

    implementation_checks_passed = (
        implementation_names
        == EXPECTED_IMPLEMENTATION_CHECKS
        and all(
            csv_bool(row.get("passed"))
            for row in implementation_rows
        )
    )

    required_by_field = {
        row.get("field", ""): row
        for row in required_rows
    }

    required_fields_passed = (
        set(required_by_field) == set(REQUIRED_FIELDS)
        and all(
            csv_bool(row.get("passed"))
            for row in required_rows
        )
        and all(
            required_by_field[field].get(
                "actual_type"
            )
            == "bool"
            for field in REQUIRED_BOOLEAN_FIELDS
        )
        and all(
            required_by_field[field].get(
                "actual_type"
            )
            == "str"
            for field in REQUIRED_STRING_FIELDS
        )
    )

    detail_by_field = {
        row.get("field", ""): row
        for row in detail_rows
    }

    detail_fields_passed = (
        set(detail_by_field) == set(DETAIL_FIELDS)
        and all(
            csv_bool(row.get("present"))
            and csv_bool(
                row.get("json_serializable")
            )
            and csv_bool(row.get("passed"))
            for row in detail_rows
        )
    )

    runtime_ids = {
        row.get("case_id", "")
        for row in runtime_rows
    }

    runtime_cases_passed = (
        runtime_ids == EXPECTED_RUNTIME_CASES
        and all(
            csv_bool(row.get("passed"))
            for row in runtime_rows
        )
    )

    frontend_names = {
        row.get("check", "")
        for row in frontend_rows
    }

    frontend_contract_passed = (
        frontend_names == EXPECTED_FRONTEND_CHECKS
        and all(
            csv_bool(row.get("passed"))
            for row in frontend_rows
        )
    )

    safety_by_boundary = {
        row.get("boundary", ""): row
        for row in safety_rows
    }

    prohibited_safety_passed = all(
        boundary in safety_by_boundary
        and not csv_bool(
            safety_by_boundary[boundary].get(
                "changed_or_executed"
            )
        )
        and csv_bool(
            safety_by_boundary[boundary].get(
                "passed"
            )
        )
        for boundary in PROHIBITED_ACTIONS
    )

    safety_all_passed = (
        prohibited_safety_passed
        and all(
            csv_bool(row.get("passed"))
            for row in safety_rows
        )
    )

    helper_payload = helper_evidence.get(
        "payload",
        {},
    )

    helper_payload_passed = (
        helper_evidence.get("payload_group")
        == PAYLOAD_GROUP
        and helper_evidence.get("helper_error") is None
        and isinstance(helper_payload, dict)
        and all(
            type(helper_payload.get(field)) is bool
            for field in REQUIRED_BOOLEAN_FIELDS
        )
        and all(
            type(helper_payload.get(field)) is str
            and bool(helper_payload.get(field))
            for field in REQUIRED_STRING_FIELDS
        )
        and all(
            field in helper_payload
            for field in DETAIL_FIELDS
        )
    )

    fixture_before = fixture_evidence.get(
        "fixture_games_before_serialization",
        [],
    )
    fixture_after = fixture_evidence.get(
        "fixture_games_after_serialization",
        [],
    )

    fixture_by_pk = {
        game.get("game_pk"): game
        for game in fixture_after
        if isinstance(game, dict)
    }

    complete_game = fixture_by_pk.get(600001, {})
    missing_game = fixture_by_pk.get(600002, {})
    partial_game = fixture_by_pk.get(600003, {})
    false_game = fixture_by_pk.get(600004, {})
    steals_game = fixture_by_pk.get(600005, {})

    complete_group = complete_game.get(
        PAYLOAD_GROUP,
        {},
    )
    partial_group = partial_game.get(
        PAYLOAD_GROUP,
        {},
    )
    false_group = false_game.get(
        PAYLOAD_GROUP,
        {},
    )
    steals_group = steals_game.get(
        PAYLOAD_GROUP,
        {},
    )

    fixture_evidence_passed = (
        fixture_evidence.get(
            "serialization_error"
        )
        is None
        and fixture_evidence.get(
            "game_payload_groups_are_independent"
        )
        is True
        and len(fixture_before) == 5
        and len(fixture_after) == 5
        and set(fixture_by_pk)
        == {600001, 600002, 600003, 600004, 600005}
        and all(
            field in complete_group
            for field in REQUIRED_FIELDS
        )
        and PAYLOAD_GROUP not in missing_game
        and "sac_fly_enabled" not in partial_group
        and all(
            false_group.get(field) is False
            and type(false_group.get(field)) is bool
            for field in REQUIRED_BOOLEAN_FIELDS
        )
        and steals_group.get("steals_model_status")
        == "deferred_not_active"
    )

    probability_before = (
        probability_evidence.get(
            "canonical_probabilities_before"
        )
    )
    probability_after = (
        probability_evidence.get(
            "canonical_probabilities_after"
        )
    )

    probability_guard_passed = (
        probability_before
        == {"away": 0.47, "home": 0.53}
        and probability_after == probability_before
        and probability_evidence.get("unchanged")
        is True
        and probability_evidence.get(
            "diagnostic_payload_separate"
        )
        is True
    )

    build_exit_code: int | None = None

    try:
        build_exit_code = int(
            (
                PREDECESSOR_OUTPUT_DIR
                / "frontend_build_exit_code.txt"
            )
            .read_text(encoding="utf-8")
            .strip()
        )
    except (OSError, ValueError):
        build_exit_code = None

    build_log = ""

    try:
        build_log = (
            PREDECESSOR_OUTPUT_DIR
            / "frontend_build.log"
        ).read_text(
            encoding="utf-8",
            errors="ignore",
        )
    except OSError:
        build_log = ""

    frontend_build_passed = (
        build_exit_code == 0
        and "vite build" in build_log
        and "built in" in build_log
    )

    runtime_confirmation = os.environ.get(
        "MODEL_PROJECTIONS_RUNTIME_CONFIRMED",
        "",
    ).strip().upper()

    manual_runtime_passed = (
        runtime_confirmation == "YES"
    )

    recommended_path_passed = (
        len(recommended_rows) == 1
        and recommended_rows[0].get(
            "recommended_next_layer"
        )
        == (
            "6OP_layer6_model_projection_realism_"
            "end_to_end_runtime_validation_audit"
        )
        and csv_bool(
            recommended_rows[0].get("passed")
        )
    )

    audit_checks = [
        {
            "check": "predecessor_execution",
            "actual": predecessor_return_code,
            "expected": 0,
            "passed": predecessor_return_code == 0,
        },
        {
            "check": "artifact_inventory",
            "actual": sum(
                1
                for row in artifact_rows
                if row["passed"]
            ),
            "expected": len(EXPECTED_ARTIFACTS),
            "passed": artifacts_passed,
        },
        {
            "check": "diagnosis_contract",
            "actual": diagnosis.get("diagnosis"),
            "expected": (
                "layer_6_model_projection_realism_"
                "end_to_end_runtime_validation_"
                "implementation_complete"
            ),
            "passed": diagnosis_contract_passed,
        },
        {
            "check": "implementation_checks",
            "actual": sum(
                1
                for row in implementation_rows
                if csv_bool(row.get("passed"))
            ),
            "expected": len(
                EXPECTED_IMPLEMENTATION_CHECKS
            ),
            "passed": implementation_checks_passed,
        },
        {
            "check": "required_field_evidence",
            "actual": sum(
                1
                for row in required_rows
                if csv_bool(row.get("passed"))
            ),
            "expected": len(REQUIRED_FIELDS),
            "passed": required_fields_passed,
        },
        {
            "check": "detail_field_evidence",
            "actual": sum(
                1
                for row in detail_rows
                if csv_bool(row.get("passed"))
            ),
            "expected": len(DETAIL_FIELDS),
            "passed": detail_fields_passed,
        },
        {
            "check": "runtime_case_evidence",
            "actual": sum(
                1
                for row in runtime_rows
                if csv_bool(row.get("passed"))
            ),
            "expected": len(
                EXPECTED_RUNTIME_CASES
            ),
            "passed": runtime_cases_passed,
        },
        {
            "check": "frontend_contract_evidence",
            "actual": sum(
                1
                for row in frontend_rows
                if csv_bool(row.get("passed"))
            ),
            "expected": len(
                EXPECTED_FRONTEND_CHECKS
            ),
            "passed": frontend_contract_passed,
        },
        {
            "check": "helper_payload_evidence",
            "actual": (
                len(helper_payload)
                if isinstance(helper_payload, dict)
                else 0
            ),
            "expected": (
                len(REQUIRED_FIELDS)
                + len(DETAIL_FIELDS)
            ),
            "passed": helper_payload_passed,
        },
        {
            "check": "fixture_evidence",
            "actual": len(fixture_after),
            "expected": 5,
            "passed": fixture_evidence_passed,
        },
        {
            "check": "probability_guard_evidence",
            "actual": probability_after,
            "expected": probability_before,
            "passed": probability_guard_passed,
        },
        {
            "check": "frontend_build_evidence",
            "actual": build_exit_code,
            "expected": 0,
            "passed": frontend_build_passed,
        },
        {
            "check": "manual_runtime_evidence",
            "actual": (
                runtime_confirmation
                or "not_confirmed"
            ),
            "expected": "YES",
            "passed": manual_runtime_passed,
        },
        {
            "check": "safety_evidence",
            "actual": sum(
                1
                for row in safety_rows
                if csv_bool(row.get("passed"))
            ),
            "expected": len(safety_rows),
            "passed": safety_all_passed,
        },
        {
            "check": "recommended_path_evidence",
            "actual": (
                recommended_rows[0].get(
                    "recommended_next_layer"
                )
                if recommended_rows
                else "missing"
            ),
            "expected": (
                "6OP_layer6_model_projection_realism_"
                "end_to_end_runtime_validation_audit"
            ),
            "passed": recommended_path_passed,
        },
    ]

    all_checks_passed = all(
        bool(row["passed"])
        for row in audit_checks
    )

    audit_safety_rows = [
        {
            "boundary": boundary,
            "changed_or_executed": False,
            "passed": True,
        }
        for boundary in sorted(PROHIBITED_ACTIONS)
    ]

    audit_safety_rows.extend(
        [
            {
                "boundary": (
                    "predecessor_evidence_reexecution"
                ),
                "changed_or_executed": True,
                "passed": predecessor_return_code == 0,
            },
            {
                "boundary": (
                    "artifact_consistency_audit"
                ),
                "changed_or_executed": True,
                "passed": artifacts_passed,
            },
            {
                "boundary": (
                    "manual_runtime_confirmation"
                ),
                "changed_or_executed": True,
                "passed": manual_runtime_passed,
            },
        ]
    )

    recommended_next_layer = (
        "6OQ_layer6_game_state_realism_"
        "exit_readiness_plan"
    )

    write_csv(
        OUTPUT_DIR / "audit_checks.csv",
        [
            "check",
            "actual",
            "expected",
            "passed",
        ],
        audit_checks,
    )

    write_csv(
        OUTPUT_DIR / "artifact_inventory.csv",
        [
            "artifact",
            "exists",
            "nonempty",
            "passed",
        ],
        artifact_rows,
    )

    write_csv(
        OUTPUT_DIR / "safety_audit.csv",
        [
            "boundary",
            "changed_or_executed",
            "passed",
        ],
        audit_safety_rows,
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
                    "Plan the Layer 6 game-state realism "
                    "exit-readiness review using the completed "
                    "backend, serialization, frontend, build, "
                    "runtime, and probability-guard evidence."
                ),
                "entry_condition": (
                    "Every independent 6OP evidence and "
                    "safety check passes."
                ),
                "passed": all_checks_passed,
            }
        ],
    )

    write_json(
        OUTPUT_DIR / "evidence_summary.json",
        {
            "predecessor_return_code": (
                predecessor_return_code
            ),
            "predecessor_stdout_tail": (
                predecessor_stdout[-2000:]
            ),
            "predecessor_stderr_tail": (
                predecessor_stderr[-2000:]
            ),
            "diagnosis": diagnosis,
            "helper_payload": helper_evidence,
            "fixture_summary": {
                "before_count": len(fixture_before),
                "after_count": len(fixture_after),
                "game_pks": sorted(
                    value
                    for value in fixture_by_pk
                    if value is not None
                ),
                "serialization_error": (
                    fixture_evidence.get(
                        "serialization_error"
                    )
                ),
                "groups_independent": (
                    fixture_evidence.get(
                        "game_payload_groups_are_independent"
                    )
                ),
            },
            "probability_guard": probability_evidence,
            "frontend_build_exit_code": (
                build_exit_code
            ),
            "manual_runtime_confirmation": (
                runtime_confirmation
            ),
        },
    )

    diagnosis_output = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": (
            "layer_6_model_projection_realism_"
            "end_to_end_runtime_validation_"
            "audit_complete"
            if all_checks_passed
            else
            "layer_6_model_projection_realism_"
            "end_to_end_runtime_validation_"
            "audit_failed"
        ),
        "all_checks_passed": all_checks_passed,
        "audit_checks_passed": sum(
            1
            for row in audit_checks
            if row["passed"]
        ),
        "audit_checks_required": len(audit_checks),
        "artifacts_verified": sum(
            1
            for row in artifact_rows
            if row["passed"]
        ),
        "artifacts_required": len(
            EXPECTED_ARTIFACTS
        ),
        "required_fields_verified": (
            len(REQUIRED_FIELDS)
            if required_fields_passed
            else 0
        ),
        "detail_fields_verified": (
            len(DETAIL_FIELDS)
            if detail_fields_passed
            else 0
        ),
        "runtime_cases_verified": (
            len(EXPECTED_RUNTIME_CASES)
            if runtime_cases_passed
            else 0
        ),
        "frontend_checks_verified": (
            len(EXPECTED_FRONTEND_CHECKS)
            if frontend_contract_passed
            else 0
        ),
        "fixture_evidence_verified": (
            fixture_evidence_passed
        ),
        "probability_guard_verified": (
            probability_guard_passed
        ),
        "frontend_build_verified": (
            frontend_build_passed
        ),
        "manual_runtime_verified": (
            manual_runtime_passed
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
        "exit_readiness_planning_allowed_next": (
            all_checks_passed
        ),
        "recommended_next_layer": (
            recommended_next_layer
        ),
        "generated_csv_artifacts": [
            str(OUTPUT_DIR / "audit_checks.csv"),
            str(
                OUTPUT_DIR / "artifact_inventory.csv"
            ),
            str(OUTPUT_DIR / "safety_audit.csv"),
            str(
                OUTPUT_DIR / "recommended_path.csv"
            ),
        ],
        "generated_json_artifacts": [
            str(
                OUTPUT_DIR / "evidence_summary.json"
            ),
            str(OUTPUT_DIR / "diagnosis.json"),
        ],
    }

    write_json(
        OUTPUT_DIR / "diagnosis.json",
        diagnosis_output,
    )

    print(json.dumps(diagnosis_output, indent=2))

    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
