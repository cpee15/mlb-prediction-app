#!/usr/bin/env python3
"""
Layer 6OV
Layer 6 Game-State Realism Exit Decision Audit

Independently re-executes and audits the deterministic exit decision
selected by 6OU.

This layer does not finalize Layer 6 or grant authority for historical
validation, tuning, pricing, edge detection, probability replacement,
or production behavior changes.
"""

from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "6OV"
LAYER_NAME = (
    "layer6_game_state_realism_exit_decision_audit"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = ROOT / (
    "tmp/layer_6OV_game_state_realism_"
    "exit_decision_audit"
)

PREDECESSOR_SCRIPT = (
    ROOT
    / "scripts/implement_6OU_layer6_game_state_realism_"
    "exit_decision.py"
)

PREDECESSOR_OUTPUT_DIR = ROOT / (
    "tmp/layer_6OU_game_state_realism_"
    "exit_decision_implementation"
)

DECISION_CHECKS_PATH = (
    PREDECESSOR_OUTPUT_DIR / "decision_checks.csv"
)
DECISION_OUTCOME_PATH = (
    PREDECESSOR_OUTPUT_DIR / "decision_outcome.csv"
)
SCOPE_BOUNDARIES_PATH = (
    PREDECESSOR_OUTPUT_DIR / "scope_boundaries.csv"
)
IMPLEMENTATION_CHECKS_PATH = (
    PREDECESSOR_OUTPUT_DIR / "implementation_checks.csv"
)
SAFETY_PATH = (
    PREDECESSOR_OUTPUT_DIR / "safety_audit.csv"
)
RECOMMENDED_PATH = (
    PREDECESSOR_OUTPUT_DIR / "recommended_path.csv"
)
EVIDENCE_PATH = (
    PREDECESSOR_OUTPUT_DIR / "evidence_summary.json"
)
DIAGNOSIS_PATH = (
    PREDECESSOR_OUTPUT_DIR / "diagnosis.json"
)

EXPECTED_ARTIFACTS = [
    DECISION_CHECKS_PATH,
    DECISION_OUTCOME_PATH,
    SCOPE_BOUNDARIES_PATH,
    IMPLEMENTATION_CHECKS_PATH,
    SAFETY_PATH,
    RECOMMENDED_PATH,
    EVIDENCE_PATH,
    DIAGNOSIS_PATH,
]

EXPECTED_DECISION_CRITERIA = {
    "L6-EXIT-01",
    "L6-EXIT-02",
    "L6-EXIT-03",
    "L6-EXIT-04",
    "L6-EXIT-05",
    "L6-EXIT-06",
    "L6-EXIT-07",
    "L6-EXIT-08",
}

EXPECTED_IMPLEMENTATION_CHECKS = {
    "6ot_plan_execution",
    "6ot_plan_contract",
    "6os_audit_execution",
    "6os_audit_contract",
    "decision_criteria_plan",
    "decision_outcomes_plan",
    "accepted_scope_boundaries",
    "post_exit_boundaries",
    "audited_evidence_consistency",
    "mandatory_decision_criteria",
    "blocking_known_gaps",
    "unidentified_blocking_gaps",
    "probability_guard",
    "frontend_runtime_guard",
    "safety_boundaries",
    "single_decision_outcome",
}

EXPECTED_ACCEPTED_SCOPE_IDS = {
    "L6-SCOPE-01",
    "L6-SCOPE-02",
    "L6-SCOPE-03",
}

EXPECTED_POST_EXIT_IDS = {
    "L6-POST-01",
    "L6-POST-02",
    "L6-POST-03",
    "L6-POST-04",
    "L6-POST-05",
    "L6-POST-06",
}

PROHIBITED_ACTIONS = {
    "accuracy_metric_generation",
    "backend_behavior_change",
    "backtest_execution",
    "bet_recommendation",
    "canonical_probability_replacement",
    "edge_detection",
    "frontend_behavior_change",
    "historical_outcome_join",
    "parameter_tuning",
    "pricing",
    "simulation_parameter_change",
    "simulation_probability_change",
}


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


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2) + "\n",
        encoding="utf-8",
    )


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def csv_bool(value: Any) -> bool:
    return str(value).strip().lower() == "true"


def csv_int(value: Any) -> int | None:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def run_predecessor() -> tuple[int | None, str, str]:
    if not PREDECESSOR_SCRIPT.exists():
        return None, "", "predecessor script missing"

    result = subprocess.run(
        [sys.executable, str(PREDECESSOR_SCRIPT)],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
        env={
            **os.environ,
            "PYTHONPATH": str(ROOT),
            "PYTHONDONTWRITEBYTECODE": "1",
            "MODEL_PROJECTIONS_RUNTIME_CONFIRMED": "YES",
        },
    )

    return result.returncode, result.stdout, result.stderr


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

    artifacts_verified = all(
        bool(row["passed"])
        for row in artifact_rows
    )

    diagnosis: dict[str, Any] = {}
    decision_checks: list[dict[str, str]] = []
    decision_outcomes: list[dict[str, str]] = []
    scope_boundaries: list[dict[str, str]] = []
    implementation_checks: list[dict[str, str]] = []
    safety_rows: list[dict[str, str]] = []
    recommended_rows: list[dict[str, str]] = []
    evidence: dict[str, Any] = {}

    if artifacts_verified:
        diagnosis = read_json(DIAGNOSIS_PATH)
        decision_checks = read_csv(
            DECISION_CHECKS_PATH
        )
        decision_outcomes = read_csv(
            DECISION_OUTCOME_PATH
        )
        scope_boundaries = read_csv(
            SCOPE_BOUNDARIES_PATH
        )
        implementation_checks = read_csv(
            IMPLEMENTATION_CHECKS_PATH
        )
        safety_rows = read_csv(SAFETY_PATH)
        recommended_rows = read_csv(
            RECOMMENDED_PATH
        )
        evidence = read_json(EVIDENCE_PATH)

    diagnosis_contract_passed = (
        predecessor_return_code == 0
        and diagnosis.get("diagnosis")
        == (
            "layer_6_game_state_realism_"
            "exit_decision_implementation_complete"
        )
        and diagnosis.get("all_checks_passed") is True
        and diagnosis.get("selected_outcome")
        == "exit_layer6_game_state_realism"
        and diagnosis.get(
            "implementation_checks_passed"
        )
        == diagnosis.get(
            "implementation_checks_required"
        )
        == 16
        and diagnosis.get(
            "decision_criteria_passed"
        )
        == diagnosis.get(
            "decision_criteria_required"
        )
        == 8
        and diagnosis.get(
            "mandatory_decision_criteria_passed"
        )
        is True
        and diagnosis.get("blocking_known_gaps") == 0
        and diagnosis.get(
            "unidentified_blocking_gaps"
        )
        == 0
        and diagnosis.get("evidence_consistent")
        is True
        and diagnosis.get(
            "probability_guard_passed"
        )
        is True
        and diagnosis.get(
            "frontend_runtime_passed"
        )
        is True
        and diagnosis.get(
            "scope_boundaries_preserved"
        )
        is True
        and diagnosis.get(
            "safety_boundaries_passed"
        )
        is True
        and diagnosis.get(
            "new_authority_granted"
        )
        is False
        and diagnosis.get(
            "layer6_exit_recommended"
        )
        is True
        and diagnosis.get(
            "layer6_exit_finalized"
        )
        is False
        and diagnosis.get(
            "exit_decision_audit_allowed_next"
        )
        is True
        and diagnosis.get(
            "recommended_next_layer"
        )
        == (
            "6OV_layer6_game_state_realism_"
            "exit_decision_audit"
        )
    )

    decision_criterion_ids = {
        row.get("criterion_id", "")
        for row in decision_checks
    }

    decision_checks_passed = (
        len(decision_checks) == 8
        and decision_criterion_ids
        == EXPECTED_DECISION_CRITERIA
        and all(
            csv_bool(row.get("mandatory"))
            and csv_bool(row.get("passed"))
            for row in decision_checks
        )
    )

    decision_outcome_passed = (
        len(decision_outcomes) == 1
        and decision_outcomes[0].get(
            "selected_outcome"
        )
        == "exit_layer6_game_state_realism"
        and csv_bool(
            decision_outcomes[0].get(
                "mandatory_criteria_passed"
            )
        )
        and csv_int(
            decision_outcomes[0].get(
                "blocking_known_gaps"
            )
        )
        == 0
        and csv_int(
            decision_outcomes[0].get(
                "unidentified_blocking_gaps"
            )
        )
        == 0
        and csv_bool(
            decision_outcomes[0].get(
                "evidence_consistent"
            )
        )
        and csv_bool(
            decision_outcomes[0].get("passed")
        )
    )

    implementation_check_names = {
        row.get("check", "")
        for row in implementation_checks
    }

    implementation_checks_passed = (
        len(implementation_checks) == 16
        and implementation_check_names
        == EXPECTED_IMPLEMENTATION_CHECKS
        and all(
            csv_bool(row.get("passed"))
            for row in implementation_checks
        )
    )

    accepted_scope_rows = [
        row
        for row in scope_boundaries
        if row.get("boundary_type")
        == "accepted_scope"
    ]

    post_exit_rows = [
        row
        for row in scope_boundaries
        if row.get("boundary_type") == "post_exit"
    ]

    accepted_scope_ids = {
        row.get("boundary_id", "")
        for row in accepted_scope_rows
    }

    post_exit_ids = {
        row.get("boundary_id", "")
        for row in post_exit_rows
    }

    accepted_scope_passed = (
        len(accepted_scope_rows) == 3
        and accepted_scope_ids
        == EXPECTED_ACCEPTED_SCOPE_IDS
        and all(
            csv_bool(row.get("accepted"))
            and not csv_bool(row.get("blocks_exit"))
            and not csv_bool(
                row.get("new_authority_granted")
            )
            and bool(row.get("status"))
            for row in accepted_scope_rows
        )
    )

    post_exit_scope_passed = (
        len(post_exit_rows) == 6
        and post_exit_ids == EXPECTED_POST_EXIT_IDS
        and all(
            csv_bool(row.get("accepted"))
            and not csv_bool(row.get("blocks_exit"))
            and not csv_bool(
                row.get("new_authority_granted")
            )
            and bool(row.get("status"))
            for row in post_exit_rows
        )
    )

    scope_boundaries_passed = (
        len(scope_boundaries) == 9
        and accepted_scope_passed
        and post_exit_scope_passed
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

    repository_mutation_guard_passed = (
        "layer6_exit_repository_mutation"
        in safety_by_boundary
        and not csv_bool(
            safety_by_boundary[
                "layer6_exit_repository_mutation"
            ].get("changed_or_executed")
        )
        and csv_bool(
            safety_by_boundary[
                "layer6_exit_repository_mutation"
            ].get("passed")
        )
    )

    safety_passed = (
        len(safety_rows) == 15
        and prohibited_safety_passed
        and repository_mutation_guard_passed
        and all(
            csv_bool(row.get("passed"))
            for row in safety_rows
        )
    )

    recommended_path_passed = (
        len(recommended_rows) == 1
        and recommended_rows[0].get(
            "recommended_next_layer"
        )
        == (
            "6OV_layer6_game_state_realism_"
            "exit_decision_audit"
        )
        and csv_bool(
            recommended_rows[0].get("passed")
        )
    )

    evidence_decision_criteria = evidence.get(
        "decision_criteria",
        {},
    )

    probability_guard = evidence.get(
        "probability_guard",
        {},
    )

    frontend_runtime = evidence.get(
        "frontend_runtime",
        {},
    )

    accepted_evidence_boundaries = evidence.get(
        "accepted_scope_boundaries",
        [],
    )

    post_exit_evidence_boundaries = evidence.get(
        "post_exit_boundaries",
        [],
    )

    probability_guard_passed = (
        probability_guard.get(
            "canonical_probabilities_before"
        )
        == {"away": 0.47, "home": 0.53}
        and probability_guard.get(
            "canonical_probabilities_after"
        )
        == probability_guard.get(
            "canonical_probabilities_before"
        )
        and probability_guard.get("unchanged")
        is True
        and probability_guard.get(
            "diagnostic_payload_separate"
        )
        is True
    )

    frontend_runtime_passed = (
        frontend_runtime.get("build_exit_code") == 0
        and frontend_runtime.get(
            "manual_runtime_confirmation"
        )
        == "YES"
        and os.environ.get(
            "MODEL_PROJECTIONS_RUNTIME_CONFIRMED",
            "",
        ).strip().upper()
        == "YES"
    )

    evidence_summary_passed = (
        evidence.get("selected_outcome")
        == "exit_layer6_game_state_realism"
        and set(evidence_decision_criteria)
        == EXPECTED_DECISION_CRITERIA
        and all(
            value is True
            for value in evidence_decision_criteria.values()
        )
        and evidence.get("blocking_known_gaps") == 0
        and evidence.get(
            "unidentified_blocking_gaps"
        )
        == 0
        and probability_guard_passed
        and frontend_runtime_passed
        and len(accepted_evidence_boundaries) == 3
        and len(post_exit_evidence_boundaries) == 6
        and evidence.get(
            "safety_boundaries_preserved"
        )
        is True
        and evidence.get(
            "new_authority_granted"
        )
        is False
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
            "expected": 8,
            "passed": artifacts_verified,
        },
        {
            "check": "diagnosis_contract",
            "actual": diagnosis.get("diagnosis"),
            "expected": (
                "layer_6_game_state_realism_"
                "exit_decision_implementation_complete"
            ),
            "passed": diagnosis_contract_passed,
        },
        {
            "check": "implementation_checks",
            "actual": sum(
                1
                for row in implementation_checks
                if csv_bool(row.get("passed"))
            ),
            "expected": 16,
            "passed": implementation_checks_passed,
        },
        {
            "check": "decision_criteria",
            "actual": sum(
                1
                for row in decision_checks
                if csv_bool(row.get("passed"))
            ),
            "expected": 8,
            "passed": decision_checks_passed,
        },
        {
            "check": "single_exit_outcome",
            "actual": (
                decision_outcomes[0].get(
                    "selected_outcome"
                )
                if decision_outcomes
                else "missing"
            ),
            "expected": (
                "exit_layer6_game_state_realism"
            ),
            "passed": decision_outcome_passed,
        },
        {
            "check": "blocking_known_gaps",
            "actual": (
                csv_int(
                    decision_outcomes[0].get(
                        "blocking_known_gaps"
                    )
                )
                if decision_outcomes
                else None
            ),
            "expected": 0,
            "passed": (
                decision_outcome_passed
                and csv_int(
                    decision_outcomes[0].get(
                        "blocking_known_gaps"
                    )
                )
                == 0
            ),
        },
        {
            "check": "unidentified_blocking_gaps",
            "actual": (
                csv_int(
                    decision_outcomes[0].get(
                        "unidentified_blocking_gaps"
                    )
                )
                if decision_outcomes
                else None
            ),
            "expected": 0,
            "passed": (
                decision_outcome_passed
                and csv_int(
                    decision_outcomes[0].get(
                        "unidentified_blocking_gaps"
                    )
                )
                == 0
            ),
        },
        {
            "check": "accepted_scope_boundaries",
            "actual": len(accepted_scope_rows),
            "expected": 3,
            "passed": accepted_scope_passed,
        },
        {
            "check": "post_exit_boundaries",
            "actual": len(post_exit_rows),
            "expected": 6,
            "passed": post_exit_scope_passed,
        },
        {
            "check": "probability_guard",
            "actual": probability_guard.get(
                "canonical_probabilities_after"
            ),
            "expected": probability_guard.get(
                "canonical_probabilities_before"
            ),
            "passed": probability_guard_passed,
        },
        {
            "check": "frontend_runtime",
            "actual": frontend_runtime,
            "expected": {
                "build_exit_code": 0,
                "manual_runtime_confirmation": "YES",
            },
            "passed": frontend_runtime_passed,
        },
        {
            "check": "evidence_summary",
            "actual": evidence.get(
                "selected_outcome"
            ),
            "expected": (
                "exit_layer6_game_state_realism"
            ),
            "passed": evidence_summary_passed,
        },
        {
            "check": "safety_boundaries",
            "actual": sum(
                1
                for row in safety_rows
                if csv_bool(row.get("passed"))
            ),
            "expected": 15,
            "passed": safety_passed,
        },
        {
            "check": "recommended_path",
            "actual": (
                recommended_rows[0].get(
                    "recommended_next_layer"
                )
                if recommended_rows
                else "missing"
            ),
            "expected": (
                "6OV_layer6_game_state_realism_"
                "exit_decision_audit"
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
                    "exit_decision_reexecution"
                ),
                "changed_or_executed": True,
                "passed": predecessor_return_code == 0,
            },
            {
                "boundary": (
                    "independent_exit_decision_audit"
                ),
                "changed_or_executed": True,
                "passed": (
                    diagnosis_contract_passed
                    and decision_outcome_passed
                ),
            },
            {
                "boundary": (
                    "layer6_exit_finalization"
                ),
                "changed_or_executed": False,
                "passed": True,
            },
        ]
    )

    recommended_next_layer = (
        "6OW_layer6_game_state_realism_"
        "exit_finalization_plan"
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
        OUTPUT_DIR / "decision_criterion_audit.csv",
        [
            "criterion_id",
            "criterion",
            "mandatory",
            "failure_outcome",
            "passed",
        ],
        decision_checks,
    )

    write_csv(
        OUTPUT_DIR / "decision_outcome_audit.csv",
        [
            "selected_outcome",
            "rationale",
            "mandatory_criteria_passed",
            "blocking_known_gaps",
            "unidentified_blocking_gaps",
            "evidence_consistent",
            "passed",
        ],
        decision_outcomes,
    )

    write_csv(
        OUTPUT_DIR / "scope_boundary_audit.csv",
        [
            "boundary_type",
            "boundary_id",
            "boundary",
            "status",
            "accepted",
            "blocks_exit",
            "new_authority_granted",
        ],
        scope_boundaries,
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
                    "Plan the formal finalization of the "
                    "independently audited Layer 6 game-state "
                    "realism exit decision."
                ),
                "entry_condition": (
                    "Every 6OV decision, evidence, scope, "
                    "probability, runtime, and safety audit "
                    "check passes."
                ),
                "passed": all_checks_passed,
            }
        ],
    )

    audited_evidence = {
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
        "selected_outcome": evidence.get(
            "selected_outcome"
        ),
        "decision_criteria": (
            evidence_decision_criteria
        ),
        "blocking_known_gaps": evidence.get(
            "blocking_known_gaps"
        ),
        "unidentified_blocking_gaps": evidence.get(
            "unidentified_blocking_gaps"
        ),
        "probability_guard": probability_guard,
        "frontend_runtime": frontend_runtime,
        "accepted_scope_boundaries": (
            accepted_evidence_boundaries
        ),
        "post_exit_boundaries": (
            post_exit_evidence_boundaries
        ),
        "scope_boundaries_verified": (
            scope_boundaries_passed
        ),
        "safety_boundaries_verified": safety_passed,
        "new_authority_granted": False,
        "layer6_exit_finalized": False,
    }

    write_json(
        OUTPUT_DIR / "audited_evidence.json",
        audited_evidence,
    )

    diagnosis_output = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": (
            "layer_6_game_state_realism_"
            "exit_decision_audit_complete"
            if all_checks_passed
            else
            "layer_6_game_state_realism_"
            "exit_decision_audit_failed"
        ),
        "all_checks_passed": all_checks_passed,
        "selected_outcome_verified": (
            decision_outcome_passed
        ),
        "selected_outcome": (
            "exit_layer6_game_state_realism"
            if decision_outcome_passed
            else None
        ),
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
        "implementation_checks_verified": (
            16 if implementation_checks_passed else 0
        ),
        "implementation_checks_required": 16,
        "decision_criteria_verified": (
            8 if decision_checks_passed else 0
        ),
        "decision_criteria_required": 8,
        "accepted_scope_boundaries_verified": (
            3 if accepted_scope_passed else 0
        ),
        "accepted_scope_boundaries_required": 3,
        "post_exit_boundaries_verified": (
            6 if post_exit_scope_passed else 0
        ),
        "post_exit_boundaries_required": 6,
        "blocking_known_gaps": (
            evidence.get("blocking_known_gaps")
        ),
        "unidentified_blocking_gaps": (
            evidence.get(
                "unidentified_blocking_gaps"
            )
        ),
        "evidence_summary_verified": (
            evidence_summary_passed
        ),
        "probability_guard_verified": (
            probability_guard_passed
        ),
        "frontend_runtime_verified": (
            frontend_runtime_passed
        ),
        "scope_boundaries_verified": (
            scope_boundaries_passed
        ),
        "safety_boundaries_verified": safety_passed,
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
        "layer6_exit_recommended": all_checks_passed,
        "layer6_exit_finalized": False,
        "exit_finalization_planning_allowed_next": (
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
            str(
                OUTPUT_DIR
                / "decision_criterion_audit.csv"
            ),
            str(
                OUTPUT_DIR
                / "decision_outcome_audit.csv"
            ),
            str(
                OUTPUT_DIR
                / "scope_boundary_audit.csv"
            ),
            str(OUTPUT_DIR / "safety_audit.csv"),
            str(
                OUTPUT_DIR / "recommended_path.csv"
            ),
        ],
        "generated_json_artifacts": [
            str(OUTPUT_DIR / "audited_evidence.json"),
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
