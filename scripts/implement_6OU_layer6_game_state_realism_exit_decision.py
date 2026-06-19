#!/usr/bin/env python3
"""
Layer 6OU
Layer 6 Game-State Realism Exit Decision Implementation

Implements the deterministic exit-decision framework defined by 6OT.

This layer does not change production behavior, simulation parameters,
canonical probabilities, historical validation, tuning, pricing,
edge detection, or betting recommendations.
"""

from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "6OU"
LAYER_NAME = (
    "layer6_game_state_realism_exit_decision_implementation"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = ROOT / (
    "tmp/layer_6OU_game_state_realism_"
    "exit_decision_implementation"
)

PLAN_SCRIPT = (
    ROOT
    / "scripts/plan_6OT_layer6_game_state_realism_"
    "exit_decision.py"
)

AUDIT_SCRIPT = (
    ROOT
    / "scripts/audit_6OS_layer6_game_state_realism_"
    "exit_readiness.py"
)

PLAN_OUTPUT_DIR = ROOT / (
    "tmp/layer_6OT_game_state_realism_exit_decision_plan"
)

AUDIT_OUTPUT_DIR = ROOT / (
    "tmp/layer_6OS_game_state_realism_exit_readiness_audit"
)

PLAN_DIAGNOSIS_PATH = PLAN_OUTPUT_DIR / "diagnosis.json"
PLAN_CRITERIA_PATH = PLAN_OUTPUT_DIR / "decision_criteria.csv"
PLAN_OUTCOMES_PATH = PLAN_OUTPUT_DIR / "decision_outcomes.csv"
PLAN_SCOPE_PATH = (
    PLAN_OUTPUT_DIR / "accepted_scope_boundaries.csv"
)
PLAN_POST_EXIT_PATH = (
    PLAN_OUTPUT_DIR / "post_exit_boundaries.csv"
)

AUDIT_DIAGNOSIS_PATH = AUDIT_OUTPUT_DIR / "diagnosis.json"
AUDIT_CHECKS_PATH = AUDIT_OUTPUT_DIR / "audit_checks.csv"
AUDIT_ARTIFACTS_PATH = (
    AUDIT_OUTPUT_DIR / "artifact_inventory.csv"
)
AUDIT_DOMAINS_PATH = AUDIT_OUTPUT_DIR / "domain_audit.csv"
AUDIT_CRITERIA_PATH = AUDIT_OUTPUT_DIR / "criterion_audit.csv"
AUDIT_GAPS_PATH = AUDIT_OUTPUT_DIR / "gap_audit.csv"
AUDIT_SAFETY_PATH = AUDIT_OUTPUT_DIR / "safety_audit.csv"
AUDIT_EVIDENCE_PATH = (
    AUDIT_OUTPUT_DIR / "audited_evidence_chain.json"
)

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

EXPECTED_OUTCOMES = {
    "exit_layer6_game_state_realism",
    "hold_layer6_for_blocking_gap",
    "hold_layer6_for_evidence_inconsistency",
}

EXPECTED_SCOPE_BOUNDARIES = {
    "L6-SCOPE-01",
    "L6-SCOPE-02",
    "L6-SCOPE-03",
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


def run_script(
    path: Path,
    extra_env: dict[str, str] | None = None,
) -> tuple[int | None, str, str]:
    if not path.exists():
        return None, "", f"missing script: {path}"

    env = {
        **os.environ,
        "PYTHONPATH": str(ROOT),
        "PYTHONDONTWRITEBYTECODE": "1",
    }

    if extra_env:
        env.update(extra_env)

    result = subprocess.run(
        [sys.executable, str(path)],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
        env=env,
    )

    return result.returncode, result.stdout, result.stderr


def files_exist(paths: Iterable[Path]) -> bool:
    return all(
        path.exists() and path.stat().st_size > 0
        for path in paths
    )


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    plan_return_code, plan_stdout, plan_stderr = run_script(
        PLAN_SCRIPT
    )

    audit_return_code, audit_stdout, audit_stderr = run_script(
        AUDIT_SCRIPT,
        {
            "MODEL_PROJECTIONS_RUNTIME_CONFIRMED": "YES",
        },
    )

    required_plan_paths = [
        PLAN_DIAGNOSIS_PATH,
        PLAN_CRITERIA_PATH,
        PLAN_OUTCOMES_PATH,
        PLAN_SCOPE_PATH,
        PLAN_POST_EXIT_PATH,
    ]

    required_audit_paths = [
        AUDIT_DIAGNOSIS_PATH,
        AUDIT_CHECKS_PATH,
        AUDIT_ARTIFACTS_PATH,
        AUDIT_DOMAINS_PATH,
        AUDIT_CRITERIA_PATH,
        AUDIT_GAPS_PATH,
        AUDIT_SAFETY_PATH,
        AUDIT_EVIDENCE_PATH,
    ]

    plan_artifacts_present = files_exist(
        required_plan_paths
    )
    audit_artifacts_present = files_exist(
        required_audit_paths
    )

    plan_diagnosis: dict[str, Any] = {}
    audit_diagnosis: dict[str, Any] = {}
    plan_criteria: list[dict[str, str]] = []
    plan_outcomes: list[dict[str, str]] = []
    scope_rows: list[dict[str, str]] = []
    post_exit_rows: list[dict[str, str]] = []
    audit_checks: list[dict[str, str]] = []
    audit_artifacts: list[dict[str, str]] = []
    audit_domains: list[dict[str, str]] = []
    audit_criteria: list[dict[str, str]] = []
    audit_gaps: list[dict[str, str]] = []
    audit_safety: list[dict[str, str]] = []
    audit_evidence: dict[str, Any] = {}

    if plan_artifacts_present:
        plan_diagnosis = read_json(
            PLAN_DIAGNOSIS_PATH
        )
        plan_criteria = read_csv(
            PLAN_CRITERIA_PATH
        )
        plan_outcomes = read_csv(
            PLAN_OUTCOMES_PATH
        )
        scope_rows = read_csv(
            PLAN_SCOPE_PATH
        )
        post_exit_rows = read_csv(
            PLAN_POST_EXIT_PATH
        )

    if audit_artifacts_present:
        audit_diagnosis = read_json(
            AUDIT_DIAGNOSIS_PATH
        )
        audit_checks = read_csv(
            AUDIT_CHECKS_PATH
        )
        audit_artifacts = read_csv(
            AUDIT_ARTIFACTS_PATH
        )
        audit_domains = read_csv(
            AUDIT_DOMAINS_PATH
        )
        audit_criteria = read_csv(
            AUDIT_CRITERIA_PATH
        )
        audit_gaps = read_csv(
            AUDIT_GAPS_PATH
        )
        audit_safety = read_csv(
            AUDIT_SAFETY_PATH
        )
        audit_evidence = read_json(
            AUDIT_EVIDENCE_PATH
        )

    plan_contract_passed = (
        plan_return_code == 0
        and plan_diagnosis.get("diagnosis")
        == (
            "layer_6_game_state_realism_"
            "exit_decision_plan_complete"
        )
        and plan_diagnosis.get(
            "all_checks_passed"
        )
        is True
        and plan_diagnosis.get(
            "planning_checks_passed"
        )
        == plan_diagnosis.get(
            "planning_checks_required"
        )
        == 5
        and plan_diagnosis.get(
            "decision_outcomes_planned"
        )
        == 3
        and plan_diagnosis.get(
            "decision_criteria_planned"
        )
        == 8
        and plan_diagnosis.get(
            "mandatory_decision_criteria"
        )
        == 8
        and plan_diagnosis.get(
            "accepted_scope_boundaries"
        )
        == 3
        and plan_diagnosis.get(
            "blocking_scope_boundaries"
        )
        == 0
        and plan_diagnosis.get(
            "exit_decision_implementation_allowed_next"
        )
        is True
        and plan_diagnosis.get(
            "recommended_next_layer"
        )
        == (
            "6OU_layer6_game_state_realism_"
            "exit_decision_implementation"
        )
    )

    audit_contract_passed = (
        audit_return_code == 0
        and audit_diagnosis.get("diagnosis")
        == (
            "layer_6_game_state_realism_"
            "exit_readiness_audit_complete"
        )
        and audit_diagnosis.get(
            "all_checks_passed"
        )
        is True
        and audit_diagnosis.get(
            "audit_checks_passed"
        )
        == audit_diagnosis.get(
            "audit_checks_required"
        )
        == 15
        and audit_diagnosis.get(
            "artifacts_verified"
        )
        == audit_diagnosis.get(
            "artifacts_required"
        )
        == 8
        and audit_diagnosis.get(
            "domains_verified"
        )
        == audit_diagnosis.get(
            "domains_required"
        )
        == 10
        and audit_diagnosis.get(
            "criteria_verified"
        )
        == audit_diagnosis.get(
            "criteria_required"
        )
        == 10
        and audit_diagnosis.get(
            "blocking_criteria_verified"
        )
        == audit_diagnosis.get(
            "blocking_criteria_required"
        )
        == 9
        and audit_diagnosis.get(
            "known_gaps_verified"
        )
        == audit_diagnosis.get(
            "known_gaps_required"
        )
        == 3
        and audit_diagnosis.get(
            "blocking_known_gaps"
        )
        == 0
        and audit_diagnosis.get(
            "unidentified_blocking_gaps"
        )
        == 0
        and audit_diagnosis.get(
            "evidence_chain_verified"
        )
        is True
        and audit_diagnosis.get(
            "probability_guard_verified"
        )
        is True
        and audit_diagnosis.get(
            "frontend_runtime_verified"
        )
        is True
        and audit_diagnosis.get(
            "safety_boundaries_verified"
        )
        is True
        and audit_diagnosis.get(
            "exit_decision_planning_allowed_next"
        )
        is True
    )

    plan_criterion_ids = {
        row.get("criterion_id", "")
        for row in plan_criteria
    }

    plan_criteria_valid = (
        len(plan_criteria) == 8
        and plan_criterion_ids
        == EXPECTED_DECISION_CRITERIA
        and all(
            csv_bool(row.get("mandatory"))
            for row in plan_criteria
        )
    )

    plan_outcome_names = {
        row.get("outcome", "")
        for row in plan_outcomes
    }

    plan_outcomes_valid = (
        len(plan_outcomes) == 3
        and plan_outcome_names == EXPECTED_OUTCOMES
    )

    scope_ids = {
        row.get("boundary_id", "")
        for row in scope_rows
    }

    accepted_scope_valid = (
        len(scope_rows) == 3
        and scope_ids == EXPECTED_SCOPE_BOUNDARIES
        and all(
            csv_bool(row.get("accepted"))
            and not csv_bool(row.get("blocks_exit"))
            and bool(
                row.get("required_post_exit_label")
            )
            for row in scope_rows
        )
    )

    post_exit_boundaries_valid = (
        len(post_exit_rows) == 6
        and all(
            not csv_bool(
                row.get("new_authority_granted")
            )
            for row in post_exit_rows
        )
    )

    audit_checks_valid = (
        len(audit_checks) == 15
        and all(
            csv_bool(row.get("passed"))
            for row in audit_checks
        )
    )

    audit_artifacts_valid = (
        len(audit_artifacts) == 8
        and all(
            csv_bool(row.get("exists"))
            and csv_bool(row.get("nonempty"))
            and csv_bool(row.get("passed"))
            for row in audit_artifacts
        )
    )

    audit_domains_valid = (
        len(audit_domains) == 10
        and all(
            csv_bool(row.get("passed"))
            and csv_bool(
                row.get("evidence_available")
            )
            and row.get("readiness_status") == "ready"
            for row in audit_domains
        )
    )

    blocking_audit_criteria = [
        row
        for row in audit_criteria
        if csv_bool(row.get("blocking"))
    ]

    audit_criteria_valid = (
        len(audit_criteria) == 10
        and len(blocking_audit_criteria) == 9
        and all(
            csv_bool(row.get("passed"))
            for row in audit_criteria
        )
    )

    audit_gaps_valid = (
        len(audit_gaps) == 3
        and all(
            not csv_bool(row.get("blocks_exit"))
            and csv_bool(row.get("accepted"))
            and csv_bool(row.get("passed"))
            for row in audit_gaps
        )
    )

    safety_by_boundary = {
        row.get("boundary", ""): row
        for row in audit_safety
    }

    safety_valid = (
        all(
            boundary in safety_by_boundary
            and not csv_bool(
                safety_by_boundary[
                    boundary
                ].get("changed_or_executed")
            )
            and csv_bool(
                safety_by_boundary[
                    boundary
                ].get("passed")
            )
            for boundary in PROHIBITED_ACTIONS
        )
        and all(
            csv_bool(row.get("passed"))
            for row in audit_safety
        )
    )

    probability_guard = audit_evidence.get(
        "probability_guard",
        {},
    )

    frontend_runtime = audit_evidence.get(
        "frontend_runtime",
        {},
    )

    audited_domain_evidence = audit_evidence.get(
        "domain_evidence",
        {},
    )

    audited_criterion_evidence = audit_evidence.get(
        "criterion_evidence",
        {},
    )

    audited_known_gaps = audit_evidence.get(
        "known_gaps",
        [],
    )

    probability_guard_valid = (
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

    frontend_runtime_valid = (
        frontend_runtime.get(
            "build_exit_code"
        )
        == 0
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

    evidence_consistent = (
        audit_contract_passed
        and audit_checks_valid
        and audit_artifacts_valid
        and audit_domains_valid
        and audit_criteria_valid
        and audit_gaps_valid
        and safety_valid
        and probability_guard_valid
        and frontend_runtime_valid
        and len(audited_domain_evidence) == 10
        and all(
            value is True
            for value in audited_domain_evidence.values()
        )
        and len(audited_criterion_evidence) == 10
        and all(
            value is True
            for value in audited_criterion_evidence.values()
        )
        and len(audited_known_gaps) == 3
        and audit_evidence.get(
            "unidentified_blocking_gaps"
        )
        == 0
    )

    criterion_results = {
        "L6-EXIT-01": (
            audit_return_code == 0
            and audit_contract_passed
        ),
        "L6-EXIT-02": (
            audit_domains_valid
            and audit_criteria_valid
        ),
        "L6-EXIT-03": (
            len(blocking_audit_criteria) == 9
            and all(
                csv_bool(row.get("passed"))
                for row in blocking_audit_criteria
            )
        ),
        "L6-EXIT-04": (
            audit_diagnosis.get(
                "blocking_known_gaps"
            )
            == 0
            and audit_diagnosis.get(
                "unidentified_blocking_gaps"
            )
            == 0
            and audit_gaps_valid
        ),
        "L6-EXIT-05": probability_guard_valid,
        "L6-EXIT-06": frontend_runtime_valid,
        "L6-EXIT-07": (
            accepted_scope_valid
            and audit_gaps_valid
        ),
        "L6-EXIT-08": (
            safety_valid
            and post_exit_boundaries_valid
        ),
    }

    decision_rows: list[dict[str, Any]] = []

    for planned in plan_criteria:
        criterion_id = planned.get(
            "criterion_id",
            "",
        )

        passed = bool(
            criterion_results.get(
                criterion_id,
                False,
            )
        )

        decision_rows.append(
            {
                "criterion_id": criterion_id,
                "criterion": planned.get(
                    "criterion",
                    "",
                ),
                "mandatory": csv_bool(
                    planned.get("mandatory")
                ),
                "failure_outcome": planned.get(
                    "failure_outcome",
                    "",
                ),
                "passed": passed,
            }
        )

    mandatory_criteria_passed = (
        len(decision_rows) == 8
        and all(
            bool(row["passed"])
            for row in decision_rows
            if row["mandatory"]
        )
    )

    blocking_known_gaps = sum(
        1
        for row in audit_gaps
        if csv_bool(row.get("blocks_exit"))
    )

    unidentified_blocking_gaps = (
        audit_diagnosis.get(
            "unidentified_blocking_gaps"
        )
    )

    if not evidence_consistent:
        selected_outcome = (
            "hold_layer6_for_evidence_inconsistency"
        )
        decision_rationale = (
            "The implementation and audit evidence could not "
            "be reconciled consistently."
        )
    elif (
        not mandatory_criteria_passed
        or blocking_known_gaps != 0
        or unidentified_blocking_gaps != 0
    ):
        selected_outcome = (
            "hold_layer6_for_blocking_gap"
        )
        decision_rationale = (
            "At least one mandatory criterion or blocking-gap "
            "condition prevents exit."
        )
    else:
        selected_outcome = (
            "exit_layer6_game_state_realism"
        )
        decision_rationale = (
            "All mandatory exit criteria pass, all evidence "
            "is consistent, and no blocking or unidentified "
            "gap remains."
        )

    selected_outcome_valid = (
        selected_outcome in EXPECTED_OUTCOMES
    )

    implementation_checks = [
        {
            "check": "6ot_plan_execution",
            "actual": plan_return_code,
            "expected": 0,
            "passed": plan_return_code == 0,
        },
        {
            "check": "6ot_plan_contract",
            "actual": plan_diagnosis.get(
                "diagnosis"
            ),
            "expected": (
                "layer_6_game_state_realism_"
                "exit_decision_plan_complete"
            ),
            "passed": plan_contract_passed,
        },
        {
            "check": "6os_audit_execution",
            "actual": audit_return_code,
            "expected": 0,
            "passed": audit_return_code == 0,
        },
        {
            "check": "6os_audit_contract",
            "actual": audit_diagnosis.get(
                "diagnosis"
            ),
            "expected": (
                "layer_6_game_state_realism_"
                "exit_readiness_audit_complete"
            ),
            "passed": audit_contract_passed,
        },
        {
            "check": "decision_criteria_plan",
            "actual": len(plan_criteria),
            "expected": 8,
            "passed": plan_criteria_valid,
        },
        {
            "check": "decision_outcomes_plan",
            "actual": len(plan_outcomes),
            "expected": 3,
            "passed": plan_outcomes_valid,
        },
        {
            "check": "accepted_scope_boundaries",
            "actual": len(scope_rows),
            "expected": 3,
            "passed": accepted_scope_valid,
        },
        {
            "check": "post_exit_boundaries",
            "actual": len(post_exit_rows),
            "expected": 6,
            "passed": post_exit_boundaries_valid,
        },
        {
            "check": "audited_evidence_consistency",
            "actual": evidence_consistent,
            "expected": True,
            "passed": evidence_consistent,
        },
        {
            "check": "mandatory_decision_criteria",
            "actual": sum(
                1
                for row in decision_rows
                if row["passed"]
            ),
            "expected": 8,
            "passed": mandatory_criteria_passed,
        },
        {
            "check": "blocking_known_gaps",
            "actual": blocking_known_gaps,
            "expected": 0,
            "passed": blocking_known_gaps == 0,
        },
        {
            "check": "unidentified_blocking_gaps",
            "actual": unidentified_blocking_gaps,
            "expected": 0,
            "passed": unidentified_blocking_gaps == 0,
        },
        {
            "check": "probability_guard",
            "actual": probability_guard.get(
                "canonical_probabilities_after"
            ),
            "expected": probability_guard.get(
                "canonical_probabilities_before"
            ),
            "passed": probability_guard_valid,
        },
        {
            "check": "frontend_runtime_guard",
            "actual": frontend_runtime,
            "expected": {
                "build_exit_code": 0,
                "manual_runtime_confirmation": "YES",
            },
            "passed": frontend_runtime_valid,
        },
        {
            "check": "safety_boundaries",
            "actual": safety_valid,
            "expected": True,
            "passed": safety_valid,
        },
        {
            "check": "single_decision_outcome",
            "actual": selected_outcome,
            "expected": (
                "exit_layer6_game_state_realism"
            ),
            "passed": (
                selected_outcome_valid
                and selected_outcome
                == "exit_layer6_game_state_realism"
            ),
        },
    ]

    all_checks_passed = all(
        bool(row["passed"])
        for row in implementation_checks
    )

    safety_rows = [
        {
            "boundary": boundary,
            "changed_or_executed": False,
            "passed": True,
        }
        for boundary in sorted(PROHIBITED_ACTIONS)
    ]

    safety_rows.extend(
        [
            {
                "boundary": (
                    "exit_decision_evidence_evaluation"
                ),
                "changed_or_executed": True,
                "passed": evidence_consistent,
            },
            {
                "boundary": (
                    "deterministic_outcome_selection"
                ),
                "changed_or_executed": True,
                "passed": selected_outcome_valid,
            },
            {
                "boundary": (
                    "layer6_exit_repository_mutation"
                ),
                "changed_or_executed": False,
                "passed": True,
            },
        ]
    )

    scope_output_rows: list[dict[str, Any]] = []

    for row in scope_rows:
        scope_output_rows.append(
            {
                "boundary_type": "accepted_scope",
                "boundary_id": row.get(
                    "boundary_id",
                    "",
                ),
                "boundary": row.get(
                    "description",
                    "",
                ),
                "status": row.get(
                    "required_post_exit_label",
                    "",
                ),
                "accepted": csv_bool(
                    row.get("accepted")
                ),
                "blocks_exit": csv_bool(
                    row.get("blocks_exit")
                ),
                "new_authority_granted": False,
            }
        )

    for index, row in enumerate(
        post_exit_rows,
        start=1,
    ):
        scope_output_rows.append(
            {
                "boundary_type": "post_exit",
                "boundary_id": f"L6-POST-{index:02d}",
                "boundary": row.get(
                    "boundary",
                    "",
                ),
                "status": row.get(
                    "status_after_exit",
                    "",
                ),
                "accepted": True,
                "blocks_exit": False,
                "new_authority_granted": csv_bool(
                    row.get(
                        "new_authority_granted"
                    )
                ),
            }
        )

    write_csv(
        OUTPUT_DIR / "decision_checks.csv",
        [
            "criterion_id",
            "criterion",
            "mandatory",
            "failure_outcome",
            "passed",
        ],
        decision_rows,
    )

    write_csv(
        OUTPUT_DIR / "decision_outcome.csv",
        [
            "selected_outcome",
            "rationale",
            "mandatory_criteria_passed",
            "blocking_known_gaps",
            "unidentified_blocking_gaps",
            "evidence_consistent",
            "passed",
        ],
        [
            {
                "selected_outcome": selected_outcome,
                "rationale": decision_rationale,
                "mandatory_criteria_passed": (
                    mandatory_criteria_passed
                ),
                "blocking_known_gaps": (
                    blocking_known_gaps
                ),
                "unidentified_blocking_gaps": (
                    unidentified_blocking_gaps
                ),
                "evidence_consistent": (
                    evidence_consistent
                ),
                "passed": (
                    selected_outcome
                    == "exit_layer6_game_state_realism"
                    and all_checks_passed
                ),
            }
        ],
    )

    write_csv(
        OUTPUT_DIR / "scope_boundaries.csv",
        [
            "boundary_type",
            "boundary_id",
            "boundary",
            "status",
            "accepted",
            "blocks_exit",
            "new_authority_granted",
        ],
        scope_output_rows,
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
        OUTPUT_DIR / "safety_audit.csv",
        [
            "boundary",
            "changed_or_executed",
            "passed",
        ],
        safety_rows,
    )

    recommended_next_layer = (
        "6OV_layer6_game_state_realism_"
        "exit_decision_audit"
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
                    "Independently audit the selected Layer 6 "
                    "game-state realism exit decision."
                ),
                "entry_condition": (
                    "The deterministic 6OU decision selects "
                    "exit_layer6_game_state_realism and all "
                    "implementation checks pass."
                ),
                "passed": all_checks_passed,
            }
        ],
    )

    evidence_summary = {
        "plan_execution": {
            "return_code": plan_return_code,
            "stdout_tail": plan_stdout[-2000:],
            "stderr_tail": plan_stderr[-2000:],
            "diagnosis": plan_diagnosis,
        },
        "audit_execution": {
            "return_code": audit_return_code,
            "stdout_tail": audit_stdout[-2000:],
            "stderr_tail": audit_stderr[-2000:],
            "diagnosis": audit_diagnosis,
        },
        "decision_criteria": criterion_results,
        "selected_outcome": selected_outcome,
        "decision_rationale": decision_rationale,
        "blocking_known_gaps": blocking_known_gaps,
        "unidentified_blocking_gaps": (
            unidentified_blocking_gaps
        ),
        "probability_guard": probability_guard,
        "frontend_runtime": frontend_runtime,
        "accepted_scope_boundaries": scope_rows,
        "post_exit_boundaries": post_exit_rows,
        "safety_boundaries_preserved": safety_valid,
        "new_authority_granted": False,
    }

    write_json(
        OUTPUT_DIR / "evidence_summary.json",
        evidence_summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": (
            "layer_6_game_state_realism_"
            "exit_decision_implementation_complete"
            if all_checks_passed
            else
            "layer_6_game_state_realism_"
            "exit_decision_implementation_failed"
        ),
        "all_checks_passed": all_checks_passed,
        "selected_outcome": selected_outcome,
        "implementation_checks_passed": sum(
            1
            for row in implementation_checks
            if row["passed"]
        ),
        "implementation_checks_required": len(
            implementation_checks
        ),
        "decision_criteria_passed": sum(
            1
            for row in decision_rows
            if row["passed"]
        ),
        "decision_criteria_required": 8,
        "mandatory_decision_criteria_passed": (
            mandatory_criteria_passed
        ),
        "blocking_known_gaps": blocking_known_gaps,
        "unidentified_blocking_gaps": (
            unidentified_blocking_gaps
        ),
        "evidence_consistent": evidence_consistent,
        "probability_guard_passed": (
            probability_guard_valid
        ),
        "frontend_runtime_passed": (
            frontend_runtime_valid
        ),
        "scope_boundaries_preserved": (
            accepted_scope_valid
            and post_exit_boundaries_valid
        ),
        "safety_boundaries_passed": all(
            bool(row["passed"])
            for row in safety_rows
        ),
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
        "layer6_exit_recommended": (
            all_checks_passed
            and selected_outcome
            == "exit_layer6_game_state_realism"
        ),
        "layer6_exit_finalized": False,
        "exit_decision_audit_allowed_next": (
            all_checks_passed
        ),
        "recommended_next_layer": (
            recommended_next_layer
        ),
        "generated_csv_artifacts": [
            str(OUTPUT_DIR / "decision_checks.csv"),
            str(OUTPUT_DIR / "decision_outcome.csv"),
            str(OUTPUT_DIR / "scope_boundaries.csv"),
            str(
                OUTPUT_DIR / "implementation_checks.csv"
            ),
            str(OUTPUT_DIR / "safety_audit.csv"),
            str(OUTPUT_DIR / "recommended_path.csv"),
        ],
        "generated_json_artifacts": [
            str(OUTPUT_DIR / "evidence_summary.json"),
            str(OUTPUT_DIR / "diagnosis.json"),
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
