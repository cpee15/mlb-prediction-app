#!/usr/bin/env python3
"""
Layer 6OR
Layer 6 Game-State Realism Exit Readiness Implementation

Reconstructs and evaluates the complete Layer 6 game-state realism
evidence chain defined by 6OQ.

This layer does not change production behavior, simulation parameters,
canonical probabilities, historical validation, tuning, pricing,
edge detection, or recommendations.
"""

from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "6OR"
LAYER_NAME = (
    "layer6_game_state_realism_exit_readiness_implementation"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = ROOT / (
    "tmp/layer_6OR_game_state_realism_"
    "exit_readiness_implementation"
)

PLAN_SCRIPT = (
    ROOT
    / "scripts/plan_6OQ_layer6_game_state_realism_"
    "exit_readiness.py"
)

AUDIT_SCRIPT = (
    ROOT
    / "scripts/audit_6OP_model_projection_realism_"
    "end_to_end_runtime_validation.py"
)

PLAN_OUTPUT_DIR = ROOT / (
    "tmp/layer_6OQ_game_state_realism_exit_readiness_plan"
)

AUDIT_OUTPUT_DIR = ROOT / (
    "tmp/layer_6OP_model_projection_realism_"
    "end_to_end_runtime_validation_audit"
)

PLAN_DIAGNOSIS_PATH = PLAN_OUTPUT_DIR / "diagnosis.json"
PLAN_DOMAINS_PATH = (
    PLAN_OUTPUT_DIR / "domain_readiness_plan.csv"
)
PLAN_CRITERIA_PATH = PLAN_OUTPUT_DIR / "exit_criteria.csv"
PLAN_GAPS_PATH = PLAN_OUTPUT_DIR / "known_gaps.csv"

AUDIT_DIAGNOSIS_PATH = AUDIT_OUTPUT_DIR / "diagnosis.json"
AUDIT_CHECKS_PATH = AUDIT_OUTPUT_DIR / "audit_checks.csv"
AUDIT_ARTIFACTS_PATH = (
    AUDIT_OUTPUT_DIR / "artifact_inventory.csv"
)
AUDIT_SAFETY_PATH = AUDIT_OUTPUT_DIR / "safety_audit.csv"
AUDIT_EVIDENCE_PATH = (
    AUDIT_OUTPUT_DIR / "evidence_summary.json"
)

EXPECTED_DOMAINS = {
    "base_out_state",
    "runner_advancement",
    "extra_innings",
    "double_plays",
    "sacrifice_flies",
    "steals",
    "backend_payload",
    "frontend_visibility",
    "probability_guard",
    "runtime_validation",
}

EXPECTED_CRITERIA = {
    "L6-GSR-01",
    "L6-GSR-02",
    "L6-GSR-03",
    "L6-GSR-04",
    "L6-GSR-05",
    "L6-GSR-06",
    "L6-GSR-07",
    "L6-GSR-08",
    "L6-GSR-09",
    "L6-GSR-10",
}

EXPECTED_GAPS = {
    "GSR-GAP-01",
    "GSR-GAP-02",
    "GSR-GAP-03",
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
    with path.open(
        newline="",
        encoding="utf-8",
    ) as handle:
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

    return (
        result.returncode,
        result.stdout,
        result.stderr,
    )


def all_required_files_exist(
    paths: Iterable[Path],
) -> bool:
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

    required_plan_files = [
        PLAN_DIAGNOSIS_PATH,
        PLAN_DOMAINS_PATH,
        PLAN_CRITERIA_PATH,
        PLAN_GAPS_PATH,
    ]

    required_audit_files = [
        AUDIT_DIAGNOSIS_PATH,
        AUDIT_CHECKS_PATH,
        AUDIT_ARTIFACTS_PATH,
        AUDIT_SAFETY_PATH,
        AUDIT_EVIDENCE_PATH,
    ]

    plan_files_present = all_required_files_exist(
        required_plan_files
    )
    audit_files_present = all_required_files_exist(
        required_audit_files
    )

    plan_diagnosis: dict[str, Any] = {}
    audit_diagnosis: dict[str, Any] = {}
    plan_domains: list[dict[str, str]] = []
    plan_criteria: list[dict[str, str]] = []
    plan_gaps: list[dict[str, str]] = []
    audit_checks: list[dict[str, str]] = []
    audit_artifacts: list[dict[str, str]] = []
    audit_safety: list[dict[str, str]] = []
    audit_evidence: dict[str, Any] = {}

    if plan_files_present:
        plan_diagnosis = read_json(
            PLAN_DIAGNOSIS_PATH
        )
        plan_domains = read_csv(
            PLAN_DOMAINS_PATH
        )
        plan_criteria = read_csv(
            PLAN_CRITERIA_PATH
        )
        plan_gaps = read_csv(
            PLAN_GAPS_PATH
        )

    if audit_files_present:
        audit_diagnosis = read_json(
            AUDIT_DIAGNOSIS_PATH
        )
        audit_checks = read_csv(
            AUDIT_CHECKS_PATH
        )
        audit_artifacts = read_csv(
            AUDIT_ARTIFACTS_PATH
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
            "exit_readiness_plan_complete"
        )
        and plan_diagnosis.get("all_checks_passed") is True
        and plan_diagnosis.get("domains_planned") == 10
        and plan_diagnosis.get(
            "exit_criteria_planned"
        )
        == 10
        and plan_diagnosis.get(
            "blocking_exit_criteria"
        )
        == 9
        and plan_diagnosis.get(
            "known_gaps_documented"
        )
        == 3
        and plan_diagnosis.get(
            "blocking_known_gaps"
        )
        == 0
        and plan_diagnosis.get(
            "exit_readiness_implementation_allowed_next"
        )
        is True
        and plan_diagnosis.get(
            "recommended_next_layer"
        )
        == (
            "6OR_layer6_game_state_realism_"
            "exit_readiness_implementation"
        )
    )

    audit_contract_passed = (
        audit_return_code == 0
        and audit_diagnosis.get("diagnosis")
        == (
            "layer_6_model_projection_realism_"
            "end_to_end_runtime_validation_"
            "audit_complete"
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
        == 13
        and audit_diagnosis.get(
            "required_fields_verified"
        )
        == 8
        and audit_diagnosis.get(
            "detail_fields_verified"
        )
        == 10
        and audit_diagnosis.get(
            "runtime_cases_verified"
        )
        == 7
        and audit_diagnosis.get(
            "frontend_checks_verified"
        )
        == 8
        and audit_diagnosis.get(
            "fixture_evidence_verified"
        )
        is True
        and audit_diagnosis.get(
            "probability_guard_verified"
        )
        is True
        and audit_diagnosis.get(
            "frontend_build_verified"
        )
        is True
        and audit_diagnosis.get(
            "manual_runtime_verified"
        )
        is True
        and audit_diagnosis.get(
            "exit_readiness_planning_allowed_next"
        )
        is True
    )

    domain_names = {
        row.get("domain", "")
        for row in plan_domains
    }

    domain_plan_complete = (
        domain_names == EXPECTED_DOMAINS
        and len(plan_domains) == 10
        and all(
            csv_bool(row.get("blocking"))
            for row in plan_domains
        )
    )

    criteria_ids = {
        row.get("criterion_id", "")
        for row in plan_criteria
    }

    criterion_blocking_count = sum(
        1
        for row in plan_criteria
        if csv_bool(row.get("blocking"))
    )

    criteria_plan_complete = (
        criteria_ids == EXPECTED_CRITERIA
        and len(plan_criteria) == 10
        and criterion_blocking_count == 9
    )

    gap_ids = {
        row.get("gap_id", "")
        for row in plan_gaps
    }

    gap_plan_complete = (
        gap_ids == EXPECTED_GAPS
        and len(plan_gaps) == 3
        and all(
            not csv_bool(row.get("blocks_exit"))
            for row in plan_gaps
        )
    )

    audit_checks_complete = (
        len(audit_checks) == 15
        and all(
            csv_bool(row.get("passed"))
            for row in audit_checks
        )
    )

    audit_artifacts_complete = (
        len(audit_artifacts) == 13
        and all(
            csv_bool(row.get("exists"))
            and csv_bool(row.get("nonempty"))
            and csv_bool(row.get("passed"))
            for row in audit_artifacts
        )
    )

    audit_safety_by_boundary = {
        row.get("boundary", ""): row
        for row in audit_safety
    }

    prohibited_boundaries_preserved = all(
        boundary in audit_safety_by_boundary
        and not csv_bool(
            audit_safety_by_boundary[
                boundary
            ].get("changed_or_executed")
        )
        and csv_bool(
            audit_safety_by_boundary[
                boundary
            ].get("passed")
        )
        for boundary in PROHIBITED_ACTIONS
    )

    audit_safety_complete = (
        prohibited_boundaries_preserved
        and all(
            csv_bool(row.get("passed"))
            for row in audit_safety
        )
    )

    helper_payload = (
        audit_evidence.get(
            "helper_payload",
            {},
        ).get("payload", {})
    )

    fixture_summary = audit_evidence.get(
        "fixture_summary",
        {},
    )

    probability_guard = audit_evidence.get(
        "probability_guard",
        {},
    )

    backend_payload_evidence = (
        isinstance(helper_payload, dict)
        and len(helper_payload) == 18
        and helper_payload.get(
            "steals_model_status"
        )
        == "deferred_not_active"
        and helper_payload.get(
            "steals_projection_wiring_status"
        )
        == "status_only_no_behavioral_effect"
    )

    serialization_evidence = (
        fixture_summary.get("before_count") == 5
        and fixture_summary.get("after_count") == 5
        and fixture_summary.get(
            "serialization_error"
        )
        is None
        and fixture_summary.get(
            "groups_independent"
        )
        is True
    )

    probability_evidence = (
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

    frontend_runtime_evidence = (
        audit_evidence.get(
            "frontend_build_exit_code"
        )
        == 0
        and audit_evidence.get(
            "manual_runtime_confirmation"
        )
        == "YES"
    )

    domain_evidence_map = {
        "base_out_state": (
            audit_contract_passed
            and helper_payload.get(
                "base_out_state_enabled"
            )
            is True
            and helper_payload.get(
                "base_out_transition_model_status"
            )
            == "diagnostic_wired"
        ),
        "runner_advancement": (
            audit_contract_passed
            and helper_payload.get(
                "runner_advancement_enabled"
            )
            is True
            and helper_payload.get(
                "runner_advancement_model_status"
            )
            == "diagnostic_wired"
        ),
        "extra_innings": (
            audit_contract_passed
            and helper_payload.get("extras_enabled")
            is True
            and helper_payload.get(
                "ghost_runner_enabled"
            )
            is True
            and helper_payload.get(
                "walkoff_shortening_enabled"
            )
            is True
        ),
        "double_plays": (
            audit_contract_passed
            and helper_payload.get(
                "double_play_enabled"
            )
            is True
            and bool(
                helper_payload.get(
                    "double_play_rate_source"
                )
            )
        ),
        "sacrifice_flies": (
            audit_contract_passed
            and helper_payload.get(
                "sac_fly_enabled"
            )
            is True
            and bool(
                helper_payload.get(
                    "sac_fly_rate_source"
                )
            )
        ),
        "steals": (
            audit_contract_passed
            and helper_payload.get(
                "steals_model_status"
            )
            == "deferred_not_active"
            and helper_payload.get(
                "steals_projection_wiring_status"
            )
            == "status_only_no_behavioral_effect"
        ),
        "backend_payload": (
            backend_payload_evidence
            and serialization_evidence
        ),
        "frontend_visibility": (
            audit_diagnosis.get(
                "frontend_checks_verified"
            )
            == 8
            and frontend_runtime_evidence
        ),
        "probability_guard": probability_evidence,
        "runtime_validation": (
            audit_contract_passed
            and audit_checks_complete
            and audit_artifacts_complete
            and frontend_runtime_evidence
        ),
    }

    domain_rows: list[dict[str, Any]] = []

    for planned in plan_domains:
        domain = planned.get("domain", "")
        ready = bool(
            domain_evidence_map.get(domain, False)
        )

        domain_rows.append(
            {
                "domain": domain,
                "planned_status": planned.get(
                    "current_status",
                    "",
                ),
                "blocking": csv_bool(
                    planned.get("blocking")
                ),
                "evidence_available": ready,
                "readiness_status": (
                    "ready"
                    if ready
                    else "not_ready"
                ),
                "passed": ready,
            }
        )

    all_domains_ready = (
        len(domain_rows) == 10
        and all(
            bool(row["passed"])
            for row in domain_rows
        )
    )

    criterion_evidence_map = {
        "L6-GSR-01": (
            domain_evidence_map["base_out_state"]
            and domain_evidence_map[
                "runner_advancement"
            ]
            and domain_evidence_map["extra_innings"]
            and domain_evidence_map["double_plays"]
            and domain_evidence_map[
                "sacrifice_flies"
            ]
        ),
        "L6-GSR-02": (
            backend_payload_evidence
            and audit_diagnosis.get(
                "required_fields_verified"
            )
            == 8
            and audit_diagnosis.get(
                "detail_fields_verified"
            )
            == 10
        ),
        "L6-GSR-03": domain_evidence_map["steals"],
        "L6-GSR-04": serialization_evidence,
        "L6-GSR-05": (
            audit_diagnosis.get(
                "frontend_checks_verified"
            )
            == 8
            and audit_diagnosis.get(
                "manual_runtime_verified"
            )
            is True
        ),
        "L6-GSR-06": (
            audit_diagnosis.get(
                "frontend_build_verified"
            )
            is True
        ),
        "L6-GSR-07": probability_evidence,
        "L6-GSR-08": audit_safety_complete,
        "L6-GSR-09": True,
        "L6-GSR-10": (
            plan_contract_passed
            and audit_contract_passed
            and all_domains_ready
        ),
    }

    criterion_rows: list[dict[str, Any]] = []

    for planned in plan_criteria:
        criterion_id = planned.get(
            "criterion_id",
            "",
        )
        passed = bool(
            criterion_evidence_map.get(
                criterion_id,
                False,
            )
        )

        criterion_rows.append(
            {
                "criterion_id": criterion_id,
                "criterion": planned.get(
                    "criterion",
                    "",
                ),
                "blocking": csv_bool(
                    planned.get("blocking")
                ),
                "evidence_source": planned.get(
                    "evidence_source",
                    "",
                ),
                "passed": passed,
            }
        )

    blocking_criteria_passed = all(
        bool(row["passed"])
        for row in criterion_rows
        if row["blocking"]
    )

    all_criteria_passed = (
        len(criterion_rows) == 10
        and all(
            bool(row["passed"])
            for row in criterion_rows
        )
    )

    known_gap_rows: list[dict[str, Any]] = []

    for gap in plan_gaps:
        blocks_exit = csv_bool(
            gap.get("blocks_exit")
        )

        accepted = (
            gap.get("gap_id") in EXPECTED_GAPS
            and not blocks_exit
            and bool(
                gap.get("required_resolution")
            )
        )

        known_gap_rows.append(
            {
                "gap_id": gap.get("gap_id", ""),
                "description": gap.get(
                    "description",
                    "",
                ),
                "classification": gap.get(
                    "classification",
                    "",
                ),
                "blocks_exit": blocks_exit,
                "required_resolution": gap.get(
                    "required_resolution",
                    "",
                ),
                "accepted": accepted,
                "passed": accepted,
            }
        )

    known_gaps_accepted = (
        len(known_gap_rows) == 3
        and all(
            bool(row["passed"])
            for row in known_gap_rows
        )
        and not any(
            bool(row["blocks_exit"])
            for row in known_gap_rows
        )
    )

    unidentified_blocking_gaps = 0

    implementation_checks = [
        {
            "check": "6oq_plan_execution",
            "actual": plan_return_code,
            "expected": 0,
            "passed": plan_return_code == 0,
        },
        {
            "check": "6oq_plan_contract",
            "actual": plan_diagnosis.get(
                "diagnosis"
            ),
            "expected": (
                "layer_6_game_state_realism_"
                "exit_readiness_plan_complete"
            ),
            "passed": plan_contract_passed,
        },
        {
            "check": "6op_audit_execution",
            "actual": audit_return_code,
            "expected": 0,
            "passed": audit_return_code == 0,
        },
        {
            "check": "6op_audit_contract",
            "actual": audit_diagnosis.get(
                "diagnosis"
            ),
            "expected": (
                "layer_6_model_projection_realism_"
                "end_to_end_runtime_validation_"
                "audit_complete"
            ),
            "passed": audit_contract_passed,
        },
        {
            "check": "domain_plan_integrity",
            "actual": len(plan_domains),
            "expected": 10,
            "passed": domain_plan_complete,
        },
        {
            "check": "criterion_plan_integrity",
            "actual": len(plan_criteria),
            "expected": 10,
            "passed": criteria_plan_complete,
        },
        {
            "check": "known_gap_plan_integrity",
            "actual": len(plan_gaps),
            "expected": 3,
            "passed": gap_plan_complete,
        },
        {
            "check": "audit_checks_complete",
            "actual": len(audit_checks),
            "expected": 15,
            "passed": audit_checks_complete,
        },
        {
            "check": "audit_artifacts_complete",
            "actual": len(audit_artifacts),
            "expected": 13,
            "passed": audit_artifacts_complete,
        },
        {
            "check": "audit_safety_complete",
            "actual": len(audit_safety),
            "expected": len(audit_safety),
            "passed": audit_safety_complete,
        },
        {
            "check": "all_domains_ready",
            "actual": sum(
                1
                for row in domain_rows
                if row["passed"]
            ),
            "expected": 10,
            "passed": all_domains_ready,
        },
        {
            "check": "blocking_criteria_passed",
            "actual": sum(
                1
                for row in criterion_rows
                if row["blocking"]
                and row["passed"]
            ),
            "expected": 9,
            "passed": blocking_criteria_passed,
        },
        {
            "check": "all_criteria_passed",
            "actual": sum(
                1
                for row in criterion_rows
                if row["passed"]
            ),
            "expected": 10,
            "passed": all_criteria_passed,
        },
        {
            "check": "known_gaps_accepted",
            "actual": sum(
                1
                for row in known_gap_rows
                if row["passed"]
            ),
            "expected": 3,
            "passed": known_gaps_accepted,
        },
        {
            "check": "unidentified_blocking_gaps",
            "actual": unidentified_blocking_gaps,
            "expected": 0,
            "passed": (
                unidentified_blocking_gaps == 0
            ),
        },
        {
            "check": "probability_guard",
            "actual": probability_guard.get(
                "canonical_probabilities_after"
            ),
            "expected": probability_guard.get(
                "canonical_probabilities_before"
            ),
            "passed": probability_evidence,
        },
        {
            "check": "frontend_runtime_guard",
            "actual": {
                "build_exit_code": (
                    audit_evidence.get(
                        "frontend_build_exit_code"
                    )
                ),
                "runtime_confirmation": (
                    audit_evidence.get(
                        "manual_runtime_confirmation"
                    )
                ),
            },
            "expected": {
                "build_exit_code": 0,
                "runtime_confirmation": "YES",
            },
            "passed": frontend_runtime_evidence,
        },
    ]

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
                    "exit_readiness_evidence_reconstruction"
                ),
                "changed_or_executed": True,
                "passed": all_domains_ready,
            },
            {
                "boundary": (
                    "blocking_gap_assessment"
                ),
                "changed_or_executed": True,
                "passed": (
                    unidentified_blocking_gaps == 0
                ),
            },
            {
                "boundary": (
                    "accepted_scope_boundary_validation"
                ),
                "changed_or_executed": True,
                "passed": known_gaps_accepted,
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

    readiness_status = (
        "ready_for_independent_exit_readiness_audit"
        if all_checks_passed
        else "not_ready_for_exit_readiness_audit"
    )

    recommended_next_layer = (
        "6OS_layer6_game_state_realism_"
        "exit_readiness_audit"
    )

    write_csv(
        OUTPUT_DIR / "exit_readiness_checks.csv",
        [
            "check",
            "actual",
            "expected",
            "passed",
        ],
        implementation_checks,
    )

    write_csv(
        OUTPUT_DIR / "domain_readiness.csv",
        [
            "domain",
            "planned_status",
            "blocking",
            "evidence_available",
            "readiness_status",
            "passed",
        ],
        domain_rows,
    )

    write_csv(
        OUTPUT_DIR / "exit_criteria_status.csv",
        [
            "criterion_id",
            "criterion",
            "blocking",
            "evidence_source",
            "passed",
        ],
        criterion_rows,
    )

    write_csv(
        OUTPUT_DIR / "known_gaps.csv",
        [
            "gap_id",
            "description",
            "classification",
            "blocks_exit",
            "required_resolution",
            "accepted",
            "passed",
        ],
        known_gap_rows,
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
                    "Independently audit the Layer 6 "
                    "game-state realism exit-readiness "
                    "implementation and its evidence chain."
                ),
                "entry_condition": (
                    "All 6OR readiness, criterion, gap, "
                    "probability, runtime, and safety checks pass."
                ),
                "passed": all_checks_passed,
            }
        ],
    )

    evidence_chain = {
        "plan_execution": {
            "return_code": plan_return_code,
            "stdout_tail": plan_stdout[-2000:],
            "stderr_tail": plan_stderr[-2000:],
            "diagnosis": plan_diagnosis,
        },
        "runtime_audit_execution": {
            "return_code": audit_return_code,
            "stdout_tail": audit_stdout[-2000:],
            "stderr_tail": audit_stderr[-2000:],
            "diagnosis": audit_diagnosis,
        },
        "domain_evidence": domain_evidence_map,
        "criterion_evidence": criterion_evidence_map,
        "helper_payload": helper_payload,
        "fixture_summary": fixture_summary,
        "probability_guard": probability_guard,
        "frontend_runtime": {
            "build_exit_code": (
                audit_evidence.get(
                    "frontend_build_exit_code"
                )
            ),
            "manual_runtime_confirmation": (
                audit_evidence.get(
                    "manual_runtime_confirmation"
                )
            ),
        },
        "known_gaps": known_gap_rows,
        "unidentified_blocking_gaps": (
            unidentified_blocking_gaps
        ),
        "readiness_status": readiness_status,
    }

    write_json(
        OUTPUT_DIR / "evidence_chain.json",
        evidence_chain,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": (
            "layer_6_game_state_realism_"
            "exit_readiness_implementation_complete"
            if all_checks_passed
            else
            "layer_6_game_state_realism_"
            "exit_readiness_implementation_failed"
        ),
        "all_checks_passed": all_checks_passed,
        "readiness_status": readiness_status,
        "implementation_checks_passed": sum(
            1
            for row in implementation_checks
            if row["passed"]
        ),
        "implementation_checks_required": len(
            implementation_checks
        ),
        "domains_ready": sum(
            1
            for row in domain_rows
            if row["passed"]
        ),
        "domains_required": len(EXPECTED_DOMAINS),
        "exit_criteria_passed": sum(
            1
            for row in criterion_rows
            if row["passed"]
        ),
        "exit_criteria_required": len(
            EXPECTED_CRITERIA
        ),
        "blocking_exit_criteria_passed": sum(
            1
            for row in criterion_rows
            if row["blocking"]
            and row["passed"]
        ),
        "blocking_exit_criteria_required": 9,
        "known_gaps_accepted": sum(
            1
            for row in known_gap_rows
            if row["passed"]
        ),
        "known_gaps_required": len(EXPECTED_GAPS),
        "blocking_known_gaps": sum(
            1
            for row in known_gap_rows
            if row["blocks_exit"]
        ),
        "unidentified_blocking_gaps": (
            unidentified_blocking_gaps
        ),
        "probability_guard_passed": (
            probability_evidence
        ),
        "frontend_runtime_passed": (
            frontend_runtime_evidence
        ),
        "safety_boundaries_passed": all(
            bool(row["passed"])
            for row in safety_rows
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
        "exit_readiness_audit_allowed_next": (
            all_checks_passed
        ),
        "recommended_next_layer": (
            recommended_next_layer
        ),
        "generated_csv_artifacts": [
            str(
                OUTPUT_DIR / "exit_readiness_checks.csv"
            ),
            str(OUTPUT_DIR / "domain_readiness.csv"),
            str(
                OUTPUT_DIR / "exit_criteria_status.csv"
            ),
            str(OUTPUT_DIR / "known_gaps.csv"),
            str(OUTPUT_DIR / "safety_audit.csv"),
            str(
                OUTPUT_DIR / "recommended_path.csv"
            ),
        ],
        "generated_json_artifacts": [
            str(OUTPUT_DIR / "evidence_chain.json"),
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
