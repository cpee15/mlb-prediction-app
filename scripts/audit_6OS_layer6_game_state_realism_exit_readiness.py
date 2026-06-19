#!/usr/bin/env python3
"""
Layer 6OS
Layer 6 Game-State Realism Exit Readiness Audit

Independently re-executes and audits the 6OR exit-readiness
implementation and its evidence chain.

No production behavior, probability, tuning, pricing, historical
validation, or edge-detection changes are made.
"""

from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "6OS"
LAYER_NAME = "layer6_game_state_realism_exit_readiness_audit"

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = ROOT / (
    "tmp/layer_6OS_game_state_realism_exit_readiness_audit"
)

PREDECESSOR_SCRIPT = (
    ROOT
    / "scripts/implement_6OR_layer6_game_state_realism_"
    "exit_readiness.py"
)

PREDECESSOR_OUTPUT_DIR = ROOT / (
    "tmp/layer_6OR_game_state_realism_"
    "exit_readiness_implementation"
)

DIAGNOSIS_PATH = PREDECESSOR_OUTPUT_DIR / "diagnosis.json"
CHECKS_PATH = PREDECESSOR_OUTPUT_DIR / "exit_readiness_checks.csv"
DOMAINS_PATH = PREDECESSOR_OUTPUT_DIR / "domain_readiness.csv"
CRITERIA_PATH = (
    PREDECESSOR_OUTPUT_DIR / "exit_criteria_status.csv"
)
GAPS_PATH = PREDECESSOR_OUTPUT_DIR / "known_gaps.csv"
SAFETY_PATH = PREDECESSOR_OUTPUT_DIR / "safety_audit.csv"
RECOMMENDED_PATH = (
    PREDECESSOR_OUTPUT_DIR / "recommended_path.csv"
)
EVIDENCE_CHAIN_PATH = (
    PREDECESSOR_OUTPUT_DIR / "evidence_chain.json"
)

EXPECTED_ARTIFACTS = [
    CHECKS_PATH,
    DOMAINS_PATH,
    CRITERIA_PATH,
    GAPS_PATH,
    SAFETY_PATH,
    RECOMMENDED_PATH,
    EVIDENCE_CHAIN_PATH,
    DIAGNOSIS_PATH,
]

EXPECTED_CHECKS = {
    "6oq_plan_execution",
    "6oq_plan_contract",
    "6op_audit_execution",
    "6op_audit_contract",
    "domain_plan_integrity",
    "criterion_plan_integrity",
    "known_gap_plan_integrity",
    "audit_checks_complete",
    "audit_artifacts_complete",
    "audit_safety_complete",
    "all_domains_ready",
    "blocking_criteria_passed",
    "all_criteria_passed",
    "known_gaps_accepted",
    "unidentified_blocking_gaps",
    "probability_guard",
    "frontend_runtime_guard",
}

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
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
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

    artifacts_passed = all(
        bool(row["passed"])
        for row in artifact_rows
    )

    diagnosis: dict[str, Any] = {}
    implementation_rows: list[dict[str, str]] = []
    domain_rows: list[dict[str, str]] = []
    criterion_rows: list[dict[str, str]] = []
    gap_rows: list[dict[str, str]] = []
    safety_rows: list[dict[str, str]] = []
    recommended_rows: list[dict[str, str]] = []
    evidence_chain: dict[str, Any] = {}

    if artifacts_passed:
        diagnosis = read_json(DIAGNOSIS_PATH)
        implementation_rows = read_csv(CHECKS_PATH)
        domain_rows = read_csv(DOMAINS_PATH)
        criterion_rows = read_csv(CRITERIA_PATH)
        gap_rows = read_csv(GAPS_PATH)
        safety_rows = read_csv(SAFETY_PATH)
        recommended_rows = read_csv(RECOMMENDED_PATH)
        evidence_chain = read_json(EVIDENCE_CHAIN_PATH)

    diagnosis_contract_passed = (
        predecessor_return_code == 0
        and diagnosis.get("diagnosis")
        == (
            "layer_6_game_state_realism_"
            "exit_readiness_implementation_complete"
        )
        and diagnosis.get("all_checks_passed") is True
        and diagnosis.get("readiness_status")
        == "ready_for_independent_exit_readiness_audit"
        and diagnosis.get("implementation_checks_passed")
        == diagnosis.get("implementation_checks_required")
        == 17
        and diagnosis.get("domains_ready")
        == diagnosis.get("domains_required")
        == 10
        and diagnosis.get("exit_criteria_passed")
        == diagnosis.get("exit_criteria_required")
        == 10
        and diagnosis.get(
            "blocking_exit_criteria_passed"
        )
        == diagnosis.get(
            "blocking_exit_criteria_required"
        )
        == 9
        and diagnosis.get("known_gaps_accepted")
        == diagnosis.get("known_gaps_required")
        == 3
        and diagnosis.get("blocking_known_gaps") == 0
        and diagnosis.get(
            "unidentified_blocking_gaps"
        )
        == 0
        and diagnosis.get("probability_guard_passed")
        is True
        and diagnosis.get("frontend_runtime_passed")
        is True
        and diagnosis.get("safety_boundaries_passed")
        is True
        and diagnosis.get(
            "exit_readiness_audit_allowed_next"
        )
        is True
        and diagnosis.get("recommended_next_layer")
        == (
            "6OS_layer6_game_state_realism_"
            "exit_readiness_audit"
        )
    )

    check_names = {
        row.get("check", "")
        for row in implementation_rows
    }

    implementation_checks_passed = (
        check_names == EXPECTED_CHECKS
        and len(implementation_rows) == 17
        and all(
            csv_bool(row.get("passed"))
            for row in implementation_rows
        )
    )

    domain_names = {
        row.get("domain", "")
        for row in domain_rows
    }

    domain_evidence_passed = (
        domain_names == EXPECTED_DOMAINS
        and len(domain_rows) == 10
        and all(
            csv_bool(row.get("blocking"))
            and csv_bool(
                row.get("evidence_available")
            )
            and row.get("readiness_status") == "ready"
            and csv_bool(row.get("passed"))
            for row in domain_rows
        )
    )

    criterion_ids = {
        row.get("criterion_id", "")
        for row in criterion_rows
    }

    blocking_criterion_rows = [
        row
        for row in criterion_rows
        if csv_bool(row.get("blocking"))
    ]

    criterion_evidence_passed = (
        criterion_ids == EXPECTED_CRITERIA
        and len(criterion_rows) == 10
        and len(blocking_criterion_rows) == 9
        and all(
            csv_bool(row.get("passed"))
            for row in criterion_rows
        )
    )

    gap_ids = {
        row.get("gap_id", "")
        for row in gap_rows
    }

    gap_evidence_passed = (
        gap_ids == EXPECTED_GAPS
        and len(gap_rows) == 3
        and all(
            not csv_bool(row.get("blocks_exit"))
            and csv_bool(row.get("accepted"))
            and csv_bool(row.get("passed"))
            and bool(row.get("required_resolution"))
            for row in gap_rows
        )
    )

    safety_by_boundary = {
        row.get("boundary", ""): row
        for row in safety_rows
    }

    prohibited_boundaries_passed = all(
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

    safety_evidence_passed = (
        prohibited_boundaries_passed
        and all(
            csv_bool(row.get("passed"))
            for row in safety_rows
        )
    )

    plan_execution = evidence_chain.get(
        "plan_execution",
        {},
    )

    runtime_audit_execution = evidence_chain.get(
        "runtime_audit_execution",
        {},
    )

    domain_evidence = evidence_chain.get(
        "domain_evidence",
        {},
    )

    criterion_evidence = evidence_chain.get(
        "criterion_evidence",
        {},
    )

    helper_payload = evidence_chain.get(
        "helper_payload",
        {},
    )

    fixture_summary = evidence_chain.get(
        "fixture_summary",
        {},
    )

    probability_guard = evidence_chain.get(
        "probability_guard",
        {},
    )

    frontend_runtime = evidence_chain.get(
        "frontend_runtime",
        {},
    )

    chain_known_gaps = evidence_chain.get(
        "known_gaps",
        [],
    )

    evidence_chain_passed = (
        plan_execution.get("return_code") == 0
        and plan_execution.get(
            "diagnosis",
            {},
        ).get("all_checks_passed")
        is True
        and runtime_audit_execution.get(
            "return_code"
        )
        == 0
        and runtime_audit_execution.get(
            "diagnosis",
            {},
        ).get("all_checks_passed")
        is True
        and set(domain_evidence)
        == EXPECTED_DOMAINS
        and all(
            value is True
            for value in domain_evidence.values()
        )
        and set(criterion_evidence)
        == EXPECTED_CRITERIA
        and all(
            value is True
            for value in criterion_evidence.values()
        )
        and isinstance(helper_payload, dict)
        and len(helper_payload) == 18
        and helper_payload.get(
            "steals_model_status"
        )
        == "deferred_not_active"
        and helper_payload.get(
            "steals_projection_wiring_status"
        )
        == "status_only_no_behavioral_effect"
        and fixture_summary.get("before_count") == 5
        and fixture_summary.get("after_count") == 5
        and fixture_summary.get(
            "serialization_error"
        )
        is None
        and fixture_summary.get(
            "groups_independent"
        )
        is True
        and len(chain_known_gaps) == 3
        and evidence_chain.get(
            "unidentified_blocking_gaps"
        )
        == 0
        and evidence_chain.get("readiness_status")
        == "ready_for_independent_exit_readiness_audit"
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

    recommended_path_passed = (
        len(recommended_rows) == 1
        and recommended_rows[0].get(
            "recommended_next_layer"
        )
        == (
            "6OS_layer6_game_state_realism_"
            "exit_readiness_audit"
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
                "layer_6_game_state_realism_"
                "exit_readiness_implementation_complete"
            ),
            "passed": diagnosis_contract_passed,
        },
        {
            "check": "implementation_check_evidence",
            "actual": sum(
                1
                for row in implementation_rows
                if csv_bool(row.get("passed"))
            ),
            "expected": 17,
            "passed": implementation_checks_passed,
        },
        {
            "check": "domain_readiness_evidence",
            "actual": sum(
                1
                for row in domain_rows
                if csv_bool(row.get("passed"))
            ),
            "expected": 10,
            "passed": domain_evidence_passed,
        },
        {
            "check": "exit_criterion_evidence",
            "actual": sum(
                1
                for row in criterion_rows
                if csv_bool(row.get("passed"))
            ),
            "expected": 10,
            "passed": criterion_evidence_passed,
        },
        {
            "check": "blocking_criterion_evidence",
            "actual": sum(
                1
                for row in blocking_criterion_rows
                if csv_bool(row.get("passed"))
            ),
            "expected": 9,
            "passed": (
                criterion_evidence_passed
                and len(blocking_criterion_rows) == 9
            ),
        },
        {
            "check": "known_gap_evidence",
            "actual": sum(
                1
                for row in gap_rows
                if csv_bool(row.get("passed"))
            ),
            "expected": 3,
            "passed": gap_evidence_passed,
        },
        {
            "check": "blocking_known_gaps",
            "actual": sum(
                1
                for row in gap_rows
                if csv_bool(row.get("blocks_exit"))
            ),
            "expected": 0,
            "passed": (
                gap_evidence_passed
                and not any(
                    csv_bool(row.get("blocks_exit"))
                    for row in gap_rows
                )
            ),
        },
        {
            "check": "unidentified_blocking_gaps",
            "actual": evidence_chain.get(
                "unidentified_blocking_gaps"
            ),
            "expected": 0,
            "passed": (
                evidence_chain.get(
                    "unidentified_blocking_gaps"
                )
                == 0
            ),
        },
        {
            "check": "evidence_chain",
            "actual": evidence_chain.get(
                "readiness_status"
            ),
            "expected": (
                "ready_for_independent_"
                "exit_readiness_audit"
            ),
            "passed": evidence_chain_passed,
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
            "check": "safety_boundaries",
            "actual": sum(
                1
                for row in safety_rows
                if csv_bool(row.get("passed"))
            ),
            "expected": len(safety_rows),
            "passed": safety_evidence_passed,
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
                "6OS_layer6_game_state_realism_"
                "exit_readiness_audit"
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
                    "exit_readiness_reexecution"
                ),
                "changed_or_executed": True,
                "passed": predecessor_return_code == 0,
            },
            {
                "boundary": (
                    "independent_evidence_chain_audit"
                ),
                "changed_or_executed": True,
                "passed": evidence_chain_passed,
            },
            {
                "boundary": (
                    "blocking_gap_independent_audit"
                ),
                "changed_or_executed": True,
                "passed": (
                    gap_evidence_passed
                    and evidence_chain.get(
                        "unidentified_blocking_gaps"
                    )
                    == 0
                ),
            },
        ]
    )

    recommended_next_layer = (
        "6OT_layer6_game_state_realism_"
        "exit_decision_plan"
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
        OUTPUT_DIR / "domain_audit.csv",
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
        OUTPUT_DIR / "criterion_audit.csv",
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
        OUTPUT_DIR / "gap_audit.csv",
        [
            "gap_id",
            "description",
            "classification",
            "blocks_exit",
            "required_resolution",
            "accepted",
            "passed",
        ],
        gap_rows,
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
                    "Plan the formal Layer 6 game-state realism "
                    "exit decision using independently audited "
                    "readiness evidence."
                ),
                "entry_condition": (
                    "Every 6OS audit, evidence, gap, "
                    "probability, runtime, and safety check passes."
                ),
                "passed": all_checks_passed,
            }
        ],
    )

    write_json(
        OUTPUT_DIR / "audited_evidence_chain.json",
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
            "domain_evidence": domain_evidence,
            "criterion_evidence": criterion_evidence,
            "helper_payload": helper_payload,
            "fixture_summary": fixture_summary,
            "probability_guard": probability_guard,
            "frontend_runtime": frontend_runtime,
            "known_gaps": chain_known_gaps,
            "unidentified_blocking_gaps": (
                evidence_chain.get(
                    "unidentified_blocking_gaps"
                )
            ),
            "readiness_status": (
                evidence_chain.get(
                    "readiness_status"
                )
            ),
        },
    )

    diagnosis_output = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": (
            "layer_6_game_state_realism_"
            "exit_readiness_audit_complete"
            if all_checks_passed
            else
            "layer_6_game_state_realism_"
            "exit_readiness_audit_failed"
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
        "artifacts_required": len(EXPECTED_ARTIFACTS),
        "domains_verified": (
            10 if domain_evidence_passed else 0
        ),
        "domains_required": 10,
        "criteria_verified": (
            10 if criterion_evidence_passed else 0
        ),
        "criteria_required": 10,
        "blocking_criteria_verified": (
            9 if criterion_evidence_passed else 0
        ),
        "blocking_criteria_required": 9,
        "known_gaps_verified": (
            3 if gap_evidence_passed else 0
        ),
        "known_gaps_required": 3,
        "blocking_known_gaps": sum(
            1
            for row in gap_rows
            if csv_bool(row.get("blocks_exit"))
        ),
        "unidentified_blocking_gaps": (
            evidence_chain.get(
                "unidentified_blocking_gaps"
            )
        ),
        "evidence_chain_verified": (
            evidence_chain_passed
        ),
        "probability_guard_verified": (
            probability_guard_passed
        ),
        "frontend_runtime_verified": (
            frontend_runtime_passed
        ),
        "safety_boundaries_verified": (
            safety_evidence_passed
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
        "exit_decision_planning_allowed_next": (
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
            str(OUTPUT_DIR / "domain_audit.csv"),
            str(OUTPUT_DIR / "criterion_audit.csv"),
            str(OUTPUT_DIR / "gap_audit.csv"),
            str(OUTPUT_DIR / "safety_audit.csv"),
            str(
                OUTPUT_DIR / "recommended_path.csv"
            ),
        ],
        "generated_json_artifacts": [
            str(
                OUTPUT_DIR
                / "audited_evidence_chain.json"
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
