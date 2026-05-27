#!/usr/bin/env python3
"""Layer 6EM audit for the 6EL runtime summary implementation plan.

This script is audit-only. It executes the merged 6EL planning script, reads the
6EL JSON/CSV evidence artifacts, and independently verifies the implementation
plan contract before the future implementation layer.
"""

from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List


PLAN_SLUG = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_implementation_plan"
)
AUDIT_SLUG = f"{PLAN_SLUG}_audit"

PLAN_SCRIPT = Path(
    "scripts/plan_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_implementation.py"
)
AUDIT_SCRIPT = Path(
    "scripts/audit_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_implementation_plan.py"
)

TMP_DIR = Path("tmp")

PLAN_JSON = TMP_DIR / f"{PLAN_SLUG}.json"
PLAN_CHECKS_CSV = TMP_DIR / f"{PLAN_SLUG}_checks.csv"
PLAN_CURRENT_STATE_CSV = TMP_DIR / f"{PLAN_SLUG}_current_state.csv"
PLAN_SOURCE_CHANGES_CSV = TMP_DIR / f"{PLAN_SLUG}_source_changes.csv"
PLAN_FIELD_CONTRACT_CSV = TMP_DIR / f"{PLAN_SLUG}_field_contract.csv"
PLAN_STATUS_CONTRACT_CSV = TMP_DIR / f"{PLAN_SLUG}_status_contract.csv"
PLAN_VALIDATION_CSV = TMP_DIR / f"{PLAN_SLUG}_validation.csv"
PLAN_NON_GOALS_CSV = TMP_DIR / f"{PLAN_SLUG}_non_goals.csv"
PLAN_IMMUTABILITY_CSV = TMP_DIR / f"{PLAN_SLUG}_immutability.csv"

AUDIT_JSON = TMP_DIR / f"{AUDIT_SLUG}.json"
AUDIT_CHECKS_CSV = TMP_DIR / f"{AUDIT_SLUG}_checks.csv"
AUDIT_ARTIFACTS_CSV = TMP_DIR / f"{AUDIT_SLUG}_artifacts.csv"
AUDIT_FIELD_CONTRACT_CSV = TMP_DIR / f"{AUDIT_SLUG}_field_contract.csv"
AUDIT_STATUS_CONTRACT_CSV = TMP_DIR / f"{AUDIT_SLUG}_status_contract.csv"
AUDIT_SOURCE_CHANGES_CSV = TMP_DIR / f"{AUDIT_SLUG}_source_changes.csv"
AUDIT_VALIDATION_CSV = TMP_DIR / f"{AUDIT_SLUG}_validation.csv"
AUDIT_NON_GOALS_CSV = TMP_DIR / f"{AUDIT_SLUG}_non_goals.csv"
AUDIT_IMMUTABILITY_CSV = TMP_DIR / f"{AUDIT_SLUG}_immutability.csv"

EXPECTED_PLAN_DIAGNOSIS = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_implementation_plan_complete"
)
AUDIT_DIAGNOSIS = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_implementation_plan_audit_complete"
)
EXPECTED_6EL_NEXT_LAYER = (
    "6EM_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_implementation_plan_audit"
)
RECOMMENDED_NEXT_LAYER = (
    "6EN_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_implementation"
)

EXPECTED_HELPER = "_candidate_bullpen_live_fetcher_runtime_summary"
EXPECTED_APPLY_HELPER = "_candidate_bullpen_apply_live_fetcher_runtime_summary"
EXPECTED_TARGET_FILE = "scripts/fetch_candidate_bullpen_statcast_live_adapter.py"

EXPECTED_RUNTIME_FIELDS = [
    "live_fetcher_runtime_summary_status",
    "live_fetcher_runtime_summary_reason",
    "live_fetcher_runtime_summary_mode",
    "live_fetcher_runtime_summary_gate",
    "live_fetcher_runtime_summary_safe_to_proceed",
    "live_fetcher_runtime_summary_external_fetch_enabled",
    "live_fetcher_runtime_summary_write_blocked",
    "live_fetcher_runtime_summary_candidate_materialization_blocked",
    "live_fetcher_runtime_summary_dependency_missing",
    "live_fetcher_runtime_summary_field_version",
]

EXPECTED_STATUS_SCENARIOS = [
    "default_no_real_gate_live_dry_run",
    "synthetic_path",
    "real_gated_monkeypatch_path",
    "dependency_missing_path",
    "live_without_dry_run",
    "live_write_attempt",
    "invalid_or_multi_date_window",
]

EXPECTED_COUNTS = {
    "checks": 8,
    "current_state": 5,
    "source_changes": 4,
    "field_contract": 10,
    "status_contract": 7,
    "validation": 10,
    "non_goals": 9,
    "immutability": 8,
}

EXPECTED_VALIDATION_KEYS = [
    "source_validation",
    "default_artifact_validation",
    "synthetic_artifact_validation",
    "monkeypatched_real_gated_artifact_validation_without_network",
    "dependency_missing_validation",
    "blocked_path_validation",
    "artifact_compatibility_validation",
    "import_boundary_validation",
    "safety_validation",
    "immutability_validation",
]

EXPECTED_NON_GOAL_PHRASES = [
    "No runtime summary implementation in 6EL.",
    "No real Statcast fetch.",
    "No CI network dependency.",
    "No DB writes.",
    "No candidate materialization.",
    "No adapter changes.",
    "No resolver gate changes.",
    "No production default changes.",
    "No write-policy changes.",
]

EXPECTED_IMMUTABILITY_PATHS = [
    "scripts/backfill_candidate_bullpen_statcast_labels.py",
    "scripts/fetch_candidate_bullpen_statcast_live_adapter.py",
    "scripts/audit_pitcher_aggregate_rate_provenance.py",
    "scripts/backtest_extras_walkoff_hybrid_pairing.py",
    "scripts/backtest_transition_parameter_sensitivity.py",
    "scripts/debug_extras_walkoff_payload_paths.py",
    "fixtures",
    "6EF/6EG/6EH/6EI/6EJ/6EK validation and audit scripts",
]


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: Iterable[Dict[str, Any]]) -> int:
    rows = list(rows)
    if not rows:
        raise ValueError(f"no rows for {path}")
    fieldnames = list(rows[0].keys())
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def boolish(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() == "true"


def contains_all(values: Iterable[str], expected: Iterable[str]) -> bool:
    value_set = set(values)
    return all(item in value_set for item in expected)


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    plan_run = subprocess.run(
        [sys.executable, str(PLAN_SCRIPT)],
        check=False,
        text=True,
        capture_output=True,
    )

    plan_json_exists = PLAN_JSON.exists()
    plan: Dict[str, Any] = {}
    if plan_json_exists:
        plan = json.loads(PLAN_JSON.read_text(encoding="utf-8"))

    artifact_paths = {
        "json": PLAN_JSON,
        "checks": PLAN_CHECKS_CSV,
        "current_state": PLAN_CURRENT_STATE_CSV,
        "source_changes": PLAN_SOURCE_CHANGES_CSV,
        "field_contract": PLAN_FIELD_CONTRACT_CSV,
        "status_contract": PLAN_STATUS_CONTRACT_CSV,
        "validation": PLAN_VALIDATION_CSV,
        "non_goals": PLAN_NON_GOALS_CSV,
        "immutability": PLAN_IMMUTABILITY_CSV,
    }

    artifact_rows = []
    loaded_csvs: Dict[str, List[Dict[str, str]]] = {}
    for name, path in artifact_paths.items():
        exists = path.exists()
        row_count = None
        expected_count = EXPECTED_COUNTS.get(name)
        if exists and path.suffix == ".csv":
            rows = read_csv_rows(path)
            loaded_csvs[name] = rows
            row_count = len(rows)
        artifact_rows.append(
            {
                "artifact": name,
                "path": str(path),
                "exists": exists,
                "row_count": "" if row_count is None else row_count,
                "expected_row_count": "" if expected_count is None else expected_count,
                "row_count_valid": True if expected_count is None else row_count == expected_count,
            }
        )

    field_rows = loaded_csvs.get("field_contract", [])
    status_rows = loaded_csvs.get("status_contract", [])
    source_rows = loaded_csvs.get("source_changes", [])
    validation_rows = loaded_csvs.get("validation", [])
    non_goal_rows = loaded_csvs.get("non_goals", [])
    immutability_rows = loaded_csvs.get("immutability", [])

    field_values = [row.get("field", "") for row in field_rows]
    status_values = [row.get("scenario", "") for row in status_rows]
    validation_values = [row.get("validation", "") for row in validation_rows]
    non_goal_values = [row.get("non_goal", "") for row in non_goal_rows]
    immutability_values = [row.get("path", "") for row in immutability_rows]

    field_audit_rows = [
        {
            "field": field,
            "present_in_json": field in plan.get("runtime_summary_fields", []),
            "present_in_csv": field in field_values,
            "csv_required": next((row.get("required") for row in field_rows if row.get("field") == field), ""),
            "csv_additive": next((row.get("additive") for row in field_rows if row.get("field") == field), ""),
            "valid": field in plan.get("runtime_summary_fields", [])
            and field in field_values
            and boolish(next((row.get("required") for row in field_rows if row.get("field") == field), False))
            and boolish(next((row.get("additive") for row in field_rows if row.get("field") == field), False)),
        }
        for field in EXPECTED_RUNTIME_FIELDS
    ]

    status_audit_rows = [
        {
            "scenario": scenario,
            "present_in_json": scenario in plan.get("status_scenarios", []),
            "present_in_csv": scenario in status_values,
            "network_allowed_in_validation": next(
                (row.get("network_allowed_in_validation") for row in status_rows if row.get("scenario") == scenario),
                "",
            ),
            "valid": scenario in plan.get("status_scenarios", [])
            and scenario in status_values
            and not boolish(
                next(
                    (
                        row.get("network_allowed_in_validation")
                        for row in status_rows
                        if row.get("scenario") == scenario
                    ),
                    True,
                )
            ),
        }
        for scenario in EXPECTED_STATUS_SCENARIOS
    ]

    source_audit_rows = [
        {
            "target_file": row.get("target_file", ""),
            "future_change": row.get("future_change", ""),
            "six_em_assessment": (
                "valid_planning_only"
                if row.get("target_file") == EXPECTED_TARGET_FILE and row.get("6el_change") == "planned_only"
                else "invalid"
            ),
            "valid": row.get("target_file") == EXPECTED_TARGET_FILE and row.get("6el_change") == "planned_only",
        }
        for row in source_rows
    ]

    validation_audit_rows = [
        {
            "validation": key,
            "present": key in validation_values,
            "valid": key in validation_values,
        }
        for key in EXPECTED_VALIDATION_KEYS
    ]

    non_goal_audit_rows = [
        {
            "non_goal": phrase,
            "present": phrase in non_goal_values,
            "valid": phrase in non_goal_values,
        }
        for phrase in EXPECTED_NON_GOAL_PHRASES
    ]

    immutability_audit_rows = [
        {
            "path": path,
            "present": path in immutability_values,
            "valid": path in immutability_values,
        }
        for path in EXPECTED_IMMUTABILITY_PATHS
    ]

    checks = [
        {
            "check": "plan_script_executed",
            "passed": plan_run.returncode == 0,
            "detail": f"returncode={plan_run.returncode}",
        },
        {
            "check": "plan_json_exists",
            "passed": plan_json_exists,
            "detail": str(PLAN_JSON),
        },
        {
            "check": "plan_diagnosis",
            "passed": plan.get("diagnosis") == EXPECTED_PLAN_DIAGNOSIS,
            "detail": str(plan.get("diagnosis")),
        },
        {
            "check": "planning_only",
            "passed": plan.get("planning_only") is True,
            "detail": str(plan.get("planning_only")),
        },
        {
            "check": "plan_all_checks_passed",
            "passed": plan.get("all_checks_passed") is True,
            "detail": str(plan.get("all_checks_passed")),
        },
        {
            "check": "plan_recommended_next_layer",
            "passed": plan.get("recommended_next_layer") == EXPECTED_6EL_NEXT_LAYER,
            "detail": str(plan.get("recommended_next_layer")),
        },
        {
            "check": "future_helper_names",
            "passed": plan.get("future_helper_name") == EXPECTED_HELPER
            and plan.get("future_apply_helper_name") == EXPECTED_APPLY_HELPER,
            "detail": f"{plan.get('future_helper_name')} / {plan.get('future_apply_helper_name')}",
        },
        {
            "check": "future_target_file",
            "passed": plan.get("future_target_file") == EXPECTED_TARGET_FILE,
            "detail": str(plan.get("future_target_file")),
        },
        {
            "check": "runtime_summary_fields_exact",
            "passed": plan.get("runtime_summary_fields") == EXPECTED_RUNTIME_FIELDS,
            "detail": f"{len(plan.get('runtime_summary_fields', []))} fields",
        },
        {
            "check": "status_scenarios_exact",
            "passed": plan.get("status_scenarios") == EXPECTED_STATUS_SCENARIOS,
            "detail": f"{len(plan.get('status_scenarios', []))} scenarios",
        },
        {
            "check": "artifact_counts",
            "passed": all(row["row_count_valid"] for row in artifact_rows),
            "detail": json.dumps({row["artifact"]: row["row_count"] for row in artifact_rows}, sort_keys=True),
        },
        {
            "check": "field_contract_required_additive",
            "passed": len(field_audit_rows) == 10 and all(row["valid"] for row in field_audit_rows),
            "detail": f"{sum(1 for row in field_audit_rows if row['valid'])}/10 valid",
        },
        {
            "check": "status_contract_no_network_validation",
            "passed": len(status_audit_rows) == 7 and all(row["valid"] for row in status_audit_rows),
            "detail": f"{sum(1 for row in status_audit_rows if row['valid'])}/7 valid",
        },
        {
            "check": "source_changes_planning_only",
            "passed": len(source_audit_rows) == 4 and all(row["valid"] for row in source_audit_rows),
            "detail": f"{sum(1 for row in source_audit_rows if row['valid'])}/4 valid",
        },
        {
            "check": "validation_plan_complete",
            "passed": len(validation_audit_rows) == 10 and all(row["valid"] for row in validation_audit_rows),
            "detail": f"{sum(1 for row in validation_audit_rows if row['valid'])}/10 valid",
        },
        {
            "check": "non_goals_complete",
            "passed": len(non_goal_audit_rows) == 9 and all(row["valid"] for row in non_goal_audit_rows),
            "detail": f"{sum(1 for row in non_goal_audit_rows if row['valid'])}/9 valid",
        },
        {
            "check": "immutability_complete",
            "passed": len(immutability_audit_rows) == 8 and all(row["valid"] for row in immutability_audit_rows),
            "detail": f"{sum(1 for row in immutability_audit_rows if row['valid'])}/8 valid",
        },
        {
            "check": "audit_only_scope",
            "passed": AUDIT_SCRIPT.name.startswith("audit_") and RECOMMENDED_NEXT_LAYER.startswith("6EN_"),
            "detail": "6EM audits 6EL and recommends 6EN implementation.",
        },
    ]

    all_checks_passed = all(row["passed"] for row in checks)

    csv_counts = {
        "checks": write_csv(AUDIT_CHECKS_CSV, checks),
        "artifacts": write_csv(AUDIT_ARTIFACTS_CSV, artifact_rows),
        "field_contract": write_csv(AUDIT_FIELD_CONTRACT_CSV, field_audit_rows),
        "status_contract": write_csv(AUDIT_STATUS_CONTRACT_CSV, status_audit_rows),
        "source_changes": write_csv(AUDIT_SOURCE_CHANGES_CSV, source_audit_rows),
        "validation": write_csv(AUDIT_VALIDATION_CSV, validation_audit_rows),
        "non_goals": write_csv(AUDIT_NON_GOALS_CSV, non_goal_audit_rows),
        "immutability": write_csv(AUDIT_IMMUTABILITY_CSV, immutability_audit_rows),
    }

    audit = {
        "layer": "6EM",
        "name": "candidate bullpen Statcast live adapter CLI live fetcher observability preflight runtime summary implementation plan audit",
        "audit_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": AUDIT_DIAGNOSIS if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "audited_plan_script": str(PLAN_SCRIPT),
        "plan_subprocess_returncode": plan_run.returncode,
        "plan_stdout_tail": plan_run.stdout[-1000:],
        "plan_stderr_tail": plan_run.stderr[-1000:],
        "expected_runtime_summary_fields": EXPECTED_RUNTIME_FIELDS,
        "expected_status_scenarios": EXPECTED_STATUS_SCENARIOS,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(AUDIT_JSON),
            "checks_csv": str(AUDIT_CHECKS_CSV),
            "artifacts_csv": str(AUDIT_ARTIFACTS_CSV),
            "field_contract_csv": str(AUDIT_FIELD_CONTRACT_CSV),
            "status_contract_csv": str(AUDIT_STATUS_CONTRACT_CSV),
            "source_changes_csv": str(AUDIT_SOURCE_CHANGES_CSV),
            "validation_csv": str(AUDIT_VALIDATION_CSV),
            "non_goals_csv": str(AUDIT_NON_GOALS_CSV),
            "immutability_csv": str(AUDIT_IMMUTABILITY_CSV),
        },
    }

    AUDIT_JSON.write_text(json.dumps(audit, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    print(json.dumps(audit, indent=2, sort_keys=True))
    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
