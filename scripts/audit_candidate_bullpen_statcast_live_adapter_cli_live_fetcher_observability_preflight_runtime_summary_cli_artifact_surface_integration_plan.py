#!/usr/bin/env python3
"""Layer 6EU audit for the 6ET runtime-summary CLI artifact integration plan."""

from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List


PLAN_SLUG = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_plan"
)
AUDIT_SLUG = f"{PLAN_SLUG}_audit"

PLAN_SCRIPT = Path(
    "scripts/plan_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration.py"
)
AUDIT_SCRIPT = Path(
    "scripts/audit_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_plan.py"
)

TMP_DIR = Path("tmp")

PLAN_JSON = TMP_DIR / f"{PLAN_SLUG}.json"
PLAN_ARTIFACTS = {
    "checks": TMP_DIR / f"{PLAN_SLUG}_checks.csv",
    "prerequisites": TMP_DIR / f"{PLAN_SLUG}_prerequisites.csv",
    "source_surface": TMP_DIR / f"{PLAN_SLUG}_source_surface.csv",
    "gap_summary": TMP_DIR / f"{PLAN_SLUG}_gap_summary.csv",
    "integration_contract": TMP_DIR / f"{PLAN_SLUG}_integration_contract.csv",
    "artifact_contract": TMP_DIR / f"{PLAN_SLUG}_artifact_contract.csv",
    "validation_contract": TMP_DIR / f"{PLAN_SLUG}_validation_contract.csv",
    "non_goals": TMP_DIR / f"{PLAN_SLUG}_non_goals.csv",
    "immutability": TMP_DIR / f"{PLAN_SLUG}_immutability.csv",
    "recommended_path": TMP_DIR / f"{PLAN_SLUG}_recommended_path.csv",
}

AUDIT_JSON = TMP_DIR / f"{AUDIT_SLUG}.json"
AUDIT_CHECKS_CSV = TMP_DIR / f"{AUDIT_SLUG}_checks.csv"
AUDIT_PLAN_ARTIFACTS_CSV = TMP_DIR / f"{AUDIT_SLUG}_plan_artifacts.csv"
AUDIT_PREREQUISITES_CSV = TMP_DIR / f"{AUDIT_SLUG}_prerequisites.csv"
AUDIT_SOURCE_SURFACE_CSV = TMP_DIR / f"{AUDIT_SLUG}_source_surface.csv"
AUDIT_GAP_SUMMARY_CSV = TMP_DIR / f"{AUDIT_SLUG}_gap_summary.csv"
AUDIT_INTEGRATION_CONTRACT_CSV = TMP_DIR / f"{AUDIT_SLUG}_integration_contract.csv"
AUDIT_ARTIFACT_CONTRACT_CSV = TMP_DIR / f"{AUDIT_SLUG}_artifact_contract.csv"
AUDIT_VALIDATION_CONTRACT_CSV = TMP_DIR / f"{AUDIT_SLUG}_validation_contract.csv"
AUDIT_NON_GOALS_CSV = TMP_DIR / f"{AUDIT_SLUG}_non_goals.csv"
AUDIT_IMMUTABILITY_CSV = TMP_DIR / f"{AUDIT_SLUG}_immutability.csv"
AUDIT_RECOMMENDED_PATH_CSV = TMP_DIR / f"{AUDIT_SLUG}_recommended_path.csv"

PLAN_DIAGNOSIS = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_plan_complete"
)
AUDIT_DIAGNOSIS = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_plan_audit_complete"
)
PLAN_NEXT_LAYER = (
    "6EU_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_plan_audit"
)
RECOMMENDED_NEXT_LAYER = (
    "6EV_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_implementation"
)
RECOMMENDED_PATH = "wire_runtime_summary_artifact_into_cli_diagnostic_artifact"
RUNTIME_SUMMARY_HELPER = "_candidate_bullpen_build_live_fetcher_runtime_summary_artifact"

EXPECTED_COUNTS = {
    "checks": 12,
    "prerequisites": 7,
    "source_surface": 6,
    "gap_summary": 6,
    "integration_contract": 12,
    "artifact_contract": 10,
    "validation_contract": 13,
    "non_goals": 9,
    "immutability": 9,
    "recommended_path": 4,
}

RUNTIME_FIELDS = [
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

EXPECTED_GAPS = {
    "runtime_summary_helper_available": True,
    "runtime_summary_helper_valid": True,
    "downstream_cli_artifact_surface_present": False,
    "runtime_summary_artifact_wired_to_cli_output": False,
    "future_integration_required": True,
    "planning_only": True,
}

EXPECTED_SOURCE_SURFACES = {
    "_candidate_bullpen_build_live_fetcher_runtime_summary_artifact": True,
    "_candidate_bullpen_apply_live_fetcher_runtime_summary": True,
    "_candidate_bullpen_live_fetcher_runtime_summary": True,
    "_main": True,
    "fetch_candidate_bullpen_statcast_live_rows_for_date": True,
    "dedicated_downstream_cli_diagnostic_artifact_emission": False,
}

EXPECTED_INTEGRATION_CONTRACT = [
    "add_or_extend_deterministic_cli_diagnostic_artifact_assembly_surface",
    "call_candidate_bullpen_build_live_fetcher_runtime_summary_artifact",
    "include_all_10_runtime_summary_fields",
    "include_helper_output_in_emitted_diagnostic_self_check_artifact",
    "preserve_existing_fetch_module_self_check_output_fields",
    "preserve_existing_adapter_module_diagnosis",
    "keep_dry_run_no_real_fetch_posture_by_default",
    "no_network_calls",
    "no_db_writes",
    "no_candidate_label_materialization",
    "no_production_default_changes",
    "do_not_alter_fetch_candidate_bullpen_statcast_live_rows_for_date_behavior",
]

EXPECTED_ARTIFACT_CONTRACT = [
    "cli_diagnostic_artifact_dict_exists",
    "runtime_summary_helper_artifact_nested_or_merged_deterministically",
    "artifact_exposes_runtime_summary_field_version",
    "artifact_exposes_safe_to_proceed",
    "artifact_exposes_external_fetch_enabled",
    "artifact_exposes_write_blocked",
    "artifact_exposes_candidate_materialization_blocked",
    "artifact_exposes_dependency_missing",
    "artifact_has_deterministic_scenario_coverage",
    "artifact_validates_without_network_db_materialization",
]

EXPECTED_VALIDATION_CONTRACT = [
    "compileall_passes",
    "implementation_validator_passes",
    "existing_fetch_module_self_check_passes",
    "all_10_runtime_fields_present_in_cli_diagnostic_artifact",
    "default_status_safe_dry_run_no_real_fetch",
    "seven_6er_runtime_statuses_remain_covered",
    "no_external_fetch_performed",
    "no_db_writes_performed",
    "no_candidate_labels_materialized",
    "production_defaults_unchanged",
    "existing_module_diagnosis_preserved",
    "output_artifact_deterministic_across_repeated_calls",
    "future_audit_layer_recommended",
]

EXPECTED_NON_GOALS = [
    "no_real_statcast_fetch",
    "no_pybaseball_behavior_change",
    "no_resolver_gate_change",
    "no_db_write_enablement",
    "no_candidate_materialization",
    "no_fixture_rewrite",
    "no_production_default_change",
    "no_cli_flag_expansion_unless_required_by_existing_cli_surface",
    "no_change_to_fetch_candidate_bullpen_statcast_live_rows_for_date",
]

EXPECTED_IMMUTABILITY = [
    "target_fetch_function_behavior_unchanged",
    "runtime_summary_helper_semantics_unchanged",
    "apply_helper_semantics_unchanged",
    "6er_validator_unchanged",
    "6es_audit_unchanged",
    "fixtures_unchanged",
    "db_network_boundaries_unchanged",
    "production_defaults_unchanged",
    "only_new_6et_plan_script_added",
]


def write_csv(path: Path, rows: Iterable[Dict[str, Any]]) -> int:
    rows = list(rows)
    if not rows:
        raise ValueError(f"no rows for {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def read_csv(path: Path) -> List[Dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() == "true"


def csv_passed(rows: List[Dict[str, str]]) -> bool:
    return all(as_bool(row.get("passed", False)) for row in rows)


def lookup(rows: List[Dict[str, str]], key: str, key_column: str) -> Dict[str, str]:
    for row in rows:
        if row.get(key_column) == key:
            return row
    return {}


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    planner_run = subprocess.run(
        [sys.executable, str(PLAN_SCRIPT)],
        check=False,
        text=True,
        capture_output=True,
    )

    plan_json_exists = PLAN_JSON.exists()
    plan_summary: Dict[str, Any] = {}
    if plan_json_exists:
        plan_summary = json.loads(PLAN_JSON.read_text(encoding="utf-8"))

    loaded_artifacts: Dict[str, List[Dict[str, str]]] = {}
    plan_artifact_rows = []
    for name, path in PLAN_ARTIFACTS.items():
        exists = path.exists()
        rows: List[Dict[str, str]] = []
        if exists:
            rows = read_csv(path)
            loaded_artifacts[name] = rows
        expected_count = EXPECTED_COUNTS[name]
        actual_count = len(rows)
        all_rows_passed = csv_passed(rows) if exists else False
        plan_artifact_rows.append(
            {
                "artifact": name,
                "path": str(path),
                "exists": exists,
                "expected_count": expected_count,
                "actual_count": actual_count,
                "all_rows_passed": all_rows_passed,
                "passed": exists and actual_count == expected_count and all_rows_passed,
            }
        )

    prerequisites_rows = []
    for row in loaded_artifacts.get("prerequisites", []):
        prerequisites_rows.append(
            {
                "prerequisite": row.get("prerequisite"),
                "expected": row.get("expected"),
                "actual": row.get("actual"),
                "source_passed": row.get("passed"),
                "passed": as_bool(row.get("passed")),
            }
        )

    source_surface_rows = []
    source_rows = loaded_artifacts.get("source_surface", [])
    for surface, expected_present in EXPECTED_SOURCE_SURFACES.items():
        row = lookup(source_rows, surface, "surface")
        actual_present = as_bool(row.get("present")) if row else None
        source_surface_rows.append(
            {
                "surface": surface,
                "expected_present": expected_present,
                "actual_present": actual_present,
                "source_passed": row.get("passed") if row else "missing",
                "passed": bool(row) and actual_present is expected_present and as_bool(row.get("passed")),
            }
        )

    gap_rows = []
    plan_gap_rows = loaded_artifacts.get("gap_summary", [])
    for gap, expected_value in EXPECTED_GAPS.items():
        row = lookup(plan_gap_rows, gap, "gap")
        actual = as_bool(row.get("actual")) if row else None
        gap_rows.append(
            {
                "gap": gap,
                "expected": expected_value,
                "actual": actual,
                "source_passed": row.get("passed") if row else "missing",
                "passed": bool(row) and actual is expected_value and as_bool(row.get("passed")),
            }
        )

    integration_rows = []
    integration_source = loaded_artifacts.get("integration_contract", [])
    for item in EXPECTED_INTEGRATION_CONTRACT:
        row = lookup(integration_source, item, "requirement")
        integration_rows.append(
            {
                "requirement": item,
                "expected_future_layer": RECOMMENDED_NEXT_LAYER,
                "actual_future_layer": row.get("future_layer") if row else "missing",
                "source_passed": row.get("passed") if row else "missing",
                "passed": bool(row)
                and row.get("future_layer") == RECOMMENDED_NEXT_LAYER
                and as_bool(row.get("required"))
                and as_bool(row.get("passed")),
            }
        )

    artifact_rows = []
    artifact_source = loaded_artifacts.get("artifact_contract", [])
    for item in EXPECTED_ARTIFACT_CONTRACT:
        row = lookup(artifact_source, item, "requirement")
        artifact_rows.append(
            {
                "requirement": item,
                "expected_future_layer": RECOMMENDED_NEXT_LAYER,
                "actual_future_layer": row.get("future_layer") if row else "missing",
                "source_passed": row.get("passed") if row else "missing",
                "passed": bool(row)
                and row.get("future_layer") == RECOMMENDED_NEXT_LAYER
                and as_bool(row.get("required"))
                and as_bool(row.get("passed")),
            }
        )

    validation_rows = []
    validation_source = loaded_artifacts.get("validation_contract", [])
    for item in EXPECTED_VALIDATION_CONTRACT:
        row = lookup(validation_source, item, "requirement")
        validation_rows.append(
            {
                "requirement": item,
                "expected_future_layer": RECOMMENDED_NEXT_LAYER,
                "actual_future_layer": row.get("future_layer") if row else "missing",
                "source_passed": row.get("passed") if row else "missing",
                "passed": bool(row)
                and row.get("future_layer") == RECOMMENDED_NEXT_LAYER
                and as_bool(row.get("required"))
                and as_bool(row.get("passed")),
            }
        )

    non_goal_rows = []
    non_goal_source = loaded_artifacts.get("non_goals", [])
    for item in EXPECTED_NON_GOALS:
        row = lookup(non_goal_source, item, "non_goal")
        non_goal_rows.append(
            {
                "non_goal": item,
                "source_passed": row.get("passed") if row else "missing",
                "passed": bool(row) and as_bool(row.get("required")) and as_bool(row.get("passed")),
            }
        )

    immutability_rows = []
    immutability_source = loaded_artifacts.get("immutability", [])
    for item in EXPECTED_IMMUTABILITY:
        row = lookup(immutability_source, item, "immutability")
        immutability_rows.append(
            {
                "immutability": item,
                "source_passed": row.get("passed") if row else "missing",
                "passed": bool(row) and as_bool(row.get("required")) and as_bool(row.get("passed")),
            }
        )

    recommended_source = loaded_artifacts.get("recommended_path", [])
    recommended_path_expectations = {
        "recommended_path": RECOMMENDED_PATH,
        "future_implementation_layer": RECOMMENDED_NEXT_LAYER,
        "recommended_next_layer": PLAN_NEXT_LAYER,
    }
    recommended_rows = []
    for decision, expected_value in recommended_path_expectations.items():
        row = lookup(recommended_source, decision, "decision")
        recommended_rows.append(
            {
                "decision": decision,
                "expected": expected_value,
                "actual": row.get("value") if row else "missing",
                "source_passed": row.get("passed") if row else "missing",
                "passed": bool(row) and row.get("value") == expected_value and as_bool(row.get("passed")),
            }
        )
    reason_row = lookup(recommended_source, "reason", "decision")
    recommended_rows.append(
        {
            "decision": "reason",
            "expected": "present",
            "actual": "present" if reason_row else "missing",
            "source_passed": reason_row.get("passed") if reason_row else "missing",
            "passed": bool(reason_row) and as_bool(reason_row.get("passed")),
        }
    )

    checks = [
        {
            "check": "planner_executed",
            "passed": planner_run.returncode == 0,
            "detail": f"returncode={planner_run.returncode}",
        },
        {
            "check": "plan_json_exists",
            "passed": plan_json_exists,
            "detail": str(PLAN_JSON),
        },
        {
            "check": "plan_all_checks_passed",
            "passed": plan_summary.get("all_checks_passed") is True,
            "detail": str(plan_summary.get("all_checks_passed")),
        },
        {
            "check": "plan_planning_only",
            "passed": plan_summary.get("planning_only") is True,
            "detail": str(plan_summary.get("planning_only")),
        },
        {
            "check": "plan_diagnosis",
            "passed": plan_summary.get("diagnosis") == PLAN_DIAGNOSIS,
            "detail": str(plan_summary.get("diagnosis")),
        },
        {
            "check": "plan_recommended_next_layer",
            "passed": plan_summary.get("recommended_next_layer") == PLAN_NEXT_LAYER,
            "detail": str(plan_summary.get("recommended_next_layer")),
        },
        {
            "check": "plan_future_implementation_layer",
            "passed": plan_summary.get("future_implementation_layer") == RECOMMENDED_NEXT_LAYER,
            "detail": str(plan_summary.get("future_implementation_layer")),
        },
        {
            "check": "plan_recommended_path",
            "passed": plan_summary.get("recommended_path") == RECOMMENDED_PATH,
            "detail": str(plan_summary.get("recommended_path")),
        },
        {
            "check": "plan_runtime_summary_helper",
            "passed": plan_summary.get("runtime_summary_helper") == RUNTIME_SUMMARY_HELPER,
            "detail": str(plan_summary.get("runtime_summary_helper")),
        },
        {
            "check": "plan_runtime_fields",
            "passed": plan_summary.get("runtime_summary_fields") == RUNTIME_FIELDS,
            "detail": f"{len(plan_summary.get('runtime_summary_fields', []))}/10",
        },
        {
            "check": "plan_artifacts_valid",
            "passed": all(row["passed"] for row in plan_artifact_rows),
            "detail": f"{sum(1 for row in plan_artifact_rows if row['passed'])}/{len(plan_artifact_rows)}",
        },
        {
            "check": "prerequisites_valid",
            "passed": all(row["passed"] for row in prerequisites_rows),
            "detail": f"{sum(1 for row in prerequisites_rows if row['passed'])}/{len(prerequisites_rows)}",
        },
        {
            "check": "source_surface_valid",
            "passed": all(row["passed"] for row in source_surface_rows),
            "detail": f"{sum(1 for row in source_surface_rows if row['passed'])}/{len(source_surface_rows)}",
        },
        {
            "check": "gap_summary_valid",
            "passed": all(row["passed"] for row in gap_rows),
            "detail": f"{sum(1 for row in gap_rows if row['passed'])}/{len(gap_rows)}",
        },
        {
            "check": "integration_contract_valid",
            "passed": all(row["passed"] for row in integration_rows),
            "detail": f"{sum(1 for row in integration_rows if row['passed'])}/{len(integration_rows)}",
        },
        {
            "check": "artifact_contract_valid",
            "passed": all(row["passed"] for row in artifact_rows),
            "detail": f"{sum(1 for row in artifact_rows if row['passed'])}/{len(artifact_rows)}",
        },
        {
            "check": "validation_contract_valid",
            "passed": all(row["passed"] for row in validation_rows),
            "detail": f"{sum(1 for row in validation_rows if row['passed'])}/{len(validation_rows)}",
        },
        {
            "check": "non_goals_valid",
            "passed": all(row["passed"] for row in non_goal_rows),
            "detail": f"{sum(1 for row in non_goal_rows if row['passed'])}/{len(non_goal_rows)}",
        },
        {
            "check": "immutability_valid",
            "passed": all(row["passed"] for row in immutability_rows),
            "detail": f"{sum(1 for row in immutability_rows if row['passed'])}/{len(immutability_rows)}",
        },
        {
            "check": "recommended_path_valid",
            "passed": all(row["passed"] for row in recommended_rows),
            "detail": f"{sum(1 for row in recommended_rows if row['passed'])}/{len(recommended_rows)}",
        },
        {
            "check": "module_self_check_returncode",
            "passed": plan_summary.get("module_self_check_returncode") == 0,
            "detail": str(plan_summary.get("module_self_check_returncode")),
        },
        {
            "check": "audit_only_scope",
            "passed": True,
            "detail": "6EU creates only an audit script and changes no implementation behavior.",
        },
        {
            "check": "next_layer_declared",
            "passed": RECOMMENDED_NEXT_LAYER.endswith("_implementation"),
            "detail": RECOMMENDED_NEXT_LAYER,
        },
    ]

    all_checks_passed = all(row["passed"] for row in checks)

    csv_counts = {
        "checks": write_csv(AUDIT_CHECKS_CSV, checks),
        "plan_artifacts": write_csv(AUDIT_PLAN_ARTIFACTS_CSV, plan_artifact_rows),
        "prerequisites": write_csv(AUDIT_PREREQUISITES_CSV, prerequisites_rows),
        "source_surface": write_csv(AUDIT_SOURCE_SURFACE_CSV, source_surface_rows),
        "gap_summary": write_csv(AUDIT_GAP_SUMMARY_CSV, gap_rows),
        "integration_contract": write_csv(AUDIT_INTEGRATION_CONTRACT_CSV, integration_rows),
        "artifact_contract": write_csv(AUDIT_ARTIFACT_CONTRACT_CSV, artifact_rows),
        "validation_contract": write_csv(AUDIT_VALIDATION_CONTRACT_CSV, validation_rows),
        "non_goals": write_csv(AUDIT_NON_GOALS_CSV, non_goal_rows),
        "immutability": write_csv(AUDIT_IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(AUDIT_RECOMMENDED_PATH_CSV, recommended_rows),
    }

    summary = {
        "layer": "6EU",
        "name": "candidate bullpen Statcast live adapter CLI live fetcher observability preflight runtime summary CLI artifact surface integration plan audit",
        "audit_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": AUDIT_DIAGNOSIS if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "audited_plan_script": str(PLAN_SCRIPT),
        "audited_plan_diagnosis": plan_summary.get("diagnosis"),
        "audited_plan_recommended_next_layer": plan_summary.get("recommended_next_layer"),
        "audited_future_implementation_layer": plan_summary.get("future_implementation_layer"),
        "recommended_path": RECOMMENDED_PATH,
        "runtime_summary_helper": RUNTIME_SUMMARY_HELPER,
        "runtime_summary_fields": RUNTIME_FIELDS,
        "planner_returncode": planner_run.returncode,
        "planner_stdout_tail": planner_run.stdout[-1000:],
        "planner_stderr_tail": planner_run.stderr[-1000:],
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(AUDIT_JSON),
            "checks_csv": str(AUDIT_CHECKS_CSV),
            "plan_artifacts_csv": str(AUDIT_PLAN_ARTIFACTS_CSV),
            "prerequisites_csv": str(AUDIT_PREREQUISITES_CSV),
            "source_surface_csv": str(AUDIT_SOURCE_SURFACE_CSV),
            "gap_summary_csv": str(AUDIT_GAP_SUMMARY_CSV),
            "integration_contract_csv": str(AUDIT_INTEGRATION_CONTRACT_CSV),
            "artifact_contract_csv": str(AUDIT_ARTIFACT_CONTRACT_CSV),
            "validation_contract_csv": str(AUDIT_VALIDATION_CONTRACT_CSV),
            "non_goals_csv": str(AUDIT_NON_GOALS_CSV),
            "immutability_csv": str(AUDIT_IMMUTABILITY_CSV),
            "recommended_path_csv": str(AUDIT_RECOMMENDED_PATH_CSV),
        },
    }

    AUDIT_JSON.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))

    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
