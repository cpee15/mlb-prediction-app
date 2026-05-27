#!/usr/bin/env python3
"""Layer 6EQ audit for the 6EP integration-gap resolution plan."""

from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List


PLAN_SLUG = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_integration_gap_resolution_plan"
)
AUDIT_SLUG = f"{PLAN_SLUG}_audit"

TMP_DIR = Path("tmp")

PLAN_SCRIPT = Path(
    "scripts/plan_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_integration_gap_resolution.py"
)
AUDIT_SCRIPT = Path(
    "scripts/audit_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_integration_gap_resolution_plan.py"
)

PLAN_JSON = TMP_DIR / f"{PLAN_SLUG}.json"
PLAN_CHECKS_CSV = TMP_DIR / f"{PLAN_SLUG}_checks.csv"
PLAN_GAP_SUMMARY_CSV = TMP_DIR / f"{PLAN_SLUG}_gap_summary.csv"
PLAN_SOURCE_SURFACE_CSV = TMP_DIR / f"{PLAN_SLUG}_source_surface.csv"
PLAN_RESOLUTION_OPTIONS_CSV = TMP_DIR / f"{PLAN_SLUG}_resolution_options.csv"
PLAN_RECOMMENDED_PATH_CSV = TMP_DIR / f"{PLAN_SLUG}_recommended_path.csv"
PLAN_FUTURE_IMPLEMENTATION_CONTRACT_CSV = TMP_DIR / f"{PLAN_SLUG}_future_implementation_contract.csv"
PLAN_VALIDATION_CONTRACT_CSV = TMP_DIR / f"{PLAN_SLUG}_validation_contract.csv"
PLAN_NON_GOALS_CSV = TMP_DIR / f"{PLAN_SLUG}_non_goals.csv"
PLAN_IMMUTABILITY_CSV = TMP_DIR / f"{PLAN_SLUG}_immutability.csv"

AUDIT_JSON = TMP_DIR / f"{AUDIT_SLUG}.json"
AUDIT_CHECKS_CSV = TMP_DIR / f"{AUDIT_SLUG}_checks.csv"
AUDIT_PLAN_ARTIFACTS_CSV = TMP_DIR / f"{AUDIT_SLUG}_plan_artifacts.csv"
AUDIT_GAP_SUMMARY_CSV = TMP_DIR / f"{AUDIT_SLUG}_gap_summary.csv"
AUDIT_SOURCE_SURFACE_CSV = TMP_DIR / f"{AUDIT_SLUG}_source_surface.csv"
AUDIT_RESOLUTION_OPTIONS_CSV = TMP_DIR / f"{AUDIT_SLUG}_resolution_options.csv"
AUDIT_RECOMMENDED_PATH_CSV = TMP_DIR / f"{AUDIT_SLUG}_recommended_path.csv"
AUDIT_FUTURE_IMPLEMENTATION_CONTRACT_CSV = TMP_DIR / f"{AUDIT_SLUG}_future_implementation_contract.csv"
AUDIT_VALIDATION_CONTRACT_CSV = TMP_DIR / f"{AUDIT_SLUG}_validation_contract.csv"
AUDIT_NON_GOALS_CSV = TMP_DIR / f"{AUDIT_SLUG}_non_goals.csv"
AUDIT_IMMUTABILITY_CSV = TMP_DIR / f"{AUDIT_SLUG}_immutability.csv"

EXPECTED_PLAN_DIAGNOSIS = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_integration_gap_resolution_plan_complete"
)
EXPECTED_PLAN_NEXT_LAYER = (
    "6EQ_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_integration_gap_resolution_plan_audit"
)
AUDIT_DIAGNOSIS = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_integration_gap_resolution_plan_audit_complete"
)
RECOMMENDED_NEXT_LAYER = (
    "6ER_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_minimal_cli_artifact_summary_surface_implementation"
)

EXPECTED_RECOMMENDED_PATH = "create_minimal_cli_artifact_summary_surface"
EXPECTED_FUTURE_HELPER = "_candidate_bullpen_build_live_fetcher_runtime_summary_artifact"

EXPECTED_PLAN_COUNTS = {
    "checks": 14,
    "gap_summary": 6,
    "source_surface": 5,
    "resolution_options": 3,
    "recommended_path": 4,
    "future_implementation_contract": 9,
    "validation_contract": 13,
    "non_goals": 9,
    "immutability": 10,
}

EXPECTED_GAP_FACTS = {
    "implementation_helper_valid": "True",
    "direct_validation_valid": "True",
    "cli_artifact_assembly_surface_present": "False",
    "cli_artifact_runtime_summary_wired": "False",
    "final_cli_artifact_integration_complete": "False",
    "follow_up_integration_layer_required": "True",
}

EXPECTED_SOURCE_SURFACES = {
    "_candidate_bullpen_live_fetcher_runtime_summary",
    "_candidate_bullpen_apply_live_fetcher_runtime_summary",
    "_main",
    "main",
    "final_downstream_cli_artifact_assembly",
}

EXPECTED_RESOLUTION_OPTIONS = {
    "wait_for_cli_surface",
    "create_minimal_cli_artifact_summary_surface",
    "integrate_into_existing_module_self_check_artifact",
}

EXPECTED_RECOMMENDED_DECISIONS = {
    "recommended_path",
    "requires_deterministic_artifact_surface",
    "must_call_existing_apply_helper",
    "must_preserve_fetcher_semantics",
}

EXPECTED_FUTURE_REQUIREMENTS = {
    "add_deterministic_artifact_assembly_helper",
    "helper_calls_runtime_summary_apply_helper",
    "include_module_self_check_safety_signals",
    "include_all_10_runtime_summary_fields",
    "no_network",
    "no_db_writes",
    "no_materialization",
    "production_default_unchanged",
    "fetcher_behavior_unchanged",
}

EXPECTED_VALIDATIONS = {
    "compile",
    "run_new_implementation_validation_script",
    "default_dry_run_artifact_10_fields",
    "synthetic_scenario",
    "dependency_missing_scenario",
    "blocked_no_dry_run_scenario",
    "blocked_write_scenario",
    "blocked_invalid_date_window_scenario",
    "no_network",
    "no_db_writes",
    "no_materialization",
    "production_default_unchanged",
    "existing_module_self_check_still_passes",
}


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


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


def boolish(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() == "true"


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

    plan_artifact_map = {
        "checks": PLAN_CHECKS_CSV,
        "gap_summary": PLAN_GAP_SUMMARY_CSV,
        "source_surface": PLAN_SOURCE_SURFACE_CSV,
        "resolution_options": PLAN_RESOLUTION_OPTIONS_CSV,
        "recommended_path": PLAN_RECOMMENDED_PATH_CSV,
        "future_implementation_contract": PLAN_FUTURE_IMPLEMENTATION_CONTRACT_CSV,
        "validation_contract": PLAN_VALIDATION_CONTRACT_CSV,
        "non_goals": PLAN_NON_GOALS_CSV,
        "immutability": PLAN_IMMUTABILITY_CSV,
    }

    loaded: Dict[str, List[Dict[str, str]]] = {}
    plan_artifact_rows = []
    for name, path in plan_artifact_map.items():
        exists = path.exists()
        rows: List[Dict[str, str]] = []
        if exists:
            rows = read_csv_rows(path)
            loaded[name] = rows
        actual_count = len(rows) if exists else 0
        expected_count = EXPECTED_PLAN_COUNTS[name]
        plan_artifact_rows.append(
            {
                "artifact": name,
                "path": str(path),
                "exists": exists,
                "actual_count": actual_count,
                "expected_count": expected_count,
                "passed": exists and actual_count == expected_count,
            }
        )

    gap_rows = loaded.get("gap_summary", [])
    gap_audit_rows = []
    for fact, expected in EXPECTED_GAP_FACTS.items():
        matching = [row for row in gap_rows if row.get("fact") == fact]
        actual = matching[0].get("actual", "") if matching else ""
        valid = matching[0].get("valid", "") if matching else ""
        gap_audit_rows.append(
            {
                "fact": fact,
                "expected_actual": expected,
                "actual": actual,
                "plan_row_valid": valid,
                "passed": actual == expected and boolish(valid),
            }
        )

    source_surface_rows = loaded.get("source_surface", [])
    source_surface_audit_rows = []
    for surface in sorted(EXPECTED_SOURCE_SURFACES):
        matching = [row for row in source_surface_rows if row.get("surface") == surface]
        valid = matching[0].get("valid", "") if matching else ""
        source_surface_audit_rows.append(
            {
                "surface": surface,
                "present": matching[0].get("present", "") if matching else "",
                "planning_interpretation": matching[0].get("planning_interpretation", "") if matching else "",
                "plan_row_valid": valid,
                "passed": bool(matching) and boolish(valid),
            }
        )

    resolution_rows = loaded.get("resolution_options", [])
    resolution_audit_rows = []
    for option in sorted(EXPECTED_RESOLUTION_OPTIONS):
        matching = [row for row in resolution_rows if row.get("option") == option]
        resolution_audit_rows.append(
            {
                "option": option,
                "present": bool(matching),
                "recommended": matching[0].get("recommended", "") if matching else "",
                "passed": bool(matching),
            }
        )
    recommended_count = sum(1 for row in resolution_rows if boolish(row.get("recommended")))
    recommended_option_valid = any(
        row.get("option") == EXPECTED_RECOMMENDED_PATH and boolish(row.get("recommended"))
        for row in resolution_rows
    )
    resolution_audit_rows.append(
        {
            "option": "recommended_option_cardinality",
            "present": True,
            "recommended": recommended_count,
            "passed": recommended_count == 1 and recommended_option_valid,
        }
    )

    recommended_rows = loaded.get("recommended_path", [])
    recommended_audit_rows = []
    for decision in sorted(EXPECTED_RECOMMENDED_DECISIONS):
        matching = [row for row in recommended_rows if row.get("decision") == decision]
        valid = matching[0].get("valid", "") if matching else ""
        recommended_audit_rows.append(
            {
                "decision": decision,
                "value": matching[0].get("value", "") if matching else "",
                "plan_row_valid": valid,
                "passed": bool(matching) and boolish(valid),
            }
        )

    future_rows = loaded.get("future_implementation_contract", [])
    future_audit_rows = []
    for requirement in sorted(EXPECTED_FUTURE_REQUIREMENTS):
        matching = [row for row in future_rows if row.get("requirement") == requirement]
        required = matching[0].get("required", "") if matching else ""
        future_audit_rows.append(
            {
                "requirement": requirement,
                "present": bool(matching),
                "required": required,
                "passed": bool(matching) and boolish(required),
            }
        )

    validation_rows = loaded.get("validation_contract", [])
    validation_audit_rows = []
    for validation in sorted(EXPECTED_VALIDATIONS):
        matching = [row for row in validation_rows if row.get("validation") == validation]
        required = matching[0].get("required", "") if matching else ""
        validation_audit_rows.append(
            {
                "validation": validation,
                "present": bool(matching),
                "required": required,
                "passed": bool(matching) and boolish(required),
            }
        )

    non_goal_rows = loaded.get("non_goals", [])
    non_goal_audit_rows = []
    for row in non_goal_rows:
        non_goal_audit_rows.append(
            {
                "non_goal": row.get("non_goal", ""),
                "valid": row.get("valid", ""),
                "passed": boolish(row.get("valid")),
            }
        )

    immutability_rows = loaded.get("immutability", [])
    immutability_audit_rows = []
    for row in immutability_rows:
        immutability_audit_rows.append(
            {
                "path": row.get("path", ""),
                "policy": row.get("policy", ""),
                "valid": row.get("valid", ""),
                "passed": boolish(row.get("valid")),
            }
        )

    checks = [
        {
            "check": "six_ep_plan_executed",
            "passed": plan_run.returncode == 0,
            "detail": f"returncode={plan_run.returncode}",
        },
        {
            "check": "six_ep_plan_json_exists",
            "passed": plan_json_exists,
            "detail": str(PLAN_JSON),
        },
        {
            "check": "six_ep_diagnosis",
            "passed": plan.get("diagnosis") == EXPECTED_PLAN_DIAGNOSIS,
            "detail": str(plan.get("diagnosis")),
        },
        {
            "check": "six_ep_all_checks_passed",
            "passed": plan.get("all_checks_passed") is True,
            "detail": str(plan.get("all_checks_passed")),
        },
        {
            "check": "six_ep_planning_only",
            "passed": plan.get("planning_only") is True,
            "detail": str(plan.get("planning_only")),
        },
        {
            "check": "six_ep_recommended_next_layer",
            "passed": plan.get("recommended_next_layer") == EXPECTED_PLAN_NEXT_LAYER,
            "detail": str(plan.get("recommended_next_layer")),
        },
        {
            "check": "six_ep_recommended_resolution_path",
            "passed": plan.get("recommended_resolution_path") == EXPECTED_RECOMMENDED_PATH,
            "detail": str(plan.get("recommended_resolution_path")),
        },
        {
            "check": "six_ep_future_artifact_helper",
            "passed": plan.get("future_artifact_helper") == EXPECTED_FUTURE_HELPER,
            "detail": str(plan.get("future_artifact_helper")),
        },
        {
            "check": "plan_artifacts_valid",
            "passed": all(row["passed"] for row in plan_artifact_rows),
            "detail": f"{sum(1 for row in plan_artifact_rows if row['passed'])}/{len(plan_artifact_rows)}",
        },
        {
            "check": "gap_summary_valid",
            "passed": len(gap_audit_rows) == 6 and all(row["passed"] for row in gap_audit_rows),
            "detail": f"{sum(1 for row in gap_audit_rows if row['passed'])}/6",
        },
        {
            "check": "source_surface_valid",
            "passed": len(source_surface_audit_rows) == 5 and all(row["passed"] for row in source_surface_audit_rows),
            "detail": f"{sum(1 for row in source_surface_audit_rows if row['passed'])}/5",
        },
        {
            "check": "resolution_options_valid",
            "passed": len(resolution_audit_rows) == 4 and all(row["passed"] for row in resolution_audit_rows),
            "detail": f"{sum(1 for row in resolution_audit_rows if row['passed'])}/4",
        },
        {
            "check": "recommended_path_valid",
            "passed": len(recommended_audit_rows) == 4 and all(row["passed"] for row in recommended_audit_rows),
            "detail": f"{sum(1 for row in recommended_audit_rows if row['passed'])}/4",
        },
        {
            "check": "future_implementation_contract_valid",
            "passed": len(future_audit_rows) == 9 and all(row["passed"] for row in future_audit_rows),
            "detail": f"{sum(1 for row in future_audit_rows if row['passed'])}/9",
        },
        {
            "check": "validation_contract_valid",
            "passed": len(validation_audit_rows) == 13 and all(row["passed"] for row in validation_audit_rows),
            "detail": f"{sum(1 for row in validation_audit_rows if row['passed'])}/13",
        },
        {
            "check": "non_goals_valid",
            "passed": len(non_goal_audit_rows) == 9 and all(row["passed"] for row in non_goal_audit_rows),
            "detail": f"{sum(1 for row in non_goal_audit_rows if row['passed'])}/9",
        },
        {
            "check": "immutability_valid",
            "passed": len(immutability_audit_rows) == 10 and all(row["passed"] for row in immutability_audit_rows),
            "detail": f"{sum(1 for row in immutability_audit_rows if row['passed'])}/10",
        },
        {
            "check": "audit_only_scope",
            "passed": True,
            "detail": "6EQ audits the 6EP plan only and implements no integration behavior.",
        },
    ]

    all_checks_passed = all(row["passed"] for row in checks)

    csv_counts = {
        "checks": write_csv(AUDIT_CHECKS_CSV, checks),
        "plan_artifacts": write_csv(AUDIT_PLAN_ARTIFACTS_CSV, plan_artifact_rows),
        "gap_summary": write_csv(AUDIT_GAP_SUMMARY_CSV, gap_audit_rows),
        "source_surface": write_csv(AUDIT_SOURCE_SURFACE_CSV, source_surface_audit_rows),
        "resolution_options": write_csv(AUDIT_RESOLUTION_OPTIONS_CSV, resolution_audit_rows),
        "recommended_path": write_csv(AUDIT_RECOMMENDED_PATH_CSV, recommended_audit_rows),
        "future_implementation_contract": write_csv(AUDIT_FUTURE_IMPLEMENTATION_CONTRACT_CSV, future_audit_rows),
        "validation_contract": write_csv(AUDIT_VALIDATION_CONTRACT_CSV, validation_audit_rows),
        "non_goals": write_csv(AUDIT_NON_GOALS_CSV, non_goal_audit_rows),
        "immutability": write_csv(AUDIT_IMMUTABILITY_CSV, immutability_audit_rows),
    }

    summary = {
        "layer": "6EQ",
        "name": "candidate bullpen Statcast live adapter CLI live fetcher observability preflight runtime summary integration gap resolution plan audit",
        "audit_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": AUDIT_DIAGNOSIS if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "audited_plan_script": str(PLAN_SCRIPT),
        "plan_subprocess_returncode": plan_run.returncode,
        "plan_stdout_tail": plan_run.stdout[-1000:],
        "plan_stderr_tail": plan_run.stderr[-1000:],
        "recommended_resolution_path": plan.get("recommended_resolution_path"),
        "future_artifact_helper": plan.get("future_artifact_helper"),
        "planning_only_confirmed": plan.get("planning_only") is True,
        "integration_gap": plan.get("integration_gap", {}),
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(AUDIT_JSON),
            "checks_csv": str(AUDIT_CHECKS_CSV),
            "plan_artifacts_csv": str(AUDIT_PLAN_ARTIFACTS_CSV),
            "gap_summary_csv": str(AUDIT_GAP_SUMMARY_CSV),
            "source_surface_csv": str(AUDIT_SOURCE_SURFACE_CSV),
            "resolution_options_csv": str(AUDIT_RESOLUTION_OPTIONS_CSV),
            "recommended_path_csv": str(AUDIT_RECOMMENDED_PATH_CSV),
            "future_implementation_contract_csv": str(AUDIT_FUTURE_IMPLEMENTATION_CONTRACT_CSV),
            "validation_contract_csv": str(AUDIT_VALIDATION_CONTRACT_CSV),
            "non_goals_csv": str(AUDIT_NON_GOALS_CSV),
            "immutability_csv": str(AUDIT_IMMUTABILITY_CSV),
        },
    }

    AUDIT_JSON.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))

    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
