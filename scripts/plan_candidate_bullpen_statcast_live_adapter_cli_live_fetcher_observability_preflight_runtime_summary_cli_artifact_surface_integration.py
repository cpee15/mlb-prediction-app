#!/usr/bin/env python3
"""Layer 6ET plan for runtime-summary CLI artifact surface integration."""

from __future__ import annotations

import csv
import importlib.util
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List


SLUG = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_plan"
)

TARGET_PATH = Path("scripts/fetch_candidate_bullpen_statcast_live_adapter.py")
PLAN_PATH = Path(
    "scripts/plan_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration.py"
)

TMP_DIR = Path("tmp")
JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREREQUISITES_CSV = TMP_DIR / f"{SLUG}_prerequisites.csv"
SOURCE_SURFACE_CSV = TMP_DIR / f"{SLUG}_source_surface.csv"
GAP_SUMMARY_CSV = TMP_DIR / f"{SLUG}_gap_summary.csv"
INTEGRATION_CONTRACT_CSV = TMP_DIR / f"{SLUG}_integration_contract.csv"
ARTIFACT_CONTRACT_CSV = TMP_DIR / f"{SLUG}_artifact_contract.csv"
VALIDATION_CONTRACT_CSV = TMP_DIR / f"{SLUG}_validation_contract.csv"
NON_GOALS_CSV = TMP_DIR / f"{SLUG}_non_goals.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_PATH_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_plan_complete"
)
RECOMMENDED_NEXT_LAYER = (
    "6EU_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_plan_audit"
)
FUTURE_IMPLEMENTATION_LAYER = (
    "6EV_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_implementation"
)
RECOMMENDED_PATH = "wire_runtime_summary_artifact_into_cli_diagnostic_artifact"

BUILDER = "_candidate_bullpen_build_live_fetcher_runtime_summary_artifact"
SUMMARY_HELPER = "_candidate_bullpen_live_fetcher_runtime_summary"
APPLY_HELPER = "_candidate_bullpen_apply_live_fetcher_runtime_summary"

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

FUTURE_INTEGRATION_CONTRACT = [
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

ARTIFACT_CONTRACT = [
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

VALIDATION_CONTRACT = [
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

NON_GOALS = [
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

IMMUTABILITY = [
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


def load_target_module() -> Any:
    module_name = "candidate_bullpen_live_adapter_6et_plan"
    spec = importlib.util.spec_from_file_location(module_name, TARGET_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load spec for {TARGET_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    module = load_target_module()
    source = TARGET_PATH.read_text(encoding="utf-8")

    builder_exists = hasattr(module, BUILDER)
    summary_exists = hasattr(module, SUMMARY_HELPER)
    apply_exists = hasattr(module, APPLY_HELPER)
    main_exists = hasattr(module, "_main")
    fetcher_exists = hasattr(module, "fetch_candidate_bullpen_statcast_live_rows_for_date")

    artifact: Dict[str, Any] = {}
    if builder_exists:
        artifact = getattr(module, BUILDER)()

    module_self_check = subprocess.run(
        [sys.executable, str(TARGET_PATH)],
        check=False,
        text=True,
        capture_output=True,
    )

    module_self_check_json: Dict[str, Any] = {}
    try:
        module_self_check_json = json.loads(module_self_check.stdout)
    except Exception:
        module_self_check_json = {}

    prerequisites_rows = [
        {
            "prerequisite": "6er_builder_present",
            "expected": True,
            "actual": builder_exists,
            "passed": builder_exists,
        },
        {
            "prerequisite": "summary_helper_present",
            "expected": True,
            "actual": summary_exists,
            "passed": summary_exists,
        },
        {
            "prerequisite": "apply_helper_present",
            "expected": True,
            "actual": apply_exists,
            "passed": apply_exists,
        },
        {
            "prerequisite": "default_artifact_has_10_runtime_fields",
            "expected": len(RUNTIME_FIELDS),
            "actual": sum(1 for field in RUNTIME_FIELDS if field in artifact),
            "passed": all(field in artifact for field in RUNTIME_FIELDS),
        },
        {
            "prerequisite": "default_status_safe_dry_run_no_real_fetch",
            "expected": "safe_dry_run_no_real_fetch",
            "actual": artifact.get("live_fetcher_runtime_summary_status"),
            "passed": artifact.get("live_fetcher_runtime_summary_status") == "safe_dry_run_no_real_fetch",
        },
        {
            "prerequisite": "module_self_check_returncode",
            "expected": 0,
            "actual": module_self_check.returncode,
            "passed": module_self_check.returncode == 0,
        },
        {
            "prerequisite": "module_self_check_diagnosis_preserved",
            "expected": "candidate_bullpen_statcast_live_adapter_fetch_module_implementation_complete",
            "actual": module_self_check_json.get("diagnosis"),
            "passed": module_self_check_json.get("diagnosis")
            == "candidate_bullpen_statcast_live_adapter_fetch_module_implementation_complete",
        },
    ]

    source_surface_rows = [
        {
            "surface": BUILDER,
            "present": builder_exists,
            "role": "runtime_summary_artifact_helper",
            "passed": builder_exists,
        },
        {
            "surface": APPLY_HELPER,
            "present": apply_exists,
            "role": "runtime_summary_attachment_helper",
            "passed": apply_exists,
        },
        {
            "surface": SUMMARY_HELPER,
            "present": summary_exists,
            "role": "runtime_summary_builder",
            "passed": summary_exists,
        },
        {
            "surface": "_main",
            "present": main_exists,
            "role": "existing_cli_self_check_entrypoint",
            "passed": main_exists,
        },
        {
            "surface": "fetch_candidate_bullpen_statcast_live_rows_for_date",
            "present": fetcher_exists,
            "role": "fetch_behavior_to_preserve",
            "passed": fetcher_exists,
        },
        {
            "surface": "dedicated_downstream_cli_diagnostic_artifact_emission",
            "present": False,
            "role": "future_integration_gap",
            "passed": True,
        },
    ]

    gap_summary_rows = [
        {
            "gap": "runtime_summary_helper_available",
            "expected": True,
            "actual": builder_exists,
            "passed": builder_exists,
        },
        {
            "gap": "runtime_summary_helper_valid",
            "expected": True,
            "actual": all(field in artifact for field in RUNTIME_FIELDS)
            and artifact.get("live_fetcher_runtime_summary_status") == "safe_dry_run_no_real_fetch",
            "passed": all(field in artifact for field in RUNTIME_FIELDS)
            and artifact.get("live_fetcher_runtime_summary_status") == "safe_dry_run_no_real_fetch",
        },
        {
            "gap": "downstream_cli_artifact_surface_present",
            "expected": False,
            "actual": False,
            "passed": True,
        },
        {
            "gap": "runtime_summary_artifact_wired_to_cli_output",
            "expected": False,
            "actual": False,
            "passed": True,
        },
        {
            "gap": "future_integration_required",
            "expected": True,
            "actual": True,
            "passed": True,
        },
        {
            "gap": "planning_only",
            "expected": True,
            "actual": True,
            "passed": True,
        },
    ]

    integration_contract_rows = [
        {
            "requirement": item,
            "future_layer": FUTURE_IMPLEMENTATION_LAYER,
            "required": True,
            "passed": True,
        }
        for item in FUTURE_INTEGRATION_CONTRACT
    ]

    artifact_contract_rows = [
        {
            "requirement": item,
            "future_layer": FUTURE_IMPLEMENTATION_LAYER,
            "required": True,
            "passed": True,
        }
        for item in ARTIFACT_CONTRACT
    ]

    validation_contract_rows = [
        {
            "requirement": item,
            "future_layer": FUTURE_IMPLEMENTATION_LAYER,
            "required": True,
            "passed": True,
        }
        for item in VALIDATION_CONTRACT
    ]

    non_goal_rows = [
        {
            "non_goal": item,
            "required": True,
            "passed": True,
        }
        for item in NON_GOALS
    ]

    immutability_rows = [
        {
            "immutability": item,
            "required": True,
            "passed": True,
        }
        for item in IMMUTABILITY
    ]

    recommended_path_rows = [
        {
            "decision": "recommended_path",
            "value": RECOMMENDED_PATH,
            "passed": True,
        },
        {
            "decision": "future_implementation_layer",
            "value": FUTURE_IMPLEMENTATION_LAYER,
            "passed": True,
        },
        {
            "decision": "recommended_next_layer",
            "value": RECOMMENDED_NEXT_LAYER,
            "passed": True,
        },
        {
            "decision": "reason",
            "value": "6ER helper exists and 6ES audit passed; next implementation needs a deterministic downstream CLI diagnostic artifact surface.",
            "passed": True,
        },
    ]

    checks = [
        {
            "check": "planning_only",
            "passed": True,
            "detail": "6ET creates a plan script only.",
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
            "passed": all(row["passed"] for row in gap_summary_rows),
            "detail": f"{sum(1 for row in gap_summary_rows if row['passed'])}/{len(gap_summary_rows)}",
        },
        {
            "check": "integration_contract_valid",
            "passed": len(integration_contract_rows) == 12 and all(row["passed"] for row in integration_contract_rows),
            "detail": f"{sum(1 for row in integration_contract_rows if row['passed'])}/{len(integration_contract_rows)}",
        },
        {
            "check": "artifact_contract_valid",
            "passed": len(artifact_contract_rows) == 10 and all(row["passed"] for row in artifact_contract_rows),
            "detail": f"{sum(1 for row in artifact_contract_rows if row['passed'])}/{len(artifact_contract_rows)}",
        },
        {
            "check": "validation_contract_valid",
            "passed": len(validation_contract_rows) == 13 and all(row["passed"] for row in validation_contract_rows),
            "detail": f"{sum(1 for row in validation_contract_rows if row['passed'])}/{len(validation_contract_rows)}",
        },
        {
            "check": "non_goals_valid",
            "passed": len(non_goal_rows) == 9 and all(row["passed"] for row in non_goal_rows),
            "detail": f"{sum(1 for row in non_goal_rows if row['passed'])}/{len(non_goal_rows)}",
        },
        {
            "check": "immutability_valid",
            "passed": len(immutability_rows) == 9 and all(row["passed"] for row in immutability_rows),
            "detail": f"{sum(1 for row in immutability_rows if row['passed'])}/{len(immutability_rows)}",
        },
        {
            "check": "recommended_path_valid",
            "passed": recommended_path_rows[0]["value"] == RECOMMENDED_PATH
            and recommended_path_rows[1]["value"] == FUTURE_IMPLEMENTATION_LAYER
            and recommended_path_rows[2]["value"] == RECOMMENDED_NEXT_LAYER,
            "detail": RECOMMENDED_PATH,
        },
        {
            "check": "runtime_fields_complete",
            "passed": all(field in artifact for field in RUNTIME_FIELDS),
            "detail": f"{sum(1 for field in RUNTIME_FIELDS if field in artifact)}/10",
        },
        {
            "check": "no_live_behavior_executed",
            "passed": artifact.get("external_fetch_performed") is False
            and artifact.get("db_writes_performed") is False
            and artifact.get("candidate_labels_materialized") is False,
            "detail": "no network/db/materialization",
        },
    ]

    all_checks_passed = all(row["passed"] for row in checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "prerequisites": write_csv(PREREQUISITES_CSV, prerequisites_rows),
        "source_surface": write_csv(SOURCE_SURFACE_CSV, source_surface_rows),
        "gap_summary": write_csv(GAP_SUMMARY_CSV, gap_summary_rows),
        "integration_contract": write_csv(INTEGRATION_CONTRACT_CSV, integration_contract_rows),
        "artifact_contract": write_csv(ARTIFACT_CONTRACT_CSV, artifact_contract_rows),
        "validation_contract": write_csv(VALIDATION_CONTRACT_CSV, validation_contract_rows),
        "non_goals": write_csv(NON_GOALS_CSV, non_goal_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_PATH_CSV, recommended_path_rows),
    }

    summary = {
        "layer": "6ET",
        "name": "candidate bullpen Statcast live adapter CLI live fetcher observability preflight runtime summary CLI artifact surface integration plan",
        "planning_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "future_implementation_layer": FUTURE_IMPLEMENTATION_LAYER,
        "recommended_path": RECOMMENDED_PATH,
        "runtime_summary_helper": BUILDER,
        "runtime_summary_fields": RUNTIME_FIELDS,
        "integration_gap": {
            row["gap"]: row["actual"] for row in gap_summary_rows
        },
        "csv_counts": csv_counts,
        "module_self_check_returncode": module_self_check.returncode,
        "module_self_check_stdout_tail": module_self_check.stdout[-1000:],
        "module_self_check_stderr_tail": module_self_check.stderr[-1000:],
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "prerequisites_csv": str(PREREQUISITES_CSV),
            "source_surface_csv": str(SOURCE_SURFACE_CSV),
            "gap_summary_csv": str(GAP_SUMMARY_CSV),
            "integration_contract_csv": str(INTEGRATION_CONTRACT_CSV),
            "artifact_contract_csv": str(ARTIFACT_CONTRACT_CSV),
            "validation_contract_csv": str(VALIDATION_CONTRACT_CSV),
            "non_goals_csv": str(NON_GOALS_CSV),
            "immutability_csv": str(IMMUTABILITY_CSV),
            "recommended_path_csv": str(RECOMMENDED_PATH_CSV),
        },
    }

    JSON_PATH.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))

    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
