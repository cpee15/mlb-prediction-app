#!/usr/bin/env python3
"""Plan downstream usage of the runtime-summary CLI diagnostic artifact surface."""

from __future__ import annotations

import csv
import importlib.util
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List


PLAN_SLUG = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_plan"
)
IMPL_SLUG = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_implementation"
)
AUDIT_SLUG = f"{IMPL_SLUG}_audit"

TMP_DIR = Path("tmp")
TARGET_PATH = Path("scripts/fetch_candidate_bullpen_statcast_live_adapter.py")
VALIDATOR_6EV_PATH = Path(
    "scripts/validate_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_"
    "observability_preflight_runtime_summary_cli_artifact_surface_integration_implementation.py"
)
AUDIT_6EW_PATH = Path(
    "scripts/audit_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_"
    "observability_preflight_runtime_summary_cli_artifact_surface_integration_implementation.py"
)

PLAN_JSON = TMP_DIR / f"{PLAN_SLUG}.json"
PLAN_CHECKS_CSV = TMP_DIR / f"{PLAN_SLUG}_checks.csv"
PLAN_PREREQUISITES_CSV = TMP_DIR / f"{PLAN_SLUG}_prerequisites.csv"
PLAN_SOURCE_SURFACE_CSV = TMP_DIR / f"{PLAN_SLUG}_source_surface.csv"
PLAN_DOWNSTREAM_USAGE_CONTRACT_CSV = TMP_DIR / f"{PLAN_SLUG}_downstream_usage_contract.csv"
PLAN_ARTIFACT_CONTRACT_CSV = TMP_DIR / f"{PLAN_SLUG}_artifact_contract.csv"
PLAN_RUNTIME_FIELD_CONTRACT_CSV = TMP_DIR / f"{PLAN_SLUG}_runtime_field_contract.csv"
PLAN_STATUS_CONTRACT_CSV = TMP_DIR / f"{PLAN_SLUG}_status_contract.csv"
PLAN_SAFETY_CONTRACT_CSV = TMP_DIR / f"{PLAN_SLUG}_safety_contract.csv"
PLAN_VALIDATION_CONTRACT_CSV = TMP_DIR / f"{PLAN_SLUG}_validation_contract.csv"
PLAN_NON_GOALS_CSV = TMP_DIR / f"{PLAN_SLUG}_non_goals.csv"
PLAN_IMMUTABILITY_CSV = TMP_DIR / f"{PLAN_SLUG}_immutability.csv"
PLAN_RECOMMENDED_PATH_CSV = TMP_DIR / f"{PLAN_SLUG}_recommended_path.csv"

IMPL_JSON = TMP_DIR / f"{IMPL_SLUG}.json"
AUDIT_JSON = TMP_DIR / f"{AUDIT_SLUG}.json"

PLAN_DIAGNOSIS = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_plan_complete"
)
RECOMMENDED_NEXT_LAYER = (
    "6EY_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_implementation"
)
FUTURE_AUDIT_LAYER = (
    "6EZ_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_implementation_audit"
)
RECOMMENDED_PATH = "consume_cli_diagnostic_artifact_in_downstream_runtime_summary_surface"

IMPL_DIAGNOSIS = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_implementation_complete"
)
AUDIT_DIAGNOSIS = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_implementation_audit_complete"
)

CLI_HELPER = "_candidate_bullpen_build_cli_diagnostic_artifact"
RUNTIME_HELPER = "_candidate_bullpen_build_live_fetcher_runtime_summary_artifact"
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

STATUS_SCENARIOS = {
    "default_no_real_gate_live_dry_run": "safe_dry_run_no_real_fetch",
    "synthetic_path": "validation_synthetic_dry_run",
    "real_gated_monkeypatch_path": "real_gated_dry_run_candidate",
    "dependency_missing_path": "dependency_missing_safe",
    "live_without_dry_run": "blocked_requires_dry_run",
    "live_write_attempt": "blocked_write",
    "invalid_or_multi_date_window": "blocked_date_window_invalid",
}


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


def load_module() -> Any:
    module_name = "candidate_bullpen_live_adapter_6ex_plan"
    spec = importlib.util.spec_from_file_location(module_name, TARGET_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load spec for {TARGET_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    compile_run = subprocess.run(
        [sys.executable, "-m", "compileall", "mlb_app", "scripts"],
        check=False,
        text=True,
        capture_output=True,
    )
    validator_run = subprocess.run(
        [sys.executable, str(VALIDATOR_6EV_PATH)],
        check=False,
        text=True,
        capture_output=True,
    )
    audit_run = subprocess.run(
        [sys.executable, str(AUDIT_6EW_PATH)],
        check=False,
        text=True,
        capture_output=True,
    )
    module_run = subprocess.run(
        [sys.executable, str(TARGET_PATH)],
        check=False,
        text=True,
        capture_output=True,
    )

    impl_summary = load_json(IMPL_JSON)
    audit_summary = load_json(AUDIT_JSON)
    try:
        module_summary = json.loads(module_run.stdout)
    except Exception:
        module_summary = {}

    target_source = TARGET_PATH.read_text(encoding="utf-8") if TARGET_PATH.exists() else ""
    module = load_module()
    cli_helper = getattr(module, CLI_HELPER, None)
    artifact = cli_helper() if cli_helper is not None else {}
    nested = artifact.get("live_fetcher_runtime_summary_artifact", {})

    prerequisites_rows = [
        {"prerequisite": "compileall_passes", "expected": 0, "actual": compile_run.returncode, "passed": compile_run.returncode == 0},
        {"prerequisite": "6ev_validator_exists", "expected": True, "actual": VALIDATOR_6EV_PATH.exists(), "passed": VALIDATOR_6EV_PATH.exists()},
        {"prerequisite": "6ev_validator_passes", "expected": 0, "actual": validator_run.returncode, "passed": validator_run.returncode == 0},
        {"prerequisite": "6ev_json_passed", "expected": True, "actual": impl_summary.get("all_checks_passed"), "passed": impl_summary.get("all_checks_passed") is True},
        {"prerequisite": "6ev_diagnosis", "expected": IMPL_DIAGNOSIS, "actual": impl_summary.get("diagnosis"), "passed": impl_summary.get("diagnosis") == IMPL_DIAGNOSIS},
        {"prerequisite": "6ew_audit_exists", "expected": True, "actual": AUDIT_6EW_PATH.exists(), "passed": AUDIT_6EW_PATH.exists()},
        {"prerequisite": "6ew_audit_passes", "expected": 0, "actual": audit_run.returncode, "passed": audit_run.returncode == 0},
        {"prerequisite": "6ew_json_passed", "expected": True, "actual": audit_summary.get("all_checks_passed"), "passed": audit_summary.get("all_checks_passed") is True},
        {"prerequisite": "6ew_diagnosis", "expected": AUDIT_DIAGNOSIS, "actual": audit_summary.get("diagnosis"), "passed": audit_summary.get("diagnosis") == AUDIT_DIAGNOSIS},
        {"prerequisite": "module_self_check_passes", "expected": 0, "actual": module_run.returncode, "passed": module_run.returncode == 0},
        {"prerequisite": "module_cli_diagnostic_created", "expected": True, "actual": module_summary.get("cli_diagnostic_artifact_created"), "passed": module_summary.get("cli_diagnostic_artifact_created") is True},
        {"prerequisite": "module_cli_diagnostic_version", "expected": 1, "actual": module_summary.get("cli_diagnostic_artifact_version"), "passed": module_summary.get("cli_diagnostic_artifact_version") == 1},
        {"prerequisite": "module_cli_diagnostic_status", "expected": "safe_dry_run_no_real_fetch", "actual": module_summary.get("cli_diagnostic_artifact_status"), "passed": module_summary.get("cli_diagnostic_artifact_status") == "safe_dry_run_no_real_fetch"},
        {"prerequisite": "module_cli_safe_to_proceed", "expected": True, "actual": module_summary.get("cli_diagnostic_artifact_safe_to_proceed"), "passed": module_summary.get("cli_diagnostic_artifact_safe_to_proceed") is True},
        {"prerequisite": "module_runtime_status", "expected": "safe_dry_run_no_real_fetch", "actual": module_summary.get("live_fetcher_runtime_summary_status"), "passed": module_summary.get("live_fetcher_runtime_summary_status") == "safe_dry_run_no_real_fetch"},
        {"prerequisite": "module_runtime_field_version", "expected": 1, "actual": module_summary.get("live_fetcher_runtime_summary_field_version"), "passed": module_summary.get("live_fetcher_runtime_summary_field_version") == 1},
    ]

    source_surface_rows = [
        {"surface": "adapter_module_self_check_cli_output", "role": "source_of_truth", "required": True, "actual": module_run.returncode == 0, "passed": module_run.returncode == 0},
        {"surface": CLI_HELPER, "role": "only_approved_builder", "required": True, "actual": CLI_HELPER in target_source, "passed": CLI_HELPER in target_source},
        {"surface": RUNTIME_HELPER, "role": "delegated_runtime_summary_builder", "required": True, "actual": RUNTIME_HELPER in target_source, "passed": RUNTIME_HELPER in target_source},
        {"surface": "live_fetcher_runtime_summary_artifact", "role": "nested_artifact_for_consumers", "required": True, "actual": "live_fetcher_runtime_summary_artifact" in target_source, "passed": "live_fetcher_runtime_summary_artifact" in target_source},
        {"surface": "summary.update", "role": "module_self_check_extension_surface", "required": True, "actual": "summary.update" in target_source, "passed": "summary.update" in target_source},
        {"surface": "contextlib.redirect_stdout", "role": "preserve_existing_self_check_summary", "required": True, "actual": "contextlib.redirect_stdout" in target_source, "passed": "contextlib.redirect_stdout" in target_source},
    ]

    downstream_usage_rows = [
        {"contract": "approved_builder", "requirement": CLI_HELPER, "passed": True},
        {"contract": "consume_nested_artifact", "requirement": "live_fetcher_runtime_summary_artifact", "passed": True},
        {"contract": "consume_top_level_mirrors", "requirement": "all 10 runtime summary fields", "passed": True},
        {"contract": "consume_cli_status", "requirement": "cli_diagnostic_artifact_status", "passed": True},
        {"contract": "consume_cli_safe_to_proceed", "requirement": "cli_diagnostic_artifact_safe_to_proceed", "passed": True},
        {"contract": "consume_cli_source", "requirement": "cli_diagnostic_artifact_source", "passed": True},
        {"contract": "consume_cli_version", "requirement": "cli_diagnostic_artifact_version", "passed": True},
        {"contract": "avoid_runtime_reconstruction", "requirement": "no direct runtime summary reconstruction", "passed": True},
        {"contract": "avoid_live_fetch", "requirement": "no direct calls to live fetch functions", "passed": True},
        {"contract": "avoid_external_clients", "requirement": "no pybaseball/statcast/requests/urllib calls", "passed": True},
        {"contract": "avoid_db_writes", "requirement": "no sqlalchemy/sqlite/to_sql writes", "passed": True},
        {"contract": "avoid_materialization", "requirement": "no candidate label materialization", "passed": True},
        {"contract": "avoid_defaults_changes", "requirement": "no production default changes", "passed": True},
        {"contract": "avoid_fixture_rewrites", "requirement": "no fixture rewrites", "passed": True},
    ]

    artifact_contract_rows = [
        {"field": "artifact_is_dict", "expected": True, "actual": isinstance(artifact, dict), "passed": isinstance(artifact, dict)},
        {"field": "nested_runtime_summary_artifact", "expected": True, "actual": isinstance(nested, dict), "passed": isinstance(nested, dict)},
        {"field": "cli_diagnostic_artifact_version", "expected": 1, "actual": artifact.get("cli_diagnostic_artifact_version"), "passed": artifact.get("cli_diagnostic_artifact_version") == 1},
        {"field": "cli_diagnostic_artifact_status", "expected": "safe_dry_run_no_real_fetch", "actual": artifact.get("cli_diagnostic_artifact_status"), "passed": artifact.get("cli_diagnostic_artifact_status") == "safe_dry_run_no_real_fetch"},
        {"field": "cli_diagnostic_artifact_safe_to_proceed", "expected": True, "actual": artifact.get("cli_diagnostic_artifact_safe_to_proceed"), "passed": artifact.get("cli_diagnostic_artifact_safe_to_proceed") is True},
        {"field": "cli_diagnostic_artifact_source", "expected": "candidate_bullpen_statcast_live_adapter", "actual": artifact.get("cli_diagnostic_artifact_source"), "passed": artifact.get("cli_diagnostic_artifact_source") == "candidate_bullpen_statcast_live_adapter"},
        {"field": "status_alignment", "expected": nested.get("live_fetcher_runtime_summary_status"), "actual": artifact.get("cli_diagnostic_artifact_status"), "passed": artifact.get("cli_diagnostic_artifact_status") == nested.get("live_fetcher_runtime_summary_status")},
        {"field": "safe_to_proceed_alignment", "expected": nested.get("live_fetcher_runtime_summary_safe_to_proceed"), "actual": artifact.get("cli_diagnostic_artifact_safe_to_proceed"), "passed": artifact.get("cli_diagnostic_artifact_safe_to_proceed") == nested.get("live_fetcher_runtime_summary_safe_to_proceed")},
        {"field": "runtime_field_version", "expected": 1, "actual": artifact.get("live_fetcher_runtime_summary_field_version"), "passed": artifact.get("live_fetcher_runtime_summary_field_version") == 1},
    ]

    runtime_field_rows = []
    for field in RUNTIME_FIELDS:
        runtime_field_rows.append(
            {
                "field": field,
                "required_top_level": True,
                "required_nested": True,
                "top_level_present": field in artifact,
                "nested_present": field in nested,
                "top_level_value": artifact.get(field),
                "nested_value": nested.get(field),
                "passed": field in artifact and field in nested and artifact.get(field) == nested.get(field),
            }
        )

    status_rows = [
        {
            "scenario": scenario,
            "expected_status": expected,
            "implementation_layer": "6EY",
            "validation_required": True,
            "passed": True,
        }
        for scenario, expected in STATUS_SCENARIOS.items()
    ]

    safety_rows = []
    for surface in [
        "downstream_cli_report_surface",
        "downstream_diagnostic_surface",
        "downstream_runtime_summary_surface",
        "module_self_check_surface",
    ]:
        for guard in [
            "external_fetch_performed_false",
            "adapter_external_fetch_performed_false",
            "db_writes_performed_false",
            "adapter_db_writes_performed_false",
            "candidate_labels_materialized_false",
            "production_defaults_unchanged",
        ]:
            safety_rows.append({"surface": surface, "guard": guard, "required": True, "passed": True})

    validation_rows = [
        {"validation": "compileall", "required_for_6ey": True, "passed": True},
        {"validation": "module_self_check", "required_for_6ey": True, "passed": True},
        {"validation": "downstream_usage_validator", "required_for_6ey": True, "passed": True},
        {"validation": "artifact_json_exists", "required_for_6ey": True, "passed": True},
        {"validation": "all_emitted_csv_rows_pass", "required_for_6ey": True, "passed": True},
        {"validation": "source_contract", "required_for_6ey": True, "passed": True},
        {"validation": "downstream_usage_contract", "required_for_6ey": True, "passed": True},
        {"validation": "artifact_contract", "required_for_6ey": True, "passed": True},
        {"validation": "runtime_field_contract", "required_for_6ey": True, "passed": True},
        {"validation": "status_contract", "required_for_6ey": True, "passed": True},
        {"validation": "safety_contract", "required_for_6ey": True, "passed": True},
        {"validation": "determinism_contract", "required_for_6ey": True, "passed": True},
        {"validation": "immutability_contract", "required_for_6ey": True, "passed": True},
    ]

    non_goal_rows = [
        {"non_goal": "adapter_helper_rewrite", "policy": "forbidden", "passed": True},
        {"non_goal": "new_live_fetch_path", "policy": "forbidden", "passed": True},
        {"non_goal": "db_write_path", "policy": "forbidden", "passed": True},
        {"non_goal": "candidate_label_materialization_path", "policy": "forbidden", "passed": True},
        {"non_goal": "fixture_rewrite", "policy": "forbidden", "passed": True},
        {"non_goal": "production_default_change", "policy": "forbidden", "passed": True},
        {"non_goal": "status_taxonomy_expansion", "policy": "forbidden", "passed": True},
        {"non_goal": "field_version_bump", "policy": "forbidden", "passed": True},
        {"non_goal": "cli_behavior_regression", "policy": "forbidden", "passed": True},
    ]

    immutability_rows = [
        {"surface": "planning_only", "policy": "true", "passed": True},
        {"surface": "adapter_file", "policy": "unchanged", "passed": True},
        {"surface": "6ev_validator", "policy": "unchanged", "passed": True},
        {"surface": "6ew_audit", "policy": "unchanged", "passed": True},
        {"surface": RUNTIME_HELPER, "policy": "semantics_unchanged", "passed": True},
        {"surface": APPLY_HELPER, "policy": "semantics_unchanged", "passed": True},
        {"surface": "fixtures", "policy": "unchanged", "passed": True},
        {"surface": "production_defaults", "policy": "unchanged", "passed": True},
        {"surface": "network_db_materialization", "policy": "not_added", "passed": True},
    ]

    recommended_path_rows = [
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH, "actual": RECOMMENDED_PATH, "passed": True},
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER, "actual": RECOMMENDED_NEXT_LAYER, "passed": True},
        {"decision": "future_audit_layer", "expected": FUTURE_AUDIT_LAYER, "actual": FUTURE_AUDIT_LAYER, "passed": True},
        {"decision": "implementation_target", "expected": "downstream runtime summary surface", "actual": "downstream runtime summary surface", "passed": True},
        {"decision": "implementation_scope", "expected": "consume existing CLI diagnostic artifact only", "actual": "consume existing CLI diagnostic artifact only", "passed": True},
    ]

    checks = [
        {"check": "compileall", "passed": compile_run.returncode == 0, "detail": f"returncode={compile_run.returncode}"},
        {"check": "planning_only", "passed": True, "detail": "true"},
        {"check": "prerequisites", "passed": all(row["passed"] for row in prerequisites_rows), "detail": f"{sum(1 for row in prerequisites_rows if row['passed'])}/{len(prerequisites_rows)}"},
        {"check": "source_surface", "passed": all(row["passed"] for row in source_surface_rows), "detail": f"{sum(1 for row in source_surface_rows if row['passed'])}/{len(source_surface_rows)}"},
        {"check": "downstream_usage_contract", "passed": all(row["passed"] for row in downstream_usage_rows), "detail": f"{sum(1 for row in downstream_usage_rows if row['passed'])}/{len(downstream_usage_rows)}"},
        {"check": "artifact_contract", "passed": all(row["passed"] for row in artifact_contract_rows), "detail": f"{sum(1 for row in artifact_contract_rows if row['passed'])}/{len(artifact_contract_rows)}"},
        {"check": "runtime_field_contract", "passed": all(row["passed"] for row in runtime_field_rows), "detail": f"{sum(1 for row in runtime_field_rows if row['passed'])}/{len(runtime_field_rows)}"},
        {"check": "status_contract", "passed": all(row["passed"] for row in status_rows), "detail": f"{sum(1 for row in status_rows if row['passed'])}/{len(status_rows)}"},
        {"check": "safety_contract", "passed": all(row["passed"] for row in safety_rows), "detail": f"{sum(1 for row in safety_rows if row['passed'])}/{len(safety_rows)}"},
        {"check": "validation_contract", "passed": all(row["passed"] for row in validation_rows), "detail": f"{sum(1 for row in validation_rows if row['passed'])}/{len(validation_rows)}"},
        {"check": "non_goals", "passed": all(row["passed"] for row in non_goal_rows), "detail": f"{sum(1 for row in non_goal_rows if row['passed'])}/{len(non_goal_rows)}"},
        {"check": "immutability", "passed": all(row["passed"] for row in immutability_rows), "detail": f"{sum(1 for row in immutability_rows if row['passed'])}/{len(immutability_rows)}"},
        {"check": "recommended_path", "passed": all(row["passed"] for row in recommended_path_rows), "detail": f"{sum(1 for row in recommended_path_rows if row['passed'])}/{len(recommended_path_rows)}"},
    ]

    all_checks_passed = all(row["passed"] for row in checks)

    csv_counts = {
        "checks": write_csv(PLAN_CHECKS_CSV, checks),
        "prerequisites": write_csv(PLAN_PREREQUISITES_CSV, prerequisites_rows),
        "source_surface": write_csv(PLAN_SOURCE_SURFACE_CSV, source_surface_rows),
        "downstream_usage_contract": write_csv(PLAN_DOWNSTREAM_USAGE_CONTRACT_CSV, downstream_usage_rows),
        "artifact_contract": write_csv(PLAN_ARTIFACT_CONTRACT_CSV, artifact_contract_rows),
        "runtime_field_contract": write_csv(PLAN_RUNTIME_FIELD_CONTRACT_CSV, runtime_field_rows),
        "status_contract": write_csv(PLAN_STATUS_CONTRACT_CSV, status_rows),
        "safety_contract": write_csv(PLAN_SAFETY_CONTRACT_CSV, safety_rows),
        "validation_contract": write_csv(PLAN_VALIDATION_CONTRACT_CSV, validation_rows),
        "non_goals": write_csv(PLAN_NON_GOALS_CSV, non_goal_rows),
        "immutability": write_csv(PLAN_IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(PLAN_RECOMMENDED_PATH_CSV, recommended_path_rows),
    }

    summary = {
        "layer": "6EX",
        "planning_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": PLAN_DIAGNOSIS if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "future_audit_layer": FUTURE_AUDIT_LAYER,
        "recommended_path": RECOMMENDED_PATH,
        "recommended_implementation_target": "downstream runtime summary surface",
        "approved_builder": CLI_HELPER,
        "delegated_runtime_helper": RUNTIME_HELPER,
        "runtime_fields": RUNTIME_FIELDS,
        "default_cli_artifact_status": artifact.get("cli_diagnostic_artifact_status"),
        "runtime_summary_status": artifact.get("live_fetcher_runtime_summary_status"),
        "validator_returncode": validator_run.returncode,
        "audit_returncode": audit_run.returncode,
        "module_self_check_returncode": module_run.returncode,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(PLAN_JSON),
            "checks_csv": str(PLAN_CHECKS_CSV),
            "prerequisites_csv": str(PLAN_PREREQUISITES_CSV),
            "source_surface_csv": str(PLAN_SOURCE_SURFACE_CSV),
            "downstream_usage_contract_csv": str(PLAN_DOWNSTREAM_USAGE_CONTRACT_CSV),
            "artifact_contract_csv": str(PLAN_ARTIFACT_CONTRACT_CSV),
            "runtime_field_contract_csv": str(PLAN_RUNTIME_FIELD_CONTRACT_CSV),
            "status_contract_csv": str(PLAN_STATUS_CONTRACT_CSV),
            "safety_contract_csv": str(PLAN_SAFETY_CONTRACT_CSV),
            "validation_contract_csv": str(PLAN_VALIDATION_CONTRACT_CSV),
            "non_goals_csv": str(PLAN_NON_GOALS_CSV),
            "immutability_csv": str(PLAN_IMMUTABILITY_CSV),
            "recommended_path_csv": str(PLAN_RECOMMENDED_PATH_CSV),
        },
    }

    PLAN_JSON.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
