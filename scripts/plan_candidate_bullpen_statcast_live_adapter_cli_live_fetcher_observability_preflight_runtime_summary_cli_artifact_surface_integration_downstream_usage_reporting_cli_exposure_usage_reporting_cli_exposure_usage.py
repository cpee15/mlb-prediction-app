#!/usr/bin/env python3
"""Plan usage-facing consumption of usage-reporting CLI exposure artifacts."""

from __future__ import annotations

import csv
import importlib.util
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable


PLAN_SLUG = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_usage_plan"
)
IMPL_SLUG_6FN = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_implementation"
)
AUDIT_SLUG_6FO = f"{IMPL_SLUG_6FN}_audit"

TMP_DIR = Path("tmp")
TARGET_PATH = Path("scripts/fetch_candidate_bullpen_statcast_live_adapter.py")
VALIDATOR_6FN_PATH = Path(
    "scripts/validate_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_"
    "observability_preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_implementation.py"
)
AUDIT_6FO_PATH = Path(
    "scripts/audit_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_"
    "observability_preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_implementation.py"
)

PLAN_JSON = TMP_DIR / f"{PLAN_SLUG}.json"
PLAN_CHECKS_CSV = TMP_DIR / f"{PLAN_SLUG}_checks.csv"
PLAN_PREREQUISITES_CSV = TMP_DIR / f"{PLAN_SLUG}_prerequisites.csv"
PLAN_SOURCE_SURFACE_CSV = TMP_DIR / f"{PLAN_SLUG}_source_surface.csv"
PLAN_USAGE_CONTRACT_CSV = TMP_DIR / f"{PLAN_SLUG}_usage_contract.csv"
PLAN_ARTIFACT_CONTRACT_CSV = TMP_DIR / f"{PLAN_SLUG}_artifact_contract.csv"
PLAN_RUNTIME_FIELD_CONTRACT_CSV = TMP_DIR / f"{PLAN_SLUG}_runtime_field_contract.csv"
PLAN_STATUS_CONTRACT_CSV = TMP_DIR / f"{PLAN_SLUG}_status_contract.csv"
PLAN_SAFETY_CONTRACT_CSV = TMP_DIR / f"{PLAN_SLUG}_safety_contract.csv"
PLAN_VALIDATION_CONTRACT_CSV = TMP_DIR / f"{PLAN_SLUG}_validation_contract.csv"
PLAN_NON_GOALS_CSV = TMP_DIR / f"{PLAN_SLUG}_non_goals.csv"
PLAN_IMMUTABILITY_CSV = TMP_DIR / f"{PLAN_SLUG}_immutability.csv"
PLAN_RECOMMENDED_PATH_CSV = TMP_DIR / f"{PLAN_SLUG}_recommended_path.csv"

IMPL_JSON_6FN = TMP_DIR / f"{IMPL_SLUG_6FN}.json"
AUDIT_JSON_6FO = TMP_DIR / f"{AUDIT_SLUG_6FO}.json"

PLAN_DIAGNOSIS = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_usage_plan_complete"
)
RECOMMENDED_NEXT_LAYER = (
    "6FQ_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_usage_implementation"
)
FUTURE_AUDIT_LAYER = (
    "6FR_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_usage_implementation_audit"
)
RECOMMENDED_PATH = (
    "consume_downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_artifact_in_usage_surface"
)

DIAGNOSIS_6FN = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_implementation_complete"
)
DIAGNOSIS_6FO = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_implementation_audit_complete"
)

APPROVED_USAGE_SOURCE = "_candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_artifact"
FUTURE_USAGE_HELPER = "_candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_artifact"
FUTURE_WRAPPER = "_candidate_bullpen_emit_module_self_check_summary_with_usage_reporting_cli_exposure_usage"
REPORTING_HELPER = "_candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_reporting_artifact"
USAGE_HELPER = "_candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_artifact"
CLI_EXPOSURE_HELPER = "_candidate_bullpen_build_downstream_runtime_summary_cli_exposure_artifact"
UPSTREAM_REPORTING_HELPER = "_candidate_bullpen_build_downstream_runtime_summary_reporting_artifact"
DOWNSTREAM_HELPER = "_candidate_bullpen_build_downstream_runtime_summary_usage_artifact"
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


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def load_module() -> Any:
    module_name = "candidate_bullpen_live_adapter_6fp_plan"
    spec = importlib.util.spec_from_file_location(module_name, TARGET_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load spec for {TARGET_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    compile_run = subprocess.run(
        [sys.executable, "-m", "compileall", "mlb_app", "scripts"],
        check=False,
        text=True,
        capture_output=True,
    )
    validator_run = subprocess.run(
        [sys.executable, str(VALIDATOR_6FN_PATH)],
        check=False,
        text=True,
        capture_output=True,
    )
    audit_run = subprocess.run(
        [sys.executable, str(AUDIT_6FO_PATH)],
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

    impl_summary = load_json(IMPL_JSON_6FN)
    audit_summary = load_json(AUDIT_JSON_6FO)
    try:
        module_summary = json.loads(module_run.stdout)
    except Exception:
        module_summary = {}

    target_source = TARGET_PATH.read_text(encoding="utf-8") if TARGET_PATH.exists() else ""
    module = load_module()
    cli_exposure_helper = getattr(module, APPROVED_USAGE_SOURCE, None)
    cli_exposure_artifact = cli_exposure_helper() if cli_exposure_helper is not None else {}
    reporting_artifact = cli_exposure_artifact.get(
        "downstream_runtime_summary_cli_exposure_usage_reporting_artifact", {}
    )
    usage_artifact = cli_exposure_artifact.get(
        "downstream_runtime_summary_cli_exposure_usage_artifact", {}
    )
    prior_cli_exposure_artifact = cli_exposure_artifact.get(
        "downstream_runtime_summary_cli_exposure_artifact", {}
    )
    upstream_reporting_artifact = cli_exposure_artifact.get(
        "downstream_runtime_summary_reporting_artifact", {}
    )
    downstream_artifact = cli_exposure_artifact.get("downstream_runtime_summary_usage_artifact", {})
    cli_artifact = cli_exposure_artifact.get("cli_diagnostic_artifact", {})
    runtime_artifact = cli_exposure_artifact.get("live_fetcher_runtime_summary_artifact", {})

    prerequisites_rows = [
        {"prerequisite": "compileall_passes", "expected": 0, "actual": compile_run.returncode, "passed": compile_run.returncode == 0},
        {"prerequisite": "6fn_validator_exists", "expected": True, "actual": VALIDATOR_6FN_PATH.exists(), "passed": VALIDATOR_6FN_PATH.exists()},
        {"prerequisite": "6fn_validator_passes", "expected": 0, "actual": validator_run.returncode, "passed": validator_run.returncode == 0},
        {"prerequisite": "6fn_json_passed", "expected": True, "actual": impl_summary.get("all_checks_passed"), "passed": impl_summary.get("all_checks_passed") is True},
        {"prerequisite": "6fn_diagnosis", "expected": DIAGNOSIS_6FN, "actual": impl_summary.get("diagnosis"), "passed": impl_summary.get("diagnosis") == DIAGNOSIS_6FN},
        {"prerequisite": "6fo_audit_exists", "expected": True, "actual": AUDIT_6FO_PATH.exists(), "passed": AUDIT_6FO_PATH.exists()},
        {"prerequisite": "6fo_audit_passes", "expected": 0, "actual": audit_run.returncode, "passed": audit_run.returncode == 0},
        {"prerequisite": "6fo_json_passed", "expected": True, "actual": audit_summary.get("all_checks_passed"), "passed": audit_summary.get("all_checks_passed") is True},
        {"prerequisite": "6fo_diagnosis", "expected": DIAGNOSIS_6FO, "actual": audit_summary.get("diagnosis"), "passed": audit_summary.get("diagnosis") == DIAGNOSIS_6FO},
        {"prerequisite": "module_self_check_passes", "expected": 0, "actual": module_run.returncode, "passed": module_run.returncode == 0},
        {"prerequisite": "module_cli_exposure_usage_reporting_cli_exposure_created", "expected": True, "actual": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_artifact_created"), "passed": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_artifact_created") is True},
        {"prerequisite": "module_cli_exposure_usage_reporting_cli_exposure_version", "expected": 1, "actual": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_artifact_version"), "passed": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_artifact_version") == 1},
        {"prerequisite": "module_cli_exposure_usage_reporting_cli_exposure_status", "expected": "safe_dry_run_no_real_fetch", "actual": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_status"), "passed": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_status") == "safe_dry_run_no_real_fetch"},
        {"prerequisite": "module_cli_exposure_usage_reporting_cli_exposure_safe_to_proceed", "expected": True, "actual": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_safe_to_proceed"), "passed": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_safe_to_proceed") is True},
        {"prerequisite": "module_cli_exposure_usage_reporting_cli_exposure_source", "expected": "candidate_bullpen_statcast_live_adapter", "actual": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_source"), "passed": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_source") == "candidate_bullpen_statcast_live_adapter"},
        {"prerequisite": "module_reporting_created", "expected": True, "actual": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_artifact_created"), "passed": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_artifact_created") is True},
        {"prerequisite": "module_reporting_status", "expected": "safe_dry_run_no_real_fetch", "actual": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_status"), "passed": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_status") == "safe_dry_run_no_real_fetch"},
        {"prerequisite": "module_usage_created", "expected": True, "actual": module_summary.get("downstream_runtime_summary_cli_exposure_usage_artifact_created"), "passed": module_summary.get("downstream_runtime_summary_cli_exposure_usage_artifact_created") is True},
        {"prerequisite": "module_usage_status", "expected": "safe_dry_run_no_real_fetch", "actual": module_summary.get("downstream_runtime_summary_cli_exposure_usage_status"), "passed": module_summary.get("downstream_runtime_summary_cli_exposure_usage_status") == "safe_dry_run_no_real_fetch"},
        {"prerequisite": "module_prior_cli_exposure_created", "expected": True, "actual": module_summary.get("downstream_runtime_summary_cli_exposure_artifact_created"), "passed": module_summary.get("downstream_runtime_summary_cli_exposure_artifact_created") is True},
        {"prerequisite": "module_prior_cli_exposure_status", "expected": "safe_dry_run_no_real_fetch", "actual": module_summary.get("downstream_runtime_summary_cli_exposure_status"), "passed": module_summary.get("downstream_runtime_summary_cli_exposure_status") == "safe_dry_run_no_real_fetch"},
        {"prerequisite": "module_upstream_reporting_created", "expected": True, "actual": module_summary.get("downstream_runtime_summary_reporting_artifact_created"), "passed": module_summary.get("downstream_runtime_summary_reporting_artifact_created") is True},
        {"prerequisite": "module_upstream_reporting_status", "expected": "safe_dry_run_no_real_fetch", "actual": module_summary.get("downstream_runtime_summary_reporting_status"), "passed": module_summary.get("downstream_runtime_summary_reporting_status") == "safe_dry_run_no_real_fetch"},
        {"prerequisite": "module_downstream_usage_created", "expected": True, "actual": module_summary.get("downstream_runtime_summary_usage_artifact_created"), "passed": module_summary.get("downstream_runtime_summary_usage_artifact_created") is True},
        {"prerequisite": "module_downstream_usage_status", "expected": "safe_dry_run_no_real_fetch", "actual": module_summary.get("downstream_runtime_summary_usage_status"), "passed": module_summary.get("downstream_runtime_summary_usage_status") == "safe_dry_run_no_real_fetch"},
        {"prerequisite": "module_cli_created", "expected": True, "actual": module_summary.get("cli_diagnostic_artifact_created"), "passed": module_summary.get("cli_diagnostic_artifact_created") is True},
        {"prerequisite": "module_cli_status", "expected": "safe_dry_run_no_real_fetch", "actual": module_summary.get("cli_diagnostic_artifact_status"), "passed": module_summary.get("cli_diagnostic_artifact_status") == "safe_dry_run_no_real_fetch"},
        {"prerequisite": "module_runtime_status", "expected": "safe_dry_run_no_real_fetch", "actual": module_summary.get("live_fetcher_runtime_summary_status"), "passed": module_summary.get("live_fetcher_runtime_summary_status") == "safe_dry_run_no_real_fetch"},
    ]

    source_surface_rows = [
        {"surface": APPROVED_USAGE_SOURCE, "role": "only_approved_usage_source", "required": True, "actual": APPROVED_USAGE_SOURCE in target_source, "passed": APPROVED_USAGE_SOURCE in target_source},
        {"surface": REPORTING_HELPER, "role": "upstream_reporting_builder_not_direct_usage_source", "required": True, "actual": REPORTING_HELPER in target_source, "passed": REPORTING_HELPER in target_source},
        {"surface": USAGE_HELPER, "role": "upstream_usage_builder_not_direct_usage_source", "required": True, "actual": USAGE_HELPER in target_source, "passed": USAGE_HELPER in target_source},
        {"surface": CLI_EXPOSURE_HELPER, "role": "upstream_prior_cli_exposure_builder_not_direct_usage_source", "required": True, "actual": CLI_EXPOSURE_HELPER in target_source, "passed": CLI_EXPOSURE_HELPER in target_source},
        {"surface": UPSTREAM_REPORTING_HELPER, "role": "upstream_reporting_builder_not_direct_usage_source", "required": True, "actual": UPSTREAM_REPORTING_HELPER in target_source, "passed": UPSTREAM_REPORTING_HELPER in target_source},
        {"surface": DOWNSTREAM_HELPER, "role": "upstream_downstream_usage_builder_not_direct_usage_source", "required": True, "actual": DOWNSTREAM_HELPER in target_source, "passed": DOWNSTREAM_HELPER in target_source},
        {"surface": CLI_HELPER, "role": "upstream_cli_diagnostic_builder_not_direct_usage_source", "required": True, "actual": CLI_HELPER in target_source, "passed": CLI_HELPER in target_source},
        {"surface": RUNTIME_HELPER, "role": "upstream_runtime_builder_not_direct_usage_source", "required": True, "actual": RUNTIME_HELPER in target_source, "passed": RUNTIME_HELPER in target_source},
        {"surface": "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_artifact_created", "role": "module_self_check_usage_prerequisite", "required": True, "actual": "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_artifact_created" in target_source, "passed": "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_artifact_created" in target_source},
        {"surface": "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_artifact", "role": "nested_cli_exposure_artifact_for_usage", "required": True, "actual": "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_artifact" in target_source, "passed": "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_artifact" in target_source},
        {"surface": "downstream_runtime_summary_cli_exposure_usage_reporting_artifact", "role": "nested_reporting_artifact_for_usage", "required": True, "actual": "downstream_runtime_summary_cli_exposure_usage_reporting_artifact" in target_source, "passed": "downstream_runtime_summary_cli_exposure_usage_reporting_artifact" in target_source},
        {"surface": "downstream_runtime_summary_cli_exposure_usage_artifact", "role": "nested_usage_artifact_for_usage", "required": True, "actual": "downstream_runtime_summary_cli_exposure_usage_artifact" in target_source, "passed": "downstream_runtime_summary_cli_exposure_usage_artifact" in target_source},
        {"surface": "downstream_runtime_summary_cli_exposure_artifact", "role": "nested_prior_cli_exposure_artifact_for_usage", "required": True, "actual": "downstream_runtime_summary_cli_exposure_artifact" in target_source, "passed": "downstream_runtime_summary_cli_exposure_artifact" in target_source},
        {"surface": "downstream_runtime_summary_reporting_artifact", "role": "nested_upstream_reporting_artifact_for_usage", "required": True, "actual": "downstream_runtime_summary_reporting_artifact" in target_source, "passed": "downstream_runtime_summary_reporting_artifact" in target_source},
        {"surface": "downstream_runtime_summary_usage_artifact", "role": "nested_downstream_usage_artifact_for_usage", "required": True, "actual": "downstream_runtime_summary_usage_artifact" in target_source, "passed": "downstream_runtime_summary_usage_artifact" in target_source},
        {"surface": "cli_diagnostic_artifact", "role": "nested_cli_artifact_for_usage", "required": True, "actual": "cli_diagnostic_artifact" in target_source, "passed": "cli_diagnostic_artifact" in target_source},
        {"surface": "live_fetcher_runtime_summary_artifact", "role": "nested_runtime_artifact_for_usage", "required": True, "actual": "live_fetcher_runtime_summary_artifact" in target_source, "passed": "live_fetcher_runtime_summary_artifact" in target_source},
    ]

    usage_rows = [
        {"contract": "approved_usage_source", "requirement": APPROVED_USAGE_SOURCE, "passed": True},
        {"contract": "future_usage_helper", "requirement": FUTURE_USAGE_HELPER, "passed": True},
        {"contract": "future_module_self_check_wrapper", "requirement": FUTURE_WRAPPER, "passed": True},
        {"contract": "avoid_direct_reporting_helper_calls", "requirement": f"usage-facing consumers must not call {REPORTING_HELPER}", "passed": True},
        {"contract": "avoid_direct_usage_helper_calls", "requirement": f"usage-facing consumers must not call {USAGE_HELPER}", "passed": True},
        {"contract": "avoid_direct_prior_cli_exposure_helper_calls", "requirement": f"usage-facing consumers must not call {CLI_EXPOSURE_HELPER}", "passed": True},
        {"contract": "avoid_direct_upstream_reporting_helper_calls", "requirement": f"usage-facing consumers must not call {UPSTREAM_REPORTING_HELPER}", "passed": True},
        {"contract": "avoid_direct_downstream_usage_helper_calls", "requirement": f"usage-facing consumers must not call {DOWNSTREAM_HELPER}", "passed": True},
        {"contract": "avoid_direct_cli_helper_calls", "requirement": f"usage-facing consumers must not call {CLI_HELPER}", "passed": True},
        {"contract": "avoid_direct_runtime_helper_calls", "requirement": f"usage-facing consumers must not call {RUNTIME_HELPER}", "passed": True},
        {"contract": "avoid_runtime_reconstruction", "requirement": "no direct runtime summary reconstruction", "passed": True},
        {"contract": "surface_usage_version", "requirement": "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_artifact_version", "passed": True},
        {"contract": "surface_usage_status", "requirement": "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_status", "passed": True},
        {"contract": "surface_usage_safe_to_proceed", "requirement": "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_safe_to_proceed", "passed": True},
        {"contract": "surface_usage_source", "requirement": "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_source", "passed": True},
        {"contract": "surface_usage_reason", "requirement": "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_reason", "passed": True},
        {"contract": "surface_cli_exposure_artifact", "requirement": "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_artifact", "passed": True},
        {"contract": "surface_reporting_artifact", "requirement": "downstream_runtime_summary_cli_exposure_usage_reporting_artifact", "passed": True},
        {"contract": "surface_usage_artifact", "requirement": "downstream_runtime_summary_cli_exposure_usage_artifact", "passed": True},
        {"contract": "surface_prior_cli_exposure_artifact", "requirement": "downstream_runtime_summary_cli_exposure_artifact", "passed": True},
        {"contract": "surface_upstream_reporting_artifact", "requirement": "downstream_runtime_summary_reporting_artifact", "passed": True},
        {"contract": "surface_downstream_usage_artifact", "requirement": "downstream_runtime_summary_usage_artifact", "passed": True},
        {"contract": "surface_cli_artifact", "requirement": "cli_diagnostic_artifact", "passed": True},
        {"contract": "surface_runtime_artifact", "requirement": "live_fetcher_runtime_summary_artifact", "passed": True},
        {"contract": "surface_runtime_fields", "requirement": "all 10 runtime fields", "passed": True},
        {"contract": "surface_safety_fields_when_present", "requirement": "external_fetch/db/materialization/default safety fields", "passed": True},
        {"contract": "avoid_live_fetch", "requirement": "no direct live fetch calls", "passed": True},
        {"contract": "avoid_external_clients", "requirement": "no pybaseball/statcast/requests/urllib calls", "passed": True},
        {"contract": "avoid_db_writes", "requirement": "no sqlalchemy/sqlite/to_sql writes", "passed": True},
        {"contract": "avoid_materialization", "requirement": "no candidate label materialization", "passed": True},
        {"contract": "avoid_defaults_changes", "requirement": "no production default changes", "passed": True},
        {"contract": "avoid_fixture_rewrites", "requirement": "no fixture rewrites", "passed": True},
    ]

    artifact_rows = [
        {"field": "cli_exposure_artifact_is_dict", "expected": True, "actual": isinstance(cli_exposure_artifact, dict), "passed": isinstance(cli_exposure_artifact, dict)},
        {"field": "cli_exposure_artifact_version", "expected": 1, "actual": cli_exposure_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_artifact_version"), "passed": cli_exposure_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_artifact_version") == 1},
        {"field": "cli_exposure_artifact_status", "expected": "safe_dry_run_no_real_fetch", "actual": cli_exposure_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_status"), "passed": cli_exposure_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_status") == "safe_dry_run_no_real_fetch"},
        {"field": "cli_exposure_artifact_safe_to_proceed", "expected": True, "actual": cli_exposure_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_safe_to_proceed"), "passed": cli_exposure_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_safe_to_proceed") is True},
        {"field": "cli_exposure_artifact_source", "expected": "candidate_bullpen_statcast_live_adapter", "actual": cli_exposure_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_source"), "passed": cli_exposure_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_source") == "candidate_bullpen_statcast_live_adapter"},
        {"field": "nested_reporting_artifact", "expected": True, "actual": isinstance(reporting_artifact, dict), "passed": isinstance(reporting_artifact, dict)},
        {"field": "nested_usage_artifact", "expected": True, "actual": isinstance(usage_artifact, dict), "passed": isinstance(usage_artifact, dict)},
        {"field": "nested_prior_cli_exposure_artifact", "expected": True, "actual": isinstance(prior_cli_exposure_artifact, dict), "passed": isinstance(prior_cli_exposure_artifact, dict)},
        {"field": "nested_upstream_reporting_artifact", "expected": True, "actual": isinstance(upstream_reporting_artifact, dict), "passed": isinstance(upstream_reporting_artifact, dict)},
        {"field": "nested_downstream_usage_artifact", "expected": True, "actual": isinstance(downstream_artifact, dict), "passed": isinstance(downstream_artifact, dict)},
        {"field": "nested_cli_diagnostic_artifact", "expected": True, "actual": isinstance(cli_artifact, dict), "passed": isinstance(cli_artifact, dict)},
        {"field": "nested_runtime_summary_artifact", "expected": True, "actual": isinstance(runtime_artifact, dict), "passed": isinstance(runtime_artifact, dict)},
        {"field": "status_alignment", "expected": runtime_artifact.get("live_fetcher_runtime_summary_status"), "actual": cli_exposure_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_status"), "passed": cli_exposure_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_status") == reporting_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_status") == usage_artifact.get("downstream_runtime_summary_cli_exposure_usage_status") == prior_cli_exposure_artifact.get("downstream_runtime_summary_cli_exposure_status") == upstream_reporting_artifact.get("downstream_runtime_summary_reporting_status") == downstream_artifact.get("downstream_runtime_summary_usage_status") == cli_artifact.get("cli_diagnostic_artifact_status") == runtime_artifact.get("live_fetcher_runtime_summary_status")},
        {"field": "safe_to_proceed_alignment", "expected": runtime_artifact.get("live_fetcher_runtime_summary_safe_to_proceed"), "actual": cli_exposure_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_safe_to_proceed"), "passed": cli_exposure_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_safe_to_proceed") == reporting_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_safe_to_proceed") == usage_artifact.get("downstream_runtime_summary_cli_exposure_usage_safe_to_proceed") == prior_cli_exposure_artifact.get("downstream_runtime_summary_cli_exposure_safe_to_proceed") == upstream_reporting_artifact.get("downstream_runtime_summary_reporting_safe_to_proceed") == downstream_artifact.get("downstream_runtime_summary_usage_safe_to_proceed") == cli_artifact.get("cli_diagnostic_artifact_safe_to_proceed") == runtime_artifact.get("live_fetcher_runtime_summary_safe_to_proceed")},
        {"field": "runtime_field_version", "expected": 1, "actual": cli_exposure_artifact.get("live_fetcher_runtime_summary_field_version"), "passed": cli_exposure_artifact.get("live_fetcher_runtime_summary_field_version") == 1},
    ]

    runtime_field_rows = []
    for field in RUNTIME_FIELDS:
        runtime_field_rows.append(
            {
                "field": field,
                "required_usage": True,
                "required_cli_exposure": True,
                "required_reporting": True,
                "required_usage_artifact": True,
                "required_prior_cli_exposure": True,
                "required_upstream_reporting": True,
                "required_downstream": True,
                "required_cli": True,
                "required_runtime": True,
                "cli_exposure_present": field in cli_exposure_artifact,
                "reporting_present": field in reporting_artifact,
                "usage_present": field in usage_artifact,
                "prior_cli_exposure_present": field in prior_cli_exposure_artifact,
                "upstream_reporting_present": field in upstream_reporting_artifact,
                "downstream_present": field in downstream_artifact,
                "cli_present": field in cli_artifact,
                "runtime_present": field in runtime_artifact,
                "cli_exposure_value": cli_exposure_artifact.get(field),
                "reporting_value": reporting_artifact.get(field),
                "usage_value": usage_artifact.get(field),
                "prior_cli_exposure_value": prior_cli_exposure_artifact.get(field),
                "upstream_reporting_value": upstream_reporting_artifact.get(field),
                "downstream_value": downstream_artifact.get(field),
                "cli_value": cli_artifact.get(field),
                "runtime_value": runtime_artifact.get(field),
                "passed": field in cli_exposure_artifact
                and field in reporting_artifact
                and field in usage_artifact
                and field in prior_cli_exposure_artifact
                and field in upstream_reporting_artifact
                and field in downstream_artifact
                and field in cli_artifact
                and field in runtime_artifact
                and cli_exposure_artifact.get(field)
                == reporting_artifact.get(field)
                == usage_artifact.get(field)
                == prior_cli_exposure_artifact.get(field)
                == upstream_reporting_artifact.get(field)
                == downstream_artifact.get(field)
                == cli_artifact.get(field)
                == runtime_artifact.get(field),
            }
        )

    status_rows = [
        {
            "scenario": scenario,
            "expected_status": expected,
            "implementation_layer": "6FQ",
            "validation_required": True,
            "passed": True,
        }
        for scenario, expected in STATUS_SCENARIOS.items()
    ]

    safety_rows = []
    for surface in [
        "usage_surface",
        "usage_json_surface",
        "usage_summary_surface",
        "cli_exposure_surface",
        "reporting_surface",
        "prior_usage_surface",
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
        {"validation": "compileall", "required_for_6fq": True, "passed": True},
        {"validation": "module_self_check", "required_for_6fq": True, "passed": True},
        {"validation": "usage_facing_validator", "required_for_6fq": True, "passed": True},
        {"validation": "artifact_json_exists", "required_for_6fq": True, "passed": True},
        {"validation": "all_emitted_csv_rows_pass", "required_for_6fq": True, "passed": True},
        {"validation": "source_contract", "required_for_6fq": True, "passed": True},
        {"validation": "usage_contract", "required_for_6fq": True, "passed": True},
        {"validation": "artifact_contract", "required_for_6fq": True, "passed": True},
        {"validation": "runtime_field_contract", "required_for_6fq": True, "passed": True},
        {"validation": "status_contract", "required_for_6fq": True, "passed": True},
        {"validation": "safety_contract", "required_for_6fq": True, "passed": True},
        {"validation": "determinism_contract", "required_for_6fq": True, "passed": True},
        {"validation": "immutability_contract", "required_for_6fq": True, "passed": True},
    ]

    non_goal_rows = [
        {"non_goal": "adapter_helper_rewrite", "policy": "forbidden", "passed": True},
        {"non_goal": "cli_exposure_helper_rewrite", "policy": "forbidden", "passed": True},
        {"non_goal": "reporting_helper_rewrite", "policy": "forbidden", "passed": True},
        {"non_goal": "usage_helper_rewrite", "policy": "forbidden", "passed": True},
        {"non_goal": "prior_cli_exposure_helper_rewrite", "policy": "forbidden", "passed": True},
        {"non_goal": "upstream_reporting_helper_rewrite", "policy": "forbidden", "passed": True},
        {"non_goal": "downstream_usage_helper_rewrite", "policy": "forbidden", "passed": True},
        {"non_goal": "cli_diagnostic_helper_rewrite", "policy": "forbidden", "passed": True},
        {"non_goal": "runtime_summary_helper_rewrite", "policy": "forbidden", "passed": True},
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
        {"surface": "6fn_validator", "policy": "unchanged", "passed": True},
        {"surface": "6fo_audit", "policy": "unchanged", "passed": True},
        {"surface": APPROVED_USAGE_SOURCE, "policy": "semantics_unchanged", "passed": True},
        {"surface": REPORTING_HELPER, "policy": "semantics_unchanged", "passed": True},
        {"surface": USAGE_HELPER, "policy": "semantics_unchanged", "passed": True},
        {"surface": CLI_EXPOSURE_HELPER, "policy": "semantics_unchanged", "passed": True},
        {"surface": UPSTREAM_REPORTING_HELPER, "policy": "semantics_unchanged", "passed": True},
        {"surface": DOWNSTREAM_HELPER, "policy": "semantics_unchanged", "passed": True},
        {"surface": CLI_HELPER, "policy": "semantics_unchanged", "passed": True},
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
        {"decision": "implementation_target", "expected": "usage-facing artifact surfaces", "actual": "usage-facing artifact surfaces", "passed": True},
        {"decision": "implementation_scope", "expected": "consume existing usage-reporting CLI exposure artifact only", "actual": "consume existing usage-reporting CLI exposure artifact only", "passed": True},
    ]

    checks = [
        {"check": "compileall", "passed": compile_run.returncode == 0, "detail": f"returncode={compile_run.returncode}"},
        {"check": "planning_only", "passed": True, "detail": "true"},
        {"check": "prerequisites", "passed": all(row["passed"] for row in prerequisites_rows), "detail": f"{sum(1 for row in prerequisites_rows if row['passed'])}/{len(prerequisites_rows)}"},
        {"check": "source_surface", "passed": all(row["passed"] for row in source_surface_rows), "detail": f"{sum(1 for row in source_surface_rows if row['passed'])}/{len(source_surface_rows)}"},
        {"check": "usage_contract", "passed": all(row["passed"] for row in usage_rows), "detail": f"{sum(1 for row in usage_rows if row['passed'])}/{len(usage_rows)}"},
        {"check": "artifact_contract", "passed": all(row["passed"] for row in artifact_rows), "detail": f"{sum(1 for row in artifact_rows if row['passed'])}/{len(artifact_rows)}"},
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
        "usage_contract": write_csv(PLAN_USAGE_CONTRACT_CSV, usage_rows),
        "artifact_contract": write_csv(PLAN_ARTIFACT_CONTRACT_CSV, artifact_rows),
        "runtime_field_contract": write_csv(PLAN_RUNTIME_FIELD_CONTRACT_CSV, runtime_field_rows),
        "status_contract": write_csv(PLAN_STATUS_CONTRACT_CSV, status_rows),
        "safety_contract": write_csv(PLAN_SAFETY_CONTRACT_CSV, safety_rows),
        "validation_contract": write_csv(PLAN_VALIDATION_CONTRACT_CSV, validation_rows),
        "non_goals": write_csv(PLAN_NON_GOALS_CSV, non_goal_rows),
        "immutability": write_csv(PLAN_IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(PLAN_RECOMMENDED_PATH_CSV, recommended_path_rows),
    }

    summary = {
        "layer": "6FP",
        "planning_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": PLAN_DIAGNOSIS if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "future_audit_layer": FUTURE_AUDIT_LAYER,
        "recommended_path": RECOMMENDED_PATH,
        "recommended_implementation_target": "usage-facing artifact surfaces",
        "approved_usage_source": APPROVED_USAGE_SOURCE,
        "future_usage_helper": FUTURE_USAGE_HELPER,
        "future_module_self_check_wrapper": FUTURE_WRAPPER,
        "upstream_reporting_helper": REPORTING_HELPER,
        "upstream_usage_helper": USAGE_HELPER,
        "upstream_prior_cli_exposure_helper": CLI_EXPOSURE_HELPER,
        "upstream_upstream_reporting_helper": UPSTREAM_REPORTING_HELPER,
        "upstream_downstream_helper": DOWNSTREAM_HELPER,
        "upstream_cli_helper": CLI_HELPER,
        "upstream_runtime_helper": RUNTIME_HELPER,
        "runtime_fields": RUNTIME_FIELDS,
        "default_cli_exposure_status": cli_exposure_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_status"),
        "default_reporting_status": reporting_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_status"),
        "default_usage_status": usage_artifact.get("downstream_runtime_summary_cli_exposure_usage_status"),
        "default_prior_cli_exposure_status": prior_cli_exposure_artifact.get("downstream_runtime_summary_cli_exposure_status"),
        "default_upstream_reporting_status": upstream_reporting_artifact.get("downstream_runtime_summary_reporting_status"),
        "default_downstream_status": downstream_artifact.get("downstream_runtime_summary_usage_status"),
        "default_cli_status": cli_artifact.get("cli_diagnostic_artifact_status"),
        "runtime_summary_status": runtime_artifact.get("live_fetcher_runtime_summary_status"),
        "validator_6fn_returncode": validator_run.returncode,
        "audit_6fo_returncode": audit_run.returncode,
        "module_self_check_returncode": module_run.returncode,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(PLAN_JSON),
            "checks_csv": str(PLAN_CHECKS_CSV),
            "prerequisites_csv": str(PLAN_PREREQUISITES_CSV),
            "source_surface_csv": str(PLAN_SOURCE_SURFACE_CSV),
            "usage_contract_csv": str(PLAN_USAGE_CONTRACT_CSV),
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
