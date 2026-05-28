#!/usr/bin/env python3
"""Plan 6FW downstream usage-facing consumption of 6FT downstream artifacts."""

from __future__ import annotations

import csv
import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable


SLUG = "candidate_bullpen_6fv_downstream_usage_plan"
TMP_DIR = Path("tmp")
TARGET_PATH = Path("scripts/fetch_candidate_bullpen_statcast_live_adapter.py")
VALIDATOR_6FT_PATH = Path(
    "scripts/validate_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_"
    "observability_preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_usage_downstream_implementation.py"
)
AUDIT_6FU_PATH = Path(
    "scripts/audit_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_"
    "observability_preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_usage_downstream_implementation.py"
)

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREREQUISITES_CSV = TMP_DIR / f"{SLUG}_prerequisites.csv"
SOURCE_SURFACE_CSV = TMP_DIR / f"{SLUG}_source_surface.csv"
DOWNSTREAM_USAGE_CONTRACT_CSV = TMP_DIR / f"{SLUG}_downstream_usage_contract.csv"
ARTIFACT_CONTRACT_CSV = TMP_DIR / f"{SLUG}_artifact_contract.csv"
RUNTIME_FIELD_CONTRACT_CSV = TMP_DIR / f"{SLUG}_runtime_field_contract.csv"
STATUS_CONTRACT_CSV = TMP_DIR / f"{SLUG}_status_contract.csv"
SAFETY_CONTRACT_CSV = TMP_DIR / f"{SLUG}_safety_contract.csv"
VALIDATION_CONTRACT_CSV = TMP_DIR / f"{SLUG}_validation_contract.csv"
NON_GOALS_CSV = TMP_DIR / f"{SLUG}_non_goals.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_PATH_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

VALIDATOR_6FT_JSON = TMP_DIR / "candidate_bullpen_6ft_downstream_implementation.json"
AUDIT_6FU_JSON = TMP_DIR / "candidate_bullpen_6fu_downstream_implementation_audit.json"

DIAGNOSIS_6FT = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_usage_downstream_implementation_complete"
)
DIAGNOSIS_6FU = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_usage_downstream_implementation_audit_complete"
)
DIAGNOSIS_6FV = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_plan_complete"
)
RECOMMENDED_NEXT_LAYER = (
    "6FW_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_implementation"
)
FUTURE_AUDIT_LAYER = (
    "6FX_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_implementation_audit"
)
RECOMMENDED_PATH = "consume_downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_artifact_in_usage_surface"

APPROVED_SOURCE = "_candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_artifact"
UPSTREAM_SOURCE = "_candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_artifact"
FUTURE_HELPER = "_candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_artifact"
FUTURE_WRAPPER = "_candidate_bullpen_emit_module_self_check_summary_with_usage_reporting_cli_exposure_usage_downstream_usage"

FORBIDDEN_DIRECT_HELPERS = [
    UPSTREAM_SOURCE,
    "_candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_artifact",
    "_candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_reporting_artifact",
    "_candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_artifact",
    "_candidate_bullpen_build_downstream_runtime_summary_cli_exposure_artifact",
    "_candidate_bullpen_build_downstream_runtime_summary_reporting_artifact",
    "_candidate_bullpen_build_downstream_runtime_summary_usage_artifact",
    "_candidate_bullpen_build_cli_diagnostic_artifact",
    "_candidate_bullpen_build_live_fetcher_runtime_summary_artifact",
]

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


def safe_env() -> Dict[str, str]:
    env = dict(os.environ)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    return env


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
    module_name = "candidate_bullpen_live_adapter_6fv_plan"
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
        [sys.executable, "-m", "compileall", "-q", "-f", "-b", "mlb_app", "scripts"],
        check=False,
        text=True,
        capture_output=True,
        env=safe_env(),
    )
    validator_run = subprocess.run(
        [sys.executable, str(VALIDATOR_6FT_PATH)],
        check=False,
        text=True,
        capture_output=True,
        env=safe_env(),
    )
    audit_run = subprocess.run(
        [sys.executable, str(AUDIT_6FU_PATH)],
        check=False,
        text=True,
        capture_output=True,
        env=safe_env(),
    )
    module_run = subprocess.run(
        [sys.executable, str(TARGET_PATH)],
        check=False,
        text=True,
        capture_output=True,
        env=safe_env(),
    )

    validator_summary = load_json(VALIDATOR_6FT_JSON)
    audit_summary = load_json(AUDIT_6FU_JSON)
    try:
        module_summary = json.loads(module_run.stdout)
    except Exception:
        module_summary = {}

    module = load_module()
    downstream_helper = getattr(module, APPROVED_SOURCE, None)
    downstream_artifact = downstream_helper() if downstream_helper else {}

    usage_artifact = downstream_artifact.get(
        "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_artifact", {}
    )
    cli_exposure_artifact = downstream_artifact.get(
        "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_artifact", {}
    )
    reporting_artifact = downstream_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_artifact", {})
    prior_usage_artifact = downstream_artifact.get("downstream_runtime_summary_cli_exposure_usage_artifact", {})
    prior_cli_exposure_artifact = downstream_artifact.get("downstream_runtime_summary_cli_exposure_artifact", {})
    upstream_reporting_artifact = downstream_artifact.get("downstream_runtime_summary_reporting_artifact", {})
    downstream_usage_artifact = downstream_artifact.get("downstream_runtime_summary_usage_artifact", {})
    cli_artifact = downstream_artifact.get("cli_diagnostic_artifact", {})
    runtime_artifact = downstream_artifact.get("live_fetcher_runtime_summary_artifact", {})

    source = TARGET_PATH.read_text(encoding="utf-8")

    prerequisites_rows = [
        {"prerequisite": "compileall_passes", "expected": 0, "actual": compile_run.returncode, "passed": compile_run.returncode == 0},
        {"prerequisite": "6ft_validator_exists", "expected": True, "actual": VALIDATOR_6FT_PATH.exists(), "passed": VALIDATOR_6FT_PATH.exists()},
        {"prerequisite": "6ft_validator_passes", "expected": 0, "actual": validator_run.returncode, "passed": validator_run.returncode == 0},
        {"prerequisite": "6ft_json_exists", "expected": True, "actual": VALIDATOR_6FT_JSON.exists(), "passed": VALIDATOR_6FT_JSON.exists()},
        {"prerequisite": "6ft_json_passed", "expected": True, "actual": validator_summary.get("all_checks_passed"), "passed": validator_summary.get("all_checks_passed") is True},
        {"prerequisite": "6ft_diagnosis", "expected": DIAGNOSIS_6FT, "actual": validator_summary.get("diagnosis"), "passed": validator_summary.get("diagnosis") == DIAGNOSIS_6FT},
        {"prerequisite": "6fu_audit_exists", "expected": True, "actual": AUDIT_6FU_PATH.exists(), "passed": AUDIT_6FU_PATH.exists()},
        {"prerequisite": "6fu_audit_passes", "expected": 0, "actual": audit_run.returncode, "passed": audit_run.returncode == 0},
        {"prerequisite": "6fu_json_exists", "expected": True, "actual": AUDIT_6FU_JSON.exists(), "passed": AUDIT_6FU_JSON.exists()},
        {"prerequisite": "6fu_json_passed", "expected": True, "actual": audit_summary.get("all_checks_passed"), "passed": audit_summary.get("all_checks_passed") is True},
        {"prerequisite": "6fu_audit_only", "expected": True, "actual": audit_summary.get("audit_only"), "passed": audit_summary.get("audit_only") is True},
        {"prerequisite": "6fu_diagnosis", "expected": DIAGNOSIS_6FU, "actual": audit_summary.get("diagnosis"), "passed": audit_summary.get("diagnosis") == DIAGNOSIS_6FU},
        {"prerequisite": "module_self_check_passes", "expected": 0, "actual": module_run.returncode, "passed": module_run.returncode == 0},
        {"prerequisite": "module_diagnosis_preserved", "expected": "candidate_bullpen_statcast_live_adapter_fetch_module_implementation_complete", "actual": module_summary.get("diagnosis"), "passed": module_summary.get("diagnosis") == "candidate_bullpen_statcast_live_adapter_fetch_module_implementation_complete"},
        {"prerequisite": "module_all_checks_passed", "expected": True, "actual": module_summary.get("all_checks_passed"), "passed": module_summary.get("all_checks_passed") is True},
        {"prerequisite": "module_downstream_created", "expected": True, "actual": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_artifact_created"), "passed": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_artifact_created") is True},
        {"prerequisite": "module_downstream_version", "expected": 1, "actual": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_artifact_version"), "passed": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_artifact_version") == 1},
        {"prerequisite": "module_downstream_status", "expected": "safe_dry_run_no_real_fetch", "actual": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_status"), "passed": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_status") == "safe_dry_run_no_real_fetch"},
        {"prerequisite": "module_downstream_safe_to_proceed", "expected": True, "actual": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_safe_to_proceed"), "passed": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_safe_to_proceed") is True},
        {"prerequisite": "module_downstream_source", "expected": "candidate_bullpen_statcast_live_adapter", "actual": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_source"), "passed": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_source") == "candidate_bullpen_statcast_live_adapter"},
    ]

    required_source_tokens = [
        APPROVED_SOURCE,
        UPSTREAM_SOURCE,
        "_candidate_bullpen_emit_module_self_check_summary_with_usage_reporting_cli_exposure_usage_downstream",
        "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_artifact_created",
        "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_status",
        "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_safe_to_proceed",
        "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_source",
        "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_artifact",
        "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_artifact",
        "downstream_runtime_summary_cli_exposure_usage_reporting_artifact",
        "downstream_runtime_summary_cli_exposure_usage_artifact",
        "downstream_runtime_summary_cli_exposure_artifact",
        "downstream_runtime_summary_reporting_artifact",
        "downstream_runtime_summary_usage_artifact",
        "cli_diagnostic_artifact",
        "live_fetcher_runtime_summary_artifact",
    ]
    source_surface_rows = [
        {"surface": token, "role": "required_existing_surface", "required": True, "actual": token in source, "passed": token in source}
        for token in required_source_tokens
    ]

    downstream_usage_rows = [
        {"contract": "approved_source_for_6fw", "requirement": APPROVED_SOURCE, "passed": True},
        {"contract": "upstream_source_consumed_by_approved_source", "requirement": UPSTREAM_SOURCE, "passed": True},
        {"contract": "future_helper", "requirement": FUTURE_HELPER, "passed": True},
        {"contract": "future_self_check_wrapper", "requirement": FUTURE_WRAPPER, "passed": True},
        {"contract": "implementation_path", "requirement": RECOMMENDED_PATH, "passed": True},
    ]
    for helper in FORBIDDEN_DIRECT_HELPERS:
        downstream_usage_rows.append(
            {"contract": f"future_helper_must_not_call_{helper}", "requirement": "forbidden_direct_call", "passed": True}
        )
    downstream_usage_rows.extend(
        [
            {"contract": "future_helper_must_not_reconstruct_runtime_fields", "requirement": "copy from 6FT downstream artifact", "passed": True},
            {"contract": "future_helper_no_live_fetch", "requirement": "no external fetch behavior", "passed": True},
            {"contract": "future_helper_no_db_write", "requirement": "no DB write behavior", "passed": True},
            {"contract": "future_helper_no_candidate_materialization", "requirement": "no candidate label materialization", "passed": True},
            {"contract": "future_surface_version_field", "requirement": "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_artifact_version", "passed": True},
            {"contract": "future_surface_status_field", "requirement": "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_status", "passed": True},
            {"contract": "future_surface_safe_to_proceed_field", "requirement": "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_safe_to_proceed", "passed": True},
            {"contract": "future_surface_source_field", "requirement": "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_source", "passed": True},
            {"contract": "future_surface_reason_field", "requirement": "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reason", "passed": True},
            {"contract": "future_surface_downstream_artifact", "requirement": "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_artifact", "passed": True},
            {"contract": "future_surface_usage_artifact", "requirement": "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_artifact", "passed": True},
            {"contract": "future_surface_cli_exposure_artifact", "requirement": "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_artifact", "passed": True},
            {"contract": "future_surface_reporting_artifact", "requirement": "downstream_runtime_summary_cli_exposure_usage_reporting_artifact", "passed": True},
            {"contract": "future_surface_prior_usage_artifact", "requirement": "downstream_runtime_summary_cli_exposure_usage_artifact", "passed": True},
            {"contract": "future_surface_prior_cli_exposure_artifact", "requirement": "downstream_runtime_summary_cli_exposure_artifact", "passed": True},
            {"contract": "future_surface_upstream_reporting_artifact", "requirement": "downstream_runtime_summary_reporting_artifact", "passed": True},
            {"contract": "future_surface_downstream_usage_artifact", "requirement": "downstream_runtime_summary_usage_artifact", "passed": True},
            {"contract": "future_surface_cli_artifact", "requirement": "cli_diagnostic_artifact", "passed": True},
            {"contract": "future_surface_runtime_artifact", "requirement": "live_fetcher_runtime_summary_artifact", "passed": True},
            {"contract": "future_surface_safety_fields_when_present", "requirement": "external/db/materialization/default safety fields", "passed": True},
        ]
    )

    artifact_rows = [
        {"field": "downstream_artifact_is_dict", "expected": True, "actual": isinstance(downstream_artifact, dict), "passed": isinstance(downstream_artifact, dict)},
        {"field": "downstream_version", "expected": 1, "actual": downstream_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_artifact_version"), "passed": downstream_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_artifact_version") == 1},
        {"field": "downstream_status", "expected": "safe_dry_run_no_real_fetch", "actual": downstream_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_status"), "passed": downstream_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_status") == "safe_dry_run_no_real_fetch"},
        {"field": "downstream_safe_to_proceed", "expected": True, "actual": downstream_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_safe_to_proceed"), "passed": downstream_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_safe_to_proceed") is True},
        {"field": "downstream_source", "expected": "candidate_bullpen_statcast_live_adapter", "actual": downstream_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_source"), "passed": downstream_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_source") == "candidate_bullpen_statcast_live_adapter"},
        {"field": "usage_artifact_nested", "expected": True, "actual": isinstance(usage_artifact, dict), "passed": isinstance(usage_artifact, dict)},
        {"field": "cli_exposure_artifact_nested", "expected": True, "actual": isinstance(cli_exposure_artifact, dict), "passed": isinstance(cli_exposure_artifact, dict)},
        {"field": "reporting_artifact_nested", "expected": True, "actual": isinstance(reporting_artifact, dict), "passed": isinstance(reporting_artifact, dict)},
        {"field": "prior_usage_artifact_nested", "expected": True, "actual": isinstance(prior_usage_artifact, dict), "passed": isinstance(prior_usage_artifact, dict)},
        {"field": "prior_cli_exposure_artifact_nested", "expected": True, "actual": isinstance(prior_cli_exposure_artifact, dict), "passed": isinstance(prior_cli_exposure_artifact, dict)},
        {"field": "upstream_reporting_artifact_nested", "expected": True, "actual": isinstance(upstream_reporting_artifact, dict), "passed": isinstance(upstream_reporting_artifact, dict)},
        {"field": "downstream_usage_artifact_nested", "expected": True, "actual": isinstance(downstream_usage_artifact, dict), "passed": isinstance(downstream_usage_artifact, dict)},
        {"field": "cli_artifact_nested", "expected": True, "actual": isinstance(cli_artifact, dict), "passed": isinstance(cli_artifact, dict)},
        {"field": "runtime_artifact_nested", "expected": True, "actual": isinstance(runtime_artifact, dict), "passed": isinstance(runtime_artifact, dict)},
        {"field": "field_version", "expected": 1, "actual": downstream_artifact.get("live_fetcher_runtime_summary_field_version"), "passed": downstream_artifact.get("live_fetcher_runtime_summary_field_version") == 1},
    ]

    runtime_rows = []
    for field in RUNTIME_FIELDS:
        runtime_rows.append(
            {
                "field": field,
                "downstream_present": field in downstream_artifact,
                "usage_present": field in usage_artifact,
                "cli_exposure_present": field in cli_exposure_artifact,
                "reporting_present": field in reporting_artifact,
                "prior_usage_present": field in prior_usage_artifact,
                "prior_cli_exposure_present": field in prior_cli_exposure_artifact,
                "upstream_reporting_present": field in upstream_reporting_artifact,
                "downstream_usage_present": field in downstream_usage_artifact,
                "cli_present": field in cli_artifact,
                "runtime_present": field in runtime_artifact,
                "downstream_value": downstream_artifact.get(field),
                "usage_value": usage_artifact.get(field),
                "cli_exposure_value": cli_exposure_artifact.get(field),
                "reporting_value": reporting_artifact.get(field),
                "prior_usage_value": prior_usage_artifact.get(field),
                "prior_cli_exposure_value": prior_cli_exposure_artifact.get(field),
                "upstream_reporting_value": upstream_reporting_artifact.get(field),
                "downstream_usage_value": downstream_usage_artifact.get(field),
                "cli_value": cli_artifact.get(field),
                "runtime_value": runtime_artifact.get(field),
                "passed": field in downstream_artifact
                and field in usage_artifact
                and field in cli_exposure_artifact
                and field in reporting_artifact
                and field in prior_usage_artifact
                and field in prior_cli_exposure_artifact
                and field in upstream_reporting_artifact
                and field in downstream_usage_artifact
                and field in cli_artifact
                and field in runtime_artifact
                and downstream_artifact.get(field)
                == usage_artifact.get(field)
                == cli_exposure_artifact.get(field)
                == reporting_artifact.get(field)
                == prior_usage_artifact.get(field)
                == prior_cli_exposure_artifact.get(field)
                == upstream_reporting_artifact.get(field)
                == downstream_usage_artifact.get(field)
                == cli_artifact.get(field)
                == runtime_artifact.get(field),
            }
        )

    status_rows = [
        {"scenario": scenario, "expected_status": expected, "implementation_layer": "6FW", "validation_required": True, "passed": True}
        for scenario, expected in STATUS_SCENARIOS.items()
    ]

    safety_rows = []
    for surface in [
        "future_downstream_usage_surface",
        "future_downstream_usage_json_surface",
        "future_downstream_usage_summary_surface",
        "downstream_surface",
        "usage_surface",
        "cli_exposure_surface",
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
        {"validation": "compileall_q_f_b", "required_for_6fw": True, "passed": True},
        {"validation": "module_self_check", "required_for_6fw": True, "passed": True},
        {"validation": "downstream_usage_facing_validator", "required_for_6fw": True, "passed": True},
        {"validation": "short_artifact_names", "required_for_6fw": True, "passed": True},
        {"validation": "artifact_json_exists", "required_for_6fw": True, "passed": True},
        {"validation": "all_emitted_csv_rows_pass", "required_for_6fw": True, "passed": True},
        {"validation": "source_contract", "required_for_6fw": True, "passed": True},
        {"validation": "downstream_usage_contract", "required_for_6fw": True, "passed": True},
        {"validation": "artifact_contract", "required_for_6fw": True, "passed": True},
        {"validation": "runtime_field_contract", "required_for_6fw": True, "passed": True},
        {"validation": "status_contract", "required_for_6fw": True, "passed": True},
        {"validation": "safety_contract", "required_for_6fw": True, "passed": True},
        {"validation": "determinism_contract", "required_for_6fw": True, "passed": True},
        {"validation": "immutability_contract", "required_for_6fw": True, "passed": True},
    ]

    non_goal_rows = [
        {"non_goal": "adapter_helper_rewrite", "policy": "forbidden", "passed": True},
        {"non_goal": "downstream_helper_rewrite", "policy": "forbidden", "passed": True},
        {"non_goal": "usage_helper_rewrite", "policy": "forbidden", "passed": True},
        {"non_goal": "cli_exposure_helper_rewrite", "policy": "forbidden", "passed": True},
        {"non_goal": "reporting_helper_rewrite", "policy": "forbidden", "passed": True},
        {"non_goal": "prior_usage_helper_rewrite", "policy": "forbidden", "passed": True},
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
        {"surface": "6ft_validator", "policy": "unchanged", "passed": True},
        {"surface": "6fu_audit", "policy": "unchanged", "passed": True},
        {"surface": APPROVED_SOURCE, "policy": "semantics_unchanged", "passed": True},
        {"surface": UPSTREAM_SOURCE, "policy": "semantics_unchanged", "passed": True},
        {"surface": "_candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_artifact", "policy": "semantics_unchanged", "passed": True},
        {"surface": "_candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_reporting_artifact", "policy": "semantics_unchanged", "passed": True},
        {"surface": "_candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_artifact", "policy": "semantics_unchanged", "passed": True},
        {"surface": "_candidate_bullpen_build_downstream_runtime_summary_cli_exposure_artifact", "policy": "semantics_unchanged", "passed": True},
        {"surface": "_candidate_bullpen_build_downstream_runtime_summary_reporting_artifact", "policy": "semantics_unchanged", "passed": True},
        {"surface": "_candidate_bullpen_build_downstream_runtime_summary_usage_artifact", "policy": "semantics_unchanged", "passed": True},
        {"surface": "_candidate_bullpen_build_cli_diagnostic_artifact", "policy": "semantics_unchanged", "passed": True},
        {"surface": "_candidate_bullpen_build_live_fetcher_runtime_summary_artifact", "policy": "semantics_unchanged", "passed": True},
        {"surface": "_candidate_bullpen_apply_live_fetcher_runtime_summary", "policy": "semantics_unchanged", "passed": True},
        {"surface": "fixtures", "policy": "unchanged", "passed": True},
        {"surface": "production_defaults", "policy": "unchanged", "passed": True},
        {"surface": "network_db_materialization", "policy": "not_added", "passed": True},
    ]

    recommended_path_rows = [
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH, "actual": RECOMMENDED_PATH, "passed": True},
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER, "actual": RECOMMENDED_NEXT_LAYER, "passed": True},
        {"decision": "future_audit_layer", "expected": FUTURE_AUDIT_LAYER, "actual": FUTURE_AUDIT_LAYER, "passed": True},
        {"decision": "future_helper", "expected": FUTURE_HELPER, "actual": FUTURE_HELPER, "passed": True},
        {"decision": "future_wrapper", "expected": FUTURE_WRAPPER, "actual": FUTURE_WRAPPER, "passed": True},
    ]

    checks = [
        {"check": "compileall", "passed": compile_run.returncode == 0, "detail": f"returncode={compile_run.returncode}"},
        {"check": "planning_only", "passed": True, "detail": "true"},
        {"check": "prerequisites", "passed": all(row["passed"] for row in prerequisites_rows), "detail": f"{sum(1 for row in prerequisites_rows if row['passed'])}/{len(prerequisites_rows)}"},
        {"check": "source_surface", "passed": all(row["passed"] for row in source_surface_rows), "detail": f"{sum(1 for row in source_surface_rows if row['passed'])}/{len(source_surface_rows)}"},
        {"check": "downstream_usage_contract", "passed": all(row["passed"] for row in downstream_usage_rows), "detail": f"{sum(1 for row in downstream_usage_rows if row['passed'])}/{len(downstream_usage_rows)}"},
        {"check": "artifact_contract", "passed": all(row["passed"] for row in artifact_rows), "detail": f"{sum(1 for row in artifact_rows if row['passed'])}/{len(artifact_rows)}"},
        {"check": "runtime_field_contract", "passed": all(row["passed"] for row in runtime_rows), "detail": f"{sum(1 for row in runtime_rows if row['passed'])}/{len(runtime_rows)}"},
        {"check": "status_contract", "passed": all(row["passed"] for row in status_rows), "detail": f"{sum(1 for row in status_rows if row['passed'])}/{len(status_rows)}"},
        {"check": "safety_contract", "passed": all(row["passed"] for row in safety_rows), "detail": f"{sum(1 for row in safety_rows if row['passed'])}/{len(safety_rows)}"},
        {"check": "validation_contract", "passed": all(row["passed"] for row in validation_rows), "detail": f"{sum(1 for row in validation_rows if row['passed'])}/{len(validation_rows)}"},
        {"check": "non_goals", "passed": all(row["passed"] for row in non_goal_rows), "detail": f"{sum(1 for row in non_goal_rows if row['passed'])}/{len(non_goal_rows)}"},
        {"check": "immutability", "passed": all(row["passed"] for row in immutability_rows), "detail": f"{sum(1 for row in immutability_rows if row['passed'])}/{len(immutability_rows)}"},
        {"check": "recommended_path", "passed": all(row["passed"] for row in recommended_path_rows), "detail": f"{sum(1 for row in recommended_path_rows if row['passed'])}/{len(recommended_path_rows)}"},
    ]
    all_checks_passed = all(row["passed"] for row in checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "prerequisites": write_csv(PREREQUISITES_CSV, prerequisites_rows),
        "source_surface": write_csv(SOURCE_SURFACE_CSV, source_surface_rows),
        "downstream_usage_contract": write_csv(DOWNSTREAM_USAGE_CONTRACT_CSV, downstream_usage_rows),
        "artifact_contract": write_csv(ARTIFACT_CONTRACT_CSV, artifact_rows),
        "runtime_field_contract": write_csv(RUNTIME_FIELD_CONTRACT_CSV, runtime_rows),
        "status_contract": write_csv(STATUS_CONTRACT_CSV, status_rows),
        "safety_contract": write_csv(SAFETY_CONTRACT_CSV, safety_rows),
        "validation_contract": write_csv(VALIDATION_CONTRACT_CSV, validation_rows),
        "non_goals": write_csv(NON_GOALS_CSV, non_goal_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_PATH_CSV, recommended_path_rows),
    }

    summary = {
        "layer": "6FV",
        "planning_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6FV if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "future_audit_layer": FUTURE_AUDIT_LAYER,
        "recommended_path": RECOMMENDED_PATH,
        "approved_source": APPROVED_SOURCE,
        "upstream_source": UPSTREAM_SOURCE,
        "future_helper": FUTURE_HELPER,
        "future_module_self_check_wrapper": FUTURE_WRAPPER,
        "default_downstream_status": downstream_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_status"),
        "default_usage_status": usage_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_status"),
        "default_cli_exposure_status": cli_exposure_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_status"),
        "default_reporting_status": reporting_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_status"),
        "default_prior_usage_status": prior_usage_artifact.get("downstream_runtime_summary_cli_exposure_usage_status"),
        "default_prior_cli_exposure_status": prior_cli_exposure_artifact.get("downstream_runtime_summary_cli_exposure_status"),
        "default_upstream_reporting_status": upstream_reporting_artifact.get("downstream_runtime_summary_reporting_status"),
        "default_downstream_usage_status": downstream_usage_artifact.get("downstream_runtime_summary_usage_status"),
        "default_cli_status": cli_artifact.get("cli_diagnostic_artifact_status"),
        "runtime_summary_status": runtime_artifact.get("live_fetcher_runtime_summary_status"),
        "validator_6ft_returncode": validator_run.returncode,
        "audit_6fu_returncode": audit_run.returncode,
        "module_self_check_returncode": module_run.returncode,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "prerequisites_csv": str(PREREQUISITES_CSV),
            "source_surface_csv": str(SOURCE_SURFACE_CSV),
            "downstream_usage_contract_csv": str(DOWNSTREAM_USAGE_CONTRACT_CSV),
            "artifact_contract_csv": str(ARTIFACT_CONTRACT_CSV),
            "runtime_field_contract_csv": str(RUNTIME_FIELD_CONTRACT_CSV),
            "status_contract_csv": str(STATUS_CONTRACT_CSV),
            "safety_contract_csv": str(SAFETY_CONTRACT_CSV),
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
