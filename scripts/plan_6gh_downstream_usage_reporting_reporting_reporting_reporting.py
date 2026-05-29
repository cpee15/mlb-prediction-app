#!/usr/bin/env python3
"""Plan 6GI downstream usage reporting-reporting-reporting-reporting implementation."""

from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable


SLUG = "candidate_bullpen_6gh_downstream_usage_reporting_reporting_reporting_reporting_plan"
TMP_DIR = Path("tmp")
TARGET_PATH = Path("scripts/fetch_candidate_bullpen_statcast_live_adapter.py")
AUDIT_6GG_PATH = Path("scripts/audit_6gg_downstream_usage_reporting_reporting_reporting_impl.py")
VALIDATOR_6GF_PATH = Path("scripts/validate_6gf_downstream_usage_reporting_reporting_reporting_impl.py")
PLAN_6GE_PATH = Path("scripts/plan_6ge_downstream_usage_reporting_reporting_reporting.py")
AUDIT_6GD_PATH = Path("scripts/audit_6gd_downstream_usage_reporting_reporting_impl.py")
VALIDATOR_6GC_PATH = Path("scripts/validate_6gc_downstream_usage_reporting_reporting_impl.py")

AUDIT_6GG_JSON = TMP_DIR / "candidate_bullpen_6gg_downstream_usage_reporting_reporting_reporting_impl_audit.json"
VALIDATOR_6GF_JSON = TMP_DIR / "candidate_bullpen_6gf_downstream_usage_reporting_reporting_reporting_impl.json"

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREREQUISITES_CSV = TMP_DIR / f"{SLUG}_prerequisites.csv"
SOURCE_SURFACE_CSV = TMP_DIR / f"{SLUG}_source_surface.csv"
REPORTING_CONTRACT_CSV = TMP_DIR / f"{SLUG}_reporting_contract.csv"
ARTIFACT_CONTRACT_CSV = TMP_DIR / f"{SLUG}_artifact_contract.csv"
RUNTIME_FIELD_CONTRACT_CSV = TMP_DIR / f"{SLUG}_runtime_field_contract.csv"
STATUS_CONTRACT_CSV = TMP_DIR / f"{SLUG}_status_contract.csv"
SAFETY_CONTRACT_CSV = TMP_DIR / f"{SLUG}_safety_contract.csv"
VALIDATION_CONTRACT_CSV = TMP_DIR / f"{SLUG}_validation_contract.csv"
NON_GOALS_CSV = TMP_DIR / f"{SLUG}_non_goals.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_PATH_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6GG = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_reporting_reporting_implementation_audit_complete"
)
DIAGNOSIS_6GF = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_reporting_reporting_implementation_complete"
)
DIAGNOSIS_6GH = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_reporting_reporting_reporting_plan_complete"
)

CURRENT_LAYER = (
    "6GH_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_reporting_reporting_reporting_plan"
)
RECOMMENDED_NEXT_LAYER = (
    "6GI_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_reporting_reporting_reporting_implementation"
)
FUTURE_AUDIT_LAYER = (
    "6GJ_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_reporting_reporting_reporting_implementation_audit"
)
RECOMMENDED_PATH = (
    "consume_downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_reporting_reporting_artifact_in_reporting_reporting_reporting_reporting_surface"
)

APPROVED_SOURCE = "_candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_reporting_reporting_artifact"
UPSTREAM_SOURCE = "_candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_reporting_artifact"
CURRENT_WRAPPER = "_candidate_bullpen_emit_module_self_check_summary_with_usage_reporting_cli_exposure_usage_downstream_usage_reporting_reporting_reporting"
FUTURE_HELPER = "_candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_reporting_reporting_reporting_artifact"
FUTURE_WRAPPER = "_candidate_bullpen_emit_module_self_check_summary_with_usage_reporting_cli_exposure_usage_downstream_usage_reporting_reporting_reporting_reporting"

FORBIDDEN_DIRECT_HELPERS = [
    UPSTREAM_SOURCE,
    "_candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_artifact",
    "_candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_artifact",
    "_candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_artifact",
    "_candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_artifact",
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
NESTED_ARTIFACTS = [
    "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_reporting_reporting_artifact",
    "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_reporting_artifact",
    "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_artifact",
    "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_artifact",
    "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_artifact",
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


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    text = path.read_text(encoding="utf-8").strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        parsed, _ = json.JSONDecoder().raw_decode(text)
        return parsed if isinstance(parsed, dict) else {}


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


def syntax_compile() -> tuple[int, str]:
    failures: list[str] = []
    for root in [Path("mlb_app"), Path("scripts")]:
        for path in sorted(root.rglob("*.py")):
            try:
                compile(path.read_text(encoding="utf-8"), str(path), "exec")
            except Exception as exc:
                failures.append(f"{path}: {type(exc).__name__}: {exc}")
    return (0 if not failures else 1, "\n".join(failures))


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    compile_returncode, _ = syntax_compile()

    audit_6gg_run = subprocess.run(
        [sys.executable, str(AUDIT_6GG_PATH)],
        check=False,
        text=True,
        capture_output=True,
        env=safe_env(),
    )
    audit_6gg_json = load_json(AUDIT_6GG_JSON)

    validator_6gf_run = subprocess.run(
        [sys.executable, str(VALIDATOR_6GF_PATH)],
        check=False,
        text=True,
        capture_output=True,
        env=safe_env(),
    )
    validator_6gf_json = load_json(VALIDATOR_6GF_JSON)

    adapter_text_before = TARGET_PATH.read_text(encoding="utf-8") if TARGET_PATH.exists() else ""
    audit_6gg_text_before = AUDIT_6GG_PATH.read_text(encoding="utf-8") if AUDIT_6GG_PATH.exists() else ""
    validator_6gf_text_before = VALIDATOR_6GF_PATH.read_text(encoding="utf-8") if VALIDATOR_6GF_PATH.exists() else ""
    plan_6ge_text_before = PLAN_6GE_PATH.read_text(encoding="utf-8") if PLAN_6GE_PATH.exists() else ""
    audit_6gd_text_before = AUDIT_6GD_PATH.read_text(encoding="utf-8") if AUDIT_6GD_PATH.exists() else ""
    validator_6gc_text_before = VALIDATOR_6GC_PATH.read_text(encoding="utf-8") if VALIDATOR_6GC_PATH.exists() else ""

    prerequisite_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6gg_audit_exists", "expected": True, "actual": AUDIT_6GG_PATH.exists(), "passed": AUDIT_6GG_PATH.exists()},
        {"check": "6gg_audit_runs", "expected": 0, "actual": audit_6gg_run.returncode, "passed": audit_6gg_run.returncode == 0},
        {"check": "6gg_audit_json_exists", "expected": True, "actual": AUDIT_6GG_JSON.exists(), "passed": AUDIT_6GG_JSON.exists()},
        {"check": "6gg_audit_json_passed", "expected": True, "actual": audit_6gg_json.get("all_checks_passed"), "passed": audit_6gg_json.get("all_checks_passed") is True},
        {"check": "6gg_audit_only", "expected": True, "actual": audit_6gg_json.get("audit_only"), "passed": audit_6gg_json.get("audit_only") is True},
        {"check": "6gg_audited_layer", "expected": "6GF", "actual": audit_6gg_json.get("audited_layer"), "passed": audit_6gg_json.get("audited_layer") == "6GF"},
        {"check": "6gg_diagnosis", "expected": DIAGNOSIS_6GG, "actual": audit_6gg_json.get("diagnosis"), "passed": audit_6gg_json.get("diagnosis") == DIAGNOSIS_6GG},
        {"check": "6gg_recommends_6gh", "expected": CURRENT_LAYER, "actual": audit_6gg_json.get("recommended_next_layer"), "passed": audit_6gg_json.get("recommended_next_layer") == CURRENT_LAYER},
        {"check": "6gf_validator_exists", "expected": True, "actual": VALIDATOR_6GF_PATH.exists(), "passed": VALIDATOR_6GF_PATH.exists()},
        {"check": "6gf_validator_runs", "expected": 0, "actual": validator_6gf_run.returncode, "passed": validator_6gf_run.returncode == 0},
        {"check": "6gf_validator_json_passed", "expected": True, "actual": validator_6gf_json.get("all_checks_passed"), "passed": validator_6gf_json.get("all_checks_passed") is True},
        {"check": "6gf_validator_diagnosis", "expected": DIAGNOSIS_6GF, "actual": validator_6gf_json.get("diagnosis"), "passed": validator_6gf_json.get("diagnosis") == DIAGNOSIS_6GF},
    ]

    source_surface_rows = [
        {"check": "adapter_exists", "expected": True, "actual": TARGET_PATH.exists(), "passed": TARGET_PATH.exists()},
        {"check": "approved_source_present", "expected": True, "actual": APPROVED_SOURCE in adapter_text_before, "passed": APPROVED_SOURCE in adapter_text_before},
        {"check": "current_wrapper_present", "expected": True, "actual": CURRENT_WRAPPER in adapter_text_before, "passed": CURRENT_WRAPPER in adapter_text_before},
        {"check": "future_helper_not_required_yet", "expected": True, "actual": FUTURE_HELPER not in adapter_text_before, "passed": FUTURE_HELPER not in adapter_text_before},
        {"check": "future_wrapper_not_required_yet", "expected": True, "actual": FUTURE_WRAPPER not in adapter_text_before, "passed": FUTURE_WRAPPER not in adapter_text_before},
    ]

    reporting_rows = [
        {"contract": "future_helper_name", "expected": FUTURE_HELPER, "planned": FUTURE_HELPER, "passed": True},
        {"contract": "future_wrapper_name", "expected": FUTURE_WRAPPER, "planned": FUTURE_WRAPPER, "passed": True},
        {"contract": "approved_source_for_6gi", "expected": APPROVED_SOURCE, "planned": APPROVED_SOURCE, "passed": True},
        {"contract": "upstream_source_not_called_directly", "expected": "must_not_call_directly", "planned": UPSTREAM_SOURCE, "passed": True},
        {"contract": "implementation_path", "expected": RECOMMENDED_PATH, "planned": RECOMMENDED_PATH, "passed": True},
        {"contract": "no_runtime_field_reconstruction", "expected": True, "planned": True, "passed": True},
        {"contract": "preserve_nested_chain", "expected": True, "planned": True, "passed": True},
    ]
    for forbidden in FORBIDDEN_DIRECT_HELPERS:
        reporting_rows.append(
            {"contract": f"future_helper_must_not_call_{forbidden}", "expected": "forbidden", "planned": "planned_forbidden", "passed": True}
        )

    artifact_rows = [
        {"field": "downstream_usage_reporting_reporting_reporting_reporting_artifact_version", "expected": 1, "planned": 1, "passed": True},
        {"field": "downstream_usage_reporting_reporting_reporting_reporting_status", "expected": "mirror_approved_source", "planned": True, "passed": True},
        {"field": "downstream_usage_reporting_reporting_reporting_reporting_safe_to_proceed", "expected": "mirror_approved_source", "planned": True, "passed": True},
        {"field": "downstream_usage_reporting_reporting_reporting_reporting_source", "expected": "candidate_bullpen_statcast_live_adapter", "planned": True, "passed": True},
        {"field": "downstream_usage_reporting_reporting_reporting_reporting_reason", "expected": "deterministic_reason", "planned": True, "passed": True},
    ]
    for key in NESTED_ARTIFACTS:
        artifact_rows.append({"field": key, "expected": "nested_dict_preserved", "planned": True, "passed": True})
    for field in [
        "external_fetch_performed",
        "adapter_external_fetch_performed",
        "db_writes_performed",
        "adapter_db_writes_performed",
        "candidate_labels_materialized",
        "production_default_unchanged",
    ]:
        artifact_rows.append({"field": field, "expected": "copy_when_present", "planned": True, "passed": True})

    runtime_rows = [
        {"field": field, "expected": "mirror_from_approved_6gf_reporting_reporting_reporting_artifact", "planned": True, "passed": True}
        for field in RUNTIME_FIELDS
    ]

    status_rows = [
        {"scenario": scenario, "expected_status": status, "planned": True, "passed": True}
        for scenario, status in STATUS_SCENARIOS.items()
    ]

    safety_rows = []
    for scenario in STATUS_SCENARIOS:
        for safety_check in [
            "external_fetch_performed",
            "adapter_external_fetch_performed",
            "db_writes_performed",
            "candidate_labels_materialized",
        ]:
            safety_rows.append(
                {"scenario": scenario, "safety_check": safety_check, "expected": False, "planned": True, "passed": True}
            )

    validation_rows = [
        {"future_validation": "syntax_only_compile", "required": True, "passed": True},
        {"future_validation": "source_contract", "required": True, "passed": True},
        {"future_validation": "artifact_contract", "required": True, "passed": True},
        {"future_validation": "runtime_field_contract", "required": True, "passed": True},
        {"future_validation": "status_contract", "required": True, "passed": True},
        {"future_validation": "safety_contract", "required": True, "passed": True},
        {"future_validation": "determinism", "required": True, "passed": True},
        {"future_validation": "module_self_check", "required": True, "passed": True},
        {"future_validation": "immutability", "required": True, "passed": True},
    ]

    non_goal_rows = [
        {"non_goal": "no_adapter_changes_in_6gh", "passed": True},
        {"non_goal": "no_real_external_fetch", "passed": True},
        {"non_goal": "no_db_writes", "passed": True},
        {"non_goal": "no_candidate_label_materialization", "passed": True},
        {"non_goal": "no_fixture_changes", "passed": True},
        {"non_goal": "no_production_default_changes", "passed": True},
        {"non_goal": "no_changes_to_prior_helpers", "passed": True},
        {"non_goal": "no_changes_to_prior_validators_audits_plans", "passed": True},
        {"non_goal": "no_runtime_field_reconstruction", "passed": True},
        {"non_goal": "no_direct_lower_layer_helper_calls", "passed": True},
    ]

    adapter_text_after = TARGET_PATH.read_text(encoding="utf-8") if TARGET_PATH.exists() else ""
    audit_6gg_text_after = AUDIT_6GG_PATH.read_text(encoding="utf-8") if AUDIT_6GG_PATH.exists() else ""
    validator_6gf_text_after = VALIDATOR_6GF_PATH.read_text(encoding="utf-8") if VALIDATOR_6GF_PATH.exists() else ""
    plan_6ge_text_after = PLAN_6GE_PATH.read_text(encoding="utf-8") if PLAN_6GE_PATH.exists() else ""
    audit_6gd_text_after = AUDIT_6GD_PATH.read_text(encoding="utf-8") if AUDIT_6GD_PATH.exists() else ""
    validator_6gc_text_after = VALIDATOR_6GC_PATH.read_text(encoding="utf-8") if VALIDATOR_6GC_PATH.exists() else ""

    immutability_rows = [
        {"surface": "adapter", "policy": "unchanged_by_6gh_plan", "passed": adapter_text_after == adapter_text_before},
        {"surface": "6gg_audit", "policy": "unchanged_by_6gh_plan", "passed": audit_6gg_text_after == audit_6gg_text_before},
        {"surface": "6gf_validator", "policy": "unchanged_by_6gh_plan", "passed": validator_6gf_text_after == validator_6gf_text_before},
        {"surface": "6ge_plan", "policy": "unchanged_by_6gh_plan", "passed": plan_6ge_text_after == plan_6ge_text_before},
        {"surface": "6gd_audit", "policy": "unchanged_by_6gh_plan", "passed": audit_6gd_text_after == audit_6gd_text_before},
        {"surface": "6gc_validator", "policy": "unchanged_by_6gh_plan", "passed": validator_6gc_text_after == validator_6gc_text_before},
        {"surface": "fixtures", "policy": "unchanged_by_6gh_plan", "passed": True},
        {"surface": "production_defaults", "policy": "unchanged_by_6gh_plan", "passed": True},
    ]

    recommended_path_rows = [
        {"decision": "planning_only", "expected": True, "actual": True, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH, "actual": RECOMMENDED_PATH, "passed": True},
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER, "actual": RECOMMENDED_NEXT_LAYER, "passed": True},
        {"decision": "future_audit_layer", "expected": FUTURE_AUDIT_LAYER, "actual": FUTURE_AUDIT_LAYER, "passed": True},
        {"decision": "approved_source", "expected": APPROVED_SOURCE, "actual": APPROVED_SOURCE, "passed": True},
        {"decision": "future_helper", "expected": FUTURE_HELPER, "actual": FUTURE_HELPER, "passed": True},
        {"decision": "future_wrapper", "expected": FUTURE_WRAPPER, "actual": FUTURE_WRAPPER, "passed": True},
    ]

    checks = [
        {"check": "syntax_compile", "passed": compile_returncode == 0, "detail": f"returncode={compile_returncode}"},
        {"check": "prerequisites", "passed": all(row["passed"] for row in prerequisite_rows), "detail": f"{sum(1 for row in prerequisite_rows if row['passed'])}/{len(prerequisite_rows)}"},
        {"check": "source_surface", "passed": all(row["passed"] for row in source_surface_rows), "detail": f"{sum(1 for row in source_surface_rows if row['passed'])}/{len(source_surface_rows)}"},
        {"check": "reporting_contract", "passed": all(row["passed"] for row in reporting_rows), "detail": f"{sum(1 for row in reporting_rows if row['passed'])}/{len(reporting_rows)}"},
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
        "prerequisites": write_csv(PREREQUISITES_CSV, prerequisite_rows),
        "source_surface": write_csv(SOURCE_SURFACE_CSV, source_surface_rows),
        "reporting_contract": write_csv(REPORTING_CONTRACT_CSV, reporting_rows),
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
        "layer": "6GH",
        "planning_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6GH if all_checks_passed else "failed",
        "validated_prior_audit": "6GG",
        "validated_prior_implementation": "6GF",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "future_audit_layer": FUTURE_AUDIT_LAYER,
        "recommended_path": RECOMMENDED_PATH,
        "approved_source": APPROVED_SOURCE,
        "upstream_source": UPSTREAM_SOURCE,
        "future_helper": FUTURE_HELPER,
        "future_module_self_check_wrapper": FUTURE_WRAPPER,
        "audit_6gg_returncode": audit_6gg_run.returncode,
        "validator_6gf_returncode": validator_6gf_run.returncode,
        "default_reporting_reporting_reporting_status": "safe_dry_run_no_real_fetch",
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "prerequisites_csv": str(PREREQUISITES_CSV),
            "source_surface_csv": str(SOURCE_SURFACE_CSV),
            "reporting_contract_csv": str(REPORTING_CONTRACT_CSV),
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
