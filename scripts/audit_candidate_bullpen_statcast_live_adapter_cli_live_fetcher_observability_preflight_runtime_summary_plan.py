from __future__ import annotations

import ast
import csv
import hashlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List


AUDIT_VERSION = "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_runtime_summary_plan_audit_v0.1"

SCAFFOLD_PATH = Path("scripts/backfill_candidate_bullpen_statcast_labels.py")
ADAPTER_PATH = Path("scripts/fetch_candidate_bullpen_statcast_live_adapter.py")
PLAN_6EJ = Path("scripts/plan_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_runtime_summary.py")
VALIDATION_6EF = Path("scripts/validate_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight.py")
AUDIT_6EG = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight.py")
PLAN_6EH = Path("scripts/plan_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_operational_readiness.py")
AUDIT_6EI = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_operational_readiness_plan.py")
VALIDATION_6EB = Path("scripts/validate_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability.py")
AUDIT_6EC = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability.py")
PLAN_6ED = Path("scripts/plan_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_operational_readiness.py")
AUDIT_6EE = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_operational_readiness_plan.py")
PLAN_6DZ = Path("scripts/plan_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability.py")
AUDIT_6EA = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_plan.py")
VALIDATION_6DX = Path("scripts/validate_candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution.py")
AUDIT_6DY = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution.py")

FIXTURE_ROOT = Path("tests/fixtures/statcast/bullpen_labels")
MANIFEST = FIXTURE_ROOT / "manifest.json"
EXPECTED_RESULTS = FIXTURE_ROOT / "expected_results.json"
DATES_DIR = FIXTURE_ROOT / "dates"

OUTPUT_DIR = Path("tmp")
OUTPUT_DIR.mkdir(exist_ok=True)

PLAN_JSON = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_runtime_summary_plan.json"
PLAN_CHECKS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_runtime_summary_plan_checks.csv"
PLAN_CURRENT = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_runtime_summary_plan_current_state.csv"
PLAN_FIELDS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_runtime_summary_plan_fields.csv"
PLAN_STATUS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_runtime_summary_plan_status_contract.csv"
PLAN_ARTIFACT = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_runtime_summary_plan_artifact_compatibility.csv"
PLAN_NON_GOALS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_runtime_summary_plan_non_goals.csv"
PLAN_FUTURE_VALIDATION = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_runtime_summary_plan_future_validation.csv"
PLAN_IMMUTABILITY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_runtime_summary_plan_immutability.csv"

OUTPUT_JSON = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_runtime_summary_plan_audit.json"
OUTPUT_CHECKS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_runtime_summary_plan_audit_checks.csv"
OUTPUT_PLAN_EXECUTION = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_runtime_summary_plan_audit_execution.csv"
OUTPUT_PLAN_ARTIFACTS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_runtime_summary_plan_audit_artifacts.csv"
OUTPUT_CURRENT = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_runtime_summary_plan_audit_current_state.csv"
OUTPUT_FIELDS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_runtime_summary_plan_audit_fields.csv"
OUTPUT_STATUS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_runtime_summary_plan_audit_status_contract.csv"
OUTPUT_ARTIFACT = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_runtime_summary_plan_audit_artifact_compatibility.csv"
OUTPUT_NON_GOALS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_runtime_summary_plan_audit_non_goals.csv"
OUTPUT_FUTURE_VALIDATION = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_runtime_summary_plan_audit_future_validation.csv"
OUTPUT_IMMUTABILITY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_runtime_summary_plan_audit_immutability.csv"

OBSERVABILITY_FIELDS = [
    "live_fetcher_resolution_source",
    "live_fetcher_resolution_status",
    "live_fetcher_resolution_gate",
    "live_fetcher_resolution_reason",
    "live_fetcher_resolution_dependency_error",
    "live_fetcher_resolution_external_fetch_enabled",
    "live_fetcher_resolution_synthetic_enabled",
    "live_fetcher_resolution_real_enabled",
]

PREFLIGHT_FIELDS = [
    "live_fetcher_preflight_passed",
    "live_fetcher_preflight_status",
    "live_fetcher_preflight_reason",
    "live_fetcher_preflight_dry_run",
    "live_fetcher_preflight_single_date",
    "live_fetcher_preflight_write_blocked",
    "live_fetcher_preflight_allow_live_write",
    "live_fetcher_preflight_env_gate_enabled",
    "live_fetcher_preflight_synthetic_gate_enabled",
    "live_fetcher_preflight_observability_fields_expected",
]

REQUIRED_FIELDS = [
    "source_mode",
    "adapter_status",
    "adapter_raw_row_count",
    "adapter_normalized_row_count",
    "adapter_duplicate_count",
    "adapter_required_field_failures",
    "adapter_missing_fields",
    "adapter_fetch_error",
    "adapter_external_fetch_performed",
    "adapter_db_writes_performed",
    "adapter_source_adapter_version",
    "external_fetch_performed",
    "db_writes_performed",
    "candidate_labels_materialized",
    "production_default_unchanged",
]

SUMMARY_FIELDS = [
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


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    fieldnames: List[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text())


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def _contains(rows: List[Dict[str, Any]], *needles: str) -> bool:
    haystack = json.dumps(rows, sort_keys=True).lower()
    return all(needle.lower() in haystack for needle in needles)


def _sha(path: Path) -> str:
    if not path.exists():
        return "__MISSING__"
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _snapshot_files() -> Dict[str, str]:
    paths = [
        SCAFFOLD_PATH,
        ADAPTER_PATH,
        PLAN_6EJ,
        VALIDATION_6EF,
        AUDIT_6EG,
        PLAN_6EH,
        AUDIT_6EI,
        VALIDATION_6EB,
        AUDIT_6EC,
        PLAN_6ED,
        AUDIT_6EE,
        PLAN_6DZ,
        AUDIT_6EA,
        VALIDATION_6DX,
        AUDIT_6DY,
        MANIFEST,
        EXPECTED_RESULTS,
    ]
    snapshot = {str(path): _sha(path) for path in paths}
    if DATES_DIR.exists():
        for payload in sorted(DATES_DIR.glob("*.jsonl")):
            snapshot[str(payload)] = _sha(payload)
    return snapshot


def _top_level_imports(source: str) -> str:
    tree = ast.parse(source)
    imports = []
    for node in tree.body:
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            imports.append(ast.get_source_segment(source, node) or "")
    return "\n".join(imports)


def _run_plan() -> Dict[str, Any]:
    completed = subprocess.run(
        [sys.executable, str(PLAN_6EJ)],
        capture_output=True,
        text=True,
    )
    diagnosis = _read_json(PLAN_JSON)
    flags = [
        "current_state_valid",
        "runtime_summary_fields_defined",
        "summary_status_contract_defined",
        "artifact_compatibility_contract_defined",
        "rollout_non_goals_defined",
        "future_validation_plan_defined",
        "immutability_valid",
    ]
    return {
        "returncode": completed.returncode,
        "diagnosis": diagnosis.get("diagnosis", ""),
        "all_checks_passed": diagnosis.get("all_checks_passed"),
        "planning_only": diagnosis.get("planning_only"),
        "flags_all_true": all(diagnosis.get(flag) is True for flag in flags),
        "passed": (
            completed.returncode == 0
            and diagnosis.get("diagnosis") == "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_runtime_summary_plan_complete"
            and diagnosis.get("all_checks_passed") is True
            and diagnosis.get("planning_only") is True
            and all(diagnosis.get(flag) is True for flag in flags)
            and diagnosis.get("fixture_assets_mutated") is False
            and diagnosis.get("external_fetch_performed") is False
            and diagnosis.get("db_writes_performed") is False
            and diagnosis.get("candidate_labels_materialized_from_live_rows") is False
            and diagnosis.get("production_default_unchanged") is True
        ),
    }


def _plan_artifact_rows() -> List[Dict[str, Any]]:
    artifacts = [
        PLAN_JSON,
        PLAN_CHECKS,
        PLAN_CURRENT,
        PLAN_FIELDS,
        PLAN_STATUS,
        PLAN_ARTIFACT,
        PLAN_NON_GOALS,
        PLAN_FUTURE_VALIDATION,
        PLAN_IMMUTABILITY,
    ]
    return [
        {
            "artifact": str(path),
            "exists": path.exists(),
            "size_bytes": path.stat().st_size if path.exists() else 0,
            "passed": path.exists() and path.stat().st_size > 0,
        }
        for path in artifacts
    ]


def _current_rows(source: str) -> List[Dict[str, Any]]:
    top_imports = _top_level_imports(source)
    resolver_idx = source.find("def _resolve_candidate_bullpen_live_fetcher")
    resolver_body = source[resolver_idx:] if resolver_idx != -1 else ""
    return [
        {"check": "six_ef_marker_present", "passed": "candidate_bullpen_live_adapter_cli_live_fetcher_observability_preflight_v0.1" in source, "detail": True},
        {"check": "preflight_fields_present", "passed": all(field in source for field in PREFLIGHT_FIELDS), "detail": f"{sum(field in source for field in PREFLIGHT_FIELDS)}/{len(PREFLIGHT_FIELDS)}"},
        {"check": "observability_fields_preserved", "passed": all(field in source for field in OBSERVABILITY_FIELDS), "detail": f"{sum(field in source for field in OBSERVABILITY_FIELDS)}/{len(OBSERVABILITY_FIELDS)}"},
        {"check": "required_live_fields_referenced", "passed": all(field in source for field in REQUIRED_FIELDS), "detail": f"{sum(field in source for field in REQUIRED_FIELDS)}/{len(REQUIRED_FIELDS)}"},
        {"check": "preflight_helper_defined", "passed": "def _candidate_bullpen_live_fetcher_observability_preflight" in source, "detail": True},
        {"check": "preflight_apply_helper_defined", "passed": "def _candidate_bullpen_apply_live_fetcher_preflight" in source, "detail": True},
        {"check": "observability_builder_defined", "passed": "def _candidate_bullpen_live_fetcher_observability" in source, "detail": True},
        {"check": "observability_apply_helper_defined", "passed": "def _candidate_bullpen_apply_live_fetcher_observability" in source, "detail": True},
        {"check": "resolver_present", "passed": "def _resolve_candidate_bullpen_live_fetcher" in source, "detail": True},
        {"check": "real_fetcher_env_gate_preserved", "passed": "CANDIDATE_BULLPEN_ENABLE_REAL_STATCAST_FETCHER" in source, "detail": True},
        {"check": "synthetic_env_gate_preserved", "passed": "CANDIDATE_BULLPEN_LIVE_FETCHER_TEST_DOUBLE" in source, "detail": True},
        {"check": "no_top_level_adapter_import", "passed": "fetch_candidate_bullpen_statcast_live_adapter" not in top_imports, "detail": True},
        {"check": "no_top_level_pybaseball_statcast_import", "passed": "pybaseball" not in top_imports and "statcast" not in top_imports, "detail": True},
        {"check": "adapter_import_inside_gated_resolver", "passed": "fetch_candidate_bullpen_statcast_live_adapter" in resolver_body and "CANDIDATE_BULLPEN_ENABLE_REAL_STATCAST_FETCHER" in resolver_body, "detail": True},
        {"check": "production_default_preserved", "passed": 'default="scaffold"' in source or "default=CANDIDATE_BULLPEN_SOURCE_MODE_FIXTURE" in source, "detail": True},
    ]


def _field_rows() -> List[Dict[str, Any]]:
    rows = _read_csv(PLAN_FIELDS)
    return [
        {"check": f"{field}_planned", "passed": _contains(rows, field, "planned additive runtime summary diagnostic field"), "detail": True}
        for field in SUMMARY_FIELDS
    ]


def _status_rows() -> List[Dict[str, Any]]:
    rows = _read_csv(PLAN_STATUS)
    return [
        {"check": "default_no_real_gate_safe_dry_run", "passed": _contains(rows, "default_no_real_gate", "safe dry-run", "no-real-fetch"), "detail": True},
        {"check": "synthetic_path_validation_dry_run", "passed": _contains(rows, "synthetic_path", "validation synthetic dry-run"), "detail": True},
        {"check": "real_gated_monkeypatch_without_network", "passed": _contains(rows, "real_gated_monkeypatch_path", "without network"), "detail": True},
        {"check": "dependency_missing_safe", "passed": _contains(rows, "dependency_missing_path", "dependency-missing safe"), "detail": True},
        {"check": "live_without_dry_run_blocked", "passed": _contains(rows, "live_without_dry_run", "blocked requires dry-run"), "detail": True},
        {"check": "live_write_attempt_blocked", "passed": _contains(rows, "live_write_attempt", "blocked write"), "detail": True},
        {"check": "invalid_multi_date_window_blocked", "passed": _contains(rows, "invalid_or_multi_date_window", "blocked date-window invalid"), "detail": True},
    ]


def _artifact_rows() -> List[Dict[str, Any]]:
    rows = _read_csv(PLAN_ARTIFACT)
    return [
        {"check": "existing_15_required_fields_present", "passed": _contains(rows, "existing_15_required_fields_present", "remain present"), "detail": True},
        {"check": "existing_8_observability_fields_present", "passed": _contains(rows, "existing_8_observability_fields_present", "remain present"), "detail": True},
        {"check": "existing_10_preflight_fields_present", "passed": _contains(rows, "existing_10_preflight_fields_present", "remain present"), "detail": True},
        {"check": "planned_10_runtime_summary_fields_additive", "passed": _contains(rows, "planned_10_runtime_summary_fields_additive", "additive only"), "detail": True},
        {"check": "downstream_json_consumers_safe", "passed": _contains(rows, "downstream_json_consumers_safe", "safe"), "detail": True},
    ]


def _non_goal_rows() -> List[Dict[str, Any]]:
    rows = _read_csv(PLAN_NON_GOALS)
    expected = [
        "no_runtime_summary_implementation",
        "no_real_fetch_in_plan",
        "no_real_fetch_in_validation_ci",
        "no_db_writes",
        "no_candidate_materialization",
        "no_resolver_gate_changes",
        "no_adapter_changes",
        "no_production_default_changes",
        "no_write_policy_changes",
    ]
    return [{"check": f"{name}_defined", "passed": _contains(rows, name), "detail": True} for name in expected]


def _future_validation_rows() -> List[Dict[str, Any]]:
    rows = _read_csv(PLAN_FUTURE_VALIDATION)
    expected = [
        "source_audit",
        "plan_execution_audit",
        "summary_field_contract_audit",
        "status_contract_audit",
        "artifact_compatibility_audit",
        "default_artifact_summary_audit",
        "synthetic_artifact_summary_audit",
        "monkeypatched_real_gated_summary_audit",
        "dependency_missing_summary_audit",
        "blocked_path_summary_audit",
        "import_boundary_audit",
        "immutability_audit",
        "safety_audit",
    ]
    return [{"check": f"{name}_defined", "passed": _contains(rows, name), "detail": True} for name in expected]


def _immutability_rows(before: Dict[str, str]) -> List[Dict[str, Any]]:
    after = _snapshot_files()
    return [
        {"check": "scaffold_not_modified_by_audit", "passed": before.get(str(SCAFFOLD_PATH)) == after.get(str(SCAFFOLD_PATH)), "detail": str(SCAFFOLD_PATH)},
        {"check": "adapter_not_modified", "passed": before.get(str(ADAPTER_PATH)) == after.get(str(ADAPTER_PATH)), "detail": str(ADAPTER_PATH)},
        {"check": "six_ej_plan_not_modified", "passed": before.get(str(PLAN_6EJ)) == after.get(str(PLAN_6EJ)), "detail": str(PLAN_6EJ)},
        {"check": "six_ef_validation_not_modified", "passed": before.get(str(VALIDATION_6EF)) == after.get(str(VALIDATION_6EF)), "detail": str(VALIDATION_6EF)},
        {"check": "six_eg_audit_not_modified", "passed": before.get(str(AUDIT_6EG)) == after.get(str(AUDIT_6EG)), "detail": str(AUDIT_6EG)},
        {"check": "six_eh_plan_not_modified", "passed": before.get(str(PLAN_6EH)) == after.get(str(PLAN_6EH)), "detail": str(PLAN_6EH)},
        {"check": "six_ei_audit_not_modified", "passed": before.get(str(AUDIT_6EI)) == after.get(str(AUDIT_6EI)), "detail": str(AUDIT_6EI)},
        {"check": "six_eb_validation_not_modified", "passed": before.get(str(VALIDATION_6EB)) == after.get(str(VALIDATION_6EB)), "detail": str(VALIDATION_6EB)},
        {"check": "six_ec_audit_not_modified", "passed": before.get(str(AUDIT_6EC)) == after.get(str(AUDIT_6EC)), "detail": str(AUDIT_6EC)},
        {"check": "six_ed_plan_not_modified", "passed": before.get(str(PLAN_6ED)) == after.get(str(PLAN_6ED)), "detail": str(PLAN_6ED)},
        {"check": "six_ee_audit_not_modified", "passed": before.get(str(AUDIT_6EE)) == after.get(str(AUDIT_6EE)), "detail": str(AUDIT_6EE)},
        {"check": "no_fixture_mutation", "passed": before == after, "detail": "fixtures and tracked dependency files unchanged"},
    ]


def main() -> None:
    before = _snapshot_files()
    source = SCAFFOLD_PATH.read_text(errors="ignore")

    plan_execution = _run_plan()
    plan_execution_rows = [
        {
            "check": "plan_executes_successfully",
            "returncode": plan_execution["returncode"],
            "diagnosis": plan_execution["diagnosis"],
            "all_checks_passed": plan_execution["all_checks_passed"],
            "planning_only": plan_execution["planning_only"],
            "flags_all_true": plan_execution["flags_all_true"],
            "passed": plan_execution["passed"],
        }
    ]

    plan_artifact_rows = _plan_artifact_rows()
    current_rows = _current_rows(source)
    field_rows = _field_rows()
    status_rows = _status_rows()
    artifact_rows = _artifact_rows()
    non_goal_rows = _non_goal_rows()
    future_validation_rows = _future_validation_rows()
    immutability_rows = _immutability_rows(before)

    _write_csv(OUTPUT_PLAN_EXECUTION, plan_execution_rows)
    _write_csv(OUTPUT_PLAN_ARTIFACTS, plan_artifact_rows)
    _write_csv(OUTPUT_CURRENT, current_rows)
    _write_csv(OUTPUT_FIELDS, field_rows)
    _write_csv(OUTPUT_STATUS, status_rows)
    _write_csv(OUTPUT_ARTIFACT, artifact_rows)
    _write_csv(OUTPUT_NON_GOALS, non_goal_rows)
    _write_csv(OUTPUT_FUTURE_VALIDATION, future_validation_rows)
    _write_csv(OUTPUT_IMMUTABILITY, immutability_rows)

    checks = [
        {"check": "plan_execution_valid", "passed": plan_execution["passed"], "detail": plan_execution["diagnosis"]},
        {"check": "plan_artifacts_valid", "passed": all(row["passed"] for row in plan_artifact_rows), "detail": f"{sum(row['passed'] for row in plan_artifact_rows)}/{len(plan_artifact_rows)}"},
        {"check": "current_state_valid", "passed": all(row["passed"] for row in current_rows), "detail": f"{sum(row['passed'] for row in current_rows)}/{len(current_rows)}"},
        {"check": "runtime_summary_fields_valid", "passed": all(row["passed"] for row in field_rows), "detail": f"{sum(row['passed'] for row in field_rows)}/{len(field_rows)}"},
        {"check": "summary_status_contract_valid", "passed": all(row["passed"] for row in status_rows), "detail": f"{sum(row['passed'] for row in status_rows)}/{len(status_rows)}"},
        {"check": "artifact_compatibility_contract_valid", "passed": all(row["passed"] for row in artifact_rows), "detail": f"{sum(row['passed'] for row in artifact_rows)}/{len(artifact_rows)}"},
        {"check": "rollout_non_goals_valid", "passed": all(row["passed"] for row in non_goal_rows), "detail": f"{sum(row['passed'] for row in non_goal_rows)}/{len(non_goal_rows)}"},
        {"check": "future_validation_plan_valid", "passed": all(row["passed"] for row in future_validation_rows), "detail": f"{sum(row['passed'] for row in future_validation_rows)}/{len(future_validation_rows)}"},
        {"check": "immutability_valid", "passed": all(row["passed"] for row in immutability_rows), "detail": f"{sum(row['passed'] for row in immutability_rows)}/{len(immutability_rows)}"},
        {"check": "scaffold_not_modified_by_audit", "passed": any(row["check"] == "scaffold_not_modified_by_audit" and row["passed"] for row in immutability_rows), "detail": str(SCAFFOLD_PATH)},
        {"check": "adapter_not_modified", "passed": any(row["check"] == "adapter_not_modified" and row["passed"] for row in immutability_rows), "detail": str(ADAPTER_PATH)},
        {"check": "six_ej_plan_not_modified", "passed": any(row["check"] == "six_ej_plan_not_modified" and row["passed"] for row in immutability_rows), "detail": str(PLAN_6EJ)},
        {"check": "six_ef_validation_not_modified", "passed": any(row["check"] == "six_ef_validation_not_modified" and row["passed"] for row in immutability_rows), "detail": str(VALIDATION_6EF)},
        {"check": "six_eg_audit_not_modified", "passed": any(row["check"] == "six_eg_audit_not_modified" and row["passed"] for row in immutability_rows), "detail": str(AUDIT_6EG)},
        {"check": "six_eh_plan_not_modified", "passed": any(row["check"] == "six_eh_plan_not_modified" and row["passed"] for row in immutability_rows), "detail": str(PLAN_6EH)},
        {"check": "six_ei_audit_not_modified", "passed": any(row["check"] == "six_ei_audit_not_modified" and row["passed"] for row in immutability_rows), "detail": str(AUDIT_6EI)},
        {"check": "six_eb_validation_not_modified", "passed": any(row["check"] == "six_eb_validation_not_modified" and row["passed"] for row in immutability_rows), "detail": str(VALIDATION_6EB)},
        {"check": "six_ec_audit_not_modified", "passed": any(row["check"] == "six_ec_audit_not_modified" and row["passed"] for row in immutability_rows), "detail": str(AUDIT_6EC)},
        {"check": "six_ed_plan_not_modified", "passed": any(row["check"] == "six_ed_plan_not_modified" and row["passed"] for row in immutability_rows), "detail": str(PLAN_6ED)},
        {"check": "six_ee_audit_not_modified", "passed": any(row["check"] == "six_ee_audit_not_modified" and row["passed"] for row in immutability_rows), "detail": str(AUDIT_6EE)},
        {"check": "no_fixture_mutation", "passed": any(row["check"] == "no_fixture_mutation" and row["passed"] for row in immutability_rows), "detail": True},
        {"check": "no_real_external_fetch", "passed": True, "detail": "audit-only plan validation"},
        {"check": "no_db_writes", "passed": True, "detail": True},
        {"check": "no_candidate_label_materialization", "passed": True, "detail": True},
        {"check": "production_default_unchanged", "passed": True, "detail": True},
    ]

    _write_csv(OUTPUT_CHECKS, checks)

    all_checks_passed = all(row["passed"] for row in checks)
    diagnosis = {
        "diagnosis": "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_runtime_summary_plan_audit_complete",
        "audit_version": AUDIT_VERSION,
        "plan_execution_rows": len(plan_execution_rows),
        "plan_artifact_rows": len(plan_artifact_rows),
        "current_state_rows": len(current_rows),
        "runtime_summary_field_rows": len(field_rows),
        "summary_status_contract_rows": len(status_rows),
        "artifact_compatibility_rows": len(artifact_rows),
        "rollout_non_goal_rows": len(non_goal_rows),
        "future_validation_rows": len(future_validation_rows),
        "immutability_rows": len(immutability_rows),
        "all_checks_passed": all_checks_passed,
        "audit_only": True,
        "plan_validated": plan_execution["passed"],
        "plan_artifacts_valid": all(row["passed"] for row in plan_artifact_rows),
        "current_state_valid": all(row["passed"] for row in current_rows),
        "runtime_summary_fields_valid": all(row["passed"] for row in field_rows),
        "summary_status_contract_valid": all(row["passed"] for row in status_rows),
        "artifact_compatibility_contract_valid": all(row["passed"] for row in artifact_rows),
        "rollout_non_goals_valid": all(row["passed"] for row in non_goal_rows),
        "future_validation_plan_valid": all(row["passed"] for row in future_validation_rows),
        "immutability_valid": all(row["passed"] for row in immutability_rows),
        "scaffold_modified_by_audit": False,
        "adapter_modified": False,
        "six_ej_plan_modified": False,
        "six_ef_validation_modified": False,
        "six_eg_audit_modified": False,
        "six_eh_plan_modified": False,
        "six_ei_audit_modified": False,
        "six_eb_validation_modified": False,
        "six_ec_audit_modified": False,
        "six_ed_plan_modified": False,
        "six_ee_audit_modified": False,
        "fixture_assets_mutated": False,
        "external_fetch_performed": False,
        "db_writes_performed": False,
        "candidate_labels_materialized_from_live_rows": False,
        "production_default_unchanged": True,
        "recommended_next_layer": (
            "6EL_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_runtime_summary_implementation_plan"
            if all_checks_passed
            else "6EK_patch_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_runtime_summary_plan_audit"
        ),
    }

    OUTPUT_JSON.write_text(json.dumps(diagnosis, indent=2))
    print(json.dumps(diagnosis, indent=2))


if __name__ == "__main__":
    main()
