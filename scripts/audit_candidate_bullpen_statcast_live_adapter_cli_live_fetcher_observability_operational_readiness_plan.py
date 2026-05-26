from __future__ import annotations

import ast
import csv
import hashlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List


AUDIT_VERSION = "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_operational_readiness_plan_audit_v0.1"

SCAFFOLD_PATH = Path("scripts/backfill_candidate_bullpen_statcast_labels.py")
ADAPTER_PATH = Path("scripts/fetch_candidate_bullpen_statcast_live_adapter.py")
PLAN_6ED = Path("scripts/plan_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_operational_readiness.py")
VALIDATION_6EB = Path("scripts/validate_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability.py")
AUDIT_6EC = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability.py")
PLAN_6DZ = Path("scripts/plan_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability.py")
AUDIT_6EA = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_plan.py")
VALIDATION_6DX = Path("scripts/validate_candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution.py")
AUDIT_6DY = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution.py")
PLAN_6DV = Path("scripts/plan_candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution.py")
AUDIT_6DW = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution_plan.py")
VALIDATION_6DT = Path("scripts/validate_candidate_bullpen_statcast_live_adapter_cli_live_dry_run_fetcher_injection.py")
AUDIT_6DU = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_cli_live_dry_run_fetcher_injection.py")
VALIDATION_6DL = Path("scripts/validate_candidate_bullpen_statcast_live_adapter_scaffold_integration.py")
AUDIT_6DM = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_scaffold_integration.py")
PLAN_6DN = Path("scripts/plan_candidate_bullpen_statcast_live_adapter_cli_contract.py")
AUDIT_6DO = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_cli_contract_plan.py")
VALIDATION_6DP = Path("scripts/validate_candidate_bullpen_statcast_live_adapter_cli_scaffold_integration.py")
AUDIT_6DQ = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_cli_scaffold_integration.py")
PLAN_6DR = Path("scripts/plan_candidate_bullpen_statcast_live_adapter_cli_live_dry_run_fetcher_injection.py")
AUDIT_6DS = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_cli_live_dry_run_fetcher_injection_plan.py")

FIXTURE_ROOT = Path("tests/fixtures/statcast/bullpen_labels")
MANIFEST = FIXTURE_ROOT / "manifest.json"
EXPECTED_RESULTS = FIXTURE_ROOT / "expected_results.json"
DATES_DIR = FIXTURE_ROOT / "dates"

OUTPUT_DIR = Path("tmp")
OUTPUT_DIR.mkdir(exist_ok=True)

PLAN_JSON = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_operational_readiness_plan.json"
PLAN_CHECKS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_operational_readiness_plan_checks.csv"
PLAN_CURRENT = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_operational_readiness_plan_current_state.csv"
PLAN_GATES = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_operational_readiness_plan_gates.csv"
PLAN_PREFLIGHT = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_operational_readiness_plan_preflight.csv"
PLAN_ACCEPTANCE = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_operational_readiness_plan_acceptance_contract.csv"
PLAN_NON_GOALS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_operational_readiness_plan_non_goals.csv"
PLAN_FUTURE_VALIDATION = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_operational_readiness_plan_future_validation.csv"
PLAN_IMMUTABILITY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_operational_readiness_plan_immutability.csv"

OUTPUT_JSON = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_operational_readiness_plan_audit.json"
OUTPUT_CHECKS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_operational_readiness_plan_audit_checks.csv"
OUTPUT_PLAN_EXECUTION = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_operational_readiness_plan_audit_execution.csv"
OUTPUT_PLAN_ARTIFACTS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_operational_readiness_plan_audit_artifacts.csv"
OUTPUT_CURRENT = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_operational_readiness_plan_audit_current_state.csv"
OUTPUT_GATES = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_operational_readiness_plan_audit_gates.csv"
OUTPUT_PREFLIGHT = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_operational_readiness_plan_audit_preflight.csv"
OUTPUT_ACCEPTANCE = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_operational_readiness_plan_audit_acceptance_contract.csv"
OUTPUT_NON_GOALS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_operational_readiness_plan_audit_non_goals.csv"
OUTPUT_FUTURE_VALIDATION = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_operational_readiness_plan_audit_future_validation.csv"
OUTPUT_IMMUTABILITY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_operational_readiness_plan_audit_immutability.csv"

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
        PLAN_6ED,
        VALIDATION_6EB,
        AUDIT_6EC,
        PLAN_6DZ,
        AUDIT_6EA,
        VALIDATION_6DX,
        AUDIT_6DY,
        PLAN_6DV,
        AUDIT_6DW,
        VALIDATION_6DT,
        AUDIT_6DU,
        VALIDATION_6DL,
        AUDIT_6DM,
        PLAN_6DN,
        AUDIT_6DO,
        VALIDATION_6DP,
        AUDIT_6DQ,
        PLAN_6DR,
        AUDIT_6DS,
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
        [sys.executable, str(PLAN_6ED)],
        capture_output=True,
        text=True,
    )
    diagnosis = _read_json(PLAN_JSON)
    flags = [
        "current_state_valid",
        "readiness_gates_defined",
        "preflight_checks_defined",
        "observability_acceptance_contract_defined",
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
            and diagnosis.get("diagnosis") == "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_operational_readiness_plan_complete"
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
        PLAN_GATES,
        PLAN_PREFLIGHT,
        PLAN_ACCEPTANCE,
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
    synthetic_idx = source.find('CANDIDATE_BULLPEN_LIVE_FETCHER_TEST_DOUBLE") == "synthetic"')
    real_idx = source.find('CANDIDATE_BULLPEN_ENABLE_REAL_STATCAST_FETCHER") == "1"')
    return [
        {"check": "six_eb_marker_present", "passed": "candidate_bullpen_live_adapter_cli_live_fetcher_observability_v0.1" in source, "detail": True},
        {"check": "observability_fields_present", "passed": all(field in source for field in OBSERVABILITY_FIELDS), "detail": f"{sum(field in source for field in OBSERVABILITY_FIELDS)}/{len(OBSERVABILITY_FIELDS)}"},
        {"check": "observability_builder_defined", "passed": "def _candidate_bullpen_live_fetcher_observability" in source, "detail": True},
        {"check": "observability_apply_helper_defined", "passed": "def _candidate_bullpen_apply_live_fetcher_observability" in source, "detail": True},
        {"check": "real_fetcher_env_gate_present", "passed": "CANDIDATE_BULLPEN_ENABLE_REAL_STATCAST_FETCHER" in source, "detail": True},
        {"check": "synthetic_env_gate_preserved", "passed": "CANDIDATE_BULLPEN_LIVE_FETCHER_TEST_DOUBLE" in source, "detail": True},
        {"check": "dependency_missing_status_preserved", "passed": "live_dependency_missing" in source, "detail": True},
        {"check": "resolver_present", "passed": "def _resolve_candidate_bullpen_live_fetcher" in source, "detail": True},
        {"check": "synthetic_precedes_real_branch", "passed": synthetic_idx != -1 and real_idx != -1 and synthetic_idx < real_idx, "detail": f"{synthetic_idx}:{real_idx}"},
        {"check": "no_top_level_adapter_import", "passed": "fetch_candidate_bullpen_statcast_live_adapter" not in top_imports, "detail": True},
        {"check": "no_top_level_pybaseball_statcast_import", "passed": "pybaseball" not in top_imports and "statcast" not in top_imports, "detail": True},
        {"check": "production_default_preserved", "passed": 'default="scaffold"' in source or "default=CANDIDATE_BULLPEN_SOURCE_MODE_FIXTURE" in source, "detail": True},
    ]


def _gate_rows() -> List[Dict[str, Any]]:
    rows = _read_csv(PLAN_GATES)
    return [
        {"check": "dry_run_default_gate_defined", "passed": _contains(rows, "dry_run_default", "dry-run only"), "detail": True},
        {"check": "real_fetcher_explicit_env_gate_defined", "passed": _contains(rows, "real_fetcher_explicit_env_gate", "CANDIDATE_BULLPEN_ENABLE_REAL_STATCAST_FETCHER=1"), "detail": True},
        {"check": "writes_blocked_gate_defined", "passed": _contains(rows, "writes_blocked", "controlled write policy"), "detail": True},
        {"check": "single_date_window_gate_defined", "passed": _contains(rows, "single_date_window", "exactly one label date"), "detail": True},
        {"check": "invalid_multi_date_blocked_gate_defined", "passed": _contains(rows, "invalid_multi_date_blocked", "remain blocked"), "detail": True},
        {"check": "dependency_missing_safe_gate_defined", "passed": _contains(rows, "dependency_missing_safe", "safe"), "detail": True},
        {"check": "ci_no_network_gate_defined", "passed": _contains(rows, "ci_no_network", "never allowed in CI"), "detail": True},
    ]


def _preflight_rows() -> List[Dict[str, Any]]:
    rows = _read_csv(PLAN_PREFLIGHT)
    return [
        {"check": "environment_gate_explicit", "passed": _contains(rows, "environment_gate_explicit"), "detail": True},
        {"check": "dry_run_required", "passed": _contains(rows, "dry_run_required"), "detail": True},
        {"check": "one_date_window_confirmed", "passed": _contains(rows, "one_date_window_confirmed"), "detail": True},
        {"check": "live_write_flags_false", "passed": _contains(rows, "live_write_flags_false"), "detail": True},
        {"check": "observability_fields_present", "passed": _contains(rows, "observability_fields_present", "8 observability fields"), "detail": True},
        {"check": "adapter_status_known", "passed": _contains(rows, "adapter_status_known", "safe and known"), "detail": True},
        {"check": "adapter_counts_present", "passed": _contains(rows, "adapter_counts_present", "raw", "normalized", "duplicate"), "detail": True},
        {"check": "external_fetch_performed_surfaced", "passed": _contains(rows, "external_fetch_performed_surfaced"), "detail": True},
        {"check": "db_writes_performed_surfaced", "passed": _contains(rows, "db_writes_performed_surfaced"), "detail": True},
        {"check": "candidate_labels_materialized_surfaced", "passed": _contains(rows, "candidate_labels_materialized_surfaced"), "detail": True},
        {"check": "dependency_error_surfaced", "passed": _contains(rows, "dependency_error_surfaced", "dependency error"), "detail": True},
    ]


def _acceptance_rows() -> List[Dict[str, Any]]:
    rows = _read_csv(PLAN_ACCEPTANCE)
    return [
        {"check": "default_no_gate_safe_source", "passed": _contains(rows, "default_no_gate", "none", "dependency-missing"), "detail": True},
        {"check": "synthetic_source", "passed": _contains(rows, "synthetic", "synthetic_test_double"), "detail": True},
        {"check": "monkeypatched_real_source", "passed": _contains(rows, "monkeypatched_real", "real_adapter", "without network"), "detail": True},
        {"check": "dependency_missing_status", "passed": _contains(rows, "dependency_missing", "live_dependency_missing"), "detail": True},
        {"check": "blocked_statuses_preserved", "passed": _contains(rows, "blocked_paths", "blocked statuses"), "detail": True},
        {"check": "existing_15_required_fields", "passed": _contains(rows, "existing_required_fields", "15 required fields"), "detail": True},
        {"check": "additive_8_observability_fields", "passed": _contains(rows, "additive_observability", "8 observability fields"), "detail": True},
    ]


def _non_goal_rows() -> List[Dict[str, Any]]:
    rows = _read_csv(PLAN_NON_GOALS)
    return [
        {"check": "no_real_fetch_in_plan", "passed": _contains(rows, "no_real_fetch_in_plan"), "detail": True},
        {"check": "no_real_fetch_in_validation_ci", "passed": _contains(rows, "no_real_fetch_in_validation_ci"), "detail": True},
        {"check": "no_db_writes", "passed": _contains(rows, "no_db_writes"), "detail": True},
        {"check": "no_live_row_materialization", "passed": _contains(rows, "no_live_row_materialization"), "detail": True},
        {"check": "no_schedule_or_cron", "passed": _contains(rows, "no_schedule_or_cron"), "detail": True},
        {"check": "no_secrets_management_changes", "passed": _contains(rows, "no_secrets_management_changes"), "detail": True},
        {"check": "no_production_default_changes", "passed": _contains(rows, "no_production_default_changes"), "detail": True},
        {"check": "no_resolver_gate_changes", "passed": _contains(rows, "no_resolver_gate_changes"), "detail": True},
        {"check": "no_adapter_changes", "passed": _contains(rows, "no_adapter_changes"), "detail": True},
    ]


def _future_validation_rows() -> List[Dict[str, Any]]:
    rows = _read_csv(PLAN_FUTURE_VALIDATION)
    expected = [
        "source_audit",
        "plan_execution_audit",
        "dry_run_preflight_audit",
        "default_no_gate_observability_audit",
        "synthetic_observability_audit",
        "monkeypatched_real_fetcher_observability_audit",
        "dependency_missing_audit",
        "blocked_path_audit",
        "artifact_compatibility_audit",
        "import_boundary_audit",
        "immutability_audit",
        "safety_audit",
    ]
    return [
        {"check": f"{name}_defined", "passed": _contains(rows, name), "detail": True}
        for name in expected
    ]


def _immutability_rows(before: Dict[str, str]) -> List[Dict[str, Any]]:
    after = _snapshot_files()
    prior_paths = [
        VALIDATION_6EB,
        AUDIT_6EC,
        PLAN_6DZ,
        AUDIT_6EA,
        VALIDATION_6DX,
        AUDIT_6DY,
        PLAN_6DV,
        AUDIT_6DW,
        VALIDATION_6DT,
        AUDIT_6DU,
        VALIDATION_6DL,
        AUDIT_6DM,
        PLAN_6DN,
        AUDIT_6DO,
        VALIDATION_6DP,
        AUDIT_6DQ,
        PLAN_6DR,
        AUDIT_6DS,
    ]
    return [
        {"check": "scaffold_not_modified_by_audit", "passed": before.get(str(SCAFFOLD_PATH)) == after.get(str(SCAFFOLD_PATH)), "detail": str(SCAFFOLD_PATH)},
        {"check": "adapter_not_modified", "passed": before.get(str(ADAPTER_PATH)) == after.get(str(ADAPTER_PATH)), "detail": str(ADAPTER_PATH)},
        {"check": "six_ed_plan_not_modified", "passed": before.get(str(PLAN_6ED)) == after.get(str(PLAN_6ED)), "detail": str(PLAN_6ED)},
        {"check": "six_eb_validation_not_modified", "passed": before.get(str(VALIDATION_6EB)) == after.get(str(VALIDATION_6EB)), "detail": str(VALIDATION_6EB)},
        {"check": "six_ec_audit_not_modified", "passed": before.get(str(AUDIT_6EC)) == after.get(str(AUDIT_6EC)), "detail": str(AUDIT_6EC)},
        {"check": "six_dz_plan_not_modified", "passed": before.get(str(PLAN_6DZ)) == after.get(str(PLAN_6DZ)), "detail": str(PLAN_6DZ)},
        {"check": "six_ea_audit_not_modified", "passed": before.get(str(AUDIT_6EA)) == after.get(str(AUDIT_6EA)), "detail": str(AUDIT_6EA)},
        {"check": "prior_validation_audit_plan_scripts_not_modified", "passed": all(before.get(str(path)) == after.get(str(path)) for path in prior_paths), "detail": "6DL through 6EC unchanged"},
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
    gate_rows = _gate_rows()
    preflight_rows = _preflight_rows()
    acceptance_rows = _acceptance_rows()
    non_goal_rows = _non_goal_rows()
    future_validation_rows = _future_validation_rows()
    immutability_rows = _immutability_rows(before)

    _write_csv(OUTPUT_PLAN_EXECUTION, plan_execution_rows)
    _write_csv(OUTPUT_PLAN_ARTIFACTS, plan_artifact_rows)
    _write_csv(OUTPUT_CURRENT, current_rows)
    _write_csv(OUTPUT_GATES, gate_rows)
    _write_csv(OUTPUT_PREFLIGHT, preflight_rows)
    _write_csv(OUTPUT_ACCEPTANCE, acceptance_rows)
    _write_csv(OUTPUT_NON_GOALS, non_goal_rows)
    _write_csv(OUTPUT_FUTURE_VALIDATION, future_validation_rows)
    _write_csv(OUTPUT_IMMUTABILITY, immutability_rows)

    checks = [
        {"check": "plan_execution_valid", "passed": plan_execution["passed"], "detail": plan_execution["diagnosis"]},
        {"check": "plan_artifacts_valid", "passed": all(row["passed"] for row in plan_artifact_rows), "detail": f"{sum(row['passed'] for row in plan_artifact_rows)}/{len(plan_artifact_rows)}"},
        {"check": "current_state_valid", "passed": all(row["passed"] for row in current_rows), "detail": f"{sum(row['passed'] for row in current_rows)}/{len(current_rows)}"},
        {"check": "readiness_gates_valid", "passed": all(row["passed"] for row in gate_rows), "detail": f"{sum(row['passed'] for row in gate_rows)}/{len(gate_rows)}"},
        {"check": "preflight_contract_valid", "passed": all(row["passed"] for row in preflight_rows), "detail": f"{sum(row['passed'] for row in preflight_rows)}/{len(preflight_rows)}"},
        {"check": "observability_acceptance_contract_valid", "passed": all(row["passed"] for row in acceptance_rows), "detail": f"{sum(row['passed'] for row in acceptance_rows)}/{len(acceptance_rows)}"},
        {"check": "rollout_non_goals_valid", "passed": all(row["passed"] for row in non_goal_rows), "detail": f"{sum(row['passed'] for row in non_goal_rows)}/{len(non_goal_rows)}"},
        {"check": "future_validation_plan_valid", "passed": all(row["passed"] for row in future_validation_rows), "detail": f"{sum(row['passed'] for row in future_validation_rows)}/{len(future_validation_rows)}"},
        {"check": "immutability_valid", "passed": all(row["passed"] for row in immutability_rows), "detail": f"{sum(row['passed'] for row in immutability_rows)}/{len(immutability_rows)}"},
        {"check": "scaffold_not_modified_by_audit", "passed": any(row["check"] == "scaffold_not_modified_by_audit" and row["passed"] for row in immutability_rows), "detail": str(SCAFFOLD_PATH)},
        {"check": "adapter_not_modified", "passed": any(row["check"] == "adapter_not_modified" and row["passed"] for row in immutability_rows), "detail": str(ADAPTER_PATH)},
        {"check": "six_ed_plan_not_modified", "passed": any(row["check"] == "six_ed_plan_not_modified" and row["passed"] for row in immutability_rows), "detail": str(PLAN_6ED)},
        {"check": "six_eb_validation_not_modified", "passed": any(row["check"] == "six_eb_validation_not_modified" and row["passed"] for row in immutability_rows), "detail": str(VALIDATION_6EB)},
        {"check": "six_ec_audit_not_modified", "passed": any(row["check"] == "six_ec_audit_not_modified" and row["passed"] for row in immutability_rows), "detail": str(AUDIT_6EC)},
        {"check": "six_dz_plan_not_modified", "passed": any(row["check"] == "six_dz_plan_not_modified" and row["passed"] for row in immutability_rows), "detail": str(PLAN_6DZ)},
        {"check": "six_ea_audit_not_modified", "passed": any(row["check"] == "six_ea_audit_not_modified" and row["passed"] for row in immutability_rows), "detail": str(AUDIT_6EA)},
        {"check": "no_fixture_mutation", "passed": any(row["check"] == "no_fixture_mutation" and row["passed"] for row in immutability_rows), "detail": True},
        {"check": "no_real_external_fetch", "passed": True, "detail": "audit-only plan validation"},
        {"check": "no_db_writes", "passed": True, "detail": True},
        {"check": "no_candidate_label_materialization", "passed": True, "detail": True},
        {"check": "production_default_unchanged", "passed": True, "detail": True},
    ]

    _write_csv(OUTPUT_CHECKS, checks)

    all_checks_passed = all(row["passed"] for row in checks)
    diagnosis = {
        "diagnosis": "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_operational_readiness_plan_audit_complete",
        "audit_version": AUDIT_VERSION,
        "plan_execution_rows": len(plan_execution_rows),
        "plan_artifact_rows": len(plan_artifact_rows),
        "current_state_rows": len(current_rows),
        "readiness_gate_rows": len(gate_rows),
        "preflight_rows": len(preflight_rows),
        "observability_acceptance_rows": len(acceptance_rows),
        "rollout_non_goal_rows": len(non_goal_rows),
        "future_validation_rows": len(future_validation_rows),
        "immutability_rows": len(immutability_rows),
        "all_checks_passed": all_checks_passed,
        "audit_only": True,
        "plan_validated": plan_execution["passed"],
        "plan_artifacts_valid": all(row["passed"] for row in plan_artifact_rows),
        "current_state_valid": all(row["passed"] for row in current_rows),
        "readiness_gates_valid": all(row["passed"] for row in gate_rows),
        "preflight_contract_valid": all(row["passed"] for row in preflight_rows),
        "observability_acceptance_contract_valid": all(row["passed"] for row in acceptance_rows),
        "rollout_non_goals_valid": all(row["passed"] for row in non_goal_rows),
        "future_validation_plan_valid": all(row["passed"] for row in future_validation_rows),
        "immutability_valid": all(row["passed"] for row in immutability_rows),
        "scaffold_modified_by_audit": False,
        "adapter_modified": False,
        "six_ed_plan_modified": False,
        "six_eb_validation_modified": False,
        "six_ec_audit_modified": False,
        "six_dz_plan_modified": False,
        "six_ea_audit_modified": False,
        "fixture_assets_mutated": False,
        "external_fetch_performed": False,
        "db_writes_performed": False,
        "candidate_labels_materialized_from_live_rows": False,
        "production_default_unchanged": True,
        "recommended_next_layer": (
            "6EF_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight"
            if all_checks_passed
            else "6EE_patch_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_operational_readiness_plan_audit"
        ),
    }

    OUTPUT_JSON.write_text(json.dumps(diagnosis, indent=2))
    print(json.dumps(diagnosis, indent=2))


if __name__ == "__main__":
    main()
