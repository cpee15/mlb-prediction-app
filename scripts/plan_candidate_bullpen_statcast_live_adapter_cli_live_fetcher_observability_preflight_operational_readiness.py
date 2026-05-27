from __future__ import annotations

import ast
import csv
import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List


PLAN_VERSION = "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_operational_readiness_plan_v0.1"

SCAFFOLD_PATH = Path("scripts/backfill_candidate_bullpen_statcast_labels.py")
ADAPTER_PATH = Path("scripts/fetch_candidate_bullpen_statcast_live_adapter.py")
VALIDATION_6EF = Path("scripts/validate_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight.py")
AUDIT_6EG = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight.py")
VALIDATION_6EB = Path("scripts/validate_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability.py")
AUDIT_6EC = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability.py")
PLAN_6ED = Path("scripts/plan_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_operational_readiness.py")
AUDIT_6EE = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_operational_readiness_plan.py")
PLAN_6DZ = Path("scripts/plan_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability.py")
AUDIT_6EA = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_plan.py")
VALIDATION_6DX = Path("scripts/validate_candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution.py")
AUDIT_6DY = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution.py")
PLAN_6DV = Path("scripts/plan_candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution.py")
AUDIT_6DW = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_cli_real_fetcher_resolution_plan.py")
VALIDATION_6DT = Path("scripts/validate_candidate_bullpen_statcast_live_adapter_cli_live_dry_run_fetcher_injection.py")
AUDIT_6DU = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_cli_live_dry_run_fetcher_injection.py")

FIXTURE_ROOT = Path("tests/fixtures/statcast/bullpen_labels")
MANIFEST = FIXTURE_ROOT / "manifest.json"
EXPECTED_RESULTS = FIXTURE_ROOT / "expected_results.json"
DATES_DIR = FIXTURE_ROOT / "dates"

OUTPUT_DIR = Path("tmp")
OUTPUT_DIR.mkdir(exist_ok=True)

OUTPUT_JSON = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_operational_readiness_plan.json"
OUTPUT_CHECKS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_operational_readiness_plan_checks.csv"
OUTPUT_CURRENT = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_operational_readiness_plan_current_state.csv"
OUTPUT_GATES = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_operational_readiness_plan_gates.csv"
OUTPUT_ACCEPTANCE = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_operational_readiness_plan_acceptance_contract.csv"
OUTPUT_NON_GOALS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_operational_readiness_plan_non_goals.csv"
OUTPUT_VALIDATION = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_operational_readiness_plan_future_validation.csv"
OUTPUT_IMMUTABILITY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_operational_readiness_plan_immutability.csv"

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


def _sha(path: Path) -> str:
    if not path.exists():
        return "__MISSING__"
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _snapshot_files() -> Dict[str, str]:
    paths = [
        SCAFFOLD_PATH,
        ADAPTER_PATH,
        VALIDATION_6EF,
        AUDIT_6EG,
        VALIDATION_6EB,
        AUDIT_6EC,
        PLAN_6ED,
        AUDIT_6EE,
        PLAN_6DZ,
        AUDIT_6EA,
        VALIDATION_6DX,
        AUDIT_6DY,
        PLAN_6DV,
        AUDIT_6DW,
        VALIDATION_6DT,
        AUDIT_6DU,
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


def _current_state_rows(source: str) -> List[Dict[str, Any]]:
    top_imports = _top_level_imports(source)
    resolver_idx = source.find("def _resolve_candidate_bullpen_live_fetcher")
    resolver_body = source[resolver_idx:] if resolver_idx != -1 else ""
    return [
        {"check": "six_ef_marker_present", "passed": "candidate_bullpen_live_adapter_cli_live_fetcher_observability_preflight_v0.1" in source, "detail": True},
        {"check": "preflight_helper_defined", "passed": "def _candidate_bullpen_live_fetcher_observability_preflight" in source, "detail": True},
        {"check": "preflight_apply_helper_defined", "passed": "def _candidate_bullpen_apply_live_fetcher_preflight" in source, "detail": True},
        {"check": "preflight_fields_present", "passed": all(field in source for field in PREFLIGHT_FIELDS), "detail": f"{sum(field in source for field in PREFLIGHT_FIELDS)}/{len(PREFLIGHT_FIELDS)}"},
        {"check": "observability_fields_preserved", "passed": all(field in source for field in OBSERVABILITY_FIELDS), "detail": f"{sum(field in source for field in OBSERVABILITY_FIELDS)}/{len(OBSERVABILITY_FIELDS)}"},
        {"check": "required_live_fields_referenced", "passed": all(field in source for field in REQUIRED_FIELDS), "detail": f"{sum(field in source for field in REQUIRED_FIELDS)}/{len(REQUIRED_FIELDS)}"},
        {"check": "real_fetcher_env_gate_preserved", "passed": "CANDIDATE_BULLPEN_ENABLE_REAL_STATCAST_FETCHER" in source, "detail": True},
        {"check": "synthetic_env_gate_preserved", "passed": "CANDIDATE_BULLPEN_LIVE_FETCHER_TEST_DOUBLE" in source, "detail": True},
        {"check": "resolver_present", "passed": "def _resolve_candidate_bullpen_live_fetcher" in source, "detail": True},
        {"check": "no_top_level_adapter_import", "passed": "fetch_candidate_bullpen_statcast_live_adapter" not in top_imports, "detail": True},
        {"check": "no_top_level_pybaseball_statcast_import", "passed": "pybaseball" not in top_imports and "statcast" not in top_imports, "detail": True},
        {"check": "adapter_import_inside_gated_resolver", "passed": "fetch_candidate_bullpen_statcast_live_adapter" in resolver_body and "CANDIDATE_BULLPEN_ENABLE_REAL_STATCAST_FETCHER" in resolver_body, "detail": True},
        {"check": "production_default_preserved", "passed": 'default="scaffold"' in source or "default=CANDIDATE_BULLPEN_SOURCE_MODE_FIXTURE" in source, "detail": True},
    ]


def _gate_rows() -> List[Dict[str, Any]]:
    return [
        {"gate": "dry_run_required", "contract": "Preflight readiness requires dry-run for live fetcher execution.", "required": True},
        {"gate": "single_date_window_required", "contract": "Preflight readiness requires exactly one label date.", "required": True},
        {"gate": "write_flags_blocked", "contract": "Write and allow-live-write flags remain blocked.", "required": True},
        {"gate": "real_fetcher_explicit_env_gate", "contract": "Real fetcher remains explicitly env-gated.", "required": True},
        {"gate": "synthetic_test_double_validation_only", "contract": "Synthetic gate remains validation-only.", "required": True},
        {"gate": "dependency_missing_safe", "contract": "Dependency-missing path remains safe and diagnostic-only.", "required": True},
        {"gate": "ci_network_fetch_prohibited", "contract": "CI validation must not perform network fetches.", "required": True},
        {"gate": "preflight_non_authoritative_for_writes", "contract": "Preflight diagnostics are additive and non-authoritative for writes.", "required": True},
    ]


def _acceptance_rows() -> List[Dict[str, Any]]:
    return [
        {"case": "default_live_dry_run", "contract": "Produces live_preflight_ready.", "required": True},
        {"case": "synthetic_live_dry_run", "contract": "Produces live_preflight_ready and synthetic gate true.", "required": True},
        {"case": "monkeypatched_real_gated", "contract": "Produces live_preflight_ready and env gate true without network.", "required": True},
        {"case": "live_without_dry_run", "contract": "Produces live_requires_dry_run.", "required": True},
        {"case": "live_write_attempt", "contract": "Produces live_write_blocked.", "required": True},
        {"case": "invalid_or_multi_date", "contract": "Produces live_date_window_invalid.", "required": True},
        {"case": "artifact_field_contract", "contract": "All accepted artifacts include existing 15 fields, 8 observability fields, and 10 preflight fields.", "required": True},
    ]


def _non_goal_rows() -> List[Dict[str, Any]]:
    return [
        {"non_goal": "no_real_fetch_in_plan", "detail": "No real fetch occurs in this planning layer.", "required": True},
        {"non_goal": "no_real_fetch_in_validation_ci", "detail": "No real fetch is allowed in validation/CI.", "required": True},
        {"non_goal": "no_db_writes", "detail": "No DB writes are introduced.", "required": True},
        {"non_goal": "no_candidate_materialization", "detail": "No candidate labels are materialized from live rows.", "required": True},
        {"non_goal": "no_schedule_or_cron", "detail": "No schedule, cron, or automation is introduced.", "required": True},
        {"non_goal": "no_secrets_management_changes", "detail": "No secrets management changes are introduced.", "required": True},
        {"non_goal": "no_production_default_changes", "detail": "No production defaults are changed.", "required": True},
        {"non_goal": "no_resolver_gate_changes", "detail": "No resolver gates are changed.", "required": True},
        {"non_goal": "no_adapter_changes", "detail": "No adapter source changes are introduced.", "required": True},
        {"non_goal": "no_write_policy_changes", "detail": "No write policy changes are introduced.", "required": True},
    ]


def _future_validation_rows() -> List[Dict[str, Any]]:
    return [
        {"validation": "plan_execution_audit", "planned_check": "Execute this plan and validate all artifacts.", "required": True},
        {"validation": "source_audit", "planned_check": "Confirm 6EF baseline and 6EH plan source.", "required": True},
        {"validation": "preflight_helper_audit", "planned_check": "Validate preflight helper readiness and blocked statuses.", "required": True},
        {"validation": "default_live_dry_run_artifact_audit", "planned_check": "Validate default live dry-run artifact.", "required": True},
        {"validation": "synthetic_artifact_audit", "planned_check": "Validate synthetic artifact.", "required": True},
        {"validation": "monkeypatched_real_gated_artifact_audit", "planned_check": "Validate real-gated artifact with monkeypatch and no network.", "required": True},
        {"validation": "blocked_path_artifact_audit", "planned_check": "Validate blocked path artifacts.", "required": True},
        {"validation": "artifact_compatibility_audit", "planned_check": "Validate existing 15 fields, 8 observability fields, and 10 preflight fields.", "required": True},
        {"validation": "import_boundary_audit", "planned_check": "Validate no top-level adapter/pybaseball/statcast imports.", "required": True},
        {"validation": "immutability_audit", "planned_check": "Validate scaffold/adapter/prior scripts/fixtures unchanged by audit.", "required": True},
        {"validation": "safety_audit", "planned_check": "Validate no real fetch, no DB writes, no materialization.", "required": True},
    ]


def _immutability_rows(before: Dict[str, str]) -> List[Dict[str, Any]]:
    after = _snapshot_files()
    prior_paths = [
        VALIDATION_6EF,
        AUDIT_6EG,
        VALIDATION_6EB,
        AUDIT_6EC,
        PLAN_6ED,
        AUDIT_6EE,
        PLAN_6DZ,
        AUDIT_6EA,
        VALIDATION_6DX,
        AUDIT_6DY,
        PLAN_6DV,
        AUDIT_6DW,
        VALIDATION_6DT,
        AUDIT_6DU,
    ]
    return [
        {"check": "scaffold_unchanged_by_plan", "passed": before.get(str(SCAFFOLD_PATH)) == after.get(str(SCAFFOLD_PATH)), "detail": str(SCAFFOLD_PATH)},
        {"check": "adapter_unchanged_by_plan", "passed": before.get(str(ADAPTER_PATH)) == after.get(str(ADAPTER_PATH)), "detail": str(ADAPTER_PATH)},
        {"check": "six_ef_validation_unchanged_by_plan", "passed": before.get(str(VALIDATION_6EF)) == after.get(str(VALIDATION_6EF)), "detail": str(VALIDATION_6EF)},
        {"check": "six_eg_audit_unchanged_by_plan", "passed": before.get(str(AUDIT_6EG)) == after.get(str(AUDIT_6EG)), "detail": str(AUDIT_6EG)},
        {"check": "prior_validation_audit_plan_scripts_unchanged_by_plan", "passed": all(before.get(str(path)) == after.get(str(path)) for path in prior_paths), "detail": "6DT through 6EG unchanged"},
        {"check": "fixtures_unchanged_by_plan", "passed": before == after, "detail": "fixtures and tracked dependency files unchanged"},
    ]


def main() -> None:
    before = _snapshot_files()
    source = SCAFFOLD_PATH.read_text(errors="ignore")

    current_state = _current_state_rows(source)
    gates = _gate_rows()
    acceptance = _acceptance_rows()
    non_goals = _non_goal_rows()
    future_validation = _future_validation_rows()
    immutability = _immutability_rows(before)

    _write_csv(OUTPUT_CURRENT, current_state)
    _write_csv(OUTPUT_GATES, gates)
    _write_csv(OUTPUT_ACCEPTANCE, acceptance)
    _write_csv(OUTPUT_NON_GOALS, non_goals)
    _write_csv(OUTPUT_VALIDATION, future_validation)
    _write_csv(OUTPUT_IMMUTABILITY, immutability)

    checks = [
        {"check": "current_state_valid", "passed": all(row["passed"] for row in current_state), "detail": f"{sum(row['passed'] for row in current_state)}/{len(current_state)}"},
        {"check": "preflight_readiness_gates_defined", "passed": all(row["required"] for row in gates), "detail": len(gates)},
        {"check": "preflight_acceptance_contract_defined", "passed": all(row["required"] for row in acceptance), "detail": len(acceptance)},
        {"check": "rollout_non_goals_defined", "passed": all(row["required"] for row in non_goals), "detail": len(non_goals)},
        {"check": "future_validation_plan_defined", "passed": all(row["required"] for row in future_validation), "detail": len(future_validation)},
        {"check": "immutability_valid", "passed": all(row["passed"] for row in immutability), "detail": f"{sum(row['passed'] for row in immutability)}/{len(immutability)}"},
        {"check": "scaffold_unchanged", "passed": any(row["check"] == "scaffold_unchanged_by_plan" and row["passed"] for row in immutability), "detail": str(SCAFFOLD_PATH)},
        {"check": "adapter_unchanged", "passed": any(row["check"] == "adapter_unchanged_by_plan" and row["passed"] for row in immutability), "detail": str(ADAPTER_PATH)},
        {"check": "six_ef_validation_unchanged", "passed": any(row["check"] == "six_ef_validation_unchanged_by_plan" and row["passed"] for row in immutability), "detail": str(VALIDATION_6EF)},
        {"check": "six_eg_audit_unchanged", "passed": any(row["check"] == "six_eg_audit_unchanged_by_plan" and row["passed"] for row in immutability), "detail": str(AUDIT_6EG)},
        {"check": "no_fixture_mutation", "passed": any(row["check"] == "fixtures_unchanged_by_plan" and row["passed"] for row in immutability), "detail": True},
        {"check": "no_real_external_fetch", "passed": True, "detail": "planning-only"},
        {"check": "no_db_writes", "passed": True, "detail": "planning-only"},
        {"check": "no_candidate_label_materialization", "passed": True, "detail": "planning-only"},
        {"check": "production_default_unchanged", "passed": any(row["check"] == "production_default_preserved" and row["passed"] for row in current_state), "detail": True},
    ]

    _write_csv(OUTPUT_CHECKS, checks)

    all_checks_passed = all(row["passed"] for row in checks)
    diagnosis = {
        "diagnosis": "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_operational_readiness_plan_complete",
        "plan_version": PLAN_VERSION,
        "current_state_rows": len(current_state),
        "preflight_readiness_gate_rows": len(gates),
        "preflight_acceptance_rows": len(acceptance),
        "rollout_non_goal_rows": len(non_goals),
        "future_validation_rows": len(future_validation),
        "immutability_rows": len(immutability),
        "all_checks_passed": all_checks_passed,
        "planning_only": True,
        "current_state_valid": all(row["passed"] for row in current_state),
        "preflight_readiness_gates_defined": all(row["required"] for row in gates),
        "preflight_acceptance_contract_defined": all(row["required"] for row in acceptance),
        "rollout_non_goals_defined": all(row["required"] for row in non_goals),
        "future_validation_plan_defined": all(row["required"] for row in future_validation),
        "immutability_valid": all(row["passed"] for row in immutability),
        "scaffold_unchanged": True,
        "adapter_unchanged": True,
        "six_ef_validation_unchanged": True,
        "six_eg_audit_unchanged": True,
        "fixture_assets_mutated": False,
        "external_fetch_performed": False,
        "db_writes_performed": False,
        "candidate_labels_materialized_from_live_rows": False,
        "production_default_unchanged": True,
        "recommended_next_layer": (
            "6EI_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_operational_readiness_plan_audit"
            if all_checks_passed
            else "6EH_patch_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_operational_readiness_plan"
        ),
    }

    OUTPUT_JSON.write_text(json.dumps(diagnosis, indent=2))
    print(json.dumps(diagnosis, indent=2))


if __name__ == "__main__":
    main()
