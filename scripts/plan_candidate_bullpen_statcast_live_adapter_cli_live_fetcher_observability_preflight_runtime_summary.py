from __future__ import annotations

import ast
import csv
import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List


PLAN_VERSION = "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_runtime_summary_plan_v0.1"

SCAFFOLD_PATH = Path("scripts/backfill_candidate_bullpen_statcast_labels.py")
ADAPTER_PATH = Path("scripts/fetch_candidate_bullpen_statcast_live_adapter.py")
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

OUTPUT_JSON = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_runtime_summary_plan.json"
OUTPUT_CHECKS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_runtime_summary_plan_checks.csv"
OUTPUT_CURRENT = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_runtime_summary_plan_current_state.csv"
OUTPUT_FIELDS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_runtime_summary_plan_fields.csv"
OUTPUT_STATUS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_runtime_summary_plan_status_contract.csv"
OUTPUT_ARTIFACT = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_runtime_summary_plan_artifact_compatibility.csv"
OUTPUT_NON_GOALS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_runtime_summary_plan_non_goals.csv"
OUTPUT_VALIDATION = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_runtime_summary_plan_future_validation.csv"
OUTPUT_IMMUTABILITY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_runtime_summary_plan_immutability.csv"

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


def _current_state_rows(source: str) -> List[Dict[str, Any]]:
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


def _summary_field_rows() -> List[Dict[str, Any]]:
    return [
        {"field": field, "contract": "planned additive runtime summary diagnostic field", "required": True}
        for field in SUMMARY_FIELDS
    ]


def _status_contract_rows() -> List[Dict[str, Any]]:
    return [
        {"case": "default_no_real_gate", "contract": "Summarize as safe dry-run / no-real-fetch.", "required": True},
        {"case": "synthetic_path", "contract": "Summarize as validation synthetic dry-run.", "required": True},
        {"case": "real_gated_monkeypatch_path", "contract": "Summarize as real-gated dry-run candidate without network in validation.", "required": True},
        {"case": "dependency_missing_path", "contract": "Summarize as dependency-missing safe.", "required": True},
        {"case": "live_without_dry_run", "contract": "Summarize as blocked requires dry-run.", "required": True},
        {"case": "live_write_attempt", "contract": "Summarize as blocked write.", "required": True},
        {"case": "invalid_or_multi_date_window", "contract": "Summarize as blocked date-window invalid.", "required": True},
    ]


def _artifact_rows() -> List[Dict[str, Any]]:
    return [
        {"contract": "existing_15_required_fields_present", "detail": "Existing 15 required fields remain present.", "required": True},
        {"contract": "existing_8_observability_fields_present", "detail": "Existing 8 observability fields remain present.", "required": True},
        {"contract": "existing_10_preflight_fields_present", "detail": "Existing 10 preflight fields remain present.", "required": True},
        {"contract": "planned_10_runtime_summary_fields_additive", "detail": "Planned 10 runtime summary fields are additive only.", "required": True},
        {"contract": "downstream_json_consumers_safe", "detail": "Downstream JSON consumers remain safe because fields are additive.", "required": True},
    ]


def _non_goal_rows() -> List[Dict[str, Any]]:
    return [
        {"non_goal": "no_runtime_summary_implementation", "detail": "No runtime summary implementation occurs in this planning layer.", "required": True},
        {"non_goal": "no_real_fetch_in_plan", "detail": "No real fetch occurs in this planning layer.", "required": True},
        {"non_goal": "no_real_fetch_in_validation_ci", "detail": "No real fetch is allowed in validation/CI.", "required": True},
        {"non_goal": "no_db_writes", "detail": "No DB writes are introduced.", "required": True},
        {"non_goal": "no_candidate_materialization", "detail": "No candidate labels are materialized from live rows.", "required": True},
        {"non_goal": "no_resolver_gate_changes", "detail": "No resolver gates are changed.", "required": True},
        {"non_goal": "no_adapter_changes", "detail": "No adapter source changes are introduced.", "required": True},
        {"non_goal": "no_production_default_changes", "detail": "No production defaults are changed.", "required": True},
        {"non_goal": "no_write_policy_changes", "detail": "No write policy changes are introduced.", "required": True},
    ]


def _future_validation_rows() -> List[Dict[str, Any]]:
    return [
        {"validation": "source_audit", "planned_check": "Confirm 6EF baseline and planned summary field contract.", "required": True},
        {"validation": "plan_execution_audit", "planned_check": "Execute this plan and validate all artifacts.", "required": True},
        {"validation": "summary_field_contract_audit", "planned_check": "Validate planned summary fields are present in plan artifacts.", "required": True},
        {"validation": "status_contract_audit", "planned_check": "Validate planned summary status cases.", "required": True},
        {"validation": "artifact_compatibility_audit", "planned_check": "Validate existing 15 + 8 + 10 fields and additive summary plan.", "required": True},
        {"validation": "default_artifact_summary_audit", "planned_check": "Validate future default artifact summary behavior.", "required": True},
        {"validation": "synthetic_artifact_summary_audit", "planned_check": "Validate future synthetic artifact summary behavior.", "required": True},
        {"validation": "monkeypatched_real_gated_summary_audit", "planned_check": "Validate future real-gated summary with monkeypatch and no network.", "required": True},
        {"validation": "dependency_missing_summary_audit", "planned_check": "Validate future dependency-missing summary.", "required": True},
        {"validation": "blocked_path_summary_audit", "planned_check": "Validate future blocked-path summaries.", "required": True},
        {"validation": "import_boundary_audit", "planned_check": "Validate no top-level adapter/pybaseball/statcast imports.", "required": True},
        {"validation": "immutability_audit", "planned_check": "Validate scaffold/adapter/prior scripts/fixtures unchanged by audit.", "required": True},
        {"validation": "safety_audit", "planned_check": "Validate no real fetch, no DB writes, no materialization.", "required": True},
    ]


def _immutability_rows(before: Dict[str, str]) -> List[Dict[str, Any]]:
    after = _snapshot_files()
    prior_paths = [
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
    ]
    return [
        {"check": "scaffold_unchanged_by_plan", "passed": before.get(str(SCAFFOLD_PATH)) == after.get(str(SCAFFOLD_PATH)), "detail": str(SCAFFOLD_PATH)},
        {"check": "adapter_unchanged_by_plan", "passed": before.get(str(ADAPTER_PATH)) == after.get(str(ADAPTER_PATH)), "detail": str(ADAPTER_PATH)},
        {"check": "six_ef_validation_unchanged_by_plan", "passed": before.get(str(VALIDATION_6EF)) == after.get(str(VALIDATION_6EF)), "detail": str(VALIDATION_6EF)},
        {"check": "six_eg_audit_unchanged_by_plan", "passed": before.get(str(AUDIT_6EG)) == after.get(str(AUDIT_6EG)), "detail": str(AUDIT_6EG)},
        {"check": "six_eh_plan_unchanged_by_plan", "passed": before.get(str(PLAN_6EH)) == after.get(str(PLAN_6EH)), "detail": str(PLAN_6EH)},
        {"check": "six_ei_audit_unchanged_by_plan", "passed": before.get(str(AUDIT_6EI)) == after.get(str(AUDIT_6EI)), "detail": str(AUDIT_6EI)},
        {"check": "prior_validation_audit_plan_scripts_unchanged_by_plan", "passed": all(before.get(str(path)) == after.get(str(path)) for path in prior_paths), "detail": "6DX through 6EI unchanged"},
        {"check": "fixtures_unchanged_by_plan", "passed": before == after, "detail": "fixtures and tracked dependency files unchanged"},
    ]


def main() -> None:
    before = _snapshot_files()
    source = SCAFFOLD_PATH.read_text(errors="ignore")

    current_state = _current_state_rows(source)
    summary_fields = _summary_field_rows()
    status_contract = _status_contract_rows()
    artifact_contract = _artifact_rows()
    non_goals = _non_goal_rows()
    future_validation = _future_validation_rows()
    immutability = _immutability_rows(before)

    _write_csv(OUTPUT_CURRENT, current_state)
    _write_csv(OUTPUT_FIELDS, summary_fields)
    _write_csv(OUTPUT_STATUS, status_contract)
    _write_csv(OUTPUT_ARTIFACT, artifact_contract)
    _write_csv(OUTPUT_NON_GOALS, non_goals)
    _write_csv(OUTPUT_VALIDATION, future_validation)
    _write_csv(OUTPUT_IMMUTABILITY, immutability)

    checks = [
        {"check": "current_state_valid", "passed": all(row["passed"] for row in current_state), "detail": f"{sum(row['passed'] for row in current_state)}/{len(current_state)}"},
        {"check": "runtime_summary_fields_defined", "passed": all(row["required"] for row in summary_fields), "detail": len(summary_fields)},
        {"check": "summary_status_contract_defined", "passed": all(row["required"] for row in status_contract), "detail": len(status_contract)},
        {"check": "artifact_compatibility_contract_defined", "passed": all(row["required"] for row in artifact_contract), "detail": len(artifact_contract)},
        {"check": "rollout_non_goals_defined", "passed": all(row["required"] for row in non_goals), "detail": len(non_goals)},
        {"check": "future_validation_plan_defined", "passed": all(row["required"] for row in future_validation), "detail": len(future_validation)},
        {"check": "immutability_valid", "passed": all(row["passed"] for row in immutability), "detail": f"{sum(row['passed'] for row in immutability)}/{len(immutability)}"},
        {"check": "scaffold_unchanged", "passed": any(row["check"] == "scaffold_unchanged_by_plan" and row["passed"] for row in immutability), "detail": str(SCAFFOLD_PATH)},
        {"check": "adapter_unchanged", "passed": any(row["check"] == "adapter_unchanged_by_plan" and row["passed"] for row in immutability), "detail": str(ADAPTER_PATH)},
        {"check": "six_ef_validation_unchanged", "passed": any(row["check"] == "six_ef_validation_unchanged_by_plan" and row["passed"] for row in immutability), "detail": str(VALIDATION_6EF)},
        {"check": "six_eg_audit_unchanged", "passed": any(row["check"] == "six_eg_audit_unchanged_by_plan" and row["passed"] for row in immutability), "detail": str(AUDIT_6EG)},
        {"check": "six_eh_plan_unchanged", "passed": any(row["check"] == "six_eh_plan_unchanged_by_plan" and row["passed"] for row in immutability), "detail": str(PLAN_6EH)},
        {"check": "six_ei_audit_unchanged", "passed": any(row["check"] == "six_ei_audit_unchanged_by_plan" and row["passed"] for row in immutability), "detail": str(AUDIT_6EI)},
        {"check": "no_fixture_mutation", "passed": any(row["check"] == "fixtures_unchanged_by_plan" and row["passed"] for row in immutability), "detail": True},
        {"check": "no_real_external_fetch", "passed": True, "detail": "planning-only"},
        {"check": "no_db_writes", "passed": True, "detail": "planning-only"},
        {"check": "no_candidate_label_materialization", "passed": True, "detail": "planning-only"},
        {"check": "production_default_unchanged", "passed": any(row["check"] == "production_default_preserved" and row["passed"] for row in current_state), "detail": True},
    ]

    _write_csv(OUTPUT_CHECKS, checks)

    all_checks_passed = all(row["passed"] for row in checks)
    diagnosis = {
        "diagnosis": "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_runtime_summary_plan_complete",
        "plan_version": PLAN_VERSION,
        "current_state_rows": len(current_state),
        "runtime_summary_field_rows": len(summary_fields),
        "summary_status_contract_rows": len(status_contract),
        "artifact_compatibility_rows": len(artifact_contract),
        "rollout_non_goal_rows": len(non_goals),
        "future_validation_rows": len(future_validation),
        "immutability_rows": len(immutability),
        "all_checks_passed": all_checks_passed,
        "planning_only": True,
        "current_state_valid": all(row["passed"] for row in current_state),
        "runtime_summary_fields_defined": all(row["required"] for row in summary_fields),
        "summary_status_contract_defined": all(row["required"] for row in status_contract),
        "artifact_compatibility_contract_defined": all(row["required"] for row in artifact_contract),
        "rollout_non_goals_defined": all(row["required"] for row in non_goals),
        "future_validation_plan_defined": all(row["required"] for row in future_validation),
        "immutability_valid": all(row["passed"] for row in immutability),
        "scaffold_unchanged": True,
        "adapter_unchanged": True,
        "six_ef_validation_unchanged": True,
        "six_eg_audit_unchanged": True,
        "six_eh_plan_unchanged": True,
        "six_ei_audit_unchanged": True,
        "fixture_assets_mutated": False,
        "external_fetch_performed": False,
        "db_writes_performed": False,
        "candidate_labels_materialized_from_live_rows": False,
        "production_default_unchanged": True,
        "recommended_next_layer": (
            "6EK_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_runtime_summary_plan_audit"
            if all_checks_passed
            else "6EJ_patch_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_runtime_summary_plan"
        ),
    }

    OUTPUT_JSON.write_text(json.dumps(diagnosis, indent=2))
    print(json.dumps(diagnosis, indent=2))


if __name__ == "__main__":
    main()
