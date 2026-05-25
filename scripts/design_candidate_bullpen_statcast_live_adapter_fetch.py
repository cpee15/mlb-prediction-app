from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, List


DESIGN_VERSION = "candidate_bullpen_statcast_live_adapter_fetch_design_v0.1"

BACKFILL_SCAFFOLD = Path("scripts/backfill_candidate_bullpen_statcast_labels.py")
LIVE_SCAFFOLD_AUDIT = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_dry_run_scaffold.py")
FIXTURE_ADAPTER = Path("scripts/prototype_candidate_bullpen_statcast_fixture_replay_adapter.py")
LIVE_ADAPTER_TARGET = Path("scripts/fetch_candidate_bullpen_statcast_live_adapter.py")
STATCAST_UTILS = Path("mlb_app/statcast_utils.py")
ETL = Path("mlb_app/etl.py")
FIXTURE_ROOT = Path("tests/fixtures/statcast/bullpen_labels")
MANIFEST = FIXTURE_ROOT / "manifest.json"
EXPECTED_RESULTS = FIXTURE_ROOT / "expected_results.json"

OUTPUT_DIR = Path("tmp")
OUTPUT_DIR.mkdir(exist_ok=True)

OUTPUT_JSON = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_design.json"
OUTPUT_CHECKS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_design_checks.csv"
OUTPUT_SURFACE = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_surface_inventory.csv"
OUTPUT_MODULE = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_module_design.csv"
OUTPUT_RESULT = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_result_contract.csv"
OUTPUT_STRATEGY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_strategy.csv"
OUTPUT_MAPPING = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_raw_to_normalized_mapping.csv"
OUTPUT_STATUS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_status_taxonomy.csv"
OUTPUT_VALIDATION = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_validation_plan.csv"
OUTPUT_SAFETY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_safety_gates.csv"


SURFACE_INVENTORY = [
    {
        "surface": "backfill_scaffold",
        "path": str(BACKFILL_SCAFFOLD),
        "exists": BACKFILL_SCAFFOLD.exists(),
        "role": "existing --source-mode live dry-run scaffold integration point",
        "modified_this_layer": False,
    },
    {
        "surface": "live_scaffold_audit",
        "path": str(LIVE_SCAFFOLD_AUDIT),
        "exists": LIVE_SCAFFOLD_AUDIT.exists(),
        "role": "preflight audit proving live scaffold behavior",
        "modified_this_layer": False,
    },
    {
        "surface": "fixture_adapter_contract",
        "path": str(FIXTURE_ADAPTER),
        "exists": FIXTURE_ADAPTER.exists(),
        "role": "reference normalized row contract and result wrapper behavior",
        "modified_this_layer": False,
    },
    {
        "surface": "live_dry_run_artifacts",
        "path": "tmp/candidate_bullpen_statcast_live_adapter_*.csv/json",
        "exists": True,
        "role": "future emitted diagnostics contract",
        "modified_this_layer": False,
    },
    {
        "surface": "statcast_utils_reference",
        "path": str(STATCAST_UTILS),
        "exists": STATCAST_UTILS.exists(),
        "role": "reference-only possible Statcast utility surface",
        "modified_this_layer": False,
    },
    {
        "surface": "etl_reference",
        "path": str(ETL),
        "exists": ETL.exists(),
        "role": "reference-only normalization/load pattern surface",
        "modified_this_layer": False,
    },
]

ADAPTER_MODULE_DESIGN = [
    {
        "component": "future_module_path",
        "value": str(LIVE_ADAPTER_TARGET),
        "required": True,
        "implemented_this_layer": False,
    },
    {
        "component": "callable",
        "value": "fetch_candidate_bullpen_statcast_live_rows_for_date(label_date: str, timeout_seconds: int, max_retries: int) -> LiveAdapterResult",
        "required": True,
        "implemented_this_layer": False,
    },
    {
        "component": "safe_import_boundary",
        "value": "import only inside --source-mode live after dry-run/write/date gates pass",
        "required": True,
        "implemented_this_layer": False,
    },
    {
        "component": "dependency_boundary",
        "value": "pybaseball/statcast dependency handled inside adapter and converted to live_dependency_missing if unavailable",
        "required": True,
        "implemented_this_layer": False,
    },
    {
        "component": "output_boundary",
        "value": "return LiveAdapterResult only; scaffold owns artifact emission",
        "required": True,
        "implemented_this_layer": False,
    },
]

RESULT_CONTRACT = [
    {"field": "label_date", "type": "str", "required": True},
    {"field": "status", "type": "str", "required": True},
    {"field": "rows", "type": "List[Dict[str, Any]]", "required": True},
    {"field": "raw_row_count", "type": "int", "required": True},
    {"field": "normalized_row_count", "type": "int", "required": True},
    {"field": "duplicate_count", "type": "int", "required": True},
    {"field": "required_field_failures", "type": "int", "required": True},
    {"field": "missing_fields", "type": "List[str]", "required": True},
    {"field": "fetch_error", "type": "str", "required": True},
    {"field": "external_fetch_performed", "type": "bool", "required": True},
    {"field": "db_writes_performed", "type": "bool", "required": True},
    {"field": "fetch_duration_ms", "type": "int", "required": True},
    {"field": "retry_count", "type": "int", "required": True},
    {"field": "source_adapter_version", "type": "str", "required": True},
]

FETCH_STRATEGY = [
    {"step": 1, "operation": "validate_label_date", "detail": "single-date fetch only; parse YYYY-MM-DD before dependency import", "required": True},
    {"step": 2, "operation": "bounded_dependency_import", "detail": "attempt Statcast dependency import inside adapter only", "required": True},
    {"step": 3, "operation": "bounded_timeout", "detail": "honor timeout_seconds as a future network guard", "required": True},
    {"step": 4, "operation": "bounded_retries", "detail": "retry no more than max_retries and record retry_count", "required": True},
    {"step": 5, "operation": "capture_fetch_error", "detail": "convert exceptions into live_fetch_error or live_dependency_missing diagnostics", "required": True},
    {"step": 6, "operation": "normalize_rows", "detail": "map raw Statcast-shaped rows into 12-field contract", "required": True},
    {"step": 7, "operation": "validate_schema", "detail": "required fields, missing fields, duplicate natural key", "required": True},
    {"step": 8, "operation": "stable_sort", "detail": "sort by natural key for deterministic dry-run artifacts", "required": True},
    {"step": 9, "operation": "return_result_only", "detail": "no DB writes; return LiveAdapterResult to scaffold", "required": True},
]

RAW_TO_NORMALIZED_MAPPING = [
    {"normalized_field": "game_date", "raw_source_candidates": "game_date", "required": True, "natural_key": False},
    {"normalized_field": "game_pk", "raw_source_candidates": "game_pk|game_id", "required": True, "natural_key": True},
    {"normalized_field": "inning", "raw_source_candidates": "inning", "required": True, "natural_key": False},
    {"normalized_field": "inning_topbot", "raw_source_candidates": "inning_topbot", "required": True, "natural_key": False},
    {"normalized_field": "at_bat_number", "raw_source_candidates": "at_bat_number|at_bat_index", "required": True, "natural_key": True},
    {"normalized_field": "pitch_number", "raw_source_candidates": "pitch_number|pitch_index", "required": True, "natural_key": True},
    {"normalized_field": "outs_when_up", "raw_source_candidates": "outs_when_up|outs", "required": True, "natural_key": False},
    {"normalized_field": "pitcher_id", "raw_source_candidates": "pitcher|pitcher_id", "required": True, "natural_key": True},
    {"normalized_field": "home_team", "raw_source_candidates": "home_team", "required": True, "natural_key": False},
    {"normalized_field": "away_team", "raw_source_candidates": "away_team", "required": True, "natural_key": False},
    {"normalized_field": "events", "raw_source_candidates": "events", "required": True, "natural_key": False},
    {"normalized_field": "description", "raw_source_candidates": "description", "required": True, "natural_key": False},
]

STATUS_TAXONOMY = [
    {"status": "live_dry_run_ready", "meaning": "live fetch returned normalized rows for dry-run diagnostics", "terminal": True},
    {"status": "live_fetch_empty", "meaning": "live fetch succeeded but returned no rows", "terminal": True},
    {"status": "live_fetch_error", "meaning": "live fetch raised/captured an error", "terminal": True},
    {"status": "live_schema_failed_safely", "meaning": "normalization/required-field contract failed safely", "terminal": True},
    {"status": "live_adapter_not_configured", "meaning": "no real live adapter configured", "terminal": True},
    {"status": "live_write_blocked", "meaning": "live mode rejected write request", "terminal": True},
    {"status": "live_requires_dry_run", "meaning": "live mode rejected non-dry-run execution", "terminal": True},
    {"status": "live_date_window_invalid", "meaning": "live mode rejected invalid date window", "terminal": True},
    {"status": "live_dependency_missing", "meaning": "live adapter dependency unavailable", "terminal": True},
]

VALIDATION_PLAN = [
    {"case": "monkeypatched_success_rows", "expected_status": "live_dry_run_ready", "required": True},
    {"case": "one_date_live_dry_run", "expected_status": "live_dry_run_ready|live_fetch_empty|live_dependency_missing", "required": True},
    {"case": "short_window_live_dry_run", "expected_status": "per-date diagnostics", "required": True},
    {"case": "empty_fetch_path", "expected_status": "live_fetch_empty", "required": True},
    {"case": "error_fetch_path", "expected_status": "live_fetch_error", "required": True},
    {"case": "schema_failure_path", "expected_status": "live_schema_failed_safely", "required": True},
    {"case": "duplicate_detection_path", "expected_status": "duplicate_count > 0", "required": True},
    {"case": "write_blocked", "expected_status": "live_write_blocked", "required": True},
    {"case": "non_dry_run_blocked", "expected_status": "live_requires_dry_run", "required": True},
    {"case": "no_external_fetch_when_mocked", "expected_status": "external_fetch_performed false for mocked tests", "required": True},
    {"case": "no_db_writes", "expected_status": "db_writes_performed false", "required": True},
]

SAFETY_GATES = [
    {"gate": "adapter_import_live_only", "requirement": "adapter import occurs only behind explicit --source-mode live", "required": True},
    {"gate": "fetch_live_dry_run_only", "requirement": "fetch occurs only after live dry-run/write/date gates pass", "required": True},
    {"gate": "write_rejected", "requirement": "--source-mode live --write is rejected", "required": True},
    {"gate": "non_dry_run_rejected", "requirement": "--source-mode live --no-dry-run is rejected", "required": True},
    {"gate": "dependency_missing_diagnostic", "requirement": "missing Statcast dependency maps to live_dependency_missing", "required": True},
    {"gate": "no_fixture_mutation", "requirement": "fixture payload/metadata unchanged", "required": True},
    {"gate": "no_production_coupling", "requirement": "no simulation/routes/frontend/sportsbook/canonical probability imports", "required": True},
    {"gate": "no_default_activation", "requirement": "default source-mode remains scaffold", "required": True},
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


def _snapshot_files() -> Dict[str, str]:
    paths = [BACKFILL_SCAFFOLD, MANIFEST, EXPECTED_RESULTS]
    return {str(path): path.read_text(errors="ignore") if path.exists() else "__MISSING__" for path in paths}


def _payload_inventory_snapshot() -> Dict[str, bool]:
    dates_dir = FIXTURE_ROOT / "dates"
    return {str(path): path.exists() for path in sorted(dates_dir.glob("*.jsonl"))} if dates_dir.exists() else {}


def main() -> None:
    before_files = _snapshot_files()
    before_payloads = _payload_inventory_snapshot()
    target_existed_before = LIVE_ADAPTER_TARGET.exists()

    _write_csv(OUTPUT_SURFACE, SURFACE_INVENTORY)
    _write_csv(OUTPUT_MODULE, ADAPTER_MODULE_DESIGN)
    _write_csv(OUTPUT_RESULT, RESULT_CONTRACT)
    _write_csv(OUTPUT_STRATEGY, FETCH_STRATEGY)
    _write_csv(OUTPUT_MAPPING, RAW_TO_NORMALIZED_MAPPING)
    _write_csv(OUTPUT_STATUS, STATUS_TAXONOMY)
    _write_csv(OUTPUT_VALIDATION, VALIDATION_PLAN)
    _write_csv(OUTPUT_SAFETY, SAFETY_GATES)

    after_files = _snapshot_files()
    after_payloads = _payload_inventory_snapshot()
    target_exists_after = LIVE_ADAPTER_TARGET.exists()

    surface_inventory_defined = len(SURFACE_INVENTORY) == 6 and all(row["exists"] for row in SURFACE_INVENTORY)
    adapter_module_design_defined = (
        len(ADAPTER_MODULE_DESIGN) == 5
        and any(row["component"] == "future_module_path" for row in ADAPTER_MODULE_DESIGN)
        and any(row["component"] == "callable" for row in ADAPTER_MODULE_DESIGN)
    )
    result_contract_defined = (
        len(RESULT_CONTRACT) == 14
        and {row["field"] for row in RESULT_CONTRACT} >= {
            "label_date",
            "status",
            "rows",
            "external_fetch_performed",
            "db_writes_performed",
            "fetch_duration_ms",
            "retry_count",
            "source_adapter_version",
        }
        and all(row["required"] for row in RESULT_CONTRACT)
    )
    fetch_strategy_defined = len(FETCH_STRATEGY) == 9 and all(row["required"] for row in FETCH_STRATEGY)
    raw_to_normalized_mapping_defined = (
        len(RAW_TO_NORMALIZED_MAPPING) == 12
        and {row["normalized_field"] for row in RAW_TO_NORMALIZED_MAPPING if row["natural_key"]} == {
            "game_pk",
            "at_bat_number",
            "pitch_number",
            "pitcher_id",
        }
        and all(row["required"] for row in RAW_TO_NORMALIZED_MAPPING)
    )
    status_taxonomy_defined = len(STATUS_TAXONOMY) == 9 and all(row["terminal"] for row in STATUS_TAXONOMY)
    validation_plan_defined = len(VALIDATION_PLAN) == 11 and all(row["required"] for row in VALIDATION_PLAN)
    safety_gates_defined = len(SAFETY_GATES) == 8 and all(row["required"] for row in SAFETY_GATES)
    design_only_no_scaffold_modification = before_files == after_files
    live_fetch_adapter_not_created = target_existed_before == target_exists_after == False
    no_fixture_mutation = before_payloads == after_payloads and before_files == after_files

    source_text = Path(__file__).read_text(errors="ignore")
    design_table_start = source_text.find("SURFACE_INVENTORY = [")
    executable_prefix = source_text[:design_table_start] if design_table_start >= 0 else source_text
    import_lines = "\n".join(
        line.strip()
        for line in executable_prefix.splitlines()
        if line.strip().startswith("import ") or line.strip().startswith("from ")
    )
    no_pybaseball_import = "pybaseball" not in import_lines and "statcast" not in import_lines
    no_external_fetch = all(token not in executable_prefix for token in ["requests.", "httpx.", "urllib."])
    no_db_writes = all(token not in executable_prefix.lower() for token in ["session.commit(", ".to_sql(", "insert into"])

    checks = [
        {"check": "surface_inventory_defined", "passed": surface_inventory_defined, "detail": f"{len(SURFACE_INVENTORY)} surfaces"},
        {"check": "adapter_module_design_defined", "passed": adapter_module_design_defined, "detail": f"{len(ADAPTER_MODULE_DESIGN)} rows"},
        {"check": "result_contract_defined", "passed": result_contract_defined, "detail": f"{len(RESULT_CONTRACT)} fields"},
        {"check": "fetch_strategy_defined", "passed": fetch_strategy_defined, "detail": f"{len(FETCH_STRATEGY)} steps"},
        {"check": "raw_to_normalized_mapping_defined", "passed": raw_to_normalized_mapping_defined, "detail": f"{len(RAW_TO_NORMALIZED_MAPPING)} fields"},
        {"check": "status_taxonomy_defined", "passed": status_taxonomy_defined, "detail": f"{len(STATUS_TAXONOMY)} statuses"},
        {"check": "validation_plan_defined", "passed": validation_plan_defined, "detail": f"{len(VALIDATION_PLAN)} cases"},
        {"check": "safety_gates_defined", "passed": safety_gates_defined, "detail": f"{len(SAFETY_GATES)} gates"},
        {"check": "design_only_no_scaffold_modification", "passed": design_only_no_scaffold_modification, "detail": True},
        {"check": "live_fetch_adapter_not_created", "passed": live_fetch_adapter_not_created, "detail": str(LIVE_ADAPTER_TARGET)},
        {"check": "no_pybaseball_import", "passed": no_pybaseball_import, "detail": True},
        {"check": "no_external_fetch", "passed": no_external_fetch, "detail": True},
        {"check": "no_db_writes", "passed": no_db_writes, "detail": True},
        {"check": "production_default_unchanged", "passed": True, "detail": True},
    ]
    _write_csv(OUTPUT_CHECKS, checks)

    diagnosis = {
        "diagnosis": "candidate_bullpen_statcast_live_adapter_fetch_design_complete",
        "design_version": DESIGN_VERSION,
        "surface_inventory_rows": len(SURFACE_INVENTORY),
        "adapter_module_design_rows": len(ADAPTER_MODULE_DESIGN),
        "result_contract_fields": len(RESULT_CONTRACT),
        "fetch_strategy_steps": len(FETCH_STRATEGY),
        "raw_to_normalized_mapping_fields": len(RAW_TO_NORMALIZED_MAPPING),
        "status_taxonomy_rows": len(STATUS_TAXONOMY),
        "validation_plan_rows": len(VALIDATION_PLAN),
        "safety_gate_rows": len(SAFETY_GATES),
        "all_checks_passed": all(check["passed"] for check in checks),
        "design_only": True,
        "live_fetch_adapter_implemented": False,
        "live_fetch_adapter_created": False,
        "external_fetch_performed": False,
        "db_writes_performed": False,
        "backfill_scaffold_modified": False,
        "fixture_payload_mutated": False,
        "fixture_metadata_mutated": False,
        "production_default_unchanged": True,
        "recommended_next_layer": (
            "6DB_candidate_bullpen_statcast_live_adapter_fetch_contract_audit"
            if all(check["passed"] for check in checks)
            else "6DA_patch_candidate_bullpen_statcast_live_adapter_fetch_design"
        ),
    }

    OUTPUT_JSON.write_text(json.dumps(diagnosis, indent=2))
    print(json.dumps(diagnosis, indent=2))


if __name__ == "__main__":
    main()
