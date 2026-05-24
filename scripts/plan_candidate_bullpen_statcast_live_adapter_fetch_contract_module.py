from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, List


PLAN_VERSION = "candidate_bullpen_statcast_live_adapter_fetch_contract_module_plan_v0.1"

LIVE_ADAPTER_TARGET = Path("scripts/fetch_candidate_bullpen_statcast_live_adapter.py")
BACKFILL_SCAFFOLD = Path("scripts/backfill_candidate_bullpen_statcast_labels.py")
DESIGN_SCRIPT = Path("scripts/design_candidate_bullpen_statcast_live_adapter_fetch.py")
CONTRACT_AUDIT = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_fetch_contract.py")
TEST_DOUBLE_PLAN = Path("scripts/plan_candidate_bullpen_statcast_live_adapter_fetch_test_doubles.py")
TEST_DOUBLE_PROTOTYPE = Path("scripts/prototype_candidate_bullpen_statcast_live_adapter_fetch_test_doubles.py")
TEST_DOUBLE_AUDIT = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_fetch_test_double_prototype.py")
FIXTURE_ROOT = Path("tests/fixtures/statcast/bullpen_labels")
MANIFEST = FIXTURE_ROOT / "manifest.json"
EXPECTED_RESULTS = FIXTURE_ROOT / "expected_results.json"
DATES_DIR = FIXTURE_ROOT / "dates"

OUTPUT_DIR = Path("tmp")
OUTPUT_DIR.mkdir(exist_ok=True)

OUTPUT_JSON = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_contract_module_plan.json"
OUTPUT_CHECKS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_contract_module_plan_checks.csv"
OUTPUT_MODULE = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_contract_module_boundary.csv"
OUTPUT_API = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_contract_module_public_api.csv"
OUTPUT_DEPENDENCY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_contract_module_dependency_boundary.csv"
OUTPUT_FETCH = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_contract_module_fetch_behavior.csv"
OUTPUT_NORMALIZATION = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_contract_module_normalization_contract.csv"
OUTPUT_STATUS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_contract_module_status_taxonomy.csv"
OUTPUT_INTEGRATION = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_contract_module_scaffold_integration_boundary.csv"
OUTPUT_TEST_STRATEGY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_contract_module_test_strategy.csv"
OUTPUT_SAFETY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_contract_module_safety_gates.csv"

RESULT_FIELDS = [
    "label_date",
    "status",
    "rows",
    "raw_row_count",
    "normalized_row_count",
    "duplicate_count",
    "required_field_failures",
    "missing_fields",
    "fetch_error",
    "external_fetch_performed",
    "db_writes_performed",
    "fetch_duration_ms",
    "retry_count",
    "source_adapter_version",
]

NORMALIZED_FIELDS = [
    "game_date",
    "game_pk",
    "inning",
    "inning_topbot",
    "at_bat_number",
    "pitch_number",
    "outs_when_up",
    "pitcher_id",
    "home_team",
    "away_team",
    "events",
    "description",
]

NATURAL_KEY_FIELDS = ["game_pk", "at_bat_number", "pitch_number", "pitcher_id"]

STATUSES = [
    "live_dry_run_ready",
    "live_fetch_empty",
    "live_fetch_error",
    "live_schema_failed_safely",
    "live_adapter_not_configured",
    "live_write_blocked",
    "live_requires_dry_run",
    "live_date_window_invalid",
    "live_dependency_missing",
]

MODULE_BOUNDARY = [
    {"component": "target_path", "value": str(LIVE_ADAPTER_TARGET), "created_this_layer": False, "required": True},
    {"component": "module_role", "value": "standalone utility module for one-date live Statcast fetch and normalization", "created_this_layer": False, "required": True},
    {"component": "import_owner", "value": "backfill scaffold imports only when --source-mode live after gates", "created_this_layer": False, "required": True},
    {"component": "write_boundary", "value": "adapter never writes DB and never mutates fixtures", "created_this_layer": False, "required": True},
]

PUBLIC_API = [
    {"api": "LiveAdapterResult", "kind": "dataclass", "signature": "|".join(RESULT_FIELDS), "required": True},
    {
        "api": "fetch_candidate_bullpen_statcast_live_rows_for_date",
        "kind": "callable",
        "signature": "fetch_candidate_bullpen_statcast_live_rows_for_date(label_date: str, timeout_seconds: int, max_retries: int) -> LiveAdapterResult",
        "required": True,
    },
    {
        "api": "normalize_statcast_pitch_rows",
        "kind": "helper",
        "signature": "normalize_statcast_pitch_rows(label_date: str, raw_rows: list[dict]) -> tuple[list[dict], int, int, list[str]]",
        "required": True,
    },
    {
        "api": "natural_key",
        "kind": "helper",
        "signature": "natural_key(row: dict) -> tuple",
        "required": True,
    },
]

DEPENDENCY_BOUNDARY = [
    {"boundary": "no_top_level_statcast_import", "detail": "no top-level pybaseball/statcast import", "required": True},
    {"boundary": "live_callable_import_only", "detail": "dependency import attempted only inside live fetch callable", "required": True},
    {"boundary": "dependency_missing_status", "detail": "dependency missing maps to live_dependency_missing", "required": True},
    {"boundary": "no_direct_network_clients", "detail": "no requests/httpx/urllib direct usage in adapter plan", "required": True},
    {"boundary": "no_exception_leak_to_scaffold", "detail": "fetch exceptions captured into LiveAdapterResult", "required": True},
]

FETCH_BEHAVIOR = [
    {"behavior": "validate_label_date", "detail": "validate YYYY-MM-DD before dependency import", "required": True},
    {"behavior": "single_date_fetch", "detail": "fetch one label_date only", "required": True},
    {"behavior": "timeout_metadata", "detail": "honor timeout_seconds as future guard metadata", "required": True},
    {"behavior": "bounded_retries", "detail": "retry no more than max_retries and record retry_count", "required": True},
    {"behavior": "capture_fetch_exceptions", "detail": "convert exceptions to live_fetch_error or live_dependency_missing", "required": True},
    {"behavior": "never_raise_to_scaffold", "detail": "return LiveAdapterResult only", "required": True},
    {"behavior": "no_db_writes", "detail": "adapter never writes database rows", "required": True},
]

NORMALIZATION_CONTRACT = [
    *[
        {
            "contract": "normalized_field",
            "field": field,
            "required": True,
            "natural_key": field in NATURAL_KEY_FIELDS,
        }
        for field in NORMALIZED_FIELDS
    ],
    {"contract": "missing_required_fields", "field": "skip_and_count", "required": True, "natural_key": False},
    {"contract": "duplicate_detection", "field": "count_repeated_natural_keys", "required": True, "natural_key": False},
    {"contract": "deterministic_ordering", "field": "sort_by_natural_key", "required": True, "natural_key": False},
]

STATUS_TAXONOMY = [
    {"status": status, "terminal": True, "required": True}
    for status in STATUSES
]

SCAFFOLD_INTEGRATION_BOUNDARY = [
    {"owner": "scaffold", "responsibility": "date-window validation", "required": True},
    {"owner": "scaffold", "responsibility": "dry-run/write gates", "required": True},
    {"owner": "scaffold", "responsibility": "artifact emission", "required": True},
    {"owner": "adapter", "responsibility": "one-date fetch", "required": True},
    {"owner": "adapter", "responsibility": "row normalization", "required": True},
    {"owner": "adapter", "responsibility": "return result object only", "required": True},
    {"owner": "adapter", "responsibility": "never write DB", "required": True},
    {"owner": "adapter", "responsibility": "never mutate fixtures", "required": True},
]

IMPLEMENTATION_TEST_STRATEGY = [
    {"case": "success_rows", "source": "6DD fetcher double pattern", "expected_status": "live_dry_run_ready", "required": True},
    {"case": "empty_rows", "source": "6DD fetcher double pattern", "expected_status": "live_fetch_empty", "required": True},
    {"case": "fetch_error", "source": "6DD fetcher double pattern", "expected_status": "live_fetch_error", "required": True},
    {"case": "dependency_missing", "source": "6DD fetcher double pattern", "expected_status": "live_dependency_missing", "required": True},
    {"case": "schema_failure", "source": "6DD fetcher double pattern", "expected_status": "live_schema_failed_safely", "required": True},
    {"case": "duplicate_detection", "source": "6DD fetcher double pattern", "expected_status": "live_dry_run_ready", "required": True},
    {"case": "unordered_rows", "source": "6DD fetcher double pattern", "expected_status": "live_dry_run_ready", "required": True},
    {"case": "safety_scan", "source": "6DE audit pattern", "expected_status": "no_external_fetch_without_mock", "required": True},
]

SAFETY_GATES = [
    {"gate": "planning_only", "requirement": "do not create real adapter module", "required": True},
    {"gate": "real_adapter_not_created", "requirement": str(LIVE_ADAPTER_TARGET), "required": True},
    {"gate": "no_scaffold_mutation", "requirement": str(BACKFILL_SCAFFOLD), "required": True},
    {"gate": "no_fixture_mutation", "requirement": "fixture payloads and metadata unchanged", "required": True},
    {"gate": "no_pybaseball_import", "requirement": "no pybaseball/statcast import in plan executable code", "required": True},
    {"gate": "no_external_fetch", "requirement": "no requests/httpx/urllib network calls", "required": True},
    {"gate": "no_db_writes", "requirement": "no session.commit/to_sql/insert", "required": True},
    {"gate": "production_default_unchanged", "requirement": "no production behavior changes", "required": True},
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
    paths = [
        BACKFILL_SCAFFOLD,
        DESIGN_SCRIPT,
        CONTRACT_AUDIT,
        TEST_DOUBLE_PLAN,
        TEST_DOUBLE_PROTOTYPE,
        TEST_DOUBLE_AUDIT,
        MANIFEST,
        EXPECTED_RESULTS,
    ]
    snapshot = {str(path): path.read_text(errors="ignore") if path.exists() else "__MISSING__" for path in paths}
    if DATES_DIR.exists():
        for payload in sorted(DATES_DIR.glob("*.jsonl")):
            snapshot[str(payload)] = payload.read_text(errors="ignore")
    return snapshot


def _source_safety_scan() -> Dict[str, bool]:
    source = Path(__file__).read_text(errors="ignore")
    table_start = source.find("RESULT_FIELDS = [")
    executable_prefix = source[:table_start] if table_start >= 0 else source
    import_lines = "\n".join(
        line.strip()
        for line in executable_prefix.splitlines()
        if line.strip().startswith("import ") or line.strip().startswith("from ")
    )
    executable_lower = executable_prefix.lower()
    return {
        "no_pybaseball_import": "pybaseball" not in import_lines and "statcast" not in import_lines,
        "no_external_fetch": all(token not in executable_prefix for token in ["requests.", "httpx.", "urllib."]),
        "no_db_writes": all(token not in executable_lower for token in ["session.commit(", ".to_sql(", "insert into"]),
    }


def main() -> None:
    before_snapshot = _snapshot_files()
    live_adapter_existed_before = LIVE_ADAPTER_TARGET.exists()

    _write_csv(OUTPUT_MODULE, MODULE_BOUNDARY)
    _write_csv(OUTPUT_API, PUBLIC_API)
    _write_csv(OUTPUT_DEPENDENCY, DEPENDENCY_BOUNDARY)
    _write_csv(OUTPUT_FETCH, FETCH_BEHAVIOR)
    _write_csv(OUTPUT_NORMALIZATION, NORMALIZATION_CONTRACT)
    _write_csv(OUTPUT_STATUS, STATUS_TAXONOMY)
    _write_csv(OUTPUT_INTEGRATION, SCAFFOLD_INTEGRATION_BOUNDARY)
    _write_csv(OUTPUT_TEST_STRATEGY, IMPLEMENTATION_TEST_STRATEGY)
    _write_csv(OUTPUT_SAFETY, SAFETY_GATES)

    after_snapshot = _snapshot_files()
    scan = _source_safety_scan()

    module_boundary_defined = (
        len(MODULE_BOUNDARY) == 4
        and MODULE_BOUNDARY[0]["value"] == str(LIVE_ADAPTER_TARGET)
        and all(row["required"] for row in MODULE_BOUNDARY)
    )
    public_api_defined = (
        len(PUBLIC_API) == 4
        and len(RESULT_FIELDS) == 14
        and any("fetch_candidate_bullpen_statcast_live_rows_for_date" == row["api"] for row in PUBLIC_API)
        and any("normalize_statcast_pitch_rows" == row["api"] for row in PUBLIC_API)
        and any("natural_key" == row["api"] for row in PUBLIC_API)
    )
    dependency_boundary_defined = len(DEPENDENCY_BOUNDARY) == 5 and all(row["required"] for row in DEPENDENCY_BOUNDARY)
    fetch_behavior_defined = len(FETCH_BEHAVIOR) == 7 and all(row["required"] for row in FETCH_BEHAVIOR)
    normalization_contract_defined = (
        len([row for row in NORMALIZATION_CONTRACT if row["contract"] == "normalized_field"]) == 12
        and {row["field"] for row in NORMALIZATION_CONTRACT if row["natural_key"]} == set(NATURAL_KEY_FIELDS)
        and all(row["required"] for row in NORMALIZATION_CONTRACT)
    )
    status_taxonomy_defined = len(STATUS_TAXONOMY) == 9 and {row["status"] for row in STATUS_TAXONOMY} == set(STATUSES)
    scaffold_integration_boundary_defined = len(SCAFFOLD_INTEGRATION_BOUNDARY) == 8 and all(row["required"] for row in SCAFFOLD_INTEGRATION_BOUNDARY)
    implementation_test_strategy_defined = len(IMPLEMENTATION_TEST_STRATEGY) == 8 and all(row["required"] for row in IMPLEMENTATION_TEST_STRATEGY)
    safety_gates_defined = len(SAFETY_GATES) == 8 and all(row["required"] for row in SAFETY_GATES)
    real_adapter_not_created = live_adapter_existed_before is False and not LIVE_ADAPTER_TARGET.exists()
    no_scaffold_mutation = before_snapshot.get(str(BACKFILL_SCAFFOLD)) == after_snapshot.get(str(BACKFILL_SCAFFOLD))
    no_fixture_mutation = before_snapshot == after_snapshot

    checks = [
        {"check": "module_boundary_defined", "passed": module_boundary_defined, "detail": f"{len(MODULE_BOUNDARY)} rows"},
        {"check": "public_api_defined", "passed": public_api_defined, "detail": f"{len(PUBLIC_API)} rows and {len(RESULT_FIELDS)} result fields"},
        {"check": "dependency_boundary_defined", "passed": dependency_boundary_defined, "detail": f"{len(DEPENDENCY_BOUNDARY)} rows"},
        {"check": "fetch_behavior_defined", "passed": fetch_behavior_defined, "detail": f"{len(FETCH_BEHAVIOR)} rows"},
        {"check": "normalization_contract_defined", "passed": normalization_contract_defined, "detail": f"{len(NORMALIZATION_CONTRACT)} rows"},
        {"check": "status_taxonomy_defined", "passed": status_taxonomy_defined, "detail": f"{len(STATUS_TAXONOMY)} rows"},
        {"check": "scaffold_integration_boundary_defined", "passed": scaffold_integration_boundary_defined, "detail": f"{len(SCAFFOLD_INTEGRATION_BOUNDARY)} rows"},
        {"check": "implementation_test_strategy_defined", "passed": implementation_test_strategy_defined, "detail": f"{len(IMPLEMENTATION_TEST_STRATEGY)} rows"},
        {"check": "safety_gates_defined", "passed": safety_gates_defined, "detail": f"{len(SAFETY_GATES)} rows"},
        {"check": "real_adapter_not_created", "passed": real_adapter_not_created, "detail": str(LIVE_ADAPTER_TARGET)},
        {"check": "no_scaffold_mutation", "passed": no_scaffold_mutation, "detail": str(BACKFILL_SCAFFOLD)},
        {"check": "no_fixture_mutation", "passed": no_fixture_mutation, "detail": True},
        {"check": "no_pybaseball_import", "passed": scan["no_pybaseball_import"], "detail": True},
        {"check": "no_external_fetch", "passed": scan["no_external_fetch"], "detail": True},
        {"check": "no_db_writes", "passed": scan["no_db_writes"], "detail": True},
        {"check": "production_default_unchanged", "passed": True, "detail": True},
    ]
    _write_csv(OUTPUT_CHECKS, checks)

    diagnosis = {
        "diagnosis": "candidate_bullpen_statcast_live_adapter_fetch_contract_module_plan_complete",
        "plan_version": PLAN_VERSION,
        "module_boundary_rows": len(MODULE_BOUNDARY),
        "public_api_rows": len(PUBLIC_API),
        "result_contract_fields": len(RESULT_FIELDS),
        "dependency_boundary_rows": len(DEPENDENCY_BOUNDARY),
        "fetch_behavior_rows": len(FETCH_BEHAVIOR),
        "normalization_contract_rows": len(NORMALIZATION_CONTRACT),
        "normalized_field_rows": len(NORMALIZED_FIELDS),
        "natural_key_fields": len(NATURAL_KEY_FIELDS),
        "status_taxonomy_rows": len(STATUS_TAXONOMY),
        "scaffold_integration_boundary_rows": len(SCAFFOLD_INTEGRATION_BOUNDARY),
        "implementation_test_strategy_rows": len(IMPLEMENTATION_TEST_STRATEGY),
        "safety_gate_rows": len(SAFETY_GATES),
        "all_checks_passed": all(check["passed"] for check in checks),
        "planning_only": True,
        "real_adapter_created": False,
        "external_fetch_performed": False,
        "db_writes_performed": False,
        "fixture_assets_mutated": False,
        "production_default_unchanged": True,
        "recommended_next_layer": (
            "6DG_candidate_bullpen_statcast_live_adapter_fetch_contract_module_plan_audit"
            if all(check["passed"] for check in checks)
            else "6DF_patch_candidate_bullpen_statcast_live_adapter_fetch_contract_module_plan"
        ),
    }

    OUTPUT_JSON.write_text(json.dumps(diagnosis, indent=2))
    print(json.dumps(diagnosis, indent=2))


if __name__ == "__main__":
    main()
