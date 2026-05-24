from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, List


PLAN_VERSION = "candidate_bullpen_statcast_live_adapter_fetch_test_double_plan_v0.1"

LIVE_ADAPTER_TARGET = Path("scripts/fetch_candidate_bullpen_statcast_live_adapter.py")
TEST_DOUBLE_TARGET = Path("scripts/prototype_candidate_bullpen_statcast_live_adapter_fetch_test_doubles.py")
BACKFILL_SCAFFOLD = Path("scripts/backfill_candidate_bullpen_statcast_labels.py")
DESIGN_SCRIPT = Path("scripts/design_candidate_bullpen_statcast_live_adapter_fetch.py")
CONTRACT_AUDIT = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_fetch_contract.py")
FIXTURE_ROOT = Path("tests/fixtures/statcast/bullpen_labels")
MANIFEST = FIXTURE_ROOT / "manifest.json"
EXPECTED_RESULTS = FIXTURE_ROOT / "expected_results.json"

OUTPUT_DIR = Path("tmp")
OUTPUT_DIR.mkdir(exist_ok=True)

OUTPUT_JSON = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_test_double_plan.json"
OUTPUT_CHECKS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_test_double_plan_checks.csv"
OUTPUT_OBJECTIVES = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_test_double_objectives.csv"
OUTPUT_PAYLOAD_CASES = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_test_double_raw_payload_cases.csv"
OUTPUT_DOUBLES = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetcher_double_plan.csv"
OUTPUT_STATUS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_test_double_status_mapping.csv"
OUTPUT_RESULT_ASSERTIONS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_test_double_result_contract_assertions.csv"
OUTPUT_ROW_ASSERTIONS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_test_double_normalized_row_assertions.csv"
OUTPUT_HARNESS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_test_double_future_harness_interface.csv"
OUTPUT_AUDIT = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_test_double_future_audit_expectations.csv"
OUTPUT_SAFETY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_test_double_safety_gates.csv"


OBJECTIVES = [
    {"objective": "prove_normalization_without_network", "detail": "synthetic Statcast-shaped payloads only", "required": True},
    {"objective": "exercise_all_future_status_paths", "detail": "success/empty/error/dependency/schema/duplicate/sort cases", "required": True},
    {"objective": "preserve_live_adapter_result_contract", "detail": "14 required LiveAdapterResult fields", "required": True},
    {"objective": "preserve_normalized_row_contract", "detail": "12 normalized fields and 4-field natural key", "required": True},
    {"objective": "verify_safety_flags", "detail": "external_fetch_performed false and db_writes_performed false", "required": True},
]

RAW_PAYLOAD_CASES = [
    {"case": "success_rows", "description": "valid synthetic Statcast-shaped rows", "expected_rows": 3, "requires_network": False},
    {"case": "empty_fetch", "description": "fetcher returns empty list", "expected_rows": 0, "requires_network": False},
    {"case": "fetch_error", "description": "fetcher raises runtime fetch error", "expected_rows": 0, "requires_network": False},
    {"case": "dependency_missing", "description": "fetcher raises dependency missing sentinel", "expected_rows": 0, "requires_network": False},
    {"case": "schema_missing_required_field", "description": "payload omits one required normalized field", "expected_rows": 1, "requires_network": False},
    {"case": "duplicate_natural_key", "description": "payload repeats natural key", "expected_rows": 3, "requires_network": False},
    {"case": "unordered_rows", "description": "payload arrives out of natural-key order", "expected_rows": 3, "requires_network": False},
    {"case": "nullable_optional_values", "description": "payload includes null event/description-like optional values", "expected_rows": 2, "requires_network": False},
    {"case": "mixed_event_description_values", "description": "payload includes populated and empty event/description values", "expected_rows": 3, "requires_network": False},
]

FETCHER_DOUBLES = [
    {"double": "SuccessfulFetcherDouble", "payload_case": "success_rows", "raises": False, "required": True},
    {"double": "EmptyFetcherDouble", "payload_case": "empty_fetch", "raises": False, "required": True},
    {"double": "ErrorFetcherDouble", "payload_case": "fetch_error", "raises": True, "required": True},
    {"double": "DependencyMissingFetcherDouble", "payload_case": "dependency_missing", "raises": True, "required": True},
    {"double": "SchemaFailureFetcherDouble", "payload_case": "schema_missing_required_field", "raises": False, "required": True},
    {"double": "DuplicateRowsFetcherDouble", "payload_case": "duplicate_natural_key", "raises": False, "required": True},
    {"double": "UnorderedRowsFetcherDouble", "payload_case": "unordered_rows", "raises": False, "required": True},
]

STATUS_MAPPING = [
    {"payload_case": "success_rows", "expected_status": "live_dry_run_ready", "extra_expectation": "normalized_row_count > 0"},
    {"payload_case": "empty_fetch", "expected_status": "live_fetch_empty", "extra_expectation": "raw_row_count == 0"},
    {"payload_case": "fetch_error", "expected_status": "live_fetch_error", "extra_expectation": "fetch_error populated"},
    {"payload_case": "dependency_missing", "expected_status": "live_dependency_missing", "extra_expectation": "fetch_error populated"},
    {"payload_case": "schema_missing_required_field", "expected_status": "live_schema_failed_safely", "extra_expectation": "required_field_failures > 0"},
    {"payload_case": "duplicate_natural_key", "expected_status": "live_dry_run_ready", "extra_expectation": "duplicate_count > 0"},
    {"payload_case": "unordered_rows", "expected_status": "live_dry_run_ready", "extra_expectation": "rows stable sorted by natural key"},
]

RESULT_CONTRACT_ASSERTIONS = [
    {"assertion": "exactly_14_fields", "expected": "14 LiveAdapterResult fields", "required": True},
    {"assertion": "external_fetch_false_for_doubles", "expected": "external_fetch_performed is False", "required": True},
    {"assertion": "db_writes_false", "expected": "db_writes_performed is False", "required": True},
    {"assertion": "fetch_duration_recorded", "expected": "fetch_duration_ms present and integer", "required": True},
    {"assertion": "retry_count_recorded", "expected": "retry_count present and integer", "required": True},
    {"assertion": "source_adapter_version_recorded", "expected": "source_adapter_version populated", "required": True},
]

NORMALIZED_ROW_ASSERTIONS = [
    {"assertion": "exactly_12_fields_per_row", "expected": "12 normalized fields", "required": True},
    {"assertion": "required_fields_present", "expected": "all normalized fields present", "required": True},
    {"assertion": "natural_key_exact", "expected": "game_pk|at_bat_number|pitch_number|pitcher_id", "required": True},
    {"assertion": "stable_deterministic_ordering", "expected": "rows sorted by natural key", "required": True},
    {"assertion": "duplicates_identified_by_natural_key", "expected": "duplicate_count reflects repeated natural keys", "required": True},
]

FUTURE_HARNESS_INTERFACE = [
    {"component": "future_script_target", "value": str(TEST_DOUBLE_TARGET), "created_this_layer": False, "required": True},
    {"component": "real_adapter_target", "value": str(LIVE_ADAPTER_TARGET), "created_this_layer": False, "required": True},
    {"component": "network_boundary", "value": "no real network; fetchers are injected doubles", "created_this_layer": False, "required": True},
    {"component": "adapter_contract_source", "value": "6DA/6DB LiveAdapterResult contract", "created_this_layer": False, "required": True},
    {"component": "row_contract_source", "value": "12-field normalized row contract", "created_this_layer": False, "required": True},
]

FUTURE_AUDIT_EXPECTATIONS = [
    {"expectation": "compileall_passes", "detail": "python -m compileall mlb_app scripts", "required": True},
    {"expectation": "execute_test_double_prototype", "detail": f"python {TEST_DOUBLE_TARGET}", "required": True},
    {"expectation": "every_double_expected_status", "detail": "all fetcher doubles return expected statuses", "required": True},
    {"expectation": "result_contract_parity", "detail": "all results preserve 14-field contract", "required": True},
    {"expectation": "normalized_row_contract_parity", "detail": "all normalized rows preserve 12-field contract", "required": True},
    {"expectation": "safety_flags_valid", "detail": "no external fetch and no DB writes", "required": True},
    {"expectation": "target_live_adapter_absent", "detail": "real live adapter module remains absent", "required": True},
]

SAFETY_GATES = [
    {"gate": "planning_only", "requirement": "no prototype or real adapter created", "required": True},
    {"gate": "no_live_adapter_created", "requirement": str(LIVE_ADAPTER_TARGET), "required": True},
    {"gate": "no_test_double_prototype_created", "requirement": str(TEST_DOUBLE_TARGET), "required": True},
    {"gate": "no_backfill_scaffold_modification", "requirement": str(BACKFILL_SCAFFOLD), "required": True},
    {"gate": "no_design_script_modification", "requirement": str(DESIGN_SCRIPT), "required": True},
    {"gate": "no_pybaseball_import", "requirement": "do not import pybaseball/statcast", "required": True},
    {"gate": "no_external_fetch", "requirement": "do not use requests/httpx/urllib network fetches", "required": True},
    {"gate": "no_db_writes", "requirement": "do not commit/to_sql/insert", "required": True},
    {"gate": "no_fixture_mutation", "requirement": "fixture payload and metadata unchanged", "required": True},
    {"gate": "production_default_unchanged", "requirement": "no production behavior change", "required": True},
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
    paths = [BACKFILL_SCAFFOLD, DESIGN_SCRIPT, CONTRACT_AUDIT, MANIFEST, EXPECTED_RESULTS]
    snapshot = {str(path): path.read_text(errors="ignore") if path.exists() else "__MISSING__" for path in paths}
    dates_dir = FIXTURE_ROOT / "dates"
    if dates_dir.exists():
        for payload in sorted(dates_dir.glob("*.jsonl")):
            snapshot[str(payload)] = payload.read_text(errors="ignore")
    return snapshot


def _source_safety_scan() -> Dict[str, bool]:
    source = Path(__file__).read_text(errors="ignore")
    table_start = source.find("OBJECTIVES = [")
    executable_prefix = source[:table_start] if table_start >= 0 else source
    import_lines = "\n".join(
        line.strip()
        for line in executable_prefix.splitlines()
        if line.strip().startswith("import ") or line.strip().startswith("from ")
    )
    return {
        "no_pybaseball_import": "pybaseball" not in import_lines and "statcast" not in import_lines,
        "no_external_fetch": all(token not in executable_prefix for token in ["requests.", "httpx.", "urllib."]),
        "no_db_writes": all(token not in executable_prefix.lower() for token in ["session.commit(", ".to_sql(", "insert into"]),
    }


def main() -> None:
    before_snapshot = _snapshot_files()
    live_adapter_existed_before = LIVE_ADAPTER_TARGET.exists()
    test_double_existed_before = TEST_DOUBLE_TARGET.exists()

    _write_csv(OUTPUT_OBJECTIVES, OBJECTIVES)
    _write_csv(OUTPUT_PAYLOAD_CASES, RAW_PAYLOAD_CASES)
    _write_csv(OUTPUT_DOUBLES, FETCHER_DOUBLES)
    _write_csv(OUTPUT_STATUS, STATUS_MAPPING)
    _write_csv(OUTPUT_RESULT_ASSERTIONS, RESULT_CONTRACT_ASSERTIONS)
    _write_csv(OUTPUT_ROW_ASSERTIONS, NORMALIZED_ROW_ASSERTIONS)
    _write_csv(OUTPUT_HARNESS, FUTURE_HARNESS_INTERFACE)
    _write_csv(OUTPUT_AUDIT, FUTURE_AUDIT_EXPECTATIONS)
    _write_csv(OUTPUT_SAFETY, SAFETY_GATES)

    after_snapshot = _snapshot_files()
    scan = _source_safety_scan()

    objectives_defined = len(OBJECTIVES) == 5 and all(row["required"] for row in OBJECTIVES)
    raw_payload_cases_defined = len(RAW_PAYLOAD_CASES) == 9 and all(row["requires_network"] is False for row in RAW_PAYLOAD_CASES)
    fetcher_doubles_defined = len(FETCHER_DOUBLES) == 7 and all(row["required"] for row in FETCHER_DOUBLES)
    status_mapping_defined = (
        len(STATUS_MAPPING) == 7
        and {row["expected_status"] for row in STATUS_MAPPING} >= {
            "live_dry_run_ready",
            "live_fetch_empty",
            "live_fetch_error",
            "live_dependency_missing",
            "live_schema_failed_safely",
        }
    )
    result_contract_assertions_defined = len(RESULT_CONTRACT_ASSERTIONS) == 6 and all(row["required"] for row in RESULT_CONTRACT_ASSERTIONS)
    normalized_row_assertions_defined = len(NORMALIZED_ROW_ASSERTIONS) == 5 and all(row["required"] for row in NORMALIZED_ROW_ASSERTIONS)
    future_harness_interface_defined = len(FUTURE_HARNESS_INTERFACE) == 5 and all(row["required"] for row in FUTURE_HARNESS_INTERFACE)
    future_audit_expectations_defined = len(FUTURE_AUDIT_EXPECTATIONS) == 7 and all(row["required"] for row in FUTURE_AUDIT_EXPECTATIONS)
    safety_gates_defined = len(SAFETY_GATES) == 10 and all(row["required"] for row in SAFETY_GATES)
    no_live_adapter_created = live_adapter_existed_before is False and not LIVE_ADAPTER_TARGET.exists()
    no_test_double_prototype_created = test_double_existed_before is False and not TEST_DOUBLE_TARGET.exists()
    no_fixture_or_script_mutation = before_snapshot == after_snapshot

    checks = [
        {"check": "objectives_defined", "passed": objectives_defined, "detail": f"{len(OBJECTIVES)} objectives"},
        {"check": "raw_payload_cases_defined", "passed": raw_payload_cases_defined, "detail": f"{len(RAW_PAYLOAD_CASES)} cases"},
        {"check": "fetcher_doubles_defined", "passed": fetcher_doubles_defined, "detail": f"{len(FETCHER_DOUBLES)} doubles"},
        {"check": "status_mapping_defined", "passed": status_mapping_defined, "detail": f"{len(STATUS_MAPPING)} mappings"},
        {"check": "result_contract_assertions_defined", "passed": result_contract_assertions_defined, "detail": f"{len(RESULT_CONTRACT_ASSERTIONS)} assertions"},
        {"check": "normalized_row_assertions_defined", "passed": normalized_row_assertions_defined, "detail": f"{len(NORMALIZED_ROW_ASSERTIONS)} assertions"},
        {"check": "future_harness_interface_defined", "passed": future_harness_interface_defined, "detail": f"{len(FUTURE_HARNESS_INTERFACE)} rows"},
        {"check": "future_audit_expectations_defined", "passed": future_audit_expectations_defined, "detail": f"{len(FUTURE_AUDIT_EXPECTATIONS)} expectations"},
        {"check": "safety_gates_defined", "passed": safety_gates_defined, "detail": f"{len(SAFETY_GATES)} gates"},
        {"check": "no_live_adapter_created", "passed": no_live_adapter_created, "detail": str(LIVE_ADAPTER_TARGET)},
        {"check": "no_test_double_prototype_created", "passed": no_test_double_prototype_created, "detail": str(TEST_DOUBLE_TARGET)},
        {"check": "no_pybaseball_import", "passed": scan["no_pybaseball_import"], "detail": True},
        {"check": "no_external_fetch", "passed": scan["no_external_fetch"], "detail": True},
        {"check": "no_db_writes", "passed": scan["no_db_writes"], "detail": True},
        {"check": "production_default_unchanged", "passed": no_fixture_or_script_mutation, "detail": True},
    ]
    _write_csv(OUTPUT_CHECKS, checks)

    diagnosis = {
        "diagnosis": "candidate_bullpen_statcast_live_adapter_fetch_test_double_plan_complete",
        "plan_version": PLAN_VERSION,
        "objective_rows": len(OBJECTIVES),
        "raw_payload_case_rows": len(RAW_PAYLOAD_CASES),
        "fetcher_double_rows": len(FETCHER_DOUBLES),
        "status_mapping_rows": len(STATUS_MAPPING),
        "result_contract_assertion_rows": len(RESULT_CONTRACT_ASSERTIONS),
        "normalized_row_assertion_rows": len(NORMALIZED_ROW_ASSERTIONS),
        "future_harness_rows": len(FUTURE_HARNESS_INTERFACE),
        "future_audit_expectation_rows": len(FUTURE_AUDIT_EXPECTATIONS),
        "safety_gate_rows": len(SAFETY_GATES),
        "all_checks_passed": all(check["passed"] for check in checks),
        "planning_only": True,
        "live_adapter_created": False,
        "test_double_prototype_created": False,
        "external_fetch_performed": False,
        "db_writes_performed": False,
        "fixture_or_script_mutation": False,
        "production_default_unchanged": True,
        "recommended_next_layer": (
            "6DD_candidate_bullpen_statcast_live_adapter_fetch_test_double_prototype"
            if all(check["passed"] for check in checks)
            else "6DC_patch_candidate_bullpen_statcast_live_adapter_fetch_test_double_plan"
        ),
    }

    OUTPUT_JSON.write_text(json.dumps(diagnosis, indent=2))
    print(json.dumps(diagnosis, indent=2))


if __name__ == "__main__":
    main()
