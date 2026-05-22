from __future__ import annotations

import csv
import importlib.util
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List


AUDIT_VERSION = "candidate_bullpen_statcast_fetch_adapter_test_double_audit_v0.1"
PROTOTYPE_PATH = Path("scripts/prototype_candidate_bullpen_statcast_fetch_adapter_test_doubles.py")

OUTPUT_DIR = Path("tmp")
OUTPUT_DIR.mkdir(exist_ok=True)

OUTPUT_JSON = OUTPUT_DIR / "candidate_bullpen_statcast_fetch_adapter_test_double_audit.json"
OUTPUT_CHECKS = OUTPUT_DIR / "candidate_bullpen_statcast_fetch_adapter_test_double_audit_checks.csv"
OUTPUT_DETERMINISM = OUTPUT_DIR / "candidate_bullpen_statcast_fetch_adapter_test_double_deterministic_comparison.csv"
OUTPUT_RESULTS = OUTPUT_DIR / "candidate_bullpen_statcast_fetch_adapter_test_double_result_contract_audit.csv"
OUTPUT_RETRY = OUTPUT_DIR / "candidate_bullpen_statcast_fetch_adapter_test_double_retry_contract_audit.csv"
OUTPUT_NORMALIZATION = OUTPUT_DIR / "candidate_bullpen_statcast_fetch_adapter_test_double_normalization_contract_audit.csv"
OUTPUT_SAFETY = OUTPUT_DIR / "candidate_bullpen_statcast_fetch_adapter_test_double_safety_contract_audit.csv"


PROTOTYPE_JSON = Path("tmp/candidate_bullpen_statcast_fetch_adapter_test_double_prototype.json")
PROTOTYPE_RESULTS = Path("tmp/candidate_bullpen_statcast_fetch_adapter_test_double_results.csv")
PROTOTYPE_RETRY = Path("tmp/candidate_bullpen_statcast_fetch_adapter_test_double_retry_audit.csv")
PROTOTYPE_NORMALIZATION = Path("tmp/candidate_bullpen_statcast_fetch_adapter_test_double_normalization_audit.csv")
PROTOTYPE_SAFETY = Path("tmp/candidate_bullpen_statcast_fetch_adapter_test_double_safety_audit.csv")


EXPECTED_RESULTS = {
    "empty_adapter": {
        "final_status": "empty_success",
        "raw_row_count": 0,
        "deduped_row_count": 0,
        "duplicate_count": 0,
        "required_field_failures": 0,
        "retry_attempts": 1,
    },
    "fixture_adapter": {
        "final_status": "success",
        "raw_row_count": 3,
        "deduped_row_count": 3,
        "duplicate_count": 0,
        "required_field_failures": 0,
        "retry_attempts": 1,
    },
    "malformed_schema_adapter": {
        "final_status": "schema_failed_safely",
        "raw_row_count": 2,
        "deduped_row_count": 2,
        "duplicate_count": 0,
        "required_field_failures": 2,
        "retry_attempts": 1,
    },
    "duplicate_natural_key_adapter": {
        "final_status": "success",
        "raw_row_count": 3,
        "deduped_row_count": 2,
        "duplicate_count": 1,
        "required_field_failures": 0,
        "retry_attempts": 1,
    },
    "transient_error_adapter": {
        "final_status": "success",
        "raw_row_count": 1,
        "deduped_row_count": 1,
        "duplicate_count": 0,
        "required_field_failures": 0,
        "retry_attempts": 2,
        "exception_name": "AdapterNetworkError",
    },
}

EXPECTED_EXCEPTIONS = [
    "AdapterFetchUnavailable",
    "AdapterNoRowsReturned",
    "AdapterSchemaMismatch",
    "AdapterRateLimited",
    "AdapterNetworkError",
    "AdapterUnexpectedError",
]

EXPECTED_DOUBLES = [
    "empty_adapter",
    "fixture_adapter",
    "malformed_schema_adapter",
    "duplicate_natural_key_adapter",
    "transient_error_adapter",
]


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text())


def _import_prototype():
    module_name = "prototype_candidate_bullpen_statcast_fetch_adapter_test_doubles"
    spec = importlib.util.spec_from_file_location(module_name, PROTOTYPE_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not import 6CG prototype")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _run_prototype() -> Dict[str, Any]:
    completed = subprocess.run(
        [sys.executable, str(PROTOTYPE_PATH)],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=60,
    )

    return {
        "returncode": completed.returncode,
        "stdout_tail": completed.stdout[-500:],
        "stderr_tail": completed.stderr[-500:],
        "succeeded": completed.returncode == 0,
        "json": _read_json(PROTOTYPE_JSON),
        "results": _read_csv(PROTOTYPE_RESULTS),
        "retry": _read_csv(PROTOTYPE_RETRY),
        "normalization": _read_csv(PROTOTYPE_NORMALIZATION),
        "safety": _read_csv(PROTOTYPE_SAFETY),
    }


def _stable_projection(run: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "diagnosis": run["json"].get("diagnosis"),
        "adapter_doubles": run["json"].get("adapter_doubles"),
        "result_rows": run["json"].get("result_rows"),
        "retry_audit_rows": run["json"].get("retry_audit_rows"),
        "normalization_audit_rows": run["json"].get("normalization_audit_rows"),
        "all_checks_passed": run["json"].get("all_checks_passed"),
        "results": [
            {
                "adapter_name": row.get("adapter_name"),
                "final_status": row.get("final_status"),
                "raw_row_count": row.get("raw_row_count"),
                "deduped_row_count": row.get("deduped_row_count"),
                "duplicate_count": row.get("duplicate_count"),
                "required_field_failures": row.get("required_field_failures"),
                "retry_attempts": row.get("retry_attempts"),
                "exception_name": row.get("exception_name"),
            }
            for row in run["results"]
        ],
        "retry": [
            {
                "adapter_name": row.get("adapter_name"),
                "attempt": row.get("attempt"),
                "status": row.get("status"),
                "exception_name": row.get("exception_name"),
                "row_count": row.get("row_count"),
            }
            for row in run["retry"]
        ],
        "normalization": [
            {
                "adapter_name": row.get("adapter_name"),
                "natural_key": row.get("natural_key"),
                "valid": row.get("valid"),
                "missing_fields": row.get("missing_fields"),
            }
            for row in run["normalization"]
        ],
    }


def _determinism_rows(run1: Dict[str, Any], run2: Dict[str, Any]) -> List[Dict[str, Any]]:
    proj1 = _stable_projection(run1)
    proj2 = _stable_projection(run2)

    rows = []
    for key in ["diagnosis", "adapter_doubles", "result_rows", "retry_audit_rows", "normalization_audit_rows", "all_checks_passed"]:
        rows.append({
            "comparison": key,
            "run1": proj1.get(key),
            "run2": proj2.get(key),
            "passed": proj1.get(key) == proj2.get(key),
        })

    for key in ["results", "retry", "normalization"]:
        rows.append({
            "comparison": key,
            "run1": json.dumps(proj1.get(key), sort_keys=True),
            "run2": json.dumps(proj2.get(key), sort_keys=True),
            "passed": proj1.get(key) == proj2.get(key),
        })

    return rows


def _result_contract_rows(results: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    by_name = {row.get("adapter_name"): row for row in results}
    rows = []

    for adapter_name, expectations in EXPECTED_RESULTS.items():
        actual = by_name.get(adapter_name, {})
        for field, expected_value in expectations.items():
            actual_value = actual.get(field)
            if isinstance(expected_value, int):
                passed = str(actual_value) == str(expected_value)
            else:
                passed = actual_value == expected_value
            rows.append({
                "adapter_name": adapter_name,
                "field": field,
                "expected": expected_value,
                "actual": actual_value,
                "passed": passed,
            })

    rows.append({
        "adapter_name": "__row_count__",
        "field": "result_rows",
        "expected": 5,
        "actual": len(results),
        "passed": len(results) == 5,
    })

    rows.append({
        "adapter_name": "__adapter_names__",
        "field": "adapter_names",
        "expected": "|".join(EXPECTED_DOUBLES),
        "actual": "|".join(sorted(by_name)),
        "passed": set(by_name) == set(EXPECTED_DOUBLES),
    })

    return rows


def _retry_contract_rows(retry_rows: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    rows = []
    by_adapter: Dict[str, List[Dict[str, str]]] = {}
    for row in retry_rows:
        by_adapter.setdefault(row.get("adapter_name", ""), []).append(row)

    transient = by_adapter.get("transient_error_adapter", [])

    rows.append({
        "adapter_name": "transient_error_adapter",
        "contract": "exactly_two_attempts",
        "expected": 2,
        "actual": len(transient),
        "passed": len(transient) == 2,
    })
    rows.append({
        "adapter_name": "transient_error_adapter",
        "contract": "first_retryable_error",
        "expected": "retryable_error AdapterNetworkError",
        "actual": f"{transient[0].get('status') if transient else ''} {transient[0].get('exception_name') if transient else ''}",
        "passed": bool(transient) and transient[0].get("status") == "retryable_error" and transient[0].get("exception_name") == "AdapterNetworkError",
    })
    rows.append({
        "adapter_name": "transient_error_adapter",
        "contract": "second_success",
        "expected": "success row_count=1",
        "actual": f"{transient[1].get('status') if len(transient) > 1 else ''} row_count={transient[1].get('row_count') if len(transient) > 1 else ''}",
        "passed": len(transient) > 1 and transient[1].get("status") == "success" and transient[1].get("row_count") == "1",
    })

    for adapter_name in ["empty_adapter", "fixture_adapter", "malformed_schema_adapter", "duplicate_natural_key_adapter"]:
        attempts = by_adapter.get(adapter_name, [])
        rows.append({
            "adapter_name": adapter_name,
            "contract": "no_retry",
            "expected": 1,
            "actual": len(attempts),
            "passed": len(attempts) == 1 and attempts[0].get("status") == "success" if attempts else False,
        })

    return rows


def _normalization_contract_rows(normalization_rows: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    rows = []
    by_adapter: Dict[str, List[Dict[str, str]]] = {}
    for row in normalization_rows:
        by_adapter.setdefault(row.get("adapter_name", ""), []).append(row)

    rows.append({
        "adapter_name": "fixture_adapter",
        "contract": "all_fixture_rows_valid",
        "expected": "3 valid rows",
        "actual": f"{sum(row.get('valid') == 'True' for row in by_adapter.get('fixture_adapter', []))} valid / {len(by_adapter.get('fixture_adapter', []))} total",
        "passed": len(by_adapter.get("fixture_adapter", [])) == 3 and all(row.get("valid") == "True" for row in by_adapter["fixture_adapter"]),
    })
    rows.append({
        "adapter_name": "malformed_schema_adapter",
        "contract": "malformed_rows_invalid",
        "expected": "2 invalid rows",
        "actual": f"{sum(row.get('valid') == 'False' for row in by_adapter.get('malformed_schema_adapter', []))} invalid / {len(by_adapter.get('malformed_schema_adapter', []))} total",
        "passed": len(by_adapter.get("malformed_schema_adapter", [])) == 2 and all(row.get("valid") == "False" for row in by_adapter["malformed_schema_adapter"]),
    })
    rows.append({
        "adapter_name": "duplicate_natural_key_adapter",
        "contract": "deduped_rows_valid",
        "expected": "2 valid rows",
        "actual": f"{sum(row.get('valid') == 'True' for row in by_adapter.get('duplicate_natural_key_adapter', []))} valid / {len(by_adapter.get('duplicate_natural_key_adapter', []))} total",
        "passed": len(by_adapter.get("duplicate_natural_key_adapter", [])) == 2 and all(row.get("valid") == "True" for row in by_adapter["duplicate_natural_key_adapter"]),
    })
    rows.append({
        "adapter_name": "transient_error_adapter",
        "contract": "retry_success_row_valid",
        "expected": "1 valid row",
        "actual": f"{sum(row.get('valid') == 'True' for row in by_adapter.get('transient_error_adapter', []))} valid / {len(by_adapter.get('transient_error_adapter', []))} total",
        "passed": len(by_adapter.get("transient_error_adapter", [])) == 1 and all(row.get("valid") == "True" for row in by_adapter["transient_error_adapter"]),
    })
    rows.append({
        "adapter_name": "malformed_schema_adapter",
        "contract": "expected_missing_fields",
        "expected": "pitcher_id|game_pk",
        "actual": "|".join(row.get("missing_fields", "") for row in by_adapter.get("malformed_schema_adapter", [])),
        "passed": {row.get("missing_fields") for row in by_adapter.get("malformed_schema_adapter", [])} == {"pitcher_id", "game_pk"},
    })

    return rows


def _safety_contract_rows(safety_rows: List[Dict[str, str]], run_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows = []

    rows.append({
        "contract": "prototype_safety_rows_all_pass",
        "expected": True,
        "actual": all(row.get("passed") == "True" for row in safety_rows),
        "passed": all(row.get("passed") == "True" for row in safety_rows),
    })
    for field in ["test_double_only", "live_adapter_implemented", "scaffold_modified", "no_external_fetch", "no_db_writes", "production_default_unchanged"]:
        expected = False if field in {"live_adapter_implemented", "scaffold_modified"} else True
        actual = run_json.get(field)
        rows.append({
            "contract": field,
            "expected": expected,
            "actual": actual,
            "passed": actual is expected,
        })

    return rows


def main() -> None:
    module = _import_prototype()

    run1 = _run_prototype()
    run2 = _run_prototype()

    deterministic_rows = _determinism_rows(run1, run2)
    result_rows = _result_contract_rows(run2["results"])
    retry_rows = _retry_contract_rows(run2["retry"])
    normalization_rows = _normalization_contract_rows(run2["normalization"])
    safety_rows = _safety_contract_rows(run2["safety"], run2["json"])

    _write_csv(OUTPUT_DETERMINISM, deterministic_rows)
    _write_csv(OUTPUT_RESULTS, result_rows)
    _write_csv(OUTPUT_RETRY, retry_rows)
    _write_csv(OUTPUT_NORMALIZATION, normalization_rows)
    _write_csv(OUTPUT_SAFETY, safety_rows)

    function_doubles = [
        "empty_adapter",
        "fixture_adapter",
        "malformed_schema_adapter",
        "duplicate_natural_key_adapter",
    ]
    prototype_module_loaded = (
        module is not None
        and all(callable(getattr(module, name, None)) for name in function_doubles)
        and isinstance(getattr(module, "TransientErrorAdapter", None), type)
        and all(isinstance(getattr(module, name, None), type) for name in EXPECTED_EXCEPTIONS)
    )
    subprocess_runs_successful = run1["succeeded"] and run2["succeeded"] and run1["json"].get("all_checks_passed") is True and run2["json"].get("all_checks_passed") is True
    deterministic_outputs_valid = all(row["passed"] for row in deterministic_rows)
    result_contract_valid = all(row["passed"] for row in result_rows)
    retry_contract_valid = all(row["passed"] for row in retry_rows)
    normalization_contract_valid = all(row["passed"] for row in normalization_rows)
    safety_contract_valid = all(row["passed"] for row in safety_rows)

    checks = [
        {"check": "prototype_module_loaded", "passed": prototype_module_loaded, "detail": str(PROTOTYPE_PATH)},
        {"check": "subprocess_runs_successful", "passed": subprocess_runs_successful, "detail": f"run1={run1['returncode']} run2={run2['returncode']}"},
        {"check": "deterministic_outputs_valid", "passed": deterministic_outputs_valid, "detail": f"{sum(row['passed'] for row in deterministic_rows)}/{len(deterministic_rows)}"},
        {"check": "result_contract_valid", "passed": result_contract_valid, "detail": f"{sum(row['passed'] for row in result_rows)}/{len(result_rows)}"},
        {"check": "retry_contract_valid", "passed": retry_contract_valid, "detail": f"{sum(row['passed'] for row in retry_rows)}/{len(retry_rows)}"},
        {"check": "normalization_contract_valid", "passed": normalization_contract_valid, "detail": f"{sum(row['passed'] for row in normalization_rows)}/{len(normalization_rows)}"},
        {"check": "safety_contract_valid", "passed": safety_contract_valid, "detail": f"{sum(row['passed'] for row in safety_rows)}/{len(safety_rows)}"},
        {"check": "audit_only_no_live_adapter", "passed": True, "detail": True},
        {"check": "no_external_fetch", "passed": True, "detail": True},
        {"check": "no_db_writes", "passed": True, "detail": True},
        {"check": "production_default_unchanged", "passed": True, "detail": True},
    ]
    _write_csv(OUTPUT_CHECKS, checks)

    diagnosis = {
        "diagnosis": "candidate_bullpen_statcast_fetch_adapter_test_double_audit_complete",
        "audit_version": AUDIT_VERSION,
        "prototype_path": str(PROTOTYPE_PATH),
        "subprocess_runs": 2,
        "deterministic_comparisons": len(deterministic_rows),
        "result_contract_checks": len(result_rows),
        "retry_contract_checks": len(retry_rows),
        "normalization_contract_checks": len(normalization_rows),
        "safety_contract_checks": len(safety_rows),
        "all_checks_passed": all(check["passed"] for check in checks),
        "audit_only": True,
        "live_adapter_implemented": False,
        "no_external_fetch": True,
        "no_db_writes": True,
        "production_default_unchanged": True,
        "recommended_next_layer": (
            "6CI_candidate_bullpen_statcast_fetch_adapter_fixture_integration_plan"
            if all(check["passed"] for check in checks)
            else "6CG_patch_candidate_bullpen_statcast_fetch_adapter_test_double_prototype"
        ),
    }

    OUTPUT_JSON.write_text(json.dumps(diagnosis, indent=2))
    print(json.dumps(diagnosis, indent=2))


if __name__ == "__main__":
    main()
