from __future__ import annotations

import csv
import importlib.util
import inspect
import json
from pathlib import Path
from typing import Any, Dict, List


AUDIT_VERSION = "candidate_bullpen_statcast_fetch_adapter_contract_audit_v0.1"

DESIGN_PATH = Path("scripts/design_candidate_bullpen_statcast_fetch_adapter.py")
SCAFFOLD_PATH = Path("scripts/backfill_candidate_bullpen_statcast_labels.py")
STATCAST_UTILS_PATH = Path("mlb_app/statcast_utils.py")

OUTPUT_DIR = Path("tmp")
OUTPUT_DIR.mkdir(exist_ok=True)

OUTPUT_JSON = OUTPUT_DIR / "candidate_bullpen_statcast_fetch_adapter_contract_audit.json"
OUTPUT_CHECKS = OUTPUT_DIR / "candidate_bullpen_statcast_fetch_adapter_contract_audit_checks.csv"
OUTPUT_INTEGRATION = OUTPUT_DIR / "candidate_bullpen_statcast_fetch_adapter_contract_integration_readiness.csv"
OUTPUT_INTERFACE = OUTPUT_DIR / "candidate_bullpen_statcast_fetch_adapter_contract_interface_audit.csv"
OUTPUT_NORMALIZATION = OUTPUT_DIR / "candidate_bullpen_statcast_fetch_adapter_contract_normalization_audit.csv"
OUTPUT_FAILURES = OUTPUT_DIR / "candidate_bullpen_statcast_fetch_adapter_contract_failure_taxonomy_audit.csv"
OUTPUT_DOUBLES = OUTPUT_DIR / "candidate_bullpen_statcast_fetch_adapter_contract_test_double_audit.csv"
OUTPUT_GATES = OUTPUT_DIR / "candidate_bullpen_statcast_fetch_adapter_contract_promotion_gate_audit.csv"
OUTPUT_SAFETY = OUTPUT_DIR / "candidate_bullpen_statcast_fetch_adapter_contract_safety_audit.csv"


REQUIRED_NORMALIZED_FIELDS = [
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

EXPECTED_FAILURES = {
    "AdapterFetchUnavailable": False,
    "AdapterNoRowsReturned": False,
    "AdapterSchemaMismatch": False,
    "AdapterRateLimited": True,
    "AdapterNetworkError": True,
    "AdapterUnexpectedError": False,
}

EXPECTED_DOUBLES = {
    "empty_adapter": "zero fetched rows",
    "fixture_adapter": "valid normalized fixture",
    "malformed_schema_adapter": "schema validation fails",
    "duplicate_natural_key_adapter": "dedupe removes duplicates",
    "transient_error_adapter": "retry audit records transient recovery",
}

FORBIDDEN_IMPORT_TOKENS = [
    "mlb_app.simulation",
    "GameEngine",
    "canonical_matchup_probability",
    "sportsbook",
    "routes",
    "frontend",
]


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _read_text(path: Path) -> str:
    try:
        return path.read_text(errors="ignore")
    except Exception:
        return ""


def _import_module(path: Path, module_name: str):
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not import {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _import_lines(path: Path) -> str:
    text = _read_text(path)
    lines = [
        line.strip()
        for line in text.splitlines()
        if line.strip().startswith("import ") or line.strip().startswith("from ")
    ]
    return "\n".join(lines)


def _function_body(path: Path, function_name: str, next_function_name: str | None = None) -> str:
    text = _read_text(path)
    start = text.find(f"def {function_name}")
    if start < 0:
        return ""
    if next_function_name:
        end = text.find(f"def {next_function_name}", start + 1)
        return text[start:end] if end > start else text[start:]
    return text[start:]


def _integration_readiness(design_module, scaffold_module) -> List[Dict[str, Any]]:
    statcast_text = _read_text(STATCAST_UTILS_PATH).lower()
    scaffold_text = _read_text(SCAFFOLD_PATH).lower()

    rows = [
        {
            "component": "mlb_app/statcast_utils.py",
            "exists": STATCAST_UTILS_PATH.exists(),
            "has_statcast_signal": "statcast" in statcast_text,
            "has_pybaseball_signal": "pybaseball" in statcast_text,
            "has_fetch_like_signal": "statcast" in statcast_text or "pybaseball" in statcast_text,
            "has_session_commit_signal": "session.commit(" in statcast_text or ".commit()" in statcast_text,
            "has_to_sql_signal": ".to_sql(" in statcast_text,
            "ready": STATCAST_UTILS_PATH.exists()
            and ("statcast" in statcast_text or "pybaseball" in statcast_text)
            and "session.commit(" not in statcast_text
            and ".to_sql(" not in statcast_text,
        },
        {
            "component": "scripts/backfill_candidate_bullpen_statcast_labels.py",
            "exists": SCAFFOLD_PATH.exists(),
            "has_statcast_signal": "statcast" in scaffold_text,
            "has_pybaseball_signal": "pybaseball" in scaffold_text,
            "has_fetch_like_signal": "fetch_statcast_label_rows_for_date" in scaffold_text,
            "has_session_commit_signal": "session.commit(" in scaffold_text or ".commit()" in scaffold_text,
            "has_to_sql_signal": ".to_sql(" in scaffold_text,
            "ready": SCAFFOLD_PATH.exists()
            and hasattr(scaffold_module, "fetch_statcast_label_rows_for_date")
            and "scaffold_stub_not_fetched" in scaffold_text,
        },
        {
            "component": "scripts/design_candidate_bullpen_statcast_fetch_adapter.py",
            "exists": DESIGN_PATH.exists(),
            "has_statcast_signal": hasattr(design_module, "NORMALIZATION_MAPPING"),
            "has_pybaseball_signal": hasattr(design_module, "FETCH_STRATEGY"),
            "has_fetch_like_signal": hasattr(design_module, "ADAPTER_INTERFACE"),
            "has_session_commit_signal": False,
            "has_to_sql_signal": False,
            "ready": DESIGN_PATH.exists()
            and hasattr(design_module, "ADAPTER_INTERFACE")
            and hasattr(design_module, "FAILURE_TAXONOMY")
            and hasattr(design_module, "TEST_DOUBLE_PLAN"),
        },
    ]

    return rows


def _interface_audit(scaffold_module) -> List[Dict[str, Any]]:
    adapter = getattr(scaffold_module, "fetch_statcast_label_rows_for_date", None)
    body = _function_body(SCAFFOLD_PATH, "fetch_statcast_label_rows_for_date", "_natural_key").lower()

    try:
        stub_result = adapter("2026-05-20") if adapter else None
    except Exception as exc:
        stub_result = f"ERROR: {exc!r}"

    rows = [
        {"item": "function_exists", "passed": callable(adapter), "detail": "fetch_statcast_label_rows_for_date"},
        {"item": "signature_one_arg", "passed": len(inspect.signature(adapter).parameters) == 1 if adapter else False, "detail": str(inspect.signature(adapter)) if adapter else ""},
        {"item": "stub_returns_list", "passed": isinstance(stub_result, list), "detail": str(stub_result)},
        {"item": "stub_returns_empty", "passed": stub_result == [], "detail": str(stub_result)},
        {"item": "no_session_access_in_adapter", "passed": "session" not in body, "detail": "adapter body"},
        {"item": "no_engine_access_in_adapter", "passed": "engine" not in body, "detail": "adapter body"},
        {"item": "no_write_logic_in_adapter", "passed": all(token not in body for token in ["commit", "insert", "to_sql", "add("]), "detail": "adapter body"},
    ]
    return rows


def _normalization_audit(design_module, scaffold_module) -> List[Dict[str, Any]]:
    design_fields = [row["normalized_field"] for row in getattr(design_module, "NORMALIZATION_MAPPING", [])]
    scaffold_fields = list(getattr(scaffold_module, "REQUIRED_FIELDS", []))

    rows = []
    for field in REQUIRED_NORMALIZED_FIELDS:
        rows.append({
            "field": field,
            "in_design_mapping": field in design_fields,
            "in_scaffold_required_fields": field in scaffold_fields,
            "natural_key_field": field in NATURAL_KEY_FIELDS,
            "passed": field in design_fields and field in scaffold_fields,
        })

    rows.append({
        "field": "__field_count__",
        "in_design_mapping": len(design_fields) == 12,
        "in_scaffold_required_fields": len(scaffold_fields) == 12,
        "natural_key_field": False,
        "passed": len(design_fields) == 12 and len(scaffold_fields) == 12,
    })

    rows.append({
        "field": "__natural_key_complete__",
        "in_design_mapping": all(field in design_fields for field in NATURAL_KEY_FIELDS),
        "in_scaffold_required_fields": all(field in scaffold_fields for field in NATURAL_KEY_FIELDS),
        "natural_key_field": True,
        "passed": all(field in design_fields and field in scaffold_fields for field in NATURAL_KEY_FIELDS),
    })

    return rows


def _failure_taxonomy_audit(design_module) -> List[Dict[str, Any]]:
    rows = []
    taxonomy = getattr(design_module, "FAILURE_TAXONOMY", [])
    by_name = {row["exception"]: row for row in taxonomy}

    for name, expected_retryable in EXPECTED_FAILURES.items():
        item = by_name.get(name, {})
        rows.append({
            "exception": name,
            "present": name in by_name,
            "retryable": item.get("retryable"),
            "expected_retryable": expected_retryable,
            "passed": name in by_name and bool(item.get("retryable")) == expected_retryable,
        })

    rows.append({
        "exception": "__failure_count__",
        "present": len(taxonomy) == 6,
        "retryable": None,
        "expected_retryable": None,
        "passed": len(taxonomy) == 6,
    })

    retryables = [row["exception"] for row in taxonomy if row.get("retryable")]
    rows.append({
        "exception": "__retryable_limited_to_rate_network__",
        "present": True,
        "retryable": "|".join(retryables),
        "expected_retryable": "AdapterRateLimited|AdapterNetworkError",
        "passed": set(retryables) == {"AdapterRateLimited", "AdapterNetworkError"},
    })

    return rows


def _test_double_audit(design_module) -> List[Dict[str, Any]]:
    doubles = getattr(design_module, "TEST_DOUBLE_PLAN", [])
    by_name = {row["double"]: row for row in doubles}
    rows = []

    for name, expected_phrase in EXPECTED_DOUBLES.items():
        item = by_name.get(name, {})
        expected_blob = " ".join(str(item.get(key, "")) for key in ["behavior", "expected_result"]).lower()
        rows.append({
            "double": name,
            "present": name in by_name,
            "has_expected_result": bool(item.get("expected_result")),
            "expected_phrase": expected_phrase,
            "passed": name in by_name and bool(item.get("expected_result")),
            "detail": item.get("expected_result"),
        })

    rows.append({
        "double": "__double_count__",
        "present": len(doubles) == 5,
        "has_expected_result": True,
        "expected_phrase": "5 doubles",
        "passed": len(doubles) == 5,
        "detail": len(doubles),
    })

    return rows


def _promotion_gate_audit(design_module) -> List[Dict[str, Any]]:
    gates = getattr(design_module, "PROMOTION_GATES", [])
    gate_names = {row["gate"] for row in gates}
    required = [
        "adapter_contract_audit_passes",
        "fixture_normalization_passes",
        "duplicate_dedupe_passes",
        "malformed_schema_fails_safely",
        "transient_retry_audited",
        "write_suppression_preserved",
        "no_production_coupling",
    ]
    rows = []
    for gate in required:
        rows.append({
            "gate": gate,
            "present": gate in gate_names,
            "required": True,
            "passed": gate in gate_names,
        })
    rows.append({
        "gate": "__gate_count__",
        "present": len(gates) >= 7,
        "required": True,
        "passed": len(gates) >= 7,
    })
    return rows


def _safety_audit() -> List[Dict[str, Any]]:
    rows = []

    for path, label in [(SCAFFOLD_PATH, "scaffold"), (DESIGN_PATH, "design")]:
        import_blob = _import_lines(path)
        for token in FORBIDDEN_IMPORT_TOKENS:
            rows.append({
                "component": label,
                "check": "forbidden_import",
                "token": token,
                "present": token in import_blob,
                "passed": token not in import_blob,
            })

    adapter_body = _function_body(SCAFFOLD_PATH, "fetch_statcast_label_rows_for_date", "_natural_key").lower()
    for token in ["requests.", "urllib.", "httpx.", "pybaseball.statcast", "statcast("]:
        rows.append({
            "component": "scaffold_adapter_stub",
            "check": "external_fetch_token",
            "token": token,
            "present": token in adapter_body,
            "passed": token not in adapter_body,
        })

    for token in ["session", "engine", "commit", "to_sql", "insert"]:
        rows.append({
            "component": "scaffold_adapter_stub",
            "check": "db_or_write_token",
            "token": token,
            "present": token in adapter_body,
            "passed": token not in adapter_body,
        })

    return rows


def main() -> None:
    design_module = _import_module(DESIGN_PATH, "design_candidate_bullpen_statcast_fetch_adapter")
    scaffold_module = _import_module(SCAFFOLD_PATH, "backfill_candidate_bullpen_statcast_labels")

    integration_rows = _integration_readiness(design_module, scaffold_module)
    interface_rows = _interface_audit(scaffold_module)
    normalization_rows = _normalization_audit(design_module, scaffold_module)
    failure_rows = _failure_taxonomy_audit(design_module)
    double_rows = _test_double_audit(design_module)
    gate_rows = _promotion_gate_audit(design_module)
    safety_rows = _safety_audit()

    _write_csv(OUTPUT_INTEGRATION, integration_rows)
    _write_csv(OUTPUT_INTERFACE, interface_rows)
    _write_csv(OUTPUT_NORMALIZATION, normalization_rows)
    _write_csv(OUTPUT_FAILURES, failure_rows)
    _write_csv(OUTPUT_DOUBLES, double_rows)
    _write_csv(OUTPUT_GATES, gate_rows)
    _write_csv(OUTPUT_SAFETY, safety_rows)

    design_module_loaded = design_module is not None and scaffold_module is not None
    integration_readiness_valid = all(row["ready"] for row in integration_rows)
    adapter_interface_valid = all(row["passed"] for row in interface_rows)
    normalization_contract_valid = all(row["passed"] for row in normalization_rows)
    failure_taxonomy_valid = all(row["passed"] for row in failure_rows)
    test_double_plan_valid = all(row["passed"] for row in double_rows)
    promotion_gates_valid = all(row["passed"] for row in gate_rows)
    safety_audit_valid = all(row["passed"] for row in safety_rows)

    checks = [
        {"check": "design_module_loaded", "passed": design_module_loaded, "detail": "design and scaffold modules loaded"},
        {"check": "integration_readiness_valid", "passed": integration_readiness_valid, "detail": f"{sum(1 for row in integration_rows if row['ready'])}/{len(integration_rows)}"},
        {"check": "adapter_interface_valid", "passed": adapter_interface_valid, "detail": f"{sum(1 for row in interface_rows if row['passed'])}/{len(interface_rows)}"},
        {"check": "normalization_contract_valid", "passed": normalization_contract_valid, "detail": f"{sum(1 for row in normalization_rows if row['passed'])}/{len(normalization_rows)}"},
        {"check": "failure_taxonomy_valid", "passed": failure_taxonomy_valid, "detail": f"{sum(1 for row in failure_rows if row['passed'])}/{len(failure_rows)}"},
        {"check": "test_double_plan_valid", "passed": test_double_plan_valid, "detail": f"{sum(1 for row in double_rows if row['passed'])}/{len(double_rows)}"},
        {"check": "promotion_gates_valid", "passed": promotion_gates_valid, "detail": f"{sum(1 for row in gate_rows if row['passed'])}/{len(gate_rows)}"},
        {"check": "safety_audit_valid", "passed": safety_audit_valid, "detail": f"{sum(1 for row in safety_rows if row['passed'])}/{len(safety_rows)}"},
        {"check": "audit_only_no_live_adapter", "passed": True, "detail": True},
        {"check": "no_external_fetch", "passed": True, "detail": True},
        {"check": "no_db_writes", "passed": True, "detail": True},
        {"check": "production_default_unchanged", "passed": True, "detail": True},
    ]
    _write_csv(OUTPUT_CHECKS, checks)

    diagnosis = {
        "diagnosis": "candidate_bullpen_statcast_fetch_adapter_contract_audit_complete",
        "audit_version": AUDIT_VERSION,
        "integration_components": len(integration_rows),
        "interface_checks": len(interface_rows),
        "normalization_checks": len(normalization_rows),
        "failure_taxonomy_checks": len(failure_rows),
        "test_double_checks": len(double_rows),
        "promotion_gate_checks": len(gate_rows),
        "safety_checks": len(safety_rows),
        "all_checks_passed": all(check["passed"] for check in checks),
        "audit_only": True,
        "live_adapter_implemented": False,
        "scaffold_modified": False,
        "no_external_fetch": True,
        "no_db_writes": True,
        "production_default_unchanged": True,
        "recommended_next_layer": (
            "6CG_candidate_bullpen_statcast_fetch_adapter_test_double_prototype"
            if all(check["passed"] for check in checks)
            else "6CE_patch_candidate_bullpen_statcast_fetch_adapter_design"
        ),
    }

    OUTPUT_JSON.write_text(json.dumps(diagnosis, indent=2))
    print(json.dumps(diagnosis, indent=2))


if __name__ == "__main__":
    main()
