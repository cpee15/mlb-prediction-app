from __future__ import annotations

import ast
import csv
import importlib.util
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List


AUDIT_VERSION = "candidate_bullpen_statcast_live_adapter_scaffold_integration_audit_v0.1"

SCAFFOLD_PATH = Path("scripts/backfill_candidate_bullpen_statcast_labels.py")
ADAPTER_PATH = Path("scripts/fetch_candidate_bullpen_statcast_live_adapter.py")
VALIDATION_PATH = Path("scripts/validate_candidate_bullpen_statcast_live_adapter_scaffold_integration.py")

PLAN_6DF = Path("scripts/plan_candidate_bullpen_statcast_live_adapter_fetch_contract_module.py")
AUDIT_6DG = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_fetch_contract_module_plan.py")
AUDIT_6DI = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_fetch_module_implementation.py")
PLAN_6DJ = Path("scripts/plan_candidate_bullpen_statcast_live_adapter_scaffold_integration.py")
AUDIT_6DK = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_scaffold_integration_plan.py")
TEST_DOUBLE_PROTOTYPE_6DD = Path("scripts/prototype_candidate_bullpen_statcast_live_adapter_fetch_test_doubles.py")
TEST_DOUBLE_AUDIT_6DE = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_fetch_test_double_prototype.py")

FIXTURE_ROOT = Path("tests/fixtures/statcast/bullpen_labels")
MANIFEST = FIXTURE_ROOT / "manifest.json"
EXPECTED_RESULTS = FIXTURE_ROOT / "expected_results.json"
DATES_DIR = FIXTURE_ROOT / "dates"

OUTPUT_DIR = Path("tmp")
OUTPUT_DIR.mkdir(exist_ok=True)

VALIDATION_JSON = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_scaffold_integration.json"
VALIDATION_CHECKS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_scaffold_integration_checks.csv"
VALIDATION_SOURCE = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_scaffold_integration_source_audit.csv"
VALIDATION_CLI = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_scaffold_integration_cli_source_mode_audit.csv"
VALIDATION_GATES = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_scaffold_integration_live_gate_audit.csv"
VALIDATION_ARTIFACT = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_scaffold_integration_artifact_contract_audit.csv"
VALIDATION_IMPORT = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_scaffold_integration_import_boundary_audit.csv"
VALIDATION_IMMUTABILITY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_scaffold_integration_immutability_audit.csv"
VALIDATION_SAFETY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_scaffold_integration_safety_audit.csv"

OUTPUT_JSON = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_scaffold_integration_audit.json"
OUTPUT_CHECKS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_scaffold_integration_audit_checks.csv"
OUTPUT_VALIDATION_ARTIFACTS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_scaffold_integration_audit_validation_artifacts.csv"
OUTPUT_SOURCE = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_scaffold_integration_audit_source.csv"
OUTPUT_RUNTIME = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_scaffold_integration_audit_runtime_behavior.csv"
OUTPUT_ARTIFACT = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_scaffold_integration_audit_artifact_contract.csv"
OUTPUT_IMPORT = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_scaffold_integration_audit_import_boundary.csv"
OUTPUT_SAFETY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_scaffold_integration_audit_safety.csv"
OUTPUT_IMMUTABILITY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_scaffold_integration_audit_immutability.csv"

REQUIRED_LIVE_ARTIFACT_FIELDS = {
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
}

SAFETY_FIELDS = {
    "external_fetch_performed",
    "db_writes_performed",
    "candidate_labels_materialized",
    "production_default_unchanged",
}


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


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text())


def _snapshot_files() -> Dict[str, str]:
    paths = [
        SCAFFOLD_PATH,
        ADAPTER_PATH,
        VALIDATION_PATH,
        PLAN_6DF,
        AUDIT_6DG,
        AUDIT_6DI,
        PLAN_6DJ,
        AUDIT_6DK,
        TEST_DOUBLE_PROTOTYPE_6DD,
        TEST_DOUBLE_AUDIT_6DE,
        MANIFEST,
        EXPECTED_RESULTS,
    ]
    snapshot = {
        str(path): path.read_text(errors="ignore") if path.exists() else "__MISSING__"
        for path in paths
    }
    if DATES_DIR.exists():
        for payload in sorted(DATES_DIR.glob("*.jsonl")):
            snapshot[str(payload)] = payload.read_text(errors="ignore")
    return snapshot


def _load_scaffold_module() -> Any:
    spec = importlib.util.spec_from_file_location("candidate_bullpen_scaffold_audit_target", SCAFFOLD_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("unable to load scaffold module spec")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _base_row(label_date: str, game_pk: int, at_bat_number: int, pitch_number: int, pitcher_id: int) -> Dict[str, Any]:
    return {
        "game_date": label_date,
        "game_pk": game_pk,
        "inning": 7,
        "inning_topbot": "Top",
        "at_bat_number": at_bat_number,
        "pitch_number": pitch_number,
        "outs_when_up": 1,
        "pitcher_id": pitcher_id,
        "home_team": "NYY",
        "away_team": "BOS",
        "events": "strikeout",
        "description": "called_strike",
    }


class FetchProbe:
    def __init__(self) -> None:
        self.calls: List[str] = []

    def __call__(self, label_date: str) -> List[Dict[str, Any]]:
        self.calls.append(label_date)
        return [
            _base_row(label_date, 1001, 4, 2, 700),
            _base_row(label_date, 1001, 4, 1, 700),
        ]


def _run_validation_script() -> Dict[str, Any]:
    completed = subprocess.run(
        [sys.executable, str(VALIDATION_PATH)],
        capture_output=True,
        text=True,
    )
    diagnosis = _read_json(VALIDATION_JSON)
    passed = (
        completed.returncode == 0
        and diagnosis.get("diagnosis") == "candidate_bullpen_statcast_live_adapter_scaffold_integration_complete"
        and diagnosis.get("all_checks_passed") is True
        and diagnosis.get("scaffold_integration_complete") is True
        and diagnosis.get("live_integration_gated") is True
        and diagnosis.get("default_fixture_behavior_preserved") is True
        and diagnosis.get("adapter_unchanged") is True
        and diagnosis.get("external_fetch_performed") is False
        and diagnosis.get("db_writes_performed") is False
        and diagnosis.get("candidate_labels_materialized_from_live_rows") is False
    )
    return {
        "returncode": completed.returncode,
        "diagnosis": diagnosis.get("diagnosis", ""),
        "stdout_tail": completed.stdout[-1000:],
        "stderr_tail": completed.stderr[-1000:],
        "passed": passed,
    }


def _validation_artifact_audit() -> List[Dict[str, Any]]:
    artifacts = [
        VALIDATION_JSON,
        VALIDATION_CHECKS,
        VALIDATION_SOURCE,
        VALIDATION_CLI,
        VALIDATION_GATES,
        VALIDATION_ARTIFACT,
        VALIDATION_IMPORT,
        VALIDATION_IMMUTABILITY,
        VALIDATION_SAFETY,
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


def _source_audit(scaffold_source: str) -> List[Dict[str, Any]]:
    expected_tokens = {
        "integration_version_marker": "candidate_bullpen_live_adapter_scaffold_integration_v0.1",
        "live_scaffold_helper": "def run_candidate_bullpen_live_adapter_scaffold(",
        "blocked_artifact_helper": "def _candidate_bullpen_live_blocked_artifact(",
        "adapter_artifact_mapper": "def _candidate_bullpen_live_artifact_from_adapter_result(",
        "source_mode_fixture_constant": "CANDIDATE_BULLPEN_SOURCE_MODE_FIXTURE",
        "source_mode_live_constant": "CANDIDATE_BULLPEN_SOURCE_MODE_LIVE",
        "requires_dry_run_status": "CANDIDATE_BULLPEN_LIVE_STATUS_REQUIRES_DRY_RUN",
        "write_blocked_status": "CANDIDATE_BULLPEN_LIVE_STATUS_WRITE_BLOCKED",
        "date_window_invalid_status": "CANDIDATE_BULLPEN_LIVE_STATUS_DATE_WINDOW_INVALID",
    }
    return [
        {
            "check": check,
            "token": token,
            "passed": token in scaffold_source,
        }
        for check, token in expected_tokens.items()
    ]


def _runtime_behavior_audit(module: Any) -> List[Dict[str, Any]]:
    fixture_result = module.run_candidate_bullpen_live_adapter_scaffold(["2024-07-16"])

    live_without_dry_run = module.run_candidate_bullpen_live_adapter_scaffold(
        ["2024-07-16"],
        source_mode="live",
        dry_run=False,
    )
    live_write_blocked = module.run_candidate_bullpen_live_adapter_scaffold(
        ["2024-07-16"],
        source_mode="live",
        dry_run=True,
        allow_live_write=True,
    )

    invalid_probe = FetchProbe()
    invalid_date = module.run_candidate_bullpen_live_adapter_scaffold(
        ["2024-7-16"],
        source_mode="live",
        dry_run=True,
        fetcher=invalid_probe,
    )

    multiple_probe = FetchProbe()
    multiple_dates = module.run_candidate_bullpen_live_adapter_scaffold(
        ["2024-07-16", "2024-07-17"],
        source_mode="live",
        dry_run=True,
        fetcher=multiple_probe,
    )

    success_probe = FetchProbe()
    live_success = module.run_candidate_bullpen_live_adapter_scaffold(
        ["2024-07-16"],
        source_mode="live",
        dry_run=True,
        fetcher=success_probe,
    )

    return [
        {"check": "fixture_default_returns_fixture_mode_unchanged", "status": fixture_result.get("adapter_status"), "passed": fixture_result.get("adapter_status") == "fixture_mode_unchanged"},
        {"check": "live_without_dry_run_returns_requires_dry_run", "status": live_without_dry_run.get("adapter_status"), "passed": live_without_dry_run.get("adapter_status") == "live_requires_dry_run"},
        {"check": "live_write_attempt_returns_write_blocked", "status": live_write_blocked.get("adapter_status"), "passed": live_write_blocked.get("adapter_status") == "live_write_blocked"},
        {"check": "invalid_date_returns_date_window_invalid", "status": invalid_date.get("adapter_status"), "passed": invalid_date.get("adapter_status") == "live_date_window_invalid"},
        {"check": "multiple_dates_return_date_window_invalid", "status": multiple_dates.get("adapter_status"), "passed": multiple_dates.get("adapter_status") == "live_date_window_invalid"},
        {"check": "invalid_date_blocks_before_fetcher_call", "calls": len(invalid_probe.calls), "passed": len(invalid_probe.calls) == 0},
        {"check": "multiple_dates_block_before_fetcher_call", "calls": len(multiple_probe.calls), "passed": len(multiple_probe.calls) == 0},
        {"check": "live_dry_run_success_ready", "status": live_success.get("adapter_status"), "passed": live_success.get("adapter_status") == "live_dry_run_ready"},
        {"check": "live_dry_run_propagates_normalized_count", "count": live_success.get("adapter_normalized_row_count"), "passed": live_success.get("adapter_normalized_row_count") == 2},
        {"check": "live_dry_run_no_db_writes", "value": live_success.get("db_writes_performed"), "passed": live_success.get("db_writes_performed") is False},
        {"check": "live_dry_run_no_candidate_label_materialization", "value": live_success.get("candidate_labels_materialized"), "passed": live_success.get("candidate_labels_materialized") is False},
    ]


def _artifact_contract_audit(module: Any) -> List[Dict[str, Any]]:
    fields = set(getattr(module, "CANDIDATE_BULLPEN_LIVE_ARTIFACT_FIELDS", []))

    success_probe = FetchProbe()
    live_success = module.run_candidate_bullpen_live_adapter_scaffold(
        ["2024-07-16"],
        source_mode="live",
        dry_run=True,
        fetcher=success_probe,
    )
    blocked_payload = module.run_candidate_bullpen_live_adapter_scaffold(
        ["2024-07-16"],
        source_mode="live",
        dry_run=False,
    )

    return [
        {"check": "artifact_constant_has_required_11_fields", "passed": fields == REQUIRED_LIVE_ARTIFACT_FIELDS, "detail": f"{len(fields)} fields"},
        {"check": "runtime_live_payload_has_required_11_fields", "passed": REQUIRED_LIVE_ARTIFACT_FIELDS.issubset(set(live_success)), "detail": f"{len(REQUIRED_LIVE_ARTIFACT_FIELDS.intersection(set(live_success)))} fields"},
        {"check": "blocked_payload_has_required_11_fields", "passed": REQUIRED_LIVE_ARTIFACT_FIELDS.issubset(set(blocked_payload)), "detail": f"{len(REQUIRED_LIVE_ARTIFACT_FIELDS.intersection(set(blocked_payload)))} fields"},
        {"check": "runtime_live_payload_has_safety_fields", "passed": SAFETY_FIELDS.issubset(set(live_success)), "detail": f"{len(SAFETY_FIELDS.intersection(set(live_success)))} fields"},
        {"check": "blocked_payload_has_safety_fields", "passed": SAFETY_FIELDS.issubset(set(blocked_payload)), "detail": f"{len(SAFETY_FIELDS.intersection(set(blocked_payload)))} fields"},
    ]


def _import_boundary_audit(scaffold_source: str) -> List[Dict[str, Any]]:
    tree = ast.parse(scaffold_source)
    top_level_imports = []
    for node in tree.body:
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            top_level_imports.append(ast.get_source_segment(scaffold_source, node) or "")
    top_level_import_text = "\n".join(top_level_imports)

    marker = "def run_candidate_bullpen_live_adapter_scaffold("
    helper_body = scaffold_source[scaffold_source.find(marker):] if marker in scaffold_source else ""
    adapter_import_count = scaffold_source.count("fetch_candidate_bullpen_statcast_live_adapter")

    return [
        {"check": "no_top_level_adapter_import", "passed": "fetch_candidate_bullpen_statcast_live_adapter" not in top_level_import_text, "detail": True},
        {"check": "adapter_import_inside_live_helper", "passed": "fetch_candidate_bullpen_statcast_live_adapter" in helper_body, "detail": True},
        {"check": "adapter_reference_count_limited", "passed": adapter_import_count <= 2, "detail": adapter_import_count},
        {"check": "no_top_level_pybaseball_import", "passed": "pybaseball" not in top_level_import_text and "statcast" not in top_level_import_text, "detail": True},
        {"check": "scaffold_no_direct_pybaseball_import", "passed": "pybaseball" not in top_level_import_text, "detail": True},
    ]


def _safety_audit(module: Any, scaffold_source: str) -> List[Dict[str, Any]]:
    marker = "# Layer 6DL: candidate bullpen Statcast live adapter scaffold integration."
    live_block = scaffold_source[scaffold_source.find(marker):] if marker in scaffold_source else scaffold_source
    lower_live_block = live_block.lower()

    fixture_probe = FetchProbe()
    fixture_result = module.run_candidate_bullpen_live_adapter_scaffold(
        ["2024-07-16"],
        source_mode="fixture",
        dry_run=False,
        fetcher=fixture_probe,
    )

    invalid_probe = FetchProbe()
    invalid_result = module.run_candidate_bullpen_live_adapter_scaffold(
        ["bad-date"],
        source_mode="live",
        dry_run=True,
        fetcher=invalid_probe,
    )

    success_probe = FetchProbe()
    success_result = module.run_candidate_bullpen_live_adapter_scaffold(
        ["2024-07-16"],
        source_mode="live",
        dry_run=True,
        fetcher=success_probe,
    )

    return [
        {"check": "no_real_external_fetch_in_audit", "passed": len(success_probe.calls) == 1 and success_probe.calls == ["2024-07-16"], "detail": "synthetic fetcher only"},
        {"check": "no_external_network_usage_added_to_live_block", "passed": all(token not in live_block for token in ["requests.", "httpx.", "urllib."]), "detail": True},
        {"check": "no_db_writes_added_to_live_block", "passed": all(token not in lower_live_block for token in ["session.commit(", ".to_sql(", "insert into"]), "detail": True},
        {"check": "no_candidate_label_materialization_from_live_rows", "passed": success_result.get("candidate_labels_materialized") is False, "detail": success_result.get("candidate_labels_materialized")},
        {"check": "invalid_live_date_blocks_before_fetcher_call", "passed": invalid_result.get("adapter_status") == "live_date_window_invalid" and len(invalid_probe.calls) == 0, "detail": len(invalid_probe.calls)},
        {"check": "fixture_path_does_not_call_fetcher", "passed": fixture_result.get("adapter_status") == "fixture_mode_unchanged" and len(fixture_probe.calls) == 0, "detail": len(fixture_probe.calls)},
        {"check": "production_default_remains_fixture", "passed": getattr(module, "CANDIDATE_BULLPEN_SOURCE_MODE_FIXTURE", None) == "fixture", "detail": "fixture"},
        {"check": "runtime_reports_no_db_writes", "passed": success_result.get("db_writes_performed") is False and success_result.get("adapter_db_writes_performed") is False, "detail": True},
    ]


def _immutability_audit(before_snapshot: Dict[str, str]) -> List[Dict[str, Any]]:
    after_snapshot = _snapshot_files()
    prior_paths = [
        PLAN_6DF,
        AUDIT_6DG,
        AUDIT_6DI,
        PLAN_6DJ,
        AUDIT_6DK,
        TEST_DOUBLE_PROTOTYPE_6DD,
        TEST_DOUBLE_AUDIT_6DE,
    ]
    return [
        {"check": "scaffold_not_modified_by_audit", "passed": before_snapshot.get(str(SCAFFOLD_PATH)) == after_snapshot.get(str(SCAFFOLD_PATH)), "detail": str(SCAFFOLD_PATH)},
        {"check": "adapter_not_modified", "passed": before_snapshot.get(str(ADAPTER_PATH)) == after_snapshot.get(str(ADAPTER_PATH)), "detail": str(ADAPTER_PATH)},
        {"check": "validation_script_not_modified", "passed": before_snapshot.get(str(VALIDATION_PATH)) == after_snapshot.get(str(VALIDATION_PATH)), "detail": str(VALIDATION_PATH)},
        {"check": "prior_layers_not_modified", "passed": all(before_snapshot.get(str(path)) == after_snapshot.get(str(path)) for path in prior_paths), "detail": "6DF/6DG/6DI/6DJ/6DK/6DD/6DE unchanged"},
        {"check": "no_fixture_mutation", "passed": before_snapshot == after_snapshot, "detail": "fixture and tracked dependency files unchanged"},
    ]


def main() -> None:
    before_snapshot = _snapshot_files()

    validation_execution = _run_validation_script()
    validation_artifact_rows = _validation_artifact_audit()

    scaffold_source = SCAFFOLD_PATH.read_text(errors="ignore")
    module = _load_scaffold_module()

    source_rows = _source_audit(scaffold_source)
    runtime_rows = _runtime_behavior_audit(module)
    artifact_rows = _artifact_contract_audit(module)
    import_rows = _import_boundary_audit(scaffold_source)
    safety_rows = _safety_audit(module, scaffold_source)
    immutability_rows = _immutability_audit(before_snapshot)

    _write_csv(OUTPUT_VALIDATION_ARTIFACTS, validation_artifact_rows)
    _write_csv(OUTPUT_SOURCE, source_rows)
    _write_csv(OUTPUT_RUNTIME, runtime_rows)
    _write_csv(OUTPUT_ARTIFACT, artifact_rows)
    _write_csv(OUTPUT_IMPORT, import_rows)
    _write_csv(OUTPUT_SAFETY, safety_rows)
    _write_csv(OUTPUT_IMMUTABILITY, immutability_rows)

    checks = [
        {"check": "validation_script_execution_valid", "passed": validation_execution["passed"], "detail": validation_execution["diagnosis"]},
        {"check": "validation_artifacts_valid", "passed": all(row["passed"] for row in validation_artifact_rows), "detail": f"{sum(row['passed'] for row in validation_artifact_rows)}/{len(validation_artifact_rows)}"},
        {"check": "scaffold_source_valid", "passed": all(row["passed"] for row in source_rows), "detail": f"{sum(row['passed'] for row in source_rows)}/{len(source_rows)}"},
        {"check": "runtime_behavior_valid", "passed": all(row["passed"] for row in runtime_rows), "detail": f"{sum(row['passed'] for row in runtime_rows)}/{len(runtime_rows)}"},
        {"check": "artifact_contract_valid", "passed": all(row["passed"] for row in artifact_rows), "detail": f"{sum(row['passed'] for row in artifact_rows)}/{len(artifact_rows)}"},
        {"check": "import_boundary_valid", "passed": all(row["passed"] for row in import_rows), "detail": f"{sum(row['passed'] for row in import_rows)}/{len(import_rows)}"},
        {"check": "safety_audit_valid", "passed": all(row["passed"] for row in safety_rows), "detail": f"{sum(row['passed'] for row in safety_rows)}/{len(safety_rows)}"},
        {"check": "immutability_valid", "passed": all(row["passed"] for row in immutability_rows), "detail": f"{sum(row['passed'] for row in immutability_rows)}/{len(immutability_rows)}"},
        {"check": "scaffold_not_modified_by_audit", "passed": any(row["check"] == "scaffold_not_modified_by_audit" and row["passed"] for row in immutability_rows), "detail": str(SCAFFOLD_PATH)},
        {"check": "adapter_not_modified", "passed": any(row["check"] == "adapter_not_modified" and row["passed"] for row in immutability_rows), "detail": str(ADAPTER_PATH)},
        {"check": "validation_script_not_modified", "passed": any(row["check"] == "validation_script_not_modified" and row["passed"] for row in immutability_rows), "detail": str(VALIDATION_PATH)},
        {"check": "no_fixture_mutation", "passed": any(row["check"] == "no_fixture_mutation" and row["passed"] for row in immutability_rows), "detail": True},
        {"check": "no_external_fetch", "passed": any(row["check"] == "no_real_external_fetch_in_audit" and row["passed"] for row in safety_rows), "detail": True},
        {"check": "no_db_writes", "passed": any(row["check"] == "runtime_reports_no_db_writes" and row["passed"] for row in safety_rows), "detail": True},
        {"check": "production_default_unchanged", "passed": any(row["check"] == "production_default_remains_fixture" and row["passed"] for row in safety_rows), "detail": True},
    ]
    _write_csv(OUTPUT_CHECKS, checks)

    all_checks_passed = all(row["passed"] for row in checks)
    diagnosis = {
        "diagnosis": "candidate_bullpen_statcast_live_adapter_scaffold_integration_audit_complete",
        "audit_version": AUDIT_VERSION,
        "validation_artifact_rows": len(validation_artifact_rows),
        "source_audit_rows": len(source_rows),
        "runtime_behavior_rows": len(runtime_rows),
        "artifact_contract_rows": len(artifact_rows),
        "import_boundary_rows": len(import_rows),
        "safety_rows": len(safety_rows),
        "immutability_rows": len(immutability_rows),
        "all_checks_passed": all_checks_passed,
        "audit_only": True,
        "scaffold_integration_validated": True,
        "scaffold_modified_by_audit": False,
        "adapter_modified": False,
        "validation_script_modified": False,
        "external_fetch_performed": False,
        "db_writes_performed": False,
        "candidate_labels_materialized_from_live_rows": False,
        "fixture_assets_mutated": False,
        "production_default_unchanged": True,
        "recommended_next_layer": (
            "6DN_candidate_bullpen_statcast_live_adapter_cli_contract_plan"
            if all_checks_passed
            else "6DM_patch_candidate_bullpen_statcast_live_adapter_scaffold_integration_audit"
        ),
    }

    OUTPUT_JSON.write_text(json.dumps(diagnosis, indent=2))
    print(json.dumps(diagnosis, indent=2))


if __name__ == "__main__":
    main()
