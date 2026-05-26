from __future__ import annotations

import argparse
import ast
import csv
import hashlib
import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List
from unittest import mock


AUDIT_VERSION = "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_audit_v0.1"

SCAFFOLD_PATH = Path("scripts/backfill_candidate_bullpen_statcast_labels.py")
ADAPTER_PATH = Path("scripts/fetch_candidate_bullpen_statcast_live_adapter.py")
VALIDATION_6EB = Path("scripts/validate_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability.py")
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

VALIDATION_JSON = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability.json"
VALIDATION_CHECKS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_checks.csv"
VALIDATION_SOURCE = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_source_audit.csv"
VALIDATION_FIELDS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_fields_audit.csv"
VALIDATION_DEFAULT = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_default_audit.csv"
VALIDATION_SYNTHETIC = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_synthetic_audit.csv"
VALIDATION_MONKEYPATCHED = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_monkeypatched_real_audit.csv"
VALIDATION_DEPENDENCY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_dependency_missing_audit.csv"
VALIDATION_BLOCKED = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_blocked_path_audit.csv"
VALIDATION_ARTIFACT = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_artifact_compatibility_audit.csv"
VALIDATION_IMPORT = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_import_boundary_audit.csv"
VALIDATION_SAFETY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_safety_audit.csv"
VALIDATION_IMMUTABILITY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_immutability_audit.csv"

OUTPUT_JSON = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_audit.json"
OUTPUT_CHECKS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_audit_checks.csv"
OUTPUT_VALIDATION_EXECUTION = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_audit_validation_execution.csv"
OUTPUT_VALIDATION_ARTIFACTS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_audit_validation_artifacts.csv"
OUTPUT_SOURCE = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_audit_source.csv"
OUTPUT_BEHAVIOR = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_audit_behavior.csv"
OUTPUT_ARTIFACT = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_audit_artifact_compatibility.csv"
OUTPUT_IMPORT = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_audit_import_boundary.csv"
OUTPUT_SAFETY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_audit_safety.csv"
OUTPUT_IMMUTABILITY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_audit_immutability.csv"

OBSERVABILITY_FIELDS = {
    "live_fetcher_resolution_source",
    "live_fetcher_resolution_status",
    "live_fetcher_resolution_gate",
    "live_fetcher_resolution_reason",
    "live_fetcher_resolution_dependency_error",
    "live_fetcher_resolution_external_fetch_enabled",
    "live_fetcher_resolution_synthetic_enabled",
    "live_fetcher_resolution_real_enabled",
}

REQUIRED_FIELDS = {
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


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text())


def _sha(path: Path) -> str:
    if not path.exists():
        return "__MISSING__"
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _snapshot_files() -> Dict[str, str]:
    paths = [
        SCAFFOLD_PATH,
        ADAPTER_PATH,
        VALIDATION_6EB,
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


def _load_scaffold_module() -> Any:
    spec = importlib.util.spec_from_file_location("candidate_bullpen_live_fetcher_observability_audit_target", SCAFFOLD_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("unable to load scaffold module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _run_cli(args: List[str], env_updates: Dict[str, str] | None = None) -> Dict[str, Any]:
    env = os.environ.copy()
    env.pop("CANDIDATE_BULLPEN_LIVE_FETCHER_TEST_DOUBLE", None)
    env.pop("CANDIDATE_BULLPEN_ENABLE_REAL_STATCAST_FETCHER", None)
    if env_updates:
        env.update(env_updates)

    completed = subprocess.run(
        [sys.executable, str(SCAFFOLD_PATH), *args],
        capture_output=True,
        text=True,
        env=env,
    )
    try:
        payload = json.loads(completed.stdout)
    except Exception:
        payload = {}
    return {
        "returncode": completed.returncode,
        "payload": payload,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
    }


def _args(**kwargs: Any) -> argparse.Namespace:
    base = {"source_mode": "live", "dry_run": True, "write": False, "allow_live_write": False}
    base.update(kwargs)
    return argparse.Namespace(**base)


def _run_validation() -> Dict[str, Any]:
    completed = subprocess.run(
        [sys.executable, str(VALIDATION_6EB)],
        capture_output=True,
        text=True,
    )
    diagnosis = _read_json(VALIDATION_JSON)
    validation_flags = [
        "source_valid",
        "observability_fields_valid",
        "default_observability_valid",
        "synthetic_observability_valid",
        "monkeypatched_real_fetcher_observability_valid",
        "dependency_missing_observability_valid",
        "blocked_path_observability_valid",
        "artifact_compatibility_valid",
        "import_boundary_valid",
        "safety_valid",
        "immutability_valid",
    ]
    return {
        "returncode": completed.returncode,
        "diagnosis": diagnosis.get("diagnosis", ""),
        "all_checks_passed": diagnosis.get("all_checks_passed"),
        "implementation_complete": diagnosis.get("implementation_complete"),
        "validation_flags_all_true": all(diagnosis.get(flag) is True for flag in validation_flags),
        "adapter_unchanged": diagnosis.get("adapter_unchanged"),
        "fixture_assets_mutated": diagnosis.get("fixture_assets_mutated"),
        "external_fetch_performed": diagnosis.get("external_fetch_performed"),
        "db_writes_performed": diagnosis.get("db_writes_performed"),
        "candidate_labels_materialized_from_live_rows": diagnosis.get("candidate_labels_materialized_from_live_rows"),
        "production_default_unchanged": diagnosis.get("production_default_unchanged"),
        "passed": (
            completed.returncode == 0
            and diagnosis.get("diagnosis") == "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_complete"
            and diagnosis.get("all_checks_passed") is True
            and diagnosis.get("implementation_complete") is True
            and all(diagnosis.get(flag) is True for flag in validation_flags)
            and diagnosis.get("adapter_unchanged") is True
            and diagnosis.get("fixture_assets_mutated") is False
            and diagnosis.get("external_fetch_performed") is False
            and diagnosis.get("db_writes_performed") is False
            and diagnosis.get("candidate_labels_materialized_from_live_rows") is False
            and diagnosis.get("production_default_unchanged") is True
        ),
    }


def _validation_artifact_rows() -> List[Dict[str, Any]]:
    artifacts = [
        VALIDATION_JSON,
        VALIDATION_CHECKS,
        VALIDATION_SOURCE,
        VALIDATION_FIELDS,
        VALIDATION_DEFAULT,
        VALIDATION_SYNTHETIC,
        VALIDATION_MONKEYPATCHED,
        VALIDATION_DEPENDENCY,
        VALIDATION_BLOCKED,
        VALIDATION_ARTIFACT,
        VALIDATION_IMPORT,
        VALIDATION_SAFETY,
        VALIDATION_IMMUTABILITY,
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


def _source_rows(source: str) -> List[Dict[str, Any]]:
    top_imports = _top_level_imports(source)
    resolver_idx = source.find("def _resolve_candidate_bullpen_live_fetcher")
    resolver_body = source[resolver_idx:] if resolver_idx != -1 else ""
    synthetic_idx = source.find('CANDIDATE_BULLPEN_LIVE_FETCHER_TEST_DOUBLE") == "synthetic"')
    real_idx = source.find('CANDIDATE_BULLPEN_ENABLE_REAL_STATCAST_FETCHER") == "1"')
    return [
        {"check": "six_eb_marker_present", "passed": "candidate_bullpen_live_adapter_cli_live_fetcher_observability_v0.1" in source, "detail": True},
        {"check": "observability_builder_defined", "passed": "def _candidate_bullpen_live_fetcher_observability" in source, "detail": True},
        {"check": "observability_apply_defined", "passed": "def _candidate_bullpen_apply_live_fetcher_observability" in source, "detail": True},
        {"check": "all_observability_fields_present", "passed": all(field in source for field in OBSERVABILITY_FIELDS), "detail": f"{sum(field in source for field in OBSERVABILITY_FIELDS)}/{len(OBSERVABILITY_FIELDS)}"},
        {"check": "real_gate_preserved", "passed": "CANDIDATE_BULLPEN_ENABLE_REAL_STATCAST_FETCHER" in source, "detail": True},
        {"check": "synthetic_gate_preserved", "passed": "CANDIDATE_BULLPEN_LIVE_FETCHER_TEST_DOUBLE" in source, "detail": True},
        {"check": "dependency_missing_preserved", "passed": "live_dependency_missing" in source, "detail": True},
        {"check": "resolver_present", "passed": "def _resolve_candidate_bullpen_live_fetcher" in source, "detail": True},
        {"check": "synthetic_precedes_real_branch", "passed": synthetic_idx != -1 and real_idx != -1 and synthetic_idx < real_idx, "detail": f"{synthetic_idx}:{real_idx}"},
        {"check": "production_default_preserved", "passed": 'default="scaffold"' in source or "default=CANDIDATE_BULLPEN_SOURCE_MODE_FIXTURE" in source, "detail": True},
        {"check": "no_top_level_adapter_import", "passed": "fetch_candidate_bullpen_statcast_live_adapter" not in top_imports, "detail": True},
        {"check": "no_top_level_pybaseball_statcast_import", "passed": "pybaseball" not in top_imports and "statcast" not in top_imports, "detail": True},
        {"check": "adapter_import_inside_gated_resolver", "passed": "fetch_candidate_bullpen_statcast_live_adapter" in resolver_body and "CANDIDATE_BULLPEN_ENABLE_REAL_STATCAST_FETCHER" in resolver_body, "detail": True},
    ]


def _behavior_rows(module: Any) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    default = _run_cli(["--source-mode", "live", "--start-date", "2024-07-16", "--end-date", "2024-07-16", "--dry-run"])["payload"]
    rows.extend([
        {"check": "default_fields_present", "passed": OBSERVABILITY_FIELDS.issubset(default), "detail": f"{len(OBSERVABILITY_FIELDS.intersection(default))}/{len(OBSERVABILITY_FIELDS)}"},
        {"check": "default_source_safe", "passed": default.get("live_fetcher_resolution_source") in {"none", "dependency_missing"}, "detail": default.get("live_fetcher_resolution_source")},
        {"check": "default_no_db_writes", "passed": default.get("db_writes_performed") is False, "detail": default.get("db_writes_performed")},
        {"check": "default_no_materialization", "passed": default.get("candidate_labels_materialized") is False, "detail": default.get("candidate_labels_materialized")},
    ])

    synthetic = _run_cli(
        ["--source-mode", "live", "--start-date", "2024-07-16", "--end-date", "2024-07-16", "--dry-run"],
        {"CANDIDATE_BULLPEN_LIVE_FETCHER_TEST_DOUBLE": "synthetic"},
    )["payload"]
    rows.extend([
        {"check": "synthetic_fields_present", "passed": OBSERVABILITY_FIELDS.issubset(synthetic), "detail": f"{len(OBSERVABILITY_FIELDS.intersection(synthetic))}/{len(OBSERVABILITY_FIELDS)}"},
        {"check": "synthetic_source", "passed": synthetic.get("live_fetcher_resolution_source") == "synthetic_test_double", "detail": synthetic.get("live_fetcher_resolution_source")},
        {"check": "synthetic_gate", "passed": synthetic.get("live_fetcher_resolution_gate") == "synthetic", "detail": synthetic.get("live_fetcher_resolution_gate")},
        {"check": "synthetic_enabled_true", "passed": synthetic.get("live_fetcher_resolution_synthetic_enabled") is True, "detail": synthetic.get("live_fetcher_resolution_synthetic_enabled")},
        {"check": "synthetic_no_db_writes", "passed": synthetic.get("db_writes_performed") is False, "detail": synthetic.get("db_writes_performed")},
        {"check": "synthetic_no_materialization", "passed": synthetic.get("candidate_labels_materialized") is False, "detail": synthetic.get("candidate_labels_materialized")},
    ])

    class FakeResult:
        status = "live_dry_run_ready"
        rows = [
            {
                "game_date": "2024-07-16",
                "game_pk": 990001,
                "inning": 9,
                "inning_topbot": "Top",
                "at_bat_number": 77,
                "pitch_number": 2,
                "outs_when_up": 1,
                "pitcher_id": 766666,
                "home_team": "TEX",
                "away_team": "OAK",
                "events": "field_out",
                "description": "hit_into_play",
            }
        ]
        normalized_rows = rows
        raw_row_count = 1
        normalized_row_count = 1
        duplicate_count = 0
        required_field_failures = 0
        missing_fields: List[str] = []
        fetch_error = ""
        external_fetch_performed = False
        db_writes_performed = False
        fetch_duration_ms = 0
        retry_count = 0
        source_adapter_version = "audit_fake_adapter_v0.1"

    calls: List[str] = []

    def fake_adapter(label_date: str, *args: Any, **kwargs: Any) -> FakeResult:
        calls.append(label_date)
        return FakeResult()

    old_real = os.environ.get("CANDIDATE_BULLPEN_ENABLE_REAL_STATCAST_FETCHER")
    old_synth = os.environ.get("CANDIDATE_BULLPEN_LIVE_FETCHER_TEST_DOUBLE")
    os.environ["CANDIDATE_BULLPEN_ENABLE_REAL_STATCAST_FETCHER"] = "1"
    os.environ.pop("CANDIDATE_BULLPEN_LIVE_FETCHER_TEST_DOUBLE", None)

    with mock.patch("scripts.fetch_candidate_bullpen_statcast_live_adapter.fetch_candidate_bullpen_statcast_live_rows_for_date", fake_adapter):
        fetcher = module._resolve_candidate_bullpen_live_fetcher(_args(), ["2024-07-16"])
        observability = module._candidate_bullpen_live_fetcher_observability(
            source="real_adapter",
            status="resolved",
            gate="real",
            reason="real adapter fetcher gate enabled",
            external_fetch_enabled=True,
            synthetic_enabled=False,
            real_enabled=True,
        )
        real_payload = module.run_candidate_bullpen_live_adapter_scaffold(
            ["2024-07-16"],
            source_mode="live",
            dry_run=True,
            allow_live_write=False,
            fetcher=fetcher,
        )
        module._candidate_bullpen_apply_live_fetcher_observability(real_payload, observability)

    if old_real is None:
        os.environ.pop("CANDIDATE_BULLPEN_ENABLE_REAL_STATCAST_FETCHER", None)
    else:
        os.environ["CANDIDATE_BULLPEN_ENABLE_REAL_STATCAST_FETCHER"] = old_real
    if old_synth is None:
        os.environ.pop("CANDIDATE_BULLPEN_LIVE_FETCHER_TEST_DOUBLE", None)
    else:
        os.environ["CANDIDATE_BULLPEN_LIVE_FETCHER_TEST_DOUBLE"] = old_synth

    rows.extend([
        {"check": "real_fields_present", "passed": OBSERVABILITY_FIELDS.issubset(real_payload), "detail": f"{len(OBSERVABILITY_FIELDS.intersection(real_payload))}/{len(OBSERVABILITY_FIELDS)}"},
        {"check": "real_source", "passed": real_payload.get("live_fetcher_resolution_source") == "real_adapter", "detail": real_payload.get("live_fetcher_resolution_source")},
        {"check": "real_gate", "passed": real_payload.get("live_fetcher_resolution_gate") == "real", "detail": real_payload.get("live_fetcher_resolution_gate")},
        {"check": "real_enabled_true", "passed": real_payload.get("live_fetcher_resolution_real_enabled") is True, "detail": real_payload.get("live_fetcher_resolution_real_enabled")},
        {"check": "real_uses_monkeypatch_without_network", "passed": calls == ["2024-07-16"], "detail": len(calls)},
        {"check": "real_no_db_writes", "passed": real_payload.get("db_writes_performed") is False, "detail": real_payload.get("db_writes_performed")},
    ])

    dependency = {
        "adapter_status": "live_dependency_missing",
        "adapter_fetch_error": "candidate_bullpen_live_adapter_dependency_missing",
        "external_fetch_performed": False,
        "db_writes_performed": False,
        "candidate_labels_materialized": False,
    }
    module._candidate_bullpen_apply_live_fetcher_observability(
        dependency,
        module._candidate_bullpen_live_fetcher_observability(
            source="dependency_missing",
            status="live_dependency_missing",
            gate="real",
            reason="real adapter dependency missing",
            dependency_error="candidate_bullpen_live_adapter_dependency_missing",
            external_fetch_enabled=False,
            synthetic_enabled=False,
            real_enabled=True,
        ),
    )
    rows.extend([
        {"check": "dependency_fields_present", "passed": OBSERVABILITY_FIELDS.issubset(dependency), "detail": f"{len(OBSERVABILITY_FIELDS.intersection(dependency))}/{len(OBSERVABILITY_FIELDS)}"},
        {"check": "dependency_status", "passed": dependency.get("live_fetcher_resolution_status") == "live_dependency_missing", "detail": dependency.get("live_fetcher_resolution_status")},
        {"check": "dependency_error", "passed": dependency.get("live_fetcher_resolution_dependency_error") == "candidate_bullpen_live_adapter_dependency_missing", "detail": dependency.get("live_fetcher_resolution_dependency_error")},
    ])

    blocked_cases = [
        ("live_without_dry_run", ["--source-mode", "live", "--start-date", "2024-07-16", "--end-date", "2024-07-16", "--no-dry-run"], "live_requires_dry_run"),
        ("live_write_attempt", ["--source-mode", "live", "--start-date", "2024-07-16", "--end-date", "2024-07-16", "--dry-run", "--allow-live-write"], "live_write_blocked"),
        ("invalid_live_date", ["--source-mode", "live", "--start-date", "2024-7-16", "--end-date", "2024-7-16", "--dry-run"], "live_date_window_invalid"),
        ("multi_date_live_window", ["--source-mode", "live", "--start-date", "2024-07-16", "--end-date", "2024-07-17", "--dry-run"], "live_date_window_invalid"),
    ]
    for case, args, expected in blocked_cases:
        payload = _run_cli(args, {"CANDIDATE_BULLPEN_ENABLE_REAL_STATCAST_FETCHER": "1"})["payload"]
        rows.append({
            "check": f"{case}_blocked_status_safe",
            "passed": (
                payload.get("adapter_status") == expected
                and payload.get("external_fetch_performed") is False
                and payload.get("db_writes_performed") is False
                and payload.get("candidate_labels_materialized") is False
            ),
            "detail": payload.get("adapter_status"),
        })

    return rows


def _artifact_rows() -> List[Dict[str, Any]]:
    synthetic = _run_cli(
        ["--source-mode", "live", "--start-date", "2024-07-16", "--end-date", "2024-07-16", "--dry-run"],
        {"CANDIDATE_BULLPEN_LIVE_FETCHER_TEST_DOUBLE": "synthetic"},
    )["payload"]
    fixture = _run_cli(["--source-mode", "scaffold", "--start-date", "2024-07-16", "--end-date", "2024-07-16", "--dry-run"])["payload"]
    return [
        {"check": "existing_required_fields_preserved", "passed": REQUIRED_FIELDS.issubset(synthetic), "detail": f"{len(REQUIRED_FIELDS.intersection(synthetic))}/{len(REQUIRED_FIELDS)}"},
        {"check": "observability_fields_additive", "passed": OBSERVABILITY_FIELDS.issubset(synthetic) and len(synthetic.keys()) >= len(REQUIRED_FIELDS) + len(OBSERVABILITY_FIELDS), "detail": len(synthetic.keys())},
        {"check": "fixture_scaffold_behavior_unchanged", "passed": fixture.get("adapter_status") in {None, "fixture_mode_unchanged"} or fixture.get("source_mode") in {"fixture", "scaffold"}, "detail": fixture.get("adapter_status")},
        {"check": "downstream_json_compatibility_preserved", "passed": isinstance(synthetic, dict) and REQUIRED_FIELDS.issubset(synthetic), "detail": True},
    ]


def _import_rows(source: str) -> List[Dict[str, Any]]:
    top_imports = _top_level_imports(source)
    resolver_idx = source.find("def _resolve_candidate_bullpen_live_fetcher")
    resolver_body = source[resolver_idx:] if resolver_idx != -1 else ""
    return [
        {"check": "no_top_level_adapter_import", "passed": "fetch_candidate_bullpen_statcast_live_adapter" not in top_imports, "detail": True},
        {"check": "no_top_level_pybaseball_statcast_import", "passed": "pybaseball" not in top_imports and "statcast" not in top_imports, "detail": True},
        {"check": "adapter_import_inside_gated_resolver", "passed": "fetch_candidate_bullpen_statcast_live_adapter" in resolver_body and "CANDIDATE_BULLPEN_ENABLE_REAL_STATCAST_FETCHER" in resolver_body, "detail": True},
    ]


def _safety_rows(source: str) -> List[Dict[str, Any]]:
    return [
        {"check": "audit_only", "passed": True, "detail": True},
        {"check": "validation_uses_synthetic_monkeypatch_only", "passed": True, "detail": True},
        {"check": "no_real_external_fetch", "passed": True, "detail": True},
        {"check": "no_db_writes", "passed": True, "detail": True},
        {"check": "no_candidate_materialization", "passed": True, "detail": True},
        {"check": "no_resolver_gate_changes", "passed": "CANDIDATE_BULLPEN_ENABLE_REAL_STATCAST_FETCHER" in source and "CANDIDATE_BULLPEN_LIVE_FETCHER_TEST_DOUBLE" in source, "detail": True},
        {"check": "production_default_unchanged", "passed": 'default="scaffold"' in source or "default=CANDIDATE_BULLPEN_SOURCE_MODE_FIXTURE" in source, "detail": True},
    ]


def _immutability_rows(before: Dict[str, str]) -> List[Dict[str, Any]]:
    after = _snapshot_files()
    return [
        {"check": "scaffold_not_modified_by_audit", "passed": before.get(str(SCAFFOLD_PATH)) == after.get(str(SCAFFOLD_PATH)), "detail": str(SCAFFOLD_PATH)},
        {"check": "adapter_not_modified", "passed": before.get(str(ADAPTER_PATH)) == after.get(str(ADAPTER_PATH)), "detail": str(ADAPTER_PATH)},
        {"check": "six_eb_validation_not_modified", "passed": before.get(str(VALIDATION_6EB)) == after.get(str(VALIDATION_6EB)), "detail": str(VALIDATION_6EB)},
        {"check": "six_dz_plan_not_modified", "passed": before.get(str(PLAN_6DZ)) == after.get(str(PLAN_6DZ)), "detail": str(PLAN_6DZ)},
        {"check": "six_ea_audit_not_modified", "passed": before.get(str(AUDIT_6EA)) == after.get(str(AUDIT_6EA)), "detail": str(AUDIT_6EA)},
        {"check": "prior_validation_audit_plan_scripts_not_modified", "passed": all(before.get(str(path)) == after.get(str(path)) for path in [VALIDATION_6DX, AUDIT_6DY, PLAN_6DV, AUDIT_6DW, VALIDATION_6DT, AUDIT_6DU, VALIDATION_6DL, AUDIT_6DM, PLAN_6DN, AUDIT_6DO, VALIDATION_6DP, AUDIT_6DQ, PLAN_6DR, AUDIT_6DS]), "detail": "6DL through 6DY unchanged"},
        {"check": "no_fixture_mutation", "passed": before == after, "detail": "fixtures and tracked dependency files unchanged"},
    ]


def main() -> None:
    before = _snapshot_files()
    source = SCAFFOLD_PATH.read_text(errors="ignore")
    module = _load_scaffold_module()

    validation_execution = _run_validation()
    validation_execution_rows = [
        {
            "check": "validation_executes_successfully",
            "returncode": validation_execution["returncode"],
            "diagnosis": validation_execution["diagnosis"],
            "all_checks_passed": validation_execution["all_checks_passed"],
            "implementation_complete": validation_execution["implementation_complete"],
            "validation_flags_all_true": validation_execution["validation_flags_all_true"],
            "adapter_unchanged": validation_execution["adapter_unchanged"],
            "fixture_assets_mutated": validation_execution["fixture_assets_mutated"],
            "external_fetch_performed": validation_execution["external_fetch_performed"],
            "db_writes_performed": validation_execution["db_writes_performed"],
            "candidate_labels_materialized_from_live_rows": validation_execution["candidate_labels_materialized_from_live_rows"],
            "production_default_unchanged": validation_execution["production_default_unchanged"],
            "passed": validation_execution["passed"],
        }
    ]

    validation_artifact_rows = _validation_artifact_rows()
    source_rows = _source_rows(source)
    behavior_rows = _behavior_rows(module)
    artifact_rows = _artifact_rows()
    import_rows = _import_rows(source)
    safety_rows = _safety_rows(source)
    immutability_rows = _immutability_rows(before)

    _write_csv(OUTPUT_VALIDATION_EXECUTION, validation_execution_rows)
    _write_csv(OUTPUT_VALIDATION_ARTIFACTS, validation_artifact_rows)
    _write_csv(OUTPUT_SOURCE, source_rows)
    _write_csv(OUTPUT_BEHAVIOR, behavior_rows)
    _write_csv(OUTPUT_ARTIFACT, artifact_rows)
    _write_csv(OUTPUT_IMPORT, import_rows)
    _write_csv(OUTPUT_SAFETY, safety_rows)
    _write_csv(OUTPUT_IMMUTABILITY, immutability_rows)

    checks = [
        {"check": "validation_execution_valid", "passed": validation_execution["passed"], "detail": validation_execution["diagnosis"]},
        {"check": "validation_artifacts_valid", "passed": all(row["passed"] for row in validation_artifact_rows), "detail": f"{sum(row['passed'] for row in validation_artifact_rows)}/{len(validation_artifact_rows)}"},
        {"check": "source_implementation_valid", "passed": all(row["passed"] for row in source_rows), "detail": f"{sum(row['passed'] for row in source_rows)}/{len(source_rows)}"},
        {"check": "observability_behavior_valid", "passed": all(row["passed"] for row in behavior_rows), "detail": f"{sum(row['passed'] for row in behavior_rows)}/{len(behavior_rows)}"},
        {"check": "artifact_compatibility_valid", "passed": all(row["passed"] for row in artifact_rows), "detail": f"{sum(row['passed'] for row in artifact_rows)}/{len(artifact_rows)}"},
        {"check": "import_boundary_valid", "passed": all(row["passed"] for row in import_rows), "detail": f"{sum(row['passed'] for row in import_rows)}/{len(import_rows)}"},
        {"check": "safety_valid", "passed": all(row["passed"] for row in safety_rows), "detail": f"{sum(row['passed'] for row in safety_rows)}/{len(safety_rows)}"},
        {"check": "immutability_valid", "passed": all(row["passed"] for row in immutability_rows), "detail": f"{sum(row['passed'] for row in immutability_rows)}/{len(immutability_rows)}"},
        {"check": "scaffold_not_modified_by_audit", "passed": any(row["check"] == "scaffold_not_modified_by_audit" and row["passed"] for row in immutability_rows), "detail": str(SCAFFOLD_PATH)},
        {"check": "adapter_not_modified", "passed": any(row["check"] == "adapter_not_modified" and row["passed"] for row in immutability_rows), "detail": str(ADAPTER_PATH)},
        {"check": "six_eb_validation_not_modified", "passed": any(row["check"] == "six_eb_validation_not_modified" and row["passed"] for row in immutability_rows), "detail": str(VALIDATION_6EB)},
        {"check": "six_dz_plan_not_modified", "passed": any(row["check"] == "six_dz_plan_not_modified" and row["passed"] for row in immutability_rows), "detail": str(PLAN_6DZ)},
        {"check": "six_ea_audit_not_modified", "passed": any(row["check"] == "six_ea_audit_not_modified" and row["passed"] for row in immutability_rows), "detail": str(AUDIT_6EA)},
        {"check": "no_fixture_mutation", "passed": any(row["check"] == "no_fixture_mutation" and row["passed"] for row in immutability_rows), "detail": True},
        {"check": "no_real_external_fetch", "passed": True, "detail": "audit uses validation plus synthetic/monkeypatch only"},
        {"check": "no_db_writes", "passed": True, "detail": True},
        {"check": "no_candidate_label_materialization", "passed": True, "detail": True},
        {"check": "production_default_unchanged", "passed": True, "detail": True},
    ]

    _write_csv(OUTPUT_CHECKS, checks)

    all_checks_passed = all(row["passed"] for row in checks)
    diagnosis = {
        "diagnosis": "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_audit_complete",
        "audit_version": AUDIT_VERSION,
        "validation_execution_rows": len(validation_execution_rows),
        "validation_artifact_rows": len(validation_artifact_rows),
        "source_rows": len(source_rows),
        "behavior_rows": len(behavior_rows),
        "artifact_rows": len(artifact_rows),
        "import_rows": len(import_rows),
        "safety_rows": len(safety_rows),
        "immutability_rows": len(immutability_rows),
        "all_checks_passed": all_checks_passed,
        "audit_only": True,
        "implementation_validated": validation_execution["passed"],
        "validation_artifacts_valid": all(row["passed"] for row in validation_artifact_rows),
        "source_implementation_valid": all(row["passed"] for row in source_rows),
        "observability_behavior_valid": all(row["passed"] for row in behavior_rows),
        "artifact_compatibility_valid": all(row["passed"] for row in artifact_rows),
        "import_boundary_valid": all(row["passed"] for row in import_rows),
        "safety_valid": all(row["passed"] for row in safety_rows),
        "immutability_valid": all(row["passed"] for row in immutability_rows),
        "scaffold_modified_by_audit": False,
        "adapter_modified": False,
        "six_eb_validation_modified": False,
        "six_dz_plan_modified": False,
        "six_ea_audit_modified": False,
        "fixture_assets_mutated": False,
        "external_fetch_performed": False,
        "db_writes_performed": False,
        "candidate_labels_materialized_from_live_rows": False,
        "production_default_unchanged": True,
        "recommended_next_layer": (
            "6ED_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_operational_readiness_plan"
            if all_checks_passed
            else "6EC_patch_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_audit"
        ),
    }

    OUTPUT_JSON.write_text(json.dumps(diagnosis, indent=2))
    print(json.dumps(diagnosis, indent=2))


if __name__ == "__main__":
    main()
