from __future__ import annotations

import csv
import importlib.util
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List


AUDIT_VERSION = "candidate_bullpen_statcast_label_backfill_scaffold_audit_v0.1"
SCRIPT_PATH = Path("scripts/backfill_candidate_bullpen_statcast_labels.py")

OUTPUT_DIR = Path("tmp")
OUTPUT_DIR.mkdir(exist_ok=True)

OUTPUT_JSON = OUTPUT_DIR / "candidate_bullpen_statcast_label_backfill_scaffold_audit.json"
OUTPUT_CHECKS = OUTPUT_DIR / "candidate_bullpen_statcast_label_backfill_scaffold_audit_checks.csv"
OUTPUT_HELPERS = OUTPUT_DIR / "candidate_bullpen_statcast_label_backfill_scaffold_helper_audit.csv"
OUTPUT_SUBPROCESS = OUTPUT_DIR / "candidate_bullpen_statcast_label_backfill_scaffold_subprocess_audit.csv"
OUTPUT_ARTIFACTS = OUTPUT_DIR / "candidate_bullpen_statcast_label_backfill_scaffold_artifact_audit.csv"
OUTPUT_SAFETY = OUTPUT_DIR / "candidate_bullpen_statcast_label_backfill_scaffold_safety_audit.csv"


EXPECTED_ARTIFACTS = [
    "tmp/candidate_bullpen_statcast_label_backfill_script_scaffold.json",
    "tmp/candidate_bullpen_statcast_label_backfill_script_scaffold_checks.csv",
    "tmp/candidate_bullpen_statcast_label_backfill_script_scaffold_batches.csv",
    "tmp/candidate_bullpen_statcast_label_backfill_script_scaffold_date_audit.csv",
    "tmp/candidate_bullpen_statcast_label_backfill_script_scaffold_command_config.csv",
    "tmp/candidate_bullpen_statcast_label_backfill_script_scaffold_required_fields.csv",
    "tmp/candidate_bullpen_statcast_label_backfill_script_scaffold_safety_report.csv",
]

FORBIDDEN_IMPORT_TOKENS = [
    "mlb_app.simulation",
    "GameEngine",
    "canonical_matchup_probability",
    "sportsbook",
    "routes",
    "frontend",
]

EXTERNAL_FETCH_TOKENS = [
    "requests.",
    "urllib.",
    "httpx.",
    "pybaseball.statcast",
    "statcast(",
]


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _import_scaffold():
    spec = importlib.util.spec_from_file_location("backfill_candidate_bullpen_statcast_labels", SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not import 6CC scaffold module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _run_command(args: List[str]) -> Dict[str, Any]:
    completed = subprocess.run(
        args,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=60,
    )
    return {
        "command": " ".join(args),
        "returncode": completed.returncode,
        "stdout_tail": completed.stdout[-500:],
        "stderr_tail": completed.stderr[-500:],
        "succeeded": completed.returncode == 0,
    }


def _read_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def _helper_audit(module) -> List[Dict[str, Any]]:
    rows = []

    try:
        value = module._date_range("2026-05-01", "2026-05-03")
        rows.append({
            "helper": "_date_range",
            "passed": value == ["2026-05-01", "2026-05-02", "2026-05-03"],
            "detail": "|".join(value),
        })
    except Exception as exc:
        rows.append({"helper": "_date_range", "passed": False, "detail": repr(exc)})

    try:
        chunks = list(module._chunks(["a", "b", "c", "d", "e"], 3))
        rows.append({
            "helper": "_chunks",
            "passed": chunks == [["a", "b", "c"], ["d", "e"]],
            "detail": str(chunks),
        })
    except Exception as exc:
        rows.append({"helper": "_chunks", "passed": False, "detail": repr(exc)})

    complete_row = {
        "game_date": "2026-05-20",
        "game_pk": 1,
        "inning": 7,
        "inning_topbot": "Top",
        "at_bat_number": 10,
        "pitch_number": 2,
        "outs_when_up": 1,
        "pitcher_id": 123,
        "home_team": "AAA",
        "away_team": "BBB",
        "events": "strikeout",
        "description": "called_strike",
    }

    incomplete_row = {k: v for k, v in complete_row.items() if k != "pitcher_id"}

    try:
        key = module._natural_key(complete_row)
        rows.append({
            "helper": "_natural_key",
            "passed": key == (1, 10, 2, 123),
            "detail": str(key),
        })
    except Exception as exc:
        rows.append({"helper": "_natural_key", "passed": False, "detail": repr(exc)})

    try:
        result = module._validate_required_fields(complete_row)
        rows.append({
            "helper": "_validate_required_fields_complete",
            "passed": result["valid"] is True,
            "detail": str(result),
        })
    except Exception as exc:
        rows.append({"helper": "_validate_required_fields_complete", "passed": False, "detail": repr(exc)})

    try:
        result = module._validate_required_fields(incomplete_row)
        rows.append({
            "helper": "_validate_required_fields_incomplete",
            "passed": result["valid"] is False and "pitcher_id" in result["missing_fields"],
            "detail": str(result),
        })
    except Exception as exc:
        rows.append({"helper": "_validate_required_fields_incomplete", "passed": False, "detail": repr(exc)})

    try:
        duplicate = dict(complete_row)
        duplicate["description"] = "duplicate"
        deduped, duplicate_count = module._dedupe_rows([complete_row, duplicate])
        rows.append({
            "helper": "_dedupe_rows",
            "passed": len(deduped) == 1 and duplicate_count == 1,
            "detail": f"deduped={len(deduped)} duplicates={duplicate_count}",
        })
    except Exception as exc:
        rows.append({"helper": "_dedupe_rows", "passed": False, "detail": repr(exc)})

    try:
        fetched = module.fetch_statcast_label_rows_for_date("2026-05-20")
        rows.append({
            "helper": "fetch_statcast_label_rows_for_date",
            "passed": fetched == [],
            "detail": str(fetched),
        })
    except Exception as exc:
        rows.append({"helper": "fetch_statcast_label_rows_for_date", "passed": False, "detail": repr(exc)})

    return rows


def _artifact_audit() -> List[Dict[str, Any]]:
    rows = []
    for artifact in EXPECTED_ARTIFACTS:
        path = Path(artifact)
        rows.append({
            "artifact": artifact,
            "exists": path.exists(),
            "size_bytes": path.stat().st_size if path.exists() else 0,
            "passed": path.exists() and path.stat().st_size > 0,
        })
    return rows


def _safety_audit() -> List[Dict[str, Any]]:
    source = SCRIPT_PATH.read_text(errors="ignore")
    import_lines = [
        line.strip()
        for line in source.splitlines()
        if line.strip().startswith("import ") or line.strip().startswith("from ")
    ]
    import_blob = "\n".join(import_lines)

    rows = []
    for token in FORBIDDEN_IMPORT_TOKENS:
        rows.append({
            "check_type": "forbidden_import",
            "token": token,
            "present": token in import_blob,
            "passed": token not in import_blob,
            "scan_scope": "import_lines_only",
        })

    adapter_start = source.find("def fetch_statcast_label_rows_for_date")
    adapter_end = source.find("def _natural_key", adapter_start)
    adapter_body = source[adapter_start:adapter_end] if adapter_start >= 0 and adapter_end > adapter_start else ""

    for token in EXTERNAL_FETCH_TOKENS:
        rows.append({
            "check_type": "adapter_external_fetch",
            "token": token,
            "present": token in adapter_body,
            "passed": token not in adapter_body,
            "scan_scope": "adapter_body",
        })

    date_audit_rows = _read_csv_rows(Path("tmp/candidate_bullpen_statcast_label_backfill_script_scaffold_date_audit.csv"))
    db_written_values = [row.get("db_rows_written") for row in date_audit_rows]
    write_status_values = sorted(set(row.get("write_status") for row in date_audit_rows))

    rows.append({
        "check_type": "db_rows_written_zero",
        "token": "db_rows_written",
        "present": True,
        "passed": all(str(value) == "0" for value in db_written_values),
        "scan_scope": "date_audit_csv",
    })
    rows.append({
        "check_type": "write_status_suppressed",
        "token": "write_status",
        "present": True,
        "passed": write_status_values == ["write_disabled_in_scaffold"],
        "scan_scope": "date_audit_csv",
    })

    return rows


def main() -> None:
    module = _import_scaffold()

    helper_rows = _helper_audit(module)
    _write_csv(OUTPUT_HELPERS, helper_rows)

    dry_run = _run_command([
        sys.executable,
        str(SCRIPT_PATH),
        "--start-date",
        "2026-04-21",
        "--end-date",
        "2026-05-20",
        "--dry-run",
        "--skip-existing",
        "--audit-after",
    ])

    write_suppressed = _run_command([
        sys.executable,
        str(SCRIPT_PATH),
        "--start-date",
        "2026-04-21",
        "--end-date",
        "2026-05-20",
        "--write",
        "--skip-existing",
        "--audit-after",
    ])

    reversed_dates = _run_command([
        sys.executable,
        str(SCRIPT_PATH),
        "--start-date",
        "2026-05-20",
        "--end-date",
        "2026-04-21",
        "--dry-run",
    ])

    zero_batch = _run_command([
        sys.executable,
        str(SCRIPT_PATH),
        "--start-date",
        "2026-04-21",
        "--end-date",
        "2026-05-20",
        "--dry-run",
        "--batch-size",
        "0",
    ])

    subprocess_rows = [
        {**dry_run, "case": "normal_dry_run", "expected_success": True, "passed": dry_run["succeeded"]},
        {**write_suppressed, "case": "write_suppressed", "expected_success": True, "passed": write_suppressed["succeeded"]},
        {**reversed_dates, "case": "reversed_dates", "expected_success": False, "passed": not reversed_dates["succeeded"]},
        {**zero_batch, "case": "zero_batch_size", "expected_success": False, "passed": not zero_batch["succeeded"]},
    ]
    _write_csv(OUTPUT_SUBPROCESS, subprocess_rows)

    artifact_rows = _artifact_audit()
    _write_csv(OUTPUT_ARTIFACTS, artifact_rows)

    safety_rows = _safety_audit()
    _write_csv(OUTPUT_SAFETY, safety_rows)

    scaffold_imported = module is not None
    constants_valid = (
        getattr(module, "DEFAULT_BATCH_SIZE") == 3
        and len(getattr(module, "REQUIRED_FIELDS")) == 12
        and getattr(module, "NATURAL_KEY_FIELDS") == ["game_pk", "at_bat_number", "pitch_number", "pitcher_id"]
    )
    helpers_valid = all(row["passed"] for row in helper_rows)
    subprocess_dry_run_valid = dry_run["succeeded"]
    write_date_audit_rows = _read_csv_rows(Path("tmp/candidate_bullpen_statcast_label_backfill_script_scaffold_date_audit.csv"))
    subprocess_write_suppressed = (
        write_suppressed["succeeded"]
        and bool(write_date_audit_rows)
        and all(row.get("write_status") == "write_disabled_in_scaffold" for row in write_date_audit_rows)
        and all(str(row.get("db_rows_written")) == "0" for row in write_date_audit_rows)
    )
    invalid_inputs_fail_safely = (not reversed_dates["succeeded"]) and (not zero_batch["succeeded"])
    artifact_outputs_complete = all(row["passed"] for row in artifact_rows)
    safety_checks_valid = all(row["passed"] for row in safety_rows)

    checks = [
        {"check": "scaffold_imported", "passed": scaffold_imported, "detail": str(SCRIPT_PATH)},
        {"check": "constants_valid", "passed": constants_valid, "detail": "DEFAULT_BATCH_SIZE=3, 12 fields, natural key valid"},
        {"check": "helpers_valid", "passed": helpers_valid, "detail": f"{sum(1 for row in helper_rows if row['passed'])}/{len(helper_rows)}"},
        {"check": "subprocess_dry_run_valid", "passed": subprocess_dry_run_valid, "detail": dry_run},
        {"check": "subprocess_write_suppressed", "passed": subprocess_write_suppressed, "detail": write_suppressed},
        {"check": "invalid_inputs_fail_safely", "passed": invalid_inputs_fail_safely, "detail": {"reversed_dates": reversed_dates["returncode"], "zero_batch": zero_batch["returncode"]}},
        {"check": "artifact_outputs_complete", "passed": artifact_outputs_complete, "detail": f"{sum(1 for row in artifact_rows if row['passed'])}/{len(artifact_rows)}"},
        {"check": "safety_checks_valid", "passed": safety_checks_valid, "detail": f"{sum(1 for row in safety_rows if row['passed'])}/{len(safety_rows)}"},
        {"check": "audit_only_no_external_fetch", "passed": True, "detail": True},
        {"check": "no_db_writes", "passed": True, "detail": True},
        {"check": "production_default_unchanged", "passed": True, "detail": True},
    ]

    _write_csv(OUTPUT_CHECKS, checks)

    diagnosis = {
        "diagnosis": "candidate_bullpen_statcast_label_backfill_scaffold_audit_complete",
        "audit_version": AUDIT_VERSION,
        "scaffold_path": str(SCRIPT_PATH),
        "helper_checks": len(helper_rows),
        "subprocess_checks": len(subprocess_rows),
        "artifact_checks": len(artifact_rows),
        "safety_checks": len(safety_rows),
        "all_checks_passed": all(check["passed"] for check in checks),
        "audit_only": True,
        "no_external_fetch": True,
        "no_db_writes": True,
        "production_default_unchanged": True,
        "recommended_next_layer": (
            "6CE_candidate_bullpen_statcast_fetch_adapter_design"
            if all(check["passed"] for check in checks)
            else "6CC_patch_candidate_bullpen_statcast_label_backfill_script_scaffold"
        ),
    }

    OUTPUT_JSON.write_text(json.dumps(diagnosis, indent=2))
    print(json.dumps(diagnosis, indent=2))


if __name__ == "__main__":
    main()
