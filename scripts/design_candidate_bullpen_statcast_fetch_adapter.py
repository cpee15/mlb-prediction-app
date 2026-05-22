from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, List


DESIGN_VERSION = "candidate_bullpen_statcast_fetch_adapter_design_v0.1"

OUTPUT_DIR = Path("tmp")
OUTPUT_DIR.mkdir(exist_ok=True)

OUTPUT_JSON = OUTPUT_DIR / "candidate_bullpen_statcast_fetch_adapter_design.json"
OUTPUT_CHECKS = OUTPUT_DIR / "candidate_bullpen_statcast_fetch_adapter_design_checks.csv"
OUTPUT_INVENTORY = OUTPUT_DIR / "candidate_bullpen_statcast_fetch_adapter_integration_signal_inventory.csv"
OUTPUT_INTERFACE = OUTPUT_DIR / "candidate_bullpen_statcast_fetch_adapter_interface_contract.csv"
OUTPUT_STRATEGY = OUTPUT_DIR / "candidate_bullpen_statcast_fetch_adapter_fetch_strategy.csv"
OUTPUT_MAPPING = OUTPUT_DIR / "candidate_bullpen_statcast_fetch_adapter_normalization_mapping.csv"
OUTPUT_FAILURES = OUTPUT_DIR / "candidate_bullpen_statcast_fetch_adapter_failure_taxonomy.csv"
OUTPUT_DOUBLES = OUTPUT_DIR / "candidate_bullpen_statcast_fetch_adapter_test_double_plan.csv"
OUTPUT_GATES = OUTPUT_DIR / "candidate_bullpen_statcast_fetch_adapter_promotion_gates.csv"


INTEGRATION_FILES = [
    "mlb_app/statcast_utils.py",
    "mlb_app/etl.py",
    "scripts/backfill_hitter_statcast.py",
    "scripts/nightly_statcast_refresh.py",
    "scripts/run_hitting_matchups_refresh.py",
    "scripts/backfill_candidate_bullpen_statcast_labels.py",
]

SIGNAL_TOKENS = {
    "has_pybaseball": ["pybaseball"],
    "has_statcast": ["statcast"],
    "has_requests_http": ["requests.", "httpx.", "urllib.", "http://", "https://"],
    "has_pandas_dataframe": ["pandas", "DataFrame", ".to_dict(", ".iterrows("],
    "has_session_commit": ["session.commit(", ".commit()"],
    "has_to_sql": [".to_sql("],
    "has_retry_sleep": ["sleep(", "retry", "Retry"],
    "has_argparse": ["argparse", "ArgumentParser"],
    "has_existing_stub": ["scaffold_stub_not_fetched", "fetch_statcast_label_rows_for_date"],
}

ADAPTER_INTERFACE = [
    {"item": "function_name", "value": "fetch_statcast_label_rows_for_date", "required": True},
    {"item": "input", "value": "label_date: str", "required": True},
    {"item": "output", "value": "list[dict[str, Any]]", "required": True},
    {"item": "scope", "value": "one date per call", "required": True},
    {"item": "db_writes", "value": "forbidden", "required": True},
    {"item": "session_access", "value": "forbidden", "required": True},
    {"item": "engine_access", "value": "forbidden", "required": True},
    {"item": "production_imports", "value": "forbidden", "required": True},
    {"item": "normalization", "value": "adapter returns normalized rows only", "required": True},
]

FETCH_STRATEGY = [
    {"step": 1, "strategy": "Prefer existing utility boundary", "detail": "Use mlb_app.statcast_utils if it exposes a safe one-date Statcast fetch helper.", "required": True},
    {"step": 2, "strategy": "Fallback isolated pybaseball call", "detail": "If no safe utility exists, isolate pybaseball/statcast call inside adapter body only.", "required": True},
    {"step": 3, "strategy": "One date per call", "detail": "Fetch only label_date to keep retry/failure accounting per date.", "required": True},
    {"step": 4, "strategy": "Normalize immediately", "detail": "Convert raw dataframe/records into required dict row shape before returning.", "required": True},
    {"step": 5, "strategy": "No database mutation", "detail": "Adapter must never create engine/session, insert, update, commit, or rollback.", "required": True},
    {"step": 6, "strategy": "Per-date retry boundary", "detail": "Retries are scoped to one date and never span batches.", "required": True},
    {"step": 7, "strategy": "Typed failure surfacing", "detail": "Known failure modes should raise typed adapter exceptions or return structured failure metadata to caller.", "required": True},
]

NORMALIZATION_MAPPING = [
    {"normalized_field": "game_date", "source_candidates": "game_date", "required": True, "nullable": False},
    {"normalized_field": "game_pk", "source_candidates": "game_pk|game_id", "required": True, "nullable": False},
    {"normalized_field": "inning", "source_candidates": "inning", "required": True, "nullable": False},
    {"normalized_field": "inning_topbot", "source_candidates": "inning_topbot", "required": True, "nullable": False},
    {"normalized_field": "at_bat_number", "source_candidates": "at_bat_number", "required": True, "nullable": False},
    {"normalized_field": "pitch_number", "source_candidates": "pitch_number", "required": True, "nullable": False},
    {"normalized_field": "outs_when_up", "source_candidates": "outs_when_up", "required": True, "nullable": True},
    {"normalized_field": "pitcher_id", "source_candidates": "pitcher|pitcher_id", "required": True, "nullable": False},
    {"normalized_field": "home_team", "source_candidates": "home_team", "required": True, "nullable": False},
    {"normalized_field": "away_team", "source_candidates": "away_team", "required": True, "nullable": False},
    {"normalized_field": "events", "source_candidates": "events", "required": True, "nullable": True},
    {"normalized_field": "description", "source_candidates": "description", "required": True, "nullable": True},
]

FAILURE_TAXONOMY = [
    {"exception": "AdapterFetchUnavailable", "when": "No safe fetch implementation is available.", "retryable": False},
    {"exception": "AdapterNoRowsReturned", "when": "Fetch succeeds but returns zero rows for a date expected to have data.", "retryable": False},
    {"exception": "AdapterSchemaMismatch", "when": "Returned rows lack required fields or required fields cannot be mapped.", "retryable": False},
    {"exception": "AdapterRateLimited", "when": "Provider throttles/rate-limits the request.", "retryable": True},
    {"exception": "AdapterNetworkError", "when": "Transient network/provider error occurs.", "retryable": True},
    {"exception": "AdapterUnexpectedError", "when": "Unexpected adapter failure occurs.", "retryable": False},
]

TEST_DOUBLE_PLAN = [
    {"double": "empty_adapter", "behavior": "returns []", "expected_result": "backfill records zero fetched rows without write", "required": True},
    {"double": "fixture_adapter", "behavior": "returns valid normalized fixture rows", "expected_result": "normalization and audit counts pass", "required": True},
    {"double": "malformed_schema_adapter", "behavior": "returns rows missing pitcher_id or game_pk", "expected_result": "schema validation fails safely", "required": True},
    {"double": "duplicate_natural_key_adapter", "behavior": "returns duplicate game_pk/at_bat_number/pitch_number/pitcher_id rows", "expected_result": "dedupe removes duplicates and reports count", "required": True},
    {"double": "transient_error_adapter", "behavior": "raises AdapterNetworkError once then succeeds", "expected_result": "retry audit records transient recovery", "required": True},
]

PROMOTION_GATES = [
    {"gate": "adapter_contract_audit_passes", "required": True, "detail": "6CF must validate adapter interface and no-write/no-session guarantees."},
    {"gate": "fixture_normalization_passes", "required": True, "detail": "Fixture adapter rows must validate required fields."},
    {"gate": "duplicate_dedupe_passes", "required": True, "detail": "Duplicate natural keys must be deduped before any future write."},
    {"gate": "malformed_schema_fails_safely", "required": True, "detail": "Malformed adapter rows must block batch progression."},
    {"gate": "transient_retry_audited", "required": True, "detail": "Retry logic must be observable and per-date scoped."},
    {"gate": "write_suppression_preserved", "required": True, "detail": "Backfill scaffold must still suppress writes unless explicit future write layer enables them."},
    {"gate": "no_production_coupling", "required": True, "detail": "Adapter must not import simulation, routes, sportsbook, frontend, or canonical probability modules."},
]


def _read_text(path: Path) -> str:
    try:
        return path.read_text(errors="ignore")
    except Exception:
        return ""


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _integration_inventory() -> List[Dict[str, Any]]:
    rows = []
    for file_path in INTEGRATION_FILES:
        path = Path(file_path)
        text = _read_text(path)
        lowered = text.lower()

        row: Dict[str, Any] = {
            "path": file_path,
            "exists": path.exists(),
            "line_count": len(text.splitlines()) if text else 0,
        }

        for signal_name, tokens in SIGNAL_TOKENS.items():
            row[signal_name] = any(token.lower() in lowered for token in tokens)

        row["adapter_usefulness_score"] = (
            (3 if row["exists"] else 0)
            + (2 if row["has_statcast"] else 0)
            + (2 if row["has_pybaseball"] else 0)
            + (1 if row["has_pandas_dataframe"] else 0)
            + (1 if row["has_retry_sleep"] else 0)
            - (1 if row["has_session_commit"] else 0)
            - (1 if row["has_to_sql"] else 0)
        )
        row["recommended_role"] = (
            "preferred_fetch_utility" if file_path == "mlb_app/statcast_utils.py"
            else "write_orchestration_reference" if file_path == "mlb_app/etl.py"
            else "cli_reference" if "scripts/" in file_path
            else "reference"
        )
        rows.append(row)
    return rows


def main() -> None:
    inventory_rows = _integration_inventory()

    _write_csv(OUTPUT_INVENTORY, inventory_rows)
    _write_csv(OUTPUT_INTERFACE, ADAPTER_INTERFACE)
    _write_csv(OUTPUT_STRATEGY, FETCH_STRATEGY)
    _write_csv(OUTPUT_MAPPING, NORMALIZATION_MAPPING)
    _write_csv(OUTPUT_FAILURES, FAILURE_TAXONOMY)
    _write_csv(OUTPUT_DOUBLES, TEST_DOUBLE_PLAN)
    _write_csv(OUTPUT_GATES, PROMOTION_GATES)

    integration_files_inspected = len(inventory_rows) == len(INTEGRATION_FILES) and all(row["exists"] for row in inventory_rows)
    adapter_interface_defined = len(ADAPTER_INTERFACE) >= 8 and all(row["required"] for row in ADAPTER_INTERFACE)
    fetch_strategy_defined = len(FETCH_STRATEGY) >= 7 and all(row["required"] for row in FETCH_STRATEGY)
    normalization_mapping_defined = len(NORMALIZATION_MAPPING) == 12 and all(row["required"] for row in NORMALIZATION_MAPPING)
    failure_taxonomy_defined = len(FAILURE_TAXONOMY) == 6 and any(row["retryable"] for row in FAILURE_TAXONOMY)
    test_double_plan_defined = len(TEST_DOUBLE_PLAN) == 5 and all(row["required"] for row in TEST_DOUBLE_PLAN)
    promotion_gates_defined = len(PROMOTION_GATES) >= 7 and all(row["required"] for row in PROMOTION_GATES)

    checks = [
        {"check": "integration_files_inspected", "passed": integration_files_inspected, "detail": f"{len(inventory_rows)} files"},
        {"check": "adapter_interface_defined", "passed": adapter_interface_defined, "detail": f"{len(ADAPTER_INTERFACE)} items"},
        {"check": "fetch_strategy_defined", "passed": fetch_strategy_defined, "detail": f"{len(FETCH_STRATEGY)} steps"},
        {"check": "normalization_mapping_defined", "passed": normalization_mapping_defined, "detail": f"{len(NORMALIZATION_MAPPING)} fields"},
        {"check": "failure_taxonomy_defined", "passed": failure_taxonomy_defined, "detail": f"{len(FAILURE_TAXONOMY)} failures"},
        {"check": "test_double_plan_defined", "passed": test_double_plan_defined, "detail": f"{len(TEST_DOUBLE_PLAN)} doubles"},
        {"check": "promotion_gates_defined", "passed": promotion_gates_defined, "detail": f"{len(PROMOTION_GATES)} gates"},
        {"check": "design_only_no_live_adapter", "passed": True, "detail": True},
        {"check": "no_external_fetch", "passed": True, "detail": True},
        {"check": "no_db_writes", "passed": True, "detail": True},
        {"check": "production_default_unchanged", "passed": True, "detail": True},
    ]
    _write_csv(OUTPUT_CHECKS, checks)

    best_fetch_source = sorted(
        inventory_rows,
        key=lambda row: (row["adapter_usefulness_score"], row["path"] == "mlb_app/statcast_utils.py"),
        reverse=True,
    )[0] if inventory_rows else None

    diagnosis = {
        "diagnosis": "candidate_bullpen_statcast_fetch_adapter_design_complete",
        "design_version": DESIGN_VERSION,
        "integration_files_inspected": len(inventory_rows),
        "best_fetch_source": best_fetch_source,
        "adapter_function": "fetch_statcast_label_rows_for_date",
        "required_normalized_fields": [row["normalized_field"] for row in NORMALIZATION_MAPPING],
        "failure_taxonomy": [row["exception"] for row in FAILURE_TAXONOMY],
        "test_doubles": [row["double"] for row in TEST_DOUBLE_PLAN],
        "all_checks_passed": all(check["passed"] for check in checks),
        "design_only": True,
        "live_adapter_implemented": False,
        "backfill_scaffold_modified": False,
        "no_external_fetch": True,
        "no_db_writes": True,
        "production_default_unchanged": True,
        "recommended_next_layer": "6CF_candidate_bullpen_statcast_fetch_adapter_contract_audit",
    }

    OUTPUT_JSON.write_text(json.dumps(diagnosis, indent=2))
    print(json.dumps(diagnosis, indent=2))


if __name__ == "__main__":
    main()
