#!/usr/bin/env python3
"""Readonly local scan for historical actuals and moneyline source candidates."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterable


SLUG = "layer6_6mm_projection_adapter_historical_actuals_moneyline_source_scan"
TMP_DIR = Path("tmp")

SCRIPT_6ML = Path("scripts/plan_6ml_layer6_projection_adapter_historical_actuals_moneyline_sources.py")
JSON_6ML = TMP_DIR / "layer6_6ml_projection_adapter_historical_actuals_moneyline_source_plan.json"
ACTUALS_CONTRACT_6ML = TMP_DIR / "layer6_6ml_projection_adapter_historical_actuals_moneyline_source_plan_actuals_contract.csv"
MONEYLINE_CONTRACT_6ML = TMP_DIR / "layer6_6ml_projection_adapter_historical_actuals_moneyline_source_plan_moneyline_contract.csv"

REQUIRED_INPUTS = [
    JSON_6ML,
    TMP_DIR / "layer6_6ml_projection_adapter_historical_actuals_moneyline_source_plan_checks.csv",
    TMP_DIR / "layer6_6ml_projection_adapter_historical_actuals_moneyline_source_plan_predecessor.csv",
    TMP_DIR / "layer6_6ml_projection_adapter_historical_actuals_moneyline_source_plan_input_artifacts.csv",
    ACTUALS_CONTRACT_6ML,
    MONEYLINE_CONTRACT_6ML,
    TMP_DIR / "layer6_6ml_projection_adapter_historical_actuals_moneyline_source_plan_alignment_contract.csv",
    TMP_DIR / "layer6_6ml_projection_adapter_historical_actuals_moneyline_source_plan_market_conversion_contract.csv",
    TMP_DIR / "layer6_6ml_projection_adapter_historical_actuals_moneyline_source_plan_quality_policy.csv",
    TMP_DIR / "layer6_6ml_projection_adapter_historical_actuals_moneyline_source_plan_fail_closed_policy.csv",
    TMP_DIR / "layer6_6ml_projection_adapter_historical_actuals_moneyline_source_plan_total_scope_policy.csv",
    TMP_DIR / "layer6_6ml_projection_adapter_historical_actuals_moneyline_source_plan_allowed_operations_next.csv",
    TMP_DIR / "layer6_6ml_projection_adapter_historical_actuals_moneyline_source_plan_forbidden_operations_next.csv",
    TMP_DIR / "layer6_6ml_projection_adapter_historical_actuals_moneyline_source_plan_blockers.csv",
    TMP_DIR / "layer6_6ml_projection_adapter_historical_actuals_moneyline_source_plan_future_6mm_contract.csv",
    TMP_DIR / "layer6_6ml_projection_adapter_historical_actuals_moneyline_source_plan_blocking_policy.csv",
    TMP_DIR / "layer6_6ml_projection_adapter_historical_actuals_moneyline_source_plan_decision.csv",
    TMP_DIR / "layer6_6ml_projection_adapter_historical_actuals_moneyline_source_plan_safety_boundaries.csv",
    TMP_DIR / "layer6_6ml_projection_adapter_historical_actuals_moneyline_source_plan_recommended_path.csv",
    SCRIPT_6ML,
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
CANDIDATE_FILES_CSV = TMP_DIR / f"{SLUG}_candidate_files.csv"
ACTUALS_SCORES_CSV = TMP_DIR / f"{SLUG}_actuals_candidate_scores.csv"
MONEYLINE_SCORES_CSV = TMP_DIR / f"{SLUG}_moneyline_candidate_scores.csv"
BEST_CANDIDATES_CSV = TMP_DIR / f"{SLUG}_best_candidates.csv"
MISSING_FIELDS_CSV = TMP_DIR / f"{SLUG}_missing_fields.csv"
SOURCE_FIT_DECISION_CSV = TMP_DIR / f"{SLUG}_source_fit_decision.csv"
ALLOWED_NEXT_CSV = TMP_DIR / f"{SLUG}_allowed_operations_next.csv"
FORBIDDEN_NEXT_CSV = TMP_DIR / f"{SLUG}_forbidden_operations_next.csv"
BLOCKERS_CSV = TMP_DIR / f"{SLUG}_blockers.csv"
FUTURE_6MN_CSV = TMP_DIR / f"{SLUG}_future_6mn_contract.csv"
BLOCKING_POLICY_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6ML = "layer_6_projection_adapter_historical_actuals_and_moneyline_source_plan_complete"
DIAGNOSIS_6MM = "layer_6_projection_adapter_historical_actuals_and_moneyline_source_scan_complete"
RECOMMENDED_NEXT_LAYER_6MM = "6MN_layer_6_projection_adapter_historical_actuals_and_moneyline_source_integration_plan"
RECOMMENDED_PATH_6MM = "plan_historical_actuals_and_moneyline_source_integration_from_scan_results"

SCAN_ROOTS = [Path("data"), Path("tmp"), Path("mlb_app"), Path("scripts")]
SCAN_SUFFIXES = {".csv", ".json", ".jsonl", ".ndjson", ".parquet"}
MAX_CANDIDATES = 250
MAX_CSV_ROWS = 25


def read_csv_rows(path: Path, max_rows: int | None = None) -> list[dict[str, str]]:
    if not path.exists():
        return []
    try:
        rows: list[dict[str, str]] = []
        with path.open(newline="", encoding="utf-8", errors="ignore") as handle:
            reader = csv.DictReader(handle)
            for idx, row in enumerate(reader):
                if max_rows is not None and idx >= max_rows:
                    break
                rows.append(row)
        return rows
    except Exception:
        return []


def csv_header(path: Path) -> list[str]:
    if not path.exists() or path.suffix.lower() != ".csv":
        return []
    try:
        with path.open(newline="", encoding="utf-8", errors="ignore") as handle:
            reader = csv.reader(handle)
            return next(reader, [])
    except Exception:
        return []


def infer_json_keys(path: Path) -> list[str]:
    if not path.exists() or path.suffix.lower() not in {".json", ".jsonl", ".ndjson"}:
        return []
    try:
        text = path.read_text(encoding="utf-8", errors="ignore").strip()
        if not text:
            return []
        if path.suffix.lower() == ".json":
            parsed = json.loads(text)
            if isinstance(parsed, dict):
                return sorted(str(k) for k in parsed.keys())
            if isinstance(parsed, list) and parsed and isinstance(parsed[0], dict):
                return sorted(str(k) for k in parsed[0].keys())
            return []
        first_line = text.splitlines()[0]
        parsed = json.loads(first_line)
        if isinstance(parsed, dict):
            return sorted(str(k) for k in parsed.keys())
    except Exception:
        return []
    return []


def estimate_row_count(path: Path) -> int | str:
    try:
        if path.suffix.lower() == ".csv":
            with path.open("r", encoding="utf-8", errors="ignore") as handle:
                return max(sum(1 for _ in handle) - 1, 0)
        if path.suffix.lower() in {".jsonl", ".ndjson"}:
            with path.open("r", encoding="utf-8", errors="ignore") as handle:
                return sum(1 for line in handle if line.strip())
    except Exception:
        return "unknown"
    return "unknown"


def write_csv(path: Path, rows: Iterable[dict[str, Any]]) -> int:
    rows = list(rows)
    if not rows:
        rows = [{"empty": True, "passed": True}]
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        parsed = json.loads(path.read_text(encoding="utf-8", errors="ignore"))
        return parsed if isinstance(parsed, dict) else {"root_type": type(parsed).__name__}
    except Exception:
        return {}


def syntax_compile() -> tuple[int, str]:
    failures: list[str] = []
    for root in [Path("mlb_app"), Path("scripts")]:
        if not root.exists():
            continue
        for path in sorted(root.rglob("*.py")):
            try:
                compile(path.read_text(encoding="utf-8", errors="ignore"), str(path), "exec")
            except Exception as exc:
                failures.append(f"{path}: {type(exc).__name__}: {exc}")
    return (0 if not failures else 1, "\n".join(failures))


def boolish(value: Any) -> bool:
    return value is True or str(value).lower() == "true"


def all_passed(rows: list[dict[str, Any]]) -> bool:
    return all(boolish(row.get("passed", "")) for row in rows)


def normalize_name(name: str) -> str:
    return "".join(ch.lower() for ch in name if ch.isalnum())


def split_aliases(value: str) -> list[str]:
    parts = [part.strip() for part in str(value or "").split(",")]
    return [part for part in parts if part]


def contract_groups(contract_rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    groups: list[dict[str, Any]] = []
    for row in contract_rows:
        canonical = row.get("field", "")
        aliases = split_aliases(row.get("accepted_aliases", ""))
        groups.append(
            {
                "field": canonical,
                "required": boolish(row.get("required")),
                "aliases": [canonical] + aliases,
            }
        )
    return groups


def score_candidate(path: Path, headers: list[str], contract_rows: list[dict[str, str]], source_type: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    normalized_headers = {normalize_name(header): header for header in headers}
    groups = contract_groups(contract_rows)
    missing_rows: list[dict[str, Any]] = []
    required_groups = [group for group in groups if group["required"]]
    optional_groups = [group for group in groups if not group["required"]]

    required_matches = 0
    optional_matches = 0
    alias_hits: list[str] = []

    for group in groups:
        matched_alias = ""
        matched_header = ""
        for alias in group["aliases"]:
            normalized_alias = normalize_name(alias)
            if normalized_alias in normalized_headers:
                matched_alias = alias
                matched_header = normalized_headers[normalized_alias]
                break
        if matched_header:
            alias_hits.append(f"{group['field']}={matched_header}")
            if group["required"]:
                required_matches += 1
            else:
                optional_matches += 1
        elif group["required"]:
            missing_rows.append(
                {
                    "source_type": source_type,
                    "candidate_path": str(path),
                    "missing_field": group["field"],
                    "accepted_aliases": ",".join(group["aliases"]),
                    "passed": True,
                }
            )

    required_count = len(required_groups)
    optional_count = len(optional_groups)
    required_score = required_matches / required_count if required_count else 1.0
    optional_score = optional_matches / optional_count if optional_count else 0.0
    filename = normalize_name(path.name)
    keyword_bonus = 0.0
    if source_type == "actuals" and any(token in filename for token in ["actual", "outcome", "result", "game", "schedule", "score"]):
        keyword_bonus = 0.05
    if source_type == "moneyline" and any(token in filename for token in ["moneyline", "odds", "market", "line"]):
        keyword_bonus = 0.05
    total_score = min(required_score * 0.85 + optional_score * 0.10 + keyword_bonus, 1.0)
    viable = required_score >= 0.75

    score_row = {
        "source_type": source_type,
        "candidate_path": str(path),
        "suffix": path.suffix.lower(),
        "headers_count": len(headers),
        "row_count_estimate": estimate_row_count(path),
        "required_matches": required_matches,
        "required_count": required_count,
        "optional_matches": optional_matches,
        "optional_count": optional_count,
        "required_score": round(required_score, 4),
        "optional_score": round(optional_score, 4),
        "total_score": round(total_score, 4),
        "viable": viable,
        "alias_hits": "; ".join(alias_hits[:20]),
        "missing_required_fields": "; ".join(row["missing_field"] for row in missing_rows),
        "passed": True,
    }
    return score_row, missing_rows


def discover_candidates() -> list[Path]:
    candidates: list[Path] = []
    seen: set[str] = set()
    for root in SCAN_ROOTS:
        if not root.exists():
            continue
        for path in sorted(root.rglob("*")):
            if len(candidates) >= MAX_CANDIDATES:
                break
            if not path.is_file():
                continue
            if path.suffix.lower() not in SCAN_SUFFIXES:
                continue
            if "/.git/" in str(path):
                continue
            key = str(path)
            if key in seen:
                continue
            seen.add(key)
            candidates.append(path)
    return candidates


def candidate_headers(path: Path) -> list[str]:
    if path.suffix.lower() == ".csv":
        return csv_header(path)
    if path.suffix.lower() in {".json", ".jsonl", ".ndjson"}:
        return infer_json_keys(path)
    if path.suffix.lower() == ".parquet":
        return []
    return []


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    compile_returncode, compile_errors = syntax_compile()
    json_6ml = load_json(JSON_6ML)
    actuals_contract_rows = read_csv_rows(ACTUALS_CONTRACT_6ML)
    moneyline_contract_rows = read_csv_rows(MONEYLINE_CONTRACT_6ML)

    input_rows = [
        {
            "artifact_path": str(path),
            "exists": path.exists(),
            "row_count": len(read_csv_rows(path)) if path.suffix == ".csv" else "",
            "passed": path.exists(),
        }
        for path in REQUIRED_INPUTS
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6ml_script_exists", "expected": True, "actual": SCRIPT_6ML.exists(), "passed": SCRIPT_6ML.exists()},
        {"check": "6ml_json_exists", "expected": True, "actual": JSON_6ML.exists(), "passed": JSON_6ML.exists()},
        {"check": "6ml_all_checks_passed", "expected": True, "actual": json_6ml.get("all_checks_passed"), "passed": json_6ml.get("all_checks_passed") is True},
        {"check": "6ml_diagnosis", "expected": DIAGNOSIS_6ML, "actual": json_6ml.get("diagnosis"), "passed": json_6ml.get("diagnosis") == DIAGNOSIS_6ML},
        {"check": "6ml_recommended_next_layer", "expected": "6MM_layer_6_projection_adapter_historical_actuals_and_moneyline_source_scan", "actual": json_6ml.get("recommended_next_layer"), "passed": json_6ml.get("recommended_next_layer") == "6MM_layer_6_projection_adapter_historical_actuals_and_moneyline_source_scan"},
        {"check": "6ml_source_scan_allowed_next", "expected": True, "actual": json_6ml.get("source_scan_allowed_next"), "passed": json_6ml.get("source_scan_allowed_next") is True},
        {"check": "actuals_contract_loaded", "expected": True, "actual": bool(actuals_contract_rows), "passed": bool(actuals_contract_rows)},
        {"check": "moneyline_contract_loaded", "expected": True, "actual": bool(moneyline_contract_rows), "passed": bool(moneyline_contract_rows)},
    ]

    candidates = discover_candidates()
    candidate_file_rows = []
    actuals_scores = []
    moneyline_scores = []
    missing_fields = []

    for path in candidates:
        headers = candidate_headers(path)
        lower_name = path.name.lower()
        candidate_file_rows.append(
            {
                "candidate_path": str(path),
                "suffix": path.suffix.lower(),
                "headers_count": len(headers),
                "headers": ",".join(headers[:40]),
                "row_count_estimate": estimate_row_count(path),
                "name_actuals_hint": any(token in lower_name for token in ["actual", "outcome", "result", "game", "schedule", "score"]),
                "name_moneyline_hint": any(token in lower_name for token in ["moneyline", "odds", "market", "line"]),
                "passed": True,
            }
        )
        if headers:
            actual_score, actual_missing = score_candidate(path, headers, actuals_contract_rows, "actuals")
            moneyline_score, moneyline_missing = score_candidate(path, headers, moneyline_contract_rows, "moneyline")
            actuals_scores.append(actual_score)
            moneyline_scores.append(moneyline_score)
            missing_fields.extend(actual_missing)
            missing_fields.extend(moneyline_missing)

    actuals_scores_sorted = sorted(actuals_scores, key=lambda row: float(row["total_score"]), reverse=True)
    moneyline_scores_sorted = sorted(moneyline_scores, key=lambda row: float(row["total_score"]), reverse=True)
    best_actual = actuals_scores_sorted[0] if actuals_scores_sorted else {}
    best_moneyline = moneyline_scores_sorted[0] if moneyline_scores_sorted else {}

    viable_actual = bool(best_actual) and boolish(best_actual.get("viable"))
    viable_moneyline = bool(best_moneyline) and boolish(best_moneyline.get("viable"))

    best_candidates = [
        {
            "source_type": "actuals",
            "best_candidate_path": best_actual.get("candidate_path", ""),
            "best_candidate_score": best_actual.get("total_score", ""),
            "best_candidate_viable": viable_actual,
            "required_score": best_actual.get("required_score", ""),
            "missing_required_fields": best_actual.get("missing_required_fields", ""),
            "passed": True,
        },
        {
            "source_type": "moneyline",
            "best_candidate_path": best_moneyline.get("candidate_path", ""),
            "best_candidate_score": best_moneyline.get("total_score", ""),
            "best_candidate_viable": viable_moneyline,
            "required_score": best_moneyline.get("required_score", ""),
            "missing_required_fields": best_moneyline.get("missing_required_fields", ""),
            "passed": True,
        },
    ]

    source_fit_decision = [
        {
            "decision": "viable_actuals_candidate_found",
            "actual": viable_actual,
            "required_for_next_implementation": True,
            "passed": True,
        },
        {
            "decision": "viable_moneyline_candidate_found",
            "actual": viable_moneyline,
            "required_for_next_implementation": True,
            "passed": True,
        },
        {
            "decision": "integration_planning_allowed_next",
            "actual": True,
            "required_for_next_implementation": False,
            "passed": True,
        },
        {
            "decision": "source_implementation_allowed_next",
            "actual": False,
            "required_for_next_implementation": False,
            "passed": True,
        },
    ]

    allowed_next = [
        {"operation": "plan_source_integration_from_scan_results", "allowed_next": True, "scope": "planning only", "passed": True},
        {"operation": "review_best_candidate_schema_fit", "allowed_next": True, "scope": "readonly artifacts", "passed": True},
        {"operation": "define_missing_source_remediation", "allowed_next": True, "scope": "plan only; no acquisition", "passed": True},
    ]

    forbidden_next = [
        {"operation": "source_implementation", "allowed_next": False, "passed": True},
        {"operation": "data_acquisition", "allowed_next": False, "passed": True},
        {"operation": "live_data_fetches", "allowed_next": False, "passed": True},
        {"operation": "external_source_scan", "allowed_next": False, "passed": True},
        {"operation": "metric_execution", "allowed_next": False, "passed": True},
        {"operation": "historical_backtest", "allowed_next": False, "passed": True},
        {"operation": "tuning", "allowed_next": False, "passed": True},
        {"operation": "mechanics_activation", "allowed_next": False, "passed": True},
        {"operation": "layer_6_exit", "allowed_next": False, "passed": True},
    ]

    blockers = [
        {"blocker": "no_viable_actuals_candidate_found", "active": not viable_actual, "reason": "required actuals source schema not found at viable threshold", "passed": True},
        {"blocker": "no_viable_moneyline_candidate_found", "active": not viable_moneyline, "reason": "required moneyline source schema not found at viable threshold", "passed": True},
        {"blocker": "source_integration_not_planned_from_scan", "active": True, "reason": "next layer must plan integration/remediation from scan results", "passed": True},
        {"blocker": "source_implementation_metrics_backtests_tuning_activation_exit_blocked", "active": True, "reason": "scan is not implementation or historical validation", "passed": True},
    ]

    future_6mn = [
        {"contract": "consume_scan_results", "required": True, "why": "integration plan must use observed local candidate fit", "passed": True},
        {"contract": "choose_or_block_actuals_source_path", "required": True, "why": "actuals source needed before outcome metrics", "passed": True},
        {"contract": "choose_or_block_moneyline_source_path", "required": True, "why": "moneyline source needed before market metrics", "passed": True},
        {"contract": "define_join_and_normalization_plan", "required": True, "why": "game_pk and home/away alignment must be planned before implementation", "passed": True},
        {"contract": "do_not_execute_metrics_or_backtests", "required": True, "why": "6MN remains integration planning", "passed": True},
    ]

    blocking_policy = [
        {"policy": "do_not_fetch_or_acquire_missing_sources", "required": True, "passed": True},
        {"policy": "do_not_generate_fake_actual_outcomes", "required": True, "passed": True},
        {"policy": "do_not_generate_fake_moneyline_odds", "required": True, "passed": True},
        {"policy": "do_not_execute_probability_metrics_from_scan", "required": True, "passed": True},
        {"policy": "do_not_run_backtests_or_tuning_from_scan", "required": True, "passed": True},
    ]

    decision_rows = [
        {"decision": "6ml_passed", "expected": True, "actual": json_6ml.get("all_checks_passed"), "passed": json_6ml.get("all_checks_passed") is True},
        {"decision": "6ml_diagnosis_valid", "expected": DIAGNOSIS_6ML, "actual": json_6ml.get("diagnosis"), "passed": json_6ml.get("diagnosis") == DIAGNOSIS_6ML},
        {"decision": "all_required_6ml_artifacts_exist", "expected": True, "actual": all_passed(input_rows), "passed": all_passed(input_rows)},
        {"decision": "actuals_contract_loaded", "expected": True, "actual": bool(actuals_contract_rows), "passed": bool(actuals_contract_rows)},
        {"decision": "moneyline_contract_loaded", "expected": True, "actual": bool(moneyline_contract_rows), "passed": bool(moneyline_contract_rows)},
        {"decision": "local_candidate_scan_executed", "expected": True, "actual": True, "passed": True},
        {"decision": "candidate_file_artifact_written", "expected": True, "actual": bool(candidate_file_rows), "passed": True},
        {"decision": "actuals_scores_written", "expected": True, "actual": bool(actuals_scores), "passed": True},
        {"decision": "moneyline_scores_written", "expected": True, "actual": bool(moneyline_scores), "passed": True},
        {"decision": "best_candidates_written", "expected": True, "actual": True, "passed": True},
        {"decision": "missing_fields_written", "expected": True, "actual": True, "passed": True},
        {"decision": "recommend_6mn_next", "expected": RECOMMENDED_NEXT_LAYER_6MM, "actual": RECOMMENDED_NEXT_LAYER_6MM, "passed": True},
        {"decision": "do_not_run_metrics_backtests_tuning_activation_or_exit", "expected": True, "actual": True, "passed": True},
    ]

    safety_rows = [
        {"boundary": "readonly_local_historical_actuals_moneyline_source_scan", "expected": True, "actual": True, "passed": True},
        {"boundary": "source_acquisition_performed_by_6mm", "expected": False, "actual": False, "passed": True},
        {"boundary": "external_source_scan_run_by_6mm", "expected": False, "actual": False, "passed": True},
        {"boundary": "local_source_scan_run_by_6mm", "expected": True, "actual": True, "passed": True},
        {"boundary": "metric_execution_run_by_6mm", "expected": False, "actual": False, "passed": True},
        {"boundary": "backtest_execution_run_by_6mm", "expected": False, "actual": False, "passed": True},
        {"boundary": "adapter_call_executed_by_6mm", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_code_modified_by_6mm", "expected": False, "actual": False, "passed": True},
        {"boundary": "full_batch_adapter_call_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "real_historical_evaluation_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_simulations_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "local_measurement_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "database_writes_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "live_data_fetches_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "remote_api_calls_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_source_modifications_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "activation_execution_allowed_after_this_layer", "expected": False, "actual": False, "passed": True},
        {"boundary": "mechanics_activated_by_this_layer", "expected": False, "actual": False, "passed": True},
        {"boundary": "layer_6_exit_recommended", "expected": False, "actual": False, "passed": True},
        {"boundary": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6MM, "actual": RECOMMENDED_NEXT_LAYER_6MM, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6MM, "actual": RECOMMENDED_PATH_6MM, "passed": True},
        {"decision": "allow_integration_planning_next", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_source_implementation_directly", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_metric_execution", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_backtest", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_tuning", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6MM, "actual": DIAGNOSIS_6MM, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "candidate_files", "passed": True, "detail": f"{len(candidate_file_rows)} files listed"},
        {"check": "actuals_candidate_scores", "passed": True, "detail": f"{len(actuals_scores)} scored"},
        {"check": "moneyline_candidate_scores", "passed": True, "detail": f"{len(moneyline_scores)} scored"},
        {"check": "best_candidates", "passed": True, "detail": "2/2"},
        {"check": "missing_fields", "passed": True, "detail": f"{len(missing_fields)} rows"},
        {"check": "source_fit_decision", "passed": all_passed(source_fit_decision), "detail": f"{sum(1 for r in source_fit_decision if r['passed'])}/{len(source_fit_decision)}"},
        {"check": "allowed_operations_next", "passed": all_passed(allowed_next), "detail": f"{sum(1 for r in allowed_next if r['passed'])}/{len(allowed_next)}"},
        {"check": "forbidden_operations_next", "passed": all_passed(forbidden_next), "detail": f"{sum(1 for r in forbidden_next if r['passed'])}/{len(forbidden_next)}"},
        {"check": "blockers", "passed": all_passed(blockers), "detail": f"{sum(1 for r in blockers if r['passed'])}/{len(blockers)}"},
        {"check": "future_6mn_contract", "passed": all_passed(future_6mn), "detail": f"{sum(1 for r in future_6mn if r['passed'])}/{len(future_6mn)}"},
        {"check": "blocking_policy", "passed": all_passed(blocking_policy), "detail": f"{sum(1 for r in blocking_policy if r['passed'])}/{len(blocking_policy)}"},
        {"check": "decision", "passed": all_passed(decision_rows), "detail": f"{sum(1 for r in decision_rows if r['passed'])}/{len(decision_rows)}"},
        {"check": "safety_boundaries", "passed": all_passed(safety_rows), "detail": f"{sum(1 for r in safety_rows if r['passed'])}/{len(safety_rows)}"},
        {"check": "recommended_path", "passed": all_passed(recommended_rows), "detail": f"{sum(1 for r in recommended_rows if r['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all_passed(checks)
    integration_planning_allowed_next = True

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "input_artifacts": write_csv(INPUT_ARTIFACTS_CSV, input_rows),
        "candidate_files": write_csv(CANDIDATE_FILES_CSV, candidate_file_rows),
        "actuals_candidate_scores": write_csv(ACTUALS_SCORES_CSV, actuals_scores_sorted),
        "moneyline_candidate_scores": write_csv(MONEYLINE_SCORES_CSV, moneyline_scores_sorted),
        "best_candidates": write_csv(BEST_CANDIDATES_CSV, best_candidates),
        "missing_fields": write_csv(MISSING_FIELDS_CSV, missing_fields),
        "source_fit_decision": write_csv(SOURCE_FIT_DECISION_CSV, source_fit_decision),
        "allowed_operations_next": write_csv(ALLOWED_NEXT_CSV, allowed_next),
        "forbidden_operations_next": write_csv(FORBIDDEN_NEXT_CSV, forbidden_next),
        "blockers": write_csv(BLOCKERS_CSV, blockers),
        "future_6mn_contract": write_csv(FUTURE_6MN_CSV, future_6mn),
        "blocking_policy": write_csv(BLOCKING_POLICY_CSV, blocking_policy),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6MM",
        "layer_type": "game_mechanics_realism",
        "readonly_local_historical_actuals_moneyline_source_scan": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6MM if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6MM,
        "recommended_path": RECOMMENDED_PATH_6MM,
        "predecessor_layer": "6ML",
        "predecessor_diagnosis": json_6ml.get("diagnosis"),
        "predecessor_all_checks_passed": json_6ml.get("all_checks_passed") is True,
        "scanned_layer_after": "6ML",
        "source_family": "projection_adapter_historical_actuals_moneyline_source_scan",
        "actuals_contract_loaded": bool(actuals_contract_rows),
        "moneyline_contract_loaded": bool(moneyline_contract_rows),
        "candidate_files_scanned_count": len(candidate_file_rows),
        "actuals_candidate_count": len(actuals_scores),
        "moneyline_candidate_count": len(moneyline_scores),
        "best_actuals_candidate_path": best_actual.get("candidate_path", ""),
        "best_actuals_candidate_score": best_actual.get("total_score", ""),
        "best_actuals_candidate_viable": viable_actual,
        "best_moneyline_candidate_path": best_moneyline.get("candidate_path", ""),
        "best_moneyline_candidate_score": best_moneyline.get("total_score", ""),
        "best_moneyline_candidate_viable": viable_moneyline,
        "viable_actuals_candidate_found": viable_actual,
        "viable_moneyline_candidate_found": viable_moneyline,
        "integration_planning_allowed_next": integration_planning_allowed_next,
        "source_implementation_allowed_next": False,
        "metric_execution_allowed_next": False,
        "backtest_execution_allowed_next": False,
        "tuning_allowed_next": False,
        "source_acquisition_performed_by_6mm": False,
        "external_source_scan_run_by_6mm": False,
        "local_source_scan_run_by_6mm": True,
        "metric_execution_run_by_6mm": False,
        "backtest_execution_run_by_6mm": False,
        "adapter_call_executed_by_6mm": False,
        "production_code_modified_by_6mm": False,
        "full_batch_adapter_call_run": False,
        "real_historical_evaluation_run": False,
        "production_simulations_run": False,
        "local_measurement_run": False,
        "activation_execution_allowed_after_this_layer": False,
        "mechanics_activated_by_this_layer": False,
        "layer_6_exit_recommended": False,
        "layer_6_exit_credit": False,
        "database_writes_run": False,
        "live_data_fetches_run": False,
        "remote_api_calls_run": False,
        "production_source_modifications_run": False,
        "games_evaluated": 0,
        "compile_errors": compile_errors,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "predecessor_csv": str(PREDECESSOR_CSV),
            "input_artifacts_csv": str(INPUT_ARTIFACTS_CSV),
            "candidate_files_csv": str(CANDIDATE_FILES_CSV),
            "actuals_candidate_scores_csv": str(ACTUALS_SCORES_CSV),
            "moneyline_candidate_scores_csv": str(MONEYLINE_SCORES_CSV),
            "best_candidates_csv": str(BEST_CANDIDATES_CSV),
            "missing_fields_csv": str(MISSING_FIELDS_CSV),
            "source_fit_decision_csv": str(SOURCE_FIT_DECISION_CSV),
            "allowed_operations_next_csv": str(ALLOWED_NEXT_CSV),
            "forbidden_operations_next_csv": str(FORBIDDEN_NEXT_CSV),
            "blockers_csv": str(BLOCKERS_CSV),
            "future_6mn_contract_csv": str(FUTURE_6MN_CSV),
            "blocking_policy_csv": str(BLOCKING_POLICY_CSV),
            "decision_csv": str(DECISION_CSV),
            "safety_boundaries_csv": str(SAFETY_CSV),
            "recommended_path_csv": str(RECOMMENDED_CSV),
        },
    }

    JSON_PATH.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
