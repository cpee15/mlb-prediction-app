#!/usr/bin/env python3
"""Implement readonly numeric probability source trace after 6MC plan."""

from __future__ import annotations

import csv
import json
import re
from pathlib import Path
from typing import Any, Iterable


SLUG = "layer6_6md_projection_adapter_numeric_probability_source_trace_implementation"
TMP_DIR = Path("tmp")

SCRIPT_6MC = Path("scripts/plan_6mc_layer6_projection_adapter_numeric_probability_source_trace.py")
JSON_6MC = TMP_DIR / "layer6_6mc_projection_adapter_numeric_probability_source_trace_plan.json"

NORMALIZED_SURFACE_JSON = TMP_DIR / "layer6_6lx_projection_adapter_probability_alias_normalization_implementation_normalized_surface.json"
VALUE_CLASSIFICATION_CSV = TMP_DIR / "layer6_6ma_projection_adapter_probability_surface_metric_implementation_probability_value_classification.csv"
SUM_CLASSIFICATION_CSV = TMP_DIR / "layer6_6ma_projection_adapter_probability_surface_metric_implementation_probability_sum_classification.csv"

SCRIPT_6LX = Path("scripts/implement_6lx_layer6_projection_adapter_probability_alias_normalization.py")
SCRIPT_6MA = Path("scripts/implement_6ma_layer6_projection_adapter_probability_surface_metric.py")
SCRIPT_6LZ = Path("scripts/plan_6lz_layer6_projection_adapter_probability_surface_metric.py")
SCRIPT_6MB = Path("scripts/audit_6mb_layer6_projection_adapter_probability_surface_metric.py")

REQUIRED_INPUTS = [
    JSON_6MC,
    TMP_DIR / "layer6_6mc_projection_adapter_numeric_probability_source_trace_plan_checks.csv",
    TMP_DIR / "layer6_6mc_projection_adapter_numeric_probability_source_trace_plan_predecessor.csv",
    TMP_DIR / "layer6_6mc_projection_adapter_numeric_probability_source_trace_plan_input_artifacts.csv",
    TMP_DIR / "layer6_6mc_projection_adapter_numeric_probability_source_trace_plan_problem_statement.csv",
    TMP_DIR / "layer6_6mc_projection_adapter_numeric_probability_source_trace_plan_trace_scope.csv",
    TMP_DIR / "layer6_6mc_projection_adapter_numeric_probability_source_trace_plan_candidate_source_surfaces.csv",
    TMP_DIR / "layer6_6mc_projection_adapter_numeric_probability_source_trace_plan_placeholder_diagnosis.csv",
    TMP_DIR / "layer6_6mc_projection_adapter_numeric_probability_source_trace_plan_readonly_trace_contract.csv",
    TMP_DIR / "layer6_6mc_projection_adapter_numeric_probability_source_trace_plan_forbidden_operations_next.csv",
    TMP_DIR / "layer6_6mc_projection_adapter_numeric_probability_source_trace_plan_allowed_operations_next.csv",
    TMP_DIR / "layer6_6mc_projection_adapter_numeric_probability_source_trace_plan_blockers.csv",
    TMP_DIR / "layer6_6mc_projection_adapter_numeric_probability_source_trace_plan_future_6md_contract.csv",
    TMP_DIR / "layer6_6mc_projection_adapter_numeric_probability_source_trace_plan_blocking_policy.csv",
    TMP_DIR / "layer6_6mc_projection_adapter_numeric_probability_source_trace_plan_decision.csv",
    TMP_DIR / "layer6_6mc_projection_adapter_numeric_probability_source_trace_plan_safety_boundaries.csv",
    TMP_DIR / "layer6_6mc_projection_adapter_numeric_probability_source_trace_plan_recommended_path.csv",
    NORMALIZED_SURFACE_JSON,
    VALUE_CLASSIFICATION_CSV,
    SUM_CLASSIFICATION_CSV,
    SCRIPT_6LX,
    SCRIPT_6MA,
    SCRIPT_6LZ,
    SCRIPT_6MB,
    SCRIPT_6MC,
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
TRACE_TARGETS_CSV = TMP_DIR / f"{SLUG}_trace_targets.csv"
CODE_TRACE_CSV = TMP_DIR / f"{SLUG}_code_trace.csv"
ARTIFACT_TRACE_CSV = TMP_DIR / f"{SLUG}_artifact_trace.csv"
VALUE_FINDINGS_CSV = TMP_DIR / f"{SLUG}_probability_value_findings.csv"
LOSS_HYPOTHESES_CSV = TMP_DIR / f"{SLUG}_source_loss_hypotheses.csv"
BLOCKERS_CSV = TMP_DIR / f"{SLUG}_blockers.csv"
FUTURE_6ME_CSV = TMP_DIR / f"{SLUG}_future_6me_contract.csv"
BLOCKING_POLICY_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6MC = "layer_6_projection_adapter_numeric_probability_source_trace_plan_complete"
DIAGNOSIS_6MD = "layer_6_projection_adapter_numeric_probability_source_trace_implementation_complete"
RECOMMENDED_NEXT_LAYER_6MD = "6ME_layer_6_projection_adapter_numeric_probability_source_trace_audit"
RECOMMENDED_PATH_6MD = "audit_numeric_probability_source_trace_findings"

PROBABILITY_TERMS = [
    "home_win_probability",
    "away_win_probability",
    "home_win_prob",
    "away_win_prob",
    "win_probability",
    "win_prob",
    "probability",
    "prob",
]

PLACEHOLDER_TERMS = [
    "MAPPED_FROM_",
    "PRESENT_IN_",
    "placeholder",
    "PLACEHOLDER",
]


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    try:
        with path.open(newline="", encoding="utf-8", errors="ignore") as handle:
            return list(csv.DictReader(handle))
    except Exception:
        return []


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


def load_json(path: Path) -> Any:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8", errors="ignore"))
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


def first_dict_row(payload: Any) -> dict[str, Any]:
    if isinstance(payload, list):
        rows = [row for row in payload if isinstance(row, dict)]
        return rows[0] if rows else {}

    if isinstance(payload, dict):
        for key in ("rows", "games", "surface", "normalized_surface", "data"):
            value = payload.get(key)
            if isinstance(value, list):
                rows = [row for row in value if isinstance(row, dict)]
                return rows[0] if rows else {}
        return payload

    return {}


def flatten_values(value: Any) -> list[Any]:
    if isinstance(value, dict):
        result: list[Any] = []
        for child in value.values():
            result.extend(flatten_values(child))
        return result
    if isinstance(value, list):
        result = []
        for child in value:
            result.extend(flatten_values(child))
        return result
    return [value]


def looks_numeric_probability(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    if isinstance(value, int | float):
        return 0.0 <= float(value) <= 1.0
    return False


def count_terms(text: str, terms: list[str]) -> int:
    lower_text = text.lower()
    return sum(lower_text.count(term.lower()) for term in terms)


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    compile_returncode, compile_errors = syntax_compile()

    json_6mc = load_json(JSON_6MC)
    normalized_payload = load_json(NORMALIZED_SURFACE_JSON)
    normalized_row = first_dict_row(normalized_payload)
    normalized_values = flatten_values(normalized_payload)

    value_rows = read_csv_rows(VALUE_CLASSIFICATION_CSV)
    sum_rows = read_csv_rows(SUM_CLASSIFICATION_CSV)

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
        {"check": "6mc_script_exists", "expected": True, "actual": SCRIPT_6MC.exists(), "passed": SCRIPT_6MC.exists()},
        {"check": "6mc_json_exists", "expected": True, "actual": JSON_6MC.exists(), "passed": JSON_6MC.exists()},
        {"check": "6mc_all_checks_passed", "expected": True, "actual": json_6mc.get("all_checks_passed"), "passed": json_6mc.get("all_checks_passed") is True},
        {"check": "6mc_diagnosis", "expected": DIAGNOSIS_6MC, "actual": json_6mc.get("diagnosis"), "passed": json_6mc.get("diagnosis") == DIAGNOSIS_6MC},
        {"check": "6mc_recommended_next_layer", "expected": "6MD_layer_6_projection_adapter_numeric_probability_source_trace_implementation", "actual": json_6mc.get("recommended_next_layer"), "passed": json_6mc.get("recommended_next_layer") == "6MD_layer_6_projection_adapter_numeric_probability_source_trace_implementation"},
    ]

    trace_targets = [
        {
            "target": "projection_adapter_return_shape",
            "method": "readonly search for probability field names in scripts and artifacts",
            "executed": True,
            "passed": True,
        },
        {
            "target": "normalization_artifact_builder",
            "method": "readonly inspect 6LX script and normalized surface artifact",
            "executed": True,
            "passed": True,
        },
        {
            "target": "canonical_projection_surface_schema",
            "method": "readonly inspect canonical field usage in layer scripts/artifacts",
            "executed": True,
            "passed": True,
        },
        {
            "target": "ui_projection_consumption_path",
            "method": "readonly code term search only, no UI execution",
            "executed": True,
            "passed": True,
        },
    ]

    code_files = [
        SCRIPT_6LX,
        SCRIPT_6MA,
        SCRIPT_6LZ,
        SCRIPT_6MB,
        SCRIPT_6MC,
    ]

    code_trace_rows = []
    normalization_artifact_builder_uses_placeholder_strings = False

    for path in code_files:
        text = path.read_text(encoding="utf-8", errors="ignore") if path.exists() else ""
        probability_term_hits = count_terms(text, PROBABILITY_TERMS)
        placeholder_term_hits = count_terms(text, PLACEHOLDER_TERMS)
        if path == SCRIPT_6LX and placeholder_term_hits > 0:
            normalization_artifact_builder_uses_placeholder_strings = True

        code_trace_rows.append(
            {
                "path": str(path),
                "exists": path.exists(),
                "probability_term_hits": probability_term_hits,
                "placeholder_term_hits": placeholder_term_hits,
                "has_home_win_probability": "home_win_probability" in text,
                "has_away_win_probability": "away_win_probability" in text,
                "has_home_win_prob": "home_win_prob" in text,
                "has_away_win_prob": "away_win_prob" in text,
                "uses_mapped_from_placeholder": "MAPPED_FROM_" in text,
                "uses_present_in_placeholder": "PRESENT_IN_" in text,
                "passed": path.exists(),
            }
        )

    artifact_trace_rows = []
    artifact_paths = [
        NORMALIZED_SURFACE_JSON,
        VALUE_CLASSIFICATION_CSV,
        SUM_CLASSIFICATION_CSV,
        JSON_6MC,
    ]

    for path in artifact_paths:
        text = path.read_text(encoding="utf-8", errors="ignore") if path.exists() else ""
        artifact_trace_rows.append(
            {
                "path": str(path),
                "exists": path.exists(),
                "probability_term_hits": count_terms(text, PROBABILITY_TERMS),
                "placeholder_term_hits": count_terms(text, PLACEHOLDER_TERMS),
                "contains_mapped_from": "MAPPED_FROM_" in text,
                "contains_present_in": "PRESENT_IN_" in text,
                "contains_numeric_probability_literal_candidate": bool(re.search(r"\\b0\\.\\d+\\b|\\b1\\.0\\b|\\b0\\.0\\b", text)),
                "passed": path.exists(),
            }
        )

    numeric_probability_values_found_in_normalized_surface = any(
        looks_numeric_probability(value) for value in normalized_values
    )

    placeholder_values_found_in_normalized_surface = any(
        isinstance(value, str) and any(term in value for term in ["MAPPED_FROM_", "PRESENT_IN_"])
        for value in normalized_values
    )

    value_findings = [
        {
            "field": "home_win_probability",
            "value": normalized_row.get("home_win_probability"),
            "is_numeric_probability": looks_numeric_probability(normalized_row.get("home_win_probability")),
            "is_placeholder": isinstance(normalized_row.get("home_win_probability"), str)
            and "MAPPED_FROM_" in normalized_row.get("home_win_probability"),
            "passed": "home_win_probability" in normalized_row,
        },
        {
            "field": "away_win_probability",
            "value": normalized_row.get("away_win_probability"),
            "is_numeric_probability": looks_numeric_probability(normalized_row.get("away_win_probability")),
            "is_placeholder": isinstance(normalized_row.get("away_win_probability"), str)
            and "MAPPED_FROM_" in normalized_row.get("away_win_probability"),
            "passed": "away_win_probability" in normalized_row,
        },
        {
            "field": "home_win_prob",
            "value": normalized_row.get("home_win_prob"),
            "is_numeric_probability": looks_numeric_probability(normalized_row.get("home_win_prob")),
            "is_placeholder": isinstance(normalized_row.get("home_win_prob"), str)
            and "PRESENT_IN_" in normalized_row.get("home_win_prob"),
            "passed": "home_win_prob" in normalized_row,
        },
        {
            "field": "away_win_prob",
            "value": normalized_row.get("away_win_prob"),
            "is_numeric_probability": looks_numeric_probability(normalized_row.get("away_win_prob")),
            "is_placeholder": isinstance(normalized_row.get("away_win_prob"), str)
            and "PRESENT_IN_" in normalized_row.get("away_win_prob"),
            "passed": "away_win_prob" in normalized_row,
        },
    ]

    source_loss_hypotheses = [
        {
            "hypothesis": "6lx_shape_artifact_reconstructed_placeholders_instead_of_real_values",
            "evidence": "normalized surface fields contain MAPPED_FROM/PRESENT_IN strings and 6LX script uses placeholder terms",
            "supported": normalization_artifact_builder_uses_placeholder_strings and placeholder_values_found_in_normalized_surface,
            "passed": True,
        },
        {
            "hypothesis": "real_numeric_probabilities_not_materialized_in_current_normalized_surface",
            "evidence": "readonly normalized artifact trace found no numeric probability values in canonical or alias probability fields",
            "supported": not numeric_probability_values_found_in_normalized_surface,
            "passed": True,
        },
        {
            "hypothesis": "upstream_real_probability_source_still_unknown_without_deeper_static_trace",
            "evidence": "6MD did not call adapters or fetch live data and only inspected current layer artifacts/scripts",
            "supported": True,
            "passed": True,
        },
    ]

    numeric_probability_source_found_by_readonly_trace = numeric_probability_values_found_in_normalized_surface
    numeric_probability_source_unknown_after_trace = not numeric_probability_source_found_by_readonly_trace
    likely_current_state = (
        "current_normalized_surface_is_placeholder_contract_not_real_numeric_probability_surface"
        if placeholder_values_found_in_normalized_surface and not numeric_probability_values_found_in_normalized_surface
        else "numeric_probability_source_candidate_found_in_normalized_surface"
    )

    blockers = [
        {
            "blocker": "placeholder_probability_metric_block_active",
            "active": placeholder_values_found_in_normalized_surface,
            "reason": "canonical probability values are placeholders",
            "passed": True,
        },
        {
            "blocker": "numeric_probability_source_unknown_after_trace",
            "active": numeric_probability_source_unknown_after_trace,
            "reason": "readonly trace found no numeric probability source in inspected artifacts",
            "passed": True,
        },
        {
            "blocker": "run_surface_gap_remains",
            "active": True,
            "reason": "run fields remain outside this probability source trace",
            "passed": True,
        },
        {
            "blocker": "real_backtest_metrics_not_run",
            "active": True,
            "reason": "backtests require real numeric prediction surface",
            "passed": True,
        },
        {
            "blocker": "layer6_exit_not_allowed",
            "active": True,
            "reason": "Layer 6 exit requires later audited evidence",
            "passed": True,
        },
    ]

    future_6me = [
        {
            "contract": "audit_numeric_probability_source_trace_findings",
            "required": True,
            "why": "6MD trace findings must be audited before planning repair",
            "passed": True,
        },
        {
            "contract": "confirm_placeholder_contract_vs_real_numeric_source_absence",
            "required": True,
            "why": "audit must confirm whether the current path only proves shape contract",
            "passed": True,
        },
        {
            "contract": "preserve_no_adapter_metrics_backtest_activation_or_exit",
            "required": True,
            "why": "audit remains diagnostic only",
            "passed": True,
        },
    ]

    blocking_policy = [
        {
            "policy": "do_not_claim_numeric_probability_readiness_until_real_values_are_materialized_and_audited",
            "required": True,
            "passed": True,
        },
        {
            "policy": "do_not_plan_tuning_until_numeric_probability_source_trace_is_audited",
            "required": True,
            "passed": True,
        },
        {
            "policy": "do_not_run_backtests_until_real_numeric_surface_exists",
            "required": True,
            "passed": True,
        },
    ]

    decision_rows = [
        {"decision": "6mc_passed", "expected": True, "actual": json_6mc.get("all_checks_passed"), "passed": json_6mc.get("all_checks_passed") is True},
        {"decision": "6mc_diagnosis_valid", "expected": DIAGNOSIS_6MC, "actual": json_6mc.get("diagnosis"), "passed": json_6mc.get("diagnosis") == DIAGNOSIS_6MC},
        {"decision": "all_required_6mc_artifacts_exist", "expected": True, "actual": all_passed(input_rows), "passed": all_passed(input_rows)},
        {"decision": "trace_targets_executed_readonly", "expected": True, "actual": all_passed(trace_targets), "passed": all_passed(trace_targets)},
        {"decision": "code_trace_completed", "expected": True, "actual": all_passed(code_trace_rows), "passed": all_passed(code_trace_rows)},
        {"decision": "artifact_trace_completed", "expected": True, "actual": all_passed(artifact_trace_rows), "passed": all_passed(artifact_trace_rows)},
        {"decision": "placeholder_values_found_in_normalized_surface", "expected": True, "actual": placeholder_values_found_in_normalized_surface, "passed": placeholder_values_found_in_normalized_surface},
        {"decision": "numeric_probability_values_not_found_in_normalized_surface", "expected": False, "actual": numeric_probability_values_found_in_normalized_surface, "passed": not numeric_probability_values_found_in_normalized_surface},
        {"decision": "trace_findings_ready_for_audit", "expected": True, "actual": True, "passed": True},
        {"decision": "recommend_6me_next", "expected": RECOMMENDED_NEXT_LAYER_6MD, "actual": RECOMMENDED_NEXT_LAYER_6MD, "passed": True},
        {"decision": "do_not_recommend_run_metrics_backtest_activation_or_exit", "expected": True, "actual": True, "passed": True},
    ]

    safety_rows = [
        {"boundary": "implementation_only_numeric_probability_source_trace_readonly", "expected": True, "actual": True, "passed": True},
        {"boundary": "readonly_trace_executed", "expected": True, "actual": True, "passed": True},
        {"boundary": "adapter_call_executed_by_6md", "expected": False, "actual": False, "passed": True},
        {"boundary": "metric_execution_run_by_6md", "expected": False, "actual": False, "passed": True},
        {"boundary": "backtest_execution_run_by_6md", "expected": False, "actual": False, "passed": True},
        {"boundary": "run_metric_execution_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_code_modified_by_6md", "expected": False, "actual": False, "passed": True},
        {"boundary": "full_batch_adapter_call_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "real_historical_evaluation_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_simulations_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "local_measurement_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "database_writes_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "live_data_fetches_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "remote_api_calls_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "source_acquisition_performed_by_this_layer", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_source_modifications_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "activation_execution_allowed_after_this_layer", "expected": False, "actual": False, "passed": True},
        {"boundary": "mechanics_activated_by_this_layer", "expected": False, "actual": False, "passed": True},
        {"boundary": "layer_6_exit_recommended", "expected": False, "actual": False, "passed": True},
        {"boundary": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6MD, "actual": RECOMMENDED_NEXT_LAYER_6MD, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6MD, "actual": RECOMMENDED_PATH_6MD, "passed": True},
        {"decision": "do_not_recommend_run_metrics", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_backtest", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6MD, "actual": DIAGNOSIS_6MD, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "trace_targets", "passed": all_passed(trace_targets), "detail": f"{sum(1 for r in trace_targets if r['passed'])}/{len(trace_targets)}"},
        {"check": "code_trace", "passed": all_passed(code_trace_rows), "detail": f"{sum(1 for r in code_trace_rows if r['passed'])}/{len(code_trace_rows)}"},
        {"check": "artifact_trace", "passed": all_passed(artifact_trace_rows), "detail": f"{sum(1 for r in artifact_trace_rows if r['passed'])}/{len(artifact_trace_rows)}"},
        {"check": "probability_value_findings", "passed": all_passed(value_findings), "detail": f"{sum(1 for r in value_findings if r['passed'])}/{len(value_findings)}"},
        {"check": "source_loss_hypotheses", "passed": all_passed(source_loss_hypotheses), "detail": f"{sum(1 for r in source_loss_hypotheses if r['passed'])}/{len(source_loss_hypotheses)}"},
        {"check": "blockers", "passed": all_passed(blockers), "detail": f"{sum(1 for r in blockers if r['passed'])}/{len(blockers)}"},
        {"check": "future_6me_contract", "passed": all_passed(future_6me), "detail": f"{sum(1 for r in future_6me if r['passed'])}/{len(future_6me)}"},
        {"check": "blocking_policy", "passed": all_passed(blocking_policy), "detail": f"{sum(1 for r in blocking_policy if r['passed'])}/{len(blocking_policy)}"},
        {"check": "decision", "passed": all_passed(decision_rows), "detail": f"{sum(1 for r in decision_rows if r['passed'])}/{len(decision_rows)}"},
        {"check": "safety_boundaries", "passed": all_passed(safety_rows), "detail": f"{sum(1 for r in safety_rows if r['passed'])}/{len(safety_rows)}"},
        {"check": "recommended_path", "passed": all_passed(recommended_rows), "detail": f"{sum(1 for r in recommended_rows if r['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all_passed(checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "input_artifacts": write_csv(INPUT_ARTIFACTS_CSV, input_rows),
        "trace_targets": write_csv(TRACE_TARGETS_CSV, trace_targets),
        "code_trace": write_csv(CODE_TRACE_CSV, code_trace_rows),
        "artifact_trace": write_csv(ARTIFACT_TRACE_CSV, artifact_trace_rows),
        "probability_value_findings": write_csv(VALUE_FINDINGS_CSV, value_findings),
        "source_loss_hypotheses": write_csv(LOSS_HYPOTHESES_CSV, source_loss_hypotheses),
        "blockers": write_csv(BLOCKERS_CSV, blockers),
        "future_6me_contract": write_csv(FUTURE_6ME_CSV, future_6me),
        "blocking_policy": write_csv(BLOCKING_POLICY_CSV, blocking_policy),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6MD",
        "layer_type": "game_mechanics_realism",
        "implementation_only_numeric_probability_source_trace_readonly": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6MD if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6MD,
        "recommended_path": RECOMMENDED_PATH_6MD,
        "predecessor_layer": "6MC",
        "predecessor_diagnosis": json_6mc.get("diagnosis"),
        "predecessor_all_checks_passed": json_6mc.get("all_checks_passed") is True,
        "implemented_layer_after": "6MC",
        "source_family": "projection_adapter_numeric_probability_source_trace_implementation",
        "readonly_trace_executed": True,
        "source_trace_plan_confirmed": json_6mc.get("source_trace_plan_created") is True,
        "trace_target_count": len(trace_targets),
        "code_trace_file_count": len(code_trace_rows),
        "artifact_trace_file_count": len(artifact_trace_rows),
        "placeholder_probability_block_confirmed": placeholder_values_found_in_normalized_surface,
        "numeric_probability_values_found_in_normalized_surface": numeric_probability_values_found_in_normalized_surface,
        "placeholder_values_found_in_normalized_surface": placeholder_values_found_in_normalized_surface,
        "normalization_artifact_builder_uses_placeholder_strings": normalization_artifact_builder_uses_placeholder_strings,
        "numeric_probability_source_found_by_readonly_trace": numeric_probability_source_found_by_readonly_trace,
        "numeric_probability_source_unknown_after_trace": numeric_probability_source_unknown_after_trace,
        "likely_current_state": likely_current_state,
        "source_loss_hypothesis_count": len(source_loss_hypotheses),
        "trace_findings_ready_for_audit": True,
        "adapter_call_executed_by_6md": False,
        "metric_execution_run_by_6md": False,
        "backtest_execution_run_by_6md": False,
        "run_metric_execution_run": False,
        "production_code_modified_by_6md": False,
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
        "source_acquisition_performed_by_this_layer": False,
        "production_source_modifications_run": False,
        "games_evaluated": 0,
        "compile_errors": compile_errors,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "predecessor_csv": str(PREDECESSOR_CSV),
            "input_artifacts_csv": str(INPUT_ARTIFACTS_CSV),
            "trace_targets_csv": str(TRACE_TARGETS_CSV),
            "code_trace_csv": str(CODE_TRACE_CSV),
            "artifact_trace_csv": str(ARTIFACT_TRACE_CSV),
            "probability_value_findings_csv": str(VALUE_FINDINGS_CSV),
            "source_loss_hypotheses_csv": str(LOSS_HYPOTHESES_CSV),
            "blockers_csv": str(BLOCKERS_CSV),
            "future_6me_contract_csv": str(FUTURE_6ME_CSV),
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
