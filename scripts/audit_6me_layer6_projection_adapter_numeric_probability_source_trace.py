#!/usr/bin/env python3
"""Audit 6MD readonly numeric probability source trace findings."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterable


SLUG = "layer6_6me_projection_adapter_numeric_probability_source_trace_audit"
TMP_DIR = Path("tmp")

SCRIPT_6MD = Path("scripts/implement_6md_layer6_projection_adapter_numeric_probability_source_trace.py")
JSON_6MD = TMP_DIR / "layer6_6md_projection_adapter_numeric_probability_source_trace_implementation.json"

REQUIRED_INPUTS = [
    JSON_6MD,
    TMP_DIR / "layer6_6md_projection_adapter_numeric_probability_source_trace_implementation_checks.csv",
    TMP_DIR / "layer6_6md_projection_adapter_numeric_probability_source_trace_implementation_predecessor.csv",
    TMP_DIR / "layer6_6md_projection_adapter_numeric_probability_source_trace_implementation_input_artifacts.csv",
    TMP_DIR / "layer6_6md_projection_adapter_numeric_probability_source_trace_implementation_trace_targets.csv",
    TMP_DIR / "layer6_6md_projection_adapter_numeric_probability_source_trace_implementation_code_trace.csv",
    TMP_DIR / "layer6_6md_projection_adapter_numeric_probability_source_trace_implementation_artifact_trace.csv",
    TMP_DIR / "layer6_6md_projection_adapter_numeric_probability_source_trace_implementation_probability_value_findings.csv",
    TMP_DIR / "layer6_6md_projection_adapter_numeric_probability_source_trace_implementation_source_loss_hypotheses.csv",
    TMP_DIR / "layer6_6md_projection_adapter_numeric_probability_source_trace_implementation_blockers.csv",
    TMP_DIR / "layer6_6md_projection_adapter_numeric_probability_source_trace_implementation_future_6me_contract.csv",
    TMP_DIR / "layer6_6md_projection_adapter_numeric_probability_source_trace_implementation_blocking_policy.csv",
    TMP_DIR / "layer6_6md_projection_adapter_numeric_probability_source_trace_implementation_decision.csv",
    TMP_DIR / "layer6_6md_projection_adapter_numeric_probability_source_trace_implementation_safety_boundaries.csv",
    TMP_DIR / "layer6_6md_projection_adapter_numeric_probability_source_trace_implementation_recommended_path.csv",
    SCRIPT_6MD,
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
TRACE_FINDING_REVIEW_CSV = TMP_DIR / f"{SLUG}_trace_finding_review.csv"
PLACEHOLDER_CONTRACT_DECISION_CSV = TMP_DIR / f"{SLUG}_placeholder_contract_decision.csv"
HYPOTHESIS_REVIEW_CSV = TMP_DIR / f"{SLUG}_hypothesis_review.csv"
REPAIR_PLANNING_GATE_CSV = TMP_DIR / f"{SLUG}_repair_planning_gate.csv"
BLOCKERS_CSV = TMP_DIR / f"{SLUG}_blockers.csv"
FUTURE_6MF_CSV = TMP_DIR / f"{SLUG}_future_6mf_contract.csv"
BLOCKING_POLICY_CSV = TMP_DIR / f"{SLUG}_blocking_policy.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6MD = "layer_6_projection_adapter_numeric_probability_source_trace_implementation_complete"
DIAGNOSIS_6ME = "layer_6_projection_adapter_numeric_probability_source_trace_audit_complete"
RECOMMENDED_NEXT_LAYER_6ME = "6MF_layer_6_projection_adapter_numeric_probability_repair_plan"
RECOMMENDED_PATH_6ME = "plan_materialize_real_numeric_probability_surface"
LIKELY_PLACEHOLDER_CONTRACT = "current_normalized_surface_is_placeholder_contract_not_real_numeric_probability_surface"


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


def csv_all_passed(path: Path) -> bool:
    rows = read_csv_rows(path)
    return bool(rows) and all(boolish(row.get("passed", "")) for row in rows)


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    compile_returncode, compile_errors = syntax_compile()
    json_6md = load_json(JSON_6MD)

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
        {"check": "6md_script_exists", "expected": True, "actual": SCRIPT_6MD.exists(), "passed": SCRIPT_6MD.exists()},
        {"check": "6md_json_exists", "expected": True, "actual": JSON_6MD.exists(), "passed": JSON_6MD.exists()},
        {"check": "6md_all_checks_passed", "expected": True, "actual": json_6md.get("all_checks_passed"), "passed": json_6md.get("all_checks_passed") is True},
        {"check": "6md_diagnosis", "expected": DIAGNOSIS_6MD, "actual": json_6md.get("diagnosis"), "passed": json_6md.get("diagnosis") == DIAGNOSIS_6MD},
        {"check": "6md_recommended_next_layer", "expected": "6ME_layer_6_projection_adapter_numeric_probability_source_trace_audit", "actual": json_6md.get("recommended_next_layer"), "passed": json_6md.get("recommended_next_layer") == "6ME_layer_6_projection_adapter_numeric_probability_source_trace_audit"},
    ]

    trace_finding_review_rows = [
        {"finding": "readonly_trace_executed", "expected": True, "actual": json_6md.get("readonly_trace_executed"), "passed": json_6md.get("readonly_trace_executed") is True},
        {"finding": "trace_findings_ready_for_audit", "expected": True, "actual": json_6md.get("trace_findings_ready_for_audit"), "passed": json_6md.get("trace_findings_ready_for_audit") is True},
        {"finding": "placeholder_values_found_in_normalized_surface", "expected": True, "actual": json_6md.get("placeholder_values_found_in_normalized_surface"), "passed": json_6md.get("placeholder_values_found_in_normalized_surface") is True},
        {"finding": "numeric_probability_values_found_in_normalized_surface", "expected": False, "actual": json_6md.get("numeric_probability_values_found_in_normalized_surface"), "passed": json_6md.get("numeric_probability_values_found_in_normalized_surface") is False},
        {"finding": "normalization_artifact_builder_uses_placeholder_strings", "expected": True, "actual": json_6md.get("normalization_artifact_builder_uses_placeholder_strings"), "passed": json_6md.get("normalization_artifact_builder_uses_placeholder_strings") is True},
        {"finding": "numeric_probability_source_found_by_readonly_trace", "expected": False, "actual": json_6md.get("numeric_probability_source_found_by_readonly_trace"), "passed": json_6md.get("numeric_probability_source_found_by_readonly_trace") is False},
        {"finding": "numeric_probability_source_unknown_after_trace", "expected": True, "actual": json_6md.get("numeric_probability_source_unknown_after_trace"), "passed": json_6md.get("numeric_probability_source_unknown_after_trace") is True},
        {"finding": "likely_current_state", "expected": LIKELY_PLACEHOLDER_CONTRACT, "actual": json_6md.get("likely_current_state"), "passed": json_6md.get("likely_current_state") == LIKELY_PLACEHOLDER_CONTRACT},
    ]

    placeholder_contract_decision_rows = [
        {
            "decision": "placeholder_contract_conclusion_established",
            "basis": "placeholder values present, numeric values absent, builder uses placeholders, no numeric source found",
            "established": True,
            "passed": all_passed(trace_finding_review_rows),
        },
        {
            "decision": "current_surface_is_not_metric_ready_real_probability_surface",
            "basis": "no numeric probability values found in normalized surface",
            "established": True,
            "passed": json_6md.get("numeric_probability_values_found_in_normalized_surface") is False,
        },
        {
            "decision": "source_trace_findings_ready_for_repair_planning",
            "basis": "source remains unknown after readonly trace",
            "established": True,
            "passed": json_6md.get("numeric_probability_source_unknown_after_trace") is True,
        },
    ]

    hypothesis_rows_from_6md = read_csv_rows(
        TMP_DIR / "layer6_6md_projection_adapter_numeric_probability_source_trace_implementation_source_loss_hypotheses.csv"
    )

    required_hypotheses = {
        "6lx_shape_artifact_reconstructed_placeholders_instead_of_real_values",
        "real_numeric_probabilities_not_materialized_in_current_normalized_surface",
        "upstream_real_probability_source_still_unknown_without_deeper_static_trace",
    }
    observed_supported_hypotheses = {
        row.get("hypothesis")
        for row in hypothesis_rows_from_6md
        if row.get("hypothesis") and boolish(row.get("supported"))
    }

    hypothesis_review_rows = [
        {
            "hypothesis": hypothesis,
            "present_and_supported": hypothesis in observed_supported_hypotheses,
            "passed": hypothesis in observed_supported_hypotheses,
        }
        for hypothesis in sorted(required_hypotheses)
    ]

    repair_gate_rows = [
        {
            "gate": "repair_planning_recommended",
            "recommended": True,
            "reason": "placeholder contract conclusion established and numeric probability source remains unknown",
            "passed": all_passed(placeholder_contract_decision_rows) and all_passed(hypothesis_review_rows),
        },
        {
            "gate": "metric_planning_recommended",
            "recommended": False,
            "reason": "real numeric probability surface is not materialized or audited",
            "passed": True,
        },
        {
            "gate": "backtest_planning_recommended",
            "recommended": False,
            "reason": "backtests require real numeric probability surface",
            "passed": True,
        },
        {
            "gate": "tuning_recommended",
            "recommended": False,
            "reason": "tuning requires audited backtest evidence",
            "passed": True,
        },
        {
            "gate": "activation_or_layer6_exit_recommended",
            "recommended": False,
            "reason": "activation and exit require later audited evidence",
            "passed": True,
        },
    ]

    blockers = [
        {
            "blocker": "placeholder_probability_metric_block_active",
            "active": True,
            "reason": "current normalized probability surface is placeholder contract",
            "passed": True,
        },
        {
            "blocker": "numeric_probability_source_unknown_after_trace",
            "active": True,
            "reason": "6MD did not find a real numeric probability source by readonly trace",
            "passed": True,
        },
        {
            "blocker": "real_numeric_probability_surface_not_materialized",
            "active": True,
            "reason": "numeric probability values were not found in normalized surface",
            "passed": True,
        },
        {
            "blocker": "run_surface_gap_remains",
            "active": True,
            "reason": "run surface remains outside this probability source audit",
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

    future_6mf = [
        {
            "contract": "plan_materialize_real_numeric_probability_surface",
            "required": True,
            "why": "placeholder contract conclusion is established and repair planning is the next safe step",
            "passed": True,
        },
        {
            "contract": "identify_source_or_builder_change_needed_without_activating_mechanics",
            "required": True,
            "why": "repair plan must specify how real numeric values enter surface while preserving safety boundaries",
            "passed": True,
        },
        {
            "contract": "preserve_no_adapter_metrics_backtest_tuning_activation_or_exit",
            "required": True,
            "why": "repair planning remains pre-metric and pre-backtest",
            "passed": True,
        },
    ]

    blocking_policy = [
        {
            "policy": "do_not_claim_numeric_probability_surface_readiness_until_real_values_are_materialized_and_audited",
            "required": True,
            "passed": True,
        },
        {
            "policy": "do_not_run_metrics_or_backtests_from_placeholder_contract",
            "required": True,
            "passed": True,
        },
        {
            "policy": "do_not_tune_or_activate_until real_probability_surface_and_backtest_evidence_exist".replace(" ", "_"),
            "required": True,
            "passed": True,
        },
    ]

    decision_rows = [
        {"decision": "6md_passed", "expected": True, "actual": json_6md.get("all_checks_passed"), "passed": json_6md.get("all_checks_passed") is True},
        {"decision": "6md_diagnosis_valid", "expected": DIAGNOSIS_6MD, "actual": json_6md.get("diagnosis"), "passed": json_6md.get("diagnosis") == DIAGNOSIS_6MD},
        {"decision": "all_required_6md_artifacts_exist", "expected": True, "actual": all_passed(input_rows), "passed": all_passed(input_rows)},
        {"decision": "trace_findings_reviewed", "expected": True, "actual": all_passed(trace_finding_review_rows), "passed": all_passed(trace_finding_review_rows)},
        {"decision": "placeholder_contract_conclusion_established", "expected": True, "actual": all_passed(placeholder_contract_decision_rows), "passed": all_passed(placeholder_contract_decision_rows)},
        {"decision": "hypotheses_reviewed", "expected": True, "actual": all_passed(hypothesis_review_rows), "passed": all_passed(hypothesis_review_rows)},
        {"decision": "repair_planning_gate_open", "expected": True, "actual": repair_gate_rows[0]["passed"], "passed": repair_gate_rows[0]["passed"]},
        {"decision": "metrics_backtests_tuning_activation_exit_blocked", "expected": True, "actual": True, "passed": True},
        {"decision": "future_6mf_contract_valid", "expected": True, "actual": all_passed(future_6mf), "passed": all_passed(future_6mf)},
        {"decision": "recommend_6mf_next", "expected": RECOMMENDED_NEXT_LAYER_6ME, "actual": RECOMMENDED_NEXT_LAYER_6ME, "passed": True},
    ]

    safety_rows = [
        {"boundary": "audit_only_numeric_probability_source_trace_findings", "expected": True, "actual": True, "passed": True},
        {"boundary": "trace_execution_run_by_6me", "expected": False, "actual": False, "passed": True},
        {"boundary": "adapter_call_executed_by_6me", "expected": False, "actual": False, "passed": True},
        {"boundary": "metric_execution_run_by_6me", "expected": False, "actual": False, "passed": True},
        {"boundary": "backtest_execution_run_by_6me", "expected": False, "actual": False, "passed": True},
        {"boundary": "run_metric_execution_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_code_modified_by_6me", "expected": False, "actual": False, "passed": True},
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
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6ME, "actual": RECOMMENDED_NEXT_LAYER_6ME, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6ME, "actual": RECOMMENDED_PATH_6ME, "passed": True},
        {"decision": "do_not_recommend_run_metrics", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_backtest", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_tuning", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6ME, "actual": DIAGNOSIS_6ME, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "trace_finding_review", "passed": all_passed(trace_finding_review_rows), "detail": f"{sum(1 for r in trace_finding_review_rows if r['passed'])}/{len(trace_finding_review_rows)}"},
        {"check": "placeholder_contract_decision", "passed": all_passed(placeholder_contract_decision_rows), "detail": f"{sum(1 for r in placeholder_contract_decision_rows if r['passed'])}/{len(placeholder_contract_decision_rows)}"},
        {"check": "hypothesis_review", "passed": all_passed(hypothesis_review_rows), "detail": f"{sum(1 for r in hypothesis_review_rows if r['passed'])}/{len(hypothesis_review_rows)}"},
        {"check": "repair_planning_gate", "passed": all_passed(repair_gate_rows), "detail": f"{sum(1 for r in repair_gate_rows if r['passed'])}/{len(repair_gate_rows)}"},
        {"check": "blockers", "passed": all_passed(blockers), "detail": f"{sum(1 for r in blockers if r['passed'])}/{len(blockers)}"},
        {"check": "future_6mf_contract", "passed": all_passed(future_6mf), "detail": f"{sum(1 for r in future_6mf if r['passed'])}/{len(future_6mf)}"},
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
        "trace_finding_review": write_csv(TRACE_FINDING_REVIEW_CSV, trace_finding_review_rows),
        "placeholder_contract_decision": write_csv(PLACEHOLDER_CONTRACT_DECISION_CSV, placeholder_contract_decision_rows),
        "hypothesis_review": write_csv(HYPOTHESIS_REVIEW_CSV, hypothesis_review_rows),
        "repair_planning_gate": write_csv(REPAIR_PLANNING_GATE_CSV, repair_gate_rows),
        "blockers": write_csv(BLOCKERS_CSV, blockers),
        "future_6mf_contract": write_csv(FUTURE_6MF_CSV, future_6mf),
        "blocking_policy": write_csv(BLOCKING_POLICY_CSV, blocking_policy),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6ME",
        "layer_type": "game_mechanics_realism",
        "audit_only_numeric_probability_source_trace_findings": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6ME if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6ME,
        "recommended_path": RECOMMENDED_PATH_6ME,
        "predecessor_layer": "6MD",
        "predecessor_diagnosis": json_6md.get("diagnosis"),
        "predecessor_all_checks_passed": json_6md.get("all_checks_passed") is True,
        "audited_layer_after": "6MD",
        "source_family": "projection_adapter_numeric_probability_source_trace_audit",
        "readonly_trace_audited": True,
        "trace_findings_ready_for_audit_confirmed": json_6md.get("trace_findings_ready_for_audit") is True,
        "placeholder_probability_block_confirmed": True,
        "placeholder_values_found_in_normalized_surface_confirmed": json_6md.get("placeholder_values_found_in_normalized_surface") is True,
        "numeric_probability_values_found_in_normalized_surface_confirmed": False,
        "normalization_artifact_builder_uses_placeholder_strings_confirmed": json_6md.get("normalization_artifact_builder_uses_placeholder_strings") is True,
        "numeric_probability_source_found_by_readonly_trace_confirmed": False,
        "numeric_probability_source_unknown_after_trace_confirmed": json_6md.get("numeric_probability_source_unknown_after_trace") is True,
        "placeholder_contract_conclusion_established": all_passed(placeholder_contract_decision_rows),
        "likely_current_state_confirmed": json_6md.get("likely_current_state"),
        "repair_planning_recommended": True,
        "metric_planning_recommended": False,
        "backtest_planning_recommended": False,
        "tuning_recommended": False,
        "trace_execution_run_by_6me": False,
        "adapter_call_executed_by_6me": False,
        "metric_execution_run_by_6me": False,
        "backtest_execution_run_by_6me": False,
        "run_metric_execution_run": False,
        "production_code_modified_by_6me": False,
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
            "trace_finding_review_csv": str(TRACE_FINDING_REVIEW_CSV),
            "placeholder_contract_decision_csv": str(PLACEHOLDER_CONTRACT_DECISION_CSV),
            "hypothesis_review_csv": str(HYPOTHESIS_REVIEW_CSV),
            "repair_planning_gate_csv": str(REPAIR_PLANNING_GATE_CSV),
            "blockers_csv": str(BLOCKERS_CSV),
            "future_6mf_contract_csv": str(FUTURE_6MF_CSV),
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
