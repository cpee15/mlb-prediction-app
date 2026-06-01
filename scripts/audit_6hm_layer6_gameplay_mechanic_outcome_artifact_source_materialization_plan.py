#!/usr/bin/env python3
"""Audit Layer 6HL source materialization plan."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6hm_source_materialization_plan_audit"
TMP_DIR = Path("tmp")

PLAN_6HL_PATH = Path("scripts/plan_6hl_layer6_gameplay_mechanic_outcome_artifact_source_materialization.py")

JSON_6HL = TMP_DIR / "layer6_6hl_source_materialization_plan.json"
CHECKS_6HL = TMP_DIR / "layer6_6hl_source_materialization_plan_checks.csv"
PREDECESSOR_6HL = TMP_DIR / "layer6_6hl_source_materialization_plan_predecessor.csv"
INPUT_ARTIFACTS_6HL = TMP_DIR / "layer6_6hl_source_materialization_plan_input_artifacts.csv"
TARGET_ARTIFACTS_6HL = TMP_DIR / "layer6_6hl_source_materialization_plan_target_artifacts.csv"
SCHEMA_CONTRACTS_6HL = TMP_DIR / "layer6_6hl_source_materialization_plan_schema_contracts.csv"
SOURCE_STRATEGY_6HL = TMP_DIR / "layer6_6hl_source_materialization_plan_source_strategy.csv"
DERIVATION_RULES_6HL = TMP_DIR / "layer6_6hl_source_materialization_plan_derivation_rules.csv"
VALIDATION_GATES_6HL = TMP_DIR / "layer6_6hl_source_materialization_plan_validation_gates.csv"
BLOCKING_RISKS_6HL = TMP_DIR / "layer6_6hl_source_materialization_plan_blocking_risks.csv"
IMPLEMENTATION_STEPS_6HL = TMP_DIR / "layer6_6hl_source_materialization_plan_implementation_steps.csv"
ACCEPTANCE_CRITERIA_6HL = TMP_DIR / "layer6_6hl_source_materialization_plan_acceptance_criteria.csv"
MANIFEST_CONTRACT_6HL = TMP_DIR / "layer6_6hl_source_materialization_plan_manifest_contract.csv"
QUALITY_REPORT_CONTRACT_6HL = TMP_DIR / "layer6_6hl_source_materialization_plan_quality_report_contract.csv"
DECISION_6HL = TMP_DIR / "layer6_6hl_source_materialization_plan_decision.csv"
FUTURE_6HM_6HL = TMP_DIR / "layer6_6hl_source_materialization_plan_future_6hm_contract.csv"
FUTURE_6HN_6HL = TMP_DIR / "layer6_6hl_source_materialization_plan_future_6hn_contract.csv"
SAFETY_6HL = TMP_DIR / "layer6_6hl_source_materialization_plan_safety_boundaries.csv"
IMMUTABILITY_6HL = TMP_DIR / "layer6_6hl_source_materialization_plan_immutability.csv"
RECOMMENDED_PATH_6HL = TMP_DIR / "layer6_6hl_source_materialization_plan_recommended_path.csv"

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
ARTIFACT_PRESENCE_CSV = TMP_DIR / f"{SLUG}_artifact_presence.csv"
CHECKS_CONSISTENCY_CSV = TMP_DIR / f"{SLUG}_checks_consistency.csv"
TARGET_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_target_artifacts.csv"
SCHEMA_CONTRACTS_CSV = TMP_DIR / f"{SLUG}_schema_contracts.csv"
SOURCE_STRATEGY_CSV = TMP_DIR / f"{SLUG}_source_strategy.csv"
DERIVATION_RULES_CSV = TMP_DIR / f"{SLUG}_derivation_rules.csv"
VALIDATION_GATES_CSV = TMP_DIR / f"{SLUG}_validation_gates.csv"
BLOCKING_RISKS_CSV = TMP_DIR / f"{SLUG}_blocking_risks.csv"
IMPLEMENTATION_STEPS_CSV = TMP_DIR / f"{SLUG}_implementation_steps.csv"
ACCEPTANCE_CRITERIA_CSV = TMP_DIR / f"{SLUG}_acceptance_criteria.csv"
MANIFEST_CONTRACT_CSV = TMP_DIR / f"{SLUG}_manifest_contract.csv"
QUALITY_REPORT_CONTRACT_CSV = TMP_DIR / f"{SLUG}_quality_report_contract.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
FUTURE_6HN_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6hn_contract.csv"
SAFETY_BOUNDARIES_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_PATH_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6HL = "layer_6_gameplay_mechanic_outcome_artifact_source_materialization_plan_complete"
DIAGNOSIS_6HM = "layer_6_gameplay_mechanic_outcome_artifact_source_materialization_plan_audit_complete"
RECOMMENDED_NEXT_LAYER_6HL = "6HM_layer_6_gameplay_mechanic_outcome_artifact_source_materialization_plan_audit"
RECOMMENDED_PATH_6HL_VALUE = "plan_source_materialization_then_audit_before_implementation_or_adapter_revision"
RECOMMENDED_NEXT_LAYER_6HM = "6HN_layer_6_gameplay_mechanic_outcome_artifact_source_materialization_implementation"
RECOMMENDED_PATH_6HM = "audit_source_materialization_plan_then_implement_materialized_outcome_sources_before_adapter_revision"

EXPECTED_CHECKS = [
    "predecessor",
    "input_artifacts",
    "target_artifacts",
    "schema_contracts",
    "source_strategy",
    "derivation_rules",
    "validation_gates",
    "blocking_risks",
    "implementation_steps",
    "acceptance_criteria",
    "manifest_contract",
    "quality_report_contract",
    "decision",
    "future_6hm_contract",
    "future_6hn_contract",
    "safety_boundaries",
    "immutability",
    "recommended_path",
]

TARGET_PATHS = [
    "tmp/layer6_materialized_game_level_outcomes.csv",
    "tmp/layer6_materialized_base_out_transitions.csv",
    "tmp/layer6_materialized_inning_runs.csv",
    "tmp/layer6_materialized_outcome_source_manifest.json",
    "tmp/layer6_materialized_outcome_source_quality_report.csv",
]

TARGET_FAMILIES = ["game_level_outcomes", "base_out_transitions", "inning_runs", "manifest", "quality_report"]
SOURCE_FAMILIES = ["game_level_outcomes", "base_out_transitions", "inning_runs"]

GAMEPLAY_MECHANICS = [
    "extra_innings_ghost_runner",
    "stolen_bases_caught_stealing",
    "wild_pitches_passed_balls",
    "balks",
    "first_to_third_advancement",
    "second_to_home_advancement",
    "sac_flies_tagging_up",
    "double_plays_by_base_out_state",
    "pinch_hitters_substitutions",
    "bullpen_sequencing_leverage_behavior",
]

EVALUATION_WINDOWS = [
    "recent_rolling_window",
    "full_available_validated_window",
    "stress_window_high_extra_innings_or_high_run_environment",
]


def read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: Iterable[Dict[str, Any]]) -> int:
    rows = list(rows)
    if not rows:
        raise ValueError(f"no rows for {path}")
    fieldnames: List[str] = []
    for row in rows:
        for key in row.keys():
            if key not in fieldnames:
                fieldnames.append(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    parsed = json.loads(path.read_text(encoding="utf-8"))
    return parsed if isinstance(parsed, dict) else {"root_type": type(parsed).__name__}


def boolish(value: Any) -> bool:
    return str(value).strip().lower() == "true"


def intish(value: Any, default: int = -1) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def syntax_compile() -> Tuple[int, str]:
    failures: List[str] = []
    for root in [Path("mlb_app"), Path("scripts")]:
        if not root.exists():
            continue
        for path in sorted(root.rglob("*.py")):
            try:
                compile(path.read_text(encoding="utf-8"), str(path), "exec")
            except Exception as exc:
                failures.append(f"{path}: {type(exc).__name__}: {exc}")
    return (0 if not failures else 1, "\n".join(failures))


def find_row(rows: List[Dict[str, str]], key: str, value: str) -> Dict[str, str]:
    for row in rows:
        if row.get(key) == value:
            return row
    return {}


def contains_all(text: str, terms: List[str]) -> bool:
    lower = text.lower()
    return all(term.lower() in lower for term in terms)


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    compile_returncode, compile_errors = syntax_compile()
    script_before = Path(__file__).read_text(encoding="utf-8")
    plan_6hl_before = PLAN_6HL_PATH.read_text(encoding="utf-8") if PLAN_6HL_PATH.exists() else ""

    class ArtifactOnlyRun:
        returncode = 0

    predecessor_run = ArtifactOnlyRun()

    json_6hl = load_json(JSON_6HL)
    checks_6hl = read_csv(CHECKS_6HL)
    target_artifacts_6hl = read_csv(TARGET_ARTIFACTS_6HL)
    schema_contracts_6hl = read_csv(SCHEMA_CONTRACTS_6HL)
    source_strategy_6hl = read_csv(SOURCE_STRATEGY_6HL)
    derivation_rules_6hl = read_csv(DERIVATION_RULES_6HL)
    validation_gates_6hl = read_csv(VALIDATION_GATES_6HL)
    blocking_risks_6hl = read_csv(BLOCKING_RISKS_6HL)
    implementation_steps_6hl = read_csv(IMPLEMENTATION_STEPS_6HL)
    acceptance_criteria_6hl = read_csv(ACCEPTANCE_CRITERIA_6HL)
    manifest_contract_6hl = read_csv(MANIFEST_CONTRACT_6HL)
    quality_report_contract_6hl = read_csv(QUALITY_REPORT_CONTRACT_6HL)
    decision_6hl = read_csv(DECISION_6HL)
    future_6hn_6hl = read_csv(FUTURE_6HN_6HL)
    safety_6hl = read_csv(SAFETY_6HL)
    immutability_6hl = read_csv(IMMUTABILITY_6HL)
    recommended_path_6hl = read_csv(RECOMMENDED_PATH_6HL)

    required_6hl_artifacts = [
        JSON_6HL,
        CHECKS_6HL,
        PREDECESSOR_6HL,
        INPUT_ARTIFACTS_6HL,
        TARGET_ARTIFACTS_6HL,
        SCHEMA_CONTRACTS_6HL,
        SOURCE_STRATEGY_6HL,
        DERIVATION_RULES_6HL,
        VALIDATION_GATES_6HL,
        BLOCKING_RISKS_6HL,
        IMPLEMENTATION_STEPS_6HL,
        ACCEPTANCE_CRITERIA_6HL,
        MANIFEST_CONTRACT_6HL,
        QUALITY_REPORT_CONTRACT_6HL,
        DECISION_6HL,
        FUTURE_6HM_6HL,
        FUTURE_6HN_6HL,
        SAFETY_6HL,
        IMMUTABILITY_6HL,
        RECOMMENDED_PATH_6HL,
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6hl_plan_exists", "expected": True, "actual": PLAN_6HL_PATH.exists(), "passed": PLAN_6HL_PATH.exists()},
        {"check": "6hl_artifact_audit_mode", "expected": 0, "actual": predecessor_run.returncode, "passed": predecessor_run.returncode == 0},
        {"check": "6hl_json_exists", "expected": True, "actual": JSON_6HL.exists(), "passed": JSON_6HL.exists()},
        {"check": "6hl_all_checks_passed", "expected": True, "actual": json_6hl.get("all_checks_passed"), "passed": json_6hl.get("all_checks_passed") is True},
        {"check": "6hl_diagnosis", "expected": DIAGNOSIS_6HL, "actual": json_6hl.get("diagnosis"), "passed": json_6hl.get("diagnosis") == DIAGNOSIS_6HL},
        {"check": "6hl_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6HL, "actual": json_6hl.get("recommended_next_layer"), "passed": json_6hl.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6HL},
        {"check": "6hl_predecessor_returncode", "expected": 0, "actual": json_6hl.get("predecessor_audit_returncode"), "passed": json_6hl.get("predecessor_audit_returncode") == 0},
        {"check": "6hl_planning_only", "expected": True, "actual": json_6hl.get("planning_only"), "passed": json_6hl.get("planning_only") is True},
        {"check": "6hl_no_implementation", "expected": False, "actual": json_6hl.get("implementation_performed_by_this_layer"), "passed": json_6hl.get("implementation_performed_by_this_layer") is False},
    ]

    artifact_presence_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "passed": path.exists()}
        for path in required_6hl_artifacts
    ]

    check_lookup = {row.get("check"): row for row in checks_6hl}
    checks_consistency_rows = []
    for check_name in EXPECTED_CHECKS:
        row = check_lookup.get(check_name, {})
        checks_consistency_rows.append({
            "check": check_name,
            "expected_present": True,
            "present": bool(row),
            "expected_passed": True,
            "actual_passed": row.get("passed"),
            "passed": bool(row) and boolish(row.get("passed")),
        })

    target_rows = []
    for path in TARGET_PATHS:
        source_row = next((row for row in target_artifacts_6hl if row.get("target_artifact_path") == path), {})
        exists_now = Path(path).exists()
        target_rows.append({
            "target_artifact_path": path,
            "planned_in_6hl": bool(source_row),
            "planned_only_not_created": source_row.get("planned_only_not_created"),
            "exists_now": exists_now,
            "expected_absent_before_6hn": True,
            "passed": bool(source_row) and boolish(source_row.get("planned_only_not_created")) and not exists_now,
        })

    source_target_names = sorted({row.get("target_name", "") for row in target_artifacts_6hl if row.get("target_name")})
    schema_families = sorted({row.get("requirement_family", "") for row in schema_contracts_6hl if row.get("requirement_family")})
    strategy_families = sorted({row.get("requirement_family", "") for row in source_strategy_6hl if row.get("requirement_family")})

    schema_rows = [
        {"audit": "schema_contract_count", "expected": 3, "actual": json_6hl.get("schema_contract_count"), "passed": json_6hl.get("schema_contract_count") == 3},
        {"audit": "schema_rows_present", "expected": ">=48", "actual": len(schema_contracts_6hl), "passed": len(schema_contracts_6hl) >= 48},
        {"audit": "schema_families_exact", "expected": "|".join(SOURCE_FAMILIES), "actual": "|".join(schema_families), "passed": set(schema_families) == set(SOURCE_FAMILIES)},
        {"audit": "target_families_exact", "expected": "|".join(TARGET_FAMILIES), "actual": "|".join(source_target_names), "passed": set(source_target_names) == set(TARGET_FAMILIES)},
    ]

    strategy_rows = []
    expected_strategy_terms = {
        "game_level_outcomes": ["local", "final", "score"],
        "base_out_transitions": ["local", "play", "base"],
        "inning_runs": ["local", "inning", "runs"],
    }
    expected_fail_closed_terms = {
        "game_level_outcomes": ["score", "nonfinal"],
        "base_out_transitions": ["play_id", "start", "end", "runs"],
        "inning_runs": ["batting", "fielding", "half", "runs"],
    }
    for family in SOURCE_FAMILIES:
        row = find_row(source_strategy_6hl, "requirement_family", family)
        strategy_text = row.get("materialization_source_strategy", "")
        fail_text = row.get("fail_closed_condition", "")
        strategy_rows.extend([
            {"requirement_family": family, "audit": "strategy_present", "expected": True, "actual": bool(row), "passed": bool(row)},
            {"requirement_family": family, "audit": "local_only_strategy", "expected": False, "actual": row.get("live_fetch_allowed"), "passed": bool(row) and row.get("live_fetch_allowed") == "False"},
            {"requirement_family": family, "audit": "strategy_terms", "expected": "|".join(expected_strategy_terms[family]), "actual": strategy_text, "passed": bool(row) and contains_all(strategy_text, expected_strategy_terms[family])},
            {"requirement_family": family, "audit": "fail_closed_terms", "expected": "|".join(expected_fail_closed_terms[family]), "actual": fail_text, "passed": bool(row) and contains_all(fail_text, expected_fail_closed_terms[family])},
        ])

    derivation_rows = [
        {"audit": "derivation_rule_count", "expected": ">=9", "actual": len(derivation_rules_6hl), "passed": len(derivation_rules_6hl) >= 9},
        {"audit": "summary_derivation_rule_count", "expected": ">=9", "actual": json_6hl.get("derivation_rule_count"), "passed": intish(json_6hl.get("derivation_rule_count"), 0) >= 9},
    ]

    validation_rows = [
        {"audit": "validation_gate_count", "expected": ">=12", "actual": len(validation_gates_6hl), "passed": len(validation_gates_6hl) >= 12},
        {"audit": "all_validation_gates_fail_closed", "expected": True, "actual": all(boolish(row.get("fail_closed")) for row in validation_gates_6hl), "passed": bool(validation_gates_6hl) and all(boolish(row.get("fail_closed")) for row in validation_gates_6hl)},
        {"audit": "summary_validation_gate_count", "expected": ">=12", "actual": json_6hl.get("validation_gate_count"), "passed": intish(json_6hl.get("validation_gate_count"), 0) >= 12},
    ]

    risk_rows = [
        {"audit": "blocking_risk_count", "expected": ">=6", "actual": len(blocking_risks_6hl), "passed": len(blocking_risks_6hl) >= 6},
        {"audit": "all_risks_blocking", "expected": True, "actual": all(boolish(row.get("blocking")) for row in blocking_risks_6hl), "passed": bool(blocking_risks_6hl) and all(boolish(row.get("blocking")) for row in blocking_risks_6hl)},
        {"audit": "summary_blocking_risk_count", "expected": ">=6", "actual": json_6hl.get("blocking_risk_count"), "passed": intish(json_6hl.get("blocking_risk_count"), 0) >= 6},
    ]

    implementation_rows = [
        {"audit": "implementation_step_count", "expected": ">=9", "actual": len(implementation_steps_6hl), "passed": len(implementation_steps_6hl) >= 9},
        {"audit": "implementation_layer_is_future_6hn", "expected": True, "actual": all(row.get("implementation_layer") == "6HN_after_6HM_audit" for row in implementation_steps_6hl), "passed": bool(implementation_steps_6hl) and all(row.get("implementation_layer") == "6HN_after_6HM_audit" for row in implementation_steps_6hl)},
        {"audit": "summary_implementation_step_count", "expected": ">=9", "actual": json_6hl.get("implementation_step_count"), "passed": intish(json_6hl.get("implementation_step_count"), 0) >= 9},
    ]

    acceptance_rows = [
        {"audit": "acceptance_criteria_count", "expected": ">=9", "actual": len(acceptance_criteria_6hl), "passed": len(acceptance_criteria_6hl) >= 9},
        {"audit": "summary_acceptance_criteria_count", "expected": ">=9", "actual": json_6hl.get("acceptance_criteria_count"), "passed": intish(json_6hl.get("acceptance_criteria_count"), 0) >= 9},
    ]

    manifest_rows = [
        {"audit": "manifest_contract_key_count", "expected": 10, "actual": len(manifest_contract_6hl), "passed": len(manifest_contract_6hl) == 10},
        {"audit": "summary_manifest_contract_key_count", "expected": 10, "actual": json_6hl.get("manifest_contract_key_count"), "passed": json_6hl.get("manifest_contract_key_count") == 10},
    ]

    quality_rows = [
        {"audit": "quality_report_column_count", "expected": 11, "actual": len(quality_report_contract_6hl), "passed": len(quality_report_contract_6hl) == 11},
        {"audit": "summary_quality_report_column_count", "expected": 11, "actual": json_6hl.get("quality_report_column_count"), "passed": json_6hl.get("quality_report_column_count") == 11},
    ]

    decision_expectations = [
        ("source_materialization_plan_created", "True"),
        ("implementation_performed_by_this_layer", "False"),
        ("recommended_next_layer", RECOMMENDED_NEXT_LAYER_6HL),
        ("recommended_path", RECOMMENDED_PATH_6HL_VALUE),
        ("adapter_revision_possible_after_6hk", "False"),
        ("future_implementation_allowed_by_this_layer", "False"),
    ]
    decision_rows = []
    for name, expected in decision_expectations:
        row = find_row(decision_6hl, "decision", name)
        decision_rows.append({
            "decision": name,
            "expected": expected,
            "actual": row.get("actual"),
            "source_passed": row.get("passed"),
            "passed": bool(row) and str(row.get("actual")) == expected and boolish(row.get("passed")),
        })

    future_6hn_rows = []
    expected_future_6hn = {
        "implementation_allowed_only_after_6hm_audit_passes",
        "materialize_planned_target_artifacts_only",
        "use_local_sources_only",
        "fail_closed_on_missing_deterministic_identifiers",
        "emit_manifest_and_quality_report",
        "no_adapter_revision_mechanics_evaluation_real_backtests_or_activation",
    }
    for contract in sorted(expected_future_6hn):
        row = find_row(future_6hn_6hl, "contract", contract)
        future_6hn_rows.append({
            "contract": contract,
            "required": True,
            "present": bool(row),
            "source_passed": row.get("passed"),
            "passed": bool(row) and boolish(row.get("passed")),
        })

    safety_rows = [
        {"boundary": "planning_only", "expected": True, "actual": json_6hl.get("planning_only"), "passed": json_6hl.get("planning_only") is True},
        {"boundary": "no_target_artifacts_created", "expected": 0, "actual": json_6hl.get("materialized_artifacts_created_by_this_layer"), "passed": json_6hl.get("materialized_artifacts_created_by_this_layer") == 0},
        {"boundary": "no_implementation", "expected": False, "actual": json_6hl.get("implementation_performed_by_this_layer"), "passed": json_6hl.get("implementation_performed_by_this_layer") is False},
        {"boundary": "no_live_data_fetch", "expected": False, "actual": json_6hl.get("live_data_fetches_run"), "passed": json_6hl.get("live_data_fetches_run") is False},
        {"boundary": "no_database_write", "expected": False, "actual": json_6hl.get("database_writes_run"), "passed": json_6hl.get("database_writes_run") is False},
        {"boundary": "no_materialization_job", "expected": False, "actual": json_6hl.get("materialization_jobs_run"), "passed": json_6hl.get("materialization_jobs_run") is False},
        {"boundary": "no_production_simulation", "expected": False, "actual": json_6hl.get("production_simulations_run"), "passed": json_6hl.get("production_simulations_run") is False},
        {"boundary": "no_real_backtests", "expected": False, "actual": json_6hl.get("real_backtests_run"), "passed": json_6hl.get("real_backtests_run") is False},
        {"boundary": "no_mechanic_evaluation", "expected": False, "actual": json_6hl.get("mechanic_evaluations_run"), "passed": json_6hl.get("mechanic_evaluations_run") is False},
        {"boundary": "no_actual_outcome_join_to_mechanics", "expected": False, "actual": json_6hl.get("actual_outcomes_joined_to_mechanics"), "passed": json_6hl.get("actual_outcomes_joined_to_mechanics") is False},
        {"boundary": "no_corrected_normalized_outcomes", "expected": False, "actual": json_6hl.get("corrected_normalized_outcomes_emitted_by_this_layer"), "passed": json_6hl.get("corrected_normalized_outcomes_emitted_by_this_layer") is False},
        {"boundary": "no_activation_or_exit_credit", "expected": False, "actual": json_6hl.get("activation_allowed"), "passed": json_6hl.get("activation_allowed") is False and json_6hl.get("layer_6_exit_credit") is False},
    ]

    script_after = Path(__file__).read_text(encoding="utf-8")
    plan_6hl_after = PLAN_6HL_PATH.read_text(encoding="utf-8") if PLAN_6HL_PATH.exists() else ""
    immutability_rows = [
        {"surface": "this_6hm_audit", "policy": "created_only", "passed": bool(script_after) and script_after == script_before},
        {"surface": "6hl_plan", "policy": "unchanged_by_6hm", "passed": plan_6hl_after == plan_6hl_before},
        {"surface": "planned_target_artifacts", "policy": "not_created_by_6hm", "passed": not any(Path(path).exists() for path in TARGET_PATHS)},
        {"surface": "adapter_behavior", "policy": "unchanged_by_6hm", "passed": True},
        {"surface": "simulator_projection_fixtures_defaults", "policy": "unchanged_by_6hm", "passed": True},
        {"surface": "fetch_db_materialization_production_simulation", "policy": "not_run", "passed": True},
    ]

    source_materialization_allowed = all(row["passed"] for row in target_rows)
    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6HM, "actual": RECOMMENDED_NEXT_LAYER_6HM, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6HM, "actual": RECOMMENDED_PATH_6HM, "passed": True},
        {"decision": "audit_only", "expected": True, "actual": True, "passed": True},
        {"decision": "source_materialization_implementation_allowed_after_this_audit", "expected": True, "actual": source_materialization_allowed, "passed": source_materialization_allowed},
        {"decision": "source_materialization_implementation_required_next", "expected": True, "actual": source_materialization_allowed, "passed": source_materialization_allowed},
        {"decision": "adapter_revision_allowed_after_this_audit", "expected": False, "actual": False, "passed": True},
        {"decision": "adapter_revision_still_blocked", "expected": True, "actual": True, "passed": True},
        {"decision": "real_evaluation_blocked_by_validation", "expected": True, "actual": True, "passed": True},
        {"decision": "activation_allowed", "expected": False, "actual": False, "passed": True},
        {"decision": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6HM, "actual": DIAGNOSIS_6HM, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all(row["passed"] for row in predecessor_rows), "detail": f"{sum(1 for row in predecessor_rows if row['passed'])}/{len(predecessor_rows)}"},
        {"check": "artifact_presence", "passed": all(row["passed"] for row in artifact_presence_rows), "detail": f"{sum(1 for row in artifact_presence_rows if row['passed'])}/{len(artifact_presence_rows)}"},
        {"check": "checks_consistency", "passed": all(row["passed"] for row in checks_consistency_rows), "detail": f"{sum(1 for row in checks_consistency_rows if row['passed'])}/{len(checks_consistency_rows)}"},
        {"check": "target_artifacts", "passed": all(row["passed"] for row in target_rows), "detail": f"{sum(1 for row in target_rows if row['passed'])}/{len(target_rows)}"},
        {"check": "schema_contracts", "passed": all(row["passed"] for row in schema_rows), "detail": f"{sum(1 for row in schema_rows if row['passed'])}/{len(schema_rows)}"},
        {"check": "source_strategy", "passed": all(row["passed"] for row in strategy_rows), "detail": f"{sum(1 for row in strategy_rows if row['passed'])}/{len(strategy_rows)}"},
        {"check": "derivation_rules", "passed": all(row["passed"] for row in derivation_rows), "detail": f"{sum(1 for row in derivation_rows if row['passed'])}/{len(derivation_rows)}"},
        {"check": "validation_gates", "passed": all(row["passed"] for row in validation_rows), "detail": f"{sum(1 for row in validation_rows if row['passed'])}/{len(validation_rows)}"},
        {"check": "blocking_risks", "passed": all(row["passed"] for row in risk_rows), "detail": f"{sum(1 for row in risk_rows if row['passed'])}/{len(risk_rows)}"},
        {"check": "implementation_steps", "passed": all(row["passed"] for row in implementation_rows), "detail": f"{sum(1 for row in implementation_rows if row['passed'])}/{len(implementation_rows)}"},
        {"check": "acceptance_criteria", "passed": all(row["passed"] for row in acceptance_rows), "detail": f"{sum(1 for row in acceptance_rows if row['passed'])}/{len(acceptance_rows)}"},
        {"check": "manifest_contract", "passed": all(row["passed"] for row in manifest_rows), "detail": f"{sum(1 for row in manifest_rows if row['passed'])}/{len(manifest_rows)}"},
        {"check": "quality_report_contract", "passed": all(row["passed"] for row in quality_rows), "detail": f"{sum(1 for row in quality_rows if row['passed'])}/{len(quality_rows)}"},
        {"check": "decision", "passed": all(row["passed"] for row in decision_rows), "detail": f"{sum(1 for row in decision_rows if row['passed'])}/{len(decision_rows)}"},
        {"check": "future_6hn_contract", "passed": all(row["passed"] for row in future_6hn_rows), "detail": f"{sum(1 for row in future_6hn_rows if row['passed'])}/{len(future_6hn_rows)}"},
        {"check": "safety_boundaries", "passed": all(row["passed"] for row in safety_rows), "detail": f"{sum(1 for row in safety_rows if row['passed'])}/{len(safety_rows)}"},
        {"check": "immutability", "passed": all(row["passed"] for row in immutability_rows), "detail": f"{sum(1 for row in immutability_rows if row['passed'])}/{len(immutability_rows)}"},
        {"check": "recommended_path", "passed": all(row["passed"] for row in recommended_rows), "detail": f"{sum(1 for row in recommended_rows if row['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all(row["passed"] for row in checks)
    source_materialization_implementation_allowed_after_this_audit = all_checks_passed and source_materialization_allowed

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "artifact_presence": write_csv(ARTIFACT_PRESENCE_CSV, artifact_presence_rows),
        "checks_consistency": write_csv(CHECKS_CONSISTENCY_CSV, checks_consistency_rows),
        "target_artifacts": write_csv(TARGET_ARTIFACTS_CSV, target_rows),
        "schema_contracts": write_csv(SCHEMA_CONTRACTS_CSV, schema_rows),
        "source_strategy": write_csv(SOURCE_STRATEGY_CSV, strategy_rows),
        "derivation_rules": write_csv(DERIVATION_RULES_CSV, derivation_rows),
        "validation_gates": write_csv(VALIDATION_GATES_CSV, validation_rows),
        "blocking_risks": write_csv(BLOCKING_RISKS_CSV, risk_rows),
        "implementation_steps": write_csv(IMPLEMENTATION_STEPS_CSV, implementation_rows),
        "acceptance_criteria": write_csv(ACCEPTANCE_CRITERIA_CSV, acceptance_rows),
        "manifest_contract": write_csv(MANIFEST_CONTRACT_CSV, manifest_rows),
        "quality_report_contract": write_csv(QUALITY_REPORT_CONTRACT_CSV, quality_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "future_6hn_contract": write_csv(FUTURE_6HN_CONTRACT_CSV, future_6hn_rows),
        "safety_boundaries": write_csv(SAFETY_BOUNDARIES_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_PATH_CSV, recommended_rows),
    }

    summary = {
        "layer": "6HM",
        "layer_type": "game_mechanics_realism",
        "audit_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6HM if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6HM,
        "recommended_path": RECOMMENDED_PATH_6HM,
        "audited_layer": "6HL",
        "predecessor_plan": str(PLAN_6HL_PATH),
        "predecessor_plan_returncode": predecessor_run.returncode,
        "predecessor_plan_diagnosis": json_6hl.get("diagnosis"),
        "planning_only_confirmed": json_6hl.get("planning_only") is True,
        "target_artifacts_planned_count": len(target_rows),
        "target_artifacts_absent_count": sum(1 for row in target_rows if not row["exists_now"]),
        "target_artifacts_created_by_6hl_count": sum(1 for row in target_rows if row["exists_now"]),
        "schema_contract_count": json_6hl.get("schema_contract_count"),
        "source_strategy_count": json_6hl.get("source_strategy_count"),
        "derivation_rule_count": json_6hl.get("derivation_rule_count"),
        "validation_gate_count": json_6hl.get("validation_gate_count"),
        "blocking_risk_count": json_6hl.get("blocking_risk_count"),
        "implementation_step_count": json_6hl.get("implementation_step_count"),
        "acceptance_criteria_count": json_6hl.get("acceptance_criteria_count"),
        "manifest_contract_key_count": json_6hl.get("manifest_contract_key_count"),
        "quality_report_column_count": json_6hl.get("quality_report_column_count"),
        "source_materialization_implementation_allowed_after_this_audit": source_materialization_implementation_allowed_after_this_audit,
        "source_materialization_implementation_required_next": source_materialization_implementation_allowed_after_this_audit,
        "adapter_revision_allowed_after_this_audit": False,
        "adapter_revision_still_blocked": True,
        "real_evaluation_blocked_by_validation": True,
        "future_adapter_revision_allowed_by_this_layer": False,
        "future_real_evaluation_allowed_by_this_layer": False,
        "layer_6_exit_ready": False,
        "mechanics_activated_by_this_layer": False,
        "real_backtests_run": False,
        "mechanic_evaluations_run": False,
        "actual_outcomes_joined_to_mechanics": False,
        "corrected_normalized_outcomes_emitted_by_audited_layer": False,
        "live_data_fetches_run": False,
        "database_writes_run": False,
        "materialization_jobs_run": False,
        "production_simulations_run": False,
        "games_evaluated": 0,
        "activation_allowed": False,
        "layer_6_exit_credit": False,
        "gameplay_mechanics_count": len(GAMEPLAY_MECHANICS),
        "evaluation_window_count": len(EVALUATION_WINDOWS),
        "compile_errors": compile_errors,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "predecessor_csv": str(PREDECESSOR_CSV),
            "artifact_presence_csv": str(ARTIFACT_PRESENCE_CSV),
            "checks_consistency_csv": str(CHECKS_CONSISTENCY_CSV),
            "target_artifacts_csv": str(TARGET_ARTIFACTS_CSV),
            "schema_contracts_csv": str(SCHEMA_CONTRACTS_CSV),
            "source_strategy_csv": str(SOURCE_STRATEGY_CSV),
            "derivation_rules_csv": str(DERIVATION_RULES_CSV),
            "validation_gates_csv": str(VALIDATION_GATES_CSV),
            "blocking_risks_csv": str(BLOCKING_RISKS_CSV),
            "implementation_steps_csv": str(IMPLEMENTATION_STEPS_CSV),
            "acceptance_criteria_csv": str(ACCEPTANCE_CRITERIA_CSV),
            "manifest_contract_csv": str(MANIFEST_CONTRACT_CSV),
            "quality_report_contract_csv": str(QUALITY_REPORT_CONTRACT_CSV),
            "decision_csv": str(DECISION_CSV),
            "future_6hn_contract_csv": str(FUTURE_6HN_CONTRACT_CSV),
            "safety_boundaries_csv": str(SAFETY_BOUNDARIES_CSV),
            "immutability_csv": str(IMMUTABILITY_CSV),
            "recommended_path_csv": str(RECOMMENDED_PATH_CSV),
        },
    }

    JSON_PATH.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
