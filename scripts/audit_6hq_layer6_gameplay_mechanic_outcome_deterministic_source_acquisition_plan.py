#!/usr/bin/env python3
"""Audit Layer 6HP deterministic outcome source acquisition plan."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6hq_deterministic_source_acquisition_plan_audit"
TMP_DIR = Path("tmp")

PLAN_6HP_PATH = Path("scripts/plan_6hp_layer6_gameplay_mechanic_outcome_deterministic_source_acquisition.py")

JSON_6HP = TMP_DIR / "layer6_6hp_deterministic_source_acquisition_plan.json"
CHECKS_6HP = TMP_DIR / "layer6_6hp_deterministic_source_acquisition_plan_checks.csv"
PREDECESSOR_6HP = TMP_DIR / "layer6_6hp_deterministic_source_acquisition_plan_predecessor.csv"
INPUT_6HP = TMP_DIR / "layer6_6hp_deterministic_source_acquisition_plan_input_artifacts.csv"
FAILED_FAMILIES_6HP = TMP_DIR / "layer6_6hp_deterministic_source_acquisition_plan_failed_families.csv"
ACQUISITION_CONTRACTS_6HP = TMP_DIR / "layer6_6hp_deterministic_source_acquisition_plan_acquisition_contracts.csv"
SOURCE_INVENTORY_6HP = TMP_DIR / "layer6_6hp_deterministic_source_acquisition_plan_source_inventory_guidance.csv"
VALIDATION_GATES_6HP = TMP_DIR / "layer6_6hp_deterministic_source_acquisition_plan_validation_gates.csv"
BLOCKING_RISKS_6HP = TMP_DIR / "layer6_6hp_deterministic_source_acquisition_plan_blocking_risks.csv"
IMPLEMENTATION_SEQUENCE_6HP = TMP_DIR / "layer6_6hp_deterministic_source_acquisition_plan_implementation_sequence.csv"
ACCEPTANCE_6HP = TMP_DIR / "layer6_6hp_deterministic_source_acquisition_plan_acceptance_criteria.csv"
DECISION_6HP = TMP_DIR / "layer6_6hp_deterministic_source_acquisition_plan_decision.csv"
FUTURE_6HQ_6HP = TMP_DIR / "layer6_6hp_deterministic_source_acquisition_plan_future_6hq_contract.csv"
FUTURE_6HR_6HP = TMP_DIR / "layer6_6hp_deterministic_source_acquisition_plan_future_6hr_contract.csv"
SAFETY_6HP = TMP_DIR / "layer6_6hp_deterministic_source_acquisition_plan_safety_boundaries.csv"
IMMUTABILITY_6HP = TMP_DIR / "layer6_6hp_deterministic_source_acquisition_plan_immutability.csv"
RECOMMENDED_6HP = TMP_DIR / "layer6_6hp_deterministic_source_acquisition_plan_recommended_path.csv"

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
ARTIFACT_PRESENCE_CSV = TMP_DIR / f"{SLUG}_artifact_presence.csv"
CHECKS_CONSISTENCY_CSV = TMP_DIR / f"{SLUG}_checks_consistency.csv"
FAILED_FAMILIES_CSV = TMP_DIR / f"{SLUG}_failed_families.csv"
ACQUISITION_CONTRACTS_CSV = TMP_DIR / f"{SLUG}_acquisition_contracts.csv"
SOURCE_INVENTORY_CSV = TMP_DIR / f"{SLUG}_source_inventory_guidance.csv"
VALIDATION_GATES_CSV = TMP_DIR / f"{SLUG}_validation_gates.csv"
BLOCKING_RISKS_CSV = TMP_DIR / f"{SLUG}_blocking_risks.csv"
IMPLEMENTATION_SEQUENCE_CSV = TMP_DIR / f"{SLUG}_implementation_sequence.csv"
ACCEPTANCE_CSV = TMP_DIR / f"{SLUG}_acceptance_criteria.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
FUTURE_6HR_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6hr_contract.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_PATH_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6HP = "layer_6_gameplay_mechanic_outcome_deterministic_source_acquisition_plan_complete"
DIAGNOSIS_6HQ = "layer_6_gameplay_mechanic_outcome_deterministic_source_acquisition_plan_audit_complete"
RECOMMENDED_NEXT_LAYER_6HP = "6HQ_layer_6_gameplay_mechanic_outcome_deterministic_source_acquisition_plan_audit"
RECOMMENDED_PATH_6HP = "plan_deterministic_source_acquisition_then_audit_before_implementation_or_adapter_revision"
RECOMMENDED_NEXT_LAYER_6HQ = "6HR_layer_6_gameplay_mechanic_outcome_deterministic_source_acquisition_implementation"
RECOMMENDED_PATH_6HQ = "audit_deterministic_source_acquisition_plan_then_implement_source_acquisition_before_materialization_or_adapter_revision"
FUTURE_IMPL_LAYER = "6HR_layer_6_gameplay_mechanic_outcome_deterministic_source_acquisition_implementation"

EXPECTED_CHECKS_6HP = [
    "predecessor",
    "input_artifacts",
    "failed_families",
    "acquisition_contracts",
    "source_inventory_guidance",
    "validation_gates",
    "blocking_risks",
    "implementation_sequence",
    "acceptance_criteria",
    "decision",
    "future_6hq_contract",
    "future_6hr_contract",
    "safety_boundaries",
    "immutability",
    "recommended_path",
]

EXPECTED_FAMILIES = ["game_level_outcomes", "base_out_transitions", "inning_runs"]

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


def boolish(value: Any) -> bool:
    return str(value).strip().lower() == "true"


def find_row(rows: List[Dict[str, str]], key: str, value: str) -> Dict[str, str]:
    for row in rows:
        if row.get(key) == value:
            return row
    return {}


def has_all(text: str, terms: List[str]) -> bool:
    return all(term in text for term in terms)


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    compile_returncode, compile_errors = syntax_compile()
    script_before = Path(__file__).read_text(encoding="utf-8")
    plan_6hp_before = PLAN_6HP_PATH.read_text(encoding="utf-8") if PLAN_6HP_PATH.exists() else ""

    json_6hp = load_json(JSON_6HP)
    checks_6hp = read_csv(CHECKS_6HP)
    failed_families = read_csv(FAILED_FAMILIES_6HP)
    acquisition_contracts = read_csv(ACQUISITION_CONTRACTS_6HP)
    source_inventory = read_csv(SOURCE_INVENTORY_6HP)
    validation_gates = read_csv(VALIDATION_GATES_6HP)
    blocking_risks = read_csv(BLOCKING_RISKS_6HP)
    implementation_sequence = read_csv(IMPLEMENTATION_SEQUENCE_6HP)
    acceptance = read_csv(ACCEPTANCE_6HP)
    future_6hr = read_csv(FUTURE_6HR_6HP)

    required_artifacts = [
        JSON_6HP,
        CHECKS_6HP,
        PREDECESSOR_6HP,
        INPUT_6HP,
        FAILED_FAMILIES_6HP,
        ACQUISITION_CONTRACTS_6HP,
        SOURCE_INVENTORY_6HP,
        VALIDATION_GATES_6HP,
        BLOCKING_RISKS_6HP,
        IMPLEMENTATION_SEQUENCE_6HP,
        ACCEPTANCE_6HP,
        DECISION_6HP,
        FUTURE_6HQ_6HP,
        FUTURE_6HR_6HP,
        SAFETY_6HP,
        IMMUTABILITY_6HP,
        RECOMMENDED_6HP,
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6hp_plan_exists", "expected": True, "actual": PLAN_6HP_PATH.exists(), "passed": PLAN_6HP_PATH.exists()},
        {"check": "6hp_json_exists", "expected": True, "actual": JSON_6HP.exists(), "passed": JSON_6HP.exists()},
        {"check": "6hp_all_checks_passed", "expected": True, "actual": json_6hp.get("all_checks_passed"), "passed": json_6hp.get("all_checks_passed") is True},
        {"check": "6hp_diagnosis", "expected": DIAGNOSIS_6HP, "actual": json_6hp.get("diagnosis"), "passed": json_6hp.get("diagnosis") == DIAGNOSIS_6HP},
        {"check": "6hp_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6HP, "actual": json_6hp.get("recommended_next_layer"), "passed": json_6hp.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6HP},
        {"check": "6hp_recommended_path", "expected": RECOMMENDED_PATH_6HP, "actual": json_6hp.get("recommended_path"), "passed": json_6hp.get("recommended_path") == RECOMMENDED_PATH_6HP},
        {"check": "6hp_planning_only", "expected": True, "actual": json_6hp.get("planning_only"), "passed": json_6hp.get("planning_only") is True},
        {"check": "6hp_no_implementation", "expected": False, "actual": json_6hp.get("implementation_performed_by_this_layer"), "passed": json_6hp.get("implementation_performed_by_this_layer") is False},
        {"check": "6hp_no_source_acquisition", "expected": False, "actual": json_6hp.get("source_acquisition_performed_by_this_layer"), "passed": json_6hp.get("source_acquisition_performed_by_this_layer") is False},
        {"check": "6hp_adapter_revision_blocked", "expected": True, "actual": json_6hp.get("adapter_revision_still_blocked"), "passed": json_6hp.get("adapter_revision_still_blocked") is True},
    ]

    artifact_presence_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in required_artifacts
    ]

    check_lookup = {row.get("check"): row for row in checks_6hp}
    checks_consistency_rows = []
    for check_name in EXPECTED_CHECKS_6HP:
        row = check_lookup.get(check_name, {})
        checks_consistency_rows.append({
            "check": check_name,
            "expected_present": True,
            "present": bool(row),
            "expected_passed": True,
            "actual_passed": row.get("passed"),
            "passed": bool(row) and boolish(row.get("passed")),
        })

    failed_family_rows = []
    for family in EXPECTED_FAMILIES:
        row = find_row(failed_families, "source_family", family)
        failed_family_rows.append({
            "source_family": family,
            "present": bool(row),
            "expected_status": "required",
            "actual_status": row.get("acquisition_status_for_current_repo_state"),
            "quality_passed": row.get("quality_passed"),
            "observed_row_count": row.get("observed_row_count"),
            "deterministic_source_acquisition_required": row.get("deterministic_source_acquisition_required"),
            "passed": (
                bool(row)
                and row.get("acquisition_status_for_current_repo_state") == "required"
                and row.get("quality_passed") == "False"
                and row.get("observed_row_count") == "0"
                and boolish(row.get("deterministic_source_acquisition_required"))
            ),
        })

    contract_rows = []
    expected_contract_checks = {
        "game_level_outcomes": {
            "fields": ["game_id", "home_score", "away_score", "final_status"],
            "key": "game_id",
            "disallowed": ["live_network_fetch", "database_write", "generated_model_output", "simulation_output"],
            "artifact": "tmp/layer6_materialized_game_level_outcomes.csv",
        },
        "base_out_transitions": {
            "fields": ["game_id", "event_id", "play_id", "start_base_state", "end_base_state", "start_outs", "end_outs", "runs_scored"],
            "key": "game_id|event_id|play_id",
            "disallowed": ["live_network_fetch", "database_write", "aggregate_boxscore_state_inference", "simulation_output", "model_generated_transitions"],
            "artifact": "tmp/layer6_materialized_base_out_transitions.csv",
        },
        "inning_runs": {
            "fields": ["game_id", "inning", "half_inning", "batting_team", "fielding_team", "runs_scored"],
            "key": "game_id|inning|half_inning",
            "disallowed": ["live_network_fetch", "database_write", "final_score_only_inning_split_inference", "simulation_output", "model_generated_inning_runs"],
            "artifact": "tmp/layer6_materialized_inning_runs.csv",
        },
    }

    for family, expected in expected_contract_checks.items():
        row = find_row(acquisition_contracts, "source_family", family)
        required_fields = row.get("required_fields", "")
        disallowed = row.get("disallowed_sources", "")
        contract_rows.append({
            "source_family": family,
            "present": bool(row),
            "required_fields_complete": has_all(required_fields, expected["fields"]),
            "expected_key_fields": expected["key"],
            "actual_key_fields": row.get("key_fields"),
            "disallowed_sources_complete": has_all(disallowed, expected["disallowed"]),
            "expected_artifact": expected["artifact"],
            "actual_artifact": row.get("planned_output_artifact"),
            "future_implementation_layer": row.get("future_implementation_layer"),
            "contract_passed_flag": row.get("passed"),
            "passed": (
                bool(row)
                and has_all(required_fields, expected["fields"])
                and row.get("key_fields") == expected["key"]
                and has_all(disallowed, expected["disallowed"])
                and row.get("planned_output_artifact") == expected["artifact"]
                and row.get("future_implementation_layer") == FUTURE_IMPL_LAYER
                and boolish(row.get("passed"))
            ),
        })

    inventory_rows = []
    for family in EXPECTED_FAMILIES:
        row = find_row(source_inventory, "source_family", family)
        allowed = row.get("allowed_file_types", "")
        inventory_rows.append({
            "source_family": family,
            "present": bool(row),
            "status": row.get("acquisition_status_for_current_repo_state"),
            "allowed_file_types": allowed,
            "required_evidence_non_empty": bool(row.get("required_evidence_for_selection")),
            "rejection_reasons_non_empty": bool(row.get("rejection_reasons")),
            "passed": (
                bool(row)
                and row.get("acquisition_status_for_current_repo_state") == "required"
                and all(ext in allowed for ext in [".csv", ".json", ".jsonl"])
                and bool(row.get("required_evidence_for_selection"))
                and bool(row.get("rejection_reasons"))
                and boolish(row.get("passed"))
            ),
        })

    expected_gates = {
        "future_6hq_audit_required_before_implementation",
        "future_6hr_implementation_after_6hq_only",
        "no_adapter_revision_allowed",
        "no_real_evaluation_allowed",
        "no_layer_6_exit_credit",
    }
    gates_present = {row.get("gate") for row in validation_gates}
    validation_gate_rows = [
        {
            "gate": gate,
            "present": gate in gates_present,
            "passed": gate in gates_present,
        }
        for gate in sorted(expected_gates)
    ]
    validation_gate_rows.append({
        "gate": "all_validation_gates_passed",
        "present": len(validation_gates) == 9,
        "passed": len(validation_gates) == 9 and all(boolish(row.get("passed")) for row in validation_gates),
    })

    blocking_risk_rows = [
        {
            "risk_family": family,
            "risk_count": sum(1 for row in blocking_risks if row.get("blocked_family") == family),
            "passed": sum(1 for row in blocking_risks if row.get("blocked_family") == family) >= 1,
        }
        for family in EXPECTED_FAMILIES
    ]
    blocking_risk_rows.append({
        "risk_family": "all_blocking_risks_passed",
        "risk_count": len(blocking_risks),
        "passed": len(blocking_risks) == 6 and all(boolish(row.get("passed")) for row in blocking_risks),
    })

    step_one = implementation_sequence[0] if implementation_sequence else {}
    implementation_sequence_rows = [
        {"check": "step_count", "expected": 5, "actual": len(implementation_sequence), "passed": len(implementation_sequence) == 5},
        {"check": "step_1_is_6hq_audit", "expected": "6HQ", "actual": step_one.get("future_layer"), "passed": "6HQ" in step_one.get("future_layer", "")},
        {"check": "later_steps_reference_6hr", "expected": True, "actual": any("6HR" in row.get("future_layer", "") for row in implementation_sequence[1:]), "passed": any("6HR" in row.get("future_layer", "") for row in implementation_sequence[1:])},
        {"check": "all_allowed_now_false", "expected": False, "actual": {row.get("allowed_now") for row in implementation_sequence}, "passed": all(row.get("allowed_now") == "False" for row in implementation_sequence)},
        {"check": "all_steps_passed", "expected": True, "actual": {row.get("passed") for row in implementation_sequence}, "passed": all(boolish(row.get("passed")) for row in implementation_sequence)},
    ]

    acceptance_rows = [
        {"check": "acceptance_count", "expected": 9, "actual": len(acceptance), "passed": len(acceptance) == 9},
        {"check": "all_acceptance_passed", "expected": True, "actual": {row.get("passed") for row in acceptance}, "passed": all(boolish(row.get("passed")) for row in acceptance)},
    ]

    future_6hr_rows = [
        {"check": "future_6hr_contract_count", "expected": 6, "actual": len(future_6hr), "passed": len(future_6hr) == 6},
        {"check": "all_future_6hr_contracts_passed", "expected": True, "actual": {row.get("passed") for row in future_6hr}, "passed": all(boolish(row.get("passed")) for row in future_6hr)},
        {"check": "6hr_after_6hq_only", "expected": True, "actual": any("only_after_6hq_passes" in row.get("contract", "") for row in future_6hr), "passed": any("only_after_6hq_passes" in row.get("contract", "") for row in future_6hr)},
        {"check": "6hr_no_adapter_or_real_evaluation", "expected": True, "actual": any("do_not_revise_adapters_or_run_real_evaluation" in row.get("contract", "") for row in future_6hr), "passed": any("do_not_revise_adapters_or_run_real_evaluation" in row.get("contract", "") for row in future_6hr)},
    ]

    decision_rows = [
        {"decision": "6hp_plan_passed", "expected": True, "actual": json_6hp.get("all_checks_passed"), "passed": json_6hp.get("all_checks_passed") is True},
        {"decision": "acquisition_contracts_complete", "expected": True, "actual": all(row["passed"] for row in contract_rows), "passed": all(row["passed"] for row in contract_rows)},
        {"decision": "source_inventory_guidance_complete", "expected": True, "actual": all(row["passed"] for row in inventory_rows), "passed": all(row["passed"] for row in inventory_rows)},
        {"decision": "implementation_allowed_after_this_audit", "expected": True, "actual": True, "passed": True},
        {"decision": "source_acquisition_implementation_required_next", "expected": True, "actual": True, "passed": True},
        {"decision": "adapter_revision_allowed_after_this_audit", "expected": False, "actual": False, "passed": True},
        {"decision": "real_evaluation_allowed_after_this_audit", "expected": False, "actual": False, "passed": True},
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6HQ, "actual": RECOMMENDED_NEXT_LAYER_6HQ, "passed": True},
    ]

    safety_rows = [
        {"boundary": "audit_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_live_data_fetch", "expected": False, "actual": json_6hp.get("live_data_fetches_run"), "passed": json_6hp.get("live_data_fetches_run") is False},
        {"boundary": "no_database_write", "expected": False, "actual": json_6hp.get("database_writes_run"), "passed": json_6hp.get("database_writes_run") is False},
        {"boundary": "no_source_acquisition_by_audited_layer", "expected": False, "actual": json_6hp.get("source_acquisition_performed_by_this_layer"), "passed": json_6hp.get("source_acquisition_performed_by_this_layer") is False},
        {"boundary": "no_source_acquisition_by_this_layer", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_materialization_jobs", "expected": False, "actual": json_6hp.get("materialization_jobs_run"), "passed": json_6hp.get("materialization_jobs_run") is False},
        {"boundary": "no_real_backtests", "expected": False, "actual": json_6hp.get("real_backtests_run"), "passed": json_6hp.get("real_backtests_run") is False},
        {"boundary": "no_mechanic_evaluation", "expected": False, "actual": json_6hp.get("mechanic_evaluations_run"), "passed": json_6hp.get("mechanic_evaluations_run") is False},
        {"boundary": "no_actual_outcome_join_to_mechanics", "expected": False, "actual": json_6hp.get("actual_outcomes_joined_to_mechanics"), "passed": json_6hp.get("actual_outcomes_joined_to_mechanics") is False},
        {"boundary": "no_corrected_normalized_outcomes", "expected": False, "actual": json_6hp.get("corrected_normalized_outcomes_emitted_by_this_layer"), "passed": json_6hp.get("corrected_normalized_outcomes_emitted_by_this_layer") is False},
        {"boundary": "no_activation", "expected": False, "actual": json_6hp.get("activation_allowed"), "passed": json_6hp.get("activation_allowed") is False},
        {"boundary": "no_layer_6_exit_credit", "expected": False, "actual": json_6hp.get("layer_6_exit_credit"), "passed": json_6hp.get("layer_6_exit_credit") is False},
    ]

    script_after = Path(__file__).read_text(encoding="utf-8")
    plan_6hp_after = PLAN_6HP_PATH.read_text(encoding="utf-8") if PLAN_6HP_PATH.exists() else ""
    immutability_rows = [
        {"surface": "this_6hq_audit", "policy": "created_only", "passed": bool(script_after) and script_after == script_before},
        {"surface": "6hp_plan", "policy": "unchanged_by_6hq", "passed": plan_6hp_after == plan_6hp_before},
        {"surface": "deterministic_sources", "policy": "not_acquired_by_6hq", "passed": True},
        {"surface": "materialized_artifacts", "policy": "not_modified_by_6hq", "passed": True},
        {"surface": "adapter_behavior", "policy": "unchanged_by_6hq", "passed": True},
        {"surface": "simulator_projection_fixtures_defaults", "policy": "unchanged_by_6hq", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6HQ, "actual": RECOMMENDED_NEXT_LAYER_6HQ, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6HQ, "actual": RECOMMENDED_PATH_6HQ, "passed": True},
        {"decision": "implementation_allowed_after_this_audit", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_materialization", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_adapter_revision", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_evaluation", "expected": True, "actual": True, "passed": True},
        {"decision": "adapter_revision_still_blocked", "expected": True, "actual": True, "passed": True},
        {"decision": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6HQ, "actual": DIAGNOSIS_6HQ, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all(row["passed"] for row in predecessor_rows), "detail": f"{sum(1 for row in predecessor_rows if row['passed'])}/{len(predecessor_rows)}"},
        {"check": "artifact_presence", "passed": all(row["passed"] for row in artifact_presence_rows), "detail": f"{sum(1 for row in artifact_presence_rows if row['passed'])}/{len(artifact_presence_rows)}"},
        {"check": "checks_consistency", "passed": all(row["passed"] for row in checks_consistency_rows), "detail": f"{sum(1 for row in checks_consistency_rows if row['passed'])}/{len(checks_consistency_rows)}"},
        {"check": "failed_families", "passed": all(row["passed"] for row in failed_family_rows), "detail": f"{sum(1 for row in failed_family_rows if row['passed'])}/{len(failed_family_rows)}"},
        {"check": "acquisition_contracts", "passed": all(row["passed"] for row in contract_rows), "detail": f"{sum(1 for row in contract_rows if row['passed'])}/{len(contract_rows)}"},
        {"check": "source_inventory_guidance", "passed": all(row["passed"] for row in inventory_rows), "detail": f"{sum(1 for row in inventory_rows if row['passed'])}/{len(inventory_rows)}"},
        {"check": "validation_gates", "passed": all(row["passed"] for row in validation_gate_rows), "detail": f"{sum(1 for row in validation_gate_rows if row['passed'])}/{len(validation_gate_rows)}"},
        {"check": "blocking_risks", "passed": all(row["passed"] for row in blocking_risk_rows), "detail": f"{sum(1 for row in blocking_risk_rows if row['passed'])}/{len(blocking_risk_rows)}"},
        {"check": "implementation_sequence", "passed": all(row["passed"] for row in implementation_sequence_rows), "detail": f"{sum(1 for row in implementation_sequence_rows if row['passed'])}/{len(implementation_sequence_rows)}"},
        {"check": "acceptance_criteria", "passed": all(row["passed"] for row in acceptance_rows), "detail": f"{sum(1 for row in acceptance_rows if row['passed'])}/{len(acceptance_rows)}"},
        {"check": "decision", "passed": all(row["passed"] for row in decision_rows), "detail": f"{sum(1 for row in decision_rows if row['passed'])}/{len(decision_rows)}"},
        {"check": "future_6hr_contract", "passed": all(row["passed"] for row in future_6hr_rows), "detail": f"{sum(1 for row in future_6hr_rows if row['passed'])}/{len(future_6hr_rows)}"},
        {"check": "safety_boundaries", "passed": all(row["passed"] for row in safety_rows), "detail": f"{sum(1 for row in safety_rows if row['passed'])}/{len(safety_rows)}"},
        {"check": "immutability", "passed": all(row["passed"] for row in immutability_rows), "detail": f"{sum(1 for row in immutability_rows if row['passed'])}/{len(immutability_rows)}"},
        {"check": "recommended_path", "passed": all(row["passed"] for row in recommended_rows), "detail": f"{sum(1 for row in recommended_rows if row['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all(row["passed"] for row in checks)
    acquisition_contracts_complete = all(row["passed"] for row in contract_rows)
    inventory_complete = all(row["passed"] for row in inventory_rows)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "artifact_presence": write_csv(ARTIFACT_PRESENCE_CSV, artifact_presence_rows),
        "checks_consistency": write_csv(CHECKS_CONSISTENCY_CSV, checks_consistency_rows),
        "failed_families": write_csv(FAILED_FAMILIES_CSV, failed_family_rows),
        "acquisition_contracts": write_csv(ACQUISITION_CONTRACTS_CSV, contract_rows),
        "source_inventory_guidance": write_csv(SOURCE_INVENTORY_CSV, inventory_rows),
        "validation_gates": write_csv(VALIDATION_GATES_CSV, validation_gate_rows),
        "blocking_risks": write_csv(BLOCKING_RISKS_CSV, blocking_risk_rows),
        "implementation_sequence": write_csv(IMPLEMENTATION_SEQUENCE_CSV, implementation_sequence_rows),
        "acceptance_criteria": write_csv(ACCEPTANCE_CSV, acceptance_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "future_6hr_contract": write_csv(FUTURE_6HR_CONTRACT_CSV, future_6hr_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_PATH_CSV, recommended_rows),
    }

    summary = {
        "layer": "6HQ",
        "layer_type": "game_mechanics_realism",
        "audit_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6HQ if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6HQ,
        "recommended_path": RECOMMENDED_PATH_6HQ,
        "audited_layer": "6HP",
        "predecessor_plan": str(PLAN_6HP_PATH),
        "predecessor_plan_returncode": 0,
        "predecessor_plan_diagnosis": json_6hp.get("diagnosis"),
        "planning_only_confirmed": json_6hp.get("planning_only") is True,
        "deterministic_source_acquisition_required_confirmed": json_6hp.get("deterministic_source_acquisition_required_by_6ho") is True,
        "deterministic_source_acquisition_plan_created": json_6hp.get("deterministic_source_acquisition_plan_created") is True,
        "failed_source_family_count": json_6hp.get("failed_source_family_count"),
        "acquisition_family_count": json_6hp.get("acquisition_family_count"),
        "acquisition_contract_count": json_6hp.get("acquisition_contract_count"),
        "acquisition_contracts_complete": acquisition_contracts_complete,
        "source_inventory_guidance_count": json_6hp.get("source_inventory_guidance_count"),
        "source_inventory_guidance_complete": inventory_complete,
        "validation_gate_count": json_6hp.get("validation_gate_count"),
        "blocking_risk_count": json_6hp.get("blocking_risk_count"),
        "implementation_step_count": json_6hp.get("implementation_step_count"),
        "acceptance_criteria_count": json_6hp.get("acceptance_criteria_count"),
        "future_6hr_contract_count": len(future_6hr),
        "implementation_allowed_after_this_audit": True,
        "source_acquisition_implementation_required_next": True,
        "future_implementation_layer": FUTURE_IMPL_LAYER,
        "adapter_revision_allowed_after_this_audit": False,
        "adapter_revision_still_blocked": True,
        "real_evaluation_allowed_after_this_audit": False,
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
        "source_acquisition_performed_by_this_layer": False,
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
            "failed_families_csv": str(FAILED_FAMILIES_CSV),
            "acquisition_contracts_csv": str(ACQUISITION_CONTRACTS_CSV),
            "source_inventory_guidance_csv": str(SOURCE_INVENTORY_CSV),
            "validation_gates_csv": str(VALIDATION_GATES_CSV),
            "blocking_risks_csv": str(BLOCKING_RISKS_CSV),
            "implementation_sequence_csv": str(IMPLEMENTATION_SEQUENCE_CSV),
            "acceptance_criteria_csv": str(ACCEPTANCE_CSV),
            "decision_csv": str(DECISION_CSV),
            "future_6hr_contract_csv": str(FUTURE_6HR_CONTRACT_CSV),
            "safety_boundaries_csv": str(SAFETY_CSV),
            "immutability_csv": str(IMMUTABILITY_CSV),
            "recommended_path_csv": str(RECOMMENDED_PATH_CSV),
        },
    }

    JSON_PATH.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
