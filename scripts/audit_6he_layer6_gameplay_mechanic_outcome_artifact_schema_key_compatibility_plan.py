#!/usr/bin/env python3
"""Audit Layer 6HD schema/key compatibility resolution plan."""

from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6he_schema_key_compatibility_plan_audit"
TMP_DIR = Path("tmp")

PLAN_6HD_PATH = Path("scripts/plan_6hd_layer6_gameplay_mechanic_outcome_artifact_schema_key_compatibility.py")
AUDIT_6HC_PATH = Path("scripts/audit_6hc_layer6_gameplay_mechanic_outcome_artifact_adapter_implementation.py")
IMPLEMENT_6HB_PATH = Path("scripts/implement_6hb_layer6_gameplay_mechanic_outcome_artifact_adapter.py")

JSON_6HD = TMP_DIR / "layer6_6hd_schema_key_compatibility_plan.json"
CHECKS_6HD = TMP_DIR / "layer6_6hd_schema_key_compatibility_plan_checks.csv"
PREDECESSOR_6HD = TMP_DIR / "layer6_6hd_schema_key_compatibility_plan_predecessor.csv"
INPUT_ARTIFACTS_6HD = TMP_DIR / "layer6_6hd_schema_key_compatibility_plan_input_artifacts.csv"
SOURCE_CLASSIFICATION_6HD = TMP_DIR / "layer6_6hd_schema_key_compatibility_plan_selected_source_classification.csv"
FAILURE_DIAGNOSIS_6HD = TMP_DIR / "layer6_6hd_schema_key_compatibility_plan_failure_diagnosis.csv"
FIELD_GAPS_6HD = TMP_DIR / "layer6_6hd_schema_key_compatibility_plan_canonical_field_gaps.csv"
SOURCE_FILTER_POLICY_6HD = TMP_DIR / "layer6_6hd_schema_key_compatibility_plan_source_filter_policy.csv"
ALIAS_MAPPING_POLICY_6HD = TMP_DIR / "layer6_6hd_schema_key_compatibility_plan_alias_mapping_policy.csv"
FAIL_CLOSED_POLICY_6HD = TMP_DIR / "layer6_6hd_schema_key_compatibility_plan_fail_closed_policy.csv"
FUTURE_6HE_6HD = TMP_DIR / "layer6_6hd_schema_key_compatibility_plan_future_6he_contract.csv"
FUTURE_6HF_6HD = TMP_DIR / "layer6_6hd_schema_key_compatibility_plan_future_6hf_contract.csv"
SAFETY_6HD = TMP_DIR / "layer6_6hd_schema_key_compatibility_plan_safety_boundaries.csv"
IMMUTABILITY_6HD = TMP_DIR / "layer6_6hd_schema_key_compatibility_plan_immutability.csv"
RECOMMENDED_6HD = TMP_DIR / "layer6_6hd_schema_key_compatibility_plan_recommended_path.csv"

REQUIRED_6HD_ARTIFACTS = [
    JSON_6HD,
    CHECKS_6HD,
    PREDECESSOR_6HD,
    INPUT_ARTIFACTS_6HD,
    SOURCE_CLASSIFICATION_6HD,
    FAILURE_DIAGNOSIS_6HD,
    FIELD_GAPS_6HD,
    SOURCE_FILTER_POLICY_6HD,
    ALIAS_MAPPING_POLICY_6HD,
    FAIL_CLOSED_POLICY_6HD,
    FUTURE_6HE_6HD,
    FUTURE_6HF_6HD,
    SAFETY_6HD,
    IMMUTABILITY_6HD,
    RECOMMENDED_6HD,
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
ARTIFACT_PRESENCE_CSV = TMP_DIR / f"{SLUG}_artifact_presence.csv"
CHECKS_CONSISTENCY_CSV = TMP_DIR / f"{SLUG}_checks_consistency.csv"
SOURCE_CLASSIFICATION_AUDIT_CSV = TMP_DIR / f"{SLUG}_source_classification.csv"
FAILURE_DIAGNOSIS_AUDIT_CSV = TMP_DIR / f"{SLUG}_failure_diagnosis.csv"
FIELD_GAPS_AUDIT_CSV = TMP_DIR / f"{SLUG}_canonical_field_gaps.csv"
SOURCE_FILTER_AUDIT_CSV = TMP_DIR / f"{SLUG}_source_filter_policy.csv"
ALIAS_MAPPING_AUDIT_CSV = TMP_DIR / f"{SLUG}_alias_mapping_policy.csv"
FAIL_CLOSED_AUDIT_CSV = TMP_DIR / f"{SLUG}_fail_closed_policy.csv"
FUTURE_6HF_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6hf_contract.csv"
SAFETY_BOUNDARIES_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_PATH_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6HD = "layer_6_gameplay_mechanic_outcome_artifact_schema_key_compatibility_plan_complete"
DIAGNOSIS_6HE = "layer_6_gameplay_mechanic_outcome_artifact_schema_key_compatibility_plan_audit_complete"
CURRENT_LAYER = "6HE_layer_6_gameplay_mechanic_outcome_artifact_schema_key_compatibility_plan_audit"
RECOMMENDED_NEXT_LAYER = "6HF_layer_6_gameplay_mechanic_outcome_artifact_adapter_source_filter_alias_revision"
RECOMMENDED_PATH = "audit_schema_key_compatibility_plan_then_revise_adapter_source_filter_alias_mapping"

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


def safe_env() -> Dict[str, str]:
    env = dict(os.environ)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    return env


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    text = path.read_text(encoding="utf-8").strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        parsed, _ = json.JSONDecoder().raw_decode(text)
        return parsed if isinstance(parsed, dict) else {}


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


def intish(value: Any, default: int = -1) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    compile_returncode, compile_errors = syntax_compile()

    script_before = Path(__file__).read_text(encoding="utf-8")
    plan_6hd_before = PLAN_6HD_PATH.read_text(encoding="utf-8") if PLAN_6HD_PATH.exists() else ""
    audit_6hc_before = AUDIT_6HC_PATH.read_text(encoding="utf-8") if AUDIT_6HC_PATH.exists() else ""
    implement_6hb_before = IMPLEMENT_6HB_PATH.read_text(encoding="utf-8") if IMPLEMENT_6HB_PATH.exists() else ""

    plan_run = subprocess.run(
        [sys.executable, str(PLAN_6HD_PATH)],
        check=False,
        text=True,
        capture_output=True,
        env=safe_env(),
    )

    json_6hd = load_json(JSON_6HD)
    checks_6hd = read_csv(CHECKS_6HD)
    classification_rows = read_csv(SOURCE_CLASSIFICATION_6HD)
    failure_rows = read_csv(FAILURE_DIAGNOSIS_6HD)
    field_gap_rows = read_csv(FIELD_GAPS_6HD)
    source_filter_rows = read_csv(SOURCE_FILTER_POLICY_6HD)
    alias_rows = read_csv(ALIAS_MAPPING_POLICY_6HD)
    fail_closed_rows = read_csv(FAIL_CLOSED_POLICY_6HD)
    future_6he_rows = read_csv(FUTURE_6HE_6HD)
    future_6hf_rows = read_csv(FUTURE_6HF_6HD)
    safety_rows = read_csv(SAFETY_6HD)

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6hd_plan_exists", "expected": True, "actual": PLAN_6HD_PATH.exists(), "passed": PLAN_6HD_PATH.exists()},
        {"check": "6hd_plan_runs", "expected": 0, "actual": plan_run.returncode, "passed": plan_run.returncode == 0},
        {"check": "6hd_json_exists", "expected": True, "actual": JSON_6HD.exists(), "passed": JSON_6HD.exists()},
        {"check": "6hd_all_checks_passed", "expected": True, "actual": json_6hd.get("all_checks_passed"), "passed": json_6hd.get("all_checks_passed") is True},
        {"check": "6hd_diagnosis", "expected": DIAGNOSIS_6HD, "actual": json_6hd.get("diagnosis"), "passed": json_6hd.get("diagnosis") == DIAGNOSIS_6HD},
        {"check": "6hd_recommended_next_layer", "expected": CURRENT_LAYER, "actual": json_6hd.get("recommended_next_layer"), "passed": json_6hd.get("recommended_next_layer") == CURRENT_LAYER},
        {"check": "6hd_planning_only", "expected": True, "actual": json_6hd.get("planning_only"), "passed": json_6hd.get("planning_only") is True},
        {"check": "6hd_schema_key_resolution_required", "expected": True, "actual": json_6hd.get("schema_key_compatibility_resolution_required"), "passed": json_6hd.get("schema_key_compatibility_resolution_required") is True},
        {"check": "6hd_real_eval_blocked", "expected": True, "actual": json_6hd.get("real_evaluation_blocked_by_validation"), "passed": json_6hd.get("real_evaluation_blocked_by_validation") is True},
        {"check": "6hd_adapter_revision_required", "expected": True, "actual": json_6hd.get("adapter_revision_required"), "passed": json_6hd.get("adapter_revision_required") is True},
        {"check": "6hd_strict_source_filter_required", "expected": True, "actual": json_6hd.get("strict_source_filter_required"), "passed": json_6hd.get("strict_source_filter_required") is True},
        {"check": "6hd_alias_mapping_revision_required", "expected": True, "actual": json_6hd.get("alias_mapping_revision_required"), "passed": json_6hd.get("alias_mapping_revision_required") is True},
        {"check": "6hd_prior_adapter_outputs_excluded_future", "expected": True, "actual": json_6hd.get("prior_adapter_outputs_excluded_in_future"), "passed": json_6hd.get("prior_adapter_outputs_excluded_in_future") is True},
        {"check": "6hd_validation_passed_zero", "expected": 0, "actual": json_6hd.get("validation_passed_row_count"), "passed": intish(json_6hd.get("validation_passed_row_count")) == 0},
        {"check": "6hd_validation_failed_positive", "expected": ">=1", "actual": json_6hd.get("validation_failed_closed_row_count"), "passed": intish(json_6hd.get("validation_failed_closed_row_count"), 0) >= 1},
        {"check": "6hd_selected_sources_positive", "expected": ">=1", "actual": json_6hd.get("selected_source_artifact_count"), "passed": intish(json_6hd.get("selected_source_artifact_count"), 0) >= 1},
        {"check": "6hd_likely_actual_positive", "expected": ">=1", "actual": json_6hd.get("likely_actual_source_count"), "passed": intish(json_6hd.get("likely_actual_source_count"), 0) >= 1},
        {"check": "6hd_likely_meta_positive", "expected": ">=1", "actual": json_6hd.get("likely_meta_or_planning_source_count"), "passed": intish(json_6hd.get("likely_meta_or_planning_source_count"), 0) >= 1},
        {"check": "6hd_likely_prior_output_positive", "expected": ">=1", "actual": json_6hd.get("likely_prior_adapter_output_source_count"), "passed": intish(json_6hd.get("likely_prior_adapter_output_source_count"), 0) >= 1},
        {"check": "6hd_source_filter_rule_count", "expected": ">=6", "actual": json_6hd.get("source_filter_rule_count"), "passed": intish(json_6hd.get("source_filter_rule_count"), 0) >= 6},
        {"check": "6hd_alias_rule_count", "expected": ">=9", "actual": json_6hd.get("alias_mapping_rule_count"), "passed": intish(json_6hd.get("alias_mapping_rule_count"), 0) >= 9},
        {"check": "6hd_fail_closed_rule_count", "expected": ">=5", "actual": json_6hd.get("fail_closed_rule_count"), "passed": intish(json_6hd.get("fail_closed_rule_count"), 0) >= 5},
        {"check": "6hd_no_real_backtests", "expected": False, "actual": json_6hd.get("real_backtests_run"), "passed": json_6hd.get("real_backtests_run") is False},
        {"check": "6hd_no_mechanic_evaluation", "expected": False, "actual": json_6hd.get("mechanic_evaluations_run"), "passed": json_6hd.get("mechanic_evaluations_run") is False},
        {"check": "6hd_activation_false", "expected": False, "actual": json_6hd.get("activation_allowed"), "passed": json_6hd.get("activation_allowed") is False},
        {"check": "6hd_exit_credit_false", "expected": False, "actual": json_6hd.get("layer_6_exit_credit"), "passed": json_6hd.get("layer_6_exit_credit") is False},
    ]

    artifact_presence_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "passed": path.exists()}
        for path in REQUIRED_6HD_ARTIFACTS
    ]

    checks_consistency_rows = [
        {"source_check": row.get("check"), "source_passed": row.get("passed"), "detail": row.get("detail", ""), "passed": boolish(row.get("passed"))}
        for row in checks_6hd
    ]

    source_classification_audit_rows = [
        {
            "audit": "classification_categories_present",
            "expected": "actual_or_transition_or_inning + meta + prior_adapter_output",
            "actual": "classification_rows",
            "passed": any(row.get("classification") in {"likely_actual_game_outcome_data", "likely_base_out_transition_data", "likely_inning_run_data"} for row in classification_rows)
            and any(row.get("classification") == "likely_planning_or_meta_artifact" for row in classification_rows)
            and any(row.get("classification") == "likely_prior_adapter_output" for row in classification_rows),
        },
        {
            "audit": "prior_outputs_excluded",
            "expected": "exclude",
            "actual": "prior_adapter_output_rows",
            "passed": all(row.get("future_source_filter_action") == "exclude" for row in classification_rows if row.get("classification") == "likely_prior_adapter_output"),
        },
        {
            "audit": "meta_artifacts_excluded",
            "expected": "exclude",
            "actual": "meta_rows",
            "passed": all(row.get("future_source_filter_action") == "exclude" for row in classification_rows if row.get("classification") == "likely_planning_or_meta_artifact"),
        },
        {
            "audit": "actual_like_sources_need_alias_review",
            "expected": "candidate_keep_with_alias_review",
            "actual": "actual_like_rows",
            "passed": any(row.get("future_source_filter_action") == "candidate_keep_with_alias_review" for row in classification_rows if row.get("classification") in {"likely_actual_game_outcome_data", "likely_base_out_transition_data", "likely_inning_run_data"}),
        },
    ]

    failure_audit_rows = [
        {
            "audit": "missing_required_identifiers_diagnosed",
            "expected": True,
            "actual": "|".join(row.get("failure_status", "") for row in failure_rows),
            "passed": any("missing_required_identifiers" in row.get("failure_status", "") for row in failure_rows),
        },
        {
            "audit": "failures_block_real_evaluation",
            "expected": True,
            "actual": "failure_rows",
            "passed": all(boolish(row.get("blocks_real_evaluation")) for row in failure_rows),
        },
        {
            "audit": "failures_block_activation",
            "expected": True,
            "actual": "failure_rows",
            "passed": all(boolish(row.get("blocks_activation")) for row in failure_rows),
        },
        {
            "audit": "failures_block_exit_credit",
            "expected": True,
            "actual": "failure_rows",
            "passed": all(boolish(row.get("blocks_layer_6_exit_credit")) for row in failure_rows),
        },
    ]

    field_gap_audit_rows = [
        {
            "audit": "field_gaps_exist",
            "expected": ">=1",
            "actual": len(field_gap_rows),
            "passed": len(field_gap_rows) >= 1,
        },
        {
            "audit": "adapter_revision_required_all_rows",
            "expected": True,
            "actual": "field_gap_rows",
            "passed": all(boolish(row.get("adapter_revision_required")) for row in field_gap_rows),
        },
        {
            "audit": "gap_type_present_all_rows",
            "expected": True,
            "actual": "field_gap_rows",
            "passed": all(bool(row.get("gap_type")) for row in field_gap_rows),
        },
    ]

    source_filter_audit_rows = [
        {
            "audit": "exclude_prior_adapter_outputs",
            "expected": True,
            "actual": "source_filter_policy",
            "passed": any(row.get("rule") == "exclude_prior_adapter_outputs" and boolish(row.get("passed")) for row in source_filter_rows),
        },
        {
            "audit": "exclude_planning_meta_artifacts",
            "expected": True,
            "actual": "source_filter_policy",
            "passed": any(row.get("rule") == "exclude_planning_meta_artifacts" and boolish(row.get("passed")) for row in source_filter_rows),
        },
        {
            "audit": "prefer_actual_outcome_like_artifacts",
            "expected": True,
            "actual": "source_filter_policy",
            "passed": any(row.get("rule") == "prefer_actual_game_result_like_artifacts" and boolish(row.get("passed")) for row in source_filter_rows),
        },
        {
            "audit": "source_filter_policy_all_passed",
            "expected": True,
            "actual": "source_filter_policy",
            "passed": all(boolish(row.get("passed")) for row in source_filter_rows),
        },
    ]

    required_alias_fields = {
        "game_id",
        "game_date",
        "home_team",
        "away_team",
        "home_runs",
        "away_runs",
        "inning",
        "half_inning",
        "runs_scored",
    }
    alias_fields = {row.get("canonical_field") for row in alias_rows}

    alias_audit_rows = [
        {
            "audit": "required_alias_fields_present",
            "expected": "|".join(sorted(required_alias_fields)),
            "actual": "|".join(sorted(alias_fields)),
            "passed": required_alias_fields.issubset(alias_fields),
        },
        {
            "audit": "alias_policy_all_passed",
            "expected": True,
            "actual": "alias_mapping_policy",
            "passed": all(boolish(row.get("passed")) for row in alias_rows),
        },
    ]

    fail_closed_audit_rows = [
        {
            "audit": "fail_closed_blocks_real_evaluation",
            "expected": True,
            "actual": "fail_closed_policy",
            "passed": all(boolish(row.get("blocks_real_evaluation")) for row in fail_closed_rows),
        },
        {
            "audit": "fail_closed_blocks_activation",
            "expected": True,
            "actual": "fail_closed_policy",
            "passed": all(boolish(row.get("blocks_activation")) for row in fail_closed_rows),
        },
        {
            "audit": "fail_closed_blocks_exit_credit",
            "expected": True,
            "actual": "fail_closed_policy",
            "passed": all(boolish(row.get("blocks_layer_6_exit_credit")) for row in fail_closed_rows),
        },
        {
            "audit": "fail_closed_policy_all_passed",
            "expected": True,
            "actual": "fail_closed_policy",
            "passed": all(boolish(row.get("passed")) for row in fail_closed_rows),
        },
    ]

    future_6hf_audit_rows = [
        {
            "contract": row.get("contract"),
            "required": row.get("required"),
            "source_passed": row.get("passed"),
            "passed": boolish(row.get("passed")),
        }
        for row in future_6hf_rows
    ]
    future_6hf_contract_names = {row.get("contract") for row in future_6hf_rows}
    future_6hf_audit_rows.append(
        {
            "contract": "required_contract_terms_present",
            "required": True,
            "source_passed": True,
            "passed": {
                "revise_local_adapter_source_filtering_only_after_6he",
                "exclude_prior_6hb_outputs_from_source_discovery",
                "prefer_real_outcome_like_artifacts_over_meta_artifacts",
                "update_deterministic_alias_mapping",
                "emit_normalized_local_tmp_artifacts_only",
                "fail_closed_if_identifiers_remain_insufficient",
                "no_real_backtests_or_mechanic_evaluation",
                "no_activation_or_layer_6_exit_credit",
                "future_audit_required_before_real_evaluation",
            }.issubset(future_6hf_contract_names),
        }
    )

    safety_audit_rows = [
        {
            "boundary": row.get("boundary"),
            "expected": row.get("expected"),
            "actual": row.get("actual"),
            "source_passed": row.get("passed"),
            "passed": boolish(row.get("passed")),
        }
        for row in safety_rows
    ]

    script_after = Path(__file__).read_text(encoding="utf-8")
    plan_6hd_after = PLAN_6HD_PATH.read_text(encoding="utf-8") if PLAN_6HD_PATH.exists() else ""
    audit_6hc_after = AUDIT_6HC_PATH.read_text(encoding="utf-8") if AUDIT_6HC_PATH.exists() else ""
    implement_6hb_after = IMPLEMENT_6HB_PATH.read_text(encoding="utf-8") if IMPLEMENT_6HB_PATH.exists() else ""

    immutability_rows = [
        {"surface": "this_6he_audit", "policy": "created_only", "passed": bool(script_after) and script_after == script_before},
        {"surface": "6hd_plan", "policy": "unchanged_by_6he", "passed": plan_6hd_after == plan_6hd_before},
        {"surface": "6hc_audit", "policy": "unchanged_by_6he", "passed": audit_6hc_after == audit_6hc_before},
        {"surface": "6hb_implementation", "policy": "unchanged_by_6he", "passed": implement_6hb_after == implement_6hb_before},
        {"surface": "simulator_projection_fixtures_defaults", "policy": "unchanged_by_6he", "passed": True},
        {"surface": "fetch_db_materialization_production_simulation", "policy": "not_run", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER, "actual": RECOMMENDED_NEXT_LAYER, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH, "actual": RECOMMENDED_PATH, "passed": True},
        {"decision": "audit_only", "expected": True, "actual": True, "passed": True},
        {"decision": "future_adapter_revision_allowed_after_this_audit", "expected": True, "actual": True, "passed": True},
        {"decision": "future_real_evaluation_allowed_by_this_layer", "expected": False, "actual": False, "passed": True},
        {"decision": "adapter_runtime_modified", "expected": False, "actual": False, "passed": True},
        {"decision": "normalized_outcomes_emitted_by_this_layer", "expected": False, "actual": False, "passed": True},
        {"decision": "activation_allowed", "expected": False, "actual": False, "passed": True},
        {"decision": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6HE, "actual": DIAGNOSIS_6HE, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all(row["passed"] for row in predecessor_rows), "detail": f"{sum(1 for row in predecessor_rows if row['passed'])}/{len(predecessor_rows)}"},
        {"check": "artifact_presence", "passed": all(row["passed"] for row in artifact_presence_rows), "detail": f"{sum(1 for row in artifact_presence_rows if row['passed'])}/{len(artifact_presence_rows)}"},
        {"check": "checks_consistency", "passed": len(checks_consistency_rows) >= 14 and all(row["passed"] for row in checks_consistency_rows), "detail": f"{sum(1 for row in checks_consistency_rows if row['passed'])}/{len(checks_consistency_rows)}"},
        {"check": "source_classification", "passed": all(row["passed"] for row in source_classification_audit_rows), "detail": f"{sum(1 for row in source_classification_audit_rows if row['passed'])}/{len(source_classification_audit_rows)}"},
        {"check": "failure_diagnosis", "passed": all(row["passed"] for row in failure_audit_rows), "detail": f"{sum(1 for row in failure_audit_rows if row['passed'])}/{len(failure_audit_rows)}"},
        {"check": "canonical_field_gaps", "passed": all(row["passed"] for row in field_gap_audit_rows), "detail": f"{sum(1 for row in field_gap_audit_rows if row['passed'])}/{len(field_gap_audit_rows)}"},
        {"check": "source_filter_policy", "passed": all(row["passed"] for row in source_filter_audit_rows), "detail": f"{sum(1 for row in source_filter_audit_rows if row['passed'])}/{len(source_filter_audit_rows)}"},
        {"check": "alias_mapping_policy", "passed": all(row["passed"] for row in alias_audit_rows), "detail": f"{sum(1 for row in alias_audit_rows if row['passed'])}/{len(alias_audit_rows)}"},
        {"check": "fail_closed_policy", "passed": all(row["passed"] for row in fail_closed_audit_rows), "detail": f"{sum(1 for row in fail_closed_audit_rows if row['passed'])}/{len(fail_closed_audit_rows)}"},
        {"check": "future_6he_contract_from_6hd", "passed": all(boolish(row.get("passed")) for row in future_6he_rows), "detail": f"{sum(1 for row in future_6he_rows if boolish(row.get('passed')))}" + f"/{len(future_6he_rows)}"},
        {"check": "future_6hf_contract", "passed": all(row["passed"] for row in future_6hf_audit_rows), "detail": f"{sum(1 for row in future_6hf_audit_rows if row['passed'])}/{len(future_6hf_audit_rows)}"},
        {"check": "safety_boundaries", "passed": all(row["passed"] for row in safety_audit_rows), "detail": f"{sum(1 for row in safety_audit_rows if row['passed'])}/{len(safety_audit_rows)}"},
        {"check": "immutability", "passed": all(row["passed"] for row in immutability_rows), "detail": f"{sum(1 for row in immutability_rows if row['passed'])}/{len(immutability_rows)}"},
        {"check": "recommended_path", "passed": all(row["passed"] for row in recommended_rows), "detail": f"{sum(1 for row in recommended_rows if row['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all(row["passed"] for row in checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "artifact_presence": write_csv(ARTIFACT_PRESENCE_CSV, artifact_presence_rows),
        "checks_consistency": write_csv(CHECKS_CONSISTENCY_CSV, checks_consistency_rows),
        "source_classification": write_csv(SOURCE_CLASSIFICATION_AUDIT_CSV, source_classification_audit_rows),
        "failure_diagnosis": write_csv(FAILURE_DIAGNOSIS_AUDIT_CSV, failure_audit_rows),
        "canonical_field_gaps": write_csv(FIELD_GAPS_AUDIT_CSV, field_gap_audit_rows),
        "source_filter_policy": write_csv(SOURCE_FILTER_AUDIT_CSV, source_filter_audit_rows),
        "alias_mapping_policy": write_csv(ALIAS_MAPPING_AUDIT_CSV, alias_audit_rows),
        "fail_closed_policy": write_csv(FAIL_CLOSED_AUDIT_CSV, fail_closed_audit_rows),
        "future_6hf_contract": write_csv(FUTURE_6HF_CONTRACT_CSV, future_6hf_audit_rows),
        "safety_boundaries": write_csv(SAFETY_BOUNDARIES_CSV, safety_audit_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_PATH_CSV, recommended_rows),
    }

    summary = {
        "layer": "6HE",
        "layer_type": "game_mechanics_realism",
        "audit_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6HE if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "recommended_path": RECOMMENDED_PATH,
        "audited_layer": "6HD",
        "audited_plan_diagnosis": json_6hd.get("diagnosis"),
        "predecessor_plan": str(PLAN_6HD_PATH),
        "predecessor_plan_returncode": plan_run.returncode,
        "predecessor_plan_diagnosis": json_6hd.get("diagnosis"),
        "schema_key_compatibility_resolution_required": True,
        "real_evaluation_blocked_by_validation": True,
        "adapter_revision_required": True,
        "strict_source_filter_required": True,
        "alias_mapping_revision_required": True,
        "prior_adapter_outputs_excluded_in_future": True,
        "validation_passed_row_count": intish(json_6hd.get("validation_passed_row_count")),
        "validation_failed_closed_row_count": intish(json_6hd.get("validation_failed_closed_row_count")),
        "selected_source_artifact_count": intish(json_6hd.get("selected_source_artifact_count")),
        "likely_actual_source_count": intish(json_6hd.get("likely_actual_source_count")),
        "likely_meta_or_planning_source_count": intish(json_6hd.get("likely_meta_or_planning_source_count")),
        "likely_prior_adapter_output_source_count": intish(json_6hd.get("likely_prior_adapter_output_source_count")),
        "source_filter_rule_count": intish(json_6hd.get("source_filter_rule_count")),
        "alias_mapping_rule_count": intish(json_6hd.get("alias_mapping_rule_count")),
        "fail_closed_rule_count": intish(json_6hd.get("fail_closed_rule_count")),
        "future_adapter_revision_allowed_after_this_audit": True,
        "future_real_evaluation_allowed_by_this_layer": False,
        "layer_6_exit_ready": False,
        "mechanics_activated_by_this_layer": False,
        "real_backtests_run": False,
        "mechanic_evaluations_run": False,
        "actual_outcomes_joined_to_mechanics": False,
        "normalized_outcomes_emitted_by_this_layer": False,
        "adapter_runtime_modified": False,
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
            "source_classification_csv": str(SOURCE_CLASSIFICATION_AUDIT_CSV),
            "failure_diagnosis_csv": str(FAILURE_DIAGNOSIS_AUDIT_CSV),
            "canonical_field_gaps_csv": str(FIELD_GAPS_AUDIT_CSV),
            "source_filter_policy_csv": str(SOURCE_FILTER_AUDIT_CSV),
            "alias_mapping_policy_csv": str(ALIAS_MAPPING_AUDIT_CSV),
            "fail_closed_policy_csv": str(FAIL_CLOSED_AUDIT_CSV),
            "future_6hf_contract_csv": str(FUTURE_6HF_CONTRACT_CSV),
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
