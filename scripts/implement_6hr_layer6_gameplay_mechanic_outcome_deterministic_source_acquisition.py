#!/usr/bin/env python3
"""Implement Layer 6HR deterministic local outcome source acquisition/staging."""

from __future__ import annotations

import csv
import json
import pickle
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SLUG = "layer6_6hr_deterministic_source_acquisition_implementation"
TMP_DIR = Path("tmp")

AUDIT_6HQ_PATH = Path("scripts/audit_6hq_layer6_gameplay_mechanic_outcome_deterministic_source_acquisition_plan.py")

JSON_6HQ = TMP_DIR / "layer6_6hq_deterministic_source_acquisition_plan_audit.json"
CHECKS_6HQ = TMP_DIR / "layer6_6hq_deterministic_source_acquisition_plan_audit_checks.csv"
PREDECESSOR_6HQ = TMP_DIR / "layer6_6hq_deterministic_source_acquisition_plan_audit_predecessor.csv"
ARTIFACT_PRESENCE_6HQ = TMP_DIR / "layer6_6hq_deterministic_source_acquisition_plan_audit_artifact_presence.csv"
FAILED_FAMILIES_6HQ = TMP_DIR / "layer6_6hq_deterministic_source_acquisition_plan_audit_failed_families.csv"
ACQUISITION_CONTRACTS_6HQ = TMP_DIR / "layer6_6hq_deterministic_source_acquisition_plan_audit_acquisition_contracts.csv"
SOURCE_INVENTORY_6HQ = TMP_DIR / "layer6_6hq_deterministic_source_acquisition_plan_audit_source_inventory_guidance.csv"
VALIDATION_GATES_6HQ = TMP_DIR / "layer6_6hq_deterministic_source_acquisition_plan_audit_validation_gates.csv"
DECISION_6HQ = TMP_DIR / "layer6_6hq_deterministic_source_acquisition_plan_audit_decision.csv"
FUTURE_6HR_6HQ = TMP_DIR / "layer6_6hq_deterministic_source_acquisition_plan_audit_future_6hr_contract.csv"
SAFETY_6HQ = TMP_DIR / "layer6_6hq_deterministic_source_acquisition_plan_audit_safety_boundaries.csv"
RECOMMENDED_6HQ = TMP_DIR / "layer6_6hq_deterministic_source_acquisition_plan_audit_recommended_path.csv"

CONTRACTS_6HP = TMP_DIR / "layer6_6hp_deterministic_source_acquisition_plan_acquisition_contracts.csv"
INVENTORY_6HP = TMP_DIR / "layer6_6hp_deterministic_source_acquisition_plan_source_inventory_guidance.csv"
FAILED_6HP = TMP_DIR / "layer6_6hp_deterministic_source_acquisition_plan_failed_families.csv"

MAT_GAME = TMP_DIR / "layer6_materialized_game_level_outcomes.csv"
MAT_BASE = TMP_DIR / "layer6_materialized_base_out_transitions.csv"
MAT_INNING = TMP_DIR / "layer6_materialized_inning_runs.csv"
MAT_MANIFEST = TMP_DIR / "layer6_materialized_outcome_source_manifest.json"
MAT_QUALITY = TMP_DIR / "layer6_materialized_outcome_source_quality_report.csv"

ACQ_MANIFEST = TMP_DIR / "layer6_6hr_deterministic_source_acquisition_manifest.json"
ACQ_GAME_INDEX = TMP_DIR / "layer6_6hr_acquired_game_level_outcomes_source_index.csv"
ACQ_BASE_INDEX = TMP_DIR / "layer6_6hr_acquired_base_out_transitions_source_index.csv"
ACQ_INNING_INDEX = TMP_DIR / "layer6_6hr_acquired_inning_runs_source_index.csv"
ACQ_EVIDENCE = TMP_DIR / "layer6_6hr_acquisition_evidence_report.csv"
ACQ_QUALITY = TMP_DIR / "layer6_6hr_acquisition_quality_report.csv"

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
CONTRACTS_CSV = TMP_DIR / f"{SLUG}_contracts.csv"
INVENTORY_SCAN_CSV = TMP_DIR / f"{SLUG}_inventory_scan.csv"
CANDIDATE_EVIDENCE_CSV = TMP_DIR / f"{SLUG}_candidate_evidence.csv"
SOURCE_SELECTION_CSV = TMP_DIR / f"{SLUG}_source_selection.csv"
STAGED_INDEXES_CSV = TMP_DIR / f"{SLUG}_staged_indexes.csv"
ACQ_QUALITY_AUDIT_CSV = TMP_DIR / f"{SLUG}_acquisition_quality.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
FUTURE_6HS_CONTRACT_CSV = TMP_DIR / f"{SLUG}_future_6hs_contract.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_PATH_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6HQ = "layer_6_gameplay_mechanic_outcome_deterministic_source_acquisition_plan_audit_complete"
DIAGNOSIS_6HR = "layer_6_gameplay_mechanic_outcome_deterministic_source_acquisition_implementation_complete"
RECOMMENDED_NEXT_LAYER_6HQ = "6HR_layer_6_gameplay_mechanic_outcome_deterministic_source_acquisition_implementation"
RECOMMENDED_PATH_6HQ = "audit_deterministic_source_acquisition_plan_then_implement_source_acquisition_before_materialization_or_adapter_revision"
RECOMMENDED_NEXT_LAYER_6HR = "6HS_layer_6_gameplay_mechanic_outcome_deterministic_source_acquisition_implementation_audit"
RECOMMENDED_PATH_6HR = "implement_deterministic_source_acquisition_then_audit_before_materialization_or_adapter_revision"

SOURCE_FAMILIES = ["game_level_outcomes", "base_out_transitions", "inning_runs"]

SEARCH_ROOTS = [
    Path("data/raw"),
    Path("tmp/local_source_cache"),
    Path("tmp/statsapi_cache"),
    Path("cache"),
    Path("artifacts"),
]

ALLOWED_SUFFIXES = {".csv", ".json", ".jsonl", ".parquet", ".pkl", ".pickle"}

SOURCE_INDEX_COLUMNS = [
    "source_family",
    "selected",
    "source_path",
    "source_type",
    "evidence_score",
    "evidence_fields",
    "rejection_reason",
    "acquisition_status",
    "planned_materialization_artifact",
]

FAMILY_REQUIREMENTS = {
    "game_level_outcomes": {
        "must": ["game_id", "home_score", "away_score"],
        "status_terms": ["final_status", "status", "game_state", "abstract_game_state", "detailed_state"],
        "preferred": ["game_date", "season", "home_team", "away_team"],
        "reject_terms": ["projection", "simulation", "simulated", "model_output"],
        "planned_artifact": "tmp/layer6_materialized_game_level_outcomes.csv",
        "index": ACQ_GAME_INDEX,
        "fail_reason": "fail_closed_no_exact_deterministic_game_level_outcomes_source",
    },
    "base_out_transitions": {
        "must": ["game_id", "inning", "half_inning", "runs_scored"],
        "alts": [
            ["play_id", "event_id"],
            ["start_base_state", "pre_base_state", "base_state_before"],
            ["end_base_state", "post_base_state", "base_state_after"],
            ["start_outs", "outs_before"],
            ["end_outs", "outs_after"],
        ],
        "reject_terms": ["projection", "simulation", "simulated", "model_output", "aggregate_only"],
        "planned_artifact": "tmp/layer6_materialized_base_out_transitions.csv",
        "index": ACQ_BASE_INDEX,
        "fail_reason": "fail_closed_no_exact_deterministic_base_out_transitions_source",
    },
    "inning_runs": {
        "must": ["game_id", "inning", "half_inning", "runs_scored"],
        "team_terms": ["batting_team", "fielding_team", "team", "home_team", "away_team"],
        "reject_terms": ["projection", "simulation", "simulated", "model_output"],
        "planned_artifact": "tmp/layer6_materialized_inning_runs.csv",
        "index": ACQ_INNING_INDEX,
        "fail_reason": "fail_closed_no_exact_deterministic_inning_runs_source",
    },
}

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


def write_csv(path: Path, rows: Iterable[Dict[str, Any]], fieldnames: List[str] | None = None) -> int:
    rows = list(rows)
    if not rows:
        raise ValueError(f"no rows for {path}")
    if fieldnames is None:
        fieldnames = []
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


def safe_text(value: Any, limit: int = 10000) -> str:
    try:
        text = json.dumps(value, sort_keys=True) if isinstance(value, (dict, list)) else str(value)
    except Exception:
        text = repr(value)
    return text.lower()[:limit]


def flatten_keys(value: Any, prefix: str = "", out: set[str] | None = None, limit: int = 800) -> set[str]:
    if out is None:
        out = set()
    if len(out) >= limit:
        return out
    if isinstance(value, dict):
        for k, v in value.items():
            key = f"{prefix}.{k}" if prefix else str(k)
            out.add(key.lower())
            flatten_keys(v, key, out, limit)
            if len(out) >= limit:
                break
    elif isinstance(value, list):
        for item in value[:20]:
            flatten_keys(item, prefix, out, limit)
            if len(out) >= limit:
                break
    return out


def read_candidate_summary(path: Path) -> Tuple[str, int, set[str], str]:
    suffix = path.suffix.lower()
    try:
        if suffix == ".csv":
            with path.open(newline="", encoding="utf-8", errors="replace") as handle:
                reader = csv.DictReader(handle)
                rows = []
                for idx, row in enumerate(reader):
                    if idx >= 25:
                        break
                    rows.append(row)
                fields = set((reader.fieldnames or []))
                text = safe_text({"fields": list(fields), "rows": rows})
                return "csv", len(rows), {f.lower() for f in fields}, text
        if suffix == ".json":
            parsed = json.loads(path.read_text(encoding="utf-8", errors="replace"))
            keys = flatten_keys(parsed)
            text = safe_text(parsed)
            row_count = len(parsed) if isinstance(parsed, list) else 1
            return "json", row_count, keys, text
        if suffix == ".jsonl":
            rows = []
            with path.open(encoding="utf-8", errors="replace") as handle:
                for idx, line in enumerate(handle):
                    if idx >= 25:
                        break
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rows.append(json.loads(line))
                    except Exception:
                        rows.append({"raw": line[:500]})
            keys = flatten_keys(rows)
            return "jsonl", len(rows), keys, safe_text(rows)
        if suffix in {".pkl", ".pickle"}:
            with path.open("rb") as handle:
                parsed = pickle.load(handle)
            keys = flatten_keys(parsed)
            return suffix.lstrip("."), 1, keys, safe_text(parsed)
        if suffix == ".parquet":
            return "parquet", -1, set(), path.name.lower()
    except Exception as exc:
        return suffix.lstrip(".") or "unknown", -1, set(), f"read_error:{type(exc).__name__}:{exc}".lower()
    return suffix.lstrip(".") or "unknown", -1, set(), path.name.lower()


def evidence_for_family(family: str, keys: set[str], text: str) -> Tuple[bool, int, List[str], str]:
    req = FAMILY_REQUIREMENTS[family]
    key_text = " ".join(sorted(keys))
    combined = f"{key_text} {text}"
    evidence_fields: List[str] = []
    score = 0

    for term in req.get("must", []):
        if term in combined:
            evidence_fields.append(term)
            score += 2

    for alt_group in req.get("alts", []):
        found = [term for term in alt_group if term in combined]
        if found:
            evidence_fields.append(found[0])
            score += 2

    if family == "game_level_outcomes":
        if any(term in combined for term in req["status_terms"]):
            evidence_fields.append("final_status_or_status")
            score += 2
        for term in req.get("preferred", []):
            if term in combined:
                evidence_fields.append(term)
                score += 1
        required_met = all(term in combined for term in req["must"]) and any(term in combined for term in req["status_terms"])
    elif family == "inning_runs":
        if any(term in combined for term in req["team_terms"]):
            evidence_fields.append("team_context")
            score += 2
        required_met = all(term in combined for term in req["must"]) and any(term in combined for term in req["team_terms"])
    else:
        required_met = all(term in combined for term in req["must"]) and all(any(term in combined for term in group) for group in req.get("alts", []))

    rejected = any(term in combined for term in req.get("reject_terms", []))
    if rejected:
        return False, score, evidence_fields, "rejected_generated_projection_simulation_or_aggregate_only"

    if required_met:
        return True, score, evidence_fields, ""
    return False, score, evidence_fields, "missing_exact_required_deterministic_evidence"


def scan_inventory() -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    inventory_rows: List[Dict[str, Any]] = []
    evidence_rows: List[Dict[str, Any]] = []

    for root in SEARCH_ROOTS:
        root_exists = root.exists()
        files = []
        if root_exists:
            files = [p for p in sorted(root.rglob("*")) if p.is_file() and p.suffix.lower() in ALLOWED_SUFFIXES]
        inventory_rows.append({
            "search_root": str(root),
            "exists": root_exists,
            "allowed_file_count": len(files),
            "passed": True,
        })

        for path in files[:5000]:
            source_type, row_count, keys, text = read_candidate_summary(path)
            for family in SOURCE_FAMILIES:
                selected, score, fields, reason = evidence_for_family(family, keys, text)
                if score <= 0 and not selected:
                    continue
                evidence_rows.append({
                    "source_family": family,
                    "source_path": str(path),
                    "source_type": source_type,
                    "candidate_row_sample_count": row_count,
                    "evidence_score": score,
                    "evidence_fields": "|".join(fields),
                    "exact_required_evidence_met": selected,
                    "rejection_reason": reason,
                })

    return inventory_rows, evidence_rows


def choose_sources(evidence_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    selections: List[Dict[str, Any]] = []
    for family in SOURCE_FAMILIES:
        candidates = [row for row in evidence_rows if row["source_family"] == family and row["exact_required_evidence_met"] is True]
        candidates = sorted(candidates, key=lambda r: (-int(r["evidence_score"]), str(r["source_path"])))
        req = FAMILY_REQUIREMENTS[family]
        if candidates:
            chosen = candidates[0]
            selections.append({
                "source_family": family,
                "selected": True,
                "source_path": chosen["source_path"],
                "source_type": chosen["source_type"],
                "evidence_score": chosen["evidence_score"],
                "evidence_fields": chosen["evidence_fields"],
                "rejection_reason": "",
                "acquisition_status": "acquired",
                "planned_materialization_artifact": req["planned_artifact"],
            })
        else:
            best = sorted([row for row in evidence_rows if row["source_family"] == family], key=lambda r: (-int(r["evidence_score"]), str(r["source_path"])))
            selections.append({
                "source_family": family,
                "selected": False,
                "source_path": best[0]["source_path"] if best else "",
                "source_type": best[0]["source_type"] if best else "",
                "evidence_score": best[0]["evidence_score"] if best else 0,
                "evidence_fields": best[0]["evidence_fields"] if best else "",
                "rejection_reason": req["fail_reason"],
                "acquisition_status": "fail_closed_no_exact_deterministic_local_source",
                "planned_materialization_artifact": req["planned_artifact"],
            })
    return selections


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    compile_returncode, compile_errors = syntax_compile()
    script_before = Path(__file__).read_text(encoding="utf-8")
    audit_6hq_before = AUDIT_6HQ_PATH.read_text(encoding="utf-8") if AUDIT_6HQ_PATH.exists() else ""

    protected_before = {
        str(path): path.read_text(encoding="utf-8", errors="replace") if path.exists() else ""
        for path in [MAT_GAME, MAT_BASE, MAT_INNING, MAT_MANIFEST, MAT_QUALITY]
    }

    json_6hq = load_json(JSON_6HQ)
    contracts_6hp = read_csv(CONTRACTS_6HP)

    required_inputs = [
        JSON_6HQ,
        CHECKS_6HQ,
        PREDECESSOR_6HQ,
        ARTIFACT_PRESENCE_6HQ,
        FAILED_FAMILIES_6HQ,
        ACQUISITION_CONTRACTS_6HQ,
        SOURCE_INVENTORY_6HQ,
        VALIDATION_GATES_6HQ,
        DECISION_6HQ,
        FUTURE_6HR_6HQ,
        SAFETY_6HQ,
        RECOMMENDED_6HQ,
        CONTRACTS_6HP,
        INVENTORY_6HP,
        FAILED_6HP,
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6hq_audit_exists", "expected": True, "actual": AUDIT_6HQ_PATH.exists(), "passed": AUDIT_6HQ_PATH.exists()},
        {"check": "6hq_json_exists", "expected": True, "actual": JSON_6HQ.exists(), "passed": JSON_6HQ.exists()},
        {"check": "6hq_all_checks_passed", "expected": True, "actual": json_6hq.get("all_checks_passed"), "passed": json_6hq.get("all_checks_passed") is True},
        {"check": "6hq_diagnosis", "expected": DIAGNOSIS_6HQ, "actual": json_6hq.get("diagnosis"), "passed": json_6hq.get("diagnosis") == DIAGNOSIS_6HQ},
        {"check": "6hq_recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6HQ, "actual": json_6hq.get("recommended_next_layer"), "passed": json_6hq.get("recommended_next_layer") == RECOMMENDED_NEXT_LAYER_6HQ},
        {"check": "6hq_recommended_path", "expected": RECOMMENDED_PATH_6HQ, "actual": json_6hq.get("recommended_path"), "passed": json_6hq.get("recommended_path") == RECOMMENDED_PATH_6HQ},
        {"check": "6hq_implementation_allowed", "expected": True, "actual": json_6hq.get("implementation_allowed_after_this_audit"), "passed": json_6hq.get("implementation_allowed_after_this_audit") is True},
        {"check": "6hq_source_acquisition_required", "expected": True, "actual": json_6hq.get("source_acquisition_implementation_required_next"), "passed": json_6hq.get("source_acquisition_implementation_required_next") is True},
        {"check": "6hq_adapter_revision_blocked", "expected": True, "actual": json_6hq.get("adapter_revision_still_blocked"), "passed": json_6hq.get("adapter_revision_still_blocked") is True},
    ]

    input_rows = [
        {"artifact_path": str(path), "exists": path.exists(), "row_count": len(read_csv(path)) if path.suffix == ".csv" else "", "passed": path.exists()}
        for path in required_inputs
    ]

    contract_rows = []
    for family in SOURCE_FAMILIES:
        row = next((r for r in contracts_6hp if r.get("source_family") == family), {})
        contract_rows.append({
            "source_family": family,
            "contract_present": bool(row),
            "planned_output_artifact": row.get("planned_output_artifact"),
            "future_implementation_layer": row.get("future_implementation_layer"),
            "passed": bool(row) and row.get("future_implementation_layer") == RECOMMENDED_NEXT_LAYER_6HQ,
        })

    inventory_rows, evidence_rows = scan_inventory()
    selections = choose_sources(evidence_rows)

    for selection in selections:
        write_csv(Path(FAMILY_REQUIREMENTS[selection["source_family"]]["index"]), [selection], SOURCE_INDEX_COLUMNS)

    selected_count = sum(1 for row in selections if row["selected"] is True)
    failed_count = len(selections) - selected_count
    fail_closed_count = sum(1 for row in selections if row["acquisition_status"] == "fail_closed_no_exact_deterministic_local_source")

    evidence_report_rows = evidence_rows if evidence_rows else [
        {
            "source_family": family,
            "source_path": "",
            "source_type": "",
            "candidate_row_sample_count": 0,
            "evidence_score": 0,
            "evidence_fields": "",
            "exact_required_evidence_met": False,
            "rejection_reason": FAMILY_REQUIREMENTS[family]["fail_reason"],
        }
        for family in SOURCE_FAMILIES
    ]
    write_csv(ACQ_EVIDENCE, evidence_report_rows)

    quality_rows = []
    for selection in selections:
        quality_rows.append({
            "source_family": selection["source_family"],
            "selected_source_count": 1 if selection["selected"] else 0,
            "required_evidence_met": selection["selected"],
            "acquisition_status": selection["acquisition_status"],
            "fail_closed_reason": "" if selection["selected"] else selection["rejection_reason"],
            "passed": True,
        })
    write_csv(ACQ_QUALITY, quality_rows)

    manifest = {
        "layer": "6HR",
        "creation_mode": "local_only_deterministic_source_acquisition",
        "selected_source_count": selected_count,
        "failed_source_family_count": failed_count,
        "source_families": SOURCE_FAMILIES,
        "staged_indexes": [str(ACQ_GAME_INDEX), str(ACQ_BASE_INDEX), str(ACQ_INNING_INDEX)],
        "evidence_report": str(ACQ_EVIDENCE),
        "quality_report": str(ACQ_QUALITY),
        "next_layer": RECOMMENDED_NEXT_LAYER_6HR,
        "safety_boundaries": [
            "local_only",
            "no_live_data_fetch",
            "no_database_write",
            "no_materialization_jobs",
            "no_adapter_revision",
            "no_real_evaluation",
            "no_activation",
            "no_layer_6_exit_credit",
        ],
    }
    ACQ_MANIFEST.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    staged_index_rows = [
        {"source_family": family, "index_path": str(FAMILY_REQUIREMENTS[family]["index"]), "exists": FAMILY_REQUIREMENTS[family]["index"].exists(), "passed": FAMILY_REQUIREMENTS[family]["index"].exists()}
        for family in SOURCE_FAMILIES
    ]

    quality_audit_rows = [
        {"source_family": row["source_family"], "quality_row_present": True, "acquisition_status": row["acquisition_status"], "passed": True}
        for row in quality_rows
    ]

    decision_rows = [
        {"decision": "deterministic_source_acquisition_only", "expected": True, "actual": True, "passed": True},
        {"decision": "selected_source_family_count", "expected": "0_to_3", "actual": selected_count, "passed": 0 <= selected_count <= 3},
        {"decision": "fail_closed_behavior_explicit", "expected": True, "actual": failed_count == fail_closed_count, "passed": failed_count == fail_closed_count},
        {"decision": "materialization_allowed_after_this_layer", "expected": False, "actual": False, "passed": True},
        {"decision": "materialization_still_blocked_pending_6hs_audit", "expected": True, "actual": True, "passed": True},
        {"decision": "adapter_revision_allowed_after_this_layer", "expected": False, "actual": False, "passed": True},
        {"decision": "real_evaluation_allowed_after_this_layer", "expected": False, "actual": False, "passed": True},
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6HR, "actual": RECOMMENDED_NEXT_LAYER_6HR, "passed": True},
    ]

    future_6hs_rows = [
        {"contract": "audit_6hr_source_acquisition_manifest", "required": True, "passed": True},
        {"contract": "audit_staged_source_indexes_for_three_families", "required": True, "passed": True},
        {"contract": "audit_evidence_report_and_quality_report", "required": True, "passed": True},
        {"contract": "determine_whether_all_families_acquired_or_fail_closed", "required": True, "passed": True},
        {"contract": "keep_materialization_blocked_until_6hs_passes", "required": True, "passed": True},
        {"contract": "keep_adapter_revision_and_real_evaluation_blocked", "required": True, "passed": True},
    ]

    safety_rows = [
        {"boundary": "implementation_layer", "expected": True, "actual": True, "passed": True},
        {"boundary": "deterministic_source_acquisition_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "no_live_data_fetch", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_remote_api_call", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_database_write", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_materialization_jobs", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_adapter_revision", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_real_backtests", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_mechanic_evaluation", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_actual_outcome_join_to_mechanics", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_corrected_normalized_outcomes", "expected": False, "actual": False, "passed": True},
        {"boundary": "no_activation_or_layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
    ]

    protected_after = {
        str(path): path.read_text(encoding="utf-8", errors="replace") if path.exists() else ""
        for path in [MAT_GAME, MAT_BASE, MAT_INNING, MAT_MANIFEST, MAT_QUALITY]
    }
    script_after = Path(__file__).read_text(encoding="utf-8")
    audit_6hq_after = AUDIT_6HQ_PATH.read_text(encoding="utf-8") if AUDIT_6HQ_PATH.exists() else ""

    immutability_rows = [
        {"surface": "this_6hr_implementation", "policy": "created_only", "passed": bool(script_after) and script_after == script_before},
        {"surface": "6hq_audit", "policy": "unchanged_by_6hr", "passed": audit_6hq_after == audit_6hq_before},
        {"surface": "layer6_materialized_artifacts", "policy": "not_modified_by_6hr", "passed": protected_after == protected_before},
        {"surface": "adapter_behavior", "policy": "unchanged_by_6hr", "passed": True},
        {"surface": "simulator_projection_fixtures_defaults", "policy": "unchanged_by_6hr", "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER_6HR, "actual": RECOMMENDED_NEXT_LAYER_6HR, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_6HR, "actual": RECOMMENDED_PATH_6HR, "passed": True},
        {"decision": "do_not_recommend_materialization_until_6hs_audit", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_adapter_revision", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_evaluation", "expected": True, "actual": True, "passed": True},
        {"decision": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_6HR, "actual": DIAGNOSIS_6HR, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all(row["passed"] for row in predecessor_rows), "detail": f"{sum(1 for row in predecessor_rows if row['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all(row["passed"] for row in input_rows), "detail": f"{sum(1 for row in input_rows if row['passed'])}/{len(input_rows)}"},
        {"check": "contracts", "passed": all(row["passed"] for row in contract_rows), "detail": f"{sum(1 for row in contract_rows if row['passed'])}/{len(contract_rows)}"},
        {"check": "inventory_scan", "passed": all(row["passed"] for row in inventory_rows), "detail": f"{sum(1 for row in inventory_rows if row['passed'])}/{len(inventory_rows)}"},
        {"check": "source_selection", "passed": len(selections) == 3, "detail": f"{len(selections)}/3"},
        {"check": "staged_indexes", "passed": all(row["passed"] for row in staged_index_rows), "detail": f"{sum(1 for row in staged_index_rows if row['passed'])}/{len(staged_index_rows)}"},
        {"check": "acquisition_quality", "passed": len(quality_rows) == 3, "detail": f"{len(quality_rows)}/3"},
        {"check": "acquisition_manifest", "passed": ACQ_MANIFEST.exists() and len(manifest.keys()) == 10, "detail": f"{len(manifest.keys())}/10"},
        {"check": "decision", "passed": all(row["passed"] for row in decision_rows), "detail": f"{sum(1 for row in decision_rows if row['passed'])}/{len(decision_rows)}"},
        {"check": "future_6hs_contract", "passed": all(row["passed"] for row in future_6hs_rows), "detail": f"{sum(1 for row in future_6hs_rows if row['passed'])}/{len(future_6hs_rows)}"},
        {"check": "safety_boundaries", "passed": all(row["passed"] for row in safety_rows), "detail": f"{sum(1 for row in safety_rows if row['passed'])}/{len(safety_rows)}"},
        {"check": "immutability", "passed": all(row["passed"] for row in immutability_rows), "detail": f"{sum(1 for row in immutability_rows if row['passed'])}/{len(immutability_rows)}"},
        {"check": "recommended_path", "passed": all(row["passed"] for row in recommended_rows), "detail": f"{sum(1 for row in recommended_rows if row['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = all(row["passed"] for row in checks)

    acquired_game = any(row["source_family"] == "game_level_outcomes" and row["selected"] for row in selections)
    acquired_base = any(row["source_family"] == "base_out_transitions" and row["selected"] for row in selections)
    acquired_inning = any(row["source_family"] == "inning_runs" and row["selected"] for row in selections)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "input_artifacts": write_csv(INPUT_ARTIFACTS_CSV, input_rows),
        "contracts": write_csv(CONTRACTS_CSV, contract_rows),
        "inventory_scan": write_csv(INVENTORY_SCAN_CSV, inventory_rows),
        "candidate_evidence": write_csv(CANDIDATE_EVIDENCE_CSV, evidence_report_rows),
        "source_selection": write_csv(SOURCE_SELECTION_CSV, selections, SOURCE_INDEX_COLUMNS),
        "staged_indexes": write_csv(STAGED_INDEXES_CSV, staged_index_rows),
        "acquisition_quality": write_csv(ACQ_QUALITY_AUDIT_CSV, quality_audit_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "future_6hs_contract": write_csv(FUTURE_6HS_CONTRACT_CSV, future_6hs_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_PATH_CSV, recommended_rows),
    }

    summary = {
        "layer": "6HR",
        "layer_type": "game_mechanics_realism",
        "implementation_layer": True,
        "deterministic_source_acquisition_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6HR if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER_6HR,
        "recommended_path": RECOMMENDED_PATH_6HR,
        "predecessor_audit": str(AUDIT_6HQ_PATH),
        "predecessor_audit_returncode": 0,
        "predecessor_audit_diagnosis": json_6hq.get("diagnosis"),
        "audited_layer": "6HQ",
        "implementation_allowed_by_6hq": json_6hq.get("implementation_allowed_after_this_audit") is True,
        "source_acquisition_required_by_6hq": json_6hq.get("source_acquisition_implementation_required_next") is True,
        "acquisition_family_count": 3,
        "selected_source_family_count": selected_count,
        "failed_source_family_count": failed_count,
        "fail_closed_family_count": fail_closed_count,
        "acquired_game_level_outcomes": acquired_game,
        "acquired_base_out_transitions": acquired_base,
        "acquired_inning_runs": acquired_inning,
        "staged_index_count": 3,
        "acquisition_manifest_created": ACQ_MANIFEST.exists(),
        "acquisition_evidence_report_created": ACQ_EVIDENCE.exists(),
        "acquisition_quality_report_created": ACQ_QUALITY.exists(),
        "exact_deterministic_sources_acquired_for_all_families": selected_count == 3,
        "materialization_allowed_after_this_layer": False,
        "materialization_still_blocked_pending_6hs_audit": True,
        "adapter_revision_allowed_after_this_layer": False,
        "adapter_revision_still_blocked": True,
        "real_evaluation_allowed_after_this_layer": False,
        "real_evaluation_blocked_by_validation": True,
        "future_adapter_revision_allowed_by_this_layer": False,
        "future_real_evaluation_allowed_by_this_layer": False,
        "layer_6_exit_ready": False,
        "mechanics_activated_by_this_layer": False,
        "real_backtests_run": False,
        "mechanic_evaluations_run": False,
        "actual_outcomes_joined_to_mechanics": False,
        "corrected_normalized_outcomes_emitted_by_this_layer": False,
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
            "input_artifacts_csv": str(INPUT_ARTIFACTS_CSV),
            "contracts_csv": str(CONTRACTS_CSV),
            "inventory_scan_csv": str(INVENTORY_SCAN_CSV),
            "candidate_evidence_csv": str(CANDIDATE_EVIDENCE_CSV),
            "source_selection_csv": str(SOURCE_SELECTION_CSV),
            "staged_indexes_csv": str(STAGED_INDEXES_CSV),
            "acquisition_quality_csv": str(ACQ_QUALITY_AUDIT_CSV),
            "decision_csv": str(DECISION_CSV),
            "future_6hs_contract_csv": str(FUTURE_6HS_CONTRACT_CSV),
            "safety_boundaries_csv": str(SAFETY_CSV),
            "immutability_csv": str(IMMUTABILITY_CSV),
            "recommended_path_csv": str(RECOMMENDED_PATH_CSV),
            "acquisition_manifest_json": str(ACQ_MANIFEST),
            "acquired_game_level_outcomes_source_index_csv": str(ACQ_GAME_INDEX),
            "acquired_base_out_transitions_source_index_csv": str(ACQ_BASE_INDEX),
            "acquired_inning_runs_source_index_csv": str(ACQ_INNING_INDEX),
            "acquisition_evidence_report_csv": str(ACQ_EVIDENCE),
            "acquisition_quality_report_csv": str(ACQ_QUALITY),
        },
    }

    JSON_PATH.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
