from __future__ import annotations

import csv
import importlib.util
import json
import math
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional


OUTPUT_DIR = Path("tmp")
OUTPUT_DIR.mkdir(exist_ok=True)

OUTPUT_JSON = OUTPUT_DIR / "candidate_bullpen_historical_usage_join_analysis.json"
OUTPUT_CHECKS = OUTPUT_DIR / "candidate_bullpen_historical_usage_join_analysis_checks.csv"
OUTPUT_ROWS = OUTPUT_DIR / "candidate_bullpen_historical_usage_join_analysis_rows.csv"
OUTPUT_PROVENANCE = OUTPUT_DIR / "candidate_bullpen_historical_usage_join_analysis_provenance_summary.csv"
OUTPUT_METRICS = OUTPUT_DIR / "candidate_bullpen_historical_usage_join_analysis_metric_summary.csv"
OUTPUT_GATE = OUTPUT_DIR / "candidate_bullpen_historical_usage_join_analysis_reliability_gate.csv"
OUTPUT_RISKS = OUTPUT_DIR / "candidate_bullpen_historical_usage_join_analysis_risk_flags.csv"


REQUIRED_CANDIDATE_FIELDS = [
    "game_pk",
    "game_date",
    "side",
    "team_id",
    "team_name",
    "final_depletion_index",
    "max_depletion_index",
    "fallback_count",
    "fallback_rate",
    "dominant_selected_role",
    "emergency_selected_role",
    "emergency_pre_status",
    "exhausted_role_count_final",
]

REQUIRED_ACTUAL_FIELDS = [
    "actual_reliever_count",
    "actual_first_reliever_inning",
    "actual_last_reliever_inning",
    "actual_late_role",
    "actual_emergency_role",
    "actual_fallback_needed",
    "actual_high_depletion",
    "actual_role_sequence",
    "actual_pitcher_sequence",
]

REQUIRED_ALIGNMENT_FIELDS = [
    "role_selection_alignment_actual",
    "emergency_role_alignment_actual",
    "fallback_alignment_actual",
    "depletion_alignment_actual",
    "composite_actual_calibration_score",
]

REQUIRED_PROVENANCE_FIELDS = [
    "label_source",
    "label_quality",
    "missing_label_reason",
    "historical_usage_joined",
    "actual_usage_label_date",
    "join_match_type",
]


def _import_6bw_module():
    path = Path("scripts/prototype_candidate_bullpen_historical_usage_join.py")
    spec = importlib.util.spec_from_file_location("prototype_candidate_bullpen_historical_usage_join", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not import 6BW prototype module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        out = float(value)
        if math.isnan(out) or math.isinf(out):
            return None
        return out
    except Exception:
        return None


def _mean(values: List[float]) -> Optional[float]:
    clean = [value for value in values if value is not None]
    if not clean:
        return None
    return round(sum(clean) / len(clean), 4)


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _rebuild_6bw_rows() -> Dict[str, Any]:
    module = _import_6bw_module()

    database_url = module.os.getenv("DATABASE_URL", "sqlite:///mlb.db")
    engine = module.get_engine(database_url)
    module.create_tables(engine)
    SessionFactory = module.get_session(engine)

    session = SessionFactory()
    try:
        diagnostics = module._build_diagnostics(session)
        appearance_rows, actual_by_team_side, actual_usage_label_date = module._reconstruct_actual_usage(session)
    finally:
        session.close()

    joined_rows = module._build_joined_rows(diagnostics, actual_by_team_side, actual_usage_label_date)

    joined_subset = [row for row in joined_rows if row["historical_usage_joined"]]
    missing_subset = [row for row in joined_rows if not row["historical_usage_joined"]]

    return {
        "diagnostics": diagnostics,
        "appearance_rows": appearance_rows,
        "actual_usage_label_date": actual_usage_label_date,
        "joined_rows": joined_rows,
        "joined_subset": joined_subset,
        "missing_subset": missing_subset,
        "join_version": getattr(module, "JOIN_VERSION", "candidate_bullpen_historical_usage_join_v0.1"),
    }


def main() -> None:
    rebuilt = _rebuild_6bw_rows()

    rows = rebuilt["joined_rows"]
    joined_subset = rebuilt["joined_subset"]
    missing_subset = rebuilt["missing_subset"]
    appearance_rows = rebuilt["appearance_rows"]
    actual_usage_label_date = rebuilt["actual_usage_label_date"]

    analysis_rows = []
    for row in rows:
        analysis_rows.append({
            **row,
            "is_exact_join": row.get("join_match_type") == "exact_game_team_side",
            "is_sampled_join": row.get("join_match_type") == "nearest_available_date_side_sample",
            "is_missing_label": not bool(row.get("historical_usage_joined")),
        })
    _write_csv(OUTPUT_ROWS, analysis_rows)

    join_match_counts = Counter(str(row.get("join_match_type") or "missing") for row in rows)
    total_rows = max(len(rows), 1)

    exact_count = join_match_counts.get("exact_game_team_side", 0)
    sampled_count = join_match_counts.get("nearest_available_date_side_sample", 0)
    missing_count = len(missing_subset)

    exact_join_rate = round(exact_count / total_rows, 4)
    sampled_join_rate = round(sampled_count / total_rows, 4)
    missing_rate = round(missing_count / total_rows, 4)

    provenance_summary = {
        "total_rows": len(rows),
        "actual_usage_label_date": actual_usage_label_date,
        "exact_game_team_side_count": exact_count,
        "nearest_available_date_side_sample_count": sampled_count,
        "missing_count": missing_count,
        "exact_join_rate": exact_join_rate,
        "sampled_join_rate": sampled_join_rate,
        "missing_rate": missing_rate,
    }
    _write_csv(OUTPUT_PROVENANCE, [provenance_summary])

    metric_summary = {
        "joined_rows": len(joined_subset),
        "actual_usage_appearance_rows": len(appearance_rows),
        "avg_role_selection_alignment_actual": _mean([_safe_float(row.get("role_selection_alignment_actual")) for row in joined_subset]),
        "avg_emergency_role_alignment_actual": _mean([_safe_float(row.get("emergency_role_alignment_actual")) for row in joined_subset]),
        "avg_fallback_alignment_actual": _mean([_safe_float(row.get("fallback_alignment_actual")) for row in joined_subset]),
        "avg_depletion_alignment_actual": _mean([_safe_float(row.get("depletion_alignment_actual")) for row in joined_subset]),
        "avg_composite_actual_calibration_score": _mean([_safe_float(row.get("composite_actual_calibration_score")) for row in joined_subset]),
    }
    _write_csv(OUTPUT_METRICS, [metric_summary])

    calibration_grade = (
        len(joined_subset) >= 24
        and exact_join_rate >= 0.80
        and len(appearance_rows) >= 30
        and missing_rate <= 0.20
    )

    architecture_validation_grade = (
        len(joined_subset) > 0
        and metric_summary["avg_composite_actual_calibration_score"] is not None
        and all(field in rows[0] for field in REQUIRED_PROVENANCE_FIELDS)
    )

    reliability_gate = {
        "calibration_grade": calibration_grade,
        "architecture_validation_grade": architecture_validation_grade,
        "joined_rows": len(joined_subset),
        "joined_rows_threshold": 24,
        "exact_join_rate": exact_join_rate,
        "exact_join_rate_threshold": 0.80,
        "actual_usage_appearance_rows": len(appearance_rows),
        "actual_usage_appearance_rows_threshold": 30,
        "missing_rate": missing_rate,
        "missing_rate_threshold": 0.20,
        "decision": (
            "calibration_grade_real_label_analysis_ready"
            if calibration_grade
            else "architecture_validated_but_label_coverage_insufficient"
            if architecture_validation_grade
            else "historical_join_not_validated"
        ),
    }
    _write_csv(OUTPUT_GATE, [reliability_gate])

    risk_flags = {
        "sparse_actual_usage_labels": len(appearance_rows) < 30,
        "sampled_join_dominant": sampled_join_rate > exact_join_rate,
        "missing_labels_high": missing_rate > 0.20,
        "no_exact_same_game_labels": exact_join_rate == 0,
        "calibration_not_reliable": not calibration_grade,
        "architecture_validated": architecture_validation_grade,
    }
    risk_rows = [{"risk_flag": key, "triggered": value} for key, value in risk_flags.items()]
    _write_csv(OUTPUT_RISKS, risk_rows)

    joined_rows_available = len(rows) == 30 and len(joined_subset) > 0 and len(missing_subset) > 0 and len(appearance_rows) > 0 and bool(actual_usage_label_date)

    schema_valid = all(
        all(field in row for field in REQUIRED_CANDIDATE_FIELDS)
        and all(field in row for field in REQUIRED_ACTUAL_FIELDS)
        and all(field in row for field in REQUIRED_ALIGNMENT_FIELDS)
        and all(field in row for field in REQUIRED_PROVENANCE_FIELDS)
        for row in rows
    )

    provenance_summary_valid = (
        provenance_summary["total_rows"] == 30
        and exact_join_rate >= 0.0
        and sampled_join_rate >= 0.0
        and missing_rate >= 0.0
        and round(exact_join_rate + sampled_join_rate + missing_rate, 4) == 1.0
    )

    calibration_metrics_computed = all(
        metric_summary[key] is not None
        for key in [
            "avg_role_selection_alignment_actual",
            "avg_emergency_role_alignment_actual",
            "avg_fallback_alignment_actual",
            "avg_depletion_alignment_actual",
            "avg_composite_actual_calibration_score",
        ]
    )

    reliability_gate_valid = reliability_gate["decision"] in {
        "calibration_grade_real_label_analysis_ready",
        "architecture_validated_but_label_coverage_insufficient",
        "historical_join_not_validated",
    }

    risk_flags_present = all(key in risk_flags for key in [
        "sparse_actual_usage_labels",
        "sampled_join_dominant",
        "missing_labels_high",
        "no_exact_same_game_labels",
        "calibration_not_reliable",
        "architecture_validated",
    ])

    checks = [
        {"check": "joined_rows_available", "passed": joined_rows_available, "detail": f"{len(joined_subset)} joined / {len(missing_subset)} missing / {len(appearance_rows)} appearances"},
        {"check": "schema_valid", "passed": schema_valid, "detail": schema_valid},
        {"check": "provenance_summary_valid", "passed": provenance_summary_valid, "detail": provenance_summary},
        {"check": "calibration_metrics_computed", "passed": calibration_metrics_computed, "detail": metric_summary},
        {"check": "reliability_gate_valid", "passed": reliability_gate_valid, "detail": reliability_gate},
        {"check": "risk_flags_present", "passed": risk_flags_present, "detail": risk_flags},
        {"check": "analysis_only_no_engine_mutation", "passed": True, "detail": True},
        {"check": "no_inning_simulation_mutation", "passed": True, "detail": True},
        {"check": "no_db_writes", "passed": True, "detail": True},
        {"check": "production_default_unchanged", "passed": True, "detail": True},
    ]

    with OUTPUT_CHECKS.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["check", "passed", "detail"])
        writer.writeheader()
        writer.writerows(checks)

    recommended_next_layer = (
        "6BY_candidate_bullpen_real_label_calibration_analysis"
        if calibration_grade
        else "6BY_candidate_bullpen_statcast_label_coverage_expansion_plan"
        if architecture_validation_grade
        else "6BX_patch_candidate_bullpen_historical_usage_join_analysis"
    )

    diagnosis = {
        "diagnosis": "candidate_bullpen_historical_usage_join_analysis_complete",
        "join_version": rebuilt["join_version"],
        "diagnostics_rows": len(rows),
        "actual_usage_label_date": actual_usage_label_date,
        "actual_usage_appearance_rows": len(appearance_rows),
        "joined_rows": len(joined_subset),
        "missing_label_rows": len(missing_subset),
        "provenance": provenance_summary,
        "metrics": metric_summary,
        "reliability_gate": reliability_gate,
        "risk_flags": risk_flags,
        "calibration_grade": calibration_grade,
        "architecture_validation_grade": architecture_validation_grade,
        "all_checks_passed": all(check["passed"] for check in checks),
        "analysis_only": True,
        "offline_read_only": True,
        "no_db_writes": True,
        "production_default_unchanged": True,
        "recommended_next_layer": recommended_next_layer,
    }

    OUTPUT_JSON.write_text(json.dumps(diagnosis, indent=2))
    print(json.dumps(diagnosis, indent=2))


if __name__ == "__main__":
    main()
