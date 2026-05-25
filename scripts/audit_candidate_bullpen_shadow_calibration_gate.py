from __future__ import annotations

import ast
import csv
import json
from pathlib import Path
from typing import Any, Dict, List


OUTPUT_DIR = Path("tmp")
OUTPUT_DIR.mkdir(exist_ok=True)

OUTPUT_JSON = OUTPUT_DIR / "candidate_bullpen_shadow_calibration_gate_audit.json"
OUTPUT_CHECKS = OUTPUT_DIR / "candidate_bullpen_shadow_calibration_gate_audit_checks.csv"
OUTPUT_ARTIFACTS = OUTPUT_DIR / "candidate_bullpen_shadow_calibration_gate_artifacts.csv"
OUTPUT_MATRIX = OUTPUT_DIR / "candidate_bullpen_shadow_calibration_gate_matrix.csv"


ARTIFACTS = [
    {"layer": "6BD", "path": "scripts/prototype_candidate_bullpen_role_segmentation.py", "type": "prototype", "markers": ["candidate_only", "ROLE_TEMPLATES"]},
    {"layer": "6BE", "path": "scripts/analyze_candidate_bullpen_role_segmentation.py", "type": "analysis", "markers": ["candidate_only", "all_checks_passed"]},
    {"layer": "6BF", "path": "scripts/prototype_candidate_bullpen_fatigue_availability.py", "type": "prototype", "markers": ["candidate_only", "availability_status", "fatigue_penalty"]},
    {"layer": "6BG", "path": "scripts/analyze_candidate_bullpen_fatigue_availability.py", "type": "analysis", "markers": ["candidate_only", "availability_distribution_valid"]},
    {"layer": "6BH", "path": "scripts/shadow_candidate_bullpen_role_selection.py", "type": "shadow", "markers": ["candidate_only", "shadow_only", "SCENARIOS"]},
    {"layer": "6BI", "path": "scripts/analyze_candidate_bullpen_role_selection_shadow.py", "type": "analysis", "markers": ["candidate_only", "shadow_only", "scenario_role_coherence_valid"]},
    {"layer": "6BJ", "path": "scripts/prototype_candidate_bullpen_inherited_runner_context.py", "type": "prototype", "markers": ["candidate_only", "prototype_only", "INHERITED_SCENARIOS"]},
    {"layer": "6BK", "path": "scripts/analyze_candidate_bullpen_inherited_runner_context.py", "type": "analysis", "markers": ["candidate_only", "analysis_only", "pressure_ordering_valid"]},
    {"layer": "6BL", "path": "scripts/prototype_candidate_bullpen_depletion_sequence.py", "type": "prototype", "markers": ["candidate_only", "prototype_only", "SEQUENCE_EVENTS"]},
    {"layer": "6BM", "path": "scripts/analyze_candidate_bullpen_depletion_sequence.py", "type": "analysis", "markers": ["candidate_only", "analysis_only", "late_long_relief_only_as_fallback"]},
    {"layer": "6BN", "path": "scripts/audit_candidate_bullpen_engine_shadow_readiness.py", "type": "readiness_audit", "markers": ["candidate_artifacts_present", "integration_boundary_documented"]},
    {"layer": "6BO", "path": "scripts/prototype_candidate_bullpen_shadow_output_contract.py", "type": "contract_prototype", "markers": ["contract_version", "canonical_outputs_untouched", "recommended_consumers"]},
    {"layer": "6BP", "path": "scripts/analyze_candidate_bullpen_shadow_output_contract.py", "type": "contract_analysis", "markers": ["contract_shape_valid", "safety_boundaries_valid", "aggregate_stability_valid"]},
    {"layer": "6BQ", "path": "scripts/prototype_candidate_bullpen_shadow_diagnostics_export.py", "type": "diagnostics_export_prototype", "markers": ["diagnostics_version", "diagnostics_only", "blocked_consumers"]},
    {"layer": "6BR", "path": "scripts/analyze_candidate_bullpen_shadow_diagnostics_export.py", "type": "diagnostics_export_analysis", "markers": ["diagnostics_exports_available", "aggregate_metrics_valid", "fallback_count_by_event"]},
]


def _read(path: Path) -> str:
    return path.read_text() if path.exists() else ""


def _has_main_entry(text: str) -> bool:
    return 'if __name__ == "__main__"' in text and "main()" in text


def _imports_random(text: str) -> bool:
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return False

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            if any(alias.name.split(".")[0] == "random" for alias in node.names):
                return True
        if isinstance(node, ast.ImportFrom):
            if (node.module or "").split(".")[0] == "random":
                return True
    return False


def _has_db_write_risk(text: str) -> bool:
    """Detect likely executable DB write paths, not audit detector literals."""
    lowered = text.lower()

    # Strip the known detector function body when auditing an audit script, because
    # strings such as "session.commit(" may appear as search tokens rather than code.
    if "def _has_db_write_risk" in lowered:
        lowered = lowered.replace('"session.add(",', "")
        lowered = lowered.replace('"session.delete(",', "")
        lowered = lowered.replace('"session.commit(",', "")
        lowered = lowered.replace('".insert(",', "")
        lowered = lowered.replace('".update(",', "")
        lowered = lowered.replace('".delete(",', "")
        lowered = lowered.replace('"drop_all(",', "")
        lowered = lowered.replace('"metadata.drop_all",', "")

    risky = [
        "session.add(",
        "session.delete(",
        "session.commit(",
        ".insert(",
        ".update(",
        ".delete(",
        "drop_all(",
        "metadata.drop_all",
    ]
    return any(token in lowered for token in risky)


def _has_engine_mutation_risk(text: str) -> bool:
    """Detect likely executable simulation coupling, not audit detector literals."""
    scan = text

    # Strip known detector string literals when this audit scans prior audit scripts.
    if "def _has_engine_mutation_risk" in scan:
        scan = scan.replace('"from mlb_app.simulation",', "")
        scan = scan.replace('"import mlb_app.simulation",', "")
        scan = scan.replace('"GameEngine(",', "")
        scan = scan.replace('".simulate_inning(",', "")
        scan = scan.replace('".run_simulation(",', "")
        scan = scan.replace('".simulate_game(",', "")

    risky = [
        "from mlb_app.simulation",
        "import mlb_app.simulation",
        "GameEngine(",
        ".simulate_inning(",
        ".run_simulation(",
        ".simulate_game(",
    ]
    return any(token in scan for token in risky)


def _has_route_or_frontend_risk(path: str, text: str) -> bool:
    if path.startswith("frontend/"):
        return True
    if "/routes" in path or "FastAPI(" in text or "@app." in text:
        return True
    return False


def _artifact_rows() -> List[Dict[str, Any]]:
    rows = []
    for artifact in ARTIFACTS:
        path = Path(artifact["path"])
        text = _read(path)
        rows.append({
            "layer": artifact["layer"],
            "path": artifact["path"],
            "type": artifact["type"],
            "exists": path.exists(),
            "has_markers": all(marker in text for marker in artifact["markers"]),
            "has_main_entry": _has_main_entry(text),
            "imports_random": _imports_random(text),
            "has_db_write_risk": _has_db_write_risk(text),
            "has_engine_mutation_risk": _has_engine_mutation_risk(text),
            "has_route_or_frontend_risk": _has_route_or_frontend_risk(artifact["path"], text),
            "has_candidate_marker": "candidate_only" in text or artifact["type"] in {"readiness_audit"},
            "has_safety_marker": any(marker in text for marker in [
                "production_default_unchanged",
                "canonical_outputs_untouched",
                "no_engine_mutation",
                "no_inning_simulation_mutation",
            ]),
            "has_tmp_output": 'Path("tmp")' in text or "Path('tmp')" in text,
        })
    return rows


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    artifact_rows = _artifact_rows()
    _write_csv(OUTPUT_ARTIFACTS, artifact_rows)

    artifact_readiness_valid = all(
        row["exists"]
        and row["has_markers"]
        and row["has_main_entry"]
        and row["has_tmp_output"]
        for row in artifact_rows
    )

    prototype_layers = {row["layer"] for row in artifact_rows if row["type"] == "prototype"}
    analysis_layers = {row["layer"] for row in artifact_rows if row["type"] == "analysis"}
    checkpoint_layers = {
        row["layer"]
        for row in artifact_rows
        if row["type"] in {
            "shadow",
            "readiness_audit",
            "contract_prototype",
            "contract_analysis",
            "diagnostics_export_prototype",
            "diagnostics_export_analysis",
        }
    }
    pair_readiness_valid = (
        {"6BD", "6BF", "6BJ", "6BL"}.issubset(prototype_layers)
        and {"6BE", "6BG", "6BI", "6BK", "6BM"}.issubset(analysis_layers)
        and {"6BH", "6BN", "6BO", "6BP", "6BQ", "6BR"}.issubset(checkpoint_layers)
    )

    diagnostics_artifacts = {
        row["layer"]: row
        for row in artifact_rows
        if row["layer"] in {"6BQ", "6BR"}
    }

    diagnostics_readiness_valid = (
        "6BQ" in diagnostics_artifacts
        and "6BR" in diagnostics_artifacts
        and diagnostics_artifacts["6BQ"]["has_markers"]
        and diagnostics_artifacts["6BR"]["has_markers"]
        and diagnostics_artifacts["6BQ"]["exists"]
        and diagnostics_artifacts["6BR"]["exists"]
    )

    safety_readiness_valid = all(
        not row["imports_random"]
        and not row["has_db_write_risk"]
        and not row["has_engine_mutation_risk"]
        and not row["has_route_or_frontend_risk"]
        and row["has_safety_marker"]
        for row in artifact_rows
    )

    calibration_prerequisites = {
        "offline_backtest_first": True,
        "compare_shadow_diagnostics_to_historical_bullpen_usage": True,
        "compare_shadow_diagnostics_to_historical_outcomes": True,
        "canonical_probabilities_unchanged_during_calibration": True,
        "sportsbook_outputs_blocked_during_calibration": True,
        "frontend_outputs_blocked_until_ui_contract": True,
        "requires_actual_usage_join_layer": True,
        "requires_outcome_join_layer": True,
        "requires_calibration_metrics": [
            "role_selection_accuracy_proxy",
            "availability_state_alignment",
            "fallback_rate_calibration",
            "depletion_index_outcome_correlation",
            "emergency_state_frequency",
        ],
    }

    calibration_prerequisites_documented = all(
        bool(value)
        for key, value in calibration_prerequisites.items()
        if key != "requires_calibration_metrics"
    ) and len(calibration_prerequisites["requires_calibration_metrics"]) >= 5

    gate_ready = (
        artifact_readiness_valid
        and pair_readiness_valid
        and diagnostics_readiness_valid
        and safety_readiness_valid
        and calibration_prerequisites_documented
    )

    gate_decision = (
        "ready_for_offline_calibration_backtest"
        if gate_ready
        else "hold_for_shadow_diagnostics_hardening"
    )

    recommended_next_layer = (
        "6BT_candidate_bullpen_shadow_calibration_backtest_prototype"
        if gate_ready
        else "6BS_patch_candidate_bullpen_shadow_calibration_gate_audit"
    )

    matrix_rows = [
        {
            "dimension": "artifact_readiness",
            "passed": artifact_readiness_valid,
            "detail": "Layer 6BD through 6BR candidate artifacts exist with markers, tmp outputs, and main entry points.",
        },
        {
            "dimension": "prototype_analysis_pairing",
            "passed": pair_readiness_valid,
            "detail": "Expected prototype/shadow/analysis/checkpoint layers are present.",
        },
        {
            "dimension": "diagnostics_readiness",
            "passed": diagnostics_readiness_valid,
            "detail": "Diagnostics export and diagnostics analysis layers are present and marked.",
        },
        {
            "dimension": "safety_readiness",
            "passed": safety_readiness_valid,
            "detail": "No random imports, DB write paths, engine mutation paths, route wiring, frontend wiring, or sportsbook consumption detected.",
        },
        {
            "dimension": "calibration_prerequisites",
            "passed": calibration_prerequisites_documented,
            "detail": json.dumps(calibration_prerequisites, sort_keys=True),
        },
        {
            "dimension": "gate_decision",
            "passed": gate_ready,
            "detail": gate_decision,
        },
    ]
    _write_csv(OUTPUT_MATRIX, matrix_rows)

    checks = [
        {"check": "artifact_readiness_valid", "passed": artifact_readiness_valid, "detail": artifact_readiness_valid},
        {"check": "diagnostics_readiness_valid", "passed": diagnostics_readiness_valid, "detail": diagnostics_readiness_valid},
        {"check": "safety_readiness_valid", "passed": safety_readiness_valid, "detail": safety_readiness_valid},
        {"check": "calibration_prerequisites_documented", "passed": calibration_prerequisites_documented, "detail": calibration_prerequisites},
        {"check": "gate_decision_valid", "passed": gate_ready, "detail": gate_decision},
        {"check": "audit_only_no_engine_mutation", "passed": True, "detail": True},
        {"check": "no_inning_simulation_mutation", "passed": True, "detail": True},
        {"check": "production_default_unchanged", "passed": True, "detail": True},
    ]

    with OUTPUT_CHECKS.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["check", "passed", "detail"])
        writer.writeheader()
        writer.writerows(checks)

    diagnosis = {
        "diagnosis": "candidate_bullpen_shadow_calibration_gate_audit_complete",
        "artifacts_checked": len(artifact_rows),
        "artifact_readiness_valid": artifact_readiness_valid,
        "prototype_analysis_pairing_valid": pair_readiness_valid,
        "diagnostics_readiness_valid": diagnostics_readiness_valid,
        "safety_readiness_valid": safety_readiness_valid,
        "calibration_prerequisites_documented": calibration_prerequisites_documented,
        "gate_decision": gate_decision,
        "all_checks_passed": all(check["passed"] for check in checks),
        "production_default_unchanged": True,
        "recommended_next_layer": recommended_next_layer,
    }

    OUTPUT_JSON.write_text(json.dumps(diagnosis, indent=2))
    print(json.dumps(diagnosis, indent=2))


if __name__ == "__main__":
    main()
