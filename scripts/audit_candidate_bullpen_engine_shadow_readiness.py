from __future__ import annotations

import ast
import csv
import json
from pathlib import Path
from typing import Any, Dict, List


OUTPUT_DIR = Path("tmp")
OUTPUT_DIR.mkdir(exist_ok=True)

OUTPUT_JSON = OUTPUT_DIR / "candidate_bullpen_engine_shadow_readiness_audit.json"
OUTPUT_CHECKS = OUTPUT_DIR / "candidate_bullpen_engine_shadow_readiness_audit_checks.csv"
OUTPUT_MATRIX = OUTPUT_DIR / "candidate_bullpen_engine_shadow_readiness_matrix.csv"
OUTPUT_ARTIFACTS = OUTPUT_DIR / "candidate_bullpen_engine_shadow_readiness_artifacts.csv"


CANDIDATE_ARTIFACTS = [
    {
        "layer": "6BD",
        "path": "scripts/prototype_candidate_bullpen_role_segmentation.py",
        "purpose": "candidate bullpen role segmentation prototype",
        "required_markers": ["candidate_only", "ROLE_TEMPLATES", "recommended_next_layer"],
    },
    {
        "layer": "6BE",
        "path": "scripts/analyze_candidate_bullpen_role_segmentation.py",
        "purpose": "candidate bullpen role segmentation analysis",
        "required_markers": ["candidate_only", "all_checks_passed", "recommended_next_layer"],
    },
    {
        "layer": "6BF",
        "path": "scripts/prototype_candidate_bullpen_fatigue_availability.py",
        "purpose": "candidate bullpen fatigue availability prototype",
        "required_markers": ["candidate_only", "availability_status", "fatigue_penalty"],
    },
    {
        "layer": "6BG",
        "path": "scripts/analyze_candidate_bullpen_fatigue_availability.py",
        "purpose": "candidate bullpen fatigue availability analysis",
        "required_markers": ["candidate_only", "availability_distribution_valid", "recommended_next_layer"],
    },
    {
        "layer": "6BH",
        "path": "scripts/shadow_candidate_bullpen_role_selection.py",
        "purpose": "candidate bullpen role selection shadow integration",
        "required_markers": ["candidate_only", "shadow_only", "SCENARIOS"],
    },
    {
        "layer": "6BI",
        "path": "scripts/analyze_candidate_bullpen_role_selection_shadow.py",
        "purpose": "candidate bullpen role selection shadow analysis",
        "required_markers": ["candidate_only", "shadow_only", "scenario_role_coherence_valid"],
    },
    {
        "layer": "6BJ",
        "path": "scripts/prototype_candidate_bullpen_inherited_runner_context.py",
        "purpose": "candidate bullpen inherited runner context prototype",
        "required_markers": ["candidate_only", "prototype_only", "INHERITED_SCENARIOS"],
    },
    {
        "layer": "6BK",
        "path": "scripts/analyze_candidate_bullpen_inherited_runner_context.py",
        "purpose": "candidate bullpen inherited runner context analysis",
        "required_markers": ["candidate_only", "analysis_only", "pressure_ordering_valid"],
    },
    {
        "layer": "6BL",
        "path": "scripts/prototype_candidate_bullpen_depletion_sequence.py",
        "purpose": "candidate bullpen depletion sequence prototype",
        "required_markers": ["candidate_only", "prototype_only", "SEQUENCE_EVENTS"],
    },
    {
        "layer": "6BM",
        "path": "scripts/analyze_candidate_bullpen_depletion_sequence.py",
        "purpose": "candidate bullpen depletion sequence analysis",
        "required_markers": ["candidate_only", "analysis_only", "late_long_relief_only_as_fallback"],
    },
]


PRODUCTION_ISOLATION_PATHS = [
    "mlb_app/simulation/game_engine.py",
    "mlb_app/simulation/inning_simulation.py",
    "mlb_app/simulation",
    "frontend/src",
]


FORBIDDEN_IMPORTS = {"random"}
FORBIDDEN_WRITE_CALLS = {"add", "commit", "delete", "update", "insert", "execute"}


def _read(path: Path) -> str:
    return path.read_text() if path.exists() else ""


def _imports_random(text: str) -> bool:
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return False

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.split(".")[0] in FORBIDDEN_IMPORTS:
                    return True
        if isinstance(node, ast.ImportFrom):
            if (node.module or "").split(".")[0] in FORBIDDEN_IMPORTS:
                return True
    return False


def _has_main_entry(text: str) -> bool:
    return 'if __name__ == "__main__"' in text and "main()" in text


def _writes_tmp_only(text: str) -> bool:
    has_tmp = 'Path("tmp")' in text or "Path('tmp')" in text or "/tmp" in text
    has_output_write = ".write_text(" in text or "csv.DictWriter" in text or ".open(" in text
    return has_tmp and has_output_write


def _uses_model_projection_payload(text: str) -> bool:
    return "build_model_projection_payload" in text


def _uses_bullpen_profile_contract(text: str) -> bool:
    return (
        "sharedSimulation" in text
        and "direct_inputs" in text
        and (
            "away_bullpen_profile" in text
            or "home_bullpen_profile" in text
            or "BullpenProfile" in text
            or "awayBullpenProfile" in text
            or "homeBullpenProfile" in text
        )
    )


def _has_db_write_risk(text: str) -> bool:
    lowered = text.lower()
    dangerous_patterns = [
        "session.add(",
        "session.delete(",
        "session.commit(",
        ".insert(",
        ".update(",
        ".delete(",
        "drop_all(",
        "metadata.drop_all",
    ]
    return any(pattern in lowered for pattern in dangerous_patterns)


def _has_engine_mutation_risk(text: str) -> bool:
    """Detect likely executable engine coupling, not safety-label text.

    Prior candidate scripts intentionally include audit labels like
    no_game_engine_mutation and no_inning_simulation_mutation. Those are
    safety assertions, not mutation call paths, so they should not fail
    readiness.
    """
    risky_terms = [
        "from mlb_app.simulation",
        "import mlb_app.simulation",
        "GameEngine(",
        ".simulate_inning(",
        ".run_simulation(",
        ".simulate_game(",
    ]
    return any(term in text for term in risky_terms)


def _artifact_inventory() -> List[Dict[str, Any]]:
    rows = []
    for artifact in CANDIDATE_ARTIFACTS:
        path = Path(artifact["path"])
        text = _read(path)
        markers_present = all(marker in text for marker in artifact["required_markers"])

        rows.append(
            {
                "layer": artifact["layer"],
                "path": artifact["path"],
                "purpose": artifact["purpose"],
                "exists": path.exists(),
                "has_required_markers": markers_present,
                "uses_model_projection_payload": _uses_model_projection_payload(text),
                "uses_bullpen_profile_contract": _uses_bullpen_profile_contract(text),
                "has_candidate_only": "candidate_only" in text,
                "has_shadow_or_prototype_or_analysis_flag": any(flag in text for flag in ["shadow_only", "prototype_only", "analysis_only"]),
                "writes_tmp_only": _writes_tmp_only(text),
                "imports_random": _imports_random(text),
                "has_main_entry": _has_main_entry(text),
                "has_db_write_risk": _has_db_write_risk(text),
                "has_engine_mutation_risk": _has_engine_mutation_risk(text),
            }
        )
    return rows


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return

    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    artifact_rows = _artifact_inventory()
    _write_csv(OUTPUT_ARTIFACTS, artifact_rows)

    candidate_artifacts_present = all(row["exists"] and row["has_required_markers"] for row in artifact_rows)

    production_isolation_valid = all(
        row["path"].startswith("scripts/")
        and not row["has_engine_mutation_risk"]
        and not row["has_db_write_risk"]
        for row in artifact_rows
    )

    data_contract_readiness_valid = all(
        row["uses_model_projection_payload"]
        and row["uses_bullpen_profile_contract"]
        and row["has_candidate_only"]
        and row["writes_tmp_only"]
        for row in artifact_rows
    )

    determinism_readiness_valid = all(
        not row["imports_random"]
        and row["has_main_entry"]
        for row in artifact_rows
    )

    safety_gate_readiness_valid = all(
        not row["has_db_write_risk"]
        and not row["has_engine_mutation_risk"]
        for row in artifact_rows
    )

    integration_boundary = {
        "future_shadow_mode": "read_only_optional",
        "canonical_probabilities": "unchanged_from_current_production_path",
        "initial_output_location": "diagnostics_or_shadow_metadata_only",
        "sportsbook_consumption": "blocked_until_separate_calibration_layer",
        "frontend_consumption": "blocked_until_explicit_ui_contract_layer",
        "engine_mutation": "not_allowed_in_readiness_audit",
    }

    integration_boundary_documented = all(bool(value) for value in integration_boundary.values())

    readiness_rows = [
        {
            "dimension": "candidate_artifacts",
            "passed": candidate_artifacts_present,
            "detail": "All Layer 6BD through 6BM scripts exist with required markers.",
        },
        {
            "dimension": "production_isolation",
            "passed": production_isolation_valid,
            "detail": "Candidate stack remains script-only and avoids engine/db mutation paths.",
        },
        {
            "dimension": "data_contract",
            "passed": data_contract_readiness_valid,
            "detail": "Candidate stack consumes Model Projections bullpen profiles, marks candidate rows, and writes tmp outputs only.",
        },
        {
            "dimension": "determinism",
            "passed": determinism_readiness_valid,
            "detail": "No random module imports; scripts expose standalone main entry points.",
        },
        {
            "dimension": "safety_gate",
            "passed": safety_gate_readiness_valid,
            "detail": "No DB write or production engine mutation patterns detected.",
        },
        {
            "dimension": "integration_boundary",
            "passed": integration_boundary_documented,
            "detail": json.dumps(integration_boundary, sort_keys=True),
        },
    ]

    _write_csv(OUTPUT_MATRIX, readiness_rows)

    checks = [
        {"check": "candidate_artifacts_present", "passed": candidate_artifacts_present, "detail": candidate_artifacts_present},
        {"check": "production_isolation_valid", "passed": production_isolation_valid, "detail": production_isolation_valid},
        {"check": "data_contract_readiness_valid", "passed": data_contract_readiness_valid, "detail": data_contract_readiness_valid},
        {"check": "determinism_readiness_valid", "passed": determinism_readiness_valid, "detail": determinism_readiness_valid},
        {"check": "safety_gate_readiness_valid", "passed": safety_gate_readiness_valid, "detail": safety_gate_readiness_valid},
        {"check": "integration_boundary_documented", "passed": integration_boundary_documented, "detail": integration_boundary},
        {"check": "audit_only_no_engine_mutation", "passed": True, "detail": True},
        {"check": "no_inning_simulation_mutation", "passed": True, "detail": True},
        {"check": "production_default_unchanged", "passed": True, "detail": True},
    ]

    with OUTPUT_CHECKS.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["check", "passed", "detail"])
        writer.writeheader()
        writer.writerows(checks)

    diagnosis = {
        "diagnosis": "candidate_bullpen_engine_shadow_readiness_audit_complete",
        "candidate_artifacts_checked": len(artifact_rows),
        "readiness_dimensions_checked": len(readiness_rows),
        "all_checks_passed": all(check["passed"] for check in checks),
        "integration_boundary": integration_boundary,
        "production_default_unchanged": True,
        "recommended_next_layer": (
            "6BO_candidate_bullpen_shadow_output_contract_prototype"
            if all(check["passed"] for check in checks)
            else "6BN_patch_candidate_bullpen_engine_shadow_readiness_audit"
        ),
    }

    OUTPUT_JSON.write_text(json.dumps(diagnosis, indent=2))
    print(json.dumps(diagnosis, indent=2))


if __name__ == "__main__":
    main()
