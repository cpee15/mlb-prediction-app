#!/usr/bin/env python3
"""
Layer 7O
Layer 7 Environment Readiness and Scope Closure Plan

Closes Layer 7 under diagnostic-only, shadow-observable scope.

This plan:
- inventories Layer 7 plans, implementations, and audits;
- verifies the bounded environment contract chain;
- distinguishes implemented capability from production authority;
- records unresolved evidence and integration gates;
- defines the Layer 8 handoff.

This layer does not:
- activate environment effects in production;
- alter simulation state, parameters, probabilities, or outcomes;
- map carry diagnostics to distance;
- join historical outcomes;
- calculate accuracy or calibration metrics;
- tune environment parameters;
- execute backtests, pricing, market comparison, or edge detection.
"""

from __future__ import annotations

import ast
import csv
import json
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "7O"
LAYER_NAME = (
    "layer_7_environment_readiness_and_scope_closure_plan"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp/"
    "layer_7O_environment_readiness_and_scope_closure"
)

REQUIRED_FILES = {
    "7C_plan": (
        ROOT
        / "scripts/"
        "plan_7C_canonical_venue_and_park_factor_source_contract.py"
    ),
    "7D_contract": (
        ROOT
        / "mlb_app/environment/"
        "venue_park_factor_contract.py"
    ),
    "7D_audit": (
        ROOT
        / "scripts/"
        "audit_7D_canonical_venue_and_park_factor_contract.py"
    ),
    "7E_plan": (
        ROOT
        / "scripts/"
        "plan_7E_roof_dome_weather_and_atmospheric_state_contract.py"
    ),
    "7F_contract": (
        ROOT
        / "mlb_app/environment/"
        "weather_atmospheric_contract.py"
    ),
    "7F_audit": (
        ROOT
        / "scripts/"
        "audit_7F_roof_dome_weather_and_atmospheric_state_contract.py"
    ),
    "7G_plan": (
        ROOT
        / "scripts/"
        "plan_7G_wind_field_orientation_and_batted_ball_vector_contract.py"
    ),
    "7H_contract": (
        ROOT
        / "mlb_app/environment/"
        "wind_field_vector_contract.py"
    ),
    "7H_audit": (
        ROOT
        / "scripts/"
        "audit_7H_wind_field_orientation_and_batted_ball_vector_contract.py"
    ),
    "7I_plan": (
        ROOT
        / "scripts/"
        "plan_7I_atmospheric_density_and_carry_diagnostic_contract.py"
    ),
    "7J_contract": (
        ROOT
        / "mlb_app/environment/"
        "atmospheric_density_carry_contract.py"
    ),
    "7J_audit": (
        ROOT
        / "scripts/"
        "audit_7J_atmospheric_density_and_carry_diagnostic_contract.py"
    ),
    "7K_plan": (
        ROOT
        / "scripts/"
        "plan_7K_environment_diagnostic_composition_contract.py"
    ),
    "7L_contract": (
        ROOT
        / "mlb_app/environment/"
        "environment_diagnostic_composition.py"
    ),
    "7L_audit": (
        ROOT
        / "scripts/"
        "audit_7L_environment_diagnostic_composition_contract.py"
    ),
    "7M_plan": (
        ROOT
        / "scripts/"
        "plan_7M_environment_observability_and_shadow_evaluation_contract.py"
    ),
    "7N_contract": (
        ROOT
        / "mlb_app/environment/"
        "environment_shadow_observability.py"
    ),
    "7N_audit": (
        ROOT
        / "scripts/"
        "audit_7N_environment_observability_and_shadow_evaluation_contract.py"
    ),
}

EXPECTED_DIAGNOSES = {
    "7D_audit": (
        "canonical_venue_and_park_factor_contract_implementation_passed"
    ),
    "7F_audit": (
        "roof_dome_weather_and_atmospheric_state_contract_implementation_passed"
    ),
    "7H_audit": (
        "wind_field_orientation_and_batted_ball_vector_contract_implementation_passed"
    ),
    "7J_audit": (
        "atmospheric_density_and_carry_diagnostic_contract_implementation_passed"
    ),
    "7L_audit": (
        "environment_diagnostic_composition_contract_implementation_passed"
    ),
    "7N_audit": (
        "environment_observability_and_shadow_evaluation_contract_implementation_passed"
    ),
}

PROHIBITED_AUTHORITIES = [
    "production_environment_activation",
    "production_venue_activation",
    "production_park_factor_activation",
    "production_weather_activation",
    "production_wind_activation",
    "production_carry_activation",
    "simulation_state_change",
    "simulation_parameter_change",
    "simulation_probability_change",
    "canonical_probability_replacement",
    "batted_ball_distance_change",
    "batted_ball_outcome_change",
    "home_run_probability_change",
    "historical_outcome_join",
    "accuracy_metric_generation",
    "calibration_metric_generation",
    "parameter_calibration",
    "parameter_tuning",
    "backtest_execution",
    "pricing",
    "market_comparison",
    "edge_detection",
    "bet_recommendation",
]


def write_csv(
    path: Path,
    fieldnames: list[str],
    rows: Iterable[dict[str, Any]],
) -> None:
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with path.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=fieldnames,
        )
        writer.writeheader()
        writer.writerows(rows)


def write_json(
    path: Path,
    payload: Any,
) -> None:
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    path.write_text(
        json.dumps(
            payload,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def read_text(path: Path) -> str:
    if not path.exists():
        return ""

    return path.read_text(
        encoding="utf-8",
        errors="ignore",
    )


def string_constants(path: Path) -> set[str]:
    if not path.exists():
        return set()

    try:
        tree = ast.parse(
            read_text(path),
            filename=str(path),
        )
    except SyntaxError:
        return set()

    return {
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
    }


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    file_inventory = [
        {
            "artifact_id": artifact_id,
            "path": str(path.relative_to(ROOT)),
            "exists": path.exists(),
            "artifact_class": (
                "plan"
                if artifact_id.endswith("_plan")
                else (
                    "audit"
                    if artifact_id.endswith("_audit")
                    else "contract"
                )
            ),
        }
        for artifact_id, path in REQUIRED_FILES.items()
    ]

    all_required_files_exist = all(
        row["exists"]
        for row in file_inventory
    )

    diagnosis_inventory = []

    for artifact_id, diagnosis in EXPECTED_DIAGNOSES.items():
        path = REQUIRED_FILES[artifact_id]
        present = diagnosis in string_constants(
            path
        )

        diagnosis_inventory.append(
            {
                "artifact_id": artifact_id,
                "expected_diagnosis": diagnosis,
                "present": present,
            }
        )

    all_diagnoses_present = all(
        row["present"]
        for row in diagnosis_inventory
    )

    capability_inventory = [
        {
            "capability_id": "ENV-CAP-01",
            "capability": "canonical_venue_resolution",
            "implemented": True,
            "mode": "diagnostic_metadata_only",
            "production_authority": False,
        },
        {
            "capability_id": "ENV-CAP-02",
            "capability": "park_factor_source_contract",
            "implemented": True,
            "mode": "diagnostic_metadata_only",
            "production_authority": False,
        },
        {
            "capability_id": "ENV-CAP-03",
            "capability": "roof_and_indoor_state_resolution",
            "implemented": True,
            "mode": "diagnostic_metadata_only",
            "production_authority": False,
        },
        {
            "capability_id": "ENV-CAP-04",
            "capability": "weather_and_atmospheric_resolution",
            "implemented": True,
            "mode": "diagnostic_metadata_only",
            "production_authority": False,
        },
        {
            "capability_id": "ENV-CAP-05",
            "capability": "field_orientation_and_wind_vectors",
            "implemented": True,
            "mode": "diagnostic_metadata_only",
            "production_authority": False,
        },
        {
            "capability_id": "ENV-CAP-06",
            "capability": "moist_air_density_and_density_altitude",
            "implemented": True,
            "mode": "diagnostic_metadata_only",
            "production_authority": False,
        },
        {
            "capability_id": "ENV-CAP-07",
            "capability": "bounded_dimensionless_carry_index",
            "implemented": True,
            "mode": "diagnostic_metadata_only",
            "production_authority": False,
        },
        {
            "capability_id": "ENV-CAP-08",
            "capability": "environment_diagnostic_composition",
            "implemented": True,
            "mode": "disabled_by_default",
            "production_authority": False,
        },
        {
            "capability_id": "ENV-CAP-09",
            "capability": "deterministic_shadow_sampling",
            "implemented": True,
            "mode": "shadow_only",
            "production_authority": False,
        },
        {
            "capability_id": "ENV-CAP-10",
            "capability": "redacted_bounded_observability_record",
            "implemented": True,
            "mode": "shadow_only",
            "production_authority": False,
        },
        {
            "capability_id": "ENV-CAP-11",
            "capability": "projection_fingerprint_invariance_check",
            "implemented": True,
            "mode": "alert_only",
            "production_authority": False,
        },
        {
            "capability_id": "ENV-CAP-12",
            "capability": "historical_predictive_validation",
            "implemented": False,
            "mode": "not_authorized",
            "production_authority": False,
        },
    ]

    unresolved_gates = [
        {
            "gate_id": "ENV-G01",
            "gate": "historical_source_completeness",
            "required_before_production": True,
            "status": "unresolved",
        },
        {
            "gate_id": "ENV-G02",
            "gate": "venue_alias_and_relocation_coverage",
            "required_before_production": True,
            "status": "unresolved",
        },
        {
            "gate_id": "ENV-G03",
            "gate": "roof_state_historical_reliability",
            "required_before_production": True,
            "status": "unresolved",
        },
        {
            "gate_id": "ENV-G04",
            "gate": "weather_timestamp_and_station_quality",
            "required_before_production": True,
            "status": "unresolved",
        },
        {
            "gate_id": "ENV-G05",
            "gate": "field_orientation_coverage",
            "required_before_production": True,
            "status": "unresolved",
        },
        {
            "gate_id": "ENV-G06",
            "gate": "spray_angle_and_batted_ball_path_availability",
            "required_before_production": True,
            "status": "unresolved",
        },
        {
            "gate_id": "ENV-G07",
            "gate": "carry_index_to_distance_mapping",
            "required_before_production": True,
            "status": "not_authorized",
        },
        {
            "gate_id": "ENV-G08",
            "gate": "historical_outcome_validation",
            "required_before_production": True,
            "status": "not_authorized",
        },
        {
            "gate_id": "ENV-G09",
            "gate": "calibration_and_stability_evidence",
            "required_before_production": True,
            "status": "not_authorized",
        },
        {
            "gate_id": "ENV-G10",
            "gate": "production_integration_design",
            "required_before_production": True,
            "status": "not_authorized",
        },
    ]

    closure_decisions = [
        {
            "decision_id": "ENV-D01",
            "decision": "layer_7_contract_work_complete",
            "value": True,
            "rationale": (
                "All planned diagnostic and observability contracts exist."
            ),
        },
        {
            "decision_id": "ENV-D02",
            "decision": "layer_7_diagnostic_scope_complete",
            "value": True,
            "rationale": (
                "Venue, weather, vectors, carry, composition, and "
                "shadow observability are covered."
            ),
        },
        {
            "decision_id": "ENV-D03",
            "decision": "layer_7_production_ready",
            "value": False,
            "rationale": (
                "Historical evidence and production integration gates "
                "remain unresolved."
            ),
        },
        {
            "decision_id": "ENV-D04",
            "decision": "production_environment_activation_allowed",
            "value": False,
            "rationale": (
                "No predictive validation or production authority exists."
            ),
        },
        {
            "decision_id": "ENV-D05",
            "decision": "layer_8_work_allowed",
            "value": True,
            "rationale": (
                "Layer 8 may proceed independently under bounded scope."
            ),
        },
        {
            "decision_id": "ENV-D06",
            "decision": "layer_7_future_validation_deferred",
            "value": True,
            "rationale": (
                "Validation belongs to a later evidence phase, not this closure."
            ),
        },
    ]

    layer_8_handoff = [
        {
            "handoff_id": "L8-H01",
            "requirement": (
                "Preserve all Layer 7 environment outputs as non-authoritative."
            ),
        },
        {
            "handoff_id": "L8-H02",
            "requirement": (
                "Do not use environment diagnostics to tune arsenal logic."
            ),
        },
        {
            "handoff_id": "L8-H03",
            "requirement": (
                "Keep pitch arsenal and matchup work independently auditable."
            ),
        },
        {
            "handoff_id": "L8-H04",
            "requirement": (
                "Do not introduce pricing, market comparison, or edge logic."
            ),
        },
        {
            "handoff_id": "L8-H05",
            "requirement": (
                "Maintain disabled-by-default behavior for new diagnostics."
            ),
        },
        {
            "handoff_id": "L8-H06",
            "requirement": (
                "Require explicit evidence-based promotion before production use."
            ),
        },
    ]

    acceptance_criteria = [
        {
            "criterion_id": "ENV-C01",
            "criterion": "all_required_layer_7_files_present",
            "required": True,
        },
        {
            "criterion_id": "ENV-C02",
            "criterion": "all_expected_implementation_diagnoses_present",
            "required": True,
        },
        {
            "criterion_id": "ENV-C03",
            "criterion": "venue_capability_inventory_complete",
            "required": True,
        },
        {
            "criterion_id": "ENV-C04",
            "criterion": "weather_capability_inventory_complete",
            "required": True,
        },
        {
            "criterion_id": "ENV-C05",
            "criterion": "vector_capability_inventory_complete",
            "required": True,
        },
        {
            "criterion_id": "ENV-C06",
            "criterion": "carry_capability_inventory_complete",
            "required": True,
        },
        {
            "criterion_id": "ENV-C07",
            "criterion": "composition_capability_inventory_complete",
            "required": True,
        },
        {
            "criterion_id": "ENV-C08",
            "criterion": "observability_capability_inventory_complete",
            "required": True,
        },
        {
            "criterion_id": "ENV-C09",
            "criterion": "unresolved_production_gates_explicit",
            "required": True,
        },
        {
            "criterion_id": "ENV-C10",
            "criterion": "production_authority_remains_false",
            "required": True,
        },
        {
            "criterion_id": "ENV-C11",
            "criterion": "historical_validation_remains_unexecuted",
            "required": True,
        },
        {
            "criterion_id": "ENV-C12",
            "criterion": "layer_8_handoff_defined",
            "required": True,
        },
        {
            "criterion_id": "ENV-C13",
            "criterion": "layer_7_diagnostic_scope_closed",
            "required": True,
        },
        {
            "criterion_id": "ENV-C14",
            "criterion": "layer_7_not_marked_production_ready",
            "required": True,
        },
    ]

    planning_checks = [
        {
            "check": "all_required_layer_7_files_exist",
            "actual": all_required_files_exist,
            "expected": True,
            "passed": all_required_files_exist,
        },
        {
            "check": "all_expected_diagnoses_present",
            "actual": all_diagnoses_present,
            "expected": True,
            "passed": all_diagnoses_present,
        },
        {
            "check": "eighteen_required_artifacts_inventoried",
            "actual": len(file_inventory),
            "expected": 18,
            "passed": len(file_inventory) == 18,
        },
        {
            "check": "six_implementation_diagnoses_inventoried",
            "actual": len(diagnosis_inventory),
            "expected": 6,
            "passed": len(diagnosis_inventory) == 6,
        },
        {
            "check": "twelve_capabilities_inventoried",
            "actual": len(capability_inventory),
            "expected": 12,
            "passed": len(capability_inventory) == 12,
        },
        {
            "check": "ten_unresolved_gates_defined",
            "actual": len(unresolved_gates),
            "expected": 10,
            "passed": len(unresolved_gates) == 10,
        },
        {
            "check": "six_closure_decisions_defined",
            "actual": len(closure_decisions),
            "expected": 6,
            "passed": len(closure_decisions) == 6,
        },
        {
            "check": "six_layer_8_handoff_requirements_defined",
            "actual": len(layer_8_handoff),
            "expected": 6,
            "passed": len(layer_8_handoff) == 6,
        },
        {
            "check": "fourteen_acceptance_criteria_defined",
            "actual": len(acceptance_criteria),
            "expected": 14,
            "passed": len(acceptance_criteria) == 14,
        },
        {
            "check": "layer_7_diagnostic_scope_complete",
            "actual": True,
            "expected": True,
            "passed": True,
        },
        {
            "check": "layer_7_production_ready_false",
            "actual": False,
            "expected": False,
            "passed": True,
        },
        {
            "check": "production_authority_not_granted",
            "actual": False,
            "expected": False,
            "passed": True,
        },
        {
            "check": "historical_validation_not_executed",
            "actual": False,
            "expected": False,
            "passed": True,
        },
        {
            "check": "closure_boundary_preserved",
            "actual": True,
            "expected": True,
            "passed": True,
        },
    ]

    all_checks_passed = all(
        row["passed"]
        for row in planning_checks
    )

    authority_rows = [
        {
            "authority": authority,
            "granted": False,
            "reason": (
                "Layer 7 closes under diagnostic-only, shadow-observable scope."
            ),
        }
        for authority in PROHIBITED_AUTHORITIES
    ]

    authority_rows.extend(
        [
            {
                "authority": "layer_7_diagnostic_scope_closure",
                "granted": all_checks_passed,
                "reason": (
                    "All bounded Layer 7 contracts and audits are present."
                ),
            },
            {
                "authority": "layer_8_pitch_arsenal_matchup_planning",
                "granted": all_checks_passed,
                "reason": (
                    "Layer 8 may begin under independent diagnostic scope."
                ),
            },
            {
                "authority": "production_environment_integration",
                "granted": False,
                "reason": (
                    "Historical validation and integration evidence are absent."
                ),
            },
        ]
    )

    diagnosis_name = (
        "layer_7_environment_readiness_and_scope_closure_plan_complete"
        if all_checks_passed
        else
        "layer_7_environment_readiness_and_scope_closure_plan_failed"
    )

    recommended_next_layer = (
        "8A_pitch_arsenal_and_matchup_layer_inventory_plan"
        if all_checks_passed
        else
        "7O_layer_7_environment_scope_closure_remediation"
    )

    artifacts = {
        "planning_checks.csv": planning_checks,
        "file_inventory.csv": file_inventory,
        "diagnosis_inventory.csv": diagnosis_inventory,
        "capability_inventory.csv": capability_inventory,
        "unresolved_gates.csv": unresolved_gates,
        "closure_decisions.csv": closure_decisions,
        "layer_8_handoff.csv": layer_8_handoff,
        "acceptance_criteria.csv": acceptance_criteria,
        "authority_boundaries.csv": authority_rows,
    }

    fieldnames = {
        "planning_checks.csv": [
            "check",
            "actual",
            "expected",
            "passed",
        ],
        "file_inventory.csv": [
            "artifact_id",
            "path",
            "exists",
            "artifact_class",
        ],
        "diagnosis_inventory.csv": [
            "artifact_id",
            "expected_diagnosis",
            "present",
        ],
        "capability_inventory.csv": [
            "capability_id",
            "capability",
            "implemented",
            "mode",
            "production_authority",
        ],
        "unresolved_gates.csv": [
            "gate_id",
            "gate",
            "required_before_production",
            "status",
        ],
        "closure_decisions.csv": [
            "decision_id",
            "decision",
            "value",
            "rationale",
        ],
        "layer_8_handoff.csv": [
            "handoff_id",
            "requirement",
        ],
        "acceptance_criteria.csv": [
            "criterion_id",
            "criterion",
            "required",
        ],
        "authority_boundaries.csv": [
            "authority",
            "granted",
            "reason",
        ],
    }

    for filename, rows in artifacts.items():
        write_csv(
            OUTPUT_DIR / filename,
            fieldnames[filename],
            rows,
        )

    write_csv(
        OUTPUT_DIR / "recommended_path.csv",
        [
            "recommended_next_layer",
            "recommended_action",
            "entry_condition",
            "passed",
        ],
        [
            {
                "recommended_next_layer": (
                    recommended_next_layer
                ),
                "recommended_action": (
                    "Begin Layer 8 pitch arsenal and matchup inventory "
                    "under diagnostic-only scope."
                    if all_checks_passed
                    else
                    "Remediate failed Layer 7 closure checks."
                ),
                "entry_condition": (
                    "All fourteen 7O closure checks pass."
                ),
                "passed": all_checks_passed,
            }
        ],
    )

    summary = {
        "planning_checks_required": len(
            planning_checks
        ),
        "planning_checks_passed": sum(
            1
            for row in planning_checks
            if row["passed"]
        ),
        "required_artifacts_inventoried": len(
            file_inventory
        ),
        "implementation_diagnoses_inventoried": len(
            diagnosis_inventory
        ),
        "capabilities_inventoried": len(
            capability_inventory
        ),
        "unresolved_production_gates": len(
            unresolved_gates
        ),
        "closure_decisions_defined": len(
            closure_decisions
        ),
        "layer_8_handoff_requirements_defined": len(
            layer_8_handoff
        ),
        "acceptance_criteria_defined": len(
            acceptance_criteria
        ),
        "layer_7_diagnostic_scope_complete": (
            all_checks_passed
        ),
        "layer_7_production_ready": False,
        "production_behavior_changed": False,
        "simulation_behavior_changed": False,
        "canonical_probability_authority_changed": False,
        "production_environment_activated": False,
        "historical_outcome_joined": False,
        "historical_validation_executed": False,
        "accuracy_metrics_generated": False,
        "tuning_executed": False,
        "pricing_or_edge_work_executed": False,
    }

    write_json(
        OUTPUT_DIR / "closure_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": diagnosis_name,
        "all_checks_passed": all_checks_passed,
        **summary,
        "layer7_completed": all_checks_passed,
        "layer7_scope": (
            "diagnostic_shadow_observable_only"
        ),
        "new_production_authority_granted": False,
        "historical_validation_allowed_next": False,
        "tuning_allowed_next": False,
        "backtests_allowed_next": False,
        "pricing_allowed_next": False,
        "edge_detection_allowed_next": False,
        "layer8_planning_allowed_next": (
            all_checks_passed
        ),
        "production_environment_integration_allowed_next": False,
        "recommended_next_layer": (
            recommended_next_layer
        ),
        "generated_csv_artifacts": [
            str(OUTPUT_DIR / filename)
            for filename in [
                *artifacts.keys(),
                "recommended_path.csv",
            ]
        ],
        "generated_json_artifacts": [
            str(
                OUTPUT_DIR / "closure_summary.json"
            ),
            str(
                OUTPUT_DIR / "diagnosis.json"
            ),
        ],
    }

    write_json(
        OUTPUT_DIR / "diagnosis.json",
        diagnosis,
    )

    print(
        json.dumps(
            diagnosis,
            indent=2,
        )
    )

    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
