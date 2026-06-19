#!/usr/bin/env python3
"""
Layer 6OX
Layer 6 Game-Management Scope Completeness Gap Analysis

This layer pauses the planned Layer 6 exit finalization and evaluates
whether important personnel-state and managerial mechanics remain
outside the previously audited scope.

Analysis only. No production behavior, probability, tuning, validation,
pricing, backtesting, or edge-detection changes are permitted.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "6OX"
LAYER_NAME = (
    "layer6_game_management_scope_completeness_gap_analysis"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = ROOT / (
    "tmp/layer_6OX_game_management_"
    "scope_completeness_gap_analysis"
)

MLB_APP_DIR = ROOT / "mlb_app"
SCRIPTS_DIR = ROOT / "scripts"
DOCS_DIR = ROOT / "docs"

MODEL_PROJECTIONS_PATH = (
    ROOT / "mlb_app/model_projections.py"
)

BULLPEN_CHAIN_PATH = (
    ROOT / "mlb_app/simulation/bullpen_chain.py"
)

BULLPEN_SELECTION_PATH = (
    ROOT / "mlb_app/simulation/bullpen_selection.py"
)

BULLPEN_INTEGRATION_PATH = (
    ROOT / "mlb_app/simulation/bullpen_integration.py"
)

FORMULA_MAP_PATH = (
    ROOT / "docs/model_projection_formula_map.md"
)

PREDECESSOR_PLAN_PATH = (
    ROOT
    / "scripts/plan_6OW_layer6_game_state_realism_"
    "exit_finalization.py"
)

REQUIRED_PATHS = [
    MODEL_PROJECTIONS_PATH,
    BULLPEN_CHAIN_PATH,
    BULLPEN_SELECTION_PATH,
    BULLPEN_INTEGRATION_PATH,
    FORMULA_MAP_PATH,
    PREDECESSOR_PLAN_PATH,
]

PROHIBITED_ACTIONS = [
    "backend_behavior_change",
    "frontend_behavior_change",
    "simulation_parameter_change",
    "simulation_probability_change",
    "canonical_probability_replacement",
    "historical_outcome_join",
    "accuracy_metric_generation",
    "parameter_tuning",
    "backtest_execution",
    "pricing",
    "edge_detection",
    "bet_recommendation",
    "layer6_exit_finalization",
]


def write_csv(
    path: Path,
    fieldnames: list[str],
    rows: Iterable[dict[str, Any]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=fieldnames,
        )
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2) + "\n",
        encoding="utf-8",
    )


def read_text(path: Path) -> str:
    if not path.exists():
        return ""

    return path.read_text(
        encoding="utf-8",
        errors="ignore",
    )


def find_matches(
    roots: list[Path],
    terms: list[str],
) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []

    normalized_terms = [
        term.lower()
        for term in terms
    ]

    for root in roots:
        if not root.exists():
            continue

        paths = (
            [root]
            if root.is_file()
            else sorted(
                path
                for path in root.rglob("*")
                if path.is_file()
                and path.suffix in {
                    ".py",
                    ".md",
                    ".json",
                    ".yaml",
                    ".yml",
                }
            )
        )

        for path in paths:
            text = read_text(path)
            lowered = text.lower()

            found_terms = sorted(
                {
                    term
                    for term in normalized_terms
                    if term in lowered
                }
            )

            if found_terms:
                matches.append(
                    {
                        "path": str(
                            path.relative_to(ROOT)
                        ),
                        "terms": "|".join(found_terms),
                        "match_count": sum(
                            lowered.count(term)
                            for term in found_terms
                        ),
                    }
                )

    return matches


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    required_paths_exist = all(
        path.exists()
        for path in REQUIRED_PATHS
    )

    model_projection_text = read_text(
        MODEL_PROJECTIONS_PATH
    )

    bullpen_chain_text = read_text(
        BULLPEN_CHAIN_PATH
    )

    bullpen_selection_text = read_text(
        BULLPEN_SELECTION_PATH
    )

    bullpen_integration_text = read_text(
        BULLPEN_INTEGRATION_PATH
    )

    formula_map_text = read_text(
        FORMULA_MAP_PATH
    )

    predecessor_text = read_text(
        PREDECESSOR_PLAN_PATH
    )

    steals_explicitly_deferred = all(
        token in model_projection_text
        for token in [
            "steals_model_status",
            "deferred_not_active",
            "steals_projection_wiring_status",
            "status_only_no_behavioral_effect",
        ]
    )

    bullpen_candidate_engine_present = all(
        token in (
            bullpen_chain_text
            + bullpen_selection_text
            + bullpen_integration_text
        )
        for token in [
            "simulate_candidate_bullpen_chain",
            "select_candidate_reliever",
            "classify_leverage_bucket",
            "candidate_leverage",
            "selection_history",
            "fatigue",
        ]
    )

    bullpen_optional_mode_present = all(
        token in bullpen_integration_text
        for token in [
            'BULLPEN_MODE_OFF = "off"',
            (
                'BULLPEN_MODE_CANDIDATE_LEVERAGE '
                '= "candidate_leverage"'
            ),
            "integration_active",
        ]
    )

    aggregate_bullpen_contract_present = all(
        token in formula_map_text
        for token in [
            "bullpen K rate",
            "bullpen BB rate",
            "bullpen hard-hit",
            "bullpen xwOBA",
        ]
    )

    previous_finalization_was_broad = all(
        token in predecessor_text
        for token in [
            "game_state_realism_scope",
            "complete_under_documented_scope",
            "finalize_layer6_game_state_realism_exit",
        ]
    )

    stolen_base_matches = find_matches(
        [MLB_APP_DIR],
        [
            "steal attempt",
            "caught stealing",
            "pickoff",
            "steal_success",
            "steal_attempt",
            "stolen_base",
        ],
    )

    substitution_matches = find_matches(
        [MLB_APP_DIR],
        [
            "pinch_hitter",
            "pinch hitter",
            "pinch_runner",
            "pinch runner",
            "defensive_replacement",
            "defensive replacement",
            "substitution_engine",
        ],
    )

    opener_bulk_production_matches = find_matches(
        [MLB_APP_DIR],
        [
            "opener",
            "bulk pitcher",
            "bulk_pitcher",
            "tandem starter",
            "tandem_starter",
            "bullpen game",
            "bullpen_game",
        ],
    )

    opener_bulk_repo_matches = find_matches(
        [SCRIPTS_DIR, DOCS_DIR],
        [
            "opener",
            "bulk pitcher",
            "bulk_pitcher",
            "tandem starter",
            "tandem_starter",
            "bullpen game",
            "bullpen_game",
        ],
    )

    dynamic_hook_matches = find_matches(
        [MLB_APP_DIR],
        [
            "starter_hook",
            "pitch_count",
            "times_through_order",
            "third_time_through",
            "starter_exit_enabled",
            "starter_quality_score",
        ],
    )

    production_steal_engine_verified = (
        not steals_explicitly_deferred
        and len(stolen_base_matches) > 0
    )

    substitution_engine_verified = (
        len(substitution_matches) > 0
    )

    # Keyword or file-name matches do not prove that production models
    # an opener, bulk follower, tandem starter, or planned bullpen game.
    # Those require an explicit pitching-plan sequence and workload model.
    opener_bulk_engine_verified = False

    bullpen_production_wiring_verified = False

    # Generic pitch-count and starter-exit references do not prove a
    # state-dependent hook model. Verification requires explicit in-game
    # removal logic using workload, TTO, score state, and bullpen context.
    dynamic_hook_engine_verified = False

    domain_rows = [
        {
            "domain": "stolen_bases_pickoffs",
            "current_status": "deferred_not_active",
            "production_engine_verified": (
                production_steal_engine_verified
            ),
            "blocking_for_broad_game_state_claim": True,
            "evidence": (
                "Explicit status-only steals diagnostics; "
                "no verified active attempt/success/pickoff engine."
            ),
            "required_resolution": (
                "Implement or retain explicit exclusion from "
                "the completed Layer 6 scope."
            ),
        },
        {
            "domain": "position_player_substitutions",
            "current_status": "not_verified",
            "production_engine_verified": (
                substitution_engine_verified
            ),
            "blocking_for_broad_game_state_claim": True,
            "evidence": (
                "No verified production pinch-hit, pinch-run, "
                "or defensive-replacement engine."
            ),
            "required_resolution": (
                "Implement substitution-state mechanics or "
                "exclude them from the completed scope."
            ),
        },
        {
            "domain": "bullpen_sequencing",
            "current_status": (
                "candidate_engine_present_"
                "production_wiring_unverified"
            ),
            "production_engine_verified": (
                bullpen_production_wiring_verified
            ),
            "blocking_for_broad_game_state_claim": True,
            "evidence": (
                "Candidate leverage/fatigue sequencing exists, "
                "but optional-mode and universal production "
                "probability wiring are not independently verified."
            ),
            "required_resolution": (
                "Audit production activation, reliever-level "
                "outcome wiring, and availability inputs."
            ),
        },
        {
            "domain": "opener_bulk_tandem_plans",
            "current_status": (
                "planning_references_only_or_unverified"
            ),
            "production_engine_verified": (
                opener_bulk_engine_verified
            ),
            "blocking_for_broad_game_state_claim": True,
            "evidence": (
                "Repository references exist outside production, "
                "but no verified active opener/bulk/tandem engine."
            ),
            "required_resolution": (
                "Implement pitching-plan sequence support or "
                "exclude it from the completed scope."
            ),
        },
        {
            "domain": "dynamic_starter_hook",
            "current_status": (
                "starter_exit_concept_present_"
                "dynamic_hook_unverified"
            ),
            "production_engine_verified": (
                dynamic_hook_engine_verified
            ),
            "blocking_for_broad_game_state_claim": True,
            "evidence": (
                "Starter exit/quality concepts exist, but a "
                "state-dependent hook using pitch count, TTO, "
                "score, and bullpen availability is not verified."
            ),
            "required_resolution": (
                "Audit whether production uses a dynamic hook "
                "or only an ex-ante innings distribution."
            ),
        },
    ]

    unresolved_domains = [
        row
        for row in domain_rows
        if not row["production_engine_verified"]
    ]

    broad_scope_claim_supported = (
        len(unresolved_domains) == 0
    )

    narrow_scope_claim_supported = all(
        [
            steals_explicitly_deferred,
            bullpen_candidate_engine_present,
            bullpen_optional_mode_present,
            aggregate_bullpen_contract_present,
            previous_finalization_was_broad,
        ]
    )

    scope_decision_rows = [
        {
            "decision": (
                "pause_layer6_exit_finalization"
            ),
            "selected": (
                not broad_scope_claim_supported
            ),
            "reason": (
                "Personnel-state and managerial mechanics remain "
                "unimplemented or production-unverified."
            ),
        },
        {
            "decision": (
                "retain_broad_game_state_realism_label"
            ),
            "selected": broad_scope_claim_supported,
            "reason": (
                "Allowed only when every identified domain is "
                "production-verified."
            ),
        },
        {
            "decision": (
                "rename_completed_scope_to_"
                "base_out_and_core_runner_transition_realism"
            ),
            "selected": (
                not broad_scope_claim_supported
                and narrow_scope_claim_supported
            ),
            "reason": (
                "Accurately reflects the mechanics already "
                "implemented and audited without overstating "
                "managerial or personnel-state realism."
            ),
        },
    ]

    analysis_checks = [
        {
            "check": "required_source_files_exist",
            "actual": sum(
                1
                for path in REQUIRED_PATHS
                if path.exists()
            ),
            "expected": len(REQUIRED_PATHS),
            "passed": required_paths_exist,
        },
        {
            "check": "steals_deferred_contract",
            "actual": steals_explicitly_deferred,
            "expected": True,
            "passed": steals_explicitly_deferred,
        },
        {
            "check": "candidate_bullpen_engine_inventory",
            "actual": bullpen_candidate_engine_present,
            "expected": True,
            "passed": bullpen_candidate_engine_present,
        },
        {
            "check": "bullpen_optional_mode_inventory",
            "actual": bullpen_optional_mode_present,
            "expected": True,
            "passed": bullpen_optional_mode_present,
        },
        {
            "check": "aggregate_bullpen_contract_inventory",
            "actual": aggregate_bullpen_contract_present,
            "expected": True,
            "passed": aggregate_bullpen_contract_present,
        },
        {
            "check": "previous_finalization_scope_inventory",
            "actual": previous_finalization_was_broad,
            "expected": True,
            "passed": previous_finalization_was_broad,
        },
        {
            "check": "five_domain_gap_analysis",
            "actual": len(domain_rows),
            "expected": 5,
            "passed": len(domain_rows) == 5,
        },
        {
            "check": "unresolved_scope_gaps_detected",
            "actual": len(unresolved_domains),
            "expected": ">=1",
            "passed": len(unresolved_domains) >= 1,
        },
        {
            "check": "broad_scope_finalization_paused",
            "actual": broad_scope_claim_supported,
            "expected": False,
            "passed": not broad_scope_claim_supported,
        },
        {
            "check": "narrow_scope_relabel_supported",
            "actual": narrow_scope_claim_supported,
            "expected": True,
            "passed": narrow_scope_claim_supported,
        },
    ]

    all_checks_passed = all(
        bool(row["passed"])
        for row in analysis_checks
    )

    safety_rows = [
        {
            "boundary": action,
            "changed_or_executed": False,
            "passed": True,
        }
        for action in PROHIBITED_ACTIONS
    ]

    safety_rows.extend(
        [
            {
                "boundary": (
                    "scope_completeness_analysis"
                ),
                "changed_or_executed": True,
                "passed": all_checks_passed,
            },
            {
                "boundary": (
                    "exit_finalization_pause"
                ),
                "changed_or_executed": True,
                "passed": (
                    not broad_scope_claim_supported
                ),
            },
        ]
    )

    recommended_next_layer = (
        "6OY_layer6_game_management_"
        "scope_resolution_plan"
    )

    write_csv(
        OUTPUT_DIR / "analysis_checks.csv",
        [
            "check",
            "actual",
            "expected",
            "passed",
        ],
        analysis_checks,
    )

    write_csv(
        OUTPUT_DIR / "domain_gap_analysis.csv",
        [
            "domain",
            "current_status",
            "production_engine_verified",
            "blocking_for_broad_game_state_claim",
            "evidence",
            "required_resolution",
        ],
        domain_rows,
    )

    write_csv(
        OUTPUT_DIR / "scope_decision.csv",
        [
            "decision",
            "selected",
            "reason",
        ],
        scope_decision_rows,
    )

    write_csv(
        OUTPUT_DIR / "stolen_base_matches.csv",
        [
            "path",
            "terms",
            "match_count",
        ],
        stolen_base_matches,
    )

    write_csv(
        OUTPUT_DIR / "substitution_matches.csv",
        [
            "path",
            "terms",
            "match_count",
        ],
        substitution_matches,
    )

    write_csv(
        OUTPUT_DIR / "opener_bulk_production_matches.csv",
        [
            "path",
            "terms",
            "match_count",
        ],
        opener_bulk_production_matches,
    )

    write_csv(
        OUTPUT_DIR / "opener_bulk_repo_matches.csv",
        [
            "path",
            "terms",
            "match_count",
        ],
        opener_bulk_repo_matches,
    )

    write_csv(
        OUTPUT_DIR / "dynamic_hook_matches.csv",
        [
            "path",
            "terms",
            "match_count",
        ],
        dynamic_hook_matches,
    )

    write_csv(
        OUTPUT_DIR / "safety_audit.csv",
        [
            "boundary",
            "changed_or_executed",
            "passed",
        ],
        safety_rows,
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
                    "Plan resolution of remaining personnel-state "
                    "and managerial game-mechanic scope gaps."
                ),
                "entry_condition": (
                    "6OX confirms the broad game-state realism "
                    "claim is unsupported and finalization must pause."
                ),
                "passed": all_checks_passed,
            }
        ],
    )

    evidence_summary = {
        "required_paths": [
            str(path)
            for path in REQUIRED_PATHS
        ],
        "steals_explicitly_deferred": (
            steals_explicitly_deferred
        ),
        "bullpen_candidate_engine_present": (
            bullpen_candidate_engine_present
        ),
        "bullpen_optional_mode_present": (
            bullpen_optional_mode_present
        ),
        "aggregate_bullpen_contract_present": (
            aggregate_bullpen_contract_present
        ),
        "production_stolen_base_engine_verified": (
            production_steal_engine_verified
        ),
        "production_substitution_engine_verified": (
            substitution_engine_verified
        ),
        "production_bullpen_sequencing_verified": (
            bullpen_production_wiring_verified
        ),
        "production_opener_bulk_engine_verified": (
            opener_bulk_engine_verified
        ),
        "production_dynamic_hook_verified": (
            dynamic_hook_engine_verified
        ),
        "unresolved_domains": [
            row["domain"]
            for row in unresolved_domains
        ],
        "broad_scope_claim_supported": (
            broad_scope_claim_supported
        ),
        "recommended_completed_scope": (
            "base_out_and_core_runner_transition_realism"
        ),
        "layer6_exit_finalization_paused": True,
        "new_authority_granted": False,
    }

    write_json(
        OUTPUT_DIR / "evidence_summary.json",
        evidence_summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": (
            "layer_6_game_management_"
            "scope_completeness_gaps_confirmed"
            if all_checks_passed
            else
            "layer_6_game_management_"
            "scope_completeness_gap_analysis_failed"
        ),
        "all_checks_passed": all_checks_passed,
        "analysis_checks_passed": sum(
            1
            for row in analysis_checks
            if row["passed"]
        ),
        "analysis_checks_required": len(
            analysis_checks
        ),
        "domains_analyzed": len(domain_rows),
        "unresolved_domains": len(
            unresolved_domains
        ),
        "unresolved_domain_names": [
            row["domain"]
            for row in unresolved_domains
        ],
        "broad_game_state_realism_claim_supported": (
            broad_scope_claim_supported
        ),
        "recommended_completed_scope": (
            "base_out_and_core_runner_transition_realism"
        ),
        "layer6_exit_recommended": False,
        "layer6_exit_finalized": False,
        "exit_finalization_paused": True,
        "new_authority_granted": False,
        "backend_behavior_change_allowed_next": False,
        "frontend_behavior_change_allowed_next": False,
        "simulation_parameter_change_allowed_next": False,
        "final_probability_replacement_allowed_next": False,
        "historical_validation_allowed_next": False,
        "tuning_allowed_next": False,
        "prediction_join_execution_allowed_next": False,
        "accuracy_metrics_allowed_next": False,
        "backtests_allowed_next": False,
        "pricing_allowed_next": False,
        "edge_detection_allowed_next": False,
        "scope_resolution_planning_allowed_next": (
            all_checks_passed
        ),
        "recommended_next_layer": (
            recommended_next_layer
        ),
        "generated_csv_artifacts": [
            str(
                OUTPUT_DIR / "analysis_checks.csv"
            ),
            str(
                OUTPUT_DIR / "domain_gap_analysis.csv"
            ),
            str(
                OUTPUT_DIR / "scope_decision.csv"
            ),
            str(
                OUTPUT_DIR / "stolen_base_matches.csv"
            ),
            str(
                OUTPUT_DIR / "substitution_matches.csv"
            ),
            str(
                OUTPUT_DIR
                / "opener_bulk_production_matches.csv"
            ),
            str(
                OUTPUT_DIR / "opener_bulk_repo_matches.csv"
            ),
            str(
                OUTPUT_DIR / "dynamic_hook_matches.csv"
            ),
            str(
                OUTPUT_DIR / "safety_audit.csv"
            ),
            str(
                OUTPUT_DIR / "recommended_path.csv"
            ),
        ],
        "generated_json_artifacts": [
            str(
                OUTPUT_DIR / "evidence_summary.json"
            ),
            str(OUTPUT_DIR / "diagnosis.json"),
        ],
    }

    write_json(
        OUTPUT_DIR / "diagnosis.json",
        diagnosis,
    )

    print(json.dumps(diagnosis, indent=2))

    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
