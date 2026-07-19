#!/usr/bin/env python3
"""
Layer 10A — DFS Box-Score Event Contract Plan

Purpose
-------
Define the planning contract for evolving the MLB simulator into an
event-driven baseball engine whose authoritative output is a projected
box score suitable for DFS projections.

This layer is planning-only. It must not:
- change production probabilities;
- execute network retrievals;
- alter pricing or betting outputs;
- emit player projections;
- mutate canonical simulation records;
- claim that baseline advancement probabilities are calibrated.

The central architectural rule is:

    The simulation produces baseball plays.
    Game state and box-score statistics are derived from those plays.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


LAYER = "10A"
PLAN_VERSION = "layer_10A_dfs_box_score_event_contract_plan_v1"
OUTPUT_DIR = Path("tmp/layer_10A_dfs_box_score_event_contract_plan")

PLAN_JSON_PATH = OUTPUT_DIR / "layer_10A_dfs_box_score_event_contract_plan.json"
PLAN_MARKDOWN_PATH = OUTPUT_DIR / "layer_10A_dfs_box_score_event_contract_plan.md"
MANIFEST_PATH = OUTPUT_DIR / "layer_10A_manifest.json"

NEXT_LAYER = "10B_play_ledger_deterministic_state_transition_implementation"


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def sha256_digest(value: Any) -> str:
    payload = (
        value
        if isinstance(value, str)
        else canonical_json(value)
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def build_plan() -> dict[str, Any]:
    return {
        "layer": LAYER,
        "plan_version": PLAN_VERSION,
        "title": "DFS box-score event contract plan",
        "status": "planning_complete",
        "primary_objective": (
            "Evolve the MLB simulation into an event-driven baseball "
            "engine whose authoritative projection product is a coherent "
            "projected box score used to calculate DFS projections."
        ),
        "guiding_principle": (
            "The simulation produces baseball plays. Game state and "
            "box-score statistics are derived from those plays."
        ),
        "dfs_projection_contract": {
            "authoritative_projection_source": (
                "projected box-score distributions reconstructed from "
                "simulated play events"
            ),
            "required_batter_outputs": [
                "plate_appearances",
                "at_bats",
                "singles",
                "doubles",
                "triples",
                "home_runs",
                "walks",
                "hit_by_pitch",
                "strikeouts",
                "runs",
                "runs_batted_in",
                "stolen_bases",
                "caught_stealing",
                "sacrifice_flies",
                "sacrifice_hits",
            ],
            "required_pitcher_outputs": [
                "batters_faced",
                "outs_recorded",
                "innings_pitched",
                "hits_allowed",
                "home_runs_allowed",
                "walks_allowed",
                "hit_batters",
                "strikeouts",
                "runs_allowed",
                "earned_runs",
                "wins",
                "losses",
                "saves",
                "holds",
            ],
            "required_team_outputs": [
                "runs",
                "hits",
                "errors",
                "left_on_base",
                "inning_lines",
                "final_score",
            ],
            "dfs_rule": (
                "Fantasy points must be derived from simulated statistical "
                "events rather than independently sampled after the game."
            ),
        },
        "canonical_game_state": {
            "identity_fields": [
                "game_id",
                "simulation_id",
                "random_seed",
                "event_sequence",
            ],
            "inning_fields": [
                "inning",
                "half_inning",
                "outs",
                "home_score",
                "away_score",
            ],
            "base_fields": [
                "runner_on_first",
                "runner_on_second",
                "runner_on_third",
            ],
            "participant_fields": [
                "batting_team",
                "fielding_team",
                "batter_id",
                "pitcher_id",
                "lineup_position",
            ],
            "optional_context_fields": [
                "pitcher_batters_faced",
                "pitcher_pitch_count",
                "pitcher_fatigue_state",
                "park_id",
                "weather_context_id",
            ],
        },
        "canonical_play_event": {
            "identity_fields": [
                "event_id",
                "event_sequence",
                "state_before_digest",
                "state_after_digest",
            ],
            "primary_fields": [
                "plate_appearance_result",
                "batter_id",
                "pitcher_id",
                "outs_before",
                "outs_after",
            ],
            "batted_ball_fields": [
                "batted_ball_type",
                "field_zone",
                "direction",
                "depth",
                "contact_quality",
            ],
            "transition_fields": [
                "batter_destination",
                "runner_movements",
                "outs_recorded",
                "runs_scored",
                "errors",
            ],
            "scoring_fields": [
                "batter_rbi",
                "sacrifice_fly",
                "sacrifice_hit",
                "grounded_into_double_play",
                "earned_run_candidates",
            ],
            "provenance_fields": [
                "primary_probability_source",
                "transition_probability_source",
                "fallback_status",
                "parameter_version",
            ],
        },
        "runner_movement_contract": {
            "required_fields": [
                "runner_id",
                "origin",
                "destination",
                "result",
                "reason",
                "forced",
                "out_sequence",
                "run_counts",
            ],
            "allowed_results": [
                "hold",
                "safe",
                "out",
            ],
            "movement_reasons": [
                "forced",
                "batter_award",
                "hit_advance",
                "tag_up",
                "groundout_advance",
                "fielder_choice",
                "error",
                "wild_pitch",
                "passed_ball",
                "stolen_base",
                "caught_stealing",
                "defensive_indifference",
            ],
        },
        "hierarchical_resolution": [
            {
                "stage": 1,
                "name": "primary_plate_appearance_outcome",
                "examples": [
                    "strikeout",
                    "walk",
                    "hit_by_pitch",
                    "home_run",
                    "ball_in_play",
                ],
            },
            {
                "stage": 2,
                "name": "batted_ball_context",
                "condition": "plate appearance produces a ball in play",
                "examples": [
                    "ground_ball",
                    "line_drive",
                    "fly_ball",
                    "popup",
                    "field_zone",
                    "depth",
                ],
            },
            {
                "stage": 3,
                "name": "fielding_resolution",
                "examples": [
                    "out",
                    "single",
                    "double",
                    "triple",
                    "error",
                    "force_out",
                    "fielders_choice",
                    "double_play",
                ],
            },
            {
                "stage": 4,
                "name": "legal_transition_enumeration",
                "description": (
                    "Determine forced, prohibited, and discretionary runner "
                    "movements before sampling advancement."
                ),
            },
            {
                "stage": 5,
                "name": "runner_advancement_resolution",
                "description": (
                    "Apply deterministic movement first, then sample only "
                    "among legal discretionary movements."
                ),
            },
            {
                "stage": 6,
                "name": "statistical_attribution",
                "examples": [
                    "run",
                    "rbi",
                    "sacrifice_fly",
                    "earned_run_candidate",
                    "putout",
                    "assist",
                ],
            },
            {
                "stage": 7,
                "name": "validated_state_transition",
                "description": (
                    "Produce the post-play state and validate all state and "
                    "accounting invariants."
                ),
            },
        ],
        "deterministic_transition_scope": [
            "home_run_runner_scoring",
            "walk_force_advancement",
            "hit_by_pitch_force_advancement",
            "batter_awards",
            "inning_termination_at_three_outs",
            "third_out_force_run_suppression",
            "base_occupancy_validation",
            "lineup_advancement",
        ],
        "baseline_probabilistic_transition_scope": [
            "runner_second_to_home_on_single",
            "runner_first_to_third_on_single",
            "runner_first_to_home_on_double",
            "runner_third_to_home_on_caught_fly",
            "runner_second_to_third_on_caught_fly",
            "runner_advancement_on_groundout",
            "extra_base_attempt",
            "out_on_base",
        ],
        "baseline_probability_policy": {
            "initial_source": (
                "explicit versioned league-average constants or tables"
            ),
            "required_metadata": [
                "parameter_name",
                "parameter_version",
                "context_dimensions",
                "source_type",
                "fallback_status",
                "calibration_status",
            ],
            "initial_calibration_status": "baseline_not_yet_calibrated",
            "upgrade_rule": (
                "Future player, fielder, park, weather, and game-context "
                "providers may replace or modify the baseline without "
                "changing the transition-engine contract."
            ),
            "modifier_policy": (
                "Prefer bounded log-odds adjustments over additive raw "
                "percentage changes."
            ),
        },
        "state_invariants": [
            "outs remain valid during an active half-inning",
            "no runner occupies multiple bases",
            "no base contains multiple runners",
            "all runs arise from explicit runner movements",
            "all outs arise from explicit out records",
            "forced movements comply with base occupancy",
            "third-out force and timing rules determine whether runs count",
            "half-inning rollover occurs only after the third out",
            "lineup order advances exactly once per completed plate appearance",
        ],
        "ledger_invariants": [
            "event sequence is strictly increasing",
            "state_before_digest matches the preceding post-play state",
            "state_after_digest matches the emitted post-play state",
            "replaying all events reproduces the final game state",
            "rebuilding the box score reproduces incremental accounting",
            "fixed random seeds produce deterministic event artifacts",
            "every DFS-relevant statistic is supported by at least one event",
        ],
        "migration_strategy": {
            "preserve_initially": [
                "existing primary plate-appearance probability model",
                "existing matchup inputs",
                "existing park and weather inputs",
                "existing DFS scoring rules",
            ],
            "replace_first": [
                "direct base-state mutation after primary outcomes",
                "direct box-score counter mutation without event evidence",
            ],
            "first_implementation_slice": {
                "layer": "10B",
                "name": (
                    "play ledger and deterministic state transition "
                    "implementation"
                ),
                "scope": [
                    "introduce canonical pre-play state",
                    "introduce canonical play event",
                    "introduce runner movement records",
                    "append events to an immutable ledger",
                    "resolve deterministic walks, hit-by-pitch, and home runs",
                    "validate base and out invariants",
                    "preserve existing primary outcome probabilities",
                ],
                "excluded": [
                    "new player-specific probabilities",
                    "new park or weather models",
                    "probabilistic tag-up logic",
                    "probabilistic hit advancement",
                    "production DFS projection replacement",
                ],
            },
        },
        "delivery_sequence": [
            {
                "layer": "10A",
                "deliverable": "DFS box-score event contract plan",
            },
            {
                "layer": "10B",
                "deliverable": (
                    "play ledger and deterministic state transitions"
                ),
            },
            {
                "layer": "10C",
                "deliverable": "baseline batted-ball context",
            },
            {
                "layer": "10D",
                "deliverable": "baseline runner advancement",
            },
            {
                "layer": "10E",
                "deliverable": (
                    "multi-out plays and official scoring-rule realism"
                ),
            },
            {
                "layer": "10F",
                "deliverable": "box-score reducer and replay validation",
            },
            {
                "layer": "10G",
                "deliverable": "simulation calibration harness",
            },
            {
                "layer": "10H+",
                "deliverable": (
                    "player, fielder, park, weather, fatigue, and "
                    "game-context modifiers"
                ),
            },
        ],
        "calibration_targets": [
            "runs_per_game",
            "hits_per_game",
            "single_double_triple_home_run_rates",
            "walk_rate",
            "strikeout_rate",
            "double_play_rate",
            "sacrifice_fly_rate",
            "runner_advancement_rates",
            "outs_on_base_rate",
            "left_on_base",
            "inning_length_distribution",
            "team_score_distribution",
            "batter_DFS_point_distribution",
            "pitcher_DFS_point_distribution",
        ],
        "planning_authority": {
            "implementation_authority_granted": True,
            "authority_scope": (
                "Layer 10B play-ledger and deterministic-state-transition "
                "implementation only"
            ),
            "production_probability_change_authorized": False,
            "production_DFS_projection_change_authorized": False,
        },
        "recommended_next_layer": NEXT_LAYER,
    }


def build_markdown(plan: dict[str, Any], plan_digest: str) -> str:
    lines = [
        "# Layer 10A — DFS Box-Score Event Contract Plan",
        "",
        "## Objective",
        "",
        plan["primary_objective"],
        "",
        "## Guiding principle",
        "",
        f"> {plan['guiding_principle']}",
        "",
        "## DFS projection contract",
        "",
        (
            "The projected box score becomes the authoritative statistical "
            "source for batter and pitcher DFS projections. Fantasy points "
            "must be derived from simulated baseball events."
        ),
        "",
        "## Resolution pipeline",
        "",
    ]

    for stage in plan["hierarchical_resolution"]:
        lines.append(
            f"{stage['stage']}. **{stage['name']}**"
        )

    lines.extend(
        [
            "",
            "## First implementation slice",
            "",
            (
                "Layer 10B introduces the append-only play ledger and "
                "deterministic base/out transitions while preserving the "
                "existing primary plate-appearance probability model."
            ),
            "",
            "## Explicitly deferred",
            "",
            "- probabilistic tag-up and hit advancement",
            "- player-specific baserunning effects",
            "- fielder arm and range effects",
            "- park and weather transition modifiers",
            "- replacement of production DFS projections",
            "",
            "## Recommended next layer",
            "",
            f"`{plan['recommended_next_layer']}`",
            "",
            "## Plan digest",
            "",
            f"`{plan_digest}`",
            "",
        ]
    )

    return "\n".join(lines)


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    plan = build_plan()
    plan_digest = sha256_digest(plan)

    plan_artifact = {
        **plan,
        "plan_digest": plan_digest,
    }

    markdown = build_markdown(plan, plan_digest)

    PLAN_JSON_PATH.write_text(
        json.dumps(plan_artifact, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    PLAN_MARKDOWN_PATH.write_text(markdown, encoding="utf-8")

    checks = {
        "planning_only": (
            not plan["planning_authority"][
                "production_probability_change_authorized"
            ]
            and not plan["planning_authority"][
                "production_DFS_projection_change_authorized"
            ]
        ),
        "event_driven_principle_present": (
            "produces baseball plays"
            in plan["guiding_principle"]
        ),
        "dfs_box_score_contract_present": bool(
            plan["dfs_projection_contract"]
        ),
        "canonical_state_defined": bool(
            plan["canonical_game_state"]
        ),
        "canonical_event_defined": bool(
            plan["canonical_play_event"]
        ),
        "runner_movement_contract_defined": bool(
            plan["runner_movement_contract"]
        ),
        "deterministic_scope_defined": bool(
            plan["deterministic_transition_scope"]
        ),
        "probabilistic_scope_defined": bool(
            plan["baseline_probabilistic_transition_scope"]
        ),
        "invariants_defined": bool(
            plan["state_invariants"]
            and plan["ledger_invariants"]
        ),
        "first_slice_is_10B": (
            plan["migration_strategy"][
                "first_implementation_slice"
            ]["layer"]
            == "10B"
        ),
        "recommended_next_layer_defined": (
            plan["recommended_next_layer"] == NEXT_LAYER
        ),
    }

    if not all(checks.values()):
        failed = [
            name
            for name, passed in checks.items()
            if not passed
        ]
        raise RuntimeError(
            "Layer 10A plan checks failed: "
            + ", ".join(failed)
        )

    manifest = {
        "layer": LAYER,
        "plan_version": PLAN_VERSION,
        "plan_digest": plan_digest,
        "checks": checks,
        "checks_passed": sum(checks.values()),
        "checks_total": len(checks),
        "artifacts": [
            str(PLAN_JSON_PATH),
            str(PLAN_MARKDOWN_PATH),
        ],
        "network_retrievals_executed": 0,
        "production_probabilities_changed": 0,
        "production_DFS_projections_changed": 0,
        "recommended_next_layer": NEXT_LAYER,
    }

    MANIFEST_PATH.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    print(
        "Layer: 10A — DFS box-score event contract plan"
    )
    print(f"Plan version: {PLAN_VERSION}")
    print(
        "Planning checks passed: "
        f"{manifest['checks_passed']}/"
        f"{manifest['checks_total']}"
    )
    print(f"Plan digest: {plan_digest}")
    print("Network retrievals executed: 0")
    print("Production probabilities changed: 0")
    print("Production DFS projections changed: 0")
    print(
        "Planning authority granted: "
        "Layer 10B deterministic transition implementation"
    )
    print(f"Recommended next layer: {NEXT_LAYER}")
    print(f"Artifacts: {OUTPUT_DIR}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
