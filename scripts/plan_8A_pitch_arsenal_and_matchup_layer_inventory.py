#!/usr/bin/env python3
"""
Layer 8A
Pitch Arsenal and Matchup Layer Inventory Plan

Inventories repository support for:
- pitch types and arsenals;
- pitch usage and sequencing;
- pitcher and batter handedness;
- count-state behavior;
- batter-versus-pitcher and pitch-type matchups;
- velocity, movement, location, command, and quality;
- swing, whiff, contact, exit velocity, and launch angle;
- production runtime integration points;
- data provenance, diagnostics, tests, and validation surfaces.

Inventory and planning only.

This layer does not:
- alter simulation state, probabilities, pitch selection, or outcomes;
- create production pitch-arsenal authority;
- tune matchup parameters;
- execute historical predictive validation or backtests;
- perform pricing, market comparison, or edge detection.
"""

from __future__ import annotations

import ast
import csv
import json
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "8A"
LAYER_NAME = "pitch_arsenal_and_matchup_layer_inventory_plan"

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp/"
    "layer_8A_pitch_arsenal_and_matchup_layer_inventory"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts/"
    "plan_7O_layer_7_environment_readiness_and_scope_closure.py"
)

EXCLUDED_DIRS = {
    ".git",
    ".venv",
    "venv",
    "node_modules",
    "dist",
    "build",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    "tmp",
}

SCANNED_SUFFIXES = {
    ".py",
    ".js",
    ".jsx",
    ".ts",
    ".tsx",
    ".json",
    ".yaml",
    ".yml",
    ".toml",
    ".md",
    ".sql",
    ".csv",
}

SEARCH_DOMAINS = {
    "pitch_identity": [
        "pitch_type",
        "pitch type",
        "pitch_name",
        "pitch name",
        "pitch_class",
        "pitch classification",
        "four_seam",
        "four-seam",
        "sinker",
        "slider",
        "curveball",
        "changeup",
        "cutter",
        "splitter",
        "sweeper",
        "knuckle",
    ],
    "arsenal_usage": [
        "arsenal",
        "pitch mix",
        "pitch_mix",
        "usage rate",
        "usage_rate",
        "pitch usage",
        "pitch_usage",
        "repertoire",
    ],
    "pitch_sequence_count": [
        "pitch sequence",
        "pitch_sequence",
        "sequencing",
        "previous pitch",
        "previous_pitch",
        "count state",
        "count_state",
        "balls",
        "strikes",
        "two strike",
        "two_strike",
        "ahead in count",
        "behind in count",
    ],
    "handedness_platoon": [
        "handedness",
        "pitcher_hand",
        "batter_hand",
        "throws",
        "bats",
        "platoon",
        "same handed",
        "opposite handed",
        "vs lhp",
        "vs rhp",
        "vsl",
        "vsr",
    ],
    "pitch_shape_quality": [
        "velocity",
        "release speed",
        "release_speed",
        "spin rate",
        "spin_rate",
        "movement",
        "horizontal break",
        "vertical break",
        "pfx_x",
        "pfx_z",
        "extension",
        "release point",
        "release_pos",
        "command",
        "control",
        "location",
        "plate_x",
        "plate_z",
        "zone",
    ],
    "batter_pitch_matchup": [
        "batter vs pitcher",
        "batter_vs_pitcher",
        "pitcher vs batter",
        "pitcher_vs_batter",
        "matchup",
        "pitch type split",
        "pitch_type_split",
        "run value",
        "run_value",
        "expected run",
    ],
    "swing_contact": [
        "swing",
        "take",
        "chase",
        "zone swing",
        "zone_swing",
        "whiff",
        "contact rate",
        "contact_rate",
        "called strike",
        "called_strike",
        "swinging strike",
        "swinging_strike",
        "csw",
    ],
    "batted_ball_quality": [
        "exit velocity",
        "exit_velocity",
        "launch angle",
        "launch_angle",
        "spray angle",
        "spray_angle",
        "barrel",
        "hard hit",
        "hard_hit",
        "ground ball",
        "ground_ball",
        "fly ball",
        "fly_ball",
        "line drive",
        "line_drive",
        "popup",
    ],
    "probability_runtime": [
        "probability",
        "transition",
        "outcome",
        "simulation",
        "simulate",
        "plate appearance",
        "plate_appearance",
        "at bat",
        "at_bat",
        "event probability",
        "event_probability",
    ],
    "data_provenance": [
        "statcast",
        "baseball savant",
        "pybaseball",
        "fangraphs",
        "pitchfx",
        "pitch f/x",
        "source",
        "provenance",
        "freshness",
        "season",
        "sample size",
        "sample_size",
    ],
    "tests_diagnostics": [
        "test_",
        "pytest",
        "audit_",
        "diagnostic",
        "shadow",
        "validation",
        "backtest",
        "calibration",
    ],
}

WORKSTREAMS = [
    {
        "workstream_id": "L8-WS01",
        "workstream": "canonical_pitch_taxonomy",
        "objective": (
            "Define canonical pitch identities, aliases, unknown handling, "
            "and source precedence."
        ),
    },
    {
        "workstream_id": "L8-WS02",
        "workstream": "pitcher_arsenal_profile",
        "objective": (
            "Define pitcher pitch mix, availability, usage, shape, quality, "
            "and sample-size metadata."
        ),
    },
    {
        "workstream_id": "L8-WS03",
        "workstream": "batter_pitch_type_profile",
        "objective": (
            "Define batter swing, whiff, contact, and batted-ball profiles "
            "by pitch type and handedness."
        ),
    },
    {
        "workstream_id": "L8-WS04",
        "workstream": "count_and_sequence_context",
        "objective": (
            "Represent count-dependent usage and bounded sequencing context."
        ),
    },
    {
        "workstream_id": "L8-WS05",
        "workstream": "pitcher_batter_matchup_composition",
        "objective": (
            "Compose non-authoritative pitcher arsenal and batter response "
            "metadata into a deterministic matchup payload."
        ),
    },
    {
        "workstream_id": "L8-WS06",
        "workstream": "contact_quality_interface",
        "objective": (
            "Define the interface from matchup diagnostics to contact-quality "
            "metadata without changing outcomes."
        ),
    },
    {
        "workstream_id": "L8-WS07",
        "workstream": "shadow_observability",
        "objective": (
            "Provide deterministic, redacted, disabled-by-default matchup "
            "observability."
        ),
    },
    {
        "workstream_id": "L8-WS08",
        "workstream": "readiness_and_promotion_gates",
        "objective": (
            "Document evidence requirements before any simulation authority."
        ),
    },
]

PROHIBITED_AUTHORITIES = [
    "production_pitch_selection",
    "production_pitch_sequence_change",
    "production_matchup_adjustment",
    "simulation_state_change",
    "simulation_parameter_change",
    "simulation_probability_change",
    "plate_appearance_outcome_change",
    "contact_quality_change",
    "exit_velocity_change",
    "launch_angle_change",
    "batted_ball_outcome_change",
    "canonical_probability_replacement",
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
    try:
        return path.read_text(
            encoding="utf-8",
            errors="ignore",
        )
    except OSError:
        return ""


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


def eligible_files() -> list[Path]:
    files: list[Path] = []

    for path in ROOT.rglob("*"):
        if not path.is_file():
            continue

        relative = path.relative_to(ROOT)

        if any(
            part in EXCLUDED_DIRS
            for part in relative.parts
        ):
            continue

        if path.suffix.lower() not in SCANNED_SUFFIXES:
            continue

        files.append(path)

    return sorted(files)


def classify_surface(path: Path) -> str:
    relative = path.relative_to(ROOT)
    parts = set(relative.parts)
    name = path.name.lower()

    if "tests" in parts or name.startswith("test_"):
        return "test"

    if "scripts" in parts:
        if name.startswith("audit_"):
            return "audit"
        if name.startswith("plan_"):
            return "plan"
        if "backtest" in name:
            return "backtest"
        return "script"

    if "mlb_app" in parts:
        return "application"

    if "frontend" in parts or "src" in parts:
        return "frontend_or_source"

    if path.suffix.lower() in {".md"}:
        return "documentation"

    if path.suffix.lower() in {".json", ".csv", ".yaml", ".yml"}:
        return "data_or_configuration"

    return "other"


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    predecessor_present = (
        "layer_7_environment_readiness_and_scope_closure_plan_complete"
        in string_constants(
            PREDECESSOR_PATH
        )
    )

    files = eligible_files()

    file_rows: list[dict[str, Any]] = []
    match_rows: list[dict[str, Any]] = []
    domain_summary: list[dict[str, Any]] = []

    texts: dict[Path, str] = {}

    for path in files:
        text = read_text(path)
        texts[path] = text
        relative = str(
            path.relative_to(ROOT)
        )

        file_rows.append(
            {
                "path": relative,
                "suffix": path.suffix.lower(),
                "surface": classify_surface(path),
                "line_count": (
                    text.count("\n") + 1
                    if text
                    else 0
                ),
                "byte_count": len(
                    text.encode(
                        "utf-8",
                        errors="ignore",
                    )
                ),
            }
        )

    for domain, terms in SEARCH_DOMAINS.items():
        domain_file_count = 0
        domain_match_count = 0

        for path, text in texts.items():
            lowered = text.lower()
            matched_terms = sorted(
                {
                    term
                    for term in terms
                    if term in lowered
                }
            )

            if not matched_terms:
                continue

            domain_file_count += 1

            for term in matched_terms:
                count = lowered.count(term)
                domain_match_count += count

                match_rows.append(
                    {
                        "domain": domain,
                        "path": str(
                            path.relative_to(ROOT)
                        ),
                        "surface": classify_surface(
                            path
                        ),
                        "term": term,
                        "occurrences": count,
                    }
                )

        domain_summary.append(
            {
                "domain": domain,
                "search_terms": len(terms),
                "matched_files": domain_file_count,
                "term_occurrences": domain_match_count,
            }
        )

    surface_summary: list[dict[str, Any]] = []

    for surface in sorted(
        {
            row["surface"]
            for row in file_rows
        }
    ):
        surface_files = [
            row
            for row in file_rows
            if row["surface"] == surface
        ]

        surface_matches = [
            row
            for row in match_rows
            if row["surface"] == surface
        ]

        surface_summary.append(
            {
                "surface": surface,
                "files_scanned": len(
                    surface_files
                ),
                "matched_rows": len(
                    surface_matches
                ),
                "term_occurrences": sum(
                    row["occurrences"]
                    for row in surface_matches
                ),
            }
        )

    inventory_questions = [
        {
            "question_id": "L8-Q01",
            "question": (
                "Is there one canonical pitch taxonomy shared across sources?"
            ),
        },
        {
            "question_id": "L8-Q02",
            "question": (
                "Where are pitcher arsenal and pitch-usage profiles sourced?"
            ),
        },
        {
            "question_id": "L8-Q03",
            "question": (
                "Are pitch aliases and source-specific classifications normalized?"
            ),
        },
        {
            "question_id": "L8-Q04",
            "question": (
                "Are usage rates conditioned by count, handedness, role, or season?"
            ),
        },
        {
            "question_id": "L8-Q05",
            "question": (
                "What batter pitch-type response metrics already exist?"
            ),
        },
        {
            "question_id": "L8-Q06",
            "question": (
                "What velocity, movement, location, and command fields exist?"
            ),
        },
        {
            "question_id": "L8-Q07",
            "question": (
                "Where are swing, chase, whiff, contact, and CSW modeled?"
            ),
        },
        {
            "question_id": "L8-Q08",
            "question": (
                "Where are exit velocity, launch angle, and contact type modeled?"
            ),
        },
        {
            "question_id": "L8-Q09",
            "question": (
                "Are pitcher-batter matchup adjustments already authoritative?"
            ),
        },
        {
            "question_id": "L8-Q10",
            "question": (
                "Which runtime paths could receive matchup metadata safely?"
            ),
        },
        {
            "question_id": "L8-Q11",
            "question": (
                "Which existing tests constrain pitch or matchup behavior?"
            ),
        },
        {
            "question_id": "L8-Q12",
            "question": (
                "What provenance, freshness, and sample-size gaps remain?"
            ),
        },
    ]

    implementation_sequence = [
        {
            "step": 1,
            "action": (
                "Inventory repository pitch, arsenal, matchup, and contact surfaces."
            ),
        },
        {
            "step": 2,
            "action": (
                "Define canonical pitch taxonomy and source contract."
            ),
        },
        {
            "step": 3,
            "action": (
                "Implement canonical pitch taxonomy as diagnostic metadata."
            ),
        },
        {
            "step": 4,
            "action": (
                "Define pitcher arsenal profile contract."
            ),
        },
        {
            "step": 5,
            "action": (
                "Implement pitcher arsenal profile diagnostics."
            ),
        },
        {
            "step": 6,
            "action": (
                "Define batter pitch-type response profile contract."
            ),
        },
        {
            "step": 7,
            "action": (
                "Implement batter response diagnostics."
            ),
        },
        {
            "step": 8,
            "action": (
                "Define count, sequence, handedness, and matchup composition."
            ),
        },
        {
            "step": 9,
            "action": (
                "Implement deterministic matchup composition."
            ),
        },
        {
            "step": 10,
            "action": (
                "Define contact-quality interface without outcome authority."
            ),
        },
        {
            "step": 11,
            "action": (
                "Implement shadow observability and invariance checks."
            ),
        },
        {
            "step": 12,
            "action": (
                "Close Layer 8 under evidence-gated scope."
            ),
        },
    ]

    planning_checks = [
        {
            "check": "layer_7_closure_predecessor_present",
            "actual": predecessor_present,
            "expected": True,
            "passed": predecessor_present,
        },
        {
            "check": "repository_files_scanned",
            "actual": len(files),
            "expected": ">0",
            "passed": len(files) > 0,
        },
        {
            "check": "eleven_search_domains_defined",
            "actual": len(SEARCH_DOMAINS),
            "expected": 11,
            "passed": len(SEARCH_DOMAINS) == 11,
        },
        {
            "check": "search_domain_inventory_completed",
            "actual": len(domain_summary),
            "expected": 11,
            "passed": len(domain_summary) == 11,
        },
        {
            "check": "file_inventory_completed",
            "actual": len(file_rows),
            "expected": len(files),
            "passed": len(file_rows) == len(files),
        },
        {
            "check": "surface_summary_completed",
            "actual": len(surface_summary),
            "expected": ">0",
            "passed": len(surface_summary) > 0,
        },
        {
            "check": "eight_workstreams_defined",
            "actual": len(WORKSTREAMS),
            "expected": 8,
            "passed": len(WORKSTREAMS) == 8,
        },
        {
            "check": "twelve_inventory_questions_defined",
            "actual": len(inventory_questions),
            "expected": 12,
            "passed": len(inventory_questions) == 12,
        },
        {
            "check": "twelve_implementation_steps_defined",
            "actual": len(implementation_sequence),
            "expected": 12,
            "passed": len(implementation_sequence) == 12,
        },
        {
            "check": "inventory_only_boundary_preserved",
            "actual": True,
            "expected": True,
            "passed": True,
        },
        {
            "check": "production_authority_not_granted",
            "actual": False,
            "expected": False,
            "passed": True,
        },
        {
            "check": "validation_tuning_pricing_edge_not_executed",
            "actual": False,
            "expected": False,
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
                "8A performs repository inventory and planning only."
            ),
        }
        for authority in PROHIBITED_AUTHORITIES
    ]

    authority_rows.extend(
        [
            {
                "authority": (
                    "canonical_pitch_taxonomy_source_contract_planning"
                ),
                "granted": all_checks_passed,
                "reason": (
                    "8B may define the bounded canonical pitch taxonomy "
                    "and source contract."
                ),
            },
            {
                "authority": (
                    "production_pitch_arsenal_integration"
                ),
                "granted": False,
                "reason": (
                    "No pitch or matchup diagnostic has production authority."
                ),
            },
        ]
    )

    diagnosis_name = (
        "pitch_arsenal_and_matchup_layer_inventory_plan_complete"
        if all_checks_passed
        else
        "pitch_arsenal_and_matchup_layer_inventory_plan_failed"
    )

    recommended_next_layer = (
        "8B_canonical_pitch_taxonomy_and_source_contract_plan"
        if all_checks_passed
        else
        "8A_pitch_arsenal_and_matchup_inventory_remediation"
    )

    write_csv(
        OUTPUT_DIR / "planning_checks.csv",
        [
            "check",
            "actual",
            "expected",
            "passed",
        ],
        planning_checks,
    )

    write_csv(
        OUTPUT_DIR / "file_inventory.csv",
        [
            "path",
            "suffix",
            "surface",
            "line_count",
            "byte_count",
        ],
        file_rows,
    )

    write_csv(
        OUTPUT_DIR / "semantic_matches.csv",
        [
            "domain",
            "path",
            "surface",
            "term",
            "occurrences",
        ],
        match_rows,
    )

    write_csv(
        OUTPUT_DIR / "domain_summary.csv",
        [
            "domain",
            "search_terms",
            "matched_files",
            "term_occurrences",
        ],
        domain_summary,
    )

    write_csv(
        OUTPUT_DIR / "surface_summary.csv",
        [
            "surface",
            "files_scanned",
            "matched_rows",
            "term_occurrences",
        ],
        surface_summary,
    )

    write_csv(
        OUTPUT_DIR / "workstreams.csv",
        [
            "workstream_id",
            "workstream",
            "objective",
        ],
        WORKSTREAMS,
    )

    write_csv(
        OUTPUT_DIR / "inventory_questions.csv",
        [
            "question_id",
            "question",
        ],
        inventory_questions,
    )

    write_csv(
        OUTPUT_DIR / "implementation_sequence.csv",
        [
            "step",
            "action",
        ],
        implementation_sequence,
    )

    write_csv(
        OUTPUT_DIR / "authority_boundaries.csv",
        [
            "authority",
            "granted",
            "reason",
        ],
        authority_rows,
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
                    "Define canonical pitch taxonomy, aliases, source "
                    "precedence, validation, and fallback behavior."
                    if all_checks_passed
                    else
                    "Remediate failed 8A inventory checks."
                ),
                "entry_condition": (
                    "All twelve 8A inventory checks pass."
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
        "files_scanned": len(files),
        "semantic_match_rows": len(
            match_rows
        ),
        "semantic_term_occurrences": sum(
            row["occurrences"]
            for row in match_rows
        ),
        "search_domains_defined": len(
            SEARCH_DOMAINS
        ),
        "surfaces_identified": len(
            surface_summary
        ),
        "workstreams_defined": len(
            WORKSTREAMS
        ),
        "inventory_questions_defined": len(
            inventory_questions
        ),
        "implementation_steps_defined": len(
            implementation_sequence
        ),
        "production_behavior_changed": False,
        "simulation_behavior_changed": False,
        "canonical_probability_authority_changed": False,
        "pitch_selection_changed": False,
        "matchup_adjustments_activated": False,
        "contact_quality_changed": False,
        "historical_outcome_joined": False,
        "historical_validation_executed": False,
        "tuning_executed": False,
        "pricing_or_edge_work_executed": False,
    }

    write_json(
        OUTPUT_DIR / "inventory_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": diagnosis_name,
        "all_checks_passed": all_checks_passed,
        **summary,
        "layer8_completed": False,
        "new_production_authority_granted": False,
        "historical_validation_allowed_next": False,
        "tuning_allowed_next": False,
        "backtests_allowed_next": False,
        "pricing_allowed_next": False,
        "edge_detection_allowed_next": False,
        "pitch_taxonomy_contract_planning_allowed_next": (
            all_checks_passed
        ),
        "production_pitch_arsenal_integration_allowed_next": False,
        "recommended_next_layer": (
            recommended_next_layer
        ),
        "generated_csv_artifacts": [
            str(
                OUTPUT_DIR / filename
            )
            for filename in [
                "planning_checks.csv",
                "file_inventory.csv",
                "semantic_matches.csv",
                "domain_summary.csv",
                "surface_summary.csv",
                "workstreams.csv",
                "inventory_questions.csv",
                "implementation_sequence.csv",
                "authority_boundaries.csv",
                "recommended_path.csv",
            ]
        ],
        "generated_json_artifacts": [
            str(
                OUTPUT_DIR
                / "inventory_summary.json"
            ),
            str(
                OUTPUT_DIR
                / "diagnosis.json"
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
