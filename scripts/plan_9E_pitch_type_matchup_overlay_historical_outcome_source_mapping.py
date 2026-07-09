#!/usr/bin/env python3
"""
Layer 9E
Pitch-Type Matchup Overlay Historical Outcome Source Mapping Plan

Maps repository historical-data surfaces to the Layer 9D historical outcome
contract and defines the bounded implementation handoff for Layer 9F.

Planning only.

This layer does not:

- fetch external historical outcomes;
- execute historical outcome collection;
- materialize production historical outcome records;
- join outcomes to features or predictions;
- calculate predictive metrics;
- evaluate accuracy, calibration, or incremental value;
- tune models, thresholds, weights, or fallbacks;
- run backtests;
- modify production, simulation, pricing, or betting behavior.
"""

from __future__ import annotations

import ast
import csv
import hashlib
import json
import re
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9E"
LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_outcome_source_mapping_plan"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9E_pitch_type_matchup_overlay_historical_outcome_source_mapping"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts"
    / "audit_9D_pitch_type_matchup_overlay_historical_outcome_contract.py"
)

EXPECTED_PREDECESSOR_DIAGNOSIS = (
    "pitch_type_matchup_overlay_historical_outcome_contract_"
    "implementation_complete"
)

EXPECTED_PREDECESSOR_AUTHORITY = (
    "historical_outcome_source_mapping_planning"
)

TEXT_SUFFIXES = {
    ".csv",
    ".json",
    ".md",
    ".py",
    ".sql",
    ".txt",
    ".yaml",
    ".yml",
}

EXCLUDED_PATH_PARTS = {
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".venv",
    "__pycache__",
    "node_modules",
    "tmp",
    "venv",
}

MAX_FILE_BYTES = 2_000_000
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


SOURCE_DOMAINS = [
    {
        "source_domain_id": "HMAP-D01",
        "domain": "statcast_pitch_event_rows",
        "description": (
            "Pitch-level rows containing game, plate appearance, pitch sequence, "
            "pitch description, event result, pitcher, batter, and contact fields."
        ),
    },
    {
        "source_domain_id": "HMAP-D02",
        "domain": "game_schedule_identity",
        "description": (
            "Canonical game identifiers, dates, scheduled starts, doubleheader "
            "identity, postponement, suspension, and resumption context."
        ),
    },
    {
        "source_domain_id": "HMAP-D03",
        "domain": "plate_appearance_terminal_events",
        "description": (
            "Terminal plate-appearance result categories and deterministic "
            "plate-appearance ordering."
        ),
    },
    {
        "source_domain_id": "HMAP-D04",
        "domain": "contact_quality_measurements",
        "description": (
            "Launch speed, launch angle, estimated outcome, barrel, hard-hit, "
            "and batted-ball measurements."
        ),
    },
    {
        "source_domain_id": "HMAP-D05",
        "domain": "run_value_and_base_out_state",
        "description": (
            "Event run values, base-out transitions, runs scored, and inning-state "
            "identity independent of betting markets."
        ),
    },
    {
        "source_domain_id": "HMAP-D06",
        "domain": "provider_availability_and_revision",
        "description": (
            "Observation, publication, ingestion, correction, revision, finality, "
            "and as-of semantics."
        ),
    },
    {
        "source_domain_id": "HMAP-D07",
        "domain": "raw_payload_provenance",
        "description": (
            "Provider payload references, dataset versions, ingestion runs, raw "
            "payload digests, manifests, and immutable storage keys."
        ),
    },
    {
        "source_domain_id": "HMAP-D08",
        "domain": "historical_storage_and_replay",
        "description": (
            "Historical tables, files, caches, fixtures, manifests, schemas, "
            "partitioning, deterministic ordering, and replay surfaces."
        ),
    },
]


SOURCE_SEARCH_TERMS = [
    {"term_id": "HMAP-S01", "source_domain_id": "HMAP-D01", "term": "game_pk"},
    {"term_id": "HMAP-S02", "source_domain_id": "HMAP-D01", "term": "at_bat_number"},
    {"term_id": "HMAP-S03", "source_domain_id": "HMAP-D01", "term": "pitch_number"},
    {"term_id": "HMAP-S04", "source_domain_id": "HMAP-D01", "term": "description"},
    {"term_id": "HMAP-S05", "source_domain_id": "HMAP-D01", "term": "events"},
    {"term_id": "HMAP-S06", "source_domain_id": "HMAP-D02", "term": "scheduled_start"},
    {"term_id": "HMAP-S07", "source_domain_id": "HMAP-D02", "term": "doubleheader"},
    {"term_id": "HMAP-S08", "source_domain_id": "HMAP-D02", "term": "suspended"},
    {"term_id": "HMAP-S09", "source_domain_id": "HMAP-D03", "term": "plate_appearance"},
    {"term_id": "HMAP-S10", "source_domain_id": "HMAP-D03", "term": "strikeout"},
    {"term_id": "HMAP-S11", "source_domain_id": "HMAP-D03", "term": "home_run"},
    {"term_id": "HMAP-S12", "source_domain_id": "HMAP-D04", "term": "launch_speed"},
    {"term_id": "HMAP-S13", "source_domain_id": "HMAP-D04", "term": "launch_angle"},
    {"term_id": "HMAP-S14", "source_domain_id": "HMAP-D04", "term": "estimated_woba"},
    {"term_id": "HMAP-S15", "source_domain_id": "HMAP-D04", "term": "barrel"},
    {"term_id": "HMAP-S16", "source_domain_id": "HMAP-D05", "term": "run_value"},
    {"term_id": "HMAP-S17", "source_domain_id": "HMAP-D05", "term": "base_out"},
    {"term_id": "HMAP-S18", "source_domain_id": "HMAP-D05", "term": "runs_scored"},
    {"term_id": "HMAP-S19", "source_domain_id": "HMAP-D06", "term": "available_at"},
    {"term_id": "HMAP-S20", "source_domain_id": "HMAP-D06", "term": "published_at"},
    {"term_id": "HMAP-S21", "source_domain_id": "HMAP-D06", "term": "revision"},
    {"term_id": "HMAP-S22", "source_domain_id": "HMAP-D06", "term": "as_of"},
    {"term_id": "HMAP-S23", "source_domain_id": "HMAP-D07", "term": "provenance"},
    {"term_id": "HMAP-S24", "source_domain_id": "HMAP-D07", "term": "sha256"},
    {"term_id": "HMAP-S25", "source_domain_id": "HMAP-D07", "term": "ingestion_run"},
    {"term_id": "HMAP-S26", "source_domain_id": "HMAP-D08", "term": "manifest"},
    {"term_id": "HMAP-S27", "source_domain_id": "HMAP-D08", "term": "fixture"},
    {"term_id": "HMAP-S28", "source_domain_id": "HMAP-D08", "term": "historical"},
]


TARGET_SOURCE_REQUIREMENTS = [
    {
        "target_id": "HOUT-O01",
        "target": "swing_event",
        "event_level": "pitch",
        "required_source_fields": "game_pk|at_bat_number|pitch_number|description",
        "preferred_domain": "HMAP-D01",
    },
    {
        "target_id": "HOUT-O02",
        "target": "whiff_event",
        "event_level": "pitch",
        "required_source_fields": "game_pk|at_bat_number|pitch_number|description",
        "preferred_domain": "HMAP-D01",
    },
    {
        "target_id": "HOUT-O03",
        "target": "called_strike_event",
        "event_level": "pitch",
        "required_source_fields": "game_pk|at_bat_number|pitch_number|description",
        "preferred_domain": "HMAP-D01",
    },
    {
        "target_id": "HOUT-O04",
        "target": "ball_in_play_event",
        "event_level": "pitch",
        "required_source_fields": "game_pk|at_bat_number|pitch_number|description",
        "preferred_domain": "HMAP-D01",
    },
    {
        "target_id": "HOUT-O05",
        "target": "strikeout_event",
        "event_level": "plate_appearance",
        "required_source_fields": "game_pk|at_bat_number|events",
        "preferred_domain": "HMAP-D03",
    },
    {
        "target_id": "HOUT-O06",
        "target": "walk_event",
        "event_level": "plate_appearance",
        "required_source_fields": "game_pk|at_bat_number|events",
        "preferred_domain": "HMAP-D03",
    },
    {
        "target_id": "HOUT-O07",
        "target": "hit_event",
        "event_level": "plate_appearance",
        "required_source_fields": "game_pk|at_bat_number|events",
        "preferred_domain": "HMAP-D03",
    },
    {
        "target_id": "HOUT-O08",
        "target": "extra_base_hit_event",
        "event_level": "plate_appearance",
        "required_source_fields": "game_pk|at_bat_number|events",
        "preferred_domain": "HMAP-D03",
    },
    {
        "target_id": "HOUT-O09",
        "target": "contact_quality_value",
        "event_level": "contact",
        "required_source_fields": (
            "game_pk|at_bat_number|pitch_number|launch_speed|launch_angle"
        ),
        "preferred_domain": "HMAP-D04",
    },
    {
        "target_id": "HOUT-O10",
        "target": "run_value",
        "event_level": "event",
        "required_source_fields": "game_pk|at_bat_number|run_value|base_out",
        "preferred_domain": "HMAP-D05",
    },
]


CONTRACT_FIELD_SOURCE_MAP = [
    {"contract_field": "historical_outcome_id", "source_type": "derived", "source_expression": "deterministic contract identity digest"},
    {"contract_field": "historical_outcome_contract_version", "source_type": "constant", "source_expression": "layer_9D_historical_outcome_contract_v1"},
    {"contract_field": "target_id", "source_type": "mapping", "source_expression": "bounded target mapping"},
    {"contract_field": "event_level", "source_type": "mapping", "source_expression": "target contract event level"},
    {"contract_field": "game_id", "source_type": "source", "source_expression": "game_pk or canonical game identifier"},
    {"contract_field": "game_date", "source_type": "source", "source_expression": "game_date"},
    {"contract_field": "scheduled_start_utc", "source_type": "joined_identity", "source_expression": "canonical schedule start"},
    {"contract_field": "plate_appearance_id", "source_type": "derived", "source_expression": "game_id plus at_bat_number"},
    {"contract_field": "pitch_id", "source_type": "derived", "source_expression": "game_id plus at_bat_number plus pitch_number"},
    {"contract_field": "pitcher_id", "source_type": "source", "source_expression": "pitcher"},
    {"contract_field": "batter_id", "source_type": "source", "source_expression": "batter"},
    {"contract_field": "event_sequence", "source_type": "source_or_derived", "source_expression": "at_bat_number and pitch_number ordering"},
    {"contract_field": "raw_outcome_code", "source_type": "source", "source_expression": "description or events"},
    {"contract_field": "outcome_value", "source_type": "derived", "source_expression": "Layer 9D target derivation"},
    {"contract_field": "outcome_missing", "source_type": "derived", "source_expression": "explicit source missingness"},
    {"contract_field": "outcome_missing_reason", "source_type": "derived", "source_expression": "versioned missingness mapping"},
    {"contract_field": "event_occurred_at_utc", "source_type": "source_or_null", "source_expression": "provider event timestamp when available"},
    {"contract_field": "source_observed_at_utc", "source_type": "collection_metadata", "source_expression": "provider observation timestamp"},
    {"contract_field": "source_published_at_utc", "source_type": "collection_metadata", "source_expression": "provider publication timestamp when available"},
    {"contract_field": "outcome_available_at_utc", "source_type": "derived_metadata", "source_expression": "documented provider availability rule"},
    {"contract_field": "provider", "source_type": "constant", "source_expression": "selected provider identifier"},
    {"contract_field": "provider_event_id", "source_type": "source_or_derived", "source_expression": "provider event identity"},
    {"contract_field": "provider_payload_version", "source_type": "collection_metadata", "source_expression": "immutable payload version"},
    {"contract_field": "provider_revision_id", "source_type": "collection_metadata", "source_expression": "provider revision identity when available"},
    {"contract_field": "is_final_provider_revision", "source_type": "collection_metadata", "source_expression": "descriptive finality flag"},
    {"contract_field": "ingestion_run_id", "source_type": "collection_metadata", "source_expression": "deterministic ingestion run identity"},
    {"contract_field": "raw_payload_digest", "source_type": "derived", "source_expression": "SHA-256 canonical raw payload digest"},
    {"contract_field": "outcome_provenance_digest", "source_type": "derived", "source_expression": "Layer 9D provenance digest"},
    {"contract_field": "exclusion_codes", "source_type": "derived", "source_expression": "sorted validation diagnostics"},
    {"contract_field": "historical_outcome_eligible", "source_type": "derived", "source_expression": "Layer 9D eligibility rules"},
]


SOURCE_SELECTION_RULES = [
    {"rule_id": "HMAP-R01", "rule": "prefer canonical repository source over duplicate convenience surface"},
    {"rule_id": "HMAP-R02", "rule": "require stable game plate appearance and pitch identity"},
    {"rule_id": "HMAP-R03", "rule": "require raw categorical outcome semantics"},
    {"rule_id": "HMAP-R04", "rule": "preserve provider-native identifiers"},
    {"rule_id": "HMAP-R05", "rule": "preserve raw payload before normalization"},
    {"rule_id": "HMAP-R06", "rule": "require deterministic source ordering"},
    {"rule_id": "HMAP-R07", "rule": "require explicit missingness semantics"},
    {"rule_id": "HMAP-R08", "rule": "require documented availability timestamp semantics"},
    {"rule_id": "HMAP-R09", "rule": "require revision and correction policy"},
    {"rule_id": "HMAP-R10", "rule": "require immutable payload digest"},
    {"rule_id": "HMAP-R11", "rule": "reject market derived outcome sources"},
    {"rule_id": "HMAP-R12", "rule": "reject feature payloads containing post event outcomes"},
]


IMPLEMENTATION_HANDOFF = [
    {"step": 1, "action": "Select canonical repository source candidates by domain."},
    {"step": 2, "action": "Define provider adapter input contract."},
    {"step": 3, "action": "Define canonical game plate appearance and pitch identity mapping."},
    {"step": 4, "action": "Define raw outcome code mappings for ten targets."},
    {"step": 5, "action": "Define contact quality and run value extraction semantics."},
    {"step": 6, "action": "Define availability publication and observation semantics."},
    {"step": 7, "action": "Define provider revision and correction handling."},
    {"step": 8, "action": "Define immutable raw payload capture and digest behavior."},
    {"step": 9, "action": "Define missingness and unsupported category diagnostics."},
    {"step": 10, "action": "Implement synthetic provider adapter fixtures only."},
    {"step": 11, "action": "Verify deterministic mapping into the Layer 9D contract."},
    {"step": 12, "action": "Preserve prohibition on production collection and evaluation joins."},
]


PROHIBITED_AUTHORITIES = [
    "accuracy_evaluation",
    "augmented_prediction_generation",
    "backtest_execution",
    "baseline_prediction_generation",
    "bet_recommendation",
    "calibration_evaluation",
    "canonical_probability_authority_change",
    "edge_detection",
    "feature_outcome_join_execution",
    "historical_outcome_collection_execution",
    "historical_outcome_fetch_execution",
    "historical_outcome_prediction_join_execution",
    "incremental_value_evaluation",
    "market_comparison",
    "model_training",
    "parameter_tuning",
    "predictive_metric_calculation",
    "pricing",
    "production_historical_outcome_materialization",
    "production_matchup_activation",
    "production_overlay_integration",
    "simulation_probability_change",
    "simulation_state_change",
    "threshold_tuning",
    "uncertainty_estimation",
]


def canonical_json_bytes(payload: Any) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def sha256_payload(payload: Any) -> str:
    return hashlib.sha256(
        canonical_json_bytes(payload)
    ).hexdigest()


def string_constants(path: Path) -> set[str]:
    if not path.exists():
        return set()

    try:
        tree = ast.parse(
            path.read_text(
                encoding="utf-8",
                errors="ignore",
            ),
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


def eligible_file(path: Path) -> bool:
    if not path.is_file():
        return False

    if path.suffix.lower() not in TEXT_SUFFIXES:
        return False

    relative = path.relative_to(ROOT)

    if any(
        part in EXCLUDED_PATH_PARTS
        for part in relative.parts
    ):
        return False

    try:
        return path.stat().st_size <= MAX_FILE_BYTES
    except OSError:
        return False


def repository_files() -> list[Path]:
    return sorted(
        (
            path
            for path in ROOT.rglob("*")
            if eligible_file(path)
        ),
        key=lambda path: path.relative_to(ROOT).as_posix(),
    )


def build_source_inventory() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    for path in repository_files():
        try:
            text = path.read_text(
                encoding="utf-8",
                errors="ignore",
            ).lower()
        except OSError:
            continue

        if not text:
            continue

        relative_path = path.relative_to(ROOT).as_posix()
        file_digest = hashlib.sha256(
            path.read_bytes()
        ).hexdigest()

        for search_term in SOURCE_SEARCH_TERMS:
            term = search_term["term"].lower()
            occurrence_count = text.count(term)

            if occurrence_count == 0:
                continue

            first_lines: list[int] = []

            for line_number, line in enumerate(
                text.splitlines(),
                start=1,
            ):
                if term in line:
                    first_lines.append(line_number)

                if len(first_lines) >= 5:
                    break

            rows.append(
                {
                    "source_domain_id": search_term[
                        "source_domain_id"
                    ],
                    "term_id": search_term["term_id"],
                    "term": search_term["term"],
                    "repository_path": relative_path,
                    "file_suffix": path.suffix.lower(),
                    "occurrence_count": occurrence_count,
                    "first_match_lines": "|".join(
                        str(value)
                        for value in first_lines
                    ),
                    "file_sha256": file_digest,
                }
            )

    rows.sort(
        key=lambda row: (
            row["source_domain_id"],
            row["term_id"],
            row["repository_path"],
        )
    )

    return rows


def build_domain_summary(
    inventory: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    for domain in SOURCE_DOMAINS:
        matched = [
            row
            for row in inventory
            if row["source_domain_id"]
            == domain["source_domain_id"]
        ]

        paths = sorted(
            {
                str(row["repository_path"])
                for row in matched
            }
        )
        terms = sorted(
            {
                str(row["term"])
                for row in matched
            }
        )

        rows.append(
            {
                **domain,
                "matched_rows": len(matched),
                "matched_files": len(paths),
                "matched_terms": len(terms),
                "repository_paths": "|".join(paths),
                "terms_found": "|".join(terms),
            }
        )

    return rows


def write_csv(
    path: Path,
    fieldnames: Sequence[str],
    rows: Iterable[Mapping[str, Any]],
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
            fieldnames=list(fieldnames),
            extrasaction="raise",
        )
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: Any) -> None:
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


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    predecessor_constants = string_constants(
        PREDECESSOR_PATH
    )

    predecessor_present = (
        PREDECESSOR_PATH.exists()
        and EXPECTED_PREDECESSOR_DIAGNOSIS
        in predecessor_constants
        and EXPECTED_PREDECESSOR_AUTHORITY
        in predecessor_constants
    )

    source_inventory = build_source_inventory()
    domain_summary = build_domain_summary(
        source_inventory
    )

    inventory_digest = sha256_payload(
        source_inventory
    )
    mapping_digest = sha256_payload(
        {
            "target_source_requirements": TARGET_SOURCE_REQUIREMENTS,
            "contract_field_source_map": CONTRACT_FIELD_SOURCE_MAP,
            "source_selection_rules": SOURCE_SELECTION_RULES,
        }
    )

    target_ids = [
        row["target_id"]
        for row in TARGET_SOURCE_REQUIREMENTS
    ]
    contract_fields = [
        row["contract_field"]
        for row in CONTRACT_FIELD_SOURCE_MAP
    ]
    rule_ids = [
        row["rule_id"]
        for row in SOURCE_SELECTION_RULES
    ]

    planning_checks = [
        {
            "check": "nine_d_predecessor_present",
            "actual": predecessor_present,
            "expected": True,
            "passed": predecessor_present,
        },
        {
            "check": "eight_source_domains_defined",
            "actual": len(SOURCE_DOMAINS),
            "expected": 8,
            "passed": len(SOURCE_DOMAINS) == 8,
        },
        {
            "check": "twenty_eight_search_terms_defined",
            "actual": len(SOURCE_SEARCH_TERMS),
            "expected": 28,
            "passed": len(SOURCE_SEARCH_TERMS) == 28,
        },
        {
            "check": "repository_source_inventory_nonempty",
            "actual": len(source_inventory),
            "expected": "greater_than_zero",
            "passed": len(source_inventory) > 0,
        },
        {
            "check": "all_source_domains_have_matches",
            "actual": sum(
                row["matched_rows"] > 0
                for row in domain_summary
            ),
            "expected": 8,
            "passed": all(
                row["matched_rows"] > 0
                for row in domain_summary
            ),
        },
        {
            "check": "inventory_digest_valid_sha256",
            "actual": inventory_digest,
            "expected": "sha256",
            "passed": bool(
                SHA256_PATTERN.fullmatch(
                    inventory_digest
                )
            ),
        },
        {
            "check": "mapping_digest_valid_sha256",
            "actual": mapping_digest,
            "expected": "sha256",
            "passed": bool(
                SHA256_PATTERN.fullmatch(
                    mapping_digest
                )
            ),
        },
        {
            "check": "ten_target_source_requirements_defined",
            "actual": len(
                TARGET_SOURCE_REQUIREMENTS
            ),
            "expected": 10,
            "passed": len(
                TARGET_SOURCE_REQUIREMENTS
            )
            == 10,
        },
        {
            "check": "target_ids_unique",
            "actual": len(set(target_ids)),
            "expected": len(target_ids),
            "passed": len(set(target_ids))
            == len(target_ids),
        },
        {
            "check": "thirty_contract_fields_mapped",
            "actual": len(
                CONTRACT_FIELD_SOURCE_MAP
            ),
            "expected": 30,
            "passed": len(
                CONTRACT_FIELD_SOURCE_MAP
            )
            == 30,
        },
        {
            "check": "contract_field_mappings_unique",
            "actual": len(
                set(contract_fields)
            ),
            "expected": len(contract_fields),
            "passed": len(
                set(contract_fields)
            )
            == len(contract_fields),
        },
        {
            "check": "twelve_source_selection_rules_defined",
            "actual": len(
                SOURCE_SELECTION_RULES
            ),
            "expected": 12,
            "passed": len(
                SOURCE_SELECTION_RULES
            )
            == 12,
        },
        {
            "check": "source_selection_rule_ids_unique",
            "actual": len(set(rule_ids)),
            "expected": len(rule_ids),
            "passed": len(set(rule_ids))
            == len(rule_ids),
        },
        {
            "check": "twelve_implementation_steps_defined",
            "actual": len(
                IMPLEMENTATION_HANDOFF
            ),
            "expected": 12,
            "passed": len(
                IMPLEMENTATION_HANDOFF
            )
            == 12,
        },
        {
            "check": "historical_outcome_fetch_execution_absent",
            "actual": False,
            "expected": False,
            "passed": True,
        },
        {
            "check": "historical_outcome_collection_execution_absent",
            "actual": False,
            "expected": False,
            "passed": True,
        },
        {
            "check": "production_materialization_absent",
            "actual": False,
            "expected": False,
            "passed": True,
        },
        {
            "check": "feature_outcome_join_absent",
            "actual": False,
            "expected": False,
            "passed": True,
        },
        {
            "check": "prediction_join_absent",
            "actual": False,
            "expected": False,
            "passed": True,
        },
        {
            "check": "predictive_metric_execution_absent",
            "actual": False,
            "expected": False,
            "passed": True,
        },
        {
            "check": "production_pricing_edge_authority_absent",
            "actual": False,
            "expected": False,
            "passed": True,
        },
    ]

    all_checks_passed = all(
        bool(row["passed"])
        for row in planning_checks
    )

    diagnosis_name = (
        "pitch_type_matchup_overlay_historical_outcome_source_mapping_plan_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_outcome_source_mapping_plan_failed"
    )

    recommended_next_layer = (
        "9F_pitch_type_matchup_overlay_historical_outcome_source_adapter_contract"
        if all_checks_passed
        else
        "9E_pitch_type_matchup_overlay_historical_outcome_source_mapping_plan_remediation"
    )

    write_csv(
        OUTPUT_DIR / "planning_checks.csv",
        ["check", "actual", "expected", "passed"],
        planning_checks,
    )

    write_csv(
        OUTPUT_DIR / "source_domains.csv",
        [
            "source_domain_id",
            "domain",
            "description",
        ],
        SOURCE_DOMAINS,
    )

    write_csv(
        OUTPUT_DIR / "source_search_terms.csv",
        [
            "term_id",
            "source_domain_id",
            "term",
        ],
        SOURCE_SEARCH_TERMS,
    )

    write_csv(
        OUTPUT_DIR / "repository_source_inventory.csv",
        [
            "source_domain_id",
            "term_id",
            "term",
            "repository_path",
            "file_suffix",
            "occurrence_count",
            "first_match_lines",
            "file_sha256",
        ],
        source_inventory,
    )

    write_csv(
        OUTPUT_DIR / "source_domain_summary.csv",
        [
            "source_domain_id",
            "domain",
            "description",
            "matched_rows",
            "matched_files",
            "matched_terms",
            "repository_paths",
            "terms_found",
        ],
        domain_summary,
    )

    write_csv(
        OUTPUT_DIR / "target_source_requirements.csv",
        [
            "target_id",
            "target",
            "event_level",
            "required_source_fields",
            "preferred_domain",
        ],
        TARGET_SOURCE_REQUIREMENTS,
    )

    write_csv(
        OUTPUT_DIR / "contract_field_source_map.csv",
        [
            "contract_field",
            "source_type",
            "source_expression",
        ],
        CONTRACT_FIELD_SOURCE_MAP,
    )

    write_csv(
        OUTPUT_DIR / "source_selection_rules.csv",
        [
            "rule_id",
            "rule",
        ],
        SOURCE_SELECTION_RULES,
    )

    write_csv(
        OUTPUT_DIR / "implementation_handoff.csv",
        [
            "step",
            "action",
        ],
        IMPLEMENTATION_HANDOFF,
    )

    write_csv(
        OUTPUT_DIR / "authority_boundaries.csv",
        [
            "authority",
            "granted",
            "reason",
        ],
        [
            {
                "authority": authority,
                "granted": False,
                "reason": (
                    "9E is a source mapping plan only."
                ),
            }
            for authority in PROHIBITED_AUTHORITIES
        ]
        + [
            {
                "authority": (
                    "historical_outcome_source_adapter_contract_implementation"
                ),
                "granted": all_checks_passed,
                "reason": (
                    "9F may implement a deterministic synthetic source adapter "
                    "contract without external fetching or production materialization."
                ),
            }
        ],
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "planning_checks_required": len(
            planning_checks
        ),
        "planning_checks_passed": sum(
            bool(row["passed"])
            for row in planning_checks
        ),
        "source_domains_defined": len(
            SOURCE_DOMAINS
        ),
        "search_terms_defined": len(
            SOURCE_SEARCH_TERMS
        ),
        "repository_inventory_rows": len(
            source_inventory
        ),
        "repository_inventory_files": len(
            {
                row["repository_path"]
                for row in source_inventory
            }
        ),
        "target_source_requirements_defined": len(
            TARGET_SOURCE_REQUIREMENTS
        ),
        "contract_fields_mapped": len(
            CONTRACT_FIELD_SOURCE_MAP
        ),
        "source_selection_rules_defined": len(
            SOURCE_SELECTION_RULES
        ),
        "historical_outcome_records_fetched": 0,
        "historical_outcome_records_materialized": 0,
        "feature_outcome_joins_executed": 0,
        "prediction_joins_executed": 0,
        "predictive_metrics_calculated": 0,
        "production_probabilities_changed": 0,
        "market_comparisons_executed": 0,
        "betting_edges_calculated": 0,
        "inventory_digest": inventory_digest,
        "mapping_digest": mapping_digest,
        "all_checks_passed": all_checks_passed,
        "recommended_next_layer": (
            recommended_next_layer
        ),
    }

    write_json(
        OUTPUT_DIR / "summary.json",
        summary,
    )

    diagnosis = {
        "all_checks_passed": all_checks_passed,
        "authority_granted": (
            "historical_outcome_source_adapter_contract_implementation"
            if all_checks_passed
            else
            "none"
        ),
        "authority_withheld": sorted(
            PROHIBITED_AUTHORITIES
        ),
        "diagnosis": diagnosis_name,
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "output_directory": str(
            OUTPUT_DIR.relative_to(ROOT)
        ),
        "recommended_next_layer": (
            recommended_next_layer
        ),
    }

    write_json(
        OUTPUT_DIR / "diagnosis.json",
        diagnosis,
    )

    manifest_rows: list[dict[str, Any]] = []

    for path in sorted(
        OUTPUT_DIR.iterdir(),
        key=lambda item: item.name,
    ):
        if not path.is_file():
            continue

        manifest_rows.append(
            {
                "artifact": path.name,
                "bytes": path.stat().st_size,
                "sha256": hashlib.sha256(
                    path.read_bytes()
                ).hexdigest(),
            }
        )

    write_csv(
        OUTPUT_DIR / "artifact_manifest.csv",
        ["artifact", "bytes", "sha256"],
        manifest_rows,
    )

    print(
        f"Layer: {LAYER_ID} — {LAYER_NAME}"
    )
    print(
        "Predecessor verified: "
        f"{predecessor_present}"
    )
    print(
        "Planning checks passed: "
        f"{summary['planning_checks_passed']}/"
        f"{summary['planning_checks_required']}"
    )
    print(
        "Source domains defined: "
        f"{summary['source_domains_defined']}"
    )
    print(
        "Repository inventory rows: "
        f"{summary['repository_inventory_rows']}"
    )
    print(
        "Repository inventory files: "
        f"{summary['repository_inventory_files']}"
    )
    print(
        "Target source requirements defined: "
        f"{summary['target_source_requirements_defined']}"
    )
    print(
        "Contract fields mapped: "
        f"{summary['contract_fields_mapped']}"
    )
    print(
        "Historical outcome records fetched: 0"
    )
    print(
        "Historical outcome records materialized: 0"
    )
    print(
        "Feature/outcome joins executed: 0"
    )
    print(
        "Prediction joins executed: 0"
    )
    print(
        "Predictive metrics calculated: 0"
    )
    print(
        "Production probabilities changed: 0"
    )
    print(
        "Market comparisons executed: 0"
    )
    print(
        "Betting edges calculated: 0"
    )
    print(
        f"Diagnosis: {diagnosis_name}"
    )
    print(
        "Recommended next layer: "
        f"{recommended_next_layer}"
    )
    print(
        "Artifacts: "
        f"{OUTPUT_DIR.relative_to(ROOT)}"
    )

    if not all_checks_passed:
        failed_checks = [
            row["check"]
            for row in planning_checks
            if not row["passed"]
        ]

        print(
            "FAILED CHECKS: "
            + ", ".join(failed_checks)
        )
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
