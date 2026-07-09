#!/usr/bin/env python3
"""
Layer 9C
Pitch-Type Matchup Overlay Historical Outcome Inventory Plan

Inventories repository surfaces that may support deterministic, leakage-safe
historical outcome construction for Layer 9 evaluation and defines the bounded
handoff contract for Layer 9D.

Planning only.

This layer does not:

- ingest or fetch historical outcomes;
- materialize historical outcome records;
- join outcomes to Layer 8 features;
- join outcomes to baseline or augmented predictions;
- calculate predictive metrics;
- evaluate accuracy, calibration, or incremental value;
- train or tune models, weights, thresholds, or fallbacks;
- run backtests;
- activate the Layer 8 overlay in production;
- modify simulation or canonical probabilities;
- compare projections with betting markets;
- price wagers, detect edges, or recommend bets.
"""

from __future__ import annotations

import ast
import csv
import hashlib
import json
import re
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9C"
LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_outcome_inventory_plan"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9C_pitch_type_matchup_overlay_historical_outcome_inventory"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts"
    / "audit_9B_pitch_type_matchup_overlay_point_in_time_historical_evaluation_contract.py"
)

EXPECTED_PREDECESSOR_DIAGNOSIS = (
    "pitch_type_matchup_overlay_point_in_time_historical_"
    "evaluation_contract_implementation_complete"
)

EXPECTED_PREDECESSOR_AUTHORITY = (
    "historical_outcome_inventory_planning"
)

TEXT_SUFFIXES = {
    ".cfg",
    ".csv",
    ".ini",
    ".json",
    ".md",
    ".py",
    ".sql",
    ".toml",
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


INVENTORY_DOMAINS = [
    {
        "domain_id": "HOUT-I01",
        "domain": "game_identity",
        "inventory_objective": (
            "Locate canonical game identifiers, game dates, scheduled start "
            "times, teams, venues, doubleheader numbers, postponements, "
            "resumptions, and rescheduled-game lineage."
        ),
    },
    {
        "domain_id": "HOUT-I02",
        "domain": "plate_appearance_identity",
        "inventory_objective": (
            "Locate deterministic game-scoped plate-appearance identifiers, "
            "batter and pitcher identifiers, inning, half-inning, outs, batting "
            "order, event sequence, and terminal-event identity."
        ),
    },
    {
        "domain_id": "HOUT-I03",
        "domain": "pitch_identity",
        "inventory_objective": (
            "Locate deterministic game- and plate-appearance-scoped pitch "
            "identifiers, pitch sequence, count state, pitch type, description, "
            "result, and provider identity."
        ),
    },
    {
        "domain_id": "HOUT-I04",
        "domain": "pitch_outcomes",
        "inventory_objective": (
            "Locate swing, whiff, called-strike, foul, ball-in-play, hit-by-pitch, "
            "and other pitch-result fields with explicit categorical semantics."
        ),
    },
    {
        "domain_id": "HOUT-I05",
        "domain": "plate_appearance_outcomes",
        "inventory_objective": (
            "Locate terminal plate-appearance events including strikeout, walk, "
            "hit, extra-base hit, home run, sacrifice, interference, error, and "
            "other official result categories."
        ),
    },
    {
        "domain_id": "HOUT-I06",
        "domain": "contact_quality_outcomes",
        "inventory_objective": (
            "Locate batted-ball type, launch speed, launch angle, spray direction, "
            "barrel or hard-hit indicators, expected outcomes, and explicit "
            "missingness semantics."
        ),
    },
    {
        "domain_id": "HOUT-I07",
        "domain": "run_value_outcomes",
        "inventory_objective": (
            "Locate runs scored, RBI-independent event run value, base-out state "
            "transition, scoring-play identity, and non-market win-probability "
            "change surfaces."
        ),
    },
    {
        "domain_id": "HOUT-I08",
        "domain": "source_availability_timestamps",
        "inventory_objective": (
            "Locate event occurrence, source observation, provider publication, "
            "ingestion, collection, correction, and finalization timestamps."
        ),
    },
    {
        "domain_id": "HOUT-I09",
        "domain": "revision_and_as_of_semantics",
        "inventory_objective": (
            "Locate immutable payload versions, provider revisions, correction "
            "history, supersession keys, as-of retrieval semantics, and original "
            "versus final-value distinctions."
        ),
    },
    {
        "domain_id": "HOUT-I10",
        "domain": "provenance_and_payload_digests",
        "inventory_objective": (
            "Locate provider names, source URLs or keys, dataset versions, "
            "ingestion-run identifiers, raw payload references, and immutable "
            "content digests."
        ),
    },
    {
        "domain_id": "HOUT-I11",
        "domain": "missingness_and_exclusion_semantics",
        "inventory_objective": (
            "Locate explicit handling for missing pitches, incomplete games, "
            "suspended games, null contact measurements, provider gaps, identity "
            "conflicts, and unsupported events."
        ),
    },
    {
        "domain_id": "HOUT-I12",
        "domain": "historical_storage_and_partitioning",
        "inventory_objective": (
            "Locate historical files, tables, caches, manifests, partitions, "
            "schemas, retention rules, and deterministic ordering conventions."
        ),
    },
]


SEARCH_TERMS = [
    {
        "term_id": "HOUT-S01",
        "domain_id": "HOUT-I01",
        "term": "game_id",
        "category": "identity",
    },
    {
        "term_id": "HOUT-S02",
        "domain_id": "HOUT-I01",
        "term": "game_pk",
        "category": "identity",
    },
    {
        "term_id": "HOUT-S03",
        "domain_id": "HOUT-I01",
        "term": "doubleheader",
        "category": "identity",
    },
    {
        "term_id": "HOUT-S04",
        "domain_id": "HOUT-I01",
        "term": "scheduled_start",
        "category": "timestamp",
    },
    {
        "term_id": "HOUT-S05",
        "domain_id": "HOUT-I02",
        "term": "plate_appearance",
        "category": "identity",
    },
    {
        "term_id": "HOUT-S06",
        "domain_id": "HOUT-I02",
        "term": "at_bat",
        "category": "identity",
    },
    {
        "term_id": "HOUT-S07",
        "domain_id": "HOUT-I03",
        "term": "pitch_id",
        "category": "identity",
    },
    {
        "term_id": "HOUT-S08",
        "domain_id": "HOUT-I03",
        "term": "pitch_number",
        "category": "sequence",
    },
    {
        "term_id": "HOUT-S09",
        "domain_id": "HOUT-I04",
        "term": "whiff",
        "category": "pitch_outcome",
    },
    {
        "term_id": "HOUT-S10",
        "domain_id": "HOUT-I04",
        "term": "called_strike",
        "category": "pitch_outcome",
    },
    {
        "term_id": "HOUT-S11",
        "domain_id": "HOUT-I04",
        "term": "ball_in_play",
        "category": "pitch_outcome",
    },
    {
        "term_id": "HOUT-S12",
        "domain_id": "HOUT-I05",
        "term": "strikeout",
        "category": "plate_appearance_outcome",
    },
    {
        "term_id": "HOUT-S13",
        "domain_id": "HOUT-I05",
        "term": "walk",
        "category": "plate_appearance_outcome",
    },
    {
        "term_id": "HOUT-S14",
        "domain_id": "HOUT-I05",
        "term": "home_run",
        "category": "plate_appearance_outcome",
    },
    {
        "term_id": "HOUT-S15",
        "domain_id": "HOUT-I05",
        "term": "event_type",
        "category": "plate_appearance_outcome",
    },
    {
        "term_id": "HOUT-S16",
        "domain_id": "HOUT-I06",
        "term": "launch_speed",
        "category": "contact_quality",
    },
    {
        "term_id": "HOUT-S17",
        "domain_id": "HOUT-I06",
        "term": "launch_angle",
        "category": "contact_quality",
    },
    {
        "term_id": "HOUT-S18",
        "domain_id": "HOUT-I06",
        "term": "estimated_woba",
        "category": "contact_quality",
    },
    {
        "term_id": "HOUT-S19",
        "domain_id": "HOUT-I06",
        "term": "barrel",
        "category": "contact_quality",
    },
    {
        "term_id": "HOUT-S20",
        "domain_id": "HOUT-I07",
        "term": "run_value",
        "category": "run_value",
    },
    {
        "term_id": "HOUT-S21",
        "domain_id": "HOUT-I07",
        "term": "runs_scored",
        "category": "run_value",
    },
    {
        "term_id": "HOUT-S22",
        "domain_id": "HOUT-I07",
        "term": "base_out",
        "category": "state_transition",
    },
    {
        "term_id": "HOUT-S23",
        "domain_id": "HOUT-I08",
        "term": "available_at",
        "category": "timestamp",
    },
    {
        "term_id": "HOUT-S24",
        "domain_id": "HOUT-I08",
        "term": "published_at",
        "category": "timestamp",
    },
    {
        "term_id": "HOUT-S25",
        "domain_id": "HOUT-I08",
        "term": "ingested_at",
        "category": "timestamp",
    },
    {
        "term_id": "HOUT-S26",
        "domain_id": "HOUT-I09",
        "term": "as_of",
        "category": "revision",
    },
    {
        "term_id": "HOUT-S27",
        "domain_id": "HOUT-I09",
        "term": "revision",
        "category": "revision",
    },
    {
        "term_id": "HOUT-S28",
        "domain_id": "HOUT-I09",
        "term": "superseded",
        "category": "revision",
    },
    {
        "term_id": "HOUT-S29",
        "domain_id": "HOUT-I10",
        "term": "provenance",
        "category": "provenance",
    },
    {
        "term_id": "HOUT-S30",
        "domain_id": "HOUT-I10",
        "term": "sha256",
        "category": "provenance",
    },
    {
        "term_id": "HOUT-S31",
        "domain_id": "HOUT-I10",
        "term": "source_version",
        "category": "provenance",
    },
    {
        "term_id": "HOUT-S32",
        "domain_id": "HOUT-I11",
        "term": "missing",
        "category": "missingness",
    },
    {
        "term_id": "HOUT-S33",
        "domain_id": "HOUT-I11",
        "term": "suspended",
        "category": "missingness",
    },
    {
        "term_id": "HOUT-S34",
        "domain_id": "HOUT-I11",
        "term": "incomplete",
        "category": "missingness",
    },
    {
        "term_id": "HOUT-S35",
        "domain_id": "HOUT-I12",
        "term": "partition",
        "category": "storage",
    },
    {
        "term_id": "HOUT-S36",
        "domain_id": "HOUT-I12",
        "term": "manifest",
        "category": "storage",
    },
    {
        "term_id": "HOUT-S37",
        "domain_id": "HOUT-I12",
        "term": "historical",
        "category": "storage",
    },
]


OUTCOME_TARGET_CONTRACT = [
    {
        "target_id": "HOUT-O01",
        "event_level": "pitch",
        "target": "swing_event",
        "target_type": "binary",
        "source_semantics": (
            "Derived only from explicit pitch-description categories."
        ),
        "required_identity": "game_id|plate_appearance_id|pitch_id",
    },
    {
        "target_id": "HOUT-O02",
        "event_level": "pitch",
        "target": "whiff_event",
        "target_type": "binary",
        "source_semantics": (
            "Derived only from explicit swinging-strike categories."
        ),
        "required_identity": "game_id|plate_appearance_id|pitch_id",
    },
    {
        "target_id": "HOUT-O03",
        "event_level": "pitch",
        "target": "called_strike_event",
        "target_type": "binary",
        "source_semantics": (
            "Derived only from explicit called-strike categories."
        ),
        "required_identity": "game_id|plate_appearance_id|pitch_id",
    },
    {
        "target_id": "HOUT-O04",
        "event_level": "pitch",
        "target": "ball_in_play_event",
        "target_type": "binary",
        "source_semantics": (
            "Derived only from explicit in-play pitch descriptions."
        ),
        "required_identity": "game_id|plate_appearance_id|pitch_id",
    },
    {
        "target_id": "HOUT-O05",
        "event_level": "plate_appearance",
        "target": "strikeout_event",
        "target_type": "binary",
        "source_semantics": (
            "Derived from the official terminal plate-appearance result."
        ),
        "required_identity": "game_id|plate_appearance_id",
    },
    {
        "target_id": "HOUT-O06",
        "event_level": "plate_appearance",
        "target": "walk_event",
        "target_type": "binary",
        "source_semantics": (
            "Derived from the official terminal plate-appearance result with "
            "intentional-walk semantics preserved separately."
        ),
        "required_identity": "game_id|plate_appearance_id",
    },
    {
        "target_id": "HOUT-O07",
        "event_level": "plate_appearance",
        "target": "hit_event",
        "target_type": "binary",
        "source_semantics": (
            "Derived from official single, double, triple, or home-run results."
        ),
        "required_identity": "game_id|plate_appearance_id",
    },
    {
        "target_id": "HOUT-O08",
        "event_level": "plate_appearance",
        "target": "extra_base_hit_event",
        "target_type": "binary",
        "source_semantics": (
            "Derived from official double, triple, or home-run results."
        ),
        "required_identity": "game_id|plate_appearance_id",
    },
    {
        "target_id": "HOUT-O09",
        "event_level": "contact",
        "target": "contact_quality_value",
        "target_type": "continuous",
        "source_semantics": (
            "Uses an explicitly versioned contact-quality measure; absent "
            "measurements remain missing and are never silently imputed."
        ),
        "required_identity": "game_id|plate_appearance_id|pitch_id",
    },
    {
        "target_id": "HOUT-O10",
        "event_level": "event",
        "target": "run_value",
        "target_type": "continuous",
        "source_semantics": (
            "Uses an explicitly versioned event run-value definition independent "
            "of betting markets."
        ),
        "required_identity": "game_id|plate_appearance_id",
    },
]


HISTORICAL_OUTCOME_RECORD_FIELDS = [
    {
        "ordinal": 1,
        "field": "historical_outcome_id",
        "type": "deterministic_string",
        "required": True,
    },
    {
        "ordinal": 2,
        "field": "historical_outcome_contract_version",
        "type": "string",
        "required": True,
    },
    {
        "ordinal": 3,
        "field": "target_id",
        "type": "enum",
        "required": True,
    },
    {
        "ordinal": 4,
        "field": "event_level",
        "type": "enum",
        "required": True,
    },
    {
        "ordinal": 5,
        "field": "game_id",
        "type": "string",
        "required": True,
    },
    {
        "ordinal": 6,
        "field": "game_date",
        "type": "date",
        "required": True,
    },
    {
        "ordinal": 7,
        "field": "scheduled_start_utc",
        "type": "datetime",
        "required": True,
    },
    {
        "ordinal": 8,
        "field": "plate_appearance_id",
        "type": "nullable_string",
        "required": True,
    },
    {
        "ordinal": 9,
        "field": "pitch_id",
        "type": "nullable_string",
        "required": True,
    },
    {
        "ordinal": 10,
        "field": "pitcher_id",
        "type": "string",
        "required": True,
    },
    {
        "ordinal": 11,
        "field": "batter_id",
        "type": "string",
        "required": True,
    },
    {
        "ordinal": 12,
        "field": "event_sequence",
        "type": "integer",
        "required": True,
    },
    {
        "ordinal": 13,
        "field": "raw_outcome_code",
        "type": "string",
        "required": True,
    },
    {
        "ordinal": 14,
        "field": "outcome_value",
        "type": "nullable_number_or_boolean",
        "required": True,
    },
    {
        "ordinal": 15,
        "field": "outcome_missing",
        "type": "boolean",
        "required": True,
    },
    {
        "ordinal": 16,
        "field": "outcome_missing_reason",
        "type": "nullable_string",
        "required": True,
    },
    {
        "ordinal": 17,
        "field": "event_occurred_at_utc",
        "type": "nullable_datetime",
        "required": True,
    },
    {
        "ordinal": 18,
        "field": "source_observed_at_utc",
        "type": "datetime",
        "required": True,
    },
    {
        "ordinal": 19,
        "field": "source_published_at_utc",
        "type": "nullable_datetime",
        "required": True,
    },
    {
        "ordinal": 20,
        "field": "outcome_available_at_utc",
        "type": "datetime",
        "required": True,
    },
    {
        "ordinal": 21,
        "field": "provider",
        "type": "string",
        "required": True,
    },
    {
        "ordinal": 22,
        "field": "provider_event_id",
        "type": "string",
        "required": True,
    },
    {
        "ordinal": 23,
        "field": "provider_payload_version",
        "type": "string",
        "required": True,
    },
    {
        "ordinal": 24,
        "field": "provider_revision_id",
        "type": "nullable_string",
        "required": True,
    },
    {
        "ordinal": 25,
        "field": "is_final_provider_revision",
        "type": "boolean",
        "required": True,
    },
    {
        "ordinal": 26,
        "field": "ingestion_run_id",
        "type": "string",
        "required": True,
    },
    {
        "ordinal": 27,
        "field": "raw_payload_digest",
        "type": "sha256_string",
        "required": True,
    },
    {
        "ordinal": 28,
        "field": "outcome_provenance_digest",
        "type": "sha256_string",
        "required": True,
    },
    {
        "ordinal": 29,
        "field": "exclusion_codes",
        "type": "sorted_unique_string_array",
        "required": True,
    },
    {
        "ordinal": 30,
        "field": "historical_outcome_eligible",
        "type": "boolean",
        "required": True,
    },
]


IDENTITY_RULES = [
    {
        "rule_id": "HOUT-ID01",
        "rule": "game_id_required_for_every_historical_outcome",
    },
    {
        "rule_id": "HOUT-ID02",
        "rule": (
            "plate_appearance_id_required_for_plate_appearance_pitch_and_contact_targets"
        ),
    },
    {
        "rule_id": "HOUT-ID03",
        "rule": "pitch_id_required_for_pitch_and_contact_targets_only",
    },
    {
        "rule_id": "HOUT-ID04",
        "rule": "pitcher_id_and_batter_id_required",
    },
    {
        "rule_id": "HOUT-ID05",
        "rule": "provider_event_id_required_and_immutable",
    },
    {
        "rule_id": "HOUT-ID06",
        "rule": "event_sequence_required_and_nonnegative",
    },
    {
        "rule_id": "HOUT-ID07",
        "rule": "doubleheader_identity_preserved",
    },
    {
        "rule_id": "HOUT-ID08",
        "rule": "suspended_and_resumed_game_identity_preserved",
    },
    {
        "rule_id": "HOUT-ID09",
        "rule": "identity_conflicts_rejected_not_overwritten",
    },
    {
        "rule_id": "HOUT-ID10",
        "rule": "historical_outcome_id_deterministic_within_contract_version",
    },
]


AVAILABILITY_RULES = [
    {
        "rule_id": "HOUT-AV01",
        "rule": "outcome_available_at_utc_required",
    },
    {
        "rule_id": "HOUT-AV02",
        "rule": "outcome_available_at_not_before_scheduled_start",
    },
    {
        "rule_id": "HOUT-AV03",
        "rule": "source_observed_at_not_after_outcome_available_at",
    },
    {
        "rule_id": "HOUT-AV04",
        "rule": "publication_timestamp_preserved_when_available",
    },
    {
        "rule_id": "HOUT-AV05",
        "rule": "ingestion_timestamp_not_substituted_for_provider_availability",
    },
    {
        "rule_id": "HOUT-AV06",
        "rule": "original_and_corrected_availability_times_distinguished",
    },
    {
        "rule_id": "HOUT-AV07",
        "rule": "unknown_availability_marks_record_ineligible",
    },
    {
        "rule_id": "HOUT-AV08",
        "rule": "availability_semantics_explicit_by_provider",
    },
]


REVISION_RULES = [
    {
        "rule_id": "HOUT-RV01",
        "rule": "provider_payload_version_required",
    },
    {
        "rule_id": "HOUT-RV02",
        "rule": "provider_revision_id_preserved_when_supplied",
    },
    {
        "rule_id": "HOUT-RV03",
        "rule": "original_provider_value_never_silently_overwritten",
    },
    {
        "rule_id": "HOUT-RV04",
        "rule": "corrected_values_link_to_superseded_revision",
    },
    {
        "rule_id": "HOUT-RV05",
        "rule": "final_revision_flag_is_descriptive_not_point_in_time_authority",
    },
    {
        "rule_id": "HOUT-RV06",
        "rule": "raw_payload_digest_required_for_every revision",
    },
    {
        "rule_id": "HOUT-RV07",
        "rule": "duplicate_revision_payloads_deduplicated_by_digest",
    },
    {
        "rule_id": "HOUT-RV08",
        "rule": "revision_order_deterministic",
    },
]


MISSINGNESS_RULES = [
    {
        "rule_id": "HOUT-MS01",
        "rule": "outcome_missing_boolean_required",
    },
    {
        "rule_id": "HOUT-MS02",
        "rule": "missing_outcome_requires_explicit_reason",
    },
    {
        "rule_id": "HOUT-MS03",
        "rule": "null_contact_measurement_not_interpreted_as_zero",
    },
    {
        "rule_id": "HOUT-MS04",
        "rule": "unsupported_event_category_not_coerced_to_negative_target",
    },
    {
        "rule_id": "HOUT-MS05",
        "rule": "incomplete_game_records_retained_with_exclusion_code",
    },
    {
        "rule_id": "HOUT-MS06",
        "rule": "provider_gap_distinguished_from_true_negative",
    },
    {
        "rule_id": "HOUT-MS07",
        "rule": "missing_identity_rejects_record",
    },
    {
        "rule_id": "HOUT-MS08",
        "rule": "missingness_rates_artifacted_by_target_provider_and_season",
    },
]


PROVENANCE_RULES = [
    {
        "rule_id": "HOUT-PV01",
        "rule": "provider_required",
    },
    {
        "rule_id": "HOUT-PV02",
        "rule": "ingestion_run_id_required",
    },
    {
        "rule_id": "HOUT-PV03",
        "rule": "raw_payload_digest_valid_sha256",
    },
    {
        "rule_id": "HOUT-PV04",
        "rule": "outcome_provenance_digest_valid_sha256",
    },
    {
        "rule_id": "HOUT-PV05",
        "rule": "feature_and_outcome_payload_digests_separated",
    },
    {
        "rule_id": "HOUT-PV06",
        "rule": "provider_mapping_version_explicit",
    },
    {
        "rule_id": "HOUT-PV07",
        "rule": "target_derivation_version_explicit",
    },
    {
        "rule_id": "HOUT-PV08",
        "rule": "all provenance fields included_in_deterministic_digest",
    },
]


FALLBACK_CONTRACTS = [
    {
        "fallback_id": "HOUT-F01",
        "condition": "game_identity_missing",
        "result": "record_rejected",
        "diagnostic_code": "historical_outcome_game_identity_missing",
    },
    {
        "fallback_id": "HOUT-F02",
        "condition": "conditional_event_identity_missing",
        "result": "record_rejected",
        "diagnostic_code": "historical_outcome_event_identity_missing",
    },
    {
        "fallback_id": "HOUT-F03",
        "condition": "provider_event_identity_missing",
        "result": "record_rejected",
        "diagnostic_code": "historical_outcome_provider_identity_missing",
    },
    {
        "fallback_id": "HOUT-F04",
        "condition": "outcome_value_unavailable",
        "result": "record_retained_as_missing",
        "diagnostic_code": "historical_outcome_value_missing",
    },
    {
        "fallback_id": "HOUT-F05",
        "condition": "outcome_availability_unknown",
        "result": "record_ineligible",
        "diagnostic_code": "historical_outcome_availability_unknown",
    },
    {
        "fallback_id": "HOUT-F06",
        "condition": "provider_revision_conflict",
        "result": "record_rejected",
        "diagnostic_code": "historical_outcome_revision_conflict",
    },
    {
        "fallback_id": "HOUT-F07",
        "condition": "raw_payload_digest_missing_or_invalid",
        "result": "record_rejected",
        "diagnostic_code": "historical_outcome_payload_digest_invalid",
    },
    {
        "fallback_id": "HOUT-F08",
        "condition": "unsupported_raw_outcome_code",
        "result": "record_retained_without_target_value",
        "diagnostic_code": "historical_outcome_code_unsupported",
    },
    {
        "fallback_id": "HOUT-F09",
        "condition": "suspended_or_incomplete_game",
        "result": "record_retained_with_status",
        "diagnostic_code": "historical_outcome_game_incomplete",
    },
    {
        "fallback_id": "HOUT-F10",
        "condition": "historical_outcome_collection_disabled",
        "result": "no_historical_outcome_record_emitted",
        "diagnostic_code": "historical_outcome_collection_disabled",
    },
]


IMPLEMENTATION_STEPS = [
    {
        "step": 1,
        "action": (
            "Load and verify the completed 9B point-in-time contract implementation."
        ),
    },
    {
        "step": 2,
        "action": (
            "Inventory repository surfaces for game, plate-appearance, pitch, "
            "contact, and run-value outcomes."
        ),
    },
    {
        "step": 3,
        "action": (
            "Define deterministic historical outcome identities by event level."
        ),
    },
    {
        "step": 4,
        "action": (
            "Define provider observation, publication, availability, ingestion, "
            "and revision timestamps."
        ),
    },
    {
        "step": 5,
        "action": (
            "Define immutable provider payload and revision provenance."
        ),
    },
    {
        "step": 6,
        "action": (
            "Define bounded target derivations for the ten Layer 9 outcome targets."
        ),
    },
    {
        "step": 7,
        "action": (
            "Define explicit missingness, unsupported-category, and incomplete-game semantics."
        ),
    },
    {
        "step": 8,
        "action": (
            "Define deterministic exclusion and fallback diagnostics."
        ),
    },
    {
        "step": 9,
        "action": (
            "Define the immutable historical outcome record contract."
        ),
    },
    {
        "step": 10,
        "action": (
            "Define an independent synthetic contract-validation fixture plan."
        ),
    },
    {
        "step": 11,
        "action": (
            "Preserve the prohibition on feature joins, prediction joins, and metrics."
        ),
    },
    {
        "step": 12,
        "action": (
            "Emit deterministic CSV and JSON planning artifacts."
        ),
    },
]


ACCEPTANCE_CRITERIA = [
    {
        "criterion_id": "HOUT-AC01",
        "criterion": "layer_9B_dependency_verified",
    },
    {
        "criterion_id": "HOUT-AC02",
        "criterion": "twelve_inventory_domains_defined",
    },
    {
        "criterion_id": "HOUT-AC03",
        "criterion": "repository_inventory_deterministic",
    },
    {
        "criterion_id": "HOUT-AC04",
        "criterion": "ten_outcome_targets_preserved",
    },
    {
        "criterion_id": "HOUT-AC05",
        "criterion": "thirty_historical_outcome_fields_defined",
    },
    {
        "criterion_id": "HOUT-AC06",
        "criterion": "ten_identity_rules_defined",
    },
    {
        "criterion_id": "HOUT-AC07",
        "criterion": "eight_availability_rules_defined",
    },
    {
        "criterion_id": "HOUT-AC08",
        "criterion": "eight_revision_rules_defined",
    },
    {
        "criterion_id": "HOUT-AC09",
        "criterion": "eight_missingness_rules_defined",
    },
    {
        "criterion_id": "HOUT-AC10",
        "criterion": "eight_provenance_rules_defined",
    },
    {
        "criterion_id": "HOUT-AC11",
        "criterion": "ten_fallback_contracts_defined",
    },
    {
        "criterion_id": "HOUT-AC12",
        "criterion": "outcome_collection_execution_absent",
    },
    {
        "criterion_id": "HOUT-AC13",
        "criterion": "feature_outcome_join_execution_absent",
    },
    {
        "criterion_id": "HOUT-AC14",
        "criterion": "prediction_join_execution_absent",
    },
    {
        "criterion_id": "HOUT-AC15",
        "criterion": "predictive_metric_execution_absent",
    },
    {
        "criterion_id": "HOUT-AC16",
        "criterion": "production_authority_absent",
    },
    {
        "criterion_id": "HOUT-AC17",
        "criterion": "implementation_handoff_bounded",
    },
    {
        "criterion_id": "HOUT-AC18",
        "criterion": "all_planning_artifacts_deterministic",
    },
]


PROHIBITED_AUTHORITIES = [
    "historical_outcome_collection_execution",
    "historical_outcome_fetch_execution",
    "historical_outcome_record_materialization",
    "historical_outcome_feature_join_execution",
    "historical_outcome_prediction_join_execution",
    "evaluation_record_materialization",
    "baseline_prediction_generation",
    "augmented_prediction_generation",
    "predictive_metric_calculation",
    "accuracy_evaluation",
    "calibration_evaluation",
    "incremental_value_evaluation",
    "uncertainty_estimation",
    "model_training",
    "parameter_tuning",
    "threshold_tuning",
    "fallback_tuning",
    "backtest_execution",
    "production_overlay_integration",
    "production_matchup_activation",
    "simulation_state_change",
    "simulation_probability_change",
    "canonical_probability_authority_change",
    "pricing",
    "market_comparison",
    "edge_detection",
    "bet_recommendation",
]


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


def canonical_json_bytes(
    payload: Any,
) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def sha256_payload(
    payload: Any,
) -> str:
    return hashlib.sha256(
        canonical_json_bytes(payload)
    ).hexdigest()


def string_constants(
    path: Path,
) -> set[str]:
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


def eligible_repository_file(
    path: Path,
) -> bool:
    if not path.is_file():
        return False

    if path.suffix.lower() not in TEXT_SUFFIXES:
        return False

    try:
        relative = path.relative_to(ROOT)
    except ValueError:
        return False

    if any(
        part in EXCLUDED_PATH_PARTS
        for part in relative.parts
    ):
        return False

    try:
        return path.stat().st_size <= MAX_FILE_BYTES
    except OSError:
        return False


def repository_text_files() -> list[Path]:
    return sorted(
        (
            path
            for path in ROOT.rglob("*")
            if eligible_repository_file(path)
        ),
        key=lambda path: path.relative_to(ROOT).as_posix(),
    )


def normalized_text(
    path: Path,
) -> str:
    try:
        return path.read_text(
            encoding="utf-8",
            errors="ignore",
        ).lower()
    except OSError:
        return ""


def line_numbers_for_term(
    text: str,
    term: str,
    limit: int = 5,
) -> list[int]:
    term_lower = term.lower()
    matches: list[int] = []

    for line_number, line in enumerate(
        text.splitlines(),
        start=1,
    ):
        if term_lower in line:
            matches.append(line_number)

        if len(matches) >= limit:
            break

    return matches


def build_repository_inventory() -> list[dict[str, Any]]:
    inventory_rows: list[dict[str, Any]] = []

    for path in repository_text_files():
        relative_path = path.relative_to(
            ROOT
        ).as_posix()
        text = normalized_text(path)

        if not text:
            continue

        file_digest = hashlib.sha256(
            path.read_bytes()
        ).hexdigest()

        for search_term in SEARCH_TERMS:
            term = search_term["term"]
            occurrence_count = text.count(
                term.lower()
            )

            if occurrence_count == 0:
                continue

            line_numbers = line_numbers_for_term(
                text,
                term,
            )

            inventory_rows.append(
                {
                    "domain_id": search_term[
                        "domain_id"
                    ],
                    "term_id": search_term[
                        "term_id"
                    ],
                    "term": term,
                    "category": search_term[
                        "category"
                    ],
                    "repository_path": relative_path,
                    "file_suffix": path.suffix.lower(),
                    "occurrence_count": occurrence_count,
                    "first_match_lines": "|".join(
                        str(line_number)
                        for line_number in line_numbers
                    ),
                    "file_sha256": file_digest,
                }
            )

    inventory_rows.sort(
        key=lambda row: (
            row["domain_id"],
            row["term_id"],
            row["repository_path"],
        )
    )

    return inventory_rows


def build_domain_summary(
    inventory_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    summary_rows: list[dict[str, Any]] = []

    for domain in INVENTORY_DOMAINS:
        domain_rows = [
            row
            for row in inventory_rows
            if row["domain_id"]
            == domain["domain_id"]
        ]

        paths = sorted(
            {
                str(row["repository_path"])
                for row in domain_rows
            }
        )
        terms = sorted(
            {
                str(row["term"])
                for row in domain_rows
            }
        )

        summary_rows.append(
            {
                "domain_id": domain[
                    "domain_id"
                ],
                "domain": domain["domain"],
                "inventory_objective": domain[
                    "inventory_objective"
                ],
                "matched_rows": len(
                    domain_rows
                ),
                "matched_files": len(paths),
                "matched_terms": len(terms),
                "repository_paths": "|".join(
                    paths
                ),
                "terms_found": "|".join(
                    terms
                ),
            }
        )

    return summary_rows


def build_rule_rows() -> list[dict[str, Any]]:
    grouped_rules = [
        (
            "identity",
            IDENTITY_RULES,
        ),
        (
            "availability",
            AVAILABILITY_RULES,
        ),
        (
            "revision",
            REVISION_RULES,
        ),
        (
            "missingness",
            MISSINGNESS_RULES,
        ),
        (
            "provenance",
            PROVENANCE_RULES,
        ),
    ]

    return [
        {
            "rule_group": rule_group,
            **rule,
        }
        for rule_group, rules in grouped_rules
        for rule in rules
    ]


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

    repository_inventory = (
        build_repository_inventory()
    )
    domain_summary = build_domain_summary(
        repository_inventory
    )
    historical_outcome_rules = (
        build_rule_rows()
    )

    outcome_target_ids = [
        row["target_id"]
        for row in OUTCOME_TARGET_CONTRACT
    ]
    outcome_field_names = [
        row["field"]
        for row in HISTORICAL_OUTCOME_RECORD_FIELDS
    ]
    fallback_ids = [
        row["fallback_id"]
        for row in FALLBACK_CONTRACTS
    ]
    rule_ids = [
        row["rule_id"]
        for row in historical_outcome_rules
    ]

    inventory_digest = sha256_payload(
        repository_inventory
    )
    domain_summary_digest = sha256_payload(
        domain_summary
    )

    planning_checks = [
        {
            "check": "nine_b_predecessor_present",
            "actual": predecessor_present,
            "expected": True,
            "passed": predecessor_present,
        },
        {
            "check": "twelve_inventory_domains_defined",
            "actual": len(
                INVENTORY_DOMAINS
            ),
            "expected": 12,
            "passed": len(
                INVENTORY_DOMAINS
            )
            == 12,
        },
        {
            "check": "thirty_seven_search_terms_defined",
            "actual": len(
                SEARCH_TERMS
            ),
            "expected": 37,
            "passed": len(
                SEARCH_TERMS
            )
            == 37,
        },
        {
            "check": "repository_inventory_nonempty",
            "actual": len(
                repository_inventory
            ),
            "expected": "greater_than_zero",
            "passed": len(
                repository_inventory
            )
            > 0,
        },
        {
            "check": "repository_inventory_deterministic_digest_valid",
            "actual": inventory_digest,
            "expected": "sha256",
            "passed": bool(
                SHA256_PATTERN.fullmatch(
                    inventory_digest
                )
            ),
        },
        {
            "check": "domain_summary_deterministic_digest_valid",
            "actual": domain_summary_digest,
            "expected": "sha256",
            "passed": bool(
                SHA256_PATTERN.fullmatch(
                    domain_summary_digest
                )
            ),
        },
        {
            "check": "ten_outcome_targets_defined",
            "actual": len(
                OUTCOME_TARGET_CONTRACT
            ),
            "expected": 10,
            "passed": len(
                OUTCOME_TARGET_CONTRACT
            )
            == 10,
        },
        {
            "check": "outcome_target_ids_unique",
            "actual": len(
                set(outcome_target_ids)
            ),
            "expected": len(
                outcome_target_ids
            ),
            "passed": len(
                set(outcome_target_ids)
            )
            == len(
                outcome_target_ids
            ),
        },
        {
            "check": "thirty_historical_outcome_fields_defined",
            "actual": len(
                HISTORICAL_OUTCOME_RECORD_FIELDS
            ),
            "expected": 30,
            "passed": len(
                HISTORICAL_OUTCOME_RECORD_FIELDS
            )
            == 30,
        },
        {
            "check": "historical_outcome_field_names_unique",
            "actual": len(
                set(outcome_field_names)
            ),
            "expected": len(
                outcome_field_names
            ),
            "passed": len(
                set(outcome_field_names)
            )
            == len(
                outcome_field_names
            ),
        },
        {
            "check": "ten_identity_rules_defined",
            "actual": len(
                IDENTITY_RULES
            ),
            "expected": 10,
            "passed": len(
                IDENTITY_RULES
            )
            == 10,
        },
        {
            "check": "eight_availability_rules_defined",
            "actual": len(
                AVAILABILITY_RULES
            ),
            "expected": 8,
            "passed": len(
                AVAILABILITY_RULES
            )
            == 8,
        },
        {
            "check": "eight_revision_rules_defined",
            "actual": len(
                REVISION_RULES
            ),
            "expected": 8,
            "passed": len(
                REVISION_RULES
            )
            == 8,
        },
        {
            "check": "eight_missingness_rules_defined",
            "actual": len(
                MISSINGNESS_RULES
            ),
            "expected": 8,
            "passed": len(
                MISSINGNESS_RULES
            )
            == 8,
        },
        {
            "check": "eight_provenance_rules_defined",
            "actual": len(
                PROVENANCE_RULES
            ),
            "expected": 8,
            "passed": len(
                PROVENANCE_RULES
            )
            == 8,
        },
        {
            "check": "historical_outcome_rule_ids_unique",
            "actual": len(
                set(rule_ids)
            ),
            "expected": len(
                rule_ids
            ),
            "passed": len(
                set(rule_ids)
            )
            == len(
                rule_ids
            ),
        },
        {
            "check": "ten_fallback_contracts_defined",
            "actual": len(
                FALLBACK_CONTRACTS
            ),
            "expected": 10,
            "passed": len(
                FALLBACK_CONTRACTS
            )
            == 10,
        },
        {
            "check": "fallback_ids_unique",
            "actual": len(
                set(fallback_ids)
            ),
            "expected": len(
                fallback_ids
            ),
            "passed": len(
                set(fallback_ids)
            )
            == len(
                fallback_ids
            ),
        },
        {
            "check": "twelve_implementation_steps_defined",
            "actual": len(
                IMPLEMENTATION_STEPS
            ),
            "expected": 12,
            "passed": len(
                IMPLEMENTATION_STEPS
            )
            == 12,
        },
        {
            "check": "eighteen_acceptance_criteria_defined",
            "actual": len(
                ACCEPTANCE_CRITERIA
            ),
            "expected": 18,
            "passed": len(
                ACCEPTANCE_CRITERIA
            )
            == 18,
        },
        {
            "check": "historical_outcome_collection_execution_absent",
            "actual": False,
            "expected": False,
            "passed": True,
        },
        {
            "check": "feature_outcome_join_execution_absent",
            "actual": False,
            "expected": False,
            "passed": True,
        },
        {
            "check": "prediction_join_execution_absent",
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

    authority_rows = [
        {
            "authority": authority,
            "granted": False,
            "reason": (
                "9C defines the historical outcome inventory and implementation "
                "contract plan only."
            ),
        }
        for authority in PROHIBITED_AUTHORITIES
    ]

    authority_rows.append(
        {
            "authority": (
                "historical_outcome_contract_implementation"
            ),
            "granted": all_checks_passed,
            "reason": (
                "9D may implement deterministic historical outcome identities, "
                "target derivation, availability, revision, missingness, and "
                "provenance validation using synthetic fixtures without joining "
                "outcomes to features or predictions."
            ),
        }
    )

    diagnosis_name = (
        "pitch_type_matchup_overlay_historical_outcome_inventory_plan_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_outcome_inventory_plan_failed"
    )

    recommended_next_layer = (
        "9D_pitch_type_matchup_overlay_historical_outcome_contract_implementation"
        if all_checks_passed
        else
        "9C_pitch_type_matchup_overlay_historical_outcome_inventory_plan_remediation"
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
        OUTPUT_DIR / "inventory_domains.csv",
        [
            "domain_id",
            "domain",
            "inventory_objective",
        ],
        INVENTORY_DOMAINS,
    )

    write_csv(
        OUTPUT_DIR / "search_terms.csv",
        [
            "term_id",
            "domain_id",
            "term",
            "category",
        ],
        SEARCH_TERMS,
    )

    write_csv(
        OUTPUT_DIR / "repository_inventory.csv",
        [
            "domain_id",
            "term_id",
            "term",
            "category",
            "repository_path",
            "file_suffix",
            "occurrence_count",
            "first_match_lines",
            "file_sha256",
        ],
        repository_inventory,
    )

    write_csv(
        OUTPUT_DIR / "inventory_domain_summary.csv",
        [
            "domain_id",
            "domain",
            "inventory_objective",
            "matched_rows",
            "matched_files",
            "matched_terms",
            "repository_paths",
            "terms_found",
        ],
        domain_summary,
    )

    write_csv(
        OUTPUT_DIR / "outcome_target_contract.csv",
        [
            "target_id",
            "event_level",
            "target",
            "target_type",
            "source_semantics",
            "required_identity",
        ],
        OUTCOME_TARGET_CONTRACT,
    )

    write_csv(
        OUTPUT_DIR / "historical_outcome_record_contract.csv",
        [
            "ordinal",
            "field",
            "type",
            "required",
        ],
        HISTORICAL_OUTCOME_RECORD_FIELDS,
    )

    write_csv(
        OUTPUT_DIR / "historical_outcome_rules.csv",
        [
            "rule_group",
            "rule_id",
            "rule",
        ],
        historical_outcome_rules,
    )

    write_csv(
        OUTPUT_DIR / "fallback_contracts.csv",
        [
            "fallback_id",
            "condition",
            "result",
            "diagnostic_code",
        ],
        FALLBACK_CONTRACTS,
    )

    write_csv(
        OUTPUT_DIR / "implementation_steps.csv",
        [
            "step",
            "action",
        ],
        IMPLEMENTATION_STEPS,
    )

    write_csv(
        OUTPUT_DIR / "acceptance_criteria.csv",
        [
            "criterion_id",
            "criterion",
        ],
        ACCEPTANCE_CRITERIA,
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
                    "Implement deterministic historical outcome identity, target "
                    "derivation, availability, revision, missingness, provenance, "
                    "and disabled-path validation using synthetic fixtures only."
                    if all_checks_passed
                    else
                    "Remediate failed 9C planning checks."
                ),
                "entry_condition": (
                    "All twenty-five 9C planning checks pass."
                ),
                "passed": all_checks_passed,
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
        "inventory_domains_defined": len(
            INVENTORY_DOMAINS
        ),
        "search_terms_defined": len(
            SEARCH_TERMS
        ),
        "repository_inventory_rows": len(
            repository_inventory
        ),
        "repository_inventory_files": len(
            {
                row["repository_path"]
                for row in repository_inventory
            }
        ),
        "repository_inventory_digest": (
            inventory_digest
        ),
        "domain_summary_digest": (
            domain_summary_digest
        ),
        "outcome_targets_defined": len(
            OUTCOME_TARGET_CONTRACT
        ),
        "historical_outcome_fields_defined": len(
            HISTORICAL_OUTCOME_RECORD_FIELDS
        ),
        "historical_outcome_rules_defined": len(
            historical_outcome_rules
        ),
        "fallback_contracts_defined": len(
            FALLBACK_CONTRACTS
        ),
        "historical_outcome_records_materialized": 0,
        "feature_outcome_joins_executed": 0,
        "prediction_joins_executed": 0,
        "predictive_metrics_calculated": 0,
        "production_probabilities_changed": 0,
        "market_comparisons_executed": 0,
        "betting_edges_calculated": 0,
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
        "diagnosis": diagnosis_name,
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "all_checks_passed": all_checks_passed,
        "authority_granted": (
            "historical_outcome_contract_implementation"
            if all_checks_passed
            else
            "none"
        ),
        "authority_withheld": sorted(
            PROHIBITED_AUTHORITIES
        ),
        "recommended_next_layer": (
            recommended_next_layer
        ),
        "output_directory": str(
            OUTPUT_DIR.relative_to(ROOT)
        ),
    }

    write_json(
        OUTPUT_DIR / "diagnosis.json",
        diagnosis,
    )

    artifact_manifest_rows: list[
        dict[str, Any]
    ] = []

    for path in sorted(
        OUTPUT_DIR.iterdir(),
        key=lambda candidate: candidate.name,
    ):
        if not path.is_file():
            continue

        artifact_manifest_rows.append(
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
        [
            "artifact",
            "bytes",
            "sha256",
        ],
        artifact_manifest_rows,
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
        "Inventory domains defined: "
        f"{summary['inventory_domains_defined']}"
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
        "Outcome targets defined: "
        f"{summary['outcome_targets_defined']}"
    )
    print(
        "Historical outcome fields defined: "
        f"{summary['historical_outcome_fields_defined']}"
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
            + ", ".join(
                failed_checks
            )
        )
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
