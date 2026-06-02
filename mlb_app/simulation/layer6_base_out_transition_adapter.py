"""Non-production Layer 6 base/out transition adapter.

This module reads the audited 6IK materialized base/out transition source candidate.
It is intentionally disabled for production simulation and performs no work on import.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Sequence


ADAPTER_PRODUCTION_ENABLED = False
MATERIALIZED_SOURCE_FAMILY = "base_out_transitions"
MATERIALIZATION_VERSION = "layer6_6ik_v1"

REQUIRED_SCHEMA_FIELDS = [
    "game_id",
    "play_id",
    "inning",
    "half_inning",
    "sequence_order",
    "source_family",
    "source_path",
    "source_provenance",
    "original_start_base_state",
    "original_end_base_state",
    "original_start_outs",
    "original_end_outs",
    "original_runs_scored",
    "corrected_start_base_state",
    "corrected_end_base_state",
    "corrected_start_outs",
    "corrected_end_outs",
    "corrected_runs_scored",
    "corrected_exact_transition_row",
    "correction_applied",
    "correction_families",
    "correction_reason",
    "prior_gap_categories",
    "prior_fixability_classification",
    "materialized_layer",
    "materialization_version",
    "upstream_audit_layer",
    "upstream_audit_diagnosis",
]


@dataclass(frozen=True)
class Layer6BaseOutTransitionRecord:
    game_id: str
    play_id: str
    inning: str
    half_inning: str
    sequence_order: str
    source_family: str
    source_path: str
    source_provenance: str
    original_start_base_state: str
    original_end_base_state: str
    original_start_outs: str
    original_end_outs: str
    original_runs_scored: str
    corrected_start_base_state: str
    corrected_end_base_state: str
    corrected_start_outs: str
    corrected_end_outs: str
    corrected_runs_scored: str
    corrected_exact_transition_row: bool
    correction_applied: str
    correction_families: str
    correction_reason: str
    prior_gap_categories: str
    prior_fixability_classification: str
    materialized_layer: str
    materialization_version: str
    upstream_audit_layer: str
    upstream_audit_diagnosis: str


@dataclass(frozen=True)
class Layer6BaseOutTransitionAdapterValidation:
    schema_complete: bool
    materialized_transition_row_count: int
    materialized_exact_transition_row_count: int
    materialized_non_exact_transition_row_count: int
    materialized_schema_field_count: int
    source_provenance_retained_for_all_rows: bool
    lineage_rows_available: int
    lineage_fields_populated_for_all_rows: bool
    production_enabled: bool
    ready_for_real_evaluation: bool
    ready_for_activation: bool
    layer_6_exit_ready: bool

    @property
    def passed(self) -> bool:
        return (
            self.schema_complete
            and self.materialized_transition_row_count == 801
            and self.materialized_exact_transition_row_count == 801
            and self.materialized_non_exact_transition_row_count == 0
            and self.materialized_schema_field_count == 28
            and self.source_provenance_retained_for_all_rows
            and self.lineage_rows_available == 801
            and self.lineage_fields_populated_for_all_rows
            and self.production_enabled is False
            and self.ready_for_real_evaluation is False
            and self.ready_for_activation is False
            and self.layer_6_exit_ready is False
        )


def _read_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"Layer 6 materialized source artifact is missing: {path}")
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _boolish(value: Any) -> bool:
    return str(value).strip().lower() == "true"


def _record_from_row(row: Dict[str, str]) -> Layer6BaseOutTransitionRecord:
    return Layer6BaseOutTransitionRecord(
        game_id=row.get("game_id", ""),
        play_id=row.get("play_id", ""),
        inning=row.get("inning", ""),
        half_inning=row.get("half_inning", ""),
        sequence_order=row.get("sequence_order", ""),
        source_family=row.get("source_family", ""),
        source_path=row.get("source_path", ""),
        source_provenance=row.get("source_provenance", ""),
        original_start_base_state=row.get("original_start_base_state", ""),
        original_end_base_state=row.get("original_end_base_state", ""),
        original_start_outs=row.get("original_start_outs", ""),
        original_end_outs=row.get("original_end_outs", ""),
        original_runs_scored=row.get("original_runs_scored", ""),
        corrected_start_base_state=row.get("corrected_start_base_state", ""),
        corrected_end_base_state=row.get("corrected_end_base_state", ""),
        corrected_start_outs=row.get("corrected_start_outs", ""),
        corrected_end_outs=row.get("corrected_end_outs", ""),
        corrected_runs_scored=row.get("corrected_runs_scored", ""),
        corrected_exact_transition_row=_boolish(row.get("corrected_exact_transition_row")),
        correction_applied=row.get("correction_applied", ""),
        correction_families=row.get("correction_families", ""),
        correction_reason=row.get("correction_reason", ""),
        prior_gap_categories=row.get("prior_gap_categories", ""),
        prior_fixability_classification=row.get("prior_fixability_classification", ""),
        materialized_layer=row.get("materialized_layer", ""),
        materialization_version=row.get("materialization_version", ""),
        upstream_audit_layer=row.get("upstream_audit_layer", ""),
        upstream_audit_diagnosis=row.get("upstream_audit_diagnosis", ""),
    )


def load_layer6_base_out_transition_records(
    materialized_table_path: str | Path,
) -> List[Layer6BaseOutTransitionRecord]:
    rows = _read_csv_rows(Path(materialized_table_path))
    return [_record_from_row(row) for row in rows]


def validate_layer6_base_out_transition_source(
    materialized_table_path: str | Path,
    schema_contract_path: str | Path,
    lineage_path: str | Path,
) -> Layer6BaseOutTransitionAdapterValidation:
    table_rows = _read_csv_rows(Path(materialized_table_path))
    schema_rows = _read_csv_rows(Path(schema_contract_path))
    lineage_rows = _read_csv_rows(Path(lineage_path))

    fieldnames = list(table_rows[0].keys()) if table_rows else []
    schema_complete = all(field in fieldnames for field in REQUIRED_SCHEMA_FIELDS) and len(fieldnames) == 28
    exact_count = sum(1 for row in table_rows if _boolish(row.get("corrected_exact_transition_row")))
    non_exact_count = len(table_rows) - exact_count
    provenance_count = sum(
        1
        for row in table_rows
        if str(row.get("source_path", "")).strip()
        and str(row.get("source_provenance", "")).strip()
    )
    lineage_complete_count = sum(
        1
        for row in lineage_rows
        if str(row.get("source_path", "")).strip()
        and str(row.get("source_provenance", "")).strip()
    )

    return Layer6BaseOutTransitionAdapterValidation(
        schema_complete=schema_complete,
        materialized_transition_row_count=len(table_rows),
        materialized_exact_transition_row_count=exact_count,
        materialized_non_exact_transition_row_count=non_exact_count,
        materialized_schema_field_count=len(schema_rows),
        source_provenance_retained_for_all_rows=provenance_count == len(table_rows) == 801,
        lineage_rows_available=len(lineage_rows),
        lineage_fields_populated_for_all_rows=lineage_complete_count == len(lineage_rows) == 801,
        production_enabled=ADAPTER_PRODUCTION_ENABLED,
        ready_for_real_evaluation=False,
        ready_for_activation=False,
        layer_6_exit_ready=False,
    )


def summarize_layer6_base_out_transition_records(
    records: Sequence[Layer6BaseOutTransitionRecord],
) -> Dict[str, Any]:
    return {
        "record_count": len(records),
        "exact_transition_count": sum(1 for record in records if record.corrected_exact_transition_row),
        "non_exact_transition_count": sum(1 for record in records if not record.corrected_exact_transition_row),
        "source_family": MATERIALIZED_SOURCE_FAMILY,
        "materialization_version": MATERIALIZATION_VERSION,
        "production_enabled": ADAPTER_PRODUCTION_ENABLED,
        "ready_for_real_evaluation": False,
        "ready_for_activation": False,
        "layer_6_exit_ready": False,
    }
