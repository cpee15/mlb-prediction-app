"""Audit cutoff-safe hitter speed evidence availability."""

from __future__ import annotations

import json
from pathlib import Path

from mlb_app.database import StatcastEvent
from mlb_app.simulation.shadow.hitter_speed_evidence_availability import (
    evaluate_hitter_speed_evidence_availability,
)


ROOT = Path(__file__).resolve().parents[1]
BASERUNNING_SETTLEMENT = (
    ROOT
    / "scripts"
    / "run_canonical_baserunning_production_settlement.py"
)
DATABASE_SOURCE = ROOT / "mlb_app" / "database.py"
SPEED_ADAPTER_CANDIDATES = (
    ROOT
    / "mlb_app"
    / "hitter_speed_source.py",
    ROOT
    / "mlb_app"
    / "sprint_speed_source.py",
    ROOT
    / "mlb_app"
    / "simulation"
    / "shadow"
    / "hitter_speed_source.py",
)
SPEED_SNAPSHOT_CANDIDATES = (
    ROOT
    / "data"
    / "sprint_speed",
    ROOT
    / "data"
    / "hitter_speed",
    ROOT
    / "fixtures"
    / "sprint_speed",
)


def _contains(path: Path, tokens) -> bool:
    if not path.is_file():
        return False
    text = path.read_text(
        encoding="utf-8",
        errors="ignore",
    )
    return all(
        token in text
        for token in tokens
    )


def build_audit():
    persisted_fields = {
        column.name
        for column in StatcastEvent.__table__.columns
    }

    adapter_paths = [
        str(path.relative_to(ROOT))
        for path in SPEED_ADAPTER_CANDIDATES
        if path.is_file()
    ]
    snapshot_paths = [
        str(path.relative_to(ROOT))
        for path in SPEED_SNAPSHOT_CANDIDATES
        if path.exists()
    ]

    play_by_play_available = _contains(
        BASERUNNING_SETTLEMENT,
        (
            "statsapi.mlb.com",
            "feed/live",
            "baserunning",
        ),
    )
    opportunity_denominators_available = (
        _contains(
            BASERUNNING_SETTLEMENT,
            (
                "stolen_base",
                "caught_stealing",
                "eligible",
            ),
        )
    )

    result = (
        evaluate_hitter_speed_evidence_availability(
            persisted_fields=persisted_fields,
            source_capabilities={
                "authoritative_source_confirmed":
                    True,
                "adapter_available":
                    bool(adapter_paths),
                "historical_snapshots_available":
                    bool(snapshot_paths),
                "cutoff_query_supported":
                    False,
                "stable_player_identifier":
                    False,
                "freshness_metadata_available":
                    False,
            },
            baserunning_capabilities={
                "play_by_play_outcomes_available":
                    play_by_play_available,
                "opportunity_denominators_available":
                    opportunity_denominators_available,
                "direct_run_measurements_available":
                    False,
            },
        )
    )

    return {
        **result,
        "schema_version":
            "historical_shadow_hitter_speed_evidence_availability_audit_v1",
        "repository_evidence": {
            "statcast_table":
                StatcastEvent.__tablename__,
            "statcast_persisted_fields":
                sorted(persisted_fields),
            "statcast_speed_fields_present":
                result[
                    "direct_speed_fields_present"
                ],
            "speed_adapter_paths":
                adapter_paths,
            "speed_snapshot_paths":
                snapshot_paths,
            "baserunning_settlement_path": (
                str(
                    BASERUNNING_SETTLEMENT.relative_to(
                        ROOT
                    )
                )
                if BASERUNNING_SETTLEMENT.is_file()
                else None
            ),
            "database_source_path": (
                str(
                    DATABASE_SOURCE.relative_to(
                        ROOT
                    )
                )
                if DATABASE_SOURCE.is_file()
                else None
            ),
        },
        "decision": {
            "current_speed_signal_usable":
                result["speed_signal_ready"],
            "predictive_speed_audit_allowed":
                result[
                    "predictive_evaluation_allowed"
                ],
            "proxy_substitution_allowed":
                False,
            "source_acquisition_required":
                not result["speed_signal_ready"],
            "recommended_next_slice":
                result["recommended_next_slice"],
        },
    }


def main():
    print(
        json.dumps(
            build_audit(),
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
