"""Persistent canonical production-monitoring ledger."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import hashlib
import json
from typing import Any, Dict, Mapping, Optional, Tuple

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from mlb_app.database import (
    CanonicalBaserunningProductionObservation,
)


CANONICAL_BASERUNNING_PRODUCTION_MONITORING_VERSION = (
    "canonical_baserunning_production_monitoring_v1"
)
CANONICAL_BASERUNNING_PRODUCTION_MONITORING_TARGET = 100
CANONICAL_BASERUNNING_PRODUCTION_MONITORING_START_DATE = (
    "2026-07-26"
)
CANONICAL_BASERUNNING_PRODUCTION_AUTHORITY = (
    "canonical_event_driven_calibrated_baserunning"
)
_CANONICAL_PRODUCTION_MONITORING_PREGAME_STATUSES = (
    "scheduled",
    "preview",
    "pre-game",
    "pregame",
    "warmup",
)


def _sha256(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


@dataclass(frozen=True)
class CanonicalBaserunningProductionMonitoringRecord:
    game_pk: int
    game_date: str
    canonical_run_id: str
    observation_digest: str
    paired_context_digest: str
    calibrated_transform_digest: str
    simulation_count: int
    status: str
    ready: bool
    production_activation: bool
    authoritative_source: str
    payload: Mapping[str, Any]
    schema_version: str = (
        CANONICAL_BASERUNNING_PRODUCTION_MONITORING_VERSION
    )

    def __post_init__(self) -> None:
        if (
            not isinstance(self.game_pk, int)
            or isinstance(self.game_pk, bool)
            or self.game_pk <= 0
        ):
            raise ValueError("game_pk must be positive")

        date.fromisoformat(self.game_date)

        for name, value in (
            ("canonical_run_id", self.canonical_run_id),
            ("observation_digest", self.observation_digest),
            (
                "paired_context_digest",
                self.paired_context_digest,
            ),
            (
                "calibrated_transform_digest",
                self.calibrated_transform_digest,
            ),
        ):
            if not isinstance(value, str) or not value:
                raise ValueError(f"{name} must be non-empty")

        if (
            not isinstance(self.simulation_count, int)
            or isinstance(self.simulation_count, bool)
            or self.simulation_count <= 0
        ):
            raise ValueError(
                "simulation_count must be positive"
            )

        if self.production_activation is not True:
            raise ValueError(
                "production monitoring requires activation"
            )

        if self.authoritative_source != (
            CANONICAL_BASERUNNING_PRODUCTION_AUTHORITY
        ):
            raise ValueError(
                "production monitoring requires canonical authority"
            )

        if self.schema_version != (
            CANONICAL_BASERUNNING_PRODUCTION_MONITORING_VERSION
        ):
            raise ValueError(
                "unsupported production monitoring version"
            )

    @property
    def digest(self) -> str:
        return _sha256(self.to_payload())

    def to_payload(self) -> Dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "game_pk": self.game_pk,
            "game_date": self.game_date,
            "canonical_run_id": self.canonical_run_id,
            "observation_digest": self.observation_digest,
            "paired_context_digest": (
                self.paired_context_digest
            ),
            "calibrated_transform_digest": (
                self.calibrated_transform_digest
            ),
            "simulation_count": self.simulation_count,
            "status": self.status,
            "ready": self.ready,
            "production_activation": (
                self.production_activation
            ),
            "authoritative_source": (
                self.authoritative_source
            ),
            "payload": dict(self.payload),
        }



def evaluate_canonical_production_monitoring_eligibility(
    *,
    game_date: str,
    game_status: str,
    activation_requested: bool,
    production_activation: bool,
    selected_execution: str,
    observation_ready: bool,
    input_parity_verified: bool,
    seed_parity_verified: bool,
    authoritative_source: str,
) -> Dict[str, Any]:
    parsed_game_date = date.fromisoformat(game_date)
    monitoring_start = date.fromisoformat(
        CANONICAL_BASERUNNING_PRODUCTION_MONITORING_START_DATE
    )
    normalized_status = str(
        game_status or ""
    ).strip().lower()

    checks = {
        "inside_monitoring_window": (
            parsed_game_date >= monitoring_start
        ),
        "pregame_status": (
            normalized_status
            in _CANONICAL_PRODUCTION_MONITORING_PREGAME_STATUSES
        ),
        "activation_requested": (
            activation_requested is True
        ),
        "production_activation": (
            production_activation is True
        ),
        "calibrated_selected": (
            selected_execution == "calibrated"
        ),
        "observation_ready": observation_ready is True,
        "input_parity_verified": (
            input_parity_verified is True
        ),
        "seed_parity_verified": (
            seed_parity_verified is True
        ),
        "canonical_authority": (
            authoritative_source
            == CANONICAL_BASERUNNING_PRODUCTION_AUTHORITY
        ),
    }
    failures = tuple(
        key
        for key, passed in checks.items()
        if not passed
    )

    return {
        "schema_version": (
            CANONICAL_BASERUNNING_PRODUCTION_MONITORING_VERSION
        ),
        "eligible": not failures,
        "failures": failures,
        "checks": checks,
        "game_date": game_date,
        "game_status": game_status,
        "normalized_game_status": normalized_status,
        "monitoring_start_date": (
            CANONICAL_BASERUNNING_PRODUCTION_MONITORING_START_DATE
        ),
        "target_game_count": (
            CANONICAL_BASERUNNING_PRODUCTION_MONITORING_TARGET
        ),
        "parameter_reselection_permitted": False,
    }

def store_canonical_baserunning_production_observation(
    session: Session,
    record: CanonicalBaserunningProductionMonitoringRecord,
) -> Tuple[
    CanonicalBaserunningProductionObservation,
    bool,
]:
    if not isinstance(
        record,
        CanonicalBaserunningProductionMonitoringRecord,
    ):
        raise TypeError("record must be canonical")

    existing = (
        session.query(
            CanonicalBaserunningProductionObservation
        )
        .filter(
            CanonicalBaserunningProductionObservation
            .observation_digest
            == record.observation_digest
        )
        .first()
    )

    if existing is not None:
        return existing, False

    row = CanonicalBaserunningProductionObservation(
        game_pk=record.game_pk,
        game_date=date.fromisoformat(record.game_date),
        canonical_run_id=record.canonical_run_id,
        observation_digest=record.observation_digest,
        paired_context_digest=(
            record.paired_context_digest
        ),
        calibrated_transform_digest=(
            record.calibrated_transform_digest
        ),
        simulation_count=record.simulation_count,
        status=record.status,
        ready=record.ready,
        production_activation=(
            record.production_activation
        ),
        authoritative_source=(
            record.authoritative_source
        ),
        payload_json=record.to_payload(),
    )
    try:
        with session.begin_nested():
            session.add(row)
            session.flush()
    except IntegrityError:
        existing = (
            session.query(
                CanonicalBaserunningProductionObservation
            )
            .filter(
                CanonicalBaserunningProductionObservation
                .observation_digest
                == record.observation_digest
            )
            .first()
        )
        if existing is None:
            raise
        return existing, False

    return row, True



def materialize_canonical_baserunning_production_monitoring(
    session: Session,
    *,
    eligibility: Mapping[str, Any],
    record: Optional[
        CanonicalBaserunningProductionMonitoringRecord
    ] = None,
) -> Dict[str, Any]:
    if not isinstance(eligibility, Mapping):
        raise TypeError("eligibility must be a mapping")

    recorded = False
    record_created = False
    observation_digest = None

    if eligibility.get("eligible") is True:
        if not isinstance(
            record,
            CanonicalBaserunningProductionMonitoringRecord,
        ):
            raise TypeError(
                "eligible monitoring requires a canonical record"
            )

        _, record_created = (
            store_canonical_baserunning_production_observation(
                session,
                record,
            )
        )
        recorded = True
        observation_digest = record.observation_digest

    rows = (
        load_canonical_baserunning_production_observations(
            session
        )
    )
    summary = (
        summarize_canonical_baserunning_production_monitoring(
            rows
        )
    )

    return {
        "schema_version": (
            CANONICAL_BASERUNNING_PRODUCTION_MONITORING_VERSION
        ),
        "recorded": recorded,
        "record_created": record_created,
        "observation_digest": observation_digest,
        "eligibility": dict(eligibility),
        "summary": summary,
        "parameter_reselection_permitted": False,
    }

def load_canonical_baserunning_production_observations(
    session: Session,
) -> Tuple[
    CanonicalBaserunningProductionObservation,
    ...
]:
    return tuple(
        session.query(
            CanonicalBaserunningProductionObservation
        )
        .order_by(
            CanonicalBaserunningProductionObservation
            .game_date,
            CanonicalBaserunningProductionObservation
            .game_pk,
            CanonicalBaserunningProductionObservation
            .id,
        )
        .all()
    )


def summarize_canonical_baserunning_production_monitoring(
    rows: Tuple[
        CanonicalBaserunningProductionObservation,
        ...
    ],
) -> Dict[str, Any]:
    games: Dict[int, Any] = {}

    for row in rows:
        games.setdefault(row.game_pk, row)

    selected = tuple(games.values())
    ready_games = tuple(
        row
        for row in selected
        if (
            row.ready is True
            and row.production_activation is True
            and row.authoritative_source
            == CANONICAL_BASERUNNING_PRODUCTION_AUTHORITY
        )
    )
    transform_digests = tuple(
        sorted(
            {
                row.calibrated_transform_digest
                for row in selected
            }
        )
    )
    game_count = len(selected)
    ready_game_count = len(ready_games)

    return {
        "schema_version": (
            CANONICAL_BASERUNNING_PRODUCTION_MONITORING_VERSION
        ),
        "target_game_count": (
            CANONICAL_BASERUNNING_PRODUCTION_MONITORING_TARGET
        ),
        "stored_observation_count": len(rows),
        "unique_game_count": game_count,
        "ready_game_count": ready_game_count,
        "duplicate_observation_count": (
            len(rows) - game_count
        ),
        "remaining_game_count": max(
            CANONICAL_BASERUNNING_PRODUCTION_MONITORING_TARGET
            - ready_game_count,
            0,
        ),
        "progress_rate": round(
            min(
                ready_game_count
                / CANONICAL_BASERUNNING_PRODUCTION_MONITORING_TARGET,
                1.0,
            ),
            6,
        ),
        "monitoring_complete": (
            ready_game_count
            >= CANONICAL_BASERUNNING_PRODUCTION_MONITORING_TARGET
        ),
        "transform_digests": transform_digests,
        "transform_frozen": (
            len(transform_digests) <= 1
        ),
        "parameter_reselection_permitted": False,
        "production_activation": True,
        "authoritative_source": (
            CANONICAL_BASERUNNING_PRODUCTION_AUTHORITY
        ),
    }
