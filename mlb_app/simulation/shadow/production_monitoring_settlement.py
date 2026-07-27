"""Settle canonical production baserunning observations."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, Mapping, Optional, Tuple

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from mlb_app.database import (
    CanonicalBaserunningProductionObservation,
    CanonicalBaserunningProductionSettlement,
)

from .mlb_play_by_play_baserunning_source import (
    CANONICAL_MLB_PLAY_BY_PLAY_BASERUNNING_SOURCE_VERSION,
    CanonicalMlbPlayByPlayBaserunningGame,
    CanonicalMlbPlayByPlayBaserunningSnapshot,
)
from .production_monitoring_ledger import (
    CANONICAL_BASERUNNING_PRODUCTION_MONITORING_TARGET,
)


CANONICAL_BASERUNNING_PRODUCTION_SETTLEMENT_VERSION = (
    "canonical_baserunning_production_settlement_v1"
)

_FINAL_STATUSES = {
    "final",
    "game over",
    "completed",
}


def _sha256(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _finite_nonnegative(
    value: Any,
    field_name: str,
) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or float(value) < 0.0
    ):
        raise ValueError(
            f"{field_name} must be nonnegative and finite"
        )
    return float(value)


def _digest(value: Any, field_name: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(
            character not in "0123456789abcdef"
            for character in value
        )
    ):
        raise ValueError(
            f"{field_name} must be a SHA-256 digest"
        )
    return value


def _mapping(
    value: Any,
    field_name: str,
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(
            f"{field_name} must be a mapping"
        )
    return value


@dataclass(frozen=True)
class CanonicalBaserunningProductionSettlementRecord:
    monitoring_observation_digest: str
    game_pk: int
    game_date: str
    canonical_run_id: str
    projected_stolen_bases: float
    projected_caught_stealing: float
    observed_stolen_bases: int
    observed_caught_stealing: int
    observed_source_version: str
    observed_source_digest: str
    payload: Mapping[str, Any]
    schema_version: str = (
        CANONICAL_BASERUNNING_PRODUCTION_SETTLEMENT_VERSION
    )

    def __post_init__(self) -> None:
        _digest(
            self.monitoring_observation_digest,
            "monitoring_observation_digest",
        )
        _digest(
            self.observed_source_digest,
            "observed_source_digest",
        )

        if (
            not isinstance(self.game_pk, int)
            or isinstance(self.game_pk, bool)
            or self.game_pk <= 0
        ):
            raise ValueError(
                "game_pk must be a positive integer"
            )

        parsed_date = date.fromisoformat(
            self.game_date
        )
        if parsed_date.isoformat() != self.game_date:
            raise ValueError(
                "game_date must use ISO format"
            )

        if (
            not isinstance(self.canonical_run_id, str)
            or not self.canonical_run_id
        ):
            raise ValueError(
                "canonical_run_id must be non-empty"
            )

        _finite_nonnegative(
            self.projected_stolen_bases,
            "projected_stolen_bases",
        )
        _finite_nonnegative(
            self.projected_caught_stealing,
            "projected_caught_stealing",
        )

        for field_name in (
            "observed_stolen_bases",
            "observed_caught_stealing",
        ):
            value = getattr(self, field_name)
            if (
                not isinstance(value, int)
                or isinstance(value, bool)
                or value < 0
            ):
                raise ValueError(
                    f"{field_name} must be a "
                    "nonnegative integer"
                )

        if (
            not isinstance(
                self.observed_source_version,
                str,
            )
            or not self.observed_source_version
        ):
            raise ValueError(
                "observed_source_version must be non-empty"
            )

        _mapping(self.payload, "payload")

        if self.schema_version != (
            CANONICAL_BASERUNNING_PRODUCTION_SETTLEMENT_VERSION
        ):
            raise ValueError(
                "unsupported production settlement version"
            )

    @property
    def projected_attempts(self) -> float:
        return round(
            self.projected_stolen_bases
            + self.projected_caught_stealing,
            6,
        )

    @property
    def observed_attempts(self) -> int:
        return (
            self.observed_stolen_bases
            + self.observed_caught_stealing
        )

    @property
    def projected_success_rate(self) -> Optional[float]:
        if self.projected_attempts == 0.0:
            return None
        return round(
            self.projected_stolen_bases
            / self.projected_attempts,
            6,
        )

    @property
    def observed_success_rate(self) -> Optional[float]:
        if self.observed_attempts == 0:
            return None
        return round(
            self.observed_stolen_bases
            / self.observed_attempts,
            6,
        )

    @property
    def comparison(self) -> Dict[str, Any]:
        projected_success = self.projected_success_rate
        observed_success = self.observed_success_rate

        return {
            "projected_stolen_bases": round(
                self.projected_stolen_bases,
                6,
            ),
            "observed_stolen_bases": (
                self.observed_stolen_bases
            ),
            "stolen_base_error": round(
                self.projected_stolen_bases
                - self.observed_stolen_bases,
                6,
            ),
            "stolen_base_absolute_error": round(
                abs(
                    self.projected_stolen_bases
                    - self.observed_stolen_bases
                ),
                6,
            ),
            "projected_caught_stealing": round(
                self.projected_caught_stealing,
                6,
            ),
            "observed_caught_stealing": (
                self.observed_caught_stealing
            ),
            "caught_stealing_error": round(
                self.projected_caught_stealing
                - self.observed_caught_stealing,
                6,
            ),
            "caught_stealing_absolute_error": round(
                abs(
                    self.projected_caught_stealing
                    - self.observed_caught_stealing
                ),
                6,
            ),
            "projected_attempts": (
                self.projected_attempts
            ),
            "observed_attempts": self.observed_attempts,
            "attempt_error": round(
                self.projected_attempts
                - self.observed_attempts,
                6,
            ),
            "attempt_absolute_error": round(
                abs(
                    self.projected_attempts
                    - self.observed_attempts
                ),
                6,
            ),
            "projected_success_rate": projected_success,
            "observed_success_rate": observed_success,
            "success_rate_absolute_error": (
                None
                if (
                    projected_success is None
                    or observed_success is None
                )
                else round(
                    abs(
                        projected_success
                        - observed_success
                    ),
                    6,
                )
            ),
        }

    def to_payload(self) -> Dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "monitoring_observation_digest": (
                self.monitoring_observation_digest
            ),
            "game_pk": self.game_pk,
            "game_date": self.game_date,
            "canonical_run_id": self.canonical_run_id,
            "observed_source_version": (
                self.observed_source_version
            ),
            "observed_source_digest": (
                self.observed_source_digest
            ),
            "comparison": self.comparison,
            "payload": dict(self.payload),
            "parameter_reselection_permitted": False,
            "production_authority_changed": False,
        }

    @property
    def digest(self) -> str:
        return _sha256(self.to_payload())


def _calibrated_validation(
    observation: CanonicalBaserunningProductionObservation,
) -> Mapping[str, Any]:
    root = _mapping(
        observation.payload_json,
        "monitoring payload",
    )
    payload = _mapping(
        root.get("payload"),
        "monitoring payload.payload",
    )
    source_observation = _mapping(
        payload.get("observation"),
        "monitoring observation",
    )
    return _mapping(
        source_observation.get(
            "calibrated_validation"
        ),
        "calibrated validation",
    )


def build_canonical_baserunning_production_settlement(
    *,
    observation: CanonicalBaserunningProductionObservation,
    observed_game: CanonicalMlbPlayByPlayBaserunningGame,
    observed_source_digest: str,
    final_status: str,
) -> CanonicalBaserunningProductionSettlementRecord:
    if not isinstance(
        observation,
        CanonicalBaserunningProductionObservation,
    ):
        raise TypeError(
            "observation must be a canonical "
            "production monitoring observation"
        )
    if not isinstance(
        observed_game,
        CanonicalMlbPlayByPlayBaserunningGame,
    ):
        raise TypeError(
            "observed_game must be canonical MLB "
            "play-by-play baserunning evidence"
        )

    if str(final_status).strip().lower() not in (
        _FINAL_STATUSES
    ):
        raise ValueError(
            "settlement requires a final game status"
        )

    if observation.game_pk != observed_game.game_pk:
        raise ValueError(
            "observed game_pk must match monitoring "
            "observation"
        )
    if (
        observation.game_date.isoformat()
        != observed_game.game_date
    ):
        raise ValueError(
            "observed game_date must match monitoring "
            "observation"
        )

    validation = _calibrated_validation(
        observation
    )

    return CanonicalBaserunningProductionSettlementRecord(
        monitoring_observation_digest=(
            observation.observation_digest
        ),
        game_pk=observation.game_pk,
        game_date=observation.game_date.isoformat(),
        canonical_run_id=observation.canonical_run_id,
        projected_stolen_bases=(
            _finite_nonnegative(
                validation.get(
                    "stolen_base_mean_total"
                ),
                "stolen_base_mean_total",
            )
        ),
        projected_caught_stealing=(
            _finite_nonnegative(
                validation.get(
                    "caught_stealing_mean_total"
                ),
                "caught_stealing_mean_total",
            )
        ),
        observed_stolen_bases=(
            observed_game.stolen_bases
        ),
        observed_caught_stealing=(
            observed_game.caught_stealing
        ),
        observed_source_version=(
            CANONICAL_MLB_PLAY_BY_PLAY_BASERUNNING_SOURCE_VERSION
        ),
        observed_source_digest=(
            _digest(
                observed_source_digest,
                "observed_source_digest",
            )
        ),
        payload={
            "final_status": final_status,
            "monitoring_schema_version": (
                observation.payload_json.get(
                    "schema_version"
                )
            ),
            "calibrated_transform_digest": (
                observation.calibrated_transform_digest
            ),
        },
    )


def store_canonical_baserunning_production_settlement(
    session: Session,
    record: CanonicalBaserunningProductionSettlementRecord,
) -> Tuple[
    CanonicalBaserunningProductionSettlement,
    bool,
]:
    if not isinstance(
        record,
        CanonicalBaserunningProductionSettlementRecord,
    ):
        raise TypeError(
            "record must be a canonical production "
            "settlement"
        )

    existing = (
        session.query(
            CanonicalBaserunningProductionSettlement
        )
        .filter(
            CanonicalBaserunningProductionSettlement
            .monitoring_observation_digest
            == record.monitoring_observation_digest
        )
        .one_or_none()
    )
    if existing is not None:
        if existing.settlement_digest != record.digest:
            raise ValueError(
                "monitoring observation already has a "
                "different settlement"
            )
        return existing, False

    existing_game = (
        session.query(
            CanonicalBaserunningProductionSettlement
        )
        .filter(
            CanonicalBaserunningProductionSettlement
            .game_pk
            == record.game_pk
        )
        .one_or_none()
    )
    if existing_game is not None:
        if (
            existing_game.monitoring_observation_digest
            != record.monitoring_observation_digest
        ):
            raise ValueError(
                "game already has a settlement from a "
                "different monitoring observation"
            )
        if existing_game.settlement_digest != record.digest:
            raise ValueError(
                "game already has a different settlement"
            )
        return existing_game, False

    row = CanonicalBaserunningProductionSettlement(
        monitoring_observation_digest=(
            record.monitoring_observation_digest
        ),
        settlement_digest=record.digest,
        game_pk=record.game_pk,
        game_date=date.fromisoformat(
            record.game_date
        ),
        canonical_run_id=record.canonical_run_id,
        observed_source_version=(
            record.observed_source_version
        ),
        observed_source_digest=(
            record.observed_source_digest
        ),
        observed_stolen_bases=(
            record.observed_stolen_bases
        ),
        observed_caught_stealing=(
            record.observed_caught_stealing
        ),
        comparison_json=record.comparison,
        payload_json=record.to_payload(),
    )

    try:
        with session.begin_nested():
            session.add(row)
            session.flush()
    except IntegrityError:
        existing = (
            session.query(
                CanonicalBaserunningProductionSettlement
            )
            .filter(
                CanonicalBaserunningProductionSettlement
                .monitoring_observation_digest
                == record.monitoring_observation_digest
            )
            .one_or_none()
        )
        if existing is None:
            raise
        if existing.settlement_digest != record.digest:
            raise ValueError(
                "monitoring observation already has a "
                "different settlement"
            )
        return existing, False

    return row, True


def load_canonical_baserunning_production_settlements(
    session: Session,
) -> Tuple[
    CanonicalBaserunningProductionSettlement,
    ...,
]:
    return tuple(
        session.query(
            CanonicalBaserunningProductionSettlement
        )
        .order_by(
            CanonicalBaserunningProductionSettlement
            .game_date,
            CanonicalBaserunningProductionSettlement
            .game_pk,
        )
        .all()
    )


def summarize_canonical_baserunning_production_settlements(
    rows: Tuple[
        CanonicalBaserunningProductionSettlement,
        ...,
    ],
) -> Dict[str, Any]:
    if not isinstance(rows, tuple):
        raise TypeError(
            "settlement rows must be a tuple"
        )

    settled_count = len(rows)
    projected_sb = sum(
        float(
            row.comparison_json[
                "projected_stolen_bases"
            ]
        )
        for row in rows
    )
    observed_sb = sum(
        row.observed_stolen_bases
        for row in rows
    )
    projected_cs = sum(
        float(
            row.comparison_json[
                "projected_caught_stealing"
            ]
        )
        for row in rows
    )
    observed_cs = sum(
        row.observed_caught_stealing
        for row in rows
    )

    def mean(key: str) -> float:
        if not rows:
            return 0.0
        return round(
            sum(
                float(row.comparison_json[key])
                for row in rows
            )
            / len(rows),
            6,
        )

    return {
        "schema_version": (
            CANONICAL_BASERUNNING_PRODUCTION_SETTLEMENT_VERSION
        ),
        "settled_game_count": settled_count,
        "target_game_count": (
            CANONICAL_BASERUNNING_PRODUCTION_MONITORING_TARGET
        ),
        "remaining_game_count": max(
            0,
            CANONICAL_BASERUNNING_PRODUCTION_MONITORING_TARGET
            - settled_count,
        ),
        "progress_rate": round(
            min(
                settled_count
                / CANONICAL_BASERUNNING_PRODUCTION_MONITORING_TARGET,
                1.0,
            ),
            6,
        ),
        "settlement_complete": (
            settled_count
            >= CANONICAL_BASERUNNING_PRODUCTION_MONITORING_TARGET
        ),
        "projected_stolen_bases": round(
            projected_sb,
            6,
        ),
        "observed_stolen_bases": observed_sb,
        "stolen_base_bias": round(
            projected_sb - observed_sb,
            6,
        ),
        "stolen_base_mae": mean(
            "stolen_base_absolute_error"
        ),
        "projected_caught_stealing": round(
            projected_cs,
            6,
        ),
        "observed_caught_stealing": observed_cs,
        "caught_stealing_bias": round(
            projected_cs - observed_cs,
            6,
        ),
        "caught_stealing_mae": mean(
            "caught_stealing_absolute_error"
        ),
        "attempt_mae": mean(
            "attempt_absolute_error"
        ),
        "parameter_reselection_permitted": (
            settled_count
            >= CANONICAL_BASERUNNING_PRODUCTION_MONITORING_TARGET
        ),
        "production_authority_changed": False,
    }


def _canonical_observations_by_game(
    session: Session,
) -> Dict[
    int,
    CanonicalBaserunningProductionObservation,
]:
    """
    Select the latest eligible stored observation for each game.

    Multiple pregame refreshes may create distinct observation digests.
    Settlement uses exactly one observation per game: the latest persisted
    eligible snapshot, with primary-key order as a deterministic tie-break.
    """

    rows = tuple(
        session.query(
            CanonicalBaserunningProductionObservation
        )
        .filter(
            CanonicalBaserunningProductionObservation
            .ready
            .is_(True)
        )
        .filter(
            CanonicalBaserunningProductionObservation
            .production_activation
            .is_(True)
        )
        .order_by(
            CanonicalBaserunningProductionObservation
            .game_date,
            CanonicalBaserunningProductionObservation
            .game_pk,
            CanonicalBaserunningProductionObservation
            .created_at,
            CanonicalBaserunningProductionObservation
            .id,
        )
        .all()
    )

    selected = {}
    for row in rows:
        selected[row.game_pk] = row

    return selected


def materialize_canonical_baserunning_production_settlements(
    session: Session,
    *,
    observed: CanonicalMlbPlayByPlayBaserunningSnapshot,
) -> Dict[str, Any]:
    """
    Settle canonical monitoring games covered by one completed-game snapshot.

    The caller owns network access and transaction commit. Games absent from
    the completed-game snapshot remain pending. Extraneous completed games
    are reported but never persisted.
    """

    if not isinstance(
        observed,
        CanonicalMlbPlayByPlayBaserunningSnapshot,
    ):
        raise TypeError(
            "observed must be a canonical MLB "
            "play-by-play baserunning snapshot"
        )

    observations = _canonical_observations_by_game(
        session
    )
    existing_rows = (
        load_canonical_baserunning_production_settlements(
            session
        )
    )
    settled_game_ids = {
        row.game_pk
        for row in existing_rows
    }

    created_game_ids = []
    reused_game_ids = []
    unmatched_observed_game_ids = []

    for observed_game in observed.games:
        monitoring = observations.get(
            observed_game.game_pk
        )
        if monitoring is None:
            unmatched_observed_game_ids.append(
                observed_game.game_pk
            )
            continue

        settlement = (
            build_canonical_baserunning_production_settlement(
                observation=monitoring,
                observed_game=observed_game,
                observed_source_digest=observed.digest,
                final_status="Final",
            )
        )
        _, created = (
            store_canonical_baserunning_production_settlement(
                session,
                settlement,
            )
        )

        if created:
            created_game_ids.append(
                observed_game.game_pk
            )
        else:
            reused_game_ids.append(
                observed_game.game_pk
            )

    rows = (
        load_canonical_baserunning_production_settlements(
            session
        )
    )
    summary = (
        summarize_canonical_baserunning_production_settlements(
            rows
        )
    )
    final_settled_game_ids = {
        row.game_pk
        for row in rows
    }
    pending_game_ids = tuple(
        sorted(
            set(observations)
            - final_settled_game_ids
        )
    )

    return {
        "schema_version": (
            CANONICAL_BASERUNNING_PRODUCTION_SETTLEMENT_VERSION
        ),
        "observed_source_version": (
            observed.source_version
        ),
        "observed_source_digest": observed.digest,
        "observed_game_count": observed.game_count,
        "eligible_monitoring_game_count": len(
            observations
        ),
        "previously_settled_game_count": len(
            settled_game_ids
        ),
        "created_game_ids": tuple(
            sorted(created_game_ids)
        ),
        "reused_game_ids": tuple(
            sorted(reused_game_ids)
        ),
        "unmatched_observed_game_ids": tuple(
            sorted(unmatched_observed_game_ids)
        ),
        "pending_game_ids": pending_game_ids,
        "summary": summary,
        "parameter_reselection_permitted": (
            summary[
                "parameter_reselection_permitted"
            ]
        ),
        "production_authority_changed": False,
    }


def load_pending_canonical_baserunning_production_observations(
    session: Session,
) -> Tuple[
    CanonicalBaserunningProductionObservation,
    ...,
]:
    """Return one canonical unsettled observation per game."""

    selected = _canonical_observations_by_game(
        session
    )
    settled_game_ids = {
        row.game_pk
        for row in (
            load_canonical_baserunning_production_settlements(
                session
            )
        )
    }

    return tuple(
        selected[game_pk]
        for game_pk in sorted(
            set(selected) - settled_game_ids,
            key=lambda value: (
                selected[value].game_date,
                value,
            ),
        )
    )
