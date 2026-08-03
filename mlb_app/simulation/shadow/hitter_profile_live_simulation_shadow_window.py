"""Aggregate paired hitter-profile simulation-shadow observations."""

from __future__ import annotations

from datetime import date

import math
from typing import Any, Mapping, Sequence


WINDOW_SCHEMA_VERSION = (
    "hitter_profile_live_simulation_shadow_window_v1"
)


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None

    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None

    return parsed if math.isfinite(parsed) else None


def _percentile(
    values: Sequence[float],
    quantile: float,
) -> float | None:
    ordered = sorted(float(value) for value in values)

    if not ordered:
        return None

    if len(ordered) == 1:
        return ordered[0]

    position = (len(ordered) - 1) * quantile
    lower = math.floor(position)
    upper = math.ceil(position)

    if lower == upper:
        return ordered[lower]

    weight = position - lower

    return (
        ordered[lower] * (1.0 - weight)
        + ordered[upper] * weight
    )


def _summary(values: Sequence[float]) -> dict[str, Any]:
    clean = [
        float(value)
        for value in values
        if _number(value) is not None
    ]

    if not clean:
        return {
            "count": 0,
            "mean": None,
            "median": None,
            "p95": None,
            "minimum": None,
            "maximum": None,
        }

    return {
        "count": len(clean),
        "mean": sum(clean) / len(clean),
        "median": _percentile(clean, 0.50),
        "p95": _percentile(clean, 0.95),
        "minimum": min(clean),
        "maximum": max(clean),
    }


def _diagnostics(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value

    converter = getattr(value, "to_diagnostics", None)

    if callable(converter):
        converted = converter()

        if isinstance(converted, Mapping):
            return converted

    return {}


def _comparison_records(
    observation: Mapping[str, Any],
) -> list[Mapping[str, Any]]:
    comparison = _diagnostics(
        observation.get("comparison")
    )

    records = comparison.get("records")

    if isinstance(records, Sequence) and not isinstance(
        records,
        (str, bytes),
    ):
        return [
            record
            for record in records
            if isinstance(record, Mapping)
        ]

    return []


def aggregate_hitter_profile_live_simulation_shadow_window(
    *,
    observations: Sequence[Any],
    target_date: str,
    requested_game_count: int,
    simulation_count: int,
) -> dict[str, Any]:
    """Aggregate read-only paired observations for one game window."""

    records: list[dict[str, Any]] = []
    state_counts: dict[str, int] = {}
    blocker_counts: dict[str, int] = {}
    scope_absolute_deltas: dict[str, list[float]] = {}
    metric_absolute_deltas: dict[str, list[float]] = {}
    maximum_deltas: list[float] = []
    comparison_counts: list[float] = []
    production_authority_unchanged = True
    production_inputs_unchanged = True
    simulation_counts_match = True
    database_writes_absent = True

    for raw_observation in observations:
        observation = _diagnostics(raw_observation)
        game_pk = observation.get("game_pk")
        status = str(
            observation.get("status") or "unknown"
        )
        executed = status == "observed"

        state = (
            "observed"
            if executed
            else "blocked"
        )

        blockers = sorted({
            str(blocker)
            for blocker in (
                observation.get("blockers") or ()
            )
            if blocker
        })

        state_counts[state] = (
            state_counts.get(state, 0) + 1
        )

        for blocker in blockers:
            blocker_counts[blocker] = (
                blocker_counts.get(blocker, 0) + 1
            )

        safety = _diagnostics(
            observation.get("safety_checks")
        )

        authority_unchanged = (
            observation.get(
                "production_authority_changed"
            )
            is False
            if (
                "production_authority_changed"
                in observation
            )
            else bool(
                safety.get(
                    "production_authority_unchanged",
                    True,
                )
            )
        )
        production_authority_unchanged = (
            production_authority_unchanged
            and authority_unchanged
        )
        production_inputs_unchanged = (
            production_inputs_unchanged
            and bool(
                safety.get(
                    "production_inputs_unchanged",
                    True,
                )
            )
        )
        simulation_counts_match = (
            simulation_counts_match
            and bool(
                safety.get(
                    "simulation_counts_match",
                    True,
                )
            )
        )
        database_writes_absent = (
            database_writes_absent
            and not bool(
                observation.get(
                    "database_writes_performed",
                    False,
                )
            )
        )

        comparison = _diagnostics(
            observation.get("comparison")
        )
        comparison_count = _number(
            comparison.get("comparison_count")
        )
        absolute_delta_summary = _diagnostics(
            comparison.get(
                "absolute_delta_summary"
            )
        )
        maximum_delta = _number(
            absolute_delta_summary.get("maximum")
        )

        if comparison_count is not None:
            comparison_counts.append(
                comparison_count
            )

        if maximum_delta is not None:
            maximum_delta = abs(maximum_delta)
            maximum_deltas.append(maximum_delta)

        for comparison_record in (
            _comparison_records(observation)
        ):
            absolute_delta = _number(
                comparison_record.get(
                    "absolute_delta"
                )
            )

            if absolute_delta is None:
                delta = _number(
                    comparison_record.get("delta")
                )
                absolute_delta = (
                    abs(delta)
                    if delta is not None
                    else None
                )

            if absolute_delta is None:
                continue

            scope = str(
                comparison_record.get("scope")
                or "unknown"
            )
            metric = str(
                comparison_record.get("metric")
                or "unknown"
            )

            scope_absolute_deltas.setdefault(
                scope,
                [],
            ).append(absolute_delta)
            metric_absolute_deltas.setdefault(
                metric,
                [],
            ).append(absolute_delta)

        materialization = _diagnostics(
            observation.get(
                "candidate_materialization"
            )
        )
        baseline_execution = _diagnostics(
            observation.get("baseline_execution")
        )
        candidate_execution = _diagnostics(
            observation.get("candidate_execution")
        )

        records.append({
            "game_pk": game_pk,
            "status": status,
            "state": state,
            "executed": executed,
            "blockers": blockers,
            "materialization_status": (
                materialization.get("status")
            ),
            "materialized_candidate_count": (
                materialization.get(
                    "candidate_count",
                    materialization.get(
                        "materialized_candidate_count",
                    ),
                )
            ),
            "materialization_state_counts": dict(
                materialization.get(
                    "state_counts"
                )
                or {}
            ),
            "materialization_blocker_counts": dict(
                materialization.get(
                    "blocker_counts"
                )
                or {}
            ),
            "materialization_blockers": list(
                materialization.get("blockers")
                or ()
            ),
            "materialization_records": [
                dict(record)
                for record in (
                    materialization.get("records")
                    or ()
                )
            ],
            "baseline_execution": {
                key: baseline_execution.get(key)
                for key in (
                    "status",
                    "executed",
                    "simulation_count",
                    "error_type",
                    "error_message",
                )
            },
            "candidate_execution": {
                key: candidate_execution.get(key)
                for key in (
                    "status",
                    "executed",
                    "simulation_count",
                    "error_type",
                    "error_message",
                )
            },
            "comparison_count": (
                int(comparison_count)
                if comparison_count is not None
                else 0
            ),
            "maximum_absolute_delta": (
                maximum_delta
            ),
        })

    audited_game_count = len(records)
    observed_game_count = state_counts.get(
        "observed",
        0,
    )
    observation_rate = (
        observed_game_count / audited_game_count
        if audited_game_count
        else 0.0
    )

    return {
        "schema_version": WINDOW_SCHEMA_VERSION,
        "status": (
            "observed"
            if observed_game_count
            else "blocked"
        ),
        "target_date": str(target_date),
        "requested_game_count": int(
            requested_game_count
        ),
        "audited_game_count": audited_game_count,
        "observed_game_count": observed_game_count,
        "observation_rate": observation_rate,
        "simulation_count": int(simulation_count),
        "state_counts": dict(
            sorted(state_counts.items())
        ),
        "blocker_counts": dict(
            sorted(blocker_counts.items())
        ),
        "comparison_count": int(
            sum(comparison_counts)
        ),
        "maximum_absolute_delta": _summary(
            maximum_deltas
        ),
        "absolute_delta_by_scope": {
            scope: _summary(values)
            for scope, values in sorted(
                scope_absolute_deltas.items()
            )
        },
        "absolute_delta_by_metric": {
            metric: _summary(values)
            for metric, values in sorted(
                metric_absolute_deltas.items()
            )
        },
        "records": sorted(
            records,
            key=lambda record: (
                record.get("game_pk") is None,
                record.get("game_pk") or 0,
            ),
        ),
        "safety_checks": {
            "all_production_authority_unchanged":
                production_authority_unchanged,
            "all_production_inputs_unchanged":
                production_inputs_unchanged,
            "all_simulation_counts_match":
                simulation_counts_match,
            "database_writes_performed":
                not database_writes_absent,
        },
        "decision": {
            "promotion_thresholds_selected": False,
            "production_activation_allowed": False,
            "recommended_next_slice":
                "define_hitter_profile_simulation_shadow_acceptance_gates",
        },
        "parameter_selected": False,
        "production_authority_changed": False,
    }

def _load_cutoff_safe_pitcher_hands(
    session: Any,
    *,
    pitcher_ids: Sequence[Any],
    as_of_date: str,
) -> tuple[
    dict[str, str],
    dict[str, dict[str, Any]],
]:
    """Load latest R/L evidence strictly before the game date."""

    from mlb_app.database import StatcastEvent

    hands: dict[str, str] = {}
    evidence: dict[str, dict[str, Any]] = {}

    for raw_pitcher_id in pitcher_ids:
        if (
            raw_pitcher_id is None
            or isinstance(raw_pitcher_id, bool)
        ):
            continue

        try:
            pitcher_id = int(raw_pitcher_id)
        except (TypeError, ValueError):
            continue

        row = (
            session.query(
                StatcastEvent.p_throws,
                StatcastEvent.game_date,
            )
            .filter(
                StatcastEvent.pitcher_id
                == pitcher_id
            )
            .filter(
                StatcastEvent.game_date
                < str(as_of_date)
            )
            .filter(
                StatcastEvent.p_throws.in_(
                    ("R", "L")
                )
            )
            .order_by(
                StatcastEvent.game_date.desc()
            )
            .first()
        )

        key = str(pitcher_id)

        if row is None:
            evidence[key] = {
                "status": "unavailable",
                "source": "statcast_events.p_throws",
                "cutoff_safe": True,
                "query_cutoff": str(as_of_date),
                "latest_source_date": None,
            }
            continue

        hand = str(row[0]).upper()
        hands[key] = hand
        evidence[key] = {
            "status": "ready",
            "hand": hand,
            "source": "statcast_events.p_throws",
            "cutoff_safe": True,
            "query_cutoff": str(as_of_date),
            "latest_source_date": (
                row[1].isoformat()
                if row[1] is not None
                else None
            ),
        }

    return hands, evidence


def run_hitter_profile_live_simulation_shadow_window(
    session: Any,
    *,
    enabled: bool = False,
    target_date: str,
    acceptance_gate: Mapping[str, Any] | None,
    simulation_count: int = 1000,
    game_limit: int = 5,
    projection_payload_builder: Any = None,
    candidate_materializer: Any = None,
    paired_audit_runner: Any = None,
    pitcher_hand_loader: Any = None,
    observation_observer: Any = None,
) -> dict[str, Any]:
    """Run a bounded read-only paired audit over projection games."""

    if simulation_count <= 0:
        raise ValueError(
            "simulation_count must be positive"
        )
    if game_limit <= 0:
        raise ValueError(
            "game_limit must be positive"
        )
    if (
        observation_observer is not None
        and not callable(observation_observer)
    ):
        raise TypeError(
            "observation_observer must be callable"
        )

    if enabled is not True:
        return {
            "schema_version": WINDOW_SCHEMA_VERSION,
            "status": "disabled",
            "target_date": str(target_date),
            "requested_game_count": int(game_limit),
            "audited_game_count": 0,
            "observed_game_count": 0,
            "observation_rate": 0.0,
            "simulation_count": int(
                simulation_count
            ),
            "records": [],
            "decision": {
                "promotion_thresholds_selected":
                    False,
                "production_activation_allowed":
                    False,
                "recommended_next_slice":
                    "run_hitter_profile_simulation_shadow_window",
            },
            "parameter_selected": False,
            "production_authority_changed": False,
        }

    gate = dict(acceptance_gate or {})
    if (
        gate.get("gate_passed") is not True
        or (
            gate.get("decision") or {}
        ).get(
            "feature_flag_integration_allowed"
        )
        is not True
        or (
            gate.get("decision") or {}
        ).get(
            "production_activation_allowed"
        )
        is not False
        or gate.get(
            "production_authority_changed"
        )
        is not False
    ):
        return {
            "schema_version": WINDOW_SCHEMA_VERSION,
            "status": "blocked",
            "target_date": str(target_date),
            "requested_game_count": int(game_limit),
            "audited_game_count": 0,
            "observed_game_count": 0,
            "observation_rate": 0.0,
            "simulation_count": int(
                simulation_count
            ),
            "state_counts": {},
            "blocker_counts": {
                "canary_acceptance_gate_not_passed":
                    1,
            },
            "records": [],
            "safety_checks": {
                "database_writes_performed": False,
            },
            "decision": {
                "promotion_thresholds_selected":
                    False,
                "production_activation_allowed":
                    False,
                "recommended_next_slice":
                    "resolve_hitter_profile_canary_gate",
            },
            "parameter_selected": False,
            "production_authority_changed": False,
        }

    if projection_payload_builder is None:
        from mlb_app.model_projections import (
            build_model_projection_payload,
        )

        projection_payload_builder = (
            build_model_projection_payload
        )

    if candidate_materializer is None:
        from mlb_app.simulation.shadow.hitter_profile_simulation_shadow_candidate_materialization import (
            materialize_hitter_profile_simulation_shadow_candidates,
        )

        candidate_materializer = (
            materialize_hitter_profile_simulation_shadow_candidates
        )

    if paired_audit_runner is None:
        from mlb_app.simulation.shadow.hitter_profile_paired_simulation_shadow_audit import (
            run_paired_hitter_profile_simulation_shadow_audit,
        )

        paired_audit_runner = (
            run_paired_hitter_profile_simulation_shadow_audit
        )

    contexts: list[Mapping[str, Any]] = []

    def observe_context(
        context: Mapping[str, Any],
    ) -> None:
        contexts.append(dict(context))

    projection_payload = projection_payload_builder(
        session,
        str(target_date),
        canonical_shadow_context_observer=(
            observe_context
        ),
    )

    def context_order(
        context: Mapping[str, Any],
    ) -> tuple[int, int | str]:
        raw_game_pk = context.get("game_pk")
        try:
            return (0, int(raw_game_pk))
        except (TypeError, ValueError):
            return (1, str(raw_game_pk or ""))

    contexts = sorted(
        contexts,
        key=context_order,
    )[:game_limit]

    observations: list[dict[str, Any]] = []

    if pitcher_hand_loader is None:
        pitcher_hand_loader = (
            _load_cutoff_safe_pitcher_hands
        )

    for context in contexts:
        game_pk = context.get("game_pk")
        raw_game_date = context.get("game_date")
        materialization_as_of_date = (
            date.fromisoformat(raw_game_date)
            if isinstance(raw_game_date, str)
            else raw_game_date
        )
        pitcher_hands = {
            str(key): value
            for key, value in (
                context.get(
                    "pitcher_hands_by_id"
                )
                or {}
            ).items()
            if str(value).upper() in {
                "R",
                "L",
            }
        }
        pitcher_profiles = {
            str(key): value
            for key, value in (
                context.get(
                    "pitcher_profiles_by_id"
                )
                or {}
            ).items()
        }
        missing_hand_ids = [
            pitcher_id
            for pitcher_id in pitcher_profiles
            if pitcher_id not in pitcher_hands
        ]
        loaded_hands: dict[str, str] = {}
        hand_evidence: dict[
            str,
            dict[str, Any],
        ] = {}

        if missing_hand_ids:
            (
                loaded_hands,
                hand_evidence,
            ) = pitcher_hand_loader(
                session,
                pitcher_ids=missing_hand_ids,
                as_of_date=str(
                    context.get("game_date")
                    or target_date
                ),
            )
            pitcher_hands.update(
                loaded_hands
            )

        try:
            materialization = (
                candidate_materializer(
                    session,
                    enabled=True,
                    acceptance_gate=gate,
                    lineups=context.get(
                        "lineups"
                    ),
                    exact_artifact_discovery=(
                        context.get(
                            "exact_artifact_discovery"
                        )
                    ),
                    pitcher_hands_by_id=(
                        pitcher_hands
                    ),
                    pitcher_profiles_by_id=(
                        pitcher_profiles
                    ),
                    environment_profile=(
                        context.get(
                            "environment_profile"
                        )
                    ),
                    season=int(
                        context.get("season")
                    ),
                    as_of_date=(
                        materialization_as_of_date
                    ),
                )
            )

            paired = paired_audit_runner(
                enabled=True,
                acceptance_gate=gate,
                candidate_materialization=(
                    materialization
                ),
                game_pk=int(game_pk),
                lineups=context.get("lineups"),
                bullpens=context.get("bullpens"),
                provider_discovery=context.get(
                    "provider_discovery"
                ),
                exact_artifact_discovery=(
                    context.get(
                        "exact_artifact_discovery"
                    )
                ),
                fallback_catalog_discovery=(
                    context.get(
                        "fallback_catalog_discovery"
                    )
                ),
                bootstrap_ready=bool(
                    context.get(
                        "bootstrap_ready"
                    )
                ),
                simulation_count=int(
                    simulation_count
                ),
            )

            diagnostics = dict(
                _diagnostics(paired)
            )
            diagnostics[
                "pitcher_hand_evidence"
            ] = hand_evidence
            diagnostics[
                "candidate_materialization"
            ] = {
                "status": materialization.get(
                    "status"
                ),
                "materialized": (
                    materialization.get(
                        "materialized"
                    )
                ),
                "candidate_count": len(
                    materialization.get(
                        "candidate_results"
                    )
                    or {}
                ),
                "state_counts": dict(
                    materialization.get(
                        "state_counts"
                    )
                    or {}
                ),
                "blocker_counts": dict(
                    materialization.get(
                        "blocker_counts"
                    )
                    or {}
                ),
                "blockers": list(
                    materialization.get("blockers")
                    or ()
                ),
                "records": [
                    dict(record)
                    for record in (
                        materialization.get("records")
                        or ()
                    )
                ],
            }
            diagnostics["game_pk"] = game_pk
            diagnostics["game_date"] = (
                context.get("game_date")
            )
            diagnostics[
                "database_writes_performed"
            ] = False
            diagnostics[
                "production_authority_changed"
            ] = False
            observations.append(diagnostics)
        except Exception as exc:
            observations.append({
                "game_pk": game_pk,
                "game_date": context.get(
                    "game_date"
                ),
                "status": "blocked",
                "blockers": [
                    "live_window_game_error",
                ],
                "error_type": type(exc).__name__,
                "database_writes_performed":
                    False,
                "production_authority_changed":
                    False,
            })

    if observation_observer is not None:
        for observation in observations:
            observation_observer(
                dict(observation)
            )

    result = (
        aggregate_hitter_profile_live_simulation_shadow_window(
            observations=observations,
            target_date=str(target_date),
            requested_game_count=int(game_limit),
            simulation_count=int(
                simulation_count
            ),
        )
    )

    games = (
        projection_payload.get("games")
        if isinstance(
            projection_payload,
            Mapping,
        )
        else []
    )

    result["source"] = {
        "projection_payload_status": (
            "ready"
            if isinstance(
                projection_payload,
                Mapping,
            )
            else "unavailable"
        ),
        "projection_game_count": (
            len(games)
            if isinstance(games, Sequence)
            and not isinstance(
                games,
                (str, bytes),
            )
            else 0
        ),
        "captured_context_count": len(
            contexts
        ),
    }
    result[
        "database_writes_performed"
    ] = False
    result[
        "production_authority_changed"
    ] = False

    return result
