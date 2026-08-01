"""Paired full-game audit for hitter-profile simulation shadow."""

from __future__ import annotations

import math
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any


SCHEMA_VERSION = (
    "hitter_profile_paired_simulation_shadow_audit_v1"
)

OUTCOME_KEYS = (
    "away_win_probability",
    "home_win_probability",
    "tie_probability",
    "extra_innings_probability",
    "walk_off_probability",
)

RESERVED_EXECUTION_KEYS = {
    "hitter_profile_shadow_enabled",
    "hitter_profile_acceptance_gate",
    "hitter_profile_candidate_results",
}


def _number(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return (
        result
        if math.isfinite(result)
        else None
    )


def _mapping(value: Any) -> dict[str, Any]:
    return (
        dict(value)
        if isinstance(value, Mapping)
        else {}
    )


def _percentile(
    values: list[float],
    quantile: float,
) -> float:
    ordered = sorted(values)
    if not ordered:
        return 0.0
    if len(ordered) == 1:
        return ordered[0]

    position = (len(ordered) - 1) * quantile
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    fraction = position - lower

    return (
        ordered[lower]
        + (
            ordered[upper]
            - ordered[lower]
        )
        * fraction
    )


def _distribution_mean(value: Any) -> float | None:
    distribution = _mapping(value)
    if not distribution:
        return None

    total = 0.0
    probability_sum = 0.0

    for raw_value, raw_probability in (
        distribution.items()
    ):
        outcome = _number(raw_value)
        probability = _number(raw_probability)
        if outcome is None or probability is None:
            return None
        total += outcome * probability
        probability_sum += probability

    if abs(probability_sum - 1.0) > 0.00001:
        return None

    return total


def _metric_means(
    projections: Any,
) -> dict[str, float]:
    result: dict[str, float] = {}

    if not isinstance(projections, list):
        return result

    for projection in projections:
        row = _mapping(projection)
        identity = str(
            row.get("team_side")
            or row.get("player_id")
            or ""
        )
        team_side = str(
            row.get("team_side") or ""
        )
        player_id = str(
            row.get("player_id") or ""
        )

        if player_id:
            identity = f"{team_side}:{player_id}"

        for raw_metric in (
            row.get("metrics") or []
        ):
            metric = _mapping(raw_metric)
            name = str(metric.get("name") or "")
            mean = _number(
                _mapping(
                    metric.get("summary")
                ).get("mean")
            )
            if identity and name and mean is not None:
                result[f"{identity}:{name}"] = mean

    return result


def _comparison_record(
    *,
    scope: str,
    identity: str,
    metric: str,
    baseline: float,
    candidate: float,
) -> dict[str, Any]:
    delta = candidate - baseline
    return {
        "scope": scope,
        "identity": identity,
        "metric": metric,
        "baseline": baseline,
        "candidate": candidate,
        "signed_delta": delta,
        "absolute_delta": abs(delta),
    }


def compare_hitter_profile_simulation_shadow_payloads(
    *,
    baseline_payload: Mapping[str, Any],
    candidate_payload: Mapping[str, Any],
) -> dict[str, Any]:
    """Compare paired canonical outputs without selecting thresholds."""

    baseline = dict(baseline_payload)
    candidate = dict(candidate_payload)

    baseline_count = int(
        baseline.get("simulation_count") or 0
    )
    candidate_count = int(
        candidate.get("simulation_count") or 0
    )

    if (
        baseline_count <= 0
        or candidate_count <= 0
        or baseline_count != candidate_count
    ):
        return {
            "status": "blocked",
            "comparison_count": 0,
            "records": [],
            "blockers": [
                "paired_simulation_count_mismatch",
            ],
        }

    records: list[dict[str, Any]] = []
    baseline_outcomes = _mapping(
        baseline.get("outcomes")
    )
    candidate_outcomes = _mapping(
        candidate.get("outcomes")
    )

    for metric in OUTCOME_KEYS:
        baseline_value = _number(
            baseline_outcomes.get(metric)
        )
        candidate_value = _number(
            candidate_outcomes.get(metric)
        )
        if (
            baseline_value is not None
            and candidate_value is not None
        ):
            records.append(
                _comparison_record(
                    scope="game",
                    identity="game",
                    metric=metric,
                    baseline=baseline_value,
                    candidate=candidate_value,
                )
            )

    for metric in (
        "away_run_distribution",
        "home_run_distribution",
        "total_run_distribution",
    ):
        baseline_value = _distribution_mean(
            baseline_outcomes.get(metric)
        )
        candidate_value = _distribution_mean(
            candidate_outcomes.get(metric)
        )
        if (
            baseline_value is not None
            and candidate_value is not None
        ):
            records.append(
                _comparison_record(
                    scope="game",
                    identity="game",
                    metric=f"{metric}_mean",
                    baseline=baseline_value,
                    candidate=candidate_value,
                )
            )

    for group in (
        "away_run_distribution",
        "home_run_distribution",
        "total_run_distribution",
        "team_total_probabilities",
        "total_probabilities",
    ):
        baseline_probabilities = _mapping(
            baseline_outcomes.get(group)
        )
        candidate_probabilities = _mapping(
            candidate_outcomes.get(group)
        )

        for metric in sorted(
            set(baseline_probabilities)
            & set(candidate_probabilities)
        ):
            baseline_value = _number(
                baseline_probabilities[metric]
            )
            candidate_value = _number(
                candidate_probabilities[metric]
            )
            if (
                baseline_value is not None
                and candidate_value is not None
            ):
                records.append(
                    _comparison_record(
                        scope="game_probability",
                        identity=group,
                        metric=str(metric),
                        baseline=baseline_value,
                        candidate=candidate_value,
                    )
                )

    for group, scope in (
        ("teams", "team"),
        ("batters", "batter"),
        ("pitchers", "pitcher"),
    ):
        baseline_metrics = _metric_means(
            baseline.get(group)
        )
        candidate_metrics = _metric_means(
            candidate.get(group)
        )

        for key in sorted(
            set(baseline_metrics)
            & set(candidate_metrics)
        ):
            identity, metric = key.rsplit(
                ":",
                1,
            )
            records.append(
                _comparison_record(
                    scope=scope,
                    identity=identity,
                    metric=metric,
                    baseline=baseline_metrics[key],
                    candidate=candidate_metrics[key],
                )
            )

    absolute_deltas = [
        float(record["absolute_delta"])
        for record in records
    ]
    ordered_changes = sorted(
        records,
        key=lambda record: (
            -record["absolute_delta"],
            record["scope"],
            record["identity"],
            record["metric"],
        ),
    )

    return {
        "schema_version": SCHEMA_VERSION,
        "status": (
            "ready"
            if records
            else "blocked"
        ),
        "simulation_count": baseline_count,
        "comparison_count": len(records),
        "records": records,
        "largest_changes": ordered_changes[:25],
        "absolute_delta_summary": {
            "mean": (
                sum(absolute_deltas)
                / len(absolute_deltas)
                if absolute_deltas
                else 0.0
            ),
            "median": _percentile(
                absolute_deltas,
                0.50,
            ),
            "p95": _percentile(
                absolute_deltas,
                0.95,
            ),
            "maximum": (
                max(absolute_deltas)
                if absolute_deltas
                else 0.0
            ),
        },
        "blockers": (
            []
            if records
            else [
                "no_comparable_canonical_metrics",
            ]
        ),
        "parameter_selected": False,
        "production_authority_changed": False,
    }


def _execution_payload(
    execution: Any,
) -> dict[str, Any] | None:
    material = getattr(
        execution,
        "material",
        None,
    )
    payload = getattr(
        material,
        "canonical_payload",
        None,
    )
    return (
        dict(payload)
        if isinstance(payload, Mapping)
        else None
    )


def _execution_diagnostics(
    execution: Any,
) -> dict[str, Any]:
    method = getattr(
        execution,
        "to_diagnostics",
        None,
    )
    return (
        dict(method())
        if callable(method)
        else {}
    )


def _materialization_summary(
    materialization: Mapping[str, Any],
) -> dict[str, Any]:
    excluded = {
        "candidate_results",
        "records",
    }
    return {
        key: value
        for key, value in materialization.items()
        if key not in excluded
    }


@dataclass(frozen=True)
class HitterProfilePairedSimulationShadowAudit:
    status: str
    baseline_execution: Any = None
    candidate_execution: Any = None
    candidate_materialization: Mapping[
        str,
        Any,
    ] | None = None
    comparison: Mapping[str, Any] | None = None
    blockers: tuple[str, ...] = ()
    enabled: bool = False
    audit_version: str = SCHEMA_VERSION

    @property
    def production_execution(self) -> Any:
        return self.baseline_execution

    def to_diagnostics(self) -> dict[str, Any]:
        baseline = _execution_diagnostics(
            self.baseline_execution
        )
        candidate = _execution_diagnostics(
            self.candidate_execution
        )
        materialization = dict(
            self.candidate_materialization or {}
        )
        candidate_overlay = _mapping(
            candidate.get(
                "hitter_profile_simulation_shadow"
            )
        )

        return {
            "schema_version": self.audit_version,
            "status": self.status,
            "enabled": self.enabled,
            "baseline_execution": baseline,
            "candidate_execution": candidate,
            "candidate_materialization": (
                _materialization_summary(
                    materialization
                )
            ),
            "comparison": dict(
                self.comparison or {}
            ),
            "blockers": list(self.blockers),
            "safety_checks": {
                "baseline_executed": (
                    baseline.get("executed")
                    is True
                ),
                "candidate_executed": (
                    candidate.get("executed")
                    is True
                ),
                "simulation_counts_match": (
                    baseline.get("simulation_count")
                    == candidate.get(
                        "simulation_count"
                    )
                ),
                "candidate_overlay_applied": (
                    candidate_overlay.get(
                        "overlay_applied"
                    )
                    is True
                ),
                "database_writes_absent": (
                    materialization.get(
                        "database_writes_performed"
                    )
                    is False
                ),
                "production_inputs_unchanged": (
                    materialization.get(
                        "production_inputs_unchanged"
                    )
                    is True
                ),
                "production_authority_unchanged": (
                    baseline.get(
                        "production_authority_changed"
                    )
                    is False
                    and candidate.get(
                        "production_authority_changed"
                    )
                    is False
                    and materialization.get(
                        "production_authority_changed"
                    )
                    is False
                ),
            },
            "production_result": "baseline_execution",
            "production_activation_allowed": False,
            "parameter_selected": False,
            "production_authority_changed": False,
            "authoritative_source": "legacy",
        }


def run_paired_hitter_profile_simulation_shadow_audit(
    *,
    enabled: bool = False,
    acceptance_gate: Mapping[str, Any] | None = None,
    candidate_materialization: Mapping[
        str,
        Any,
    ] | None = None,
    execution_runner: Callable[..., Any] | None = None,
    **production_inputs: Any,
) -> HitterProfilePairedSimulationShadowAudit:
    """Run baseline and candidate canonical shadows with paired inputs."""

    if enabled is not True:
        return HitterProfilePairedSimulationShadowAudit(
            status="disabled",
            enabled=False,
        )

    reserved = (
        RESERVED_EXECUTION_KEYS
        & set(production_inputs)
    )
    if reserved:
        raise ValueError(
            "paired audit owns hitter-profile "
            "execution arguments"
        )

    materialization = dict(
        candidate_materialization or {}
    )
    candidates = _mapping(
        materialization.get(
            "candidate_results"
        )
    )

    if (
        materialization.get("status") != "ready"
        or materialization.get(
            "materialized"
        )
        is not True
        or not candidates
        or materialization.get(
            "database_writes_performed"
        )
        is not False
        or materialization.get(
            "production_inputs_unchanged"
        )
        is not True
        or materialization.get(
            "production_authority_changed"
        )
        is not False
    ):
        return HitterProfilePairedSimulationShadowAudit(
            status="blocked",
            candidate_materialization=(
                materialization
            ),
            blockers=(
                "candidate_materialization_not_ready",
            ),
            enabled=True,
        )

    if execution_runner is None:
        from mlb_app.simulation.shadow.production_execution import (
            run_canonical_production_shadow,
        )

        execution_runner = (
            run_canonical_production_shadow
        )

    baseline = execution_runner(
        **production_inputs,
        hitter_profile_shadow_enabled=False,
    )
    candidate = execution_runner(
        **production_inputs,
        hitter_profile_shadow_enabled=True,
        hitter_profile_acceptance_gate=(
            acceptance_gate
        ),
        hitter_profile_candidate_results=(
            candidates
        ),
    )

    baseline_payload = _execution_payload(
        baseline
    )
    candidate_payload = _execution_payload(
        candidate
    )

    blockers = []
    comparison = None
    baseline_diagnostics = (
        _execution_diagnostics(baseline)
    )
    candidate_diagnostics = (
        _execution_diagnostics(candidate)
    )
    baseline_overlay = _mapping(
        baseline_diagnostics.get(
            "hitter_profile_simulation_shadow"
        )
    )
    candidate_overlay = _mapping(
        candidate_diagnostics.get(
            "hitter_profile_simulation_shadow"
        )
    )

    if baseline_payload is None:
        blockers.append(
            "baseline_execution_not_ready"
        )
    if candidate_payload is None:
        blockers.append(
            "candidate_execution_not_ready"
        )
    if baseline_overlay.get(
        "overlay_applied"
    ) is True:
        blockers.append(
            "baseline_overlay_unexpected"
        )
    if (
        candidate_payload is not None
        and candidate_overlay.get(
            "overlay_applied"
        )
        is not True
    ):
        blockers.append(
            "candidate_overlay_not_applied"
        )
    if (
        baseline_diagnostics.get(
            "simulation_count"
        )
        != candidate_diagnostics.get(
            "simulation_count"
        )
    ):
        blockers.append(
            "paired_execution_count_mismatch"
        )

    if not blockers:
        comparison = (
            compare_hitter_profile_simulation_shadow_payloads(
                baseline_payload=baseline_payload,
                candidate_payload=candidate_payload,
            )
        )
        if comparison.get("status") != "ready":
            blockers.extend(
                comparison.get("blockers") or ()
            )

    return HitterProfilePairedSimulationShadowAudit(
        status=(
            "observed"
            if not blockers
            else "blocked"
        ),
        baseline_execution=baseline,
        candidate_execution=candidate,
        candidate_materialization=materialization,
        comparison=comparison,
        blockers=tuple(sorted(set(blockers))),
        enabled=True,
    )
