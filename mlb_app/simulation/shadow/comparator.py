"""Compare legacy simulation output with canonical projections."""

from __future__ import annotations

from typing import Any, Dict, Iterable, Optional, Tuple

from mlb_app.simulation.projections import (
    CanonicalProjectionPayload,
    projection_payload_to_dict,
)

from .contracts import (
    CanonicalShadowDiagnostics,
    MetricComparison,
    RangeComparison,
    ShadowCoverage,
)


POSSIBLE_COMPARISONS = (
    "simulation_count",
    "away_runs_mean",
    "home_runs_mean",
    "total_runs_mean",
    "away_runs_range",
    "home_runs_range",
    "total_runs_range",
    "home_win_probability",
)


def compare_shadow_payloads(
    *,
    legacy_result: Dict[str, Any],
    canonical_payload,
) -> CanonicalShadowDiagnostics:
    """Build non-authoritative comparison diagnostics."""

    legacy = _select_legacy_simulation(legacy_result)
    canonical = _canonical_dict(canonical_payload)

    legacy_count = _as_int(
        legacy.get("simulations")
        or _nested_get(
            legacy,
            "metadata.simulation_count",
        )
    )
    canonical_count = _as_int(
        canonical.get("simulation_count")
    )

    comparisons = (
        _comparison(
            "simulation_count",
            legacy_count,
            canonical_count,
        ),
        _comparison(
            "away_runs_mean",
            _as_float(
                legacy.get("away_expected_runs")
            ),
            _canonical_team_metric(
                canonical,
                "away",
                "runs",
                "mean",
            ),
        ),
        _comparison(
            "home_runs_mean",
            _as_float(
                legacy.get("home_expected_runs")
            ),
            _canonical_team_metric(
                canonical,
                "home",
                "runs",
                "mean",
            ),
        ),
        _comparison(
            "total_runs_mean",
            _as_float(
                legacy.get("total_expected_runs")
            ),
            _canonical_total_metric(
                canonical,
                "runs",
                "mean",
            ),
        ),
        _comparison(
            "home_win_probability",
            _as_float(
                legacy.get("home_win_probability")
            ),
            _canonical_outcome_probability(
                canonical,
                "home_win_probability",
            ),
        ),
    )

    ranges = (
        _range_comparison(
            "away_runs_range",
            _legacy_distribution_range(
                legacy.get("away_run_distribution")
            ),
            _canonical_team_range(
                canonical,
                "away",
                "runs",
            ),
        ),
        _range_comparison(
            "home_runs_range",
            _legacy_distribution_range(
                legacy.get("home_run_distribution")
            ),
            _canonical_team_range(
                canonical,
                "home",
                "runs",
            ),
        ),
        _range_comparison(
            "total_runs_range",
            _legacy_distribution_range(
                legacy.get("total_run_distribution")
            ),
            _canonical_total_range(
                canonical,
                "runs",
            ),
        ),
    )

    compared_count = sum(
        comparison.available
        for comparison in comparisons
    ) + sum(
        comparison.available
        for comparison in ranges
    )

    possible_count = len(POSSIBLE_COMPARISONS)
    comparison_rate = round(
        compared_count / possible_count,
        6,
    )

    canonical_diagnostics = (
        canonical.get("diagnostics")
        if isinstance(
            canonical.get("diagnostics"),
            dict,
        )
        else {}
    )

    warnings = set(
        str(warning)
        for warning in (
            canonical_diagnostics.get("warnings")
            or []
        )
    )

    if legacy_count != canonical_count:
        warnings.add("simulation_count_mismatch")

    unavailable = [
        item.name
        for item in comparisons + ranges
        if not item.available
    ]

    for name in unavailable:
        warnings.add(
            f"comparison_unavailable:{name}"
        )

    status = (
        "complete"
        if compared_count == possible_count
        else "partial"
    )

    return CanonicalShadowDiagnostics(
        status=status,
        enabled=True,
        canonical_available=True,
        authoritative_source="legacy",
        comparisons=tuple(
            sorted(
                comparisons,
                key=lambda item: item.name,
            )
        ),
        ranges=tuple(
            sorted(
                ranges,
                key=lambda item: item.name,
            )
        ),
        coverage=ShadowCoverage(
            compared_metric_count=compared_count,
            possible_metric_count=possible_count,
            comparison_rate=comparison_rate,
        ),
        legacy_simulation_count=legacy_count,
        canonical_simulation_count=canonical_count,
        pitcher_attribution_complete_rate=(
            _as_float(
                canonical_diagnostics.get(
                    "pitcher_attribution_complete_rate"
                )
            )
        ),
        replay_validation_pass_rate=(
            _as_float(
                canonical_diagnostics.get(
                    "replay_validation_pass_rate"
                )
            )
        ),
        earned_run_status=(
            canonical_diagnostics.get(
                "earned_run_status"
            )
        ),
        warnings=tuple(sorted(warnings)),
    )


def _select_legacy_simulation(
    result: Dict[str, Any],
) -> Dict[str, Any]:
    if not isinstance(result, dict):
        raise TypeError(
            "legacy_result must be a dictionary"
        )

    if _looks_like_simulation(result):
        return result

    derived = _nested_get(
        result,
        "derived_outputs",
    )

    if isinstance(derived, dict):
        bullpen = derived.get(
            "bullpen_adjusted_game_simulation"
        )
        if isinstance(bullpen, dict) and bullpen:
            return bullpen

        base = derived.get("game_simulation")
        if isinstance(base, dict) and base:
            return base

    raise ValueError(
        "legacy simulation output was not found"
    )


def _canonical_dict(payload) -> Dict[str, Any]:
    if isinstance(
        payload,
        CanonicalProjectionPayload,
    ):
        canonical = projection_payload_to_dict(payload)
    elif isinstance(payload, dict):
        canonical = payload
    else:
        raise TypeError(
            "canonical_payload must be a canonical "
            "payload or dictionary"
        )

    _validate_canonical_payload(canonical)
    return canonical


def _validate_canonical_payload(
    payload: Dict[str, Any],
) -> None:
    simulation_count = _as_int(
        payload.get("simulation_count")
    )

    if simulation_count is None or simulation_count <= 0:
        raise ValueError(
            "canonical payload requires a positive "
            "simulation_count"
        )

    teams = payload.get("teams")

    if not isinstance(teams, list):
        raise ValueError(
            "canonical payload requires a teams list"
        )

    team_sides = tuple(
        team.get("team_side")
        for team in teams
        if isinstance(team, dict)
    )

    if team_sides != ("away", "home"):
        raise ValueError(
            "canonical payload teams must be ordered "
            "away then home"
        )

    for team in teams:
        metrics = team.get("metrics")

        if not isinstance(metrics, list):
            raise ValueError(
                "canonical team projection requires "
                "a metrics list"
            )

        runs_metric = next(
            (
                metric
                for metric in metrics
                if (
                    isinstance(metric, dict)
                    and metric.get("name") == "runs"
                )
            ),
            None,
        )

        if not isinstance(runs_metric, dict):
            raise ValueError(
                "canonical team projection requires "
                "a runs metric"
            )

        summary = runs_metric.get("summary")

        if not isinstance(summary, dict):
            raise ValueError(
                "canonical runs metric requires "
                "a summary"
            )

        for field in (
            "mean",
            "minimum",
            "maximum",
        ):
            if _as_float(summary.get(field)) is None:
                raise ValueError(
                    "canonical runs summary requires "
                    f"{field}"
                )

    diagnostics = payload.get("diagnostics")

    if not isinstance(diagnostics, dict):
        raise ValueError(
            "canonical payload requires diagnostics"
        )


def _looks_like_simulation(
    value: Dict[str, Any],
) -> bool:
    return any(
        key in value
        for key in (
            "away_expected_runs",
            "home_expected_runs",
            "total_expected_runs",
        )
    )


def _comparison(
    name: str,
    legacy_value,
    canonical_value,
) -> MetricComparison:
    legacy_number = _as_float(legacy_value)
    canonical_number = _as_float(canonical_value)

    available = (
        legacy_number is not None
        and canonical_number is not None
    )

    difference = (
        round(
            abs(
                legacy_number
                - canonical_number
            ),
            6,
        )
        if available
        else None
    )

    return MetricComparison(
        name=name,
        legacy_value=legacy_number,
        canonical_value=canonical_number,
        absolute_difference=difference,
        available=available,
    )


def _range_comparison(
    name: str,
    legacy_range,
    canonical_range,
) -> RangeComparison:
    available = (
        legacy_range is not None
        and canonical_range is not None
    )

    return RangeComparison(
        name=name,
        legacy_minimum=(
            legacy_range[0]
            if available
            else None
        ),
        legacy_maximum=(
            legacy_range[1]
            if available
            else None
        ),
        canonical_minimum=(
            canonical_range[0]
            if available
            else None
        ),
        canonical_maximum=(
            canonical_range[1]
            if available
            else None
        ),
        available=available,
    )


def _canonical_team_metric(
    payload: Dict[str, Any],
    team_side: str,
    metric_name: str,
    summary_name: str,
) -> Optional[float]:
    metric = _canonical_metric(
        payload,
        team_side,
        metric_name,
    )

    if not isinstance(metric, dict):
        return None

    summary = metric.get("summary")

    if not isinstance(summary, dict):
        return None

    return _as_float(summary.get(summary_name))


def _canonical_team_range(
    payload: Dict[str, Any],
    team_side: str,
    metric_name: str,
) -> Optional[Tuple[float, float]]:
    minimum = _canonical_team_metric(
        payload,
        team_side,
        metric_name,
        "minimum",
    )
    maximum = _canonical_team_metric(
        payload,
        team_side,
        metric_name,
        "maximum",
    )

    if minimum is None or maximum is None:
        return None

    return minimum, maximum


def _canonical_total_metric(
    payload: Dict[str, Any],
    metric_name: str,
    summary_name: str,
) -> Optional[float]:
    away = _canonical_team_metric(
        payload,
        "away",
        metric_name,
        summary_name,
    )
    home = _canonical_team_metric(
        payload,
        "home",
        metric_name,
        summary_name,
    )

    if away is None or home is None:
        return None

    return round(away + home, 6)


def _canonical_total_range(
    payload: Dict[str, Any],
    metric_name: str,
) -> Optional[Tuple[float, float]]:
    away = _canonical_team_range(
        payload,
        "away",
        metric_name,
    )
    home = _canonical_team_range(
        payload,
        "home",
        metric_name,
    )

    if away is None or home is None:
        return None

    return (
        round(away[0] + home[0], 6),
        round(away[1] + home[1], 6),
    )


def _canonical_metric(
    payload: Dict[str, Any],
    team_side: str,
    metric_name: str,
):
    teams = payload.get("teams")

    if not isinstance(teams, list):
        return None

    team = next(
        (
            item
            for item in teams
            if (
                isinstance(item, dict)
                and item.get("team_side")
                == team_side
            )
        ),
        None,
    )

    if not isinstance(team, dict):
        return None

    metrics = team.get("metrics")

    if not isinstance(metrics, list):
        return None

    return next(
        (
            item
            for item in metrics
            if (
                isinstance(item, dict)
                and item.get("name")
                == metric_name
            )
        ),
        None,
    )


def _canonical_outcome_probability(
    payload: Dict[str, Any],
    name: str,
) -> Optional[float]:
    outcomes = payload.get("outcomes")

    if not isinstance(outcomes, dict):
        return None

    return _as_float(outcomes.get(name))


def _legacy_distribution_range(
    distribution,
) -> Optional[Tuple[float, float]]:
    if not isinstance(distribution, dict):
        return None

    supported = []

    for raw_value, probability in distribution.items():
        probability_number = _as_float(probability)
        value_number = _as_float(raw_value)

        if (
            value_number is not None
            and probability_number is not None
            and probability_number > 0.0
        ):
            supported.append(value_number)

    if not supported:
        return None

    return min(supported), max(supported)


def _nested_get(
    value: Dict[str, Any],
    path: str,
):
    current = value

    for key in path.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(key)

    return current


def _as_float(value) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None

    try:
        number = float(value)
    except (TypeError, ValueError):
        return None

    return round(number, 6)


def _as_int(value) -> Optional[int]:
    number = _as_float(value)

    if number is None:
        return None

    return int(number)
