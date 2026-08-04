"""Audit canonical pitcher roles and projected innings attribution."""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from typing import Any


SCHEMA_VERSION = (
    "canonical_pitcher_role_and_innings_"
    "attribution_audit_v1"
)

VALID_PLAN_TYPES = frozenset({
    "traditional_starter",
    "opener_bulk",
    "tandem",
    "bullpen_game",
    "workload_capped_starter",
    "unknown_fallback",
})

VALID_ROLES = frozenset({
    "starter",
    "opener",
    "bulk_follower",
    "tandem_primary",
    "tandem_secondary",
    "reliever",
    "unexpected_pitcher",
})

SUMMARY_FIELDS = (
    "count",
    "minimum",
    "p10",
    "median",
    "mean",
    "p90",
    "p95",
    "maximum",
)


def _value(
    source: Any,
    field_name: str,
    default: Any = None,
) -> Any:
    if isinstance(source, Mapping):
        return source.get(field_name, default)

    return getattr(source, field_name, default)


def _mapping(value: Any) -> Mapping[str, Any]:
    return (
        value
        if isinstance(value, Mapping)
        else {}
    )


def _identifier(value: Any) -> str | None:
    if value is None:
        return None

    normalized = str(value).strip()
    return normalized or None


def _identifier_tuple(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()

    if isinstance(value, (str, bytes)):
        values = (value,)
    else:
        try:
            values = tuple(value)
        except TypeError:
            values = (value,)

    result = []
    seen = set()

    for candidate in values:
        normalized = _identifier(candidate)
        if (
            normalized is not None
            and normalized not in seen
        ):
            result.append(normalized)
            seen.add(normalized)

    return tuple(result)


def _number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None

    if isinstance(value, (int, float)):
        return float(value)

    return None


def _metric_summary(
    projection: Mapping[str, Any],
    metric_name: str,
) -> Mapping[str, Any]:
    metrics = projection.get("metrics") or ()

    if isinstance(metrics, Mapping):
        metric = metrics.get(metric_name)
        if isinstance(metric, Mapping):
            summary = metric.get("summary", metric)
            return _mapping(summary)
        return {}

    for metric in metrics:
        if not isinstance(metric, Mapping):
            continue
        if metric.get("name") != metric_name:
            continue
        return _mapping(metric.get("summary"))

    return {}


def _innings_summary(
    outs_summary: Mapping[str, Any],
) -> dict[str, Any]:
    result = {}

    for field_name in SUMMARY_FIELDS:
        value = _number(
            outs_summary.get(field_name)
        )
        result[field_name] = (
            value / 3.0
            if value is not None
            else None
        )

    return result


def _plan_contract(
    plan: Any,
    expected_team_side: str,
) -> dict[str, Any]:
    team_side = _value(
        plan,
        "team_side",
        expected_team_side,
    )
    team_side = str(team_side or "").strip()

    if team_side != expected_team_side:
        raise ValueError(
            "pitching plan team_side does not "
            "match its audit side"
        )

    starter_id = _identifier(
        _value(plan, "starter_id")
    )
    if starter_id is None:
        raise ValueError(
            "pitching plan requires starter_id"
        )

    bullpen_ids = _identifier_tuple(
        _value(plan, "bullpen_pitcher_ids", ())
    )
    preferred_ids = _identifier_tuple(
        _value(
            plan,
            "preferred_replacement_pitcher_ids",
            (),
        )
    )
    plan_type = str(
        _value(
            plan,
            "plan_type",
            "traditional_starter",
        )
        or "traditional_starter"
    )

    if plan_type not in VALID_PLAN_TYPES:
        raise ValueError(
            f"unsupported pitching plan type: "
            f"{plan_type}"
        )

    if starter_id in bullpen_ids:
        raise ValueError(
            "starter cannot also be in bullpen"
        )

    if any(
        pitcher_id not in bullpen_ids
        for pitcher_id in preferred_ids
    ):
        raise ValueError(
            "preferred replacement must be in bullpen"
        )

    return {
        "team_side": team_side,
        "starter_id": starter_id,
        "bullpen_pitcher_ids": bullpen_ids,
        "preferred_replacement_pitcher_ids": (
            preferred_ids
        ),
        "plan_type": plan_type,
    }


def _starter_role(plan_type: str) -> str:
    if plan_type in {
        "opener_bulk",
        "bullpen_game",
    }:
        return "opener"

    if plan_type == "tandem":
        return "tandem_primary"

    return "starter"


def _planned_roles(
    plan: Mapping[str, Any],
) -> dict[str, str]:
    plan_type = str(plan["plan_type"])
    preferred_ids = tuple(
        plan[
            "preferred_replacement_pitcher_ids"
        ]
    )

    roles = {
        str(plan["starter_id"]):
            _starter_role(plan_type),
    }

    for pitcher_id in plan[
        "bullpen_pitcher_ids"
    ]:
        roles[str(pitcher_id)] = "reliever"

    if preferred_ids:
        if plan_type == "opener_bulk":
            roles[preferred_ids[0]] = (
                "bulk_follower"
            )
        elif plan_type == "tandem":
            roles[preferred_ids[0]] = (
                "tandem_secondary"
            )

    return roles


def _projection_pitchers(
    projection_payload: Mapping[str, Any],
) -> tuple[Mapping[str, Any], ...]:
    raw_pitchers = (
        projection_payload.get("pitchers") or ()
    )

    if isinstance(raw_pitchers, Mapping):
        raw_pitchers = raw_pitchers.values()

    pitchers = []

    for projection in raw_pitchers:
        if not isinstance(projection, Mapping):
            raise TypeError(
                "pitcher projections must be mappings"
            )
        pitchers.append(projection)

    return tuple(pitchers)


def audit_canonical_pitcher_role_and_innings_attribution(
    *,
    projection_payload: Mapping[str, Any],
    away_pitching_plan: Any,
    home_pitching_plan: Any,
    expected_roles_by_id: (
        Mapping[str, str] | None
    ) = None,
) -> dict[str, Any]:
    """
    Compare canonical pitcher projections with canonical pitching plans.

    This audit does not alter projection values, pitcher sequencing,
    production inputs, database state, or production authority.
    """

    if not isinstance(
        projection_payload,
        Mapping,
    ):
        raise TypeError(
            "projection_payload must be a mapping"
        )

    if (
        expected_roles_by_id is not None
        and not isinstance(
            expected_roles_by_id,
            Mapping,
        )
    ):
        raise TypeError(
            "expected_roles_by_id must be a mapping"
        )

    plans = {
        "away": _plan_contract(
            away_pitching_plan,
            "away",
        ),
        "home": _plan_contract(
            home_pitching_plan,
            "home",
        ),
    }
    roles_by_side = {
        side: _planned_roles(plan)
        for side, plan in plans.items()
    }

    expected_roles = {
        str(player_id): str(role)
        for player_id, role in (
            expected_roles_by_id or {}
        ).items()
    }

    invalid_expected_roles = sorted({
        role
        for role in expected_roles.values()
        if role not in VALID_ROLES
    })
    if invalid_expected_roles:
        raise ValueError(
            "unsupported expected pitcher role: "
            + ", ".join(invalid_expected_roles)
        )

    records = []
    projected_keys = set()
    anomalies = []

    for projection in _projection_pitchers(
        projection_payload
    ):
        player_id = _identifier(
            projection.get("player_id")
        )
        team_side = str(
            projection.get("team_side") or ""
        ).strip()

        if player_id is None:
            raise ValueError(
                "pitcher projection requires player_id"
            )
        if team_side not in plans:
            raise ValueError(
                "pitcher projection team_side must "
                "be away or home"
            )

        key = (team_side, player_id)
        if key in projected_keys:
            raise ValueError(
                "duplicate pitcher projection"
            )
        projected_keys.add(key)

        planned_role = roles_by_side[
            team_side
        ].get(
            player_id,
            "unexpected_pitcher",
        )
        role_source = (
            "canonical_pitching_plan"
            if planned_role
            != "unexpected_pitcher"
            else "projection_only"
        )
        expected_role = expected_roles.get(
            player_id
        )

        record_anomalies = []

        if planned_role == "unexpected_pitcher":
            record_anomalies.append(
                "projected_pitcher_outside_plan"
            )

        if (
            expected_role is not None
            and expected_role != planned_role
        ):
            record_anomalies.append(
                "expected_role_mismatch"
            )

        outs_summary = _metric_summary(
            projection,
            "outs_recorded",
        )
        if not outs_summary:
            record_anomalies.append(
                "outs_projection_unavailable"
            )

        anomalies.extend(record_anomalies)

        records.append({
            "player_id": player_id,
            "team_side": team_side,
            "plan_type":
                plans[team_side]["plan_type"],
            "role": planned_role,
            "role_source": role_source,
            "expected_role": expected_role,
            "is_planned_starter": (
                player_id
                == plans[team_side]["starter_id"]
            ),
            "is_planned_bullpen_pitcher": (
                player_id
                in plans[team_side][
                    "bullpen_pitcher_ids"
                ]
            ),
            "is_preferred_replacement": (
                player_id
                in plans[team_side][
                    "preferred_replacement_pitcher_ids"
                ]
            ),
            "outs_recorded": {
                field_name:
                    _number(
                        outs_summary.get(
                            field_name
                        )
                    )
                for field_name in SUMMARY_FIELDS
            },
            "innings_pitched": (
                _innings_summary(outs_summary)
            ),
            "anomalies": sorted(
                set(record_anomalies)
            ),
        })

    for team_side, plan in plans.items():
        starter_key = (
            team_side,
            str(plan["starter_id"]),
        )
        if starter_key not in projected_keys:
            anomalies.append(
                "planned_starter_not_projected"
            )

        for pitcher_id in plan[
            "preferred_replacement_pitcher_ids"
        ]:
            if (
                team_side,
                str(pitcher_id),
            ) not in projected_keys:
                anomalies.append(
                    "preferred_replacement_not_projected"
                )

    records.sort(
        key=lambda record: (
            record["team_side"],
            record["role"],
            record["player_id"],
        )
    )

    anomaly_counts = dict(
        sorted(Counter(anomalies).items())
    )
    projected_plan_count = sum(
        record["role"] != "unexpected_pitcher"
        for record in records
    )
    projected_pitcher_count = len(records)

    return {
        "schema_version": SCHEMA_VERSION,
        "status": "observed",
        "audited": True,
        "simulation_count":
            projection_payload.get(
                "simulation_count"
            ),
        "pitching_plans": {
            side: {
                "team_side": plan["team_side"],
                "plan_type": plan["plan_type"],
                "starter_id": plan["starter_id"],
                "bullpen_pitcher_ids": list(
                    plan["bullpen_pitcher_ids"]
                ),
                "preferred_replacement_pitcher_ids":
                    list(
                        plan[
                            "preferred_replacement_pitcher_ids"
                        ]
                    ),
            }
            for side, plan in plans.items()
        },
        "projected_pitcher_count":
            projected_pitcher_count,
        "planned_projected_pitcher_count":
            projected_plan_count,
        "unexpected_projected_pitcher_count":
            sum(
                record["role"]
                == "unexpected_pitcher"
                for record in records
            ),
        "role_attribution_complete_rate": (
            projected_plan_count
            / projected_pitcher_count
            if projected_pitcher_count
            else 0.0
        ),
        "anomaly_counts": anomaly_counts,
        "records": records,
        "limitations": [
            (
                "aggregate_projection_payload_does_"
                "not_expose_per_trial_pitcher_"
                "appearance_sequence"
            ),
            (
                "starter_versus_relief_innings_"
                "cannot_be_proven_from_aggregate_"
                "pitcher_metrics"
            ),
        ],
        "safety_checks": {
            "projection_values_unchanged": True,
            "pitching_plans_unchanged": True,
            "database_writes_performed": False,
            "production_authority_changed": False,
        },
        "decision": {
            "sequencing_activation_allowed": False,
            "production_activation_allowed": False,
            "recommended_next_slice": (
                "audit_canonical_pitcher_"
                "appearance_sequence"
            ),
        },
        "database_writes_performed": False,
        "production_authority_changed": False,
    }
