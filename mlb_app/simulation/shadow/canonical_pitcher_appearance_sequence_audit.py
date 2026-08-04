"""Audit per-trial canonical pitcher appearance sequencing."""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from typing import Any


SCHEMA_VERSION = (
    "canonical_pitcher_appearance_sequence_audit_v1"
)

VALID_PLAN_TYPES = frozenset({
    "traditional_starter",
    "opener_bulk",
    "tandem",
    "bullpen_game",
    "workload_capped_starter",
    "unknown_fallback",
})


def _value(
    source: Any,
    field_name: str,
    default: Any = None,
) -> Any:
    if isinstance(source, Mapping):
        return source.get(field_name, default)

    return getattr(source, field_name, default)


def _identifier(value: Any) -> str | None:
    if value is None:
        return None

    normalized = str(value).strip()
    return normalized or None


def _identifiers(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()

    if isinstance(value, (str, bytes)):
        candidates = (value,)
    else:
        try:
            candidates = tuple(value)
        except TypeError:
            candidates = (value,)

    result = []
    seen = set()

    for candidate in candidates:
        normalized = _identifier(candidate)
        if (
            normalized is not None
            and normalized not in seen
        ):
            result.append(normalized)
            seen.add(normalized)

    return tuple(result)


def _plan(
    value: Any,
    team_side: str,
) -> dict[str, Any]:
    actual_side = str(
        _value(value, "team_side", team_side)
        or ""
    ).strip()

    if actual_side != team_side:
        raise ValueError(
            "pitching plan team_side does not "
            "match audit side"
        )

    starter_id = _identifier(
        _value(value, "starter_id")
    )
    if starter_id is None:
        raise ValueError(
            "pitching plan requires starter_id"
        )

    bullpen_ids = _identifiers(
        _value(
            value,
            "bullpen_pitcher_ids",
            (),
        )
    )
    preferred_ids = _identifiers(
        _value(
            value,
            "preferred_replacement_pitcher_ids",
            (),
        )
    )
    plan_type = str(
        _value(
            value,
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


def _planned_roles(
    plan: Mapping[str, Any],
) -> dict[str, str]:
    plan_type = plan["plan_type"]

    if plan_type in {
        "opener_bulk",
        "bullpen_game",
    }:
        starter_role = "opener"
    elif plan_type == "tandem":
        starter_role = "tandem_primary"
    else:
        starter_role = "starter"

    roles = {
        plan["starter_id"]: starter_role,
    }

    for pitcher_id in plan[
        "bullpen_pitcher_ids"
    ]:
        roles[pitcher_id] = "reliever"

    preferred = plan[
        "preferred_replacement_pitcher_ids"
    ]

    if preferred:
        if plan_type == "opener_bulk":
            roles[preferred[0]] = (
                "bulk_follower"
            )
        elif plan_type == "tandem":
            roles[preferred[0]] = (
                "tandem_secondary"
            )

    return roles


def _state_value(
    event: Any,
    state_name: str,
    field_name: str,
    default: Any = None,
) -> Any:
    state = _value(
        event,
        state_name,
    )
    return _value(
        state,
        field_name,
        default,
    )


def _fielding_side(event: Any) -> str:
    half = _state_value(
        event,
        "state_before",
        "half",
    )

    if half == "top":
        return "home"
    if half == "bottom":
        return "away"

    raise ValueError(
        "event half must be top or bottom"
    )


def _event_outs(event: Any) -> int:
    outs = _value(
        event,
        "outs_recorded",
        (),
    )

    try:
        count = len(outs)
    except TypeError as exc:
        raise TypeError(
            "event outs_recorded must be sized"
        ) from exc

    if count < 0:
        raise ValueError(
            "event outs cannot be negative"
        )

    return count


def _percentile(
    ordered: tuple[float, ...],
    fraction: float,
) -> float | None:
    if not ordered:
        return None

    if len(ordered) == 1:
        return ordered[0]

    position = fraction * (len(ordered) - 1)
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower

    return (
        ordered[lower] * (1.0 - weight)
        + ordered[upper] * weight
    )


def _summary(
    values: Iterable[int | float],
) -> dict[str, Any]:
    ordered = tuple(
        sorted(float(value) for value in values)
    )

    if not ordered:
        return {
            "count": 0,
            "minimum": None,
            "p10": None,
            "median": None,
            "mean": None,
            "p90": None,
            "p95": None,
            "maximum": None,
        }

    return {
        "count": len(ordered),
        "minimum": ordered[0],
        "p10": _percentile(ordered, 0.10),
        "median": _percentile(ordered, 0.50),
        "mean": sum(ordered) / len(ordered),
        "p90": _percentile(ordered, 0.90),
        "p95": _percentile(ordered, 0.95),
        "maximum": ordered[-1],
    }


def _appearance_record(
    *,
    trial_index: int,
    team_side: str,
    appearance_index: int,
    pitcher_id: str,
    planned_role: str,
    first_event: Any,
) -> dict[str, Any]:
    inning = _state_value(
        first_event,
        "state_before",
        "inning",
    )
    half = _state_value(
        first_event,
        "state_before",
        "half",
    )
    outs = _state_value(
        first_event,
        "state_before",
        "outs",
    )
    sequence = _value(
        first_event,
        "sequence",
    )

    if not isinstance(inning, int) or inning < 1:
        raise ValueError(
            "appearance entry inning must be positive"
        )
    if half not in {"top", "bottom"}:
        raise ValueError(
            "appearance entry half must be valid"
        )
    if not isinstance(outs, int) or not 0 <= outs <= 2:
        raise ValueError(
            "appearance entry outs must be 0-2"
        )
    if not isinstance(sequence, int) or sequence < 0:
        raise ValueError(
            "event sequence must be non-negative"
        )

    return {
        "trial_index": trial_index,
        "team_side": team_side,
        "appearance_index": appearance_index,
        "pitcher_id": pitcher_id,
        "planned_role": planned_role,
        "actual_role": (
            "first_pitcher"
            if appearance_index == 0
            else "reliever"
        ),
        "entered_inning": inning,
        "entered_half": half,
        "entered_outs": outs,
        "entry_sequence": sequence,
        "exit_sequence": sequence,
        "batters_faced": 0,
        "outs_recorded": 0,
        "innings_equivalent": 0.0,
        "anomalies": [],
    }


def audit_canonical_pitcher_appearance_sequence(
    *,
    games: Iterable[Any],
    away_pitching_plan: Any,
    home_pitching_plan: Any,
) -> dict[str, Any]:
    """
    Audit actual pitcher ordering from immutable trial event streams.

    Only plate-appearance events define pitcher appearances. The audit
    observes existing simulations and does not influence pitcher choice.
    """

    try:
        trial_games = tuple(games)
    except TypeError as exc:
        raise TypeError(
            "games must be iterable"
        ) from exc

    if not trial_games:
        raise ValueError(
            "at least one canonical game is required"
        )

    plans = {
        "away": _plan(
            away_pitching_plan,
            "away",
        ),
        "home": _plan(
            home_pitching_plan,
            "home",
        ),
    }
    roles = {
        side: _planned_roles(plan)
        for side, plan in plans.items()
    }

    records = []
    trial_records = []
    all_anomalies = []
    affected_trials = set()

    for trial_index, game in enumerate(
        trial_games
    ):
        events = _value(game, "events")
        if events is None:
            raise TypeError(
                "canonical game must expose events"
            )

        active_record = {
            "away": None,
            "home": None,
        }
        used_ids = {
            "away": [],
            "home": [],
        }
        trial_anomalies = []

        for event in events:
            if (
                _value(
                    event,
                    "is_plate_appearance",
                    True,
                )
                is not True
            ):
                continue

            pitcher_id = _identifier(
                _value(event, "pitcher_id")
            )
            if pitcher_id is None:
                raise ValueError(
                    "plate appearance requires pitcher_id"
                )

            team_side = _fielding_side(event)
            current = active_record[team_side]

            if (
                current is None
                or current["pitcher_id"]
                != pitcher_id
            ):
                appearance_index = len(
                    used_ids[team_side]
                )
                planned_role = roles[
                    team_side
                ].get(
                    pitcher_id,
                    "unexpected_pitcher",
                )

                appearance = _appearance_record(
                    trial_index=trial_index,
                    team_side=team_side,
                    appearance_index=(
                        appearance_index
                    ),
                    pitcher_id=pitcher_id,
                    planned_role=planned_role,
                    first_event=event,
                )

                if pitcher_id in used_ids[
                    team_side
                ]:
                    appearance["anomalies"].append(
                        "pitcher_reentry"
                    )

                if (
                    appearance_index == 0
                    and pitcher_id
                    != plans[team_side]["starter_id"]
                ):
                    appearance["anomalies"].append(
                        "planned_starter_not_first"
                    )

                if (
                    appearance_index > 0
                    and pitcher_id
                    == plans[team_side]["starter_id"]
                ):
                    appearance["anomalies"].append(
                        "planned_starter_used_in_relief"
                    )

                if planned_role == (
                    "unexpected_pitcher"
                ):
                    appearance["anomalies"].append(
                        "pitcher_outside_plan"
                    )

                used_ids[team_side].append(
                    pitcher_id
                )
                active_record[team_side] = (
                    appearance
                )
                records.append(appearance)
                current = appearance

            current["batters_faced"] += 1
            current["outs_recorded"] += (
                _event_outs(event)
            )
            current["innings_equivalent"] = (
                current["outs_recorded"] / 3.0
            )
            current["exit_sequence"] = _value(
                event,
                "sequence",
            )

        for team_side, plan in plans.items():
            used = used_ids[team_side]

            if not used:
                trial_anomalies.append(
                    f"{team_side}:"
                    "no_pitcher_appearances"
                )
                continue

            preferred = plan[
                "preferred_replacement_pitcher_ids"
            ]
            if (
                plan["plan_type"]
                in {"opener_bulk", "tandem"}
                and len(used) > 1
                and preferred
                and used[1] != preferred[0]
            ):
                trial_anomalies.append(
                    f"{team_side}:"
                    "preferred_follower_skipped"
                )

        appearance_anomalies = [
            anomaly
            for record in records
            if record["trial_index"] == trial_index
            for anomaly in record["anomalies"]
        ]
        trial_anomalies.extend(
            appearance_anomalies
        )

        if trial_anomalies:
            affected_trials.add(trial_index)

        all_anomalies.extend(trial_anomalies)

        trial_records.append({
            "trial_index": trial_index,
            "away_pitcher_ids": list(
                used_ids["away"]
            ),
            "home_pitcher_ids": list(
                used_ids["home"]
            ),
            "away_appearance_count": len(
                used_ids["away"]
            ),
            "home_appearance_count": len(
                used_ids["home"]
            ),
            "anomalies": sorted(
                set(trial_anomalies)
            ),
        })

    role_outs = defaultdict(list)
    role_appearances = Counter()
    role_team_trials = defaultdict(set)

    for record in records:
        role = record["planned_role"]
        role_outs[role].append(
            record["outs_recorded"]
        )
        role_appearances[role] += 1
        role_team_trials[role].add((
            record["trial_index"],
            record["team_side"],
        ))
        record["anomalies"] = sorted(
            set(record["anomalies"])
        )

    role_summaries = {}

    for role in sorted(role_appearances):
        outs_summary = _summary(
            role_outs[role]
        )
        team_trial_appearance_count = len(
            role_team_trials[role]
        )
        role_summaries[role] = {
            "appearance_count":
                role_appearances[role],
            "team_trial_appearance_count": (
                team_trial_appearance_count
            ),
            "appearance_rate": (
                team_trial_appearance_count
                / (len(trial_games) * 2)
            ),
            "outs_recorded": outs_summary,
            "innings_equivalent": {
                key: (
                    value / 3.0
                    if (
                        key != "count"
                        and value is not None
                    )
                    else value
                )
                for key, value
                in outs_summary.items()
            },
        }

    anomaly_counts = dict(
        sorted(Counter(all_anomalies).items())
    )
    starter_relief_count = anomaly_counts.get(
        "planned_starter_used_in_relief",
        0,
    )

    return {
        "schema_version": SCHEMA_VERSION,
        "status": "observed",
        "audited": True,
        "trial_count": len(trial_games),
        "appearance_count": len(records),
        "affected_trial_count": len(
            affected_trials
        ),
        "affected_trial_rate": (
            len(affected_trials)
            / len(trial_games)
        ),
        "starter_relief_appearance_count": (
            starter_relief_count
        ),
        "starter_relief_detected": (
            starter_relief_count > 0
        ),
        "anomaly_counts": anomaly_counts,
        "role_summaries": role_summaries,
        "records": sorted(
            records,
            key=lambda record: (
                record["trial_index"],
                record["team_side"],
                record["appearance_index"],
            ),
        ),
        "trials": trial_records,
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
        "safety_checks": {
            "event_streams_unchanged": True,
            "pitching_plans_unchanged": True,
            "database_writes_performed": False,
            "production_authority_changed": False,
        },
        "decision": {
            "pitcher_sequence_activation_allowed":
                False,
            "production_activation_allowed": False,
            "recommended_next_slice": (
                "enforce_canonical_bullpen_eligibility"
            ),
        },
        "database_writes_performed": False,
        "production_authority_changed": False,
    }
