"""Run the read-only hitter-profile shadow canary audit."""

from __future__ import annotations

import argparse
import collections
import copy
import datetime as dt
import json
import math
from typing import Any, Iterable, Mapping

from sqlalchemy import func

from mlb_app.database import StatcastEvent
from mlb_app.model_projection_routes import (
    _session_factory,
)
from mlb_app.simulation.shadow.combined_hitter_profile import (
    load_combined_shadow_hitter_profile,
)
from mlb_app.simulation.shadow.hitter_profile_activation_readiness import (
    synthesize_hitter_profile_activation_readiness,
)
from mlb_app.simulation.shadow.hitter_profile_canary_signal_adapter import (
    load_hitter_profile_canary_signals,
)
from mlb_app.simulation.shadow.hitter_profile_shadow_canary import (
    run_hitter_profile_shadow_canary,
)


SCHEMA_VERSION = (
    "hitter_profile_shadow_canary_audit_v1"
)
OUTCOME_KEYS = (
    "bb",
    "double",
    "hbp",
    "hr",
    "k",
    "out",
    "reached_on_error",
    "single",
    "triple",
)


def _date(value: Any) -> dt.date | None:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    try:
        return dt.date.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None


def percentile(
    values: Iterable[float],
    quantile: float,
) -> float | None:
    ordered = sorted(
        float(value)
        for value in values
        if math.isfinite(float(value))
    )
    if not ordered:
        return None
    if len(ordered) == 1:
        return ordered[0]

    position = (
        max(0.0, min(1.0, float(quantile)))
        * (len(ordered) - 1)
    )
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        return ordered[lower]

    fraction = position - lower
    return (
        ordered[lower]
        + (
            ordered[upper]
            - ordered[lower]
        )
        * fraction
    )


def summarize_canary_records(
    records: Iterable[Mapping[str, Any]],
    *,
    season: int,
    as_of_date: Any,
    candidate_count: int,
    limit: int | None,
) -> dict[str, Any]:
    rows = [
        dict(record)
        for record in records
    ]
    executed = [
        row
        for row in rows
        if row.get("executed") is True
    ]

    state_counts = collections.Counter(
        str(row.get("state") or "unknown")
        for row in rows
    )
    blocker_counts = collections.Counter()
    fallback_counts = collections.Counter()
    maximum_deltas = []
    probability_sums = []
    absolute_outcome_deltas = {
        key: []
        for key in OUTCOME_KEYS
    }

    for row in rows:
        blocker_counts.update(
            row.get("blockers") or ()
        )
        canary = dict(
            row.get("canary") or {}
        )
        telemetry = dict(
            canary.get("fallback_telemetry")
            or {}
        )
        for signal, payload in (
            telemetry.get("by_signal") or {}
        ).items():
            if payload.get("fallback_used") is True:
                fallback_counts[str(signal)] += 1

        maximum_delta = canary.get(
            "maximum_absolute_probability_delta"
        )
        if maximum_delta is not None:
            maximum_deltas.append(
                float(maximum_delta)
            )

        probability_sum = canary.get(
            "candidate_probability_sum"
        )
        if probability_sum is not None:
            probability_sums.append(
                float(probability_sum)
            )

        for key, value in (
            canary.get("probability_deltas")
            or {}
        ).items():
            if (
                key in absolute_outcome_deltas
                and value is not None
            ):
                absolute_outcome_deltas[key].append(
                    abs(float(value))
                )

    execution_count = len(executed)
    fallback_total = sum(
        fallback_counts.values()
    )
    possible_signal_count = (
        execution_count * 4
    )

    production_inputs_unchanged = all(
        (
            row.get("canary") or {}
        ).get("production_inputs_unchanged")
        is True
        for row in executed
    )
    production_authority_unchanged = all(
        (
            row.get("canary") or {}
        ).get("production_authority_changed")
        is False
        for row in executed
    )
    probabilities_normalized = all(
        0.999 <= value <= 1.001
        for value in probability_sums
    )

    return {
        "schema_version": SCHEMA_VERSION,
        "status": "observed",
        "season": int(season),
        "as_of_date": str(as_of_date),
        "candidate_population_count":
            int(candidate_count),
        "audit_limit": limit,
        "audited_player_split_count":
            len(rows),
        "executed_player_split_count":
            execution_count,
        "execution_rate": (
            execution_count / len(rows)
            if rows
            else 0.0
        ),
        "state_counts": dict(
            sorted(state_counts.items())
        ),
        "blocker_counts": dict(
            sorted(blocker_counts.items())
        ),
        "fallback_telemetry": {
            "fallback_counts_by_signal": dict(
                sorted(fallback_counts.items())
            ),
            "fallback_count": fallback_total,
            "possible_signal_count":
                possible_signal_count,
            "fallback_rate": (
                fallback_total
                / possible_signal_count
                if possible_signal_count
                else 0.0
            ),
        },
        "maximum_absolute_probability_delta": {
            "median": percentile(
                maximum_deltas,
                0.50,
            ),
            "p95": percentile(
                maximum_deltas,
                0.95,
            ),
            "maximum": (
                max(maximum_deltas)
                if maximum_deltas
                else None
            ),
        },
        "absolute_probability_delta_by_outcome": {
            key: {
                "median": percentile(
                    values,
                    0.50,
                ),
                "p95": percentile(
                    values,
                    0.95,
                ),
                "maximum": (
                    max(values)
                    if values
                    else None
                ),
            }
            for key, values in (
                absolute_outcome_deltas.items()
            )
        },
        "safety_checks": {
            "all_production_inputs_unchanged":
                production_inputs_unchanged,
            "all_production_authority_unchanged":
                production_authority_unchanged,
            "all_candidate_probabilities_normalized":
                probabilities_normalized,
            "database_writes_performed": False,
        },
        "decision": {
            "production_activation_allowed": False,
            "parameter_selected": False,
            "promotion_thresholds_selected": False,
            "recommended_next_slice":
                "define_hitter_profile_canary_acceptance_gates",
        },
        "production_authority_changed": False,
        "records": rows,
    }


def run_live_audit(
    *,
    season: int | None = None,
    as_of_date: Any = None,
    limit: int | None = 100,
) -> dict[str, Any]:
    factory = _session_factory()
    session = factory()

    try:
        source_maximum = (
            session.query(
                func.max(StatcastEvent.game_date)
            )
            .scalar()
        )
        cutoff = (
            _date(as_of_date)
            or source_maximum
        )
        if cutoff is None:
            return summarize_canary_records(
                [],
                season=(
                    int(season)
                    if season is not None
                    else dt.date.today().year
                ),
                as_of_date=None,
                candidate_count=0,
                limit=limit,
            )

        target_season = int(
            season or cutoff.year
        )
        query = (
            session.query(
                StatcastEvent.batter_id,
                StatcastEvent.p_throws,
                func.count(
                    StatcastEvent.id
                ).label("row_count"),
            )
            .filter(
                StatcastEvent.game_date
                >= dt.date(target_season, 1, 1),
                StatcastEvent.game_date <= cutoff,
                StatcastEvent.p_throws.in_(
                    ("R", "L")
                ),
            )
            .group_by(
                StatcastEvent.batter_id,
                StatcastEvent.p_throws,
            )
            .order_by(
                func.count(
                    StatcastEvent.id
                ).desc(),
                StatcastEvent.batter_id,
                StatcastEvent.p_throws,
            )
        )
        candidates = query.all()
        candidate_count = len(candidates)
        if limit is not None:
            candidates = candidates[
                : max(0, int(limit))
            ]

        readiness = (
            synthesize_hitter_profile_activation_readiness()
        )
        records = []

        for player_id, hand, row_count in candidates:
            split = (
                "vsR"
                if hand == "R"
                else "vsL"
            )
            record = {
                "player_id": int(player_id),
                "split": split,
                "raw_row_count":
                    int(row_count),
                "state": "not_evaluated",
                "executed": False,
                "blockers": [],
            }

            try:
                signals = (
                    load_hitter_profile_canary_signals(
                        session,
                        player_id=int(player_id),
                        season=target_season,
                        split=split,
                        as_of_date=cutoff,
                    )
                )
                record["signal_status"] = (
                    signals.get("status")
                )
                record["signal_coverage"] = dict(
                    signals.get("coverage") or {}
                )

                if signals.get("status") != "ready":
                    record["state"] = (
                        "signal_evidence_blocked"
                    )
                    record["blockers"] = list(
                        signals.get("blockers")
                        or ()
                    )
                    records.append(record)
                    continue

                combined = (
                    load_combined_shadow_hitter_profile(
                        session,
                        player_id=int(player_id),
                        season=target_season,
                        split=split,
                        as_of_date=cutoff,
                    )
                )
                record["combined_status"] = (
                    combined.get("status")
                )

                if combined.get("status") != "ready":
                    record["state"] = (
                        "production_profile_blocked"
                    )
                    record["blockers"] = sorted(
                        {
                            blocker
                            for blockers in (
                                combined.get(
                                    "evidence_blockers"
                                )
                                or {}
                            ).values()
                            for blocker in (
                                blockers or ()
                            )
                        }
                    )
                    records.append(record)
                    continue

                production_profile = copy.deepcopy(
                    dict(
                        combined.get(
                            "candidate_profile"
                        )
                        or {}
                    )
                )
                production_original = copy.deepcopy(
                    production_profile
                )

                canary = (
                    run_hitter_profile_shadow_canary(
                        enabled=True,
                        production_batter_profile=(
                            production_profile
                        ),
                        candidate_signals=signals,
                        readiness=readiness,
                    )
                )
                record["canary"] = canary
                record["executed"] = (
                    canary.get("executed")
                    is True
                )
                record["state"] = (
                    "executed"
                    if canary.get("status")
                    == "ready"
                    else "canary_blocked"
                )
                record["blockers"] = list(
                    canary.get("blockers")
                    or ()
                )
                record[
                    "production_profile_unchanged"
                ] = (
                    production_profile
                    == production_original
                )
            except Exception as exc:
                record["state"] = "audit_error"
                record["blockers"] = [
                    "audit_exception",
                ]
                record["error_type"] = (
                    type(exc).__name__
                )

            records.append(record)

        return summarize_canary_records(
            records,
            season=target_season,
            as_of_date=cutoff.isoformat(),
            candidate_count=candidate_count,
            limit=limit,
        )
    finally:
        session.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--season",
        type=int,
    )
    parser.add_argument(
        "--as-of-date",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
    )
    args = parser.parse_args()

    print(
        json.dumps(
            run_live_audit(
                season=args.season,
                as_of_date=args.as_of_date,
                limit=args.limit,
            ),
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
