"""Collect cutoff-safe hitter-profile simulation-shadow evidence."""

from __future__ import annotations

from typing import Any, Mapping, Sequence


COLLECTION_SCHEMA_VERSION = (
    "hitter_profile_simulation_shadow_evidence_collection_v1"
)


def _target_dates(
    values: Sequence[Any],
) -> tuple[str, ...]:
    dates = sorted({
        str(value).strip()
        for value in values
        if str(value).strip()
    })

    if not dates:
        raise ValueError(
            "target_dates must contain at least one date"
        )

    return tuple(dates)


def _game_key(
    observation: Mapping[str, Any],
) -> str | None:
    game_pk = observation.get("game_pk")

    if (
        game_pk is None
        or isinstance(game_pk, bool)
    ):
        return None

    return str(game_pk)


def collect_hitter_profile_simulation_shadow_evidence(
    session: Any,
    *,
    enabled: bool = False,
    target_dates: Sequence[Any],
    acceptance_gates_by_date: Mapping[
        str,
        Mapping[str, Any],
    ],
    simulation_count: int = 1000,
    game_limit: int = 5,
    window_runner: Any = None,
    window_aggregator: Any = None,
    acceptance_evaluator: Any = None,
) -> dict[str, Any]:
    """Pool raw paired observations across deterministic dates."""

    dates = _target_dates(target_dates)

    if simulation_count <= 0:
        raise ValueError(
            "simulation_count must be positive"
        )
    if game_limit <= 0:
        raise ValueError(
            "game_limit must be positive"
        )

    if enabled is not True:
        return {
            "schema_version":
                COLLECTION_SCHEMA_VERSION,
            "status": "disabled",
            "target_dates": list(dates),
            "requested_date_count": len(dates),
            "requested_game_count":
                len(dates) * int(game_limit),
            "audited_game_count": 0,
            "observed_game_count": 0,
            "observation_rate": 0.0,
            "simulation_count":
                int(simulation_count),
            "records": [],
            "windows": [],
            "simulation_acceptance": None,
            "decision": {
                "extended_shadow_evaluation_allowed":
                    False,
                "production_activation_allowed":
                    False,
                "recommended_next_slice":
                    "collect_additional_hitter_profile_"
                    "simulation_shadow_evidence",
            },
            "database_writes_performed": False,
            "production_authority_changed": False,
        }

    if window_runner is None:
        from mlb_app.simulation.shadow.hitter_profile_live_simulation_shadow_window import (
            run_hitter_profile_live_simulation_shadow_window,
        )

        window_runner = (
            run_hitter_profile_live_simulation_shadow_window
        )

    if window_aggregator is None:
        from mlb_app.simulation.shadow.hitter_profile_live_simulation_shadow_window import (
            aggregate_hitter_profile_live_simulation_shadow_window,
        )

        window_aggregator = (
            aggregate_hitter_profile_live_simulation_shadow_window
        )

    if acceptance_evaluator is None:
        from mlb_app.simulation.shadow.hitter_profile_simulation_shadow_acceptance_gate import (
            evaluate_hitter_profile_simulation_shadow_acceptance,
        )

        acceptance_evaluator = (
            evaluate_hitter_profile_simulation_shadow_acceptance
        )

    observations_by_game: dict[
        str,
        dict[str, Any],
    ] = {}
    windows: list[dict[str, Any]] = []

    for date_index, target_date in enumerate(dates):
        captured: list[dict[str, Any]] = []

        def observe(
            observation: Mapping[str, Any],
        ) -> None:
            record = dict(observation)
            record["evidence_target_date"] = (
                target_date
            )
            captured.append(record)

        try:
            window = dict(
                window_runner(
                    session,
                    enabled=True,
                    target_date=target_date,
                    acceptance_gate=dict(
                        acceptance_gates_by_date.get(
                            target_date,
                            {},
                        )
                    ),
                    simulation_count=int(
                        simulation_count
                    ),
                    game_limit=int(game_limit),
                    observation_observer=observe,
                )
            )
        except Exception as exc:
            window = {
                "status": "blocked",
                "audited_game_count": 0,
                "observed_game_count": 0,
                "blocker_counts": {
                    "evidence_window_error": 1,
                },
                "error_type":
                    type(exc).__name__,
                "error_message": str(exc),
                "database_writes_performed":
                    False,
                "production_authority_changed":
                    False,
            }

        if not captured:
            blocker_counts = dict(
                window.get("blocker_counts")
                or {}
            )
            blockers = sorted(
                str(blocker)
                for blocker, count in (
                    blocker_counts.items()
                )
                if count
            )
            captured.append({
                "game_pk": -(date_index + 1),
                "game_date": target_date,
                "evidence_target_date":
                    target_date,
                "status": "blocked",
                "blockers": (
                    blockers
                    or [
                        "live_window_no_game_observations"
                    ]
                ),
                "candidate_materialization": {
                    "status": "blocked",
                    "candidate_count": 0,
                    "state_counts": {},
                    "blocker_counts":
                        blocker_counts,
                    "blockers": blockers,
                    "records": [],
                },
                "database_writes_performed":
                    False,
                "production_authority_changed":
                    False,
            })

        duplicate_game_count = 0

        for observation in captured:
            key = _game_key(observation)

            if key is None:
                continue
            if key in observations_by_game:
                duplicate_game_count += 1
                continue

            observations_by_game[key] = (
                observation
            )

        windows.append({
            "target_date": target_date,
            "status": window.get("status"),
            "audited_game_count":
                window.get(
                    "audited_game_count"
                ),
            "observed_game_count":
                window.get(
                    "observed_game_count"
                ),
            "captured_observation_count":
                len(captured),
            "duplicate_game_count":
                duplicate_game_count,
            "blocker_counts": dict(
                window.get("blocker_counts")
                or {}
            ),
            "source": dict(
                window.get("source") or {}
            ),
            "database_writes_performed":
                False,
            "production_authority_changed":
                False,
        })

    observations = sorted(
        observations_by_game.values(),
        key=lambda value: (
            str(
                value.get(
                    "evidence_target_date"
                )
                or ""
            ),
            str(value.get("game_pk") or ""),
        ),
    )

    combined = dict(
        window_aggregator(
            observations=observations,
            target_date=(
                dates[0]
                if len(dates) == 1
                else f"{dates[0]}..{dates[-1]}"
            ),
            requested_game_count=(
                len(dates) * int(game_limit)
            ),
            simulation_count=int(
                simulation_count
            ),
        )
    )

    combined["schema_version"] = (
        COLLECTION_SCHEMA_VERSION
    )
    combined["target_dates"] = list(dates)
    combined["requested_date_count"] = len(
        dates
    )
    combined["captured_observation_count"] = (
        sum(
            window[
                "captured_observation_count"
            ]
            for window in windows
        )
    )
    combined["deduplicated_game_count"] = len(
        observations
    )
    combined["duplicate_game_count"] = sum(
        window["duplicate_game_count"]
        for window in windows
    )
    combined["windows"] = windows
    combined[
        "database_writes_performed"
    ] = False
    combined[
        "production_authority_changed"
    ] = False

    simulation_acceptance = dict(
        acceptance_evaluator(combined)
    )
    combined["simulation_acceptance"] = (
        simulation_acceptance
    )
    combined["decision"] = {
        "extended_shadow_evaluation_allowed":
            (
                simulation_acceptance.get(
                    "decision"
                )
                or {}
            ).get(
                "extended_shadow_evaluation_allowed"
            )
            is True,
        "production_activation_allowed": False,
        "recommended_next_slice": (
            simulation_acceptance.get(
                "decision"
            )
            or {}
        ).get(
            "recommended_next_slice",
            "collect_additional_hitter_profile_"
            "simulation_shadow_evidence",
        ),
    }
    combined[
        "database_writes_performed"
    ] = False
    combined[
        "production_authority_changed"
    ] = False

    return combined
