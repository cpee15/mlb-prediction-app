"""Request-scoped hitter-profile shadow candidate materialization."""

from __future__ import annotations

import copy
from collections.abc import Callable, Mapping
from typing import Any


SCHEMA_VERSION = (
    "hitter_profile_simulation_shadow_candidate_materialization_v1"
)


def _accepted_gate(
    gate: Mapping[str, Any] | None,
) -> bool:
    payload = dict(gate or {})
    decision = dict(
        payload.get("decision") or {}
    )
    activation_scope = dict(
        payload.get("activation_scope") or {}
    )

    return (
        payload.get("status")
        == "accepted_for_feature_flag_integration"
        and payload.get("gate_passed") is True
        and decision.get(
            "feature_flag_integration_allowed"
        )
        is True
        and decision.get(
            "production_activation_allowed"
        )
        is False
        and activation_scope.get(
            "production_enabled"
        )
        is False
        and payload.get(
            "production_authority_changed"
        )
        is False
    )


def _base_result(
    *,
    status: str,
    enabled: bool,
    blockers: list[str],
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "enabled": enabled,
        "materialized": False,
        "candidate_results": {},
        "candidate_batter_count": 0,
        "requested_batter_count": 0,
        "blocked_batter_count": 0,
        "state_counts": {},
        "blocker_counts": {},
        "records": [],
        "blockers": sorted(set(blockers)),
        "database_writes_performed": False,
        "production_inputs_unchanged": True,
        "production_authority_changed": False,
    }


def _identifier(value: Any) -> str | None:
    if value is None:
        return None
    result = str(value).strip()
    return result or None


def _pitcher_split(value: Any) -> str | None:
    hand = str(value or "").strip().upper()
    if hand == "R":
        return "vsR"
    if hand == "L":
        return "vsL"
    return None


def _increment(
    values: dict[str, int],
    key: str,
) -> None:
    values[key] = values.get(key, 0) + 1


def materialize_hitter_profile_simulation_shadow_candidates(
    session: Any,
    *,
    enabled: bool = False,
    acceptance_gate: Mapping[str, Any] | None = None,
    lineups: Any,
    exact_artifact_discovery: Any,
    pitcher_hands_by_id: Mapping[str, Any] | None = None,
    pitcher_profiles_by_id: Mapping[
        str,
        Mapping[str, Any],
    ] | None = None,
    environment_profile: Mapping[
        str,
        Any,
    ] | None = None,
    season: int,
    as_of_date: Any,
    readiness: Mapping[str, Any] | None = None,
    combined_profile_loader: Callable[..., Mapping[str, Any]]
    | None = None,
    signal_loader: Callable[..., Mapping[str, Any]]
    | None = None,
    canary_runner: Callable[..., Mapping[str, Any]]
    | None = None,
) -> dict[str, Any]:
    """Build eligible canary results once, outside simulation execution."""

    if enabled is not True:
        return _base_result(
            status="disabled",
            enabled=False,
            blockers=[],
        )

    if not _accepted_gate(acceptance_gate):
        return _base_result(
            status="blocked",
            enabled=True,
            blockers=[
                "canary_acceptance_gate_not_passed",
            ],
        )

    if getattr(lineups, "ready", False) is not True:
        return _base_result(
            status="blocked",
            enabled=True,
            blockers=[
                "canonical_lineups_not_ready",
            ],
        )

    artifact = getattr(
        exact_artifact_discovery,
        "artifact",
        None,
    )
    if artifact is None:
        return _base_result(
            status="blocked",
            enabled=True,
            blockers=[
                "canonical_exact_artifact_not_ready",
            ],
        )

    if environment_profile is None:
        return _base_result(
            status="blocked",
            enabled=True,
            blockers=[
                "environment_profile_unavailable",
            ],
        )

    if combined_profile_loader is None:
        from mlb_app.simulation.shadow.combined_hitter_profile import (
            load_combined_shadow_hitter_profile,
        )

        combined_profile_loader = (
            load_combined_shadow_hitter_profile
        )

    if signal_loader is None:
        from mlb_app.simulation.shadow.hitter_profile_canary_signal_adapter import (
            load_hitter_profile_canary_signals,
        )

        signal_loader = (
            load_hitter_profile_canary_signals
        )

    if canary_runner is None:
        from mlb_app.simulation.shadow.hitter_profile_shadow_canary import (
            run_hitter_profile_shadow_canary,
        )

        canary_runner = (
            run_hitter_profile_shadow_canary
        )

    lineup_ids = {
        str(value)
        for value in (
            tuple(lineups.away_player_ids)
            + tuple(lineups.home_player_ids)
        )
    }
    pitcher_hands = {
        str(key): value
        for key, value in (
            pitcher_hands_by_id or {}
        ).items()
    }
    pitcher_profiles = {
        str(key): copy.deepcopy(dict(value))
        for key, value in (
            pitcher_profiles_by_id or {}
        ).items()
    }
    environment = copy.deepcopy(
        dict(environment_profile or {})
    )

    batter_matchups: dict[
        str,
        tuple[str, str],
    ] = {}
    matchup_blockers: dict[str, str] = {}
    ambiguous_batters: set[str] = set()

    for artifact_record in artifact.records:
        batter_id = _identifier(
            artifact_record.batter_id
        )
        pitcher_id = _identifier(
            artifact_record.pitcher_id
        )

        if (
            batter_id is None
            or pitcher_id is None
            or batter_id not in lineup_ids
        ):
            continue

        split = _pitcher_split(
            pitcher_hands.get(pitcher_id)
        )
        if split is None:
            matchup_blockers[batter_id] = (
                "opposing_pitcher_hand_unavailable"
            )
            continue

        if pitcher_id not in pitcher_profiles:
            matchup_blockers[batter_id] = (
                "opposing_pitcher_profile_unavailable"
            )
            continue

        matchup = (pitcher_id, split)
        existing = batter_matchups.get(batter_id)
        if (
            existing is not None
            and existing != matchup
        ):
            ambiguous_batters.add(batter_id)
            continue

        batter_matchups[batter_id] = matchup

    for batter_id in ambiguous_batters:
        batter_matchups.pop(batter_id, None)

    records: list[dict[str, Any]] = []
    candidate_results: dict[
        str,
        Mapping[str, Any],
    ] = {}
    state_counts: dict[str, int] = {}
    blocker_counts: dict[str, int] = {}

    unresolved_batters = sorted(
        lineup_ids - set(batter_matchups)
    )
    for batter_id in unresolved_batters:
        blocker = (
            "ambiguous_opposing_pitcher_matchup"
            if batter_id in ambiguous_batters
            else matchup_blockers.get(
                batter_id,
                "canonical_matchup_record_unavailable",
            )
        )
        records.append({
            "batter_id": batter_id,
            "split": None,
            "state": "matchup_blocked",
            "executed": False,
            "blockers": [blocker],
        })
        _increment(
            state_counts,
            "matchup_blocked",
        )
        _increment(blocker_counts, blocker)

    for batter_id in sorted(batter_matchups):
        pitcher_id, split = (
            batter_matchups[batter_id]
        )
        record: dict[str, Any] = {
            "batter_id": batter_id,
            "pitcher_id": pitcher_id,
            "split": split,
            "state": "not_evaluated",
            "executed": False,
            "blockers": [],
        }

        try:
            signals = dict(
                signal_loader(
                    session,
                    player_id=int(batter_id),
                    season=int(season),
                    split=split,
                    as_of_date=as_of_date,
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
                    signals.get("blockers") or ()
                )
            else:
                combined = dict(
                    combined_profile_loader(
                        session,
                        player_id=int(batter_id),
                        season=int(season),
                        split=split,
                        as_of_date=as_of_date,
                    )
                )
                record["combined_status"] = (
                    combined.get("status")
                )

                if combined.get("status") != "ready":
                    record["state"] = (
                        "production_profile_blocked"
                    )
                    record["blockers"] = sorted({
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
                    })
                else:
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
                    canary = dict(
                        canary_runner(
                            enabled=True,
                            production_batter_profile=(
                                production_profile
                            ),
                            pitcher_profile=copy.deepcopy(
                                pitcher_profiles.get(
                                    pitcher_id,
                                    {},
                                )
                            ),
                            environment_profile=(
                                copy.deepcopy(
                                    environment
                                )
                            ),
                            candidate_signals=signals,
                            readiness=readiness,
                        )
                    )

                    record["executed"] = (
                        canary.get("executed")
                        is True
                    )
                    record[
                        "production_profile_unchanged"
                    ] = (
                        production_profile
                        == production_original
                    )

                    if (
                        canary.get("status") == "ready"
                        and canary.get("executed")
                        is True
                    ):
                        record["state"] = "materialized"
                        candidate_results[
                            batter_id
                        ] = canary
                    else:
                        record["state"] = (
                            "canary_blocked"
                        )
                        record["blockers"] = list(
                            canary.get("blockers")
                            or ()
                        )
        except Exception as exc:
            record["state"] = "materialization_error"
            record["blockers"] = [
                "materialization_exception",
            ]
            record["error_type"] = (
                type(exc).__name__
            )

        _increment(
            state_counts,
            record["state"],
        )
        for blocker in record["blockers"]:
            _increment(
                blocker_counts,
                str(blocker),
            )
        records.append(record)

    candidate_count = len(candidate_results)
    requested_count = len(lineup_ids)
    blocked_count = (
        requested_count - candidate_count
    )

    return {
        "schema_version": SCHEMA_VERSION,
        "status": (
            "ready"
            if candidate_count > 0
            else "fallback"
        ),
        "enabled": True,
        "materialized": candidate_count > 0,
        "candidate_results":
            candidate_results,
        "candidate_batter_count":
            candidate_count,
        "requested_batter_count":
            requested_count,
        "blocked_batter_count":
            blocked_count,
        "state_counts":
            dict(sorted(state_counts.items())),
        "blocker_counts":
            dict(sorted(blocker_counts.items())),
        "records": records,
        "blockers": (
            []
            if candidate_count > 0
            else [
                "no_eligible_hitter_profile_candidates",
            ]
        ),
        "database_writes_performed": False,
        "production_inputs_unchanged": True,
        "production_authority_changed": False,
    }
