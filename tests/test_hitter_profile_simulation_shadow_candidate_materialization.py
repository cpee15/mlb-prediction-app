from types import SimpleNamespace

from mlb_app.simulation.shadow.hitter_profile_simulation_shadow_candidate_materialization import (
    materialize_hitter_profile_simulation_shadow_candidates,
)


def gate():
    return {
        "status":
            "accepted_for_feature_flag_integration",
        "gate_passed": True,
        "decision": {
            "feature_flag_integration_allowed":
                True,
            "production_activation_allowed":
                False,
        },
        "activation_scope": {
            "production_enabled": False,
        },
        "production_authority_changed": False,
    }


def lineups():
    return SimpleNamespace(
        ready=True,
        away_player_ids=("10",),
        home_player_ids=("20",),
    )


def artifact():
    return SimpleNamespace(
        records=(
            SimpleNamespace(
                batter_id="10",
                pitcher_id="200",
            ),
            SimpleNamespace(
                batter_id="20",
                pitcher_id="100",
            ),
        )
    )


def discovery():
    return SimpleNamespace(
        artifact=artifact(),
    )


def signals(
    session,
    *,
    player_id,
    season,
    split,
    as_of_date,
):
    return {
        "status": "ready",
        "cutoff_safe": True,
        "coverage": {
            "pitch_count": 100,
        },
        "signals": {
            "called_ball_rate": 0.30,
        },
        "blockers": [],
    }


def combined(
    session,
    *,
    player_id,
    season,
    split,
    as_of_date,
):
    return {
        "status": "ready",
        "candidate_profile": {
            "contact_skill": {
                "hit_skill": 0.25,
                "k_rate": 0.22,
            },
            "plate_discipline": {
                "bb_rate": 0.08,
            },
            "power": {
                "iso": 0.17,
            },
        },
        "evidence_blockers": {
            "actual": [],
            "expected": [],
        },
    }


def canary(
    *,
    enabled,
    production_batter_profile,
    pitcher_profile,
    environment_profile,
    candidate_signals,
    readiness,
):
    return {
        "status": "ready",
        "executed": True,
        "production_inputs_unchanged": True,
        "production_authority_changed": False,
        "fallback_telemetry": {
            "fallback_count": 0,
        },
        "probability_deltas": {
            "bb": 0.01,
            "double": 0.0,
            "hbp": 0.0,
            "hr": 0.0,
            "k": -0.01,
            "out": 0.0,
            "reached_on_error": 0.0,
            "single": 0.0,
            "triple": 0.0,
        },
        "blockers": [],
    }


def materialize(**overrides):
    values = {
        "enabled": True,
        "acceptance_gate": gate(),
        "lineups": lineups(),
        "exact_artifact_discovery":
            discovery(),
        "pitcher_hands_by_id": {
            "100": "R",
            "200": "L",
        },
        "pitcher_profiles_by_id": {
            "100": {
                "strikeout_rate": 0.24,
            },
            "200": {
                "strikeout_rate": 0.21,
            },
        },
        "environment_profile": {
            "park_run_factor": 1.02,
        },
        "season": 2026,
        "as_of_date": "2026-05-03",
        "readiness": {
            "status": "ready_for_activation",
        },
        "combined_profile_loader": combined,
        "signal_loader": signals,
        "canary_runner": canary,
    }
    values.update(overrides)
    return (
        materialize_hitter_profile_simulation_shadow_candidates(
            object(),
            **values,
        )
    )


def test_disabled_by_default_without_loading_evidence():
    result = (
        materialize_hitter_profile_simulation_shadow_candidates(
            object(),
            lineups=lineups(),
            exact_artifact_discovery=discovery(),
            season=2026,
            as_of_date="2026-05-03",
        )
    )

    assert result["status"] == "disabled"
    assert result["materialized"] is False
    assert result["candidate_results"] == {}


def test_rejects_unaccepted_gate():
    blocked = gate()
    blocked["gate_passed"] = False

    result = materialize(
        acceptance_gate=blocked,
    )

    assert result["status"] == "blocked"
    assert result["blockers"] == [
        "canary_acceptance_gate_not_passed",
    ]


def test_materializes_lineup_candidates_by_batter_id():
    result = materialize()

    assert result["status"] == "ready"
    assert result["materialized"] is True
    assert result["candidate_batter_count"] == 2
    assert result["requested_batter_count"] == 2
    assert sorted(
        result["candidate_results"]
    ) == ["10", "20"]
    assert result["state_counts"] == {
        "materialized": 2,
    }
    assert (
        result["database_writes_performed"]
        is False
    )
    assert (
        result["production_authority_changed"]
        is False
    )


def test_uses_opposing_pitcher_hand_for_split():
    observed = {}

    def capture(
        session,
        *,
        player_id,
        season,
        split,
        as_of_date,
    ):
        observed[player_id] = split
        return signals(
            session,
            player_id=player_id,
            season=season,
            split=split,
            as_of_date=as_of_date,
        )

    result = materialize(
        signal_loader=capture,
    )

    assert result["status"] == "ready"
    assert observed == {
        10: "vsL",
        20: "vsR",
    }


def test_missing_pitcher_hand_fails_open_per_batter():
    result = materialize(
        pitcher_hands_by_id={
            "100": "R",
        },
    )

    assert result["status"] == "ready"
    assert result["candidate_batter_count"] == 1
    assert result["blocked_batter_count"] == 1
    assert result["state_counts"] == {
        "matchup_blocked": 1,
        "materialized": 1,
    }
    assert result["blocker_counts"] == {
        "opposing_pitcher_hand_unavailable": 1,
    }


def test_signal_evidence_failure_isolated_to_batter():
    def partial(
        session,
        *,
        player_id,
        season,
        split,
        as_of_date,
    ):
        if player_id == 10:
            return {
                "status": "blocked",
                "blockers": [
                    "insufficient_pre_cutoff_ab",
                ],
            }
        return signals(
            session,
            player_id=player_id,
            season=season,
            split=split,
            as_of_date=as_of_date,
        )

    result = materialize(
        signal_loader=partial,
    )

    assert result["status"] == "ready"
    assert sorted(
        result["candidate_results"]
    ) == ["20"]
    assert result["state_counts"] == {
        "materialized": 1,
        "signal_evidence_blocked": 1,
    }
    assert result["blocker_counts"] == {
        "insufficient_pre_cutoff_ab": 1,
    }


def test_all_blocked_returns_fallback():
    result = materialize(
        pitcher_hands_by_id={},
    )

    assert result["status"] == "fallback"
    assert result["materialized"] is False
    assert result["candidate_results"] == {}
    assert result["blockers"] == [
        "no_eligible_hitter_profile_candidates",
    ]

def test_passes_matching_pitcher_and_environment_context():
    observed = {}

    def capture(
        *,
        enabled,
        production_batter_profile,
        pitcher_profile,
        environment_profile,
        candidate_signals,
        readiness,
    ):
        observed[
            pitcher_profile["strikeout_rate"]
        ] = environment_profile[
            "park_run_factor"
        ]
        return canary(
            enabled=enabled,
            production_batter_profile=(
                production_batter_profile
            ),
            pitcher_profile=pitcher_profile,
            environment_profile=environment_profile,
            candidate_signals=candidate_signals,
            readiness=readiness,
        )

    result = materialize(
        canary_runner=capture,
    )

    assert result["status"] == "ready"
    assert observed == {
        0.21: 1.02,
        0.24: 1.02,
    }

def test_missing_pitcher_profile_fails_open_per_batter():
    result = materialize(
        pitcher_profiles_by_id={
            "100": {
                "strikeout_rate": 0.24,
            },
        },
    )

    assert result["status"] == "ready"
    assert sorted(
        result["candidate_results"]
    ) == ["20"]
    assert result["blocker_counts"] == {
        "opposing_pitcher_profile_unavailable": 1,
    }


def test_missing_environment_blocks_materialization():
    result = materialize(
        environment_profile=None,
    )

    assert result["status"] == "blocked"
    assert result["materialized"] is False
    assert result["candidate_results"] == {}
    assert result["blockers"] == [
        "environment_profile_unavailable",
    ]
