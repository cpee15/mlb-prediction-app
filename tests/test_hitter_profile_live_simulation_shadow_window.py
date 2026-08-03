from mlb_app.simulation.shadow.hitter_profile_live_simulation_shadow_window import (
    aggregate_hitter_profile_live_simulation_shadow_window,
    run_hitter_profile_live_simulation_shadow_window,
)


def observation(
    *,
    game_pk,
    delta,
    scope="game_probability",
    metric="home_win_probability",
):
    return {
        "game_pk": game_pk,
        "status": "observed",
        "blockers": [],
        "comparison": {
            "comparison_count": 1,
            "absolute_delta_summary": {
                "maximum": abs(delta),
            },
            "records": [
                {
                    "scope": scope,
                    "metric": metric,
                    "delta": delta,
                    "absolute_delta": abs(delta),
                },
            ],
        },
        "safety_checks": {
            "production_authority_unchanged": True,
            "production_inputs_unchanged": True,
            "simulation_counts_match": True,
        },
        "database_writes_performed": False,
        "production_authority_changed": False,
    }


def test_aggregates_observed_game_window():
    result = (
        aggregate_hitter_profile_live_simulation_shadow_window(
            observations=[
                observation(
                    game_pk=2,
                    delta=-0.04,
                ),
                observation(
                    game_pk=1,
                    delta=0.02,
                ),
            ],
            target_date="2026-05-03",
            requested_game_count=2,
            simulation_count=1000,
        )
    )

    assert result["status"] == "observed"
    assert result["audited_game_count"] == 2
    assert result["observed_game_count"] == 2
    assert result["observation_rate"] == 1.0
    assert result["comparison_count"] == 2
    assert [
        record["game_pk"]
        for record in result["records"]
    ] == [1, 2]
    assert result["maximum_absolute_delta"][
        "median"
    ] == 0.03
    assert result["absolute_delta_by_scope"][
        "game_probability"
    ]["maximum"] == 0.04
    assert result["absolute_delta_by_metric"][
        "home_win_probability"
    ]["count"] == 2


def test_isolates_blocked_games_and_blockers():
    blocked = {
        "game_pk": 3,
        "status": "blocked",
        "blockers": [
            "candidate_materialization_not_ready",
        ],
        "comparison": {},
        "safety_checks": {
            "production_authority_unchanged": True,
            "production_inputs_unchanged": True,
            "simulation_counts_match": True,
        },
        "database_writes_performed": False,
        "production_authority_changed": False,
    }

    result = (
        aggregate_hitter_profile_live_simulation_shadow_window(
            observations=[
                observation(
                    game_pk=1,
                    delta=0.01,
                ),
                blocked,
            ],
            target_date="2026-05-03",
            requested_game_count=2,
            simulation_count=500,
        )
    )

    assert result["status"] == "observed"
    assert result["observed_game_count"] == 1
    assert result["observation_rate"] == 0.5
    assert result["state_counts"] == {
        "blocked": 1,
        "observed": 1,
    }
    assert result["blocker_counts"] == {
        "candidate_materialization_not_ready":
            1,
    }


def test_all_blocked_window_reports_blocked():
    result = (
        aggregate_hitter_profile_live_simulation_shadow_window(
            observations=[
                {
                    "game_pk": 4,
                    "status": "blocked",
                    "blockers": [
                        "lineup_discovery_not_ready",
                    ],
                    "production_authority_changed":
                        False,
                },
            ],
            target_date="2026-05-03",
            requested_game_count=1,
            simulation_count=100,
        )
    )

    assert result["status"] == "blocked"
    assert result["observed_game_count"] == 0
    assert result["comparison_count"] == 0
    assert result["maximum_absolute_delta"][
        "count"
    ] == 0


def test_accepts_audit_objects_with_diagnostics():
    class Audit:
        def to_diagnostics(self):
            return observation(
                game_pk=5,
                delta=0.025,
                scope="batter",
                metric="dfs_points",
            )

    result = (
        aggregate_hitter_profile_live_simulation_shadow_window(
            observations=[Audit()],
            target_date="2026-05-03",
            requested_game_count=1,
            simulation_count=100,
        )
    )

    assert result["absolute_delta_by_scope"][
        "batter"
    ]["maximum"] == 0.025
    assert result["absolute_delta_by_metric"][
        "dfs_points"
    ]["maximum"] == 0.025


def test_never_selects_activation_or_parameters():
    result = (
        aggregate_hitter_profile_live_simulation_shadow_window(
            observations=[],
            target_date="2026-05-03",
            requested_game_count=0,
            simulation_count=100,
        )
    )

    assert result["decision"] == {
        "promotion_thresholds_selected": False,
        "production_activation_allowed": False,
        "recommended_next_slice":
            "define_hitter_profile_simulation_shadow_acceptance_gates",
    }
    assert result["parameter_selected"] is False
    assert (
        result["production_authority_changed"]
        is False
    )
    assert result["safety_checks"][
        "database_writes_performed"
    ] is False


def accepted_gate():
    return {
        "gate_passed": True,
        "decision": {
            "feature_flag_integration_allowed":
                True,
            "production_activation_allowed":
                False,
        },
        "production_authority_changed": False,
    }


def context(game_pk):
    return {
        "game_pk": game_pk,
        "game_date": "2026-05-03",
        "season": 2026,
        "lineups": object(),
        "bullpens": object(),
        "provider_discovery": object(),
        "exact_artifact_discovery": object(),
        "fallback_catalog_discovery": object(),
        "bootstrap_ready": True,
        "pitcher_hands_by_id": {
            "10": "R",
        },
        "pitcher_profiles_by_id": {
            "10": {},
        },
        "environment_profile": {},
    }


def test_live_runner_materializes_and_pairs_contexts():
    observed = {
        "materialized": [],
        "paired": [],
    }

    def payload_builder(
        session,
        target_date,
        *,
        canonical_shadow_context_observer,
    ):
        canonical_shadow_context_observer(
            context(20)
        )
        canonical_shadow_context_observer(
            context(10)
        )
        canonical_shadow_context_observer(
            context(30)
        )
        return {
            "games": [
                {"game_pk": 20},
                {"game_pk": 10},
                {"game_pk": 30},
            ],
        }

    def materializer(session, **kwargs):
        observed["materialized"].append(
            kwargs
        )
        return {
            "status": "ready",
            "materialized": True,
            "candidate_results": {
                "1": {
                    "status": "ready",
                },
            },
            "database_writes_performed": False,
            "production_inputs_unchanged": True,
            "production_authority_changed": False,
        }

    class Paired:
        def __init__(self, game_pk):
            self.game_pk = game_pk

        def to_diagnostics(self):
            return observation(
                game_pk=self.game_pk,
                delta=0.01,
            )

    def paired_runner(**kwargs):
        observed["paired"].append(kwargs)
        return Paired(kwargs["game_pk"])

    result = (
        run_hitter_profile_live_simulation_shadow_window(
            object(),
            enabled=True,
            target_date="2026-05-03",
            acceptance_gate=accepted_gate(),
            simulation_count=250,
            game_limit=2,
            projection_payload_builder=(
                payload_builder
            ),
            candidate_materializer=materializer,
            paired_audit_runner=paired_runner,
        )
    )

    assert result["status"] == "observed"
    assert result["audited_game_count"] == 2
    assert result["observed_game_count"] == 2
    assert result["source"] == {
        "projection_payload_status": "ready",
        "projection_game_count": 3,
        "captured_context_count": 2,
    }
    assert len(observed["materialized"]) == 2
    assert len(observed["paired"]) == 2
    assert [
        call["game_pk"]
        for call in observed["paired"]
    ] == [10, 20]
    assert [
        call["as_of_date"].isoformat()
        for call in observed["materialized"]
    ] == [
        "2026-05-03",
        "2026-05-03",
    ]
    assert all(
        not isinstance(
            call["as_of_date"],
            str,
        )
        for call in observed["materialized"]
    )
    assert all(
        call["simulation_count"] == 250
        for call in observed["paired"]
    )
    assert all(
        call[
            "candidate_materialization"
        ]["materialized"]
        is True
        for call in observed["paired"]
    )


def test_live_runner_is_disabled_by_default():
    called = False

    def payload_builder(*args, **kwargs):
        nonlocal called
        called = True
        return {}

    result = (
        run_hitter_profile_live_simulation_shadow_window(
            object(),
            target_date="2026-05-03",
            acceptance_gate=accepted_gate(),
            projection_payload_builder=(
                payload_builder
            ),
        )
    )

    assert result["status"] == "disabled"
    assert called is False
    assert (
        result["decision"][
            "production_activation_allowed"
        ]
        is False
    )


def test_live_runner_rejects_unaccepted_gate():
    result = (
        run_hitter_profile_live_simulation_shadow_window(
            object(),
            enabled=True,
            target_date="2026-05-03",
            acceptance_gate={
                "gate_passed": False,
            },
        )
    )

    assert result["status"] == "blocked"
    assert result["blocker_counts"] == {
        "canary_acceptance_gate_not_passed":
            1,
    }


def test_live_runner_isolates_game_errors():
    def payload_builder(
        session,
        target_date,
        *,
        canonical_shadow_context_observer,
    ):
        canonical_shadow_context_observer(
            context(30)
        )
        return {
            "games": [
                {"game_pk": 30},
            ],
        }

    def materializer(*args, **kwargs):
        raise RuntimeError("evidence unavailable")

    result = (
        run_hitter_profile_live_simulation_shadow_window(
            object(),
            enabled=True,
            target_date="2026-05-03",
            acceptance_gate=accepted_gate(),
            simulation_count=100,
            game_limit=1,
            projection_payload_builder=(
                payload_builder
            ),
            candidate_materializer=materializer,
        )
    )

    assert result["status"] == "blocked"
    assert result["blocker_counts"] == {
        "live_window_game_error": 1,
    }
    assert result["records"][0][
        "game_pk"
    ] == 30
    assert result[
        "database_writes_performed"
    ] is False
    assert (
        result["production_authority_changed"]
        is False
    )


def test_live_runner_validates_bounds():
    import pytest

    with pytest.raises(
        ValueError,
        match="simulation_count",
    ):
        run_hitter_profile_live_simulation_shadow_window(
            object(),
            enabled=True,
            target_date="2026-05-03",
            acceptance_gate=accepted_gate(),
            simulation_count=0,
        )

    with pytest.raises(
        ValueError,
        match="game_limit",
    ):
        run_hitter_profile_live_simulation_shadow_window(
            object(),
            enabled=True,
            target_date="2026-05-03",
            acceptance_gate=accepted_gate(),
            game_limit=0,
        )

def test_preserves_paired_execution_errors():
    result = (
        aggregate_hitter_profile_live_simulation_shadow_window(
            observations=[
                {
                    "game_pk": 6,
                    "status": "blocked",
                    "blockers": [
                        "baseline_execution_not_ready",
                        "candidate_execution_not_ready",
                    ],
                    "baseline_execution": {
                        "status": "error",
                        "executed": False,
                        "simulation_count": 0,
                        "error_type": "ValueError",
                        "error_message": "baseline failed",
                    },
                    "candidate_execution": {
                        "status": "error",
                        "executed": False,
                        "simulation_count": 0,
                        "error_type": "ValueError",
                        "error_message": "candidate failed",
                    },
                    "production_authority_changed":
                        False,
                },
            ],
            target_date="2025-09-28",
            requested_game_count=1,
            simulation_count=1000,
        )
    )

    record = result["records"][0]

    assert record["baseline_execution"] == {
        "status": "error",
        "executed": False,
        "simulation_count": 0,
        "error_type": "ValueError",
        "error_message": "baseline failed",
    }
    assert record["candidate_execution"] == {
        "status": "error",
        "executed": False,
        "simulation_count": 0,
        "error_type": "ValueError",
        "error_message": "candidate failed",
    }

def test_live_runner_emits_raw_observations():
    emitted = []

    def payload_builder(
        session,
        target_date,
        *,
        canonical_shadow_context_observer,
    ):
        canonical_shadow_context_observer(
            context(10)
        )
        return {
            "games": [{"game_pk": 10}],
        }

    def materializer(*args, **kwargs):
        return {
            "status": "ready",
            "materialized": True,
            "candidate_results": {
                "1": {"status": "ready"},
            },
            "database_writes_performed": False,
            "production_inputs_unchanged": True,
            "production_authority_changed": False,
        }

    class Paired:
        def to_diagnostics(self):
            return observation(
                game_pk=10,
                delta=0.01,
            )

    run_hitter_profile_live_simulation_shadow_window(
        object(),
        enabled=True,
        target_date="2026-05-03",
        acceptance_gate=accepted_gate(),
        game_limit=1,
        projection_payload_builder=payload_builder,
        candidate_materializer=materializer,
        paired_audit_runner=(
            lambda **kwargs: Paired()
        ),
        observation_observer=emitted.append,
    )

    assert len(emitted) == 1
    assert emitted[0]["game_pk"] == 10
    assert emitted[0]["comparison"][
        "records"
    ][0]["absolute_delta"] == 0.01


def test_live_runner_validates_observer():
    import pytest

    with pytest.raises(
        TypeError,
        match="observation_observer",
    ):
        run_hitter_profile_live_simulation_shadow_window(
            object(),
            target_date="2026-05-03",
            acceptance_gate=accepted_gate(),
            observation_observer=object(),
        )
