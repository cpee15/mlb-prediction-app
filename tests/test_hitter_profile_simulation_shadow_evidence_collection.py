from mlb_app.simulation.shadow.hitter_profile_simulation_shadow_evidence_collection import (
    collect_hitter_profile_simulation_shadow_evidence,
)


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


def observation(game_pk, delta):
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
                    "scope":
                        "game_probability",
                    "metric":
                        "home_win_probability",
                    "delta": delta,
                    "absolute_delta":
                        abs(delta),
                },
            ],
        },
        "candidate_materialization": {
            "status": "ready",
            "candidate_count": 1,
        },
        "safety_checks": {
            "production_authority_unchanged":
                True,
            "production_inputs_unchanged":
                True,
            "simulation_counts_match": True,
        },
        "database_writes_performed": False,
        "production_authority_changed": False,
    }


def test_collects_dates_and_deduplicates_games():
    calls = []

    def window_runner(
        session,
        *,
        target_date,
        observation_observer,
        **kwargs,
    ):
        calls.append((target_date, kwargs))
        observation_observer(
            observation(
                10,
                0.01
                if target_date == "2026-05-01"
                else 0.02,
            )
        )
        observation_observer(
            observation(
                20
                if target_date == "2026-05-01"
                else 30,
                0.03,
            )
        )
        return {
            "status": "observed",
            "audited_game_count": 2,
            "observed_game_count": 2,
            "blocker_counts": {},
            "source": {},
        }

    evaluated = {}

    def evaluator(payload):
        evaluated.update(payload)
        return {
            "status": "blocked",
            "gate_passed": False,
            "decision": {
                "extended_shadow_evaluation_allowed":
                    False,
                "production_activation_allowed":
                    False,
                "recommended_next_slice":
                    "collect_additional_hitter_profile_"
                    "simulation_shadow_evidence",
            },
        }

    result = (
        collect_hitter_profile_simulation_shadow_evidence(
            object(),
            enabled=True,
            target_dates=[
                "2026-05-02",
                "2026-05-01",
                "2026-05-02",
            ],
            acceptance_gates_by_date={
                "2026-05-01": accepted_gate(),
                "2026-05-02": accepted_gate(),
            },
            simulation_count=1000,
            game_limit=2,
            window_runner=window_runner,
            acceptance_evaluator=evaluator,
        )
    )

    assert [
        call[0] for call in calls
    ] == [
        "2026-05-01",
        "2026-05-02",
    ]
    assert result["target_dates"] == [
        "2026-05-01",
        "2026-05-02",
    ]
    assert result[
        "captured_observation_count"
    ] == 4
    assert result[
        "deduplicated_game_count"
    ] == 3
    assert result["duplicate_game_count"] == 1
    assert result["audited_game_count"] == 3
    assert result["comparison_count"] == 3
    assert evaluated["audited_game_count"] == 3
    assert (
        evaluated[
            "database_writes_performed"
        ]
        is False
    )
    assert (
        evaluated[
            "production_authority_changed"
        ]
        is False
    )
    assert result["absolute_delta_by_scope"][
        "game_probability"
    ]["maximum"] == 0.03


def test_uses_cutoff_safe_gate_for_each_date():
    observed_gates = {}

    def window_runner(
        session,
        *,
        target_date,
        acceptance_gate,
        observation_observer,
        **kwargs,
    ):
        observed_gates[target_date] = (
            acceptance_gate["identity"]
        )
        observation_observer(
            observation(
                int(target_date[-2:]),
                0.01,
            )
        )
        return {
            "status": "observed",
            "audited_game_count": 1,
            "observed_game_count": 1,
        }

    collect_hitter_profile_simulation_shadow_evidence(
        object(),
        enabled=True,
        target_dates=[
            "2026-05-01",
            "2026-05-02",
        ],
        acceptance_gates_by_date={
            "2026-05-01": {
                **accepted_gate(),
                "identity": "gate-one",
            },
            "2026-05-02": {
                **accepted_gate(),
                "identity": "gate-two",
            },
        },
        window_runner=window_runner,
        acceptance_evaluator=lambda payload: {
            "decision": {},
        },
    )

    assert observed_gates == {
        "2026-05-01": "gate-one",
        "2026-05-02": "gate-two",
    }


def test_isolates_failed_date():
    def window_runner(
        session,
        *,
        target_date,
        observation_observer,
        **kwargs,
    ):
        if target_date == "2026-05-01":
            raise RuntimeError(
                "projection unavailable"
            )

        observation_observer(
            observation(2, 0.01)
        )
        return {
            "status": "observed",
            "audited_game_count": 1,
            "observed_game_count": 1,
        }

    result = (
        collect_hitter_profile_simulation_shadow_evidence(
            object(),
            enabled=True,
            target_dates=[
                "2026-05-01",
                "2026-05-02",
            ],
            acceptance_gates_by_date={
                "2026-05-01": accepted_gate(),
                "2026-05-02": accepted_gate(),
            },
            window_runner=window_runner,
            acceptance_evaluator=lambda payload: {
                "decision": {},
            },
        )
    )

    assert result["audited_game_count"] == 2
    assert result["observed_game_count"] == 1
    assert result["blocker_counts"] == {
        "evidence_window_error": 1,
    }
    assert result["windows"][0][
        "status"
    ] == "blocked"


def test_disabled_by_default():
    called = False

    def runner(*args, **kwargs):
        nonlocal called
        called = True

    result = (
        collect_hitter_profile_simulation_shadow_evidence(
            object(),
            target_dates=[
                "2026-05-01",
            ],
            acceptance_gates_by_date={},
            window_runner=runner,
        )
    )

    assert result["status"] == "disabled"
    assert called is False
    assert result["database_writes_performed"] is False
    assert result["production_authority_changed"] is False
    assert result["decision"][
        "production_activation_allowed"
    ] is False


def test_validates_bounds_and_dates():
    import pytest

    with pytest.raises(
        ValueError,
        match="target_dates",
    ):
        collect_hitter_profile_simulation_shadow_evidence(
            object(),
            enabled=True,
            target_dates=[],
            acceptance_gates_by_date={},
        )

    with pytest.raises(
        ValueError,
        match="simulation_count",
    ):
        collect_hitter_profile_simulation_shadow_evidence(
            object(),
            enabled=True,
            target_dates=[
                "2026-05-01",
            ],
            acceptance_gates_by_date={},
            simulation_count=0,
        )
