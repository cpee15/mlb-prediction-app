from __future__ import annotations

from copy import deepcopy

import pytest

import mlb_app.simulation.game_simulation_builder as builder


def legacy_payload():
    return {
        "derived_outputs": {
            "game_simulation": {
                "model_version": "full_game_sim_v1",
                "simulations": 2,
                "away_expected_runs": 3.5,
                "home_expected_runs": 3.0,
                "total_expected_runs": 6.5,
                "home_win_probability": 0.52,
                "away_run_distribution": {
                    "2": 0.5,
                    "5": 0.5,
                },
                "home_run_distribution": {
                    "1": 0.5,
                    "5": 0.5,
                },
                "total_run_distribution": {
                    "4": 0.5,
                    "9": 0.5,
                },
                "metadata": {
                    "simulation_count": 2,
                },
            }
        },
        "diagnostics": {
            "sources": ["legacy"],
        },
        "meta": {
            "engine": "legacy",
        },
    }


def canonical_payload():
    def summary(mean, minimum, maximum):
        return {
            "count": 2,
            "mean": mean,
            "median": mean,
            "p10": minimum,
            "p25": minimum,
            "p75": maximum,
            "p90": maximum,
            "minimum": minimum,
            "maximum": maximum,
        }

    return {
        "schema_version": "canonical_projection_payload_v1",
        "run_id": "canonical-test",
        "model_version": "canonical-event-model-v1",
        "simulation_count": 2,
        "teams": [
            {
                "team_side": "away",
                "metrics": [
                    {
                        "name": "runs",
                        "summary": summary(
                            4.0,
                            3.0,
                            5.0,
                        ),
                    }
                ],
            },
            {
                "team_side": "home",
                "metrics": [
                    {
                        "name": "runs",
                        "summary": summary(
                            3.0,
                            2.0,
                            4.0,
                        ),
                    }
                ],
            },
        ],
        "batters": [],
        "pitchers": [],
        "diagnostics": {
            "pitcher_attribution_complete_rate": 0.5,
            "replay_validation_pass_rate": 1.0,
            "earned_run_status": "not_reconstructed",
            "warnings": [
                "earned_runs_not_fully_reconstructed"
            ],
        },
    }


def install_builder_stubs(monkeypatch, captured):
    def fake_engine(game_pk, config):
        captured["game_pk"] = game_pk
        captured["engine_config"] = deepcopy(config)
        return legacy_payload()

    monkeypatch.setattr(
        builder,
        "_load_sandbox_engine",
        lambda: fake_engine,
    )

    monkeypatch.setattr(
        builder,
        "_normalize_metadata",
        lambda payload, **kwargs: payload,
    )

    monkeypatch.setattr(
        builder,
        "_attach_pitching_plan_diagnostics",
        lambda payload, **kwargs: payload,
    )
    monkeypatch.setattr(
        builder,
        "_attach_starter_hook_diagnostics",
        lambda payload, **kwargs: payload,
    )
    monkeypatch.setattr(
        builder,
        "_attach_bullpen_sequence_diagnostics",
        lambda payload, **kwargs: payload,
    )
    monkeypatch.setattr(
        builder,
        "_attach_stolen_base_pickoff_diagnostics",
        lambda payload, **kwargs: payload,
    )
    monkeypatch.setattr(
        builder,
        "_attach_position_player_substitution_diagnostics",
        lambda payload, **kwargs: payload,
    )


def test_absent_flag_preserves_historical_payload(monkeypatch):
    monkeypatch.delenv(
        builder.CANONICAL_SHADOW_ENABLED_ENV,
        raising=False,
    )

    payload = legacy_payload()

    attached = builder._attach_canonical_shadow_diagnostics(
        payload,
        config={},
    )

    assert attached is payload
    assert "canonical_shadow" not in attached["diagnostics"]


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (True, True),
        (False, False),
        ("true", True),
        ("1", True),
        ("yes", True),
        ("on", True),
        ("false", False),
        ("0", False),
        ("no", False),
        ("off", False),
    ],
)
def test_boolean_flag_parsing(value, expected):
    assert builder._parse_boolean_flag(value) is expected


def test_explicit_config_precedes_environment(monkeypatch):
    monkeypatch.setenv(
        builder.CANONICAL_SHADOW_ENABLED_ENV,
        "true",
    )

    assert (
        builder._canonical_shadow_enabled(
            {"canonical_shadow_enabled": False}
        )
        is False
    )


def test_environment_enables_shadow_when_config_absent(
    monkeypatch,
):
    monkeypatch.setenv(
        builder.CANONICAL_SHADOW_ENABLED_ENV,
        "true",
    )

    assert builder._canonical_shadow_requested({}) is True
    assert builder._canonical_shadow_enabled({}) is True


def test_explicit_false_attaches_disabled_diagnostics(
    monkeypatch,
):
    monkeypatch.delenv(
        builder.CANONICAL_SHADOW_ENABLED_ENV,
        raising=False,
    )

    attached = builder._attach_canonical_shadow_diagnostics(
        legacy_payload(),
        config={
            "canonical_shadow_enabled": False,
        },
    )

    shadow = attached["diagnostics"]["canonical_shadow"]

    assert shadow["status"] == "disabled"
    assert shadow["enabled"] is False
    assert shadow["authoritative_source"] == "legacy"


def test_enabled_without_payload_is_unavailable(monkeypatch):
    monkeypatch.delenv(
        builder.CANONICAL_SHADOW_ENABLED_ENV,
        raising=False,
    )

    attached = builder._attach_canonical_shadow_diagnostics(
        legacy_payload(),
        config={
            "canonical_shadow_enabled": True,
        },
    )

    shadow = attached["diagnostics"]["canonical_shadow"]

    assert shadow["status"] == "unavailable"
    assert shadow["canonical_available"] is False
    assert shadow["authoritative_source"] == "legacy"


def test_enabled_payload_attaches_partial_comparison(
    monkeypatch,
):
    monkeypatch.delenv(
        builder.CANONICAL_SHADOW_ENABLED_ENV,
        raising=False,
    )

    original = legacy_payload()

    attached = builder._attach_canonical_shadow_diagnostics(
        original,
        config={
            "canonical_shadow_enabled": True,
            "canonical_shadow_payload": (
                canonical_payload()
            ),
        },
    )

    shadow = attached["diagnostics"]["canonical_shadow"]

    assert shadow["status"] == "partial"
    assert shadow["authoritative_source"] == "legacy"
    assert shadow["canonical_available"] is True
    assert original["diagnostics"] == {
        "sources": ["legacy"]
    }
    assert attached["derived_outputs"] == (
        original["derived_outputs"]
    )


def test_shadow_keys_do_not_reach_engine(monkeypatch):
    captured = {}
    install_builder_stubs(monkeypatch, captured)

    result = builder.build_game_simulation(
        123,
        config={
            "simulation_count": 25,
            "canonical_shadow_enabled": True,
            "canonical_shadow_payload": (
                canonical_payload()
            ),
        },
    )

    assert captured["game_pk"] == 123
    assert captured["engine_config"] == {
        "simulation_count": 25,
    }

    shadow = result["diagnostics"]["canonical_shadow"]

    assert shadow["status"] == "partial"
    assert shadow["authoritative_source"] == "legacy"


def test_builder_default_does_not_add_shadow_block(
    monkeypatch,
):
    captured = {}
    install_builder_stubs(monkeypatch, captured)

    monkeypatch.delenv(
        builder.CANONICAL_SHADOW_ENABLED_ENV,
        raising=False,
    )

    result = builder.build_game_simulation(
        123,
        config={
            "simulation_count": 25,
        },
    )

    assert "canonical_shadow" not in result["diagnostics"]


def test_outer_shadow_boundary_is_fail_open(
    monkeypatch,
):
    payload = legacy_payload()

    def fail_import(*args, **kwargs):
        raise RuntimeError("unexpected shadow failure")

    monkeypatch.setattr(
        builder,
        "_canonical_shadow_payload",
        fail_import,
    )

    attached = builder._attach_canonical_shadow_diagnostics(
        payload,
        config={
            "canonical_shadow_enabled": True,
        },
    )

    shadow = attached["diagnostics"]["canonical_shadow"]

    assert shadow["status"] == "error"
    assert shadow["authoritative_source"] == "legacy"
    assert shadow["error_type"] == "RuntimeError"
    assert attached["derived_outputs"] == (
        payload["derived_outputs"]
    )
