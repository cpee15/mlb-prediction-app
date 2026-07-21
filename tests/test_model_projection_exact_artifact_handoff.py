from __future__ import annotations

from mlb_app import model_projections
from mlb_app.simulation.game import (
    CanonicalProbabilityProviderIdentity,
)
from mlb_app.simulation.shadow import (
    discover_canonical_shadow_exact_artifact,
)


PROVIDER = CanonicalProbabilityProviderIdentity(
    provider_name="model_projections_pa_outcome",
    provider_version="pa_outcome_v1",
)


class DummySession:
    pass


def confirmed_context_inputs(prefix):
    return {
        "source": "confirmed_lineup_player_splits",
        "lineup": [
            {
                "batter_id": int(
                    f"{prefix}{index}"
                ),
                "has_player_split": True,
                "has_batter_aggregate": False,
                "simulation_inputs": {
                    "k_pct": 0.22,
                    "bb_pct": 0.09,
                    "batting_avg": 0.255,
                    "on_base_pct": 0.330,
                    "slugging_pct": 0.430,
                    "iso": 0.175,
                    "hard_hit_pct": 0.40,
                    "barrel_pct": 0.08,
                },
            }
            for index in range(1, 10)
        ],
    }


def pitcher_profile():
    return {
        "bat_missing": {
            "k_rate": 0.24,
        },
        "command_control": {
            "bb_rate": 0.08,
        },
        "contact_management": {
            "barrel_rate_allowed": 0.07,
            "hard_hit_rate_allowed": 0.38,
            "xba_allowed": 0.245,
        },
    }


def test_preserved_context_can_build_exact_artifact(
    monkeypatch,
):
    away_inputs = confirmed_context_inputs(1)
    home_inputs = confirmed_context_inputs(2)

    monkeypatch.setattr(
        model_projections,
        "_team_split_inputs",
        lambda *args, **kwargs: {
            "source": "team_splits",
        },
    )

    away = {
        "pitcher_id": 100,
        "offense_inputs": (
            model_projections
            ._projection_offense_inputs(
                matchup={
                    "away_offense_inputs": (
                        away_inputs
                    ),
                },
                side="away",
                session=DummySession(),
                team_id=1,
                season=2026,
            )
        ),
    }

    home = {
        "pitcher_id": 200,
        "offense_inputs": (
            model_projections
            ._projection_offense_inputs(
                matchup={
                    "home_offense_inputs": (
                        home_inputs
                    ),
                },
                side="home",
                session=DummySession(),
                team_id=2,
                season=2026,
            )
        ),
    }

    result = (
        discover_canonical_shadow_exact_artifact(
            away_context=away,
            home_context=home,
            workspace={
                "awayPitcherProfile": (
                    pitcher_profile()
                ),
                "homePitcherProfile": (
                    pitcher_profile()
                ),
                "environmentProfile": {
                    "run_environment": {
                        "hr_boost_index": 1.0,
                        "hit_boost_index": 1.0,
                        "run_scoring_index": 1.0,
                    },
                },
            },
            provider=PROVIDER,
        )
    )

    assert result.ready is True
    assert result.away_record_count == 9
    assert result.home_record_count == 9
    assert len(result.artifact.records) == 18
