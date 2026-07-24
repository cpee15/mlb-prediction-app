import json

import pytest

from mlb_app.simulation.box_score import (
    DRAFTKINGS_CLASSIC_BATTER_RULES,
    DRAFTKINGS_CLASSIC_PITCHER_RULES,
    BatterBoxScore,
    BatterDfsScoringRules,
    PitcherBoxScore,
    PitcherDfsScoringRules,
    ReducedBoxScore,
    TeamBoxScore,
)
from mlb_app.simulation.projections import (
    aggregate_projection_payload,
    projection_payload_to_dict,
    summarize_values,
    validate_projection_payload,
)


def run(
    *,
    away_runs,
    home_runs,
    batter_runs,
    pitcher_runs,
    complete=True,
):
    return ReducedBoxScore(
        away=TeamBoxScore(
            team_side="away",
            runs=away_runs,
            hits=1,
        ),
        home=TeamBoxScore(
            team_side="home",
            runs=home_runs,
        ),
        batters=(
            BatterBoxScore(
                player_id="batter",
                team_side="away",
                plate_appearances=1,
                at_bats=1,
                home_runs=1,
                runs=batter_runs,
                rbi=1,
            ),
        ),
        pitchers=(
            PitcherBoxScore(
                player_id="pitcher",
                team_side="home",
                batters_faced=1,
                hits_allowed=1,
                home_runs_allowed=1,
                runs_allowed=pitcher_runs,
            ),
        ),
        pitcher_attribution_complete=complete,
    )


def metric(projection, name):
    return next(
        item
        for item in projection.metrics
        if item.name == name
    )


def test_statistical_summary_is_deterministic():
    summary = summarize_values((4, 1, 3, 2))

    assert summary.count == 4
    assert summary.mean == 2.5
    assert summary.median == 2.5
    assert summary.minimum == 1.0
    assert summary.maximum == 4.0


def test_empty_summary_is_rejected():
    with pytest.raises(
        ValueError,
        match="empty value sequence",
    ):
        summarize_values(())


def test_team_run_distribution_is_aggregated():
    payload = aggregate_projection_payload(
        box_scores=(
            run(
                away_runs=2,
                home_runs=1,
                batter_runs=1,
                pitcher_runs=2,
            ),
            run(
                away_runs=4,
                home_runs=3,
                batter_runs=2,
                pitcher_runs=4,
            ),
        ),
        model_version="canonical_event_model_v1",
    )

    away_runs = metric(
        payload.teams[0],
        "runs",
    ).summary

    assert payload.simulation_count == 2
    assert away_runs.mean == 3.0
    assert away_runs.minimum == 2.0
    assert away_runs.maximum == 4.0


def test_batter_metrics_zero_fill_missing_run():
    first = run(
        away_runs=1,
        home_runs=0,
        batter_runs=1,
        pitcher_runs=1,
    )
    second = ReducedBoxScore(
        away=TeamBoxScore(team_side="away"),
        home=TeamBoxScore(team_side="home"),
    )

    payload = aggregate_projection_payload(
        box_scores=(first, second),
        model_version="canonical_event_model_v1",
    )

    runs = metric(
        payload.batters[0],
        "runs",
    ).summary

    assert runs.count == 2
    assert runs.mean == 0.5


def test_dfs_distributions_are_configurable():
    payload = aggregate_projection_payload(
        box_scores=(
            run(
                away_runs=1,
                home_runs=0,
                batter_runs=1,
                pitcher_runs=1,
            ),
        ),
        model_version="canonical_event_model_v1",
        batter_dfs_rules=BatterDfsScoringRules(
            home_run=10.0,
            run=2.0,
            rbi=2.0,
        ),
        pitcher_dfs_rules=PitcherDfsScoringRules(
            hit_allowed=-1.0,
            run_allowed=-2.0,
        ),
    )

    batter_dfs = metric(
        payload.batters[0],
        "dfs_points",
    ).summary
    pitcher_dfs = metric(
        payload.pitchers[0],
        "dfs_points",
    ).summary

    assert batter_dfs.mean == 14.0
    assert pitcher_dfs.mean == -3.0


def test_earned_run_weight_requires_reconstruction():
    payload = aggregate_projection_payload(
        box_scores=(
            run(
                away_runs=1,
                home_runs=0,
                batter_runs=1,
                pitcher_runs=1,
            ),
        ),
        model_version="canonical_event_model_v1",
        pitcher_dfs_rules=PitcherDfsScoringRules(
            earned_run=-2.0,
        ),
    )

    pitcher_metric_names = {
        item.name
        for item in payload.pitchers[0].metrics
    }

    assert "dfs_points" not in pitcher_metric_names
    assert (
        "pitcher_dfs_earned_runs_unavailable"
        in payload.diagnostics.warnings
    )


def test_reconstructed_earned_runs_are_scored():
    box_score = run(
        away_runs=1,
        home_runs=0,
        batter_runs=1,
        pitcher_runs=1,
    )
    pitcher = box_score.pitchers[0]

    reconstructed = ReducedBoxScore(
        away=box_score.away,
        home=box_score.home,
        batters=box_score.batters,
        pitchers=(
            PitcherBoxScore(
                player_id=pitcher.player_id,
                team_side=pitcher.team_side,
                batters_faced=pitcher.batters_faced,
                outs_recorded=pitcher.outs_recorded,
                hits_allowed=pitcher.hits_allowed,
                home_runs_allowed=(
                    pitcher.home_runs_allowed
                ),
                walks=pitcher.walks,
                hit_batters=pitcher.hit_batters,
                strikeouts=pitcher.strikeouts,
                runs_allowed=pitcher.runs_allowed,
                earned_runs=1,
                earned_run_status="reconstructed",
            ),
        ),
        pitcher_attribution_complete=True,
    )

    payload = aggregate_projection_payload(
        box_scores=(reconstructed,),
        model_version="canonical_event_model_v1",
        pitcher_dfs_rules=PitcherDfsScoringRules(
            earned_run=-2.0,
        ),
    )

    dfs = metric(
        payload.pitchers[0],
        "dfs_points",
    ).summary

    assert dfs.mean == -2.0
    assert (
        "pitcher_dfs_earned_runs_unavailable"
        not in payload.diagnostics.warnings
    )


def test_diagnostics_report_incomplete_inputs():
    payload = aggregate_projection_payload(
        box_scores=(
            run(
                away_runs=1,
                home_runs=0,
                batter_runs=1,
                pitcher_runs=1,
                complete=True,
            ),
            run(
                away_runs=2,
                home_runs=0,
                batter_runs=1,
                pitcher_runs=2,
                complete=False,
            ),
        ),
        model_version="canonical_event_model_v1",
        replay_validation_passes=(True, False),
    )

    assert (
        payload.diagnostics
        .pitcher_attribution_complete_rate
        == 0.5
    )
    assert (
        payload.diagnostics
        .replay_validation_pass_rate
        == 0.5
    )
    assert (
        "pitcher_attribution_incomplete"
        in payload.diagnostics.warnings
    )
    assert (
        "replay_validation_failures_present"
        in payload.diagnostics.warnings
    )


def test_run_identifier_is_deterministic():
    runs = (
        run(
            away_runs=1,
            home_runs=0,
            batter_runs=1,
            pitcher_runs=1,
        ),
    )

    first = aggregate_projection_payload(
        box_scores=runs,
        model_version="canonical_event_model_v1",
    )
    second = aggregate_projection_payload(
        box_scores=runs,
        model_version="canonical_event_model_v1",
    )

    assert first.run_id == second.run_id


def test_serialization_is_plain_json_compatible():
    payload = aggregate_projection_payload(
        box_scores=(
            run(
                away_runs=1,
                home_runs=0,
                batter_runs=1,
                pitcher_runs=1,
            ),
        ),
        model_version="canonical_event_model_v1",
    )

    serialized = projection_payload_to_dict(payload)
    encoded = json.dumps(serialized, sort_keys=True)

    assert isinstance(serialized["teams"], list)
    assert "canonical_projection_payload_v1" in encoded


def test_payload_validation_passes():
    payload = aggregate_projection_payload(
        box_scores=(
            run(
                away_runs=1,
                home_runs=0,
                batter_runs=1,
                pitcher_runs=1,
            ),
            run(
                away_runs=2,
                home_runs=1,
                batter_runs=1,
                pitcher_runs=2,
            ),
        ),
        model_version="canonical_event_model_v1",
    )

    validation = validate_projection_payload(payload)

    assert validation.passed is True
    assert validation.warnings == ()


def test_validation_count_must_match_simulations():
    with pytest.raises(
        ValueError,
        match="validation count must match",
    ):
        aggregate_projection_payload(
            box_scores=(
                run(
                    away_runs=1,
                    home_runs=0,
                    batter_runs=1,
                    pitcher_runs=1,
                ),
            ),
            model_version="canonical_event_model_v1",
            replay_validation_passes=(True, False),
        )



def test_draftkings_projection_means_reconcile_to_trial_lines():
    first = ReducedBoxScore(
        away=TeamBoxScore(
            team_side="away",
            runs=1,
            hits=1,
        ),
        home=TeamBoxScore(team_side="home"),
        batters=(
            BatterBoxScore(
                player_id="batter",
                team_side="away",
                plate_appearances=1,
                at_bats=1,
                singles=1,
                walks=1,
                runs=1,
                rbi=1,
            ),
        ),
        pitchers=(
            PitcherBoxScore(
                player_id="pitcher",
                team_side="home",
                batters_faced=24,
                outs_recorded=18,
                hits_allowed=4,
                walks=2,
                hit_batters=1,
                strikeouts=7,
                runs_allowed=2,
                earned_runs=2,
                earned_run_status="reconstructed",
            ),
        ),
        pitcher_attribution_complete=True,
    )

    second = ReducedBoxScore(
        away=TeamBoxScore(
            team_side="away",
            runs=2,
            hits=2,
        ),
        home=TeamBoxScore(team_side="home"),
        batters=(
            BatterBoxScore(
                player_id="batter",
                team_side="away",
                plate_appearances=1,
                at_bats=1,
                doubles=1,
                home_runs=1,
                runs=1,
                rbi=2,
            ),
        ),
        pitchers=(
            PitcherBoxScore(
                player_id="pitcher",
                team_side="home",
                batters_faced=24,
                outs_recorded=15,
                hits_allowed=6,
                walks=3,
                strikeouts=5,
                runs_allowed=4,
                earned_runs=4,
                earned_run_status="reconstructed",
            ),
        ),
        pitcher_attribution_complete=True,
    )

    payload = aggregate_projection_payload(
        box_scores=(first, second),
        model_version="canonical_event_model_v1",
        batter_dfs_rules=(
            DRAFTKINGS_CLASSIC_BATTER_RULES
        ),
        pitcher_dfs_rules=(
            DRAFTKINGS_CLASSIC_PITCHER_RULES
        ),
    )

    batter_dfs = metric(
        payload.batters[0],
        "dfs_points",
    ).summary
    pitcher_dfs = metric(
        payload.pitchers[0],
        "dfs_points",
    ).summary

    assert batter_dfs.count == 2
    assert batter_dfs.minimum == 9.0
    assert batter_dfs.maximum == 21.0
    assert batter_dfs.mean == 15.0

    assert pitcher_dfs.count == 2
    assert pitcher_dfs.minimum == 7.85
    assert pitcher_dfs.maximum == 19.3
    assert pitcher_dfs.mean == 13.575

    assert (
        payload.diagnostics.earned_run_status
        == "reconstructed"
    )
    assert payload.diagnostics.warnings == ()
