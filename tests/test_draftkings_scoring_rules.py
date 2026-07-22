from mlb_app.simulation.box_score import (
    DRAFTKINGS_CLASSIC_BATTER_RULES,
    DRAFTKINGS_CLASSIC_PITCHER_RULES,
    DRAFTKINGS_CLASSIC_UNSUPPORTED_CATEGORIES,
    BatterBoxScore,
    PitcherBoxScore,
    score_batter,
    score_pitcher,
)


def test_draftkings_classic_batter_supported_scoring():
    line = BatterBoxScore(
        player_id="batter",
        team_side="away",
        singles=1,
        doubles=1,
        triples=1,
        home_runs=1,
        walks=1,
        hit_by_pitch=1,
        runs=1,
        rbi=1,
    )

    assert score_batter(
        line,
        DRAFTKINGS_CLASSIC_BATTER_RULES,
    ) == 34.0


def test_draftkings_classic_pitcher_supported_scoring():
    line = PitcherBoxScore(
        player_id="pitcher",
        team_side="home",
        outs_recorded=18,
        strikeouts=7,
        hits_allowed=4,
        walks=2,
        hit_batters=1,
        runs_allowed=3,
        earned_runs=2,
        earned_run_status="reconstructed",
    )

    assert score_pitcher(
        line,
        DRAFTKINGS_CLASSIC_PITCHER_RULES,
    ) == 19.3


def test_unsupported_categories_are_explicit():
    assert DRAFTKINGS_CLASSIC_UNSUPPORTED_CATEGORIES == (
        "batter_stolen_base",
        "pitcher_win",
        "pitcher_complete_game",
        "pitcher_complete_game_shutout",
        "pitcher_no_hitter",
    )
