import pytest

from mlb_app.simulation.shadow import (
    CANONICAL_HISTORICAL_LINEUP_BULLPEN_COVERAGE_REPORT_VERSION,
    CanonicalHistoricalLineupBullpenGameSnapshot,
    CanonicalHistoricalLineupBullpenWindow,
    report_historical_lineup_bullpen_coverage,
)


def game(
    *,
    game_pk,
    away_lineup=tuple(str(value) for value in range(1, 10)),
    home_lineup=tuple(str(value) for value in range(11, 20)),
    away_bullpen=("21", "22"),
    home_bullpen=("31", "32"),
):
    lineups_ready = (
        len(away_lineup) == 9
        and len(home_lineup) == 9
    )
    bullpens_ready = bool(
        away_bullpen
        and home_bullpen
    )

    return CanonicalHistoricalLineupBullpenGameSnapshot(
        game_pk=game_pk,
        game_date=(
            "2026-04-20"
            if game_pk == 1
            else "2026-04-21"
        ),
        away_lineup_ids=away_lineup,
        home_lineup_ids=home_lineup,
        away_bullpen_ids=away_bullpen,
        home_bullpen_ids=home_bullpen,
        lineup_digest=(
            "a" * 64
            if lineups_ready
            else None
        ),
        bullpen_digest=(
            "b" * 64
            if bullpens_ready
            else None
        ),
    )


def window(*games):
    return CanonicalHistoricalLineupBullpenWindow(
        observed_window_digest="c" * 64,
        games=games,
        digest="d" * 64,
    )


def test_complete_coverage_is_reported():
    result = report_historical_lineup_bullpen_coverage(
        window(
            game(game_pk=1),
            game(game_pk=2),
        )
    )

    assert result.complete is True
    assert result.game_count == 2
    assert result.lineup_ready_game_count == 2
    assert result.bullpen_ready_game_count == 2
    assert result.ready_game_count == 2
    assert result.partial_game_count == 0
    assert result.unavailable_game_count == 0
    assert result.lineup_coverage_rate == 1.0
    assert result.bullpen_coverage_rate == 1.0


def test_partial_coverage_counts_each_blocker():
    result = report_historical_lineup_bullpen_coverage(
        window(
            game(game_pk=1),
            game(
                game_pk=2,
                away_bullpen=(),
            ),
        )
    )

    assert result.complete is False
    assert result.lineup_ready_game_count == 2
    assert result.bullpen_ready_game_count == 1
    assert result.ready_game_count == 1
    assert result.partial_game_count == 1
    assert result.missing_away_bullpen_count == 1
    assert result.missing_home_bullpen_count == 0
    assert result.complete_game_coverage_rate == 0.5


def test_unavailable_game_is_reported():
    result = report_historical_lineup_bullpen_coverage(
        window(
            game(
                game_pk=1,
                away_lineup=(),
                home_lineup=(),
                away_bullpen=(),
                home_bullpen=(),
            ),
        )
    )

    assert result.ready_game_count == 0
    assert result.partial_game_count == 0
    assert result.unavailable_game_count == 1
    assert result.missing_away_lineup_count == 1
    assert result.missing_home_lineup_count == 1
    assert result.missing_away_bullpen_count == 1
    assert result.missing_home_bullpen_count == 1


def test_wrong_input_contract_is_rejected():
    with pytest.raises(
        TypeError,
        match=(
            "window must be "
            "CanonicalHistoricalLineupBullpenWindow"
        ),
    ):
        report_historical_lineup_bullpen_coverage(
            object()
        )


def test_report_is_deterministic():
    source = window(
        game(game_pk=1),
        game(game_pk=2),
    )

    first = report_historical_lineup_bullpen_coverage(
        source
    )
    second = report_historical_lineup_bullpen_coverage(
        source
    )

    assert first == second
    assert first.report_digest == second.report_digest


def test_diagnostics_preserve_shadow_authority():
    diagnostics = (
        report_historical_lineup_bullpen_coverage(
            window(game(game_pk=1))
        ).to_diagnostics()
    )

    assert diagnostics["complete"] is True
    assert diagnostics["player_identifiers_exposed"] is False
    assert diagnostics["current_active_roster_used"] is False
    assert (
        diagnostics[
            "used_pitchers_substituted_for_bullpen"
        ]
        is False
    )
    assert diagnostics["historical_replay_executed"] is False
    assert (
        diagnostics["calibration_execution_permitted"]
        is False
    )
    assert diagnostics["production_activation"] is False
    assert diagnostics["authoritative_source"] == "legacy"


def test_report_version_is_explicit():
    assert (
        CANONICAL_HISTORICAL_LINEUP_BULLPEN_COVERAGE_REPORT_VERSION
        == (
            "canonical_historical_lineup_bullpen_"
            "coverage_report_v1"
        )
    )
