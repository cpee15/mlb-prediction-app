from datetime import date, timedelta
import hashlib

import pytest

from mlb_app.simulation.game import (
    CanonicalBaserunningEvidenceCatalog,
    CanonicalCatcherBaserunningProfile,
    CanonicalPitcherBaserunningProfile,
    CanonicalRunnerBaserunningProfile,
)
from mlb_app.simulation.shadow.historical_baserunning_replay_evidence_source import (
    CANONICAL_HISTORICAL_BASERUNNING_REPLAY_EVIDENCE_VERSION,
    HISTORICAL_BASERUNNING_CALIBRATION_PROXY_POLICY,
    HISTORICAL_BASERUNNING_EVIDENCE_QUALITY,
    source_historical_baserunning_replay_evidence,
)
from mlb_app.simulation.shadow.historical_lineup_bullpen_source import (
    CanonicalHistoricalLineupBullpenGameSnapshot,
    CanonicalHistoricalLineupBullpenWindow,
)


DIGEST = hashlib.sha256(b"fixture").hexdigest()


def catalog():
    return CanonicalBaserunningEvidenceCatalog(
        runners=(
            CanonicalRunnerBaserunningProfile(
                runner_id="1",
                speed_score=0.5,
                attempt_rate=0.1,
                success_rate=0.75,
                lead_quality=0.5,
                fatigue_index=0.0,
            ),
        ),
        pitchers=(
            CanonicalPitcherBaserunningProfile(
                pitcher_id="2",
                hold_score=0.5,
                delivery_time_score=0.5,
                pickoff_attempt_rate=0.05,
                pickoff_success_rate=0.1,
            ),
        ),
        away_catcher=CanonicalCatcherBaserunningProfile(
            catcher_id="3",
            team_side="away",
            throwing_score=0.5,
            pop_time_score=0.5,
        ),
        home_catcher=CanonicalCatcherBaserunningProfile(
            catcher_id="4",
            team_side="home",
            throwing_score=0.5,
            pop_time_score=0.5,
        ),
    )


def lineup_window():
    games = []

    for game_pk, game_date in (
        (11, "2026-04-20"),
        (12, "2026-04-21"),
    ):
        games.append(
            CanonicalHistoricalLineupBullpenGameSnapshot(
                game_pk=game_pk,
                game_date=game_date,
                away_lineup_ids=tuple(
                    str(value)
                    for value in range(1, 10)
                ),
                home_lineup_ids=tuple(
                    str(value)
                    for value in range(11, 20)
                ),
                away_bullpen_ids=("21",),
                home_bullpen_ids=("22",),
                lineup_digest=DIGEST,
                bullpen_digest=DIGEST,
            )
        )

    return CanonicalHistoricalLineupBullpenWindow(
        observed_window_digest=DIGEST,
        games=tuple(games),
        digest=DIGEST,
    )


def source():
    window = lineup_window()

    return source_historical_baserunning_replay_evidence(
        lineup_bullpen=window,
        catalogs={
            game.game_pk: catalog()
            for game in window.games
        },
        statistics_through_dates={
            game.game_pk: (
                date.fromisoformat(game.game_date)
                - timedelta(days=1)
            ).isoformat()
            for game in window.games
        },
        evidence_counts={
            game.game_pk: (10, 5, 1)
            for game in window.games
        },
    )


def test_window_is_ready_and_guarded():
    result = source()
    diagnostics = result.to_diagnostics()

    assert result.ready is True
    assert result.game_count == 2
    assert diagnostics["ready_game_count"] == 2
    assert diagnostics["direct_evidence_count"] == 20
    assert diagnostics["proxy_evidence_count"] == 10
    assert diagnostics["fallback_evidence_count"] == 2
    assert diagnostics["evidence_quality"] == (
        HISTORICAL_BASERUNNING_EVIDENCE_QUALITY
    )
    assert diagnostics["tracking_proxy_policy"] == (
        HISTORICAL_BASERUNNING_CALIBRATION_PROXY_POLICY
    )
    assert diagnostics["target_game_outcomes_used"] is False
    assert diagnostics["future_data_permitted"] is False
    assert diagnostics["historical_replay_executed"] is False
    assert diagnostics["production_activation"] is False
    assert diagnostics["production_authority_changed"] is False
    assert diagnostics["authoritative_source"] == "legacy"


def test_inputs_must_exactly_cover_games():
    window = lineup_window()

    with pytest.raises(
        ValueError,
        match="exactly cover",
    ):
        source_historical_baserunning_replay_evidence(
            lineup_bullpen=window,
            catalogs={11: catalog()},
            statistics_through_dates={
                11: "2026-04-19",
                12: "2026-04-20",
            },
            evidence_counts={
                11: (1, 1, 0),
                12: (1, 1, 0),
            },
        )


def test_cutoff_must_be_previous_calendar_date():
    window = lineup_window()

    with pytest.raises(
        ValueError,
        match="strict previous calendar date",
    ):
        source_historical_baserunning_replay_evidence(
            lineup_bullpen=window,
            catalogs={
                game.game_pk: catalog()
                for game in window.games
            },
            statistics_through_dates={
                11: "2026-04-20",
                12: "2026-04-20",
            },
            evidence_counts={
                11: (1, 1, 0),
                12: (1, 1, 0),
            },
        )


def test_source_is_deterministic():
    first = source()
    second = source()

    assert first == second
    assert first.digest == second.digest
    assert (
        first.to_diagnostics()
        == second.to_diagnostics()
    )


def test_version_is_explicit():
    assert (
        CANONICAL_HISTORICAL_BASERUNNING_REPLAY_EVIDENCE_VERSION
        == "canonical_historical_baserunning_replay_evidence_v1"
    )
