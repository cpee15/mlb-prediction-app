import pytest

from mlb_app.simulation.shadow import (
    CANONICAL_HISTORICAL_BASERUNNING_PROFILE_MATERIALIZATION_VERSION,
    CanonicalHistoricalCatcherBaserunningCounts,
    CanonicalHistoricalPitcherBaserunningCounts,
    CanonicalHistoricalRunnerBaserunningCounts,
    materialize_historical_baserunning_profiles,
)


def materialize():
    return materialize_historical_baserunning_profiles(
        required_runner_ids=("1", "2"),
        required_pitcher_ids=("10",),
        runner_counts=(
            CanonicalHistoricalRunnerBaserunningCounts(
                runner_id="1",
                opportunity_count=100,
                stolen_bases=8,
                caught_stealing=2,
            ),
            CanonicalHistoricalRunnerBaserunningCounts(
                runner_id="2",
                opportunity_count=0,
                stolen_bases=0,
                caught_stealing=0,
            ),
        ),
        pitcher_counts=(
            CanonicalHistoricalPitcherBaserunningCounts(
                pitcher_id="10",
                batters_faced=200,
                stolen_bases_allowed=6,
                caught_stealing=2,
                pickoffs=1,
            ),
        ),
        away_catcher_counts=(
            CanonicalHistoricalCatcherBaserunningCounts(
                catcher_id="20",
                team_side="away",
                stolen_bases_allowed=9,
                caught_stealing=3,
            )
        ),
        home_catcher_counts=(
            CanonicalHistoricalCatcherBaserunningCounts(
                catcher_id="21",
                team_side="home",
                stolen_bases_allowed=0,
                caught_stealing=0,
            )
        ),
    )


def test_materializes_complete_catalog():
    result = materialize()

    assert len(result.catalog.runners) == 2
    assert len(result.catalog.pitchers) == 1
    assert result.catalog.away_catcher.catcher_id == "20"
    assert result.catalog.home_catcher.catcher_id == "21"
    assert result.direct_evidence_count == 3
    assert result.fallback_evidence_count == 2
    assert result.proxy_evidence_count == 10


def test_direct_rates_and_proxies_are_explicit():
    result = materialize()
    runner = result.catalog.runners[0]
    pitcher = result.catalog.pitchers[0]
    diagnostics = result.to_diagnostics()

    assert runner.attempt_rate == pytest.approx(0.10)
    assert runner.success_rate == pytest.approx(0.80)
    assert runner.speed_score == pytest.approx(2 / 3)
    assert runner.lead_quality == runner.success_rate
    assert runner.fatigue_index == 0.0
    assert runner.injury_limit_flag is False

    assert pitcher.pickoff_attempt_rate == pytest.approx(
        0.005
    )
    assert pitcher.delivery_time_score == (
        pitcher.hold_score
    )

    assert diagnostics["direct_outcome_evidence"] is True
    assert (
        diagnostics["tracking_observations_available"]
        is False
    )
    assert (
        diagnostics["zero_sample_rows_labeled_direct"]
        is False
    )
    assert diagnostics["activation_permitted"] is False
    assert diagnostics["authoritative_source"] == "legacy"


def test_zero_sample_uses_observed_prior():
    result = materialize()
    observed, fallback = result.catalog.runners

    assert fallback.attempt_rate == observed.attempt_rate
    assert fallback.success_rate == observed.success_rate


def test_counts_must_exactly_cover_required_ids():
    with pytest.raises(
        ValueError,
        match="exactly cover required runners",
    ):
        materialize_historical_baserunning_profiles(
            required_runner_ids=("1", "2"),
            required_pitcher_ids=("10",),
            runner_counts=(
                CanonicalHistoricalRunnerBaserunningCounts(
                    runner_id="1",
                    opportunity_count=10,
                    stolen_bases=1,
                    caught_stealing=0,
                ),
            ),
            pitcher_counts=(
                CanonicalHistoricalPitcherBaserunningCounts(
                    pitcher_id="10",
                    batters_faced=10,
                    stolen_bases_allowed=0,
                    caught_stealing=0,
                    pickoffs=0,
                ),
            ),
            away_catcher_counts=(
                CanonicalHistoricalCatcherBaserunningCounts(
                    catcher_id="20",
                    team_side="away",
                    stolen_bases_allowed=0,
                    caught_stealing=0,
                )
            ),
            home_catcher_counts=(
                CanonicalHistoricalCatcherBaserunningCounts(
                    catcher_id="21",
                    team_side="home",
                    stolen_bases_allowed=0,
                    caught_stealing=0,
                )
            ),
        )


def test_runner_attempts_cannot_exceed_opportunities():
    with pytest.raises(
        ValueError,
        match="cannot exceed opportunities",
    ):
        CanonicalHistoricalRunnerBaserunningCounts(
            runner_id="1",
            opportunity_count=1,
            stolen_bases=2,
            caught_stealing=0,
        )


def test_materialization_is_deterministic():
    first = materialize()
    second = materialize()

    assert first == second
    assert first.catalog.digest == second.catalog.digest
    assert (
        first.to_diagnostics()
        == second.to_diagnostics()
    )


def test_version_is_explicit():
    assert (
        CANONICAL_HISTORICAL_BASERUNNING_PROFILE_MATERIALIZATION_VERSION
        == "canonical_historical_baserunning_profile_materialization_v1"
    )
