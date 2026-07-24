import pytest

from mlb_app.simulation.events import Base, GameState
from mlb_app.simulation.game import (
    CANONICAL_BASERUNNING_EVIDENCE_CATALOG_VERSION,
    CanonicalBaserunningEvidenceCatalog,
    CanonicalBaserunningEvidenceQuery,
    CanonicalCatcherBaserunningProfile,
    CanonicalPitcherBaserunningProfile,
    CanonicalRunnerBaserunningProfile,
    build_canonical_baserunning_state_provider,
)


def runner(runner_id="runner"):
    return CanonicalRunnerBaserunningProfile(
        runner_id=runner_id,
        speed_score=0.85,
        attempt_rate=0.30,
        success_rate=0.80,
        lead_quality=0.75,
        fatigue_index=0.10,
    )


def pitcher(pitcher_id="home-pitcher"):
    return CanonicalPitcherBaserunningProfile(
        pitcher_id=pitcher_id,
        hold_score=0.40,
        delivery_time_score=0.45,
        pickoff_attempt_rate=0.08,
        pickoff_success_rate=0.02,
    )


def catcher(side):
    return CanonicalCatcherBaserunningProfile(
        catcher_id=f"{side}-catcher",
        team_side=side,
        throwing_score=0.45,
        pop_time_score=0.40,
    )


def catalog(
    *,
    runners=None,
    pitchers=None,
):
    return CanonicalBaserunningEvidenceCatalog(
        runners=tuple(
            runners
            if runners is not None
            else (runner(),)
        ),
        pitchers=tuple(
            pitchers
            if pitchers is not None
            else (
                pitcher("home-pitcher"),
                pitcher("away-pitcher"),
            )
        ),
        away_catcher=catcher("away"),
        home_catcher=catcher("home"),
    )


def opportunity(half="top"):
    return CanonicalBaserunningEvidenceQuery(
        state=GameState(
            inning=7,
            half=half,
            outs=1,
            bases=("runner", None, None),
            away_score=3,
            home_score=2,
            batting_order_index=4,
            plate_appearance_number=25,
        ),
        batter_id="batter",
        runner_id="runner",
        origin_base=Base.FIRST,
        target_base=Base.SECOND,
    )


def test_catalog_assembles_complete_top_half_state():
    provider = build_canonical_baserunning_state_provider(
        catalog=catalog(),
        active_pitcher_provider=(
            lambda query: "home-pitcher"
        ),
    )

    state = provider(opportunity("top"))

    assert state is not None
    assert state["runner"]["runner_id"] == "runner"
    assert state["runner"]["evidence_complete"] is True
    assert (
        state["pitcher"]["pitcher_id"]
        == "home-pitcher"
    )
    assert (
        state["catcher"]["catcher_id"]
        == "home-catcher"
    )
    assert state["score_margin"] == 1
    assert state["origin_base"] == "first"
    assert state["target_base"] == "second"
    assert state["base_state"] == {
        "first": True,
        "second": False,
        "third": False,
    }


def test_bottom_half_uses_away_catcher_and_batting_margin():
    provider = build_canonical_baserunning_state_provider(
        catalog=catalog(),
        active_pitcher_provider=(
            lambda query: "away-pitcher"
        ),
    )

    state = provider(opportunity("bottom"))

    assert state is not None
    assert (
        state["catcher"]["catcher_id"]
        == "away-catcher"
    )
    assert state["score_margin"] == -1


def test_missing_runner_profile_fails_open():
    provider = build_canonical_baserunning_state_provider(
        catalog=catalog(runners=()),
        active_pitcher_provider=(
            lambda query: "home-pitcher"
        ),
    )

    assert provider(opportunity()) is None


def test_missing_pitcher_profile_fails_open():
    provider = build_canonical_baserunning_state_provider(
        catalog=catalog(pitchers=()),
        active_pitcher_provider=(
            lambda query: "home-pitcher"
        ),
    )

    assert provider(opportunity()) is None


def test_missing_active_pitcher_fails_open():
    provider = build_canonical_baserunning_state_provider(
        catalog=catalog(),
        active_pitcher_provider=lambda query: None,
    )

    assert provider(opportunity()) is None


def test_active_pitcher_provider_exception_fails_open():
    def active_pitcher_provider(query):
        raise RuntimeError("pitcher unavailable")

    provider = build_canonical_baserunning_state_provider(
        catalog=catalog(),
        active_pitcher_provider=active_pitcher_provider,
    )

    assert provider(opportunity()) is None


def test_catalog_rejects_duplicate_runner_profiles():
    with pytest.raises(
        ValueError,
        match="runner profile identifiers must be unique",
    ):
        catalog(
            runners=(
                runner(),
                runner(),
            )
        )


def test_profile_rejects_invalid_rate():
    with pytest.raises(
        ValueError,
        match="speed_score must be between 0 and 1",
    ):
        CanonicalRunnerBaserunningProfile(
            runner_id="runner",
            speed_score=1.1,
            attempt_rate=0.30,
            success_rate=0.80,
            lead_quality=0.75,
            fatigue_index=0.10,
        )


def test_catalog_preserves_version():
    assert (
        catalog().catalog_version
        == CANONICAL_BASERUNNING_EVIDENCE_CATALOG_VERSION
    )


def test_catalog_digest_is_explicit():
    value = catalog()

    assert len(value.digest) == 64
    assert value.digest == value.digest


def test_catalog_digest_is_profile_order_independent():
    value = catalog()

    reversed_catalog = CanonicalBaserunningEvidenceCatalog(
        runners=tuple(reversed(value.runners)),
        pitchers=tuple(reversed(value.pitchers)),
        away_catcher=value.away_catcher,
        home_catcher=value.home_catcher,
    )

    assert reversed_catalog.digest == value.digest


def test_runner_evidence_change_changes_catalog_digest():
    value = catalog()
    runner = value.runners[0]

    changed = CanonicalBaserunningEvidenceCatalog(
        runners=(
            CanonicalRunnerBaserunningProfile(
                runner_id=runner.runner_id,
                speed_score=runner.speed_score,
                attempt_rate=runner.attempt_rate,
                success_rate=runner.success_rate,
                lead_quality=runner.lead_quality,
                fatigue_index=0.20,
                injury_limit_flag=(
                    runner.injury_limit_flag
                ),
            ),
            *value.runners[1:],
        ),
        pitchers=value.pitchers,
        away_catcher=value.away_catcher,
        home_catcher=value.home_catcher,
    )

    assert changed.digest != value.digest


def test_pitcher_evidence_change_changes_catalog_digest():
    value = catalog()
    pitcher = value.pitchers[0]

    changed = CanonicalBaserunningEvidenceCatalog(
        runners=value.runners,
        pitchers=(
            CanonicalPitcherBaserunningProfile(
                pitcher_id=pitcher.pitcher_id,
                hold_score=pitcher.hold_score,
                delivery_time_score=(
                    pitcher.delivery_time_score
                ),
                pickoff_attempt_rate=0.25,
                pickoff_success_rate=(
                    pitcher.pickoff_success_rate
                ),
            ),
            *value.pitchers[1:],
        ),
        away_catcher=value.away_catcher,
        home_catcher=value.home_catcher,
    )

    assert changed.digest != value.digest


def test_catcher_evidence_change_changes_catalog_digest():
    value = catalog()

    changed = CanonicalBaserunningEvidenceCatalog(
        runners=value.runners,
        pitchers=value.pitchers,
        away_catcher=CanonicalCatcherBaserunningProfile(
            catcher_id=value.away_catcher.catcher_id,
            team_side="away",
            throwing_score=0.10,
            pop_time_score=(
                value.away_catcher.pop_time_score
            ),
        ),
        home_catcher=value.home_catcher,
    )

    assert changed.digest != value.digest
