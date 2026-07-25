from dataclasses import replace

import pytest

from mlb_app.simulation.events import (
    GameState,
    OutRecord,
    PlayEvent,
)
from mlb_app.simulation.game import (
    CANONICAL_BASERUNNING_COMPOSITION_VERSION,
    CanonicalBaserunningEvidenceCatalog,
    CanonicalCatalogBaserunningResolverFactory,
    CanonicalCatcherBaserunningProfile,
    CanonicalPitcherBaserunningProfile,
    CanonicalRunnerBaserunningProfile,
    build_canonical_catalog_baserunning_resolver_factory,
    build_canonical_trial_factory_input,
    build_canonical_trial_resolver_context,
)


def catalog():
    return CanonicalBaserunningEvidenceCatalog(
        runners=(
            CanonicalRunnerBaserunningProfile(
                runner_id="runner",
                speed_score=0.90,
                attempt_rate=0.80,
                success_rate=0.85,
                lead_quality=0.80,
                fatigue_index=0.05,
            ),
        ),
        pitchers=(
            CanonicalPitcherBaserunningProfile(
                pitcher_id="active-pitcher",
                hold_score=0.35,
                delivery_time_score=0.40,
                pickoff_attempt_rate=0.08,
                pickoff_success_rate=0.02,
            ),
        ),
        away_catcher=CanonicalCatcherBaserunningProfile(
            catcher_id="away-catcher",
            team_side="away",
            throwing_score=0.45,
            pop_time_score=0.45,
        ),
        home_catcher=CanonicalCatcherBaserunningProfile(
            catcher_id="home-catcher",
            team_side="home",
            throwing_score=0.45,
            pop_time_score=0.45,
        ),
    )


def context():
    inputs = build_canonical_trial_factory_input(
        game_pk=123,
        config={
            "simulation_count": 1,
            "seed": 98765,
            "canonical_model_version": (
                "baserunning-composition-test-v1"
            ),
        },
    )

    return build_canonical_trial_resolver_context(
        factory_input=inputs,
        trial_index=0,
    )


def opportunity_state():
    return GameState(
        inning=7,
        half="top",
        outs=1,
        bases=("runner", None, None),
        away_score=2,
        home_score=2,
        batting_order_index=4,
        plate_appearance_number=25,
    )


class PlateAppearanceResolver:
    def __init__(self):
        self.identity_states = []
        self.baserunning_events = []

    def active_pitcher_id(self, state):
        self.identity_states.append(state)
        return "active-pitcher"

    def record_baserunning_event(self, event):
        self.baserunning_events.append(event)

    def __call__(self, state, batter_id, sequence):
        next_out = state.outs + 1

        return PlayEvent(
            sequence=sequence,
            event_type="out",
            batter_id=batter_id,
            pitcher_id="active-pitcher",
            state_before=state,
            state_after=replace(
                state,
                outs=next_out,
                batting_order_index=(
                    state.batting_order_index + 1
                ) % 9,
                plate_appearance_number=(
                    state.plate_appearance_number + 1
                ),
            ),
            outs_recorded=(
                OutRecord(
                    runner_id=batter_id,
                    out_number=next_out,
                    reason="composition_test_out",
                ),
            ),
        )


def test_composition_uses_trial_active_pitcher_identity():
    plate_appearance_resolver = PlateAppearanceResolver()
    factory = (
        build_canonical_catalog_baserunning_resolver_factory(
            catalog=catalog(),
        )
    )
    resolver = factory(
        context(),
        plate_appearance_resolver,
    )
    state = opportunity_state()

    event = resolver(
        state,
        "batter",
        10,
    )

    assert plate_appearance_resolver.identity_states == [
        state
    ]

    if event is not None:
        assert event.pitcher_id == "active-pitcher"
        assert event.state_before == state
        assert event.is_plate_appearance is False
        assert (
            plate_appearance_resolver
            .baserunning_events
            == [event]
        )
    else:
        assert (
            plate_appearance_resolver
            .baserunning_events
            == []
        )


def test_composed_resolver_replays_identically():
    factory = CanonicalCatalogBaserunningResolverFactory(
        catalog=catalog(),
    )

    first = factory(
        context(),
        PlateAppearanceResolver(),
    )(
        opportunity_state(),
        "batter",
        10,
    )
    second = factory(
        context(),
        PlateAppearanceResolver(),
    )(
        opportunity_state(),
        "batter",
        10,
    )

    assert first == second


def test_missing_active_pitcher_interface_fails_open():
    def plate_appearance_resolver(
        state,
        batter_id,
        sequence,
    ):
        raise AssertionError(
            "plate appearance must not execute"
        )

    resolver = (
        build_canonical_catalog_baserunning_resolver_factory(
            catalog=catalog(),
        )(
            context(),
            plate_appearance_resolver,
        )
    )

    assert resolver(
        opportunity_state(),
        "batter",
        10,
    ) is None


def test_composition_rejects_non_callable_pa_resolver():
    factory = CanonicalCatalogBaserunningResolverFactory(
        catalog=catalog(),
    )

    with pytest.raises(
        TypeError,
        match=(
            "plate_appearance_resolver must be callable"
        ),
    ):
        factory(
            context(),
            object(),
        )


def test_composition_preserves_version():
    assert (
        CanonicalCatalogBaserunningResolverFactory(
            catalog=catalog(),
        ).composition_version
        == CANONICAL_BASERUNNING_COMPOSITION_VERSION
    )
