from mlb_app.simulation.events import Base, GameState
from mlb_app.simulation.game import (
    CanonicalBaserunningEvidence,
    CanonicalBaserunningResolverAdapterFactory,
    build_canonical_trial_factory_input,
    build_canonical_trial_resolver_context,
)


def context():
    inputs = build_canonical_trial_factory_input(
        game_pk=824406,
        config={
            "simulation_count": 1,
            "seed": 123456,
            "canonical_model_version": (
                "canonical-baserunning-test-v1"
            ),
        },
    )
    return build_canonical_trial_resolver_context(
        factory_input=inputs,
        trial_index=0,
    )


def state(
    *,
    bases=("runner", None, None),
    outs=1,
):
    return GameState(
        inning=7,
        half="top",
        outs=outs,
        bases=bases,
        away_score=2,
        home_score=2,
        batting_order_index=4,
        plate_appearance_number=25,
    )


def test_resolver_returns_none_without_eligible_runner():
    calls = []

    def provider(query):
        calls.append(query)
        return CanonicalBaserunningEvidence(
            pitcher_id="pitcher",
            attempt_probability=1.0,
            success_probability=1.0,
            probability_provenance="test-provider-v1",
        )

    resolver = CanonicalBaserunningResolverAdapterFactory(
        evidence_provider=provider,
    )(context())

    event = resolver(
        state(bases=(None, None, None)),
        "batter",
        10,
    )

    assert event is None
    assert calls == []


def test_missing_evidence_fails_open():
    resolver = CanonicalBaserunningResolverAdapterFactory(
        evidence_provider=lambda query: None,
    )(context())

    assert resolver(
        state(),
        "batter",
        10,
    ) is None


def test_provider_exception_fails_open():
    def provider(query):
        raise RuntimeError("evidence unavailable")

    resolver = CanonicalBaserunningResolverAdapterFactory(
        evidence_provider=provider,
    )(context())

    assert resolver(
        state(),
        "batter",
        10,
    ) is None


def test_complete_evidence_emits_stolen_base_event():
    observed = []

    def provider(query):
        observed.append(query)
        return CanonicalBaserunningEvidence(
            pitcher_id="active-pitcher",
            attempt_probability=1.0,
            success_probability=1.0,
            probability_provenance="test-provider-v1",
        )

    resolver = CanonicalBaserunningResolverAdapterFactory(
        evidence_provider=provider,
    )(context())

    event = resolver(
        state(),
        "batter",
        10,
    )

    assert event is not None
    assert event.event_type == "stolen_base"
    assert event.sequence == 10
    assert event.batter_id == "batter"
    assert event.pitcher_id == "active-pitcher"
    assert event.is_plate_appearance is False
    assert event.state_after.bases == (
        None,
        "runner",
        None,
    )
    assert observed[0].runner_id == "runner"
    assert observed[0].origin_base is Base.FIRST
    assert observed[0].target_base is Base.SECOND


def test_failed_attempt_emits_caught_stealing_event():
    resolver = CanonicalBaserunningResolverAdapterFactory(
        evidence_provider=lambda query: (
            CanonicalBaserunningEvidence(
                pitcher_id="active-pitcher",
                attempt_probability=1.0,
                success_probability=0.0,
                probability_provenance="test-provider-v1",
            )
        ),
    )(context())

    event = resolver(
        state(),
        "batter",
        10,
    )

    assert event is not None
    assert event.event_type == "caught_stealing"
    assert event.state_after.bases == (
        None,
        None,
        None,
    )
    assert event.state_after.outs == 2


def test_hold_sample_emits_no_event():
    resolver = CanonicalBaserunningResolverAdapterFactory(
        evidence_provider=lambda query: (
            CanonicalBaserunningEvidence(
                pitcher_id="active-pitcher",
                attempt_probability=0.0,
                success_probability=1.0,
                probability_provenance="test-provider-v1",
            )
        ),
    )(context())

    assert resolver(
        state(),
        "batter",
        10,
    ) is None


def test_lead_runner_opportunity_is_selected_first():
    observed = []

    def provider(query):
        observed.append(query)
        return None

    resolver = CanonicalBaserunningResolverAdapterFactory(
        evidence_provider=provider,
    )(context())

    resolver(
        state(
            bases=(
                "runner-first",
                "runner-second",
                None,
            )
        ),
        "batter",
        10,
    )

    assert observed[0].runner_id == "runner-second"
    assert observed[0].origin_base is Base.SECOND
    assert observed[0].target_base is Base.THIRD


def test_same_trial_identity_reproduces_event():
    factory = CanonicalBaserunningResolverAdapterFactory(
        evidence_provider=lambda query: (
            CanonicalBaserunningEvidence(
                pitcher_id="active-pitcher",
                attempt_probability=1.0,
                success_probability=1.0,
                probability_provenance="test-provider-v1",
            )
        ),
    )

    first = factory(context())(
        state(),
        "batter",
        10,
    )
    second = factory(context())(
        state(),
        "batter",
        10,
    )

    assert first == second
