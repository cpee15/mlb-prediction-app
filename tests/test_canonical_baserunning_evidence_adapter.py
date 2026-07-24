from copy import deepcopy

import pytest

from mlb_app.simulation.events import Base, GameState
from mlb_app.simulation.game import (
    CANONICAL_BASERUNNING_EVIDENCE_ADAPTER_VERSION,
    CanonicalBaserunningEvidenceQuery,
    build_canonical_baserunning_evidence_provider,
)


def query():
    return CanonicalBaserunningEvidenceQuery(
        state=GameState(
            inning=7,
            half="top",
            outs=1,
            bases=("runner", None, None),
            away_score=2,
            home_score=2,
            batting_order_index=4,
            plate_appearance_number=25,
        ),
        batter_id="batter",
        runner_id="runner",
        origin_base=Base.FIRST,
        target_base=Base.SECOND,
    )


def complete_state():
    return {
        "inning": 7,
        "half": "top",
        "outs": 1,
        "base_state": {
            "first": True,
            "second": False,
            "third": False,
        },
        "score_margin": 0,
        "runner": {
            "runner_id": "runner",
            "evidence_complete": True,
            "speed_score": 0.85,
            "attempt_rate": 0.30,
            "success_rate": 0.80,
            "lead_quality": 0.75,
            "fatigue_index": 0.10,
            "injury_limit_flag": False,
        },
        "origin_base": "first",
        "target_base": "second",
        "pitcher": {
            "pitcher_id": "pitcher",
            "evidence_complete": True,
            "hold_score": 0.40,
            "delivery_time_score": 0.45,
            "pickoff_attempt_rate": 0.08,
            "pickoff_success_rate": 0.02,
        },
        "catcher": {
            "catcher_id": "catcher",
            "evidence_complete": True,
            "throwing_score": 0.45,
            "pop_time_score": 0.40,
        },
    }


def test_complete_evaluation_adapts_to_typed_evidence():
    source = complete_state()
    provider = build_canonical_baserunning_evidence_provider(
        state_provider=lambda opportunity: source,
    )

    evidence = provider(query())

    assert evidence is not None
    assert evidence.pitcher_id == "pitcher"
    assert 0.0 <= evidence.attempt_probability <= 1.0
    assert 0.0 <= evidence.success_probability <= 1.0
    assert (
        evidence.probability_provenance
        == CANONICAL_BASERUNNING_EVIDENCE_ADAPTER_VERSION
    )


def test_adapter_does_not_mutate_provider_state():
    source = complete_state()
    snapshot = deepcopy(source)
    provider = build_canonical_baserunning_evidence_provider(
        state_provider=lambda opportunity: source,
    )

    provider(query())

    assert source == snapshot


@pytest.mark.parametrize(
    "field,value",
    (
        ("inning", 8),
        ("half", "bottom"),
        ("outs", 2),
        ("origin_base", "second"),
        ("target_base", "third"),
    ),
)
def test_mismatched_game_state_fails_open(
    field,
    value,
):
    source = complete_state()
    source[field] = value
    provider = build_canonical_baserunning_evidence_provider(
        state_provider=lambda opportunity: source,
    )

    assert provider(query()) is None


def test_mismatched_runner_identity_fails_open():
    source = complete_state()
    source["runner"]["runner_id"] = "other-runner"
    provider = build_canonical_baserunning_evidence_provider(
        state_provider=lambda opportunity: source,
    )

    assert provider(query()) is None


def test_mismatched_base_occupancy_fails_open():
    source = complete_state()
    source["base_state"]["second"] = True
    provider = build_canonical_baserunning_evidence_provider(
        state_provider=lambda opportunity: source,
    )

    assert provider(query()) is None


def test_partial_participant_evidence_fails_open():
    source = complete_state()
    source["catcher"]["evidence_complete"] = False
    provider = build_canonical_baserunning_evidence_provider(
        state_provider=lambda opportunity: source,
    )

    assert provider(query()) is None


def test_ineligible_evaluation_fails_open():
    source = complete_state()
    source["runner"]["injury_limit_flag"] = True
    provider = build_canonical_baserunning_evidence_provider(
        state_provider=lambda opportunity: source,
    )

    assert provider(query()) is None


def test_missing_state_fails_open():
    provider = build_canonical_baserunning_evidence_provider(
        state_provider=lambda opportunity: None,
    )

    assert provider(query()) is None


def test_state_provider_exception_fails_open():
    def state_provider(opportunity):
        raise RuntimeError("source unavailable")

    provider = build_canonical_baserunning_evidence_provider(
        state_provider=state_provider,
    )

    assert provider(query()) is None


def test_non_query_input_fails_open():
    provider = build_canonical_baserunning_evidence_provider(
        state_provider=lambda opportunity: complete_state(),
    )

    assert provider(object()) is None
