import hashlib
from datetime import date, timedelta

import pytest

from mlb_app.simulation.shadow import (
    CANONICAL_LIVE_BASERUNNING_SHADOW_MONITOR_VERSION,
    CANONICAL_LIVE_BASERUNNING_SHADOW_OBSERVATION_VERSION,
    CanonicalBaserunningOutputValidation,
    CanonicalLiveBaserunningShadowMonitor,
    CanonicalLiveBaserunningShadowObservation,
    summarize_live_baserunning_shadow,
)


DIGEST = hashlib.sha256(b"context").hexdigest()
TRANSFORM_DIGEST = hashlib.sha256(
    b"transform"
).hexdigest()
CATALOG_DIGEST = hashlib.sha256(
    b"catalog"
).hexdigest()


def validation(
    *,
    stolen_bases,
    caught_stealing,
    status="ready",
):
    return CanonicalBaserunningOutputValidation(
        status=status,
        simulation_count=25,
        catalog_digest=CATALOG_DIGEST,
        runner_projection_count=18,
        stolen_base_mean_total=stolen_bases,
        caught_stealing_mean_total=caught_stealing,
    )


def observation(
    game_pk=1,
    game_date="2026-07-20",
):
    return CanonicalLiveBaserunningShadowObservation(
        game_pk=game_pk,
        game_date=game_date,
        paired_context_digest=DIGEST,
        calibrated_transform_digest=(
            TRANSFORM_DIGEST
        ),
        legacy_validation=validation(
            stolen_bases=2.0,
            caught_stealing=1.0,
        ),
        calibrated_validation=validation(
            stolen_bases=1.25,
            caught_stealing=0.5,
        ),
        input_parity_verified=True,
        seed_parity_verified=True,
    )


def test_paired_observation_reports_deltas():
    value = observation()
    diagnostics = value.to_diagnostics()

    assert value.ready is True
    assert value.status == "ready"
    assert value.stolen_base_delta == -0.75
    assert value.caught_stealing_delta == -0.5
    assert diagnostics["input_parity_verified"] is True
    assert diagnostics["seed_parity_verified"] is True
    assert diagnostics["activation_permitted"] is False
    assert diagnostics["production_activation"] is False
    assert (
        diagnostics["production_authority_changed"]
        is False
    )
    assert diagnostics["authoritative_source"] == "legacy"


def test_ready_observation_requires_pair_parity():
    with pytest.raises(
        ValueError,
        match="requires input and seed parity",
    ):
        CanonicalLiveBaserunningShadowObservation(
            game_pk=1,
            game_date="2026-07-20",
            paired_context_digest=DIGEST,
            calibrated_transform_digest=(
                TRANSFORM_DIGEST
            ),
            legacy_validation=validation(
                stolen_bases=2.0,
                caught_stealing=1.0,
            ),
            calibrated_validation=validation(
                stolen_bases=1.25,
                caught_stealing=0.5,
            ),
            input_parity_verified=False,
            seed_parity_verified=True,
        )


def test_monitor_requires_unique_games():
    value = observation()

    with pytest.raises(
        ValueError,
        match="must be unique",
    ):
        CanonicalLiveBaserunningShadowMonitor(
            observations=(value, value),
        )


def test_live_shadow_threshold_requires_100_games_and_7_days():
    start = date(2026, 7, 1)
    observations = tuple(
        observation(
            game_pk=index + 1,
            game_date=(
                start
                + timedelta(days=index % 7)
            ).isoformat(),
        )
        for index in range(100)
    )

    monitor = summarize_live_baserunning_shadow(
        observations
    )
    diagnostics = monitor.to_diagnostics()

    assert monitor.game_count == 100
    assert monitor.ready_count == 100
    assert monitor.day_span == 7
    assert monitor.live_shadow_complete is True
    assert (
        diagnostics["eligible_for_activation_review"]
        is True
    )
    assert diagnostics["activation_permitted"] is False


def test_six_day_monitor_is_incomplete():
    start = date(2026, 7, 1)
    observations = tuple(
        observation(
            game_pk=index + 1,
            game_date=(
                start
                + timedelta(days=index % 6)
            ).isoformat(),
        )
        for index in range(100)
    )

    monitor = summarize_live_baserunning_shadow(
        observations
    )

    assert monitor.day_span == 6
    assert monitor.live_shadow_complete is False


def test_versions_are_explicit():
    assert (
        CANONICAL_LIVE_BASERUNNING_SHADOW_OBSERVATION_VERSION
        == "canonical_live_baserunning_shadow_observation_v1"
    )
    assert (
        CANONICAL_LIVE_BASERUNNING_SHADOW_MONITOR_VERSION
        == "canonical_live_baserunning_shadow_monitor_v1"
    )
