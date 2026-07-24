from datetime import datetime

import pytest

from mlb_app.simulation.shadow import (
    CANONICAL_BASERUNNING_SMOKE_WINDOW_END,
    CANONICAL_BASERUNNING_SMOKE_WINDOW_START,
    CANONICAL_STATCAST_BASERUNNING_WINDOW_SOURCE_VERSION,
    source_statcast_baserunning_window,
)


def row(**overrides):
    value = {
        "game_date": "2026-04-20",
        "game_pk": 1,
        "at_bat_number": 10,
        "pitch_number": 3,
        "des": (
            "Batter strikes out. "
            "Runner steals (1) 2nd base."
        ),
        "on_1b": 300,
        "on_2b": None,
        "on_3b": None,
        "pitcher": 100,
        "fielder_2": 200,
    }
    value.update(overrides)
    return value


def source(rows=None):
    return source_statcast_baserunning_window(
        rows=(
            rows
            if rows is not None
            else (
                row(
                    game_pk=2,
                    game_date="2026-04-21",
                    at_bat_number=1,
                    pitch_number=1,
                    des="Batter grounds out.",
                    on_1b=None,
                ),
                row(),
                row(
                    at_bat_number=20,
                    on_1b=301,
                    des=(
                        "Runner caught stealing 2nd, "
                        "catcher."
                    ),
                ),
            )
        ),
        window_start=(
            CANONICAL_BASERUNNING_SMOKE_WINDOW_START
        ),
        window_end=(
            CANONICAL_BASERUNNING_SMOKE_WINDOW_END
        ),
    )


def test_sources_complete_window_snapshot():
    result = source()

    assert result.snapshot.game_count == 2
    assert result.snapshot.row_count == 3
    assert result.snapshot.outcome_count == 2
    assert result.snapshot.stolen_bases == 1
    assert result.snapshot.caught_stealing == 1
    assert result.snapshot.game_keys == (
        (1, "2026-04-20"),
        (2, "2026-04-21"),
    )


def test_rows_are_sorted_deterministically():
    result = source()

    assert tuple(
        (
            value["game_pk"],
            value["at_bat_number"],
        )
        for value in result.rows
    ) == (
        (1, 10),
        (1, 20),
        (2, 1),
    )


def test_datetime_game_date_is_canonicalized():
    result = source(
        rows=(
            row(
                game_date=datetime(
                    2026,
                    4,
                    20,
                    12,
                    0,
                )
            ),
        )
    )

    assert result.rows[0]["game_date"] == (
        "2026-04-20"
    )


def test_duplicate_pitch_is_rejected():
    value = row()

    with pytest.raises(
        ValueError,
        match=(
            "Statcast pitch identifiers "
            "must be unique"
        ),
    ):
        source(rows=(value, value))


def test_out_of_window_row_is_rejected():
    with pytest.raises(
        ValueError,
        match=(
            "Statcast row game_date must fall "
            "within source window"
        ),
    ):
        source(
            rows=(
                row(game_date="2026-04-19"),
            )
        )


def test_missing_column_is_rejected():
    value = row()
    del value["fielder_2"]

    with pytest.raises(
        ValueError,
        match=(
            "Statcast row missing required columns: "
            "fielder_2"
        ),
    ):
        source(rows=(value,))


def test_one_game_pk_cannot_have_multiple_dates():
    with pytest.raises(
        ValueError,
        match=(
            "game_pk must map to one game_date"
        ),
    ):
        source(
            rows=(
                row(),
                row(
                    game_date="2026-04-21",
                    at_bat_number=11,
                ),
            )
        )


def test_snapshot_digest_is_deterministic():
    first = source()
    second = source()

    assert first.snapshot.digest == (
        second.snapshot.digest
    )
    assert first == second


def test_diagnostics_preserve_legacy_authority():
    diagnostics = source().snapshot.to_diagnostics()

    assert diagnostics["coverage_complete"] is False
    assert diagnostics[
        "calibration_observed_source_eligible"
    ] is False
    assert diagnostics["coverage_warning"] == (
        "pitch-level Statcast descriptions "
        "do not contain every baserunning event"
    )
    assert diagnostics["production_activation"] is False
    assert diagnostics[
        "production_authority_changed"
    ] is False
    assert diagnostics["authoritative_source"] == "legacy"


def test_versions_and_window_are_explicit():
    assert (
        CANONICAL_STATCAST_BASERUNNING_WINDOW_SOURCE_VERSION
        == "canonical_statcast_baserunning_window_source_v1"
    )
    assert (
        CANONICAL_BASERUNNING_SMOKE_WINDOW_START
        == "2026-04-20"
    )
    assert (
        CANONICAL_BASERUNNING_SMOKE_WINDOW_END
        == "2026-05-03"
    )
