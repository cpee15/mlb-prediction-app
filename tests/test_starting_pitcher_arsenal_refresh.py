import pandas as pd

from mlb_app.starting_pitcher_arsenal_refresh import (
    aggregate_pitch_rows_to_arsenal,
)


def test_usage_pct_aggregation():
    df = pd.DataFrame({
        "pitch_type": ["FF"] * 6 + ["SL"] * 4,
        "description": ["swing"] * 10,
        "events": [""] * 10,
    })

    rows = aggregate_pitch_rows_to_arsenal(df, pitcher_id=1, season=2025, source_window="test")

    ff = next(r for r in rows if r["pitch_type"] == "FF")
    sl = next(r for r in rows if r["pitch_type"] == "SL")

    assert round(ff["usage_pct"], 2) == 0.6
    assert round(sl["usage_pct"], 2) == 0.4


def test_whiff_pct_calculation():
    df = pd.DataFrame({
        "pitch_type": ["FF", "FF", "FF"],
        "description": ["swinging_strike", "foul", "hit_into_play"],
        "events": ["", "", ""],
    })

    rows = aggregate_pitch_rows_to_arsenal(df, pitcher_id=1, season=2025, source_window="test")
    ff = rows[0]

    # 1 whiff / 3 swings
    assert round(ff["whiff_pct"], 3) == round(1 / 3, 3)


def test_pa_end_k_pct():
    df = pd.DataFrame({
        "pitch_type": ["FF", "FF", "FF"],
        "description": ["swing", "swing", "swing"],
        "events": ["strikeout", "", "strikeout"],
    })

    rows = aggregate_pitch_rows_to_arsenal(df, pitcher_id=1, season=2025, source_window="test")
    ff = rows[0]

    # 2 strikeouts / 2 PA-ended
    assert ff["strikeout_pct"] == 1.0


def test_missing_xwoba():
    df = pd.DataFrame({
        "pitch_type": ["FF", "FF"],
        "description": ["swing", "swing"],
        "events": ["", ""],
        "estimated_woba_using_speedangle": [None, None],
    })

    rows = aggregate_pitch_rows_to_arsenal(df, pitcher_id=1, season=2025, source_window="test")
    ff = rows[0]

    assert ff["xwoba"] is None
    assert ff["xwoba_sample_count"] == 0
    assert "no_xwoba_sample" in ff["quality_flags"]


def test_small_sample_flags():
    df = pd.DataFrame({
        "pitch_type": ["FF"],
        "description": ["swing"],
        "events": [""],
    })

    rows = aggregate_pitch_rows_to_arsenal(df, pitcher_id=1, season=2025, source_window="test")
    ff = rows[0]

    assert "small_pitch_sample" in ff["quality_flags"]
    assert "unstable_pa_end_k_rate" in ff["quality_flags"]


def test_legacy_fields_present():
    df = pd.DataFrame({
        "pitch_type": ["FF", "SL"],
        "description": ["swing", "swing"],
        "events": ["", ""],
    })

    rows = aggregate_pitch_rows_to_arsenal(df, pitcher_id=1, season=2025, source_window="test")

    for row in rows:
        for field in [
            "pitch_type",
            "pitch_name",
            "pitch_count",
            "usage_pct",
            "whiff_pct",
            "strikeout_pct",
            "xwoba",
            "hard_hit_pct",
        ]:
            assert field in row
