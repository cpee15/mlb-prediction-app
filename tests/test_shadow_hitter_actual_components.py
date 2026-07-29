import datetime as dt

import pytest

from mlb_app.simulation.shadow.hitter_actual_components import (
    build_shadow_hitter_actual_components,
)


def row(
    year,
    number,
    event,
    *,
    hand="R",
    month=4,
    day=1,
    pitch=1,
    row_id=None,
):
    return {
        "id": row_id if row_id is not None else year * 1000 + number,
        "game_date": dt.date(year, month, day),
        "game_pk": year * 10,
        "at_bat_number": number,
        "pitch_number": pitch,
        "p_throws": hand,
        "events": event,
    }


def season_rows(year, start_number, outcomes, *, hand="R"):
    return [
        row(year, start_number + index, outcome, hand=hand)
        for index, outcome in enumerate(outcomes)
    ]


def ready_rows():
    current = (
        ["single"] * 6
        + ["double"] * 3
        + ["home_run"] * 2
        + ["walk"] * 3
        + ["strikeout"] * 6
        + ["field_out"] * 10
    )
    prior = ["single"] * 10 + ["walk"] * 5 + ["strikeout"] * 10 + ["field_out"] * 25
    career = ["single"] * 12 + ["double"] * 4 + ["home_run"] * 4 + ["walk"] * 10 + ["strikeout"] * 20 + ["field_out"] * 50
    return (
        season_rows(2026, 1, current)
        + season_rows(2025, 100, prior)
        + season_rows(2024, 200, career)
    )


def build(rows=None, **overrides):
    args = {
        "player_id": 7,
        "season": 2026,
        "split": "vsR",
        "statcast_events": ready_rows() if rows is None else rows,
        "as_of_date": dt.date(2026, 7, 27),
        "source_latest_date": dt.date(2026, 7, 27),
        "career_start_season": 2023,
    }
    args.update(overrides)
    return build_shadow_hitter_actual_components(**args)


def test_builds_disjoint_cutoff_safe_windows_and_canonical_rates():
    result = build()

    assert result["status"] == "ready"
    assert set(result["windows"]) == {
        "current_season",
        "prior_season",
        "career_pre_prior",
    }
    current = result["windows"]["current_season"]
    assert current["pa"] == 30
    assert current["ab"] == 27
    assert current["hits"] == 11
    assert current["walks"] == 3
    assert current["strikeouts"] == 6
    assert current["total_bases"] == 20
    assert current["k_pct"] == pytest.approx(0.20)
    assert current["bb_pct"] == pytest.approx(0.10)
    assert current["batting_avg"] == pytest.approx(11 / 27)
    assert current["slugging_pct"] == pytest.approx(20 / 27)
    assert current["iso"] == pytest.approx(9 / 27)
    assert sum(
        window["normalized_weight"]
        for window in result["windows"].values()
    ) == pytest.approx(1.0)
    assert result["shadow_only"] is True
    assert result["production_authority_changed"] is False


def test_dedupes_terminal_pa_and_excludes_future_and_wrong_hand():
    rows = ready_rows()
    rows.extend(
        [
            row(2026, 999, "single", pitch=1, row_id=1),
            row(2026, 999, "home_run", pitch=2, row_id=2),
            row(2026, 1000, "home_run", hand="L"),
            row(2026, 1001, "home_run", month=8, day=1),
        ]
    )
    result = build(rows)
    current = result["windows"]["current_season"]

    assert current["pa"] == 31
    assert current["hits"] == 12
    assert current["total_bases"] == 24


def test_walk_hbp_and_sacrifice_are_plate_appearances_but_not_at_bats():
    rows = ready_rows() + [
        row(2026, 1002, "intent_walk"),
        row(2026, 1003, "hit_by_pitch"),
        row(2026, 1004, "sac_fly"),
    ]
    current = build(rows)["windows"]["current_season"]

    assert current["pa"] == 33
    assert current["ab"] == 27
    assert current["walks"] == 4
    assert current["bb_pct"] == pytest.approx(4 / 33)


def test_strict_cutoff_prevents_later_plate_appearances_from_leaking():
    rows = ready_rows() + [
        row(2026, 1002, "home_run", month=6, day=1),
        row(2026, 1003, "home_run", month=7, day=1),
    ]
    early = build(
        rows,
        as_of_date=dt.date(2026, 6, 15),
        source_latest_date=dt.date(2026, 6, 15),
    )
    late = build(rows)

    assert early["windows"]["current_season"]["pa"] == 31
    assert late["windows"]["current_season"]["pa"] == 32
    assert (
        early["windows"]["current_season"]["slugging_pct"]
        < late["windows"]["current_season"]["slugging_pct"]
    )


def test_blocks_low_current_sample_and_stale_global_source():
    rows = [
        row for row in ready_rows()
        if _year(row) != 2026
    ] + season_rows(2026, 1, ["single"] * 10)
    result = build(
        rows,
        source_latest_date=dt.date(2026, 5, 3),
    )

    assert result["status"] == "blocked"
    assert "insufficient_current_season_pa" in result["blockers"]
    assert "stale_statcast_source" in result["blockers"]


def _year(row):
    return row["game_date"].year


def test_global_source_freshness_is_separate_from_player_recency():
    rows = ready_rows()
    result = build(
        rows,
        source_latest_date=dt.date(2026, 7, 26),
    )

    assert result["status"] == "ready"
    assert result["source_age_days"] == 1
    assert result["player_latest_date"] == "2026-04-01"


def test_rejects_unknown_split():
    with pytest.raises(ValueError):
        build(split="all")
