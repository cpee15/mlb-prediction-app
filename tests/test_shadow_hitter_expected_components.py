import datetime as dt

from mlb_app.simulation.shadow.hitter_expected_components import (
    SHADOW_HITTER_EXPECTED_COMPONENTS_VERSION,
    build_shadow_hitter_expected_components,
)


def event(
    year,
    pa,
    outcome,
    *,
    pitch=1,
    xwoba=None,
    xba=None,
    throws="R",
    month=6,
    day=1,
):
    return {
        "id": year * 10000 + pa * 10 + pitch,
        "game_date": dt.date(year, month, day),
        "game_pk": year * 1000 + pa,
        "at_bat_number": pa,
        "pitch_number": pitch,
        "events": outcome,
        "p_throws": throws,
        "estimated_woba_using_speedangle": xwoba,
        "estimated_ba_using_speedangle": xba,
    }


def sample_rows(latest_day=25):
    rows = []
    for year in (2023, 2024, 2025, 2026):
        for pa in range(1, 31):
            outcome = "strikeout" if pa % 5 == 0 else "field_out"
            rows.append(
                event(
                    year,
                    pa,
                    outcome,
                    xwoba=0.0 if outcome == "strikeout" else 0.310,
                    xba=None if outcome == "strikeout" else 0.250,
                    month=7 if year == 2026 else 6,
                    day=latest_day if year == 2026 else 1,
                )
            )
    return rows


def test_builds_disjoint_fresh_expected_windows_without_authority_change():
    result = build_shadow_hitter_expected_components(
        player_id=7,
        season=2026,
        split="vsR",
        statcast_events=sample_rows(),
        as_of_date=dt.date(2026, 7, 27),
    )

    assert result["schema_version"] == SHADOW_HITTER_EXPECTED_COMPONENTS_VERSION
    assert result["status"] == "ready"
    assert result["shadow_only"] is True
    assert result["production_authority_changed"] is False
    assert result["expected_adjustment_applied"] is False
    assert result["windows"]["current_season"]["pa"] == 30
    assert result["windows"]["prior_season"]["pa"] == 30
    assert result["windows"]["career_pre_prior"]["pa"] == 60
    assert result["blended_expected_metrics"]["xwoba"] is not None
    assert result["blended_expected_metrics"]["xba"] is not None


def test_xba_uses_all_at_bats_and_counts_strikeouts_as_zero():
    result = build_shadow_hitter_expected_components(
        player_id=7,
        season=2026,
        split="vsR",
        statcast_events=sample_rows(),
        as_of_date=dt.date(2026, 7, 27),
    )

    current = result["windows"]["current_season"]
    assert current["ab"] == 30
    assert current["contact_ab"] == 24
    assert current["xba_contact_sample"] == 24
    assert current["xba"] == 0.2


def test_deduplicates_plate_appearances_and_excludes_future_and_wrong_split():
    rows = sample_rows()
    rows.extend(
        [
            event(2026, 1, "field_out", pitch=2, xwoba=0.4, xba=0.3, month=7, day=25),
            event(2026, 99, "field_out", xwoba=0.9, xba=0.9, throws="L", month=7, day=25),
            event(2026, 100, "field_out", xwoba=0.9, xba=0.9, month=7, day=28),
        ]
    )
    result = build_shadow_hitter_expected_components(
        player_id=7,
        season=2026,
        split="vsR",
        statcast_events=rows,
        as_of_date=dt.date(2026, 7, 27),
    )

    assert result["windows"]["current_season"]["pa"] == 30
    assert result["future_rows_excluded"] == 1
    assert "future_rows_excluded" in result["warnings"]


def test_blocks_stale_or_incomplete_expected_evidence():
    stale = build_shadow_hitter_expected_components(
        player_id=7,
        season=2026,
        split="vsR",
        statcast_events=sample_rows(latest_day=1),
        as_of_date=dt.date(2026, 7, 27),
    )
    incomplete = build_shadow_hitter_expected_components(
        player_id=7,
        season=2026,
        split="vsR",
        statcast_events=[
            event(2026, pa, "field_out", month=7, day=25)
            for pa in range(1, 31)
        ],
        as_of_date=dt.date(2026, 7, 27),
    )

    assert stale["status"] == "blocked"
    assert "stale_statcast_source" in stale["blockers"]
    assert incomplete["status"] == "blocked"
    assert "insufficient_xwoba_windows" in incomplete["blockers"]
    assert "insufficient_xba_windows" in incomplete["blockers"]



def test_requires_current_expected_coverage_even_when_history_is_usable():
    rows = sample_rows()
    for row in rows:
        if row["game_date"].year == 2026:
            row["estimated_woba_using_speedangle"] = None
            row["estimated_ba_using_speedangle"] = None
    result = build_shadow_hitter_expected_components(
        player_id=7,
        season=2026,
        split="vsR",
        statcast_events=rows,
        as_of_date=dt.date(2026, 7, 27),
    )

    assert result["status"] == "blocked"
    assert "insufficient_current_xwoba_coverage" in result["blockers"]
    assert "insufficient_current_xba_coverage" in result["blockers"]


def test_global_source_date_controls_freshness_not_player_split_recency():
    result = build_shadow_hitter_expected_components(
        player_id=7,
        season=2026,
        split="vsR",
        statcast_events=sample_rows(latest_day=1),
        as_of_date=dt.date(2026, 7, 27),
        source_latest_date=dt.date(2026, 7, 26),
    )

    assert result["status"] == "ready"
    assert result["source_age_days"] == 1
    assert result["source_latest_date"] == "2026-07-26"
    assert result["player_latest_date"] == "2026-07-01"
