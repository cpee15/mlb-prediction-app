import datetime as dt

from mlb_app.simulation.shadow.hitter_profile_canary_signal_adapter import (
    build_hitter_profile_canary_signals,
)


def rows():
    result = []
    row_id = 0

    for at_bat in range(1, 61):
        event = (
            "single"
            if at_bat <= 12
            else "double"
            if at_bat <= 18
            else "triple"
            if at_bat == 19
            else "home_run"
            if at_bat <= 23
            else "strikeout"
            if at_bat <= 33
            else "field_out"
        )

        for pitch_number in (1, 2):
            row_id += 1
            terminal = (
                pitch_number == 2
            )
            description = (
                "ball"
                if pitch_number == 1
                else "hit_into_play"
                if event
                not in {
                    "strikeout",
                }
                else "swinging_strike"
            )
            result.append({
                "id": row_id,
                "game_date":
                    dt.date(2026, 7, 1),
                "game_pk": 1,
                "at_bat_number":
                    at_bat,
                "pitch_number":
                    pitch_number,
                "batter_id": 7,
                "p_throws": "R",
                "description":
                    description,
                "events":
                    event
                    if terminal
                    else None,
                "estimated_ba_using_speedangle":
                    (
                        0.30
                        if terminal
                        and event
                        != "strikeout"
                        else None
                    ),
                "estimated_woba_using_speedangle":
                    (
                        0.42
                        if terminal
                        and event
                        != "strikeout"
                        else None
                    ),
            })

    return result


def test_builds_cutoff_safe_signals():
    result = (
        build_hitter_profile_canary_signals(
            rows(),
            player_id=7,
            season=2026,
            split="vsR",
            as_of_date="2026-07-31",
        )
    )

    assert result["status"] == "ready"
    assert result["cutoff_safe"] is True
    assert (
        result[
            "production_authority_changed"
        ]
        is False
    )
    assert result["coverage"][
        "pitch_count"
    ] == 120
    assert result["coverage"][
        "ab_count"
    ] == 60
    assert result["signals"][
        "called_ball_rate"
    ] == 0.5
    assert result["signals"][
        "whiff_rate"
    ] > 0


def test_expected_damage_definitions():
    result = (
        build_hitter_profile_canary_signals(
            rows(),
            player_id=7,
            season=2026,
            split="vsR",
            as_of_date="2026-07-31",
        )
    )
    signals = result["signals"]

    assert signals[
        "expected_damage_per_bbe"
    ] > 0
    assert signals[
        "expected_damage_per_ab"
    ] > 0
    assert (
        signals[
            "expected_damage_per_ab"
        ]
        < signals[
            "expected_damage_per_bbe"
        ]
    )


def test_builds_actual_hit_allocation():
    result = (
        build_hitter_profile_canary_signals(
            rows(),
            player_id=7,
            season=2026,
            split="vsR",
            as_of_date="2026-07-31",
        )
    )
    allocation = result[
        "signals"
    ]["actual_allocation"]

    assert allocation == {
        "single": 12 / 23,
        "double": 6 / 23,
        "triple": 1 / 23,
        "home_run": 4 / 23,
    }
    assert sum(allocation.values()) == 1.0


def test_excludes_post_cutoff_rows():
    payload = rows()
    future = dict(payload[-1])
    future["id"] = 999
    future["game_date"] = (
        dt.date(2026, 8, 1)
    )
    payload.append(future)

    result = (
        build_hitter_profile_canary_signals(
            payload,
            player_id=7,
            season=2026,
            split="vsR",
            as_of_date="2026-07-31",
        )
    )

    assert result["coverage"][
        "pitch_count"
    ] == 120


def test_blocks_insufficient_samples():
    result = (
        build_hitter_profile_canary_signals(
            rows()[:20],
            player_id=7,
            season=2026,
            split="vsR",
            as_of_date="2026-07-31",
        )
    )

    assert result["status"] == "blocked"
    assert (
        "insufficient_pre_cutoff_pitches"
        in result["blockers"]
    )
    assert (
        "insufficient_pre_cutoff_ab"
        in result["blockers"]
    )


def test_filters_player_and_split():
    payload = rows()
    wrong_player = dict(payload[0])
    wrong_player["id"] = 1001
    wrong_player["batter_id"] = 99
    wrong_hand = dict(payload[0])
    wrong_hand["id"] = 1002
    wrong_hand["p_throws"] = "L"
    payload.extend(
        [
            wrong_player,
            wrong_hand,
        ]
    )

    result = (
        build_hitter_profile_canary_signals(
            payload,
            player_id=7,
            season=2026,
            split="vsR",
            as_of_date="2026-07-31",
        )
    )

    assert result["coverage"][
        "pitch_count"
    ] == 120
