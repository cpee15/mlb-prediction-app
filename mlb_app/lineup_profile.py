from __future__ import annotations

import datetime as dt

from typing import Any, Dict, List, Optional

import requests

from .lineup_handedness import build_lineup_handedness_mix
from sqlalchemy.orm import Session

from .db_utils import get_batter_aggregate, get_player_split
from .etl import MLB_STATS_BASE


STARTING_BATTING_ORDERS = {100, 200, 300, 400, 500, 600, 700, 800, 900}
MIN_USABLE_HITTERS = 7


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _avg(values: List[Optional[float]]) -> Optional[float]:
    clean = [float(v) for v in values if v is not None]
    if not clean:
        return None
    return round(sum(clean) / len(clean), 4)


def _sum_int(values: List[Optional[int]]) -> Optional[int]:
    clean = [int(v) for v in values if v is not None]
    if not clean:
        return None
    return sum(clean)


def _fallback_value(team_fallback: Dict[str, Any], key: str) -> Any:
    return (team_fallback or {}).get(key)


def _compute_iso(batting_avg: Optional[float], slugging_pct: Optional[float], explicit_iso: Optional[float] = None) -> Optional[float]:
    if explicit_iso is not None:
        # Team ETL has historically stored OPS in iso for some rows. PlayerSplit should
        # be closer to true ISO, but still guard against impossible values.
        iso = _safe_float(explicit_iso)
        if iso is not None and 0.0 <= iso <= 0.45:
            return round(iso, 4)

    avg = _safe_float(batting_avg)
    slg = _safe_float(slugging_pct)
    if avg is None or slg is None:
        return None
    return round(max(slg - avg, 0.0), 4)


def fetch_boxscore_lineup(game_pk: int) -> Dict[str, List[Dict[str, Any]]]:
    """
    Fetch confirmed starting lineups from MLB Stats API boxscore.

    Only starting batting-order slots are returned:
    100, 200, ..., 900. Substitution values like 301/601/901 are ignored
    for the first aggregate lineup model.
    """
    url = f"{MLB_STATS_BASE}/game/{game_pk}/boxscore"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    boxscore = resp.json()

    return {
        "away": _extract_starting_lineup(boxscore, "away"),
        "home": _extract_starting_lineup(boxscore, "home"),
    }


def _extract_starting_lineup(boxscore: Dict[str, Any], side: str) -> List[Dict[str, Any]]:
    team = ((boxscore.get("teams") or {}).get(side) or {})
    players = team.get("players") or {}

    lineup: List[Dict[str, Any]] = []

    for raw_key, player_obj in players.items():
        person = player_obj.get("person") or {}
        position = player_obj.get("position") or {}

        batting_order = _safe_int(player_obj.get("battingOrder"))
        if batting_order not in STARTING_BATTING_ORDERS:
            continue

        lineup.append({
            "raw_key": raw_key,
            "batter_id": _safe_int(person.get("id")),
            "name": person.get("fullName"),
            "batting_order": batting_order,
            "lineup_slot": int(batting_order / 100),
            "position": position.get("abbreviation") or position.get("name"),
            "position_code": position.get("code"),
        })

    lineup.sort(key=lambda row: row.get("batting_order") or 999999)
    return lineup


def _split_to_dict(split_obj: Any) -> Dict[str, Any]:
    if not split_obj:
        return {}
    batting_avg = _safe_float(split_obj.batting_avg)
    slugging_pct = _safe_float(split_obj.slugging_pct)
    return {
        "pa": split_obj.pa,
        "hits": split_obj.hits,
        "doubles": split_obj.doubles,
        "triples": split_obj.triples,
        "home_runs": split_obj.home_runs,
        "walks": split_obj.walks,
        "strikeouts": split_obj.strikeouts,
        "batting_avg": batting_avg,
        "on_base_pct": _safe_float(split_obj.on_base_pct),
        "slugging_pct": slugging_pct,
        "iso": _compute_iso(batting_avg, slugging_pct, _safe_float(split_obj.iso)),
        "k_pct": _safe_float(split_obj.k_pct),
        "bb_pct": _safe_float(split_obj.bb_pct),
    }


def _batter_aggregate_to_dict(agg: Any) -> Dict[str, Any]:
    if not agg:
        return {}
    return {
        "batting_avg": _safe_float(agg.batting_avg),
        "k_pct": _safe_float(agg.k_pct),
        "bb_pct": _safe_float(agg.bb_pct),
        "avg_exit_velocity": _safe_float(agg.avg_exit_velocity),
        "avg_launch_angle": _safe_float(agg.avg_launch_angle),
        "hard_hit_pct": _safe_float(agg.hard_hit_pct),
        "barrel_pct": _safe_float(agg.barrel_pct),
    }


def build_hitter_profile(
    session: Session,
    player_id: int,
    season: int,
    split: str,
    team_fallback: Dict[str, Any],
    lineup_player: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Build one hitter profile for lineup-average simulation.

    Source priority:
    1. PlayerSplit selected split
    2. Opposite PlayerSplit
    3. BatterAggregate 90d
    4. team_fallback values
    """
    lineup_player = lineup_player or {}
    opposite_split = "vsL" if split == "vsR" else "vsR"

    selected = get_player_split(session, player_id, season, split)
    opposite = None if selected else get_player_split(session, player_id, season, opposite_split)
    split_source = selected or opposite

    split_data = _split_to_dict(split_source)
    agg_data = _batter_aggregate_to_dict(get_batter_aggregate(session, player_id, "90d"))

    profile_source = "team_fallback"
    if selected:
        profile_source = "player_split"
    elif opposite:
        profile_source = "opposite_player_split"
    elif agg_data:
        profile_source = "batter_aggregate"

    def pick(key: str) -> Any:
        if split_data.get(key) is not None:
            return split_data.get(key)
        if agg_data.get(key) is not None:
            return agg_data.get(key)
        return _fallback_value(team_fallback, key)

    batting_avg = pick("batting_avg")
    slugging_pct = pick("slugging_pct")
    iso = split_data.get("iso")
    if iso is None:
        iso = _compute_iso(batting_avg, slugging_pct, None)
    if iso is None:
        iso = _fallback_value(team_fallback, "iso")

    simulation_inputs = {
        "k_pct": pick("k_pct"),
        "bb_pct": pick("bb_pct"),
        "batting_avg": batting_avg,
        "on_base_pct": pick("on_base_pct"),
        "slugging_pct": slugging_pct,
        "iso": iso,
        "pa": pick("pa"),
        "hits": pick("hits"),
        "doubles": pick("doubles"),
        "triples": pick("triples"),
        "home_runs": pick("home_runs"),
        "walks": pick("walks"),
        "strikeouts": pick("strikeouts"),
        "avg_exit_velocity": agg_data.get("avg_exit_velocity"),
        "avg_launch_angle": agg_data.get("avg_launch_angle"),
        "hard_hit_pct": agg_data.get("hard_hit_pct"),
        "barrel_pct": agg_data.get("barrel_pct"),
    }

    usable_core_fields = [
        "k_pct",
        "bb_pct",
        "batting_avg",
        "slugging_pct",
        "iso",
    ]
    has_usable_profile = any(simulation_inputs.get(key) is not None for key in usable_core_fields)

    return {
        "batter_id": player_id,
        "name": lineup_player.get("name"),
        "batting_order": lineup_player.get("batting_order"),
        "lineup_slot": lineup_player.get("lineup_slot"),
        "position": lineup_player.get("position"),
        "split": split,
        "profile_source": profile_source,
        "has_player_split": bool(selected or opposite),
        "has_batter_aggregate": bool(agg_data),
        "used_opposite_split": bool(opposite and not selected),
        "has_usable_profile": has_usable_profile,
        "simulation_inputs": simulation_inputs,
    }


def _aggregate_hitter_profiles(
    profiles: List[Dict[str, Any]],
    team_id: int,
    season: int,
    split: str,
    team_fallback: Dict[str, Any],
    fallback_player_count: int,
) -> Dict[str, Any]:
    inputs = [profile.get("simulation_inputs") or {} for profile in profiles]

    def avg_key(key: str) -> Optional[float]:
        fallback = _safe_float(_fallback_value(team_fallback, key))
        values = [
            _safe_float(row.get(key)) if row.get(key) is not None else fallback
            for row in inputs
        ]
        return _avg(values)

    def sum_key(key: str) -> Optional[int]:
        return _sum_int([_safe_int(row.get(key)) for row in inputs])

    batting_avg = avg_key("batting_avg")
    slugging_pct = avg_key("slugging_pct")
    iso = avg_key("iso")
    if iso is None:
        iso = _compute_iso(batting_avg, slugging_pct, None)

    player_count_used = len(profiles)

    return {
        "source": "confirmed_lineup_player_splits",
        "team_id": team_id,
        "season": season,
        "split": split,
        "pa": sum_key("pa"),
        "hits": sum_key("hits"),
        "doubles": sum_key("doubles"),
        "triples": sum_key("triples"),
        "home_runs": sum_key("home_runs"),
        "walks": sum_key("walks"),
        "strikeouts": sum_key("strikeouts"),
        "batting_avg": batting_avg,
        "on_base_pct": avg_key("on_base_pct"),
        "slugging_pct": slugging_pct,
        "iso": iso,
        "stored_iso": None,
        "k_pct": avg_key("k_pct"),
        "bb_pct": avg_key("bb_pct"),
        "avg_exit_velocity": avg_key("avg_exit_velocity"),
        "avg_launch_angle": avg_key("avg_launch_angle"),
        "hard_hit_pct": avg_key("hard_hit_pct"),
        "barrel_pct": avg_key("barrel_pct"),
        "lineup_source": "mlb_boxscore_confirmed",
        "profile_granularity": "lineup_average",
        "player_count_used": player_count_used,
        "fallback_player_count": fallback_player_count,
        "real_player_profile_count": player_count_used - fallback_player_count,
        "minimum_required_players": MIN_USABLE_HITTERS,
        "unavailable_reason": None,
        "lineup": [
            {
                "batter_id": profile.get("batter_id"),
                "name": profile.get("name"),
                "batting_order": profile.get("batting_order"),
                "lineup_slot": profile.get("lineup_slot"),
                "position": profile.get("position"),
                "profile_source": profile.get("profile_source"),
                "has_player_split": profile.get("has_player_split"),
                "has_batter_aggregate": profile.get("has_batter_aggregate"),
                "used_opposite_split": profile.get("used_opposite_split"),
                "simulation_inputs": profile.get("simulation_inputs"),
            }
            for profile in profiles
        ],
        "sample_blend": {
            "type": "confirmed_lineup_player_split_average",
            "season": season,
            "split": split,
            "lineup_players": player_count_used,
            "fallback_players": fallback_player_count,
            "real_player_profiles": player_count_used - fallback_player_count,
        },
    }


def build_lineup_offense_inputs(
    session: Session,
    game_pk: int,
    side: str,
    team_id: int,
    season: int,
    split: str,
    team_fallback: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """
    Return lineup-average offense inputs when confirmed lineups are usable.

    Returns None when lineups are unavailable/incomplete so callers can preserve
    the existing team_splits fallback unchanged.
    """
    if not game_pk:
        return None

    lineups = fetch_boxscore_lineup(int(game_pk))
    lineup = lineups.get(side) or []

    if len(lineup) < MIN_USABLE_HITTERS:
        return None

    profiles: List[Dict[str, Any]] = []
    fallback_player_count = 0
    real_player_profile_count = 0

    for player in lineup:
        player_id = player.get("batter_id")
        if not player_id:
            fallback_player_count += 1
            continue

        profile = build_hitter_profile(
            session=session,
            player_id=int(player_id),
            season=season,
            split=split,
            team_fallback=team_fallback,
            lineup_player=player,
        )

        has_real_player_profile = bool(profile.get("has_player_split") or profile.get("has_batter_aggregate"))
        if has_real_player_profile:
            real_player_profile_count += 1
        else:
            fallback_player_count += 1

        if profile.get("has_usable_profile"):
            profiles.append(profile)

    # Do not activate the lineup layer when it is only repeating team_splits
    # for each hitter. In that case, preserve the existing team_splits fallback
    # exactly and avoid adding API/payload overhead without model signal.
    if real_player_profile_count < MIN_USABLE_HITTERS:
        return None

    if len(profiles) < MIN_USABLE_HITTERS:
        return None

    aggregate = _aggregate_hitter_profiles(
        profiles=profiles,
        team_id=team_id,
        season=season,
        split=split,
        team_fallback=team_fallback,
        fallback_player_count=fallback_player_count,
    )

    lineup_handedness_mix = None
    lineup_handedness_unavailable_reason = None

    try:
        target_date = dt.date(int(season), 12, 31)
        season_start = dt.date(int(season), 1, 1)

        hitter_ids_for_handedness = []
        for profile in profiles:
            player_id = profile.get("player_id") or profile.get("batter_id")
            if player_id is not None:
                try:
                    hitter_ids_for_handedness.append(int(player_id))
                except Exception:
                    continue

        if hitter_ids_for_handedness:
            lineup_handedness_mix = build_lineup_handedness_mix(
                session,
                hitter_ids_for_handedness,
                season_start,
                target_date,
            )
        else:
            lineup_handedness_unavailable_reason = "missing_hitter_ids"

    except Exception as exc:
        lineup_handedness_mix = None
        lineup_handedness_unavailable_reason = f"handedness_mix_error:{exc}"

    aggregate["lineup_handedness_mix"] = lineup_handedness_mix
    aggregate["lineup_handedness_mix_source"] = (
        lineup_handedness_mix or {}
    ).get("source") if lineup_handedness_mix else None
    aggregate["lineup_handedness_coverage_rate"] = (
        lineup_handedness_mix or {}
    ).get("coverage_rate") if lineup_handedness_mix else None
    aggregate["lineup_handedness_counts"] = (
        lineup_handedness_mix or {}
    ).get("counts") if lineup_handedness_mix else None
    aggregate["lineup_handedness_player_count"] = (
        lineup_handedness_mix or {}
    ).get("hitter_count") if lineup_handedness_mix else None
    aggregate["lineup_handedness_unavailable_reason"] = lineup_handedness_unavailable_reason

    return aggregate


__all__ = [
    "MIN_USABLE_HITTERS",
    "fetch_boxscore_lineup",
    "build_hitter_profile",
    "build_lineup_offense_inputs",
]
