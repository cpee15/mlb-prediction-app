"""
Matchup generation — assembles game-level feature vectors from the DB
and computes win probabilities via the scoring engine.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Dict, List, Optional

from sqlalchemy.orm import Session

from .etl import fetch_schedule
from .db_utils import (
    get_pitcher_aggregate,
    get_batter_aggregate,
    get_pitch_arsenal,
    get_player_split,
    get_team_split,
)
from .scoring import compute_win_probability
from .lineup_profile import build_lineup_offense_inputs, build_lineup_offense_diagnostics

log = logging.getLogger(__name__)


def _format_pitcher_features(session: Session, pitcher_id: int) -> Dict[str, Optional[float]]:
    agg = get_pitcher_aggregate(session, pitcher_id, "90d")
    if not agg:
        return {k: None for k in [
            "avg_velocity", "avg_spin_rate", "hard_hit_pct", "k_pct", "bb_pct",
            "xwoba", "xba", "avg_horiz_break", "avg_vert_break",
            "avg_release_pos_x", "avg_release_pos_z", "avg_release_extension",
        ]}
    return {
        "avg_velocity": agg.avg_velocity,
        "avg_spin_rate": agg.avg_spin_rate,
        "hard_hit_pct": agg.hard_hit_pct,
        "k_pct": agg.k_pct,
        "bb_pct": agg.bb_pct,
        "xwoba": agg.xwoba,
        "xba": agg.xba,
        "avg_horiz_break": agg.avg_horiz_break,
        "avg_vert_break": agg.avg_vert_break,
        "avg_release_pos_x": agg.avg_release_pos_x,
        "avg_release_pos_z": agg.avg_release_pos_z,
        "avg_release_extension": agg.avg_release_extension,
    }


def _format_pitch_arsenal(session: Session, pitcher_id: int, season: int) -> Dict:
    records = get_pitch_arsenal(session, pitcher_id, season)
    return {
        rec.pitch_type or "": {
            "usage_pct": rec.usage_pct,
            "whiff_pct": rec.whiff_pct,
            "strikeout_pct": rec.strikeout_pct,
            "rv_per_100": rec.rv_per_100,
            "xwoba": rec.xwoba,
            "hard_hit_pct": rec.hard_hit_pct,
        }
        for rec in records
    }


def _format_batter_features(session: Session, batter_id: int) -> Dict[str, Optional[float]]:
    agg = get_batter_aggregate(session, batter_id, "90d")
    if not agg:
        return {k: None for k in [
            "avg_exit_velocity", "avg_launch_angle", "hard_hit_pct",
            "barrel_pct", "k_pct", "bb_pct", "batting_avg",
        ]}
    return {
        "avg_exit_velocity": agg.avg_exit_velocity,
        "avg_launch_angle": agg.avg_launch_angle,
        "hard_hit_pct": agg.hard_hit_pct,
        "barrel_pct": agg.barrel_pct,
        "k_pct": agg.k_pct,
        "bb_pct": agg.bb_pct,
        "batting_avg": agg.batting_avg,
    }


def _with_lineup_fallback_diagnostics(
    offense_inputs: Dict,
    diagnostics: Optional[Dict],
) -> Dict:
    updated = dict(offense_inputs or {})
    diagnostics = diagnostics or {}

    for key in (
        "lineup_fallback_reason",
        "lineup_fallback_stage",
        "lineup_fetch_attempted",
        "lineup_fetch_succeeded",
        "lineup_side_found",
        "starting_lineup_count",
        "usable_hitter_profile_count",
        "real_player_profile_count",
        "fallback_player_count",
        "min_usable_hitters",
        "confirmed_lineup_inputs_would_activate",
    ):
        if key in diagnostics:
            updated[key] = diagnostics.get(key)

    return updated


def _with_lineup_exception_diagnostics(
    offense_inputs: Dict,
    exc: Exception,
) -> Dict:
    message = str(exc)
    exc_type = exc.__class__.__name__
    lowered = message.lower()

    updated = dict(offense_inputs or {})
    updated.update({
        "lineup_fallback_reason": "confirmed_lineup_fetch_or_build_error",
        "lineup_fallback_stage": "exception",
        "lineup_fetch_attempted": True,
        "lineup_fetch_succeeded": False,
        "lineup_fetch_error_type": exc_type,
        "lineup_fetch_error_message": message,
        "lineup_fetch_timeout": (
            "timeout" in lowered
            or "timed out" in lowered
            or exc_type in {"ReadTimeout", "ReadTimeoutError", "TimeoutError"}
        ),
    })
    return updated



def _format_team_offense_inputs(session: Session, team_id: int, season: int, split: str = "vsR") -> Dict[str, Optional[float]]:
    """
    Team-level offense inputs for game simulation.

    This is intentionally conservative: use team_splits when lineup-level inputs
    are not yet available. Compute ISO from SLG - AVG because the stored iso
    field may contain OPS in older ETL rows.
    """
    row = get_team_split(session, team_id, season, split) or get_team_split(
        session,
        team_id,
        season,
        "vsL" if split == "vsR" else "vsR",
    )

    if not row:
        return {
            "source": "missing_team_splits",
            "team_id": team_id,
            "season": season,
            "split": split,
            "pa": None,
            "hits": None,
            "doubles": None,
            "triples": None,
            "home_runs": None,
            "walks": None,
            "strikeouts": None,
            "batting_avg": None,
            "on_base_pct": None,
            "slugging_pct": None,
            "iso": None,
            "k_pct": None,
            "bb_pct": None,
        }

    batting_avg = row.batting_avg
    slugging_pct = row.slugging_pct
    computed_iso = None
    if batting_avg is not None and slugging_pct is not None:
        computed_iso = round(max(float(slugging_pct) - float(batting_avg), 0.0), 3)

    return {
        "source": "team_splits",
        "team_id": team_id,
        "season": season,
        "split": row.split,
        "pa": row.pa,
        "hits": row.hits,
        "doubles": row.doubles,
        "triples": row.triples,
        "home_runs": row.home_runs,
        "walks": row.walks,
        "strikeouts": row.strikeouts,
        "batting_avg": batting_avg,
        "on_base_pct": row.on_base_pct,
        "slugging_pct": slugging_pct,
        "iso": computed_iso,
        "stored_iso": row.iso,
        "k_pct": row.k_pct,
        "bb_pct": row.bb_pct,
        "lineup_source": "team_splits_fallback_not_confirmed_lineup",
        "sample_blend": {
            "type": "team_split",
            "season": season,
            "split": row.split,
        },
    }


def generate_matchups_for_date(session: Session, date_str: str) -> List[Dict]:
    try:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        raise ValueError("date_str must be in YYYY-MM-DD format")

    schedule = fetch_schedule(date_str)
    season = date_obj.year
    matchups = []

    for game in schedule:
        home_team = game.get("home", {}).get("team", {}).get("id")
        away_team = game.get("away", {}).get("team", {}).get("id")
        home_pitcher_id = game.get("home", {}).get("probablePitcher", {}).get("id")
        away_pitcher_id = game.get("away", {}).get("probablePitcher", {}).get("id")

        home_record = game.get("home", {}).get("leagueRecord", {})
        away_record = game.get("away", {}).get("leagueRecord", {})

        base_matchup = {
            "game_date": date_str,
            "game_pk": game.get("_game_pk"),
            "game_time": game.get("_game_date"),
            "venue": game.get("_venue"),
            "status": game.get("_status"),
            "weather": game.get("_weather"),
            "home_team_id": home_team,
            "away_team_id": away_team,
            "home_team_name": game.get("home", {}).get("team", {}).get("name"),
            "away_team_name": game.get("away", {}).get("team", {}).get("name"),
            "home_team_record": f"{home_record.get('wins', 0)}-{home_record.get('losses', 0)}" if home_record else None,
            "away_team_record": f"{away_record.get('wins', 0)}-{away_record.get('losses', 0)}" if away_record else None,
            "home_pitcher_id": home_pitcher_id,
            "away_pitcher_id": away_pitcher_id,
            "home_pitcher_name": game.get("home", {}).get("probablePitcher", {}).get("fullName"),
            "away_pitcher_name": game.get("away", {}).get("probablePitcher", {}).get("fullName"),
            "home_win_prob": None,
            "away_win_prob": None,
            "home_pitcher_features": {},
            "away_pitcher_features": {},
            "home_pitch_arsenal": {},
            "away_pitch_arsenal": {},
            "home_offense_inputs": {},
            "away_offense_inputs": {},
        }

        if not all([home_team, away_team, home_pitcher_id, away_pitcher_id]):
            # Still include games without probable pitchers — just no win probs
            matchups.append(base_matchup)
            continue

        try:
            home_win_prob, away_win_prob = compute_win_probability(
                session,
                home_pitcher_id=home_pitcher_id,
                away_pitcher_id=away_pitcher_id,
                home_team_id=home_team,
                away_team_id=away_team,
                season=season,
            )
            base_matchup["home_win_prob"] = home_win_prob
            base_matchup["away_win_prob"] = away_win_prob
        except Exception:
            log.exception(
                "Win probability failed for game_pk=%s date=%s home_pitcher_id=%s away_pitcher_id=%s",
                game.get("_game_pk"),
                date_str,
                home_pitcher_id,
                away_pitcher_id,
            )

        try:
            base_matchup["home_pitcher_features"] = _format_pitcher_features(session, home_pitcher_id)
            base_matchup["away_pitcher_features"] = _format_pitcher_features(session, away_pitcher_id)
        except Exception:
            log.exception(
                "Pitcher feature formatting failed for game_pk=%s date=%s home_pitcher_id=%s away_pitcher_id=%s",
                game.get("_game_pk"),
                date_str,
                home_pitcher_id,
                away_pitcher_id,
            )

        try:
            base_matchup["home_pitch_arsenal"] = _format_pitch_arsenal(session, home_pitcher_id, season)
            base_matchup["away_pitch_arsenal"] = _format_pitch_arsenal(session, away_pitcher_id, season)
        except Exception:
            log.exception(
                "Pitch arsenal formatting failed for game_pk=%s date=%s home_pitcher_id=%s away_pitcher_id=%s season=%s",
                game.get("_game_pk"),
                date_str,
                home_pitcher_id,
                away_pitcher_id,
                season,
            )

        try:
            home_split = "vsL" if game.get("away", {}).get("probablePitcher", {}).get("pitchHand", {}).get("code") == "L" else "vsR"
            away_split = "vsL" if game.get("home", {}).get("probablePitcher", {}).get("pitchHand", {}).get("code") == "L" else "vsR"

            home_team_fallback = _format_team_offense_inputs(session, home_team, season, home_split)
            away_team_fallback = _format_team_offense_inputs(session, away_team, season, away_split)

            base_matchup["home_offense_inputs"] = home_team_fallback
            base_matchup["away_offense_inputs"] = away_team_fallback

            try:
                home_lineup_diagnostics = build_lineup_offense_diagnostics(
                    session=session,
                    game_pk=game.get("_game_pk"),
                    side="home",
                    team_id=home_team,
                    season=season,
                    split=home_split,
                    team_fallback=home_team_fallback,
                )
                home_lineup_inputs = build_lineup_offense_inputs(
                    session=session,
                    game_pk=game.get("_game_pk"),
                    side="home",
                    team_id=home_team,
                    season=season,
                    split=home_split,
                    team_fallback=home_team_fallback,
                )
                if home_lineup_inputs:
                    base_matchup["home_offense_inputs"] = home_lineup_inputs
                else:
                    base_matchup["home_offense_inputs"] = _with_lineup_fallback_diagnostics(
                        base_matchup["home_offense_inputs"],
                        home_lineup_diagnostics,
                    )
            except Exception as exc:
                base_matchup["home_offense_inputs"] = _with_lineup_exception_diagnostics(
                    base_matchup["home_offense_inputs"],
                    exc,
                )
                log.exception(
                    "Confirmed home lineup offense input failed; using team_splits fallback for game_pk=%s date=%s home_team_id=%s",
                    game.get("_game_pk"),
                    date_str,
                    home_team,
                )

            try:
                away_lineup_diagnostics = build_lineup_offense_diagnostics(
                    session=session,
                    game_pk=game.get("_game_pk"),
                    side="away",
                    team_id=away_team,
                    season=season,
                    split=away_split,
                    team_fallback=away_team_fallback,
                )
                away_lineup_inputs = build_lineup_offense_inputs(
                    session=session,
                    game_pk=game.get("_game_pk"),
                    side="away",
                    team_id=away_team,
                    season=season,
                    split=away_split,
                    team_fallback=away_team_fallback,
                )
                if away_lineup_inputs:
                    base_matchup["away_offense_inputs"] = away_lineup_inputs
                else:
                    base_matchup["away_offense_inputs"] = _with_lineup_fallback_diagnostics(
                        base_matchup["away_offense_inputs"],
                        away_lineup_diagnostics,
                    )
            except Exception as exc:
                base_matchup["away_offense_inputs"] = _with_lineup_exception_diagnostics(
                    base_matchup["away_offense_inputs"],
                    exc,
                )
                log.exception(
                    "Confirmed away lineup offense input failed; using team_splits fallback for game_pk=%s date=%s away_team_id=%s",
                    game.get("_game_pk"),
                    date_str,
                    away_team,
                )
        except Exception:
            log.exception(
                "Team offense input formatting failed for game_pk=%s date=%s home_team_id=%s away_team_id=%s season=%s",
                game.get("_game_pk"),
                date_str,
                home_team,
                away_team,
                season,
            )

        matchups.append(base_matchup)

    return matchups


__all__ = ["generate_matchups_for_date"]
