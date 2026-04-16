"""
Matchup scoring engine.

Core concept: for each pitch type in the pitcher's arsenal, weight their
effectiveness (whiff%, K%, RV/100, xwOBA) against the batter's performance
vs that handedness split. Combine into a composite advantage score and
convert to win probability using a logistic transform.

The formula is intentionally transparent so you can tune weights as more
data accumulates.
"""

from __future__ import annotations

import math
from typing import Dict, List, Optional, Tuple

from sqlalchemy.orm import Session

from .db_utils import (
    get_pitcher_aggregate,
    get_batter_aggregate,
    get_pitch_arsenal,
    get_player_split,
    get_team_split,
)


# ---------------------------------------------------------------------------
# Weight configuration (tune these)
# ---------------------------------------------------------------------------

# Pitcher aggregate weights (higher = better pitcher performance)
PITCHER_WEIGHTS = {
    "k_pct": 2.0,
    "bb_pct": -1.5,       # walks hurt
    "hard_hit_pct": -1.5,
    "xwoba": -2.0,         # lower xwOBA against = better
    "avg_velocity": 0.05,  # small bonus for velo
}

# Batter aggregate weights (higher = better batter performance vs pitcher)
BATTER_WEIGHTS = {
    "avg_exit_velocity": 0.03,
    "hard_hit_pct": 1.5,
    "barrel_pct": 2.0,
    "k_pct": -1.5,
    "bb_pct": 0.5,
    "batting_avg": 2.0,
}

# Arsenal matchup weights
ARSENAL_WEIGHTS = {
    "whiff_pct": 1.5,
    "strikeout_pct": 1.5,
    "rv_per_100": -1.0,   # negative RV/100 = good for pitcher
    "xwoba": -2.0,
}

# Home-field advantage logit bump
HOME_FIELD_LOGIT = 0.12


# ---------------------------------------------------------------------------
# Stat normalization baselines (league average 2025 approximations)
# ---------------------------------------------------------------------------

PITCHER_BASELINE = {
    "k_pct": 0.225,
    "bb_pct": 0.085,
    "hard_hit_pct": 0.37,
    "xwoba": 0.315,
    "avg_velocity": 93.5,
}

BATTER_BASELINE = {
    "avg_exit_velocity": 88.5,
    "hard_hit_pct": 0.37,
    "barrel_pct": 0.075,
    "k_pct": 0.225,
    "bb_pct": 0.085,
    "batting_avg": 0.248,
}

ARSENAL_BASELINE = {
    "whiff_pct": 0.25,
    "strikeout_pct": 0.225,
    "rv_per_100": 0.0,
    "xwoba": 0.315,
}


# ---------------------------------------------------------------------------
# Core math
# ---------------------------------------------------------------------------

def _normalize(value: Optional[float], baseline: float, scale: float = 1.0) -> float:
    """Return (value - baseline) / scale, or 0 if value is None."""
    if value is None:
        return 0.0
    return (value - baseline) / scale


def _logistic(x: float) -> float:
    """Sigmoid function mapping any real to (0, 1)."""
    return 1.0 / (1.0 + math.exp(-x))


def _pitcher_advantage(agg) -> float:
    """Compute pitcher quality score vs league average."""
    if agg is None:
        return 0.0
    score = 0.0
    for field, weight in PITCHER_WEIGHTS.items():
        val = getattr(agg, field, None)
        baseline = PITCHER_BASELINE.get(field, 0.0)
        score += weight * _normalize(val, baseline)
    return score


def _batter_advantage(agg) -> float:
    """Compute batter quality score vs league average."""
    if agg is None:
        return 0.0
    score = 0.0
    for field, weight in BATTER_WEIGHTS.items():
        val = getattr(agg, field, None)
        baseline = BATTER_BASELINE.get(field, 0.0)
        score += weight * _normalize(val, baseline)
    return score


def _arsenal_vs_batter(arsenal: List, batter_split) -> float:
    """
    For each pitch in arsenal, score pitcher effectiveness weighted by usage%.
    batter_split is a PlayerSplit or TeamSplit ORM object (or None).
    """
    if not arsenal:
        return 0.0

    # If we have batter split, use their OBP as a proxy for vulnerability
    batter_obp = None
    if batter_split:
        batter_obp = getattr(batter_split, "on_base_pct", None)

    score = 0.0
    for pitch in arsenal:
        usage = pitch.usage_pct or 0.0
        pitch_score = 0.0
        for field, weight in ARSENAL_WEIGHTS.items():
            val = getattr(pitch, field, None)
            baseline = ARSENAL_BASELINE.get(field, 0.0)
            pitch_score += weight * _normalize(val, baseline)
        score += usage * pitch_score

    # Adjust by batter OBP vs baseline (.320 league avg OBP)
    if batter_obp is not None:
        score -= (batter_obp - 0.320) * 2.0

    return score


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def score_pitcher_vs_lineup(
    session: Session,
    pitcher_id: int,
    opposing_team_id: int,
    season: int,
    pitcher_throws: str = "R",
) -> float:
    """
    Compute a raw advantage score for a pitcher facing an opposing lineup.
    Positive = pitcher advantage, negative = batter advantage.
    """
    agg = get_pitcher_aggregate(session, pitcher_id, "90d")
    arsenal = get_pitch_arsenal(session, pitcher_id, season)

    # Determine relevant split for opposing batters
    split_key = "vsR" if pitcher_throws == "R" else "vsL"
    team_split = get_team_split(session, opposing_team_id, season, split_key)

    pitcher_score = _pitcher_advantage(agg)
    arsenal_score = _arsenal_vs_batter(arsenal, team_split)

    return pitcher_score + arsenal_score


def compute_win_probability(
    session: Session,
    home_pitcher_id: int,
    away_pitcher_id: int,
    home_team_id: int,
    away_team_id: int,
    season: int,
    home_pitcher_throws: str = "R",
    away_pitcher_throws: str = "R",
) -> Tuple[float, float]:
    """
    Return (home_win_prob, away_win_prob) for a game matchup.

    Uses pitcher quality, arsenal effectiveness vs opposing lineup, and
    home field advantage.
    """
    home_pitcher_score = score_pitcher_vs_lineup(
        session, home_pitcher_id, away_team_id, season, home_pitcher_throws
    )
    away_pitcher_score = score_pitcher_vs_lineup(
        session, away_pitcher_id, home_team_id, season, away_pitcher_throws
    )

    # Net advantage: home pitcher better = positive logit
    net_logit = (home_pitcher_score - away_pitcher_score) + HOME_FIELD_LOGIT

    home_win_prob = _logistic(net_logit)
    away_win_prob = 1.0 - home_win_prob

    return round(home_win_prob, 4), round(away_win_prob, 4)


def score_individual_matchup(
    session: Session,
    pitcher_id: int,
    batter_id: int,
    season: int,
    pitcher_throws: str = "R",
) -> Dict[str, float]:
    """
    Score a specific pitcher vs batter matchup.
    Returns a dict with pitcher_advantage, batter_advantage, and net_score.
    """
    pitcher_agg = get_pitcher_aggregate(session, pitcher_id, "90d")
    batter_agg = get_batter_aggregate(session, batter_id, "90d")
    arsenal = get_pitch_arsenal(session, pitcher_id, season)

    split_key = "vsR" if pitcher_throws == "R" else "vsL"
    batter_split = get_player_split(session, batter_id, season, split_key)

    p_score = _pitcher_advantage(pitcher_agg)
    b_score = _batter_advantage(batter_agg)
    a_score = _arsenal_vs_batter(arsenal, batter_split)

    net = (p_score + a_score) - b_score

    return {
        "pitcher_advantage": round(p_score + a_score, 4),
        "batter_advantage": round(b_score, 4),
        "net_score": round(net, 4),
        "pitcher_win_prob": round(_logistic(net), 4),
    }
