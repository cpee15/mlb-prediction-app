"""
Aggregation utilities for rolling and seasonal MLB statistics.

This module defines helper functions to compute rolling‑window and
seasonal aggregate statistics for pitchers and batters from raw
Statcast event data.  These aggregations support the feature
engineering required for the prediction model, allowing us to
summarise a player's recent performance over multiple time windows
and distinguish between different seasons.

The functions operate on pandas DataFrames that contain at least the
following columns (matching the Statcast CSV schema【974644565369066†L204-L317】):

* ``game_date`` – the date of the game (as a datetime or string).
* ``pitcher_id`` / ``batter_id`` – MLBAM identifiers for the player.
* ``release_speed`` – pitch velocity in mph.
* ``release_spin_rate`` – spin rate in rpm.
* ``launch_speed`` – exit velocity of batted balls (may be NaN for non‑hit events).
* ``launch_angle`` – launch angle in degrees (may be NaN for non‑hit events).
* ``events`` – plate appearance result (e.g. ``strikeout``, ``walk``, ``single``, etc.).

These functions are written defensively: if a requested window has no
events for the player, the returned dictionary will be empty.  It is
assumed that the caller handles missing data appropriately (e.g. by
falling back to career averages or leaving fields blank).

Note: Real Statcast datasets are large; for efficiency, consider
filtering the DataFrame to include only the player of interest before
calling these functions.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Any
import pandas as pd  # type: ignore


def _prepare_dataframe(df: pd.DataFrame, date_column: str = "game_date") -> pd.DataFrame:
    """Ensure the DataFrame has a datetime index based on ``game_date``.

    A helper used internally by the rolling computations.  It
    coerces the date column to pandas datetime and sets it as the
    DataFrame index to simplify slicing by date ranges.
    """
    if not pd.api.types.is_datetime64_any_dtype(df[date_column]):
        df = df.copy()
        df[date_column] = pd.to_datetime(df[date_column])
    return df.set_index(date_column).sort_index()


def compute_pitcher_rolling_metrics(
    events_df: pd.DataFrame,
    player_id: int,
    as_of_date: str,
    windows: Optional[List[int]] = None,
) -> Dict[int, Dict[str, Optional[float]]]:
    """Compute rolling aggregates for a pitcher over multiple time windows.

    Parameters
    ----------
    events_df : DataFrame
        Raw Statcast events containing pitches thrown by many pitchers.
    player_id : int
        MLBAM identifier of the pitcher to summarise.
    as_of_date : str
        End date for the rolling windows (inclusive) in ``YYYY-MM-DD`` format.
    windows : list of int, optional
        Rolling windows, in days, over which to compute aggregates.  If
        ``None``, uses ``[90, 180, 270, 365]`` days.

    Returns
    -------
    dict
        A mapping from window size (in days) to a dictionary of
        aggregated metrics.  Each metric will be ``None`` if no
        events are present in that window.
    """
    if windows is None:
        windows = [90, 180, 270, 365]
    # Ensure DataFrame has proper index
    df = _prepare_dataframe(events_df)
    if "pitcher_id" not in df.columns:
        raise ValueError("events_df must contain a 'pitcher_id' column")
    # Filter to the pitcher of interest
    df = df[df["pitcher_id"] == player_id]
    if df.empty:
        return {w: {} for w in windows}
    as_of = pd.to_datetime(as_of_date)
    metrics: Dict[int, Dict[str, Optional[float]]] = {}
    for window in windows:
        start = as_of - pd.Timedelta(days=window)
        subset = df.loc[start:as_of]
        if subset.empty:
            metrics[window] = {}
            continue
        total_pitches = len(subset)
        strikeouts = (subset["events"] == "strikeout").sum()
        walks = (subset["events"] == "walk").sum()
        # Hard‑hit is defined as batted balls with exit velocity >= 95 mph
        hard_hits = (
            pd.to_numeric(subset.get("launch_speed", pd.Series([], dtype=float)), errors="coerce") >= 95
        ).sum()
        metrics[window] = {
            "AvgVelo": float(subset["release_speed"].mean()) if "release_speed" in subset else None,
            "AvgSpin": float(subset["release_spin_rate"].mean()) if "release_spin_rate" in subset else None,
            "HardHit%": (hard_hits / total_pitches) * 100 if total_pitches else None,
            "K%": (strikeouts / total_pitches) * 100 if total_pitches else None,
            "BB%": (walks / total_pitches) * 100 if total_pitches else None,
        }
    return metrics


def compute_batter_rolling_metrics(
    events_df: pd.DataFrame,
    player_id: int,
    as_of_date: str,
    windows: Optional[List[int]] = None,
) -> Dict[int, Dict[str, Optional[float]]]:
    """Compute rolling aggregates for a batter over multiple time windows.

    Parameters
    ----------
    events_df : DataFrame
        Raw Statcast events containing plate appearances for many batters.
    player_id : int
        MLBAM identifier of the batter to summarise.
    as_of_date : str
        End date for the rolling windows (inclusive) in ``YYYY-MM-DD`` format.
    windows : list of int, optional
        Rolling windows, in days, over which to compute aggregates.  If
        ``None``, uses ``[90, 180, 270, 365]`` days.

    Returns
    -------
    dict
        A mapping from window size (in days) to a dictionary of
        aggregated metrics.  Each metric will be ``None`` if no
        events are present in that window.
    """
    if windows is None:
        windows = [90, 180, 270, 365]
    df = _prepare_dataframe(events_df)
    if "batter_id" not in df.columns:
        raise ValueError("events_df must contain a 'batter_id' column")
    df = df[df["batter_id"] == player_id]
    if df.empty:
        return {w: {} for w in windows}
    as_of = pd.to_datetime(as_of_date)
    metrics: Dict[int, Dict[str, Optional[float]]] = {}
    for window in windows:
        start = as_of - pd.Timedelta(days=window)
        subset = df.loc[start:as_of]
        if subset.empty:
            metrics[window] = {}
            continue
        total_pa = len(subset)
        hits = subset["events"].isin(["single", "double", "triple", "home_run"]).sum()
        strikeouts = (subset["events"] == "strikeout").sum()
        walks = (subset["events"] == "walk").sum()
        hard_hits = (
            pd.to_numeric(subset.get("launch_speed", pd.Series([], dtype=float)), errors="coerce") >= 95
        ).sum()
        at_bats = subset["events"].isin([
            "single",
            "double",
            "triple",
            "home_run",
            "field_out",
            "force_out",
            "grounded_into_double_play",
            "strikeout",
        ]).sum()
        metrics[window] = {
            "AvgEV": float(subset["launch_speed"].mean()) if "launch_speed" in subset else None,
            "AvgLA": float(subset.get("launch_angle", pd.Series([], dtype=float)).mean()) if "launch_angle" in subset else None,
            "HardHit%": (hard_hits / total_pa) * 100 if total_pa else None,
            "AVG": (hits / at_bats) if at_bats else None,
            "K%": (strikeouts / total_pa) * 100 if total_pa else None,
            "BB%": (walks / total_pa) * 100 if total_pa else None,
        }
    return metrics


def compute_seasonal_metrics(
    events_df: pd.DataFrame,
    player_id: int,
    season_year: int,
    player_type: str = "pitcher",
) -> Dict[str, Optional[float]]:
    """Aggregate a player's statistics for a specific season.

    This helper mirrors the rolling functions but filters the DataFrame
    by calendar year.  It can be used to compute season‑level features
    such as average velocity and exit velocity, strikeout and walk
    rates, and hard‑hit percentage.

    Parameters
    ----------
    events_df : DataFrame
        Raw Statcast events.
    player_id : int
        MLBAM identifier of the player.
    season_year : int
        Year (e.g. 2025).
    player_type : {"pitcher", "batter"}
        Whether to treat ``player_id`` as a pitcher or batter.

    Returns
    -------
    dict
        Aggregated metrics for the season.  Returns empty dict if
        there are no events for the player in the given year.
    """
    df = _prepare_dataframe(events_df)
    start = pd.Timestamp(season_year, 1, 1)
    end = pd.Timestamp(season_year, 12, 31)
    id_col = "pitcher_id" if player_type == "pitcher" else "batter_id"
    if id_col not in df.columns:
        raise ValueError(f"events_df must contain a '{id_col}' column")
    df = df[(df[id_col] == player_id) & (df.index >= start) & (df.index <= end)]
    if df.empty:
        return {}
    total_rows = len(df)
    if player_type == "pitcher":
        strikeouts = (df["events"] == "strikeout").sum()
        walks = (df["events"] == "walk").sum()
        hard_hits = (
            pd.to_numeric(df.get("launch_speed", pd.Series([], dtype=float)), errors="coerce") >= 95
        ).sum()
        return {
            "AvgVelo": float(df["release_speed"].mean()) if "release_speed" in df else None,
            "AvgSpin": float(df["release_spin_rate"].mean()) if "release_spin_rate" in df else None,
            "HardHit%": (hard_hits / total_rows) * 100 if total_rows else None,
            "K%": (strikeouts / total_rows) * 100 if total_rows else None,
            "BB%": (walks / total_rows) * 100 if total_rows else None,
        }
    else:
        hits = df["events"].isin(["single", "double", "triple", "home_run"]).sum()
        strikeouts = (df["events"] == "strikeout").sum()
        walks = (df["events"] == "walk").sum()
        hard_hits = (
            pd.to_numeric(df.get("launch_speed", pd.Series([], dtype=float)), errors="coerce") >= 95
        ).sum()
        at_bats = df["events"].isin([
            "single",
            "double",
            "triple",
            "home_run",
            "field_out",
            "force_out",
            "grounded_into_double_play",
            "strikeout",
        ]).sum()
        return {
            "AvgEV": float(df["launch_speed"].mean()) if "launch_speed" in df else None,
            "AvgLA": float(df.get("launch_angle", pd.Series([], dtype=float)).mean()) if "launch_angle" in df else None,
            "HardHit%": (hard_hits / total_rows) * 100 if total_rows else None,
            "AVG": (hits / at_bats) if at_bats else None,
            "K%": (strikeouts / total_rows) * 100 if total_rows else None,
            "BB%": (walks / total_rows) * 100 if total_rows else None,
        }
