"""FastAPI application exposing MLB matchup data.

This module defines a simple FastAPI server that surfaces the
prediction pipeline built in this project.  The API exposes
endpoints to fetch daily matchups as well as individual pitcher
and batter statistics.  Real Statcast data and database queries
will eventually feed these endpoints, but the current
implementation relies on stub functions that should be replaced
with calls into your ETL pipeline and aggregation layers.

Example usage::

    uvicorn mlb_app.app:app --reload
    # Then open http://127.0.0.1:8000/matchups?date=2026-04-15

Note: FastAPI is not installed in this environment, so this
module may not run here.  It is provided for completeness so
that you can deploy it in a proper Python environment with
FastAPI installed.
"""

from __future__ import annotations

from typing import List, Optional

import datetime

try:
    from fastapi import FastAPI, HTTPException
    from pydantic import BaseModel
except ImportError:
    # Provide stubs if FastAPI is not installed.  This prevents
    # import errors in environments where FastAPI is unavailable.
    FastAPI = None  # type: ignore
    HTTPException = Exception  # type: ignore
    BaseModel = object  # type: ignore

import os

from .database import get_engine, get_session
from .matchup_generator import generate_matchups_for_date
from .db_utils import (
    get_pitcher_aggregate,
    get_batter_aggregate,
    get_pitch_arsenal,
    get_player_split,
    get_team_split,
)


class Matchup(BaseModel):  # type: ignore[misc]
    """Pydantic model representing a single game matchup.

    This model is intentionally simple and loosely typed.  It
    mirrors the dictionary structure returned by
    ``generate_daily_matchups`` and will accept arbitrary keys.
    Future iterations should define explicit fields based on the
    final feature set.
    """

    class Config:
        arbitrary_types_allowed = True


def create_app() -> Optional[FastAPI]:
    """Create and configure a FastAPI application.

    Returns ``None`` if FastAPI is not available.  In a proper
    deployment environment with FastAPI installed, this will
    return a fully configured application instance.
    """
    if FastAPI is None:
        return None
    app = FastAPI(title="MLB Prediction API", version="0.1.0")

    @app.get("/matchups", response_model=List[Matchup])
    def list_matchups(date: Optional[str] = None):
        """Return matchups for a given date.

        This endpoint uses the database-backed matchup generator.  If
        no date is provided, the current date is assumed.  A new
        database session is opened for each request.  Any errors
        retrieving data from the database will result in a 500
        response.
        """
        # Default to today's date if none provided
        if not date:
            date = datetime.date.today().isoformat()
        # Acquire DB engine and session
        db_url = os.getenv("DATABASE_URL", "sqlite:///mlb.db")
        engine = get_engine(db_url)
        SessionLocal = get_session(engine)
        with SessionLocal() as session:
            try:
                matchups = generate_matchups_for_date(session, date)
            except Exception as exc:  # pragma: no cover
                raise HTTPException(status_code=500, detail=str(exc))
        return [Matchup(**m) for m in matchups]

    @app.get("/pitcher/{player_id}")
    def get_pitcher(player_id: int):
        """Return aggregated statistics for a pitcher.

        Queries the database for the latest 90‑day aggregate and
        seasonal pitch‑arsenal metrics for the given pitcher.  If no
        records exist, returns a 404 error.
        """
        db_url = os.getenv("DATABASE_URL", "sqlite:///mlb.db")
        engine = get_engine(db_url)
        SessionLocal = get_session(engine)
        with SessionLocal() as session:
            agg = get_pitcher_aggregate(session, player_id, "90d")
            season = datetime.date.today().year
            arsenal = get_pitch_arsenal(session, player_id, season)
            if not agg and not arsenal:
                raise HTTPException(
                    status_code=404,
                    detail=f"No aggregate or arsenal data found for pitcher {player_id}",
                )
            return {
                "player_id": player_id,
                "aggregate": agg.__dict__ if agg else None,
                "arsenal": [
                    {
                        "pitch_type": rec.pitch_type,
                        "usage_pct": rec.usage_pct,
                        "whiff_pct": rec.whiff_pct,
                        "strikeout_pct": rec.strikeout_pct,
                        "rv_per_100": rec.rv_per_100,
                        "xwoba": rec.xwoba,
                        "hard_hit_pct": rec.hard_hit_pct,
                    }
                    for rec in arsenal
                ],
            }

    @app.get("/batter/{player_id}")
    def get_batter(player_id: int):
        """Return aggregated statistics for a batter.

        Queries the database for the latest 90‑day aggregate and
        platoon splits for the given batter.  If no records exist,
        returns a 404 error.
        """
        db_url = os.getenv("DATABASE_URL", "sqlite:///mlb.db")
        engine = get_engine(db_url)
        SessionLocal = get_session(engine)
        with SessionLocal() as session:
            agg = get_batter_aggregate(session, player_id, "90d")
            season = datetime.date.today().year
            # Retrieve splits vs L and R
            split_L = get_player_split(session, player_id, season, "vsL")
            split_R = get_player_split(session, player_id, season, "vsR")
            if not agg and not split_L and not split_R:
                raise HTTPException(
                    status_code=404,
                    detail=f"No aggregate or split data found for batter {player_id}",
                )
            return {
                "player_id": player_id,
                "aggregate": agg.__dict__ if agg else None,
                "splits": {
                    "vsL": split_L.__dict__ if split_L else None,
                    "vsR": split_R.__dict__ if split_R else None,
                },
            }

    return app


# Instantiate the FastAPI app when this module is imported.  This
# allows tools like uvicorn to discover ``app`` automatically.
app = create_app()
