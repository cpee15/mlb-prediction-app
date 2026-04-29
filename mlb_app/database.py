"""
Database models and utilities for the MLB prediction app.

This module defines the SQLAlchemy ORM models used to store raw Statcast
events, aggregated pitch-arsenal statistics, platoon splits, rolling/seasonal
metrics and game-level matchups.  It also provides helper functions to
instantiate a database engine and session maker based on a connection URL.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Optional

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    String,
    create_engine,
    Index,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session

Base = declarative_base()


class StatcastEvent(Base):
    __tablename__ = "statcast_events"

    id: int = Column(Integer, primary_key=True, autoincrement=True)
    game_date: date = Column(Date, nullable=False, index=True)
    pitcher_id: int = Column(Integer, nullable=False, index=True)
    batter_id: int = Column(Integer, nullable=False, index=True)
    pitch_type: Optional[str] = Column(String(5), nullable=True)
    release_speed: Optional[float] = Column(Float, nullable=True)
    release_spin_rate: Optional[float] = Column(Float, nullable=True)
    pfx_x: Optional[float] = Column(Float, nullable=True)
    pfx_z: Optional[float] = Column(Float, nullable=True)
    plate_x: Optional[float] = Column(Float, nullable=True)
    plate_z: Optional[float] = Column(Float, nullable=True)
    balls: Optional[int] = Column(Integer, nullable=True)
    strikes: Optional[int] = Column(Integer, nullable=True)
    events: Optional[str] = Column(String(50), nullable=True)
    launch_speed: Optional[float] = Column(Float, nullable=True)
    launch_angle: Optional[float] = Column(Float, nullable=True)
    stand: Optional[str] = Column(String(1), nullable=True)
    p_throws: Optional[str] = Column(String(1), nullable=True)

    __table_args__ = (
        Index("ix_statcast_events_date_pitcher", "game_date", "pitcher_id"),
        Index("ix_statcast_events_date_batter", "game_date", "batter_id"),
    )


class BatterPitchTypeMatchup(Base):
    """Restored hitter-centered hittingMatchups aggregate.

    One row per batter, opposing pitcher, pitch type, and date window. This
    mirrors the old hittingMatchups workbook fields used by Batter vs Arsenal.
    """

    __tablename__ = "batter_pitch_type_matchups"

    id: int = Column(Integer, primary_key=True, autoincrement=True)
    batter_id: int = Column(Integer, nullable=False, index=True)
    batter_name: Optional[str] = Column(String(120), nullable=True)
    batter_team_id: Optional[int] = Column(Integer, nullable=True, index=True)
    opposing_pitcher_id: int = Column(Integer, nullable=False, index=True)
    pitch_type: str = Column(String(5), nullable=False, index=True)
    game_pk: Optional[int] = Column(Integer, nullable=True, index=True)
    target_date: Optional[date] = Column(Date, nullable=True, index=True)
    date_start: Optional[date] = Column(Date, nullable=True)
    date_end: Optional[date] = Column(Date, nullable=True)
    days_back: Optional[int] = Column(Integer, nullable=True)
    source: Optional[str] = Column(String(40), nullable=True)

    raw_rows: Optional[int] = Column(Integer, nullable=True)
    deduped_rows: Optional[int] = Column(Integer, nullable=True)
    duplicate_rows_removed: Optional[int] = Column(Integer, nullable=True)
    pitches_seen: Optional[int] = Column(Integer, nullable=True)
    swings: Optional[int] = Column(Integer, nullable=True)
    whiffs: Optional[int] = Column(Integer, nullable=True)
    strikeouts: Optional[int] = Column(Integer, nullable=True)
    putaway_swings: Optional[int] = Column(Integer, nullable=True)
    two_strike_pitches: Optional[int] = Column(Integer, nullable=True)
    pa: Optional[int] = Column(Integer, nullable=True)
    pa_ended: Optional[int] = Column(Integer, nullable=True)
    ab: Optional[int] = Column(Integer, nullable=True)
    hits: Optional[int] = Column(Integer, nullable=True)

    batting_avg: Optional[float] = Column(Float, nullable=True)
    xwoba: Optional[float] = Column(Float, nullable=True)
    xba: Optional[float] = Column(Float, nullable=True)
    avg_ev: Optional[float] = Column(Float, nullable=True)
    avg_exit_velocity: Optional[float] = Column(Float, nullable=True)
    avg_la: Optional[float] = Column(Float, nullable=True)
    avg_launch_angle: Optional[float] = Column(Float, nullable=True)
    batted_ball_count: Optional[int] = Column(Integer, nullable=True)
    hard_hit_count: Optional[int] = Column(Integer, nullable=True)
    whiff_pct: Optional[float] = Column(Float, nullable=True)
    k_pct: Optional[float] = Column(Float, nullable=True)
    putaway_pct: Optional[float] = Column(Float, nullable=True)
    hardhit_pct: Optional[float] = Column(Float, nullable=True)
    hard_hit_pct: Optional[float] = Column(Float, nullable=True)

    refreshed_at: datetime = Column(DateTime, default=datetime.utcnow, nullable=False)

    __table_args__ = (
        Index(
            "ix_batter_pitch_type_matchups_lookup",
            "batter_id",
            "opposing_pitcher_id",
            "pitch_type",
            "target_date",
        ),
    )


class PitchArsenal(Base):
    __tablename__ = "pitch_arsenal"

    id: int = Column(Integer, primary_key=True, autoincrement=True)
    season: int = Column(Integer, nullable=False, index=True)
    pitcher_id: int = Column(Integer, nullable=False, index=True)
    pitch_type: Optional[str] = Column(String(5), nullable=True)
    pitch_name: Optional[str] = Column(String(50), nullable=True)
    pitch_count: Optional[int] = Column(Integer, nullable=True)
    usage_pct: Optional[float] = Column(Float, nullable=True)
    whiff_pct: Optional[float] = Column(Float, nullable=True)
    strikeout_pct: Optional[float] = Column(Float, nullable=True)
    rv_per_100: Optional[float] = Column(Float, nullable=True)
    xwoba: Optional[float] = Column(Float, nullable=True)
    hard_hit_pct: Optional[float] = Column(Float, nullable=True)

    __table_args__ = (Index("ix_pitch_arsenal_season_pitcher", "season", "pitcher_id"),)


class TeamSplit(Base):
    __tablename__ = "team_splits"

    id: int = Column(Integer, primary_key=True, autoincrement=True)
    season: int = Column(Integer, nullable=False, index=True)
    team_id: int = Column(Integer, nullable=False, index=True)
    split: str = Column(String(3), nullable=False)
    pa: Optional[int] = Column(Integer, nullable=True)
    hits: Optional[int] = Column(Integer, nullable=True)
    doubles: Optional[int] = Column(Integer, nullable=True)
    triples: Optional[int] = Column(Integer, nullable=True)
    home_runs: Optional[int] = Column(Integer, nullable=True)
    walks: Optional[int] = Column(Integer, nullable=True)
    strikeouts: Optional[int] = Column(Integer, nullable=True)
    batting_avg: Optional[float] = Column(Float, nullable=True)
    on_base_pct: Optional[float] = Column(Float, nullable=True)
    slugging_pct: Optional[float] = Column(Float, nullable=True)
    iso: Optional[float] = Column(Float, nullable=True)
    k_pct: Optional[float] = Column(Float, nullable=True)
    bb_pct: Optional[float] = Column(Float, nullable=True)

    __table_args__ = (Index("ix_team_splits_season_team", "season", "team_id"),)


class PlayerSplit(Base):
    __tablename__ = "player_splits"

    id: int = Column(Integer, primary_key=True, autoincrement=True)
    season: int = Column(Integer, nullable=False, index=True)
    player_id: int = Column(Integer, nullable=False, index=True)
    split: str = Column(String(3), nullable=False)
    pa: Optional[int] = Column(Integer, nullable=True)
    hits: Optional[int] = Column(Integer, nullable=True)
    doubles: Optional[int] = Column(Integer, nullable=True)
    triples: Optional[int] = Column(Integer, nullable=True)
    home_runs: Optional[int] = Column(Integer, nullable=True)
    walks: Optional[int] = Column(Integer, nullable=True)
    strikeouts: Optional[int] = Column(Integer, nullable=True)
    batting_avg: Optional[float] = Column(Float, nullable=True)
    on_base_pct: Optional[float] = Column(Float, nullable=True)
    slugging_pct: Optional[float] = Column(Float, nullable=True)
    iso: Optional[float] = Column(Float, nullable=True)
    k_pct: Optional[float] = Column(Float, nullable=True)
    bb_pct: Optional[float] = Column(Float, nullable=True)

    __table_args__ = (Index("ix_player_splits_season_player", "season", "player_id"),)


class PitcherAggregate(Base):
    __tablename__ = "pitcher_aggregates"

    id: int = Column(Integer, primary_key=True, autoincrement=True)
    pitcher_id: int = Column(Integer, nullable=False, index=True)
    window: str = Column(String(10), nullable=False)
    end_date: date = Column(Date, nullable=False, index=True)
    avg_velocity: Optional[float] = Column(Float, nullable=True)
    avg_spin_rate: Optional[float] = Column(Float, nullable=True)
    hard_hit_pct: Optional[float] = Column(Float, nullable=True)
    k_pct: Optional[float] = Column(Float, nullable=True)
    bb_pct: Optional[float] = Column(Float, nullable=True)
    xwoba: Optional[float] = Column(Float, nullable=True)
    xba: Optional[float] = Column(Float, nullable=True)
    avg_horiz_break: Optional[float] = Column(Float, nullable=True)
    avg_vert_break: Optional[float] = Column(Float, nullable=True)
    avg_release_pos_x: Optional[float] = Column(Float, nullable=True)
    avg_release_pos_z: Optional[float] = Column(Float, nullable=True)
    avg_release_extension: Optional[float] = Column(Float, nullable=True)

    __table_args__ = (Index("ix_pitcher_aggregates_date_pitcher", "end_date", "pitcher_id"),)


class BatterAggregate(Base):
    __tablename__ = "batter_aggregates"

    id: int = Column(Integer, primary_key=True, autoincrement=True)
    batter_id: int = Column(Integer, nullable=False, index=True)
    window: str = Column(String(10), nullable=False)
    end_date: date = Column(Date, nullable=False, index=True)
    avg_exit_velocity: Optional[float] = Column(Float, nullable=True)
    avg_launch_angle: Optional[float] = Column(Float, nullable=True)
    hard_hit_pct: Optional[float] = Column(Float, nullable=True)
    barrel_pct: Optional[float] = Column(Float, nullable=True)
    k_pct: Optional[float] = Column(Float, nullable=True)
    bb_pct: Optional[float] = Column(Float, nullable=True)
    batting_avg: Optional[float] = Column(Float, nullable=True)

    __table_args__ = (Index("ix_batter_aggregates_date_batter", "end_date", "batter_id"),)


class Matchup(Base):
    __tablename__ = "matchups"

    id: int = Column(Integer, primary_key=True, autoincrement=True)
    game_date: date = Column(Date, nullable=False, index=True)
    home_team_id: int = Column(Integer, nullable=False)
    away_team_id: int = Column(Integer, nullable=False)
    home_pitcher_id: int = Column(Integer, nullable=False)
    away_pitcher_id: int = Column(Integer, nullable=False)
    home_win_prob: Optional[float] = Column(Float, nullable=True)
    away_win_prob: Optional[float] = Column(Float, nullable=True)
    prediction: Optional[float] = Column(Float, nullable=True)

    __table_args__ = (Index("ix_matchups_date_home_away", "game_date", "home_team_id", "away_team_id"),)


def get_engine(database_url: str):
    return create_engine(database_url, echo=False, future=True)


def create_tables(engine) -> None:
    Base.metadata.create_all(engine)


def get_session(engine) -> sessionmaker:
    return sessionmaker(bind=engine, autocommit=False, autoflush=False, future=True)
