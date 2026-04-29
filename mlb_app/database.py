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
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    String,
    create_engine,
    Index,
    inspect,
    text,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


Base = declarative_base()


class StatcastEvent(Base):
    """Pitch-level Statcast event data."""

    __tablename__ = "statcast_events"

    id: int = Column(Integer, primary_key=True, autoincrement=True)
    game_date: date = Column(Date, nullable=False, index=True)
    game_pk: Optional[int] = Column(Integer, nullable=True, index=True)
    at_bat_number: Optional[int] = Column(Integer, nullable=True)
    pitch_number: Optional[int] = Column(Integer, nullable=True)
    inning: Optional[int] = Column(Integer, nullable=True)
    inning_topbot: Optional[str] = Column(String(10), nullable=True)
    outs_when_up: Optional[int] = Column(Integer, nullable=True)
    home_team: Optional[str] = Column(String(10), nullable=True)
    away_team: Optional[str] = Column(String(10), nullable=True)
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
        Index("ix_statcast_events_batter_order", "batter_id", "game_date", "game_pk", "at_bat_number", "pitch_number"),
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


STATCAST_EVENT_SAFE_COLUMNS = {
    "game_pk": "INTEGER",
    "at_bat_number": "INTEGER",
    "pitch_number": "INTEGER",
    "inning": "INTEGER",
    "inning_topbot": "VARCHAR(10)",
    "outs_when_up": "INTEGER",
    "home_team": "VARCHAR(10)",
    "away_team": "VARCHAR(10)",
}


def _ensure_statcast_event_columns(engine) -> None:
    """Add missing nullable Statcast ordering columns without touching existing data.

    This is intentionally additive only. It never drops tables, deletes rows,
    rewrites existing values, or changes cron/refresh behavior.
    """
    try:
        inspector = inspect(engine)
        if "statcast_events" not in inspector.get_table_names():
            return
        existing_columns = {col["name"] for col in inspector.get_columns("statcast_events")}
        missing_columns = {
            name: sql_type
            for name, sql_type in STATCAST_EVENT_SAFE_COLUMNS.items()
            if name not in existing_columns
        }
        if not missing_columns:
            return
        dialect = engine.dialect.name
        with engine.begin() as conn:
            for name, sql_type in missing_columns.items():
                if dialect == "postgresql":
                    stmt = text(f"ALTER TABLE statcast_events ADD COLUMN IF NOT EXISTS {name} {sql_type}")
                else:
                    stmt = text(f"ALTER TABLE statcast_events ADD COLUMN {name} {sql_type}")
                try:
                    conn.execute(stmt)
                except Exception as exc:
                    if "duplicate column" in str(exc).lower() or "already exists" in str(exc).lower():
                        continue
                    raise
    except Exception as exc:
        print(f"[database] Non-fatal statcast_events schema guard skipped: {exc}")


def get_engine(database_url: str):
    return create_engine(database_url, echo=False, future=True)


def create_tables(engine) -> None:
    Base.metadata.create_all(engine)
    _ensure_statcast_event_columns(engine)


def get_session(engine) -> sessionmaker:
    return sessionmaker(bind=engine, autocommit=False, autoflush=False, future=True)
