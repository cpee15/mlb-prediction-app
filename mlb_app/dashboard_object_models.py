"""Canonical object-store schema for metadata-driven dashboard reports.

This module only defines the durable contract. Population and report routing are
deliberately implemented in later sprint slices so the existing solver routes
remain unchanged while the schema is deployed and reviewed.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Optional

from sqlalchemy import Boolean, Column, Date, DateTime, Float, Index, Integer, JSON, String, Text, UniqueConstraint

from .database import Base


class DashboardPlayer(Base):
    __tablename__ = "dashboard_players"

    mlb_player_id: int = Column(Integer, primary_key=True, autoincrement=False)
    full_name: str = Column(String(255), nullable=False)
    current_team_id: Optional[int] = Column(Integer, nullable=True)
    current_team_name: Optional[str] = Column(String(120), nullable=True)
    primary_position: Optional[str] = Column(String(16), nullable=True)
    player_type: str = Column(String(32), nullable=False)
    bats: Optional[str] = Column(String(8), nullable=True)
    throws: Optional[str] = Column(String(8), nullable=True)
    is_active: bool = Column(Boolean, nullable=False, default=False)
    active_status_reason: Optional[str] = Column(String(80), nullable=True)
    first_tracked_date: date = Column(Date, nullable=False)
    last_tracked_date: date = Column(Date, nullable=False)
    most_recent_lineup_date: Optional[date] = Column(Date, nullable=True)
    most_recent_game_date: Optional[date] = Column(Date, nullable=True)
    lineup_appearance_count: int = Column(Integer, nullable=False, default=0)
    tracked_game_count: int = Column(Integer, nullable=False, default=0)
    source_provenance_json = Column(JSON, nullable=False, default=dict)
    identity_resolution_status: str = Column(String(32), nullable=False)
    created_at: datetime = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at: datetime = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index("ix_dashboard_players_active_type", "is_active", "player_type", "mlb_player_id"),
        Index("ix_dashboard_players_team_active", "current_team_id", "is_active"),
        Index("ix_dashboard_players_last_tracked", "last_tracked_date"),
        Index("ix_dashboard_players_identity_status", "identity_resolution_status"),
    )


class DashboardProjectionRun(Base):
    __tablename__ = "dashboard_projection_runs"

    id: int = Column(Integer, primary_key=True, autoincrement=True)
    run_type: str = Column(String(32), nullable=False)
    target_date: date = Column(Date, nullable=False)
    status: str = Column(String(24), nullable=False)
    started_at: datetime = Column(DateTime, nullable=False)
    completed_at: Optional[datetime] = Column(DateTime, nullable=True)
    canonical_count: int = Column(Integer, nullable=False, default=0)
    active_count: int = Column(Integer, nullable=False, default=0)
    current_count: int = Column(Integer, nullable=False, default=0)
    snapshot_count: int = Column(Integer, nullable=False, default=0)
    projection_version: Optional[str] = Column(String(64), nullable=True)
    error_type: Optional[str] = Column(String(120), nullable=True)
    error_message: Optional[str] = Column(Text, nullable=True)
    result_json = Column(JSON, nullable=False, default=dict)

    __table_args__ = (
        Index("ix_dashboard_projection_runs_status_completed", "status", "completed_at"),
        Index("ix_dashboard_projection_runs_target_type", "target_date", "run_type"),
    )


class DashboardPlayerSnapshot(Base):
    __tablename__ = "dashboard_player_snapshots"

    id: int = Column(Integer, primary_key=True, autoincrement=True)
    mlb_player_id: int = Column(Integer, nullable=False)
    snapshot_date: date = Column(Date, nullable=False)
    analytical_context: str = Column(String(64), nullable=False)
    snapshot_version: str = Column(String(64), nullable=False)
    team_id: Optional[int] = Column(Integer, nullable=True)
    team_name: Optional[str] = Column(String(120), nullable=True)
    opponent_team_id: Optional[int] = Column(Integer, nullable=True)
    game_pk: Optional[int] = Column(Integer, nullable=True)
    lineup_status: Optional[str] = Column(String(80), nullable=True)
    lineup_position: Optional[int] = Column(Integer, nullable=True)
    model_score: Optional[float] = Column(Float, nullable=True)
    confidence: Optional[str] = Column(String(32), nullable=True)
    xwoba: Optional[float] = Column(Float, nullable=True)
    xba: Optional[float] = Column(Float, nullable=True)
    exit_velocity: Optional[float] = Column(Float, nullable=True)
    launch_angle: Optional[float] = Column(Float, nullable=True)
    hard_hit_rate: Optional[float] = Column(Float, nullable=True)
    barrel_rate: Optional[float] = Column(Float, nullable=True)
    strikeout_rate: Optional[float] = Column(Float, nullable=True)
    walk_rate: Optional[float] = Column(Float, nullable=True)
    iso: Optional[float] = Column(Float, nullable=True)
    obp: Optional[float] = Column(Float, nullable=True)
    slg: Optional[float] = Column(Float, nullable=True)
    plate_appearances: Optional[int] = Column(Integer, nullable=True)
    metrics_json = Column(JSON, nullable=False, default=dict)
    source_versions_json = Column(JSON, nullable=False, default=dict)
    provenance_json = Column(JSON, nullable=False, default=dict)
    generated_at: datetime = Column(DateTime, nullable=False)
    refreshed_at: datetime = Column(DateTime, nullable=False)
    is_approved: bool = Column(Boolean, nullable=False, default=False)

    __table_args__ = (
        UniqueConstraint("mlb_player_id", "snapshot_date", "analytical_context", "snapshot_version", name="uq_dashboard_player_snapshot_version"),
        Index("ix_dashboard_snapshots_player_date", "mlb_player_id", "snapshot_date"),
        Index("ix_dashboard_snapshots_context_date", "analytical_context", "snapshot_date"),
        Index("ix_dashboard_snapshots_approved", "snapshot_date", "analytical_context", "is_approved"),
    )


class DashboardPlayerCurrent(Base):
    __tablename__ = "dashboard_player_current"

    mlb_player_id: int = Column(Integer, primary_key=True, autoincrement=False)
    snapshot_id: int = Column(Integer, nullable=False)
    player_type: str = Column(String(32), nullable=False)
    full_name: str = Column(String(255), nullable=False)
    team_id: Optional[int] = Column(Integer, nullable=True)
    team_name: Optional[str] = Column(String(120), nullable=True)
    primary_position: Optional[str] = Column(String(16), nullable=True)
    is_active: bool = Column(Boolean, nullable=False)
    model_score: Optional[float] = Column(Float, nullable=True)
    confidence: Optional[str] = Column(String(32), nullable=True)
    xwoba: Optional[float] = Column(Float, nullable=True)
    xba: Optional[float] = Column(Float, nullable=True)
    exit_velocity: Optional[float] = Column(Float, nullable=True)
    launch_angle: Optional[float] = Column(Float, nullable=True)
    hard_hit_rate: Optional[float] = Column(Float, nullable=True)
    barrel_rate: Optional[float] = Column(Float, nullable=True)
    strikeout_rate: Optional[float] = Column(Float, nullable=True)
    walk_rate: Optional[float] = Column(Float, nullable=True)
    iso: Optional[float] = Column(Float, nullable=True)
    obp: Optional[float] = Column(Float, nullable=True)
    slg: Optional[float] = Column(Float, nullable=True)
    plate_appearances: Optional[int] = Column(Integer, nullable=True)
    metrics_json = Column(JSON, nullable=False, default=dict)
    projection_version: str = Column(String(64), nullable=False)
    source_freshness_json = Column(JSON, nullable=False, default=dict)
    provenance_json = Column(JSON, nullable=False, default=dict)
    promoted_at: datetime = Column(DateTime, nullable=False)
    updated_at: datetime = Column(DateTime, nullable=False)

    __table_args__ = (
        Index("ix_dashboard_current_active_type", "is_active", "player_type", "mlb_player_id"),
        Index("ix_dashboard_current_team_active", "team_id", "is_active"),
        Index("ix_dashboard_current_hitter_xwoba", "player_type", "is_active", "xwoba"),
        Index("ix_dashboard_current_pitcher_score", "player_type", "is_active", "model_score"),
    )
