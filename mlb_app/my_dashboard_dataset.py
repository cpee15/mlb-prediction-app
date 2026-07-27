from __future__ import annotations

import datetime as dt
import hashlib
import json
import uuid
from typing import Any, Callable, Dict, Iterable, List, Optional

from sqlalchemy import Boolean, Column, Date, DateTime, Float, Index, Integer, JSON, String, Text, UniqueConstraint, and_

from .database import Base


DATASET_MODE_STANDARD = "standard"
DATASET_MODE_ACTIVE_LINEUPS = "active_lineups"
DATASET_SCHEMA_VERSION = 1


class MyDashboardRecord(Base):
    """Persisted, reportable records owned exclusively by My Dashboard.

    Analytical solvers build these rows. The Workbench query layer reads them.
    User-specific filters and weights must never be persisted here.
    """

    __tablename__ = "my_dashboard_records"

    id: int = Column(Integer, primary_key=True, autoincrement=True)
    dataset_date: dt.date = Column(Date, nullable=False)
    component: str = Column(String(40), nullable=False)
    dataset_mode: str = Column(String(32), nullable=False, default=DATASET_MODE_STANDARD)
    dataset_version: str = Column(String(64), nullable=False)
    dataset_schema_version: int = Column(Integer, nullable=False, default=DATASET_SCHEMA_VERSION)

    entity_key: str = Column(String(255), nullable=False)
    entity_id: Optional[str] = Column(String(128), nullable=True)
    entity_name: Optional[str] = Column(String(255), nullable=True)
    entity_type: Optional[str] = Column(String(64), nullable=True)
    player_type: Optional[str] = Column(String(64), nullable=True)
    game_pk: Optional[int] = Column(Integer, nullable=True)
    team: Optional[str] = Column(String(120), nullable=True)
    opponent: Optional[str] = Column(String(120), nullable=True)
    category: Optional[str] = Column(String(120), nullable=True)
    pitch_type: Optional[str] = Column(String(20), nullable=True)
    pitch_name: Optional[str] = Column(String(80), nullable=True)

    score: Optional[float] = Column(Float, nullable=True)
    base_score: Optional[float] = Column(Float, nullable=True)
    adjusted_score: Optional[float] = Column(Float, nullable=True)
    confidence: Optional[str] = Column(String(32), nullable=True)
    primary_reason: Optional[str] = Column(Text, nullable=True)
    source: Optional[str] = Column(String(255), nullable=True)

    metrics_json = Column(JSON, nullable=True)
    reasoning_json = Column(JSON, nullable=True)
    missing_data_json = Column(JSON, nullable=True)
    best_pitch_angles_json = Column(JSON, nullable=True)
    record_json = Column(JSON, nullable=False)
    data_quality_json = Column(JSON, nullable=True)

    lineup_verified: Optional[bool] = Column(Boolean, nullable=True)
    lineup_source: Optional[str] = Column(String(120), nullable=True)
    confirmed_lineup_date: Optional[dt.date] = Column(Date, nullable=True)
    lineup_revision: Optional[str] = Column(String(128), nullable=True)
    model_state: Optional[str] = Column(String(40), nullable=True)

    solver_version: Optional[str] = Column(String(64), nullable=True)
    source_hash: str = Column(String(64), nullable=False)
    generated_at: dt.datetime = Column(DateTime, nullable=False)
    refreshed_at: dt.datetime = Column(DateTime, nullable=False)
    expires_at: Optional[dt.datetime] = Column(DateTime, nullable=True)
    is_current: bool = Column(Boolean, nullable=False, default=False)

    __table_args__ = (
        UniqueConstraint(
            "dataset_date",
            "component",
            "dataset_mode",
            "dataset_version",
            "entity_key",
            name="uq_my_dashboard_record_version_entity",
        ),
        Index("ix_my_dashboard_records_date_component", "dataset_date", "component"),
        Index("ix_my_dashboard_records_current", "dataset_date", "component", "dataset_mode", "is_current"),
        Index("ix_my_dashboard_records_score", "dataset_date", "component", "dataset_mode", "score"),
        Index("ix_my_dashboard_records_team", "dataset_date", "component", "dataset_mode", "team"),
        Index("ix_my_dashboard_records_game", "dataset_date", "component", "dataset_mode", "game_pk"),
        Index("ix_my_dashboard_records_lineup", "dataset_date", "component", "dataset_mode", "lineup_verified"),
        Index("ix_my_dashboard_records_revision", "dataset_date", "component", "dataset_mode", "lineup_revision"),
    )


def _parse_date(value: Any) -> Optional[dt.date]:
    if value in (None, ""):
        return None
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    return dt.date.fromisoformat(str(value)[:10])


def _safe_int(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _json_safe(value: Any) -> Any:
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    return value


def _canonical_hash(value: Any) -> str:
    encoded = json.dumps(_json_safe(value), sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _entity_key(component: str, row: Dict[str, Any], index: int) -> str:
    explicit = row.get("dedupe_key") or row.get("entity_key")
    if explicit:
        return str(explicit)
    entity_id = row.get("entity_id")
    game_pk = row.get("game_pk")
    pitch = row.get("pitch_type") or row.get("pitch_name")
    if entity_id not in (None, ""):
        return ":".join(str(part) for part in (component, entity_id, game_pk or "", pitch or ""))
    return f"{component}:row:{index}:{_canonical_hash(row)[:20]}"


def normalize_dashboard_record(
    *,
    dataset_date: dt.date,
    component: str,
    dataset_mode: str,
    dataset_version: str,
    row: Dict[str, Any],
    index: int,
    generated_at: dt.datetime,
    refreshed_at: dt.datetime,
    expires_at: Optional[dt.datetime],
    solver_version: Optional[str],
    lineup_revision: Optional[str],
    model_state: Optional[str],
    data_quality: Any,
) -> MyDashboardRecord:
    safe_row = _json_safe(dict(row))
    entity_key = _entity_key(component, safe_row, index)
    record_lineup_revision = safe_row.get("lineup_revision") or lineup_revision
    record_model_state = safe_row.get("model_state") or model_state
    return MyDashboardRecord(
        dataset_date=dataset_date,
        component=component,
        dataset_mode=dataset_mode,
        dataset_version=dataset_version,
        dataset_schema_version=DATASET_SCHEMA_VERSION,
        entity_key=entity_key,
        entity_id=str(safe_row.get("entity_id")) if safe_row.get("entity_id") not in (None, "") else None,
        entity_name=safe_row.get("entity_name"),
        entity_type=safe_row.get("entity_type"),
        player_type=safe_row.get("player_type"),
        game_pk=_safe_int(safe_row.get("game_pk")),
        team=str(safe_row.get("team")) if safe_row.get("team") not in (None, "") else None,
        opponent=str(safe_row.get("opponent")) if safe_row.get("opponent") not in (None, "") else None,
        category=safe_row.get("category"),
        pitch_type=safe_row.get("pitch_type"),
        pitch_name=safe_row.get("pitch_name"),
        score=_safe_float(safe_row.get("score")),
        base_score=_safe_float(safe_row.get("base_score")),
        adjusted_score=_safe_float(safe_row.get("adjusted_score")),
        confidence=safe_row.get("confidence"),
        primary_reason=safe_row.get("primary_reason"),
        source=safe_row.get("source"),
        metrics_json=safe_row.get("metrics") or {},
        reasoning_json=safe_row.get("reasoning") or [],
        missing_data_json=safe_row.get("missing_data") or [],
        best_pitch_angles_json=safe_row.get("best_pitch_angles") or [],
        record_json=safe_row,
        data_quality_json=_json_safe(data_quality),
        lineup_verified=safe_row.get("lineup_verified"),
        lineup_source=safe_row.get("lineup_source"),
        confirmed_lineup_date=_parse_date(safe_row.get("confirmed_lineup_date")),
        lineup_revision=str(record_lineup_revision) if record_lineup_revision not in (None, "") else None,
        model_state=str(record_model_state) if record_model_state not in (None, "") else None,
        solver_version=solver_version,
        source_hash=_canonical_hash(safe_row),
        generated_at=generated_at,
        refreshed_at=refreshed_at,
        expires_at=expires_at,
        is_current=False,
    )


def hydrate_dashboard_dataset(
    *,
    session: Any,
    date: str,
    component: str,
    payload_builder: Callable[[], Dict[str, Any]],
    active_lineups: bool = False,
    force: bool = False,
    ttl_seconds: Optional[int] = None,
    solver_version: Optional[str] = None,
    now: Optional[dt.datetime] = None,
) -> Dict[str, Any]:
    """Build and atomically promote one complete dataset version.

    `payload_builder` must call the existing authoritative solver without report
    filters. The previous current version remains intact whenever building or
    persistence fails.
    """

    del force  # Reserved for route-level freshness policy; hydration is idempotent by version.
    target_date = _parse_date(date)
    if target_date is None:
        raise ValueError("date is required")
    normalized_component = str(component or "").strip().lower()
    if not normalized_component:
        raise ValueError("component is required")

    current_time = now or dt.datetime.utcnow()
    mode = DATASET_MODE_ACTIVE_LINEUPS if active_lineups else DATASET_MODE_STANDARD
    payload = payload_builder() or {}
    source_items = [
        dict(item)
        for item in (payload.get("items") or payload.get("records") or [])
        if isinstance(item, dict)
    ]
    items: List[Dict[str, Any]] = []
    seen_entity_keys = set()
    for source_index, item in enumerate(source_items):
        key = _entity_key(normalized_component, item, source_index)
        if key in seen_entity_keys:
            continue
        seen_entity_keys.add(key)
        items.append(item)
    duplicate_rows_removed = len(source_items) - len(items)
    dataset_version = uuid.uuid4().hex
    expires_at = current_time + dt.timedelta(seconds=int(ttl_seconds)) if ttl_seconds else None
    lineup_revision = payload.get("lineup_revision")
    model_state = payload.get("model_state")
    data_quality = payload.get("data_quality")

    records = [
        normalize_dashboard_record(
            dataset_date=target_date,
            component=normalized_component,
            dataset_mode=mode,
            dataset_version=dataset_version,
            row=row,
            index=index,
            generated_at=current_time,
            refreshed_at=current_time,
            expires_at=expires_at,
            solver_version=solver_version,
            lineup_revision=lineup_revision,
            model_state=model_state,
            data_quality=data_quality,
        )
        for index, row in enumerate(items)
    ]

    try:
        session.add_all(records)
        session.flush()
        session.query(MyDashboardRecord).filter(
            and_(
                MyDashboardRecord.dataset_date == target_date,
                MyDashboardRecord.component == normalized_component,
                MyDashboardRecord.dataset_mode == mode,
                MyDashboardRecord.is_current.is_(True),
            )
        ).update({MyDashboardRecord.is_current: False}, synchronize_session=False)
        if records:
            session.query(MyDashboardRecord).filter(
                MyDashboardRecord.dataset_version == dataset_version
            ).update({MyDashboardRecord.is_current: True}, synchronize_session=False)
        session.commit()
    except Exception:
        session.rollback()
        raise

    return {
        "dataset_source": "my_dashboard_records",
        "dataset_date": target_date.isoformat(),
        "component": normalized_component,
        "dataset_mode": mode,
        "dataset_version": dataset_version,
        "dataset_schema_version": DATASET_SCHEMA_VERSION,
        "dataset_row_count": len(records),
        "source_row_count": len(source_items),
        "duplicate_rows_removed": duplicate_rows_removed,
        "hydrated": True,
        "hydrated_at": current_time.isoformat(),
        "expires_at": expires_at.isoformat() if expires_at else None,
        "lineup_revision": lineup_revision,
        "model_state": model_state,
        "stale": False,
    }


def dashboard_dataset_status(
    *,
    session: Any,
    date: str,
    component: str,
    active_lineups: bool = False,
    now: Optional[dt.datetime] = None,
) -> Dict[str, Any]:
    target_date = _parse_date(date)
    if target_date is None:
        raise ValueError("date is required")
    mode = DATASET_MODE_ACTIVE_LINEUPS if active_lineups else DATASET_MODE_STANDARD
    normalized_component = str(component or "").strip().lower()
    rows: List[MyDashboardRecord] = (
        session.query(MyDashboardRecord)
        .filter(
            MyDashboardRecord.dataset_date == target_date,
            MyDashboardRecord.component == normalized_component,
            MyDashboardRecord.dataset_mode == mode,
            MyDashboardRecord.is_current.is_(True),
        )
        .order_by(MyDashboardRecord.id.asc())
        .all()
    )
    current_time = now or dt.datetime.utcnow()
    expires_at = min((row.expires_at for row in rows if row.expires_at), default=None)
    refreshed_at = max((row.refreshed_at for row in rows if row.refreshed_at), default=None)
    first = rows[0] if rows else None
    return {
        "dataset_source": "my_dashboard_records",
        "dataset_date": target_date.isoformat(),
        "component": normalized_component,
        "dataset_mode": mode,
        "ready": bool(rows),
        "dataset_version": first.dataset_version if first else None,
        "dataset_schema_version": first.dataset_schema_version if first else DATASET_SCHEMA_VERSION,
        "dataset_row_count": len(rows),
        "refreshed_at": refreshed_at.isoformat() if refreshed_at else None,
        "expires_at": expires_at.isoformat() if expires_at else None,
        "lineup_revision": first.lineup_revision if first else None,
        "model_state": first.model_state if first else None,
        "stale": bool(expires_at and expires_at <= current_time),
    }
