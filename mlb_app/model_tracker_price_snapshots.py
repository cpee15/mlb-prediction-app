from __future__ import annotations

import datetime as dt
import hashlib
import json
import re
from typing import Any, Dict, Iterable, List, Optional

from sqlalchemy import Column, Date, DateTime, Float, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Session

from .database import Base
from .sportsbook_bet105_runtime_v10 import fetch_board as fetch_bet105_board


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _json(value: Any) -> Optional[str]:
    if value is None:
        return None
    try:
        return json.dumps(value, default=str, sort_keys=True)
    except Exception:
        return json.dumps(str(value))


def _loads(value: Optional[str]) -> Any:
    if not value:
        return None
    try:
        return json.loads(value)
    except Exception:
        return value


def _slug(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(value or "").lower()).strip("_") or "unknown"


def _normalize_line(value: Any) -> str:
    number = _safe_float(value)
    if number is None:
        return "none"
    return str(int(number)) if float(number).is_integer() else str(number)


def american_to_decimal(price: Any) -> Optional[float]:
    number = _safe_float(price)
    if number is None or number == 0:
        return None
    if number > 0:
        return round(1 + number / 100.0, 6)
    return round(1 + 100.0 / abs(number), 6)


def implied_probability_from_american(price: Any) -> Optional[float]:
    decimal = american_to_decimal(price)
    if decimal is None or decimal <= 1:
        return None
    return round(1 / decimal, 6)


def snapshot_hour(value: Optional[dt.datetime] = None) -> dt.datetime:
    stamp = value or dt.datetime.utcnow()
    if stamp.tzinfo is not None:
        stamp = stamp.astimezone(dt.timezone.utc).replace(tzinfo=None)
    return stamp.replace(minute=0, second=0, microsecond=0)


class ModelTrackerPriceSnapshot(Base):
    """Provider-aware hourly sportsbook price snapshot for Model Tracker CLV analysis."""

    __tablename__ = "model_tracker_price_snapshots"

    id = Column(Integer, primary_key=True, autoincrement=True)
    snapshot_key = Column(String(700), nullable=False, unique=True, index=True)
    snapshot_date = Column(Date, nullable=False, index=True)
    captured_at = Column(DateTime, nullable=False, index=True)
    snapshot_bucket = Column(DateTime, nullable=False, index=True)
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False)

    provider = Column(String(40), nullable=False, index=True)
    book = Column(String(40), nullable=True)
    game_pk = Column(Integer, nullable=True, index=True)
    event_id = Column(String(120), nullable=True, index=True)
    fixture_id = Column(String(120), nullable=True)
    start_time = Column(String(80), nullable=True)
    away_team = Column(String(180), nullable=True)
    home_team = Column(String(180), nullable=True)

    market_type = Column(String(100), nullable=True, index=True)
    market_key = Column(String(100), nullable=True, index=True)
    market_name = Column(String(180), nullable=True)
    selection_key = Column(String(240), nullable=True, index=True)
    selection_label = Column(String(240), nullable=True)
    player_id = Column(Integer, nullable=True, index=True)
    player_name = Column(String(180), nullable=True)
    team_name = Column(String(180), nullable=True)

    line = Column(Float, nullable=True)
    line_key = Column(String(80), nullable=False, default="none")
    price = Column(Float, nullable=True)
    decimal_price = Column(Float, nullable=True)
    implied_probability = Column(Float, nullable=True)
    raw_market_id = Column(String(120), nullable=True)
    raw_selection_id = Column(String(120), nullable=True)
    source_endpoint = Column(String(180), nullable=True)
    raw_payload_json = Column(Text, nullable=True)

    __table_args__ = (
        UniqueConstraint("snapshot_key", name="uq_model_tracker_price_snapshot_key"),
        Index("ix_model_tracker_price_date_provider", "snapshot_date", "provider"),
        Index("ix_model_tracker_price_event_market", "snapshot_date", "provider", "event_id", "market_key", "selection_key"),
    )


def _ensure_price_table(session: Session) -> None:
    bind = session.get_bind()
    ModelTrackerPriceSnapshot.__table__.create(bind=bind, checkfirst=True)


def _team_name(value: Any) -> Optional[str]:
    if isinstance(value, dict):
        nested = value.get("name")
        if isinstance(nested, dict):
            return nested.get("name") or nested.get("display_name") or nested.get("fullName")
        return nested or value.get("display_name") or value.get("fullName")
    return value if value not in (None, "") else None


def _selection_label(selection: Dict[str, Any]) -> str:
    return str(selection.get("description") or selection.get("name") or selection.get("team") or selection.get("side") or "Selection")


def _extract_raw_id(value: Dict[str, Any], keys: Iterable[str]) -> Optional[str]:
    for key in keys:
        if value.get(key) not in (None, ""):
            return str(value.get(key))
    raw = value.get("raw") if isinstance(value.get("raw"), dict) else {}
    for key in keys:
        if raw.get(key) not in (None, ""):
            return str(raw.get(key))
    info = raw.get("info") if isinstance(raw.get("info"), dict) else {}
    for key in keys:
        if info.get(key) not in (None, ""):
            return str(info.get(key))
    return None


def _snapshot_key(row: Dict[str, Any]) -> str:
    parts = [
        row.get("snapshot_date"),
        row.get("provider"),
        row.get("event_id") or row.get("game_pk"),
        row.get("market_key"),
        row.get("selection_key"),
        row.get("line_key"),
        row.get("snapshot_bucket"),
    ]
    raw = "|".join(str(part or "") for part in parts)
    if len(raw) <= 650:
        return raw
    return raw[:590] + "|" + hashlib.sha1(raw.encode("utf-8")).hexdigest()


def normalize_price_rows_from_board(payload: Dict[str, Any], target_date: str, provider: str, captured_at: Optional[dt.datetime] = None) -> List[Dict[str, Any]]:
    captured = captured_at or dt.datetime.utcnow()
    bucket = snapshot_hour(captured)
    rows: List[Dict[str, Any]] = []
    for event in payload.get("events") or []:
        if not isinstance(event, dict):
            continue
        event_id = str(event.get("event_id") or event.get("fixture_id") or event.get("game_pk") or "") or None
        game_pk = _safe_int(event.get("game_pk"))
        away_team = _team_name(event.get("away_team"))
        home_team = _team_name(event.get("home_team"))
        for market in event.get("markets") or []:
            if not isinstance(market, dict):
                continue
            market_key = str(market.get("market_key") or market.get("market_type") or market.get("market_name") or "unknown_market")
            market_name = str(market.get("market_name") or market_key)
            raw_market_id = _extract_raw_id(market, ("market_id", "marketId", "id"))
            for selection in market.get("selections") or []:
                if not isinstance(selection, dict):
                    continue
                price = _safe_float(selection.get("price") or selection.get("price_american") or (selection.get("odds") or {}).get("american"))
                if price is None:
                    continue
                line = _safe_float(selection.get("line"))
                line_key = _normalize_line(line)
                label = _selection_label(selection)
                raw_selection_id = _extract_raw_id(selection, ("selection_id", "selectionId", "outcome_id", "outcomeId", "id", "participant_id", "participantId"))
                selection_key = raw_selection_id or f"{_slug(label)}:{line_key}"
                row = {
                    "snapshot_date": target_date[:10],
                    "captured_at": captured,
                    "snapshot_bucket": bucket,
                    "provider": provider,
                    "book": payload.get("book") or provider,
                    "game_pk": game_pk,
                    "event_id": event_id,
                    "fixture_id": str(event.get("fixture_id") or "") or None,
                    "start_time": event.get("start_time") or event.get("commence_time"),
                    "away_team": away_team,
                    "home_team": home_team,
                    "market_type": market_key,
                    "market_key": market_key,
                    "market_name": market_name,
                    "selection_key": selection_key,
                    "selection_label": label,
                    "player_id": _safe_int(selection.get("player_id") or selection.get("participant_id")),
                    "player_name": selection.get("player_name"),
                    "team_name": selection.get("team") if isinstance(selection.get("team"), str) else None,
                    "line": line,
                    "line_key": line_key,
                    "price": price,
                    "decimal_price": american_to_decimal(price),
                    "implied_probability": selection.get("implied_probability") or (selection.get("odds") or {}).get("implied_probability") or implied_probability_from_american(price),
                    "raw_market_id": raw_market_id,
                    "raw_selection_id": raw_selection_id,
                    "source_endpoint": "/odds/bet105/events" if provider == "bet105" else None,
                    "raw_payload_json": _json({"event": event, "market": market, "selection": selection}),
                }
                row["snapshot_key"] = _snapshot_key(row)
                rows.append(row)
    return rows


def upsert_price_rows(session: Session, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    _ensure_price_table(session)
    inserted = 0
    updated = 0
    now = dt.datetime.utcnow()
    for row in rows:
        key = row.get("snapshot_key") or _snapshot_key(row)
        existing = session.query(ModelTrackerPriceSnapshot).filter(ModelTrackerPriceSnapshot.snapshot_key == key).first()
        target = existing or ModelTrackerPriceSnapshot(snapshot_key=key)
        if existing:
            updated += 1
        else:
            session.add(target)
            inserted += 1
        for field, value in row.items():
            if field == "snapshot_date" and isinstance(value, str):
                value = dt.date.fromisoformat(value[:10])
            if hasattr(target, field):
                setattr(target, field, value)
        target.updated_at = now
    session.commit()
    return {"inserted": inserted, "updated": updated, "total_rows_seen": len(rows)}


def capture_bet105_price_snapshots(session: Session, target_date: str, captured_at: Optional[dt.datetime] = None) -> Dict[str, Any]:
    target_date = target_date[:10]
    dt.date.fromisoformat(target_date)
    board = fetch_bet105_board(date=target_date, raw=False, live_only=False)
    status = board.get("status")
    if status in {"provider_not_configured", "provider_error"}:
        return {"date": target_date, "provider": "bet105", "status": status, "rows_collected": 0, "upsert": {"inserted": 0, "updated": 0, "total_rows_seen": 0}, "errors": board.get("errors") or []}
    rows = normalize_price_rows_from_board(board, target_date, "bet105", captured_at=captured_at)
    upsert = upsert_price_rows(session, rows)
    return {
        "date": target_date,
        "provider": "bet105",
        "status": status or "unknown",
        "event_count": board.get("event_count"),
        "market_count": board.get("market_count"),
        "rows_collected": len(rows),
        "snapshot_bucket": snapshot_hour(captured_at).isoformat() if captured_at else snapshot_hour().isoformat(),
        "upsert": upsert,
        "errors": board.get("errors") or [],
    }


def _row_payload(row: ModelTrackerPriceSnapshot) -> Dict[str, Any]:
    return {
        "id": row.id,
        "snapshot_key": row.snapshot_key,
        "snapshot_date": row.snapshot_date.isoformat() if row.snapshot_date else None,
        "captured_at": row.captured_at.isoformat() if row.captured_at else None,
        "snapshot_bucket": row.snapshot_bucket.isoformat() if row.snapshot_bucket else None,
        "provider": row.provider,
        "book": row.book,
        "game_pk": row.game_pk,
        "event_id": row.event_id,
        "fixture_id": row.fixture_id,
        "start_time": row.start_time,
        "away_team": row.away_team,
        "home_team": row.home_team,
        "market_type": row.market_type,
        "market_key": row.market_key,
        "market_name": row.market_name,
        "selection_key": row.selection_key,
        "selection_label": row.selection_label,
        "player_id": row.player_id,
        "player_name": row.player_name,
        "team_name": row.team_name,
        "line": row.line,
        "line_key": row.line_key,
        "price": row.price,
        "decimal_price": row.decimal_price,
        "implied_probability": row.implied_probability,
        "raw_market_id": row.raw_market_id,
        "raw_selection_id": row.raw_selection_id,
        "source_endpoint": row.source_endpoint,
        "raw_payload": _loads(row.raw_payload_json),
    }


def _price_sort_value(price: Optional[float]) -> float:
    return float(price) if price is not None else -999999.0


def summarize_price_rows(rows: List[ModelTrackerPriceSnapshot]) -> List[Dict[str, Any]]:
    groups: Dict[str, List[ModelTrackerPriceSnapshot]] = {}
    for row in rows:
        key = "|".join(str(part or "") for part in (row.snapshot_date, row.provider, row.event_id or row.game_pk, row.market_key, row.selection_key, row.line_key))
        groups.setdefault(key, []).append(row)
    summaries: List[Dict[str, Any]] = []
    for key, group in groups.items():
        group.sort(key=lambda item: item.snapshot_bucket or item.captured_at or dt.datetime.min)
        first = group[0]
        latest = group[-1]
        best = max(group, key=lambda item: _price_sort_value(item.price))
        summaries.append({
            "summary_key": key,
            "provider": latest.provider,
            "event_id": latest.event_id,
            "game_pk": latest.game_pk,
            "game": f"{latest.away_team or 'Away'} @ {latest.home_team or 'Home'}",
            "market_key": latest.market_key,
            "market_name": latest.market_name,
            "selection_key": latest.selection_key,
            "selection_label": latest.selection_label,
            "line": latest.line,
            "line_key": latest.line_key,
            "snapshot_count": len(group),
            "first_seen_at": first.snapshot_bucket.isoformat() if first.snapshot_bucket else None,
            "first_seen_price": first.price,
            "first_seen_implied_probability": first.implied_probability,
            "latest_seen_at": latest.snapshot_bucket.isoformat() if latest.snapshot_bucket else None,
            "latest_price": latest.price,
            "latest_implied_probability": latest.implied_probability,
            "best_price_seen": best.price,
            "best_price_seen_at": best.snapshot_bucket.isoformat() if best.snapshot_bucket else None,
            "price_move_american": (latest.price - first.price) if latest.price is not None and first.price is not None else None,
            "implied_probability_move": (latest.implied_probability - first.implied_probability) if latest.implied_probability is not None and first.implied_probability is not None else None,
        })
    summaries.sort(key=lambda item: (item.get("game") or "", item.get("market_key") or "", item.get("selection_label") or ""))
    return summaries


def list_price_snapshots(session: Session, target_date: str, provider: Optional[str] = None) -> Dict[str, Any]:
    _ensure_price_table(session)
    parsed = dt.date.fromisoformat(target_date[:10])
    query = session.query(ModelTrackerPriceSnapshot).filter(ModelTrackerPriceSnapshot.snapshot_date == parsed)
    if provider:
        query = query.filter(ModelTrackerPriceSnapshot.provider == provider)
    records = query.order_by(ModelTrackerPriceSnapshot.snapshot_bucket, ModelTrackerPriceSnapshot.provider, ModelTrackerPriceSnapshot.event_id, ModelTrackerPriceSnapshot.market_key).all()
    return {
        "date": parsed.isoformat(),
        "provider": provider or "all",
        "snapshot_count": len(records),
        "snapshots": [_row_payload(row) for row in records],
        "summaries": summarize_price_rows(records),
    }
