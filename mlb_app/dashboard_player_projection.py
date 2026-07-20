"""Versioned player snapshots and atomic current-projection promotion."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence

from .dashboard_object_models import DashboardPlayer, DashboardPlayerCurrent, DashboardPlayerSnapshot
from .dashboard_player_population import CANONICAL_POPULATION_POLICY_VERSION
from .database import BatterAggregate, PitcherAggregate
from .my_dashboard_dataset import MyDashboardRecord


SNAPSHOT_CONTEXT_CURRENT = "current_player_metrics"
HITTER_COMPONENTS = {"hitters", "overall_players"}
PITCHER_COMPONENTS = {"pitchers", "overall_players"}
SNAPSHOT_SCALARS = (
    "model_score",
    "confidence",
    "xwoba",
    "xba",
    "exit_velocity",
    "launch_angle",
    "hard_hit_rate",
    "barrel_rate",
    "strikeout_rate",
    "walk_rate",
    "iso",
    "obp",
    "slg",
    "plate_appearances",
)


def _json_safe(value: Any) -> Any:
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    return value


def _hash(value: Any) -> str:
    encoded = json.dumps(_json_safe(value), sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _safe_int(value: Any) -> Optional[int]:
    try:
        parsed = int(value)
        return parsed if parsed > 0 else None
    except (TypeError, ValueError):
        return None


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _metric(metrics: Dict[str, Any], *names: str) -> Optional[float]:
    for name in names:
        value = _safe_float(metrics.get(name))
        if value is not None:
            return value
    return None


def _coalesce(*values: Any) -> Any:
    return next((value for value in values if value is not None), None)


def _choose_latest(rows: Iterable[Any], id_attribute: str, snapshot_date: dt.date) -> Dict[int, Any]:
    selected: Dict[int, Any] = {}
    window_priority = {"season": 1, "365d": 2, "90d": 3, "30d": 4, "15d": 5}
    for row in rows:
        player_id = int(getattr(row, id_attribute))
        end_date = getattr(row, "end_date", None)
        if end_date and end_date > snapshot_date:
            continue
        current = selected.get(player_id)
        row_key = (end_date or dt.date.min, window_priority.get(str(getattr(row, "window", "")).lower(), 0), getattr(row, "id", 0))
        if current is None:
            selected[player_id] = row
            continue
        current_key = (
            getattr(current, "end_date", None) or dt.date.min,
            window_priority.get(str(getattr(current, "window", "")).lower(), 0),
            getattr(current, "id", 0),
        )
        if row_key > current_key:
            selected[player_id] = row
    return selected


def _latest_dashboard_overlays(session: Any, snapshot_date: dt.date, player_types: Dict[int, str]) -> Dict[int, MyDashboardRecord]:
    if not player_types:
        return {}
    player_ids = set(player_types)
    rows = (
        session.query(MyDashboardRecord)
        .filter(
            MyDashboardRecord.is_current.is_(True),
            MyDashboardRecord.dataset_date <= snapshot_date,
            MyDashboardRecord.entity_id.isnot(None),
            MyDashboardRecord.entity_id.in_([str(value) for value in sorted(player_ids)]),
            MyDashboardRecord.component.in_(sorted(HITTER_COMPONENTS | PITCHER_COMPONENTS)),
        )
        .order_by(MyDashboardRecord.refreshed_at.desc(), MyDashboardRecord.id.desc())
        .all()
    )
    overlays: Dict[int, MyDashboardRecord] = {}
    for row in rows:
        player_id = _safe_int(row.entity_id)
        expected_type = player_types.get(player_id)
        compatible_components = HITTER_COMPONENTS if expected_type == "hitter" else PITCHER_COMPONENTS
        compatible_type = not row.player_type or str(row.player_type).lower() in {expected_type, "player"}
        if player_id in player_ids and player_id not in overlays and row.component in compatible_components and compatible_type:
            overlays[player_id] = row
    return overlays


def build_player_snapshot_rows(
    session: Any,
    snapshot_date: dt.date,
    *,
    player_ids: Optional[Sequence[int]] = None,
) -> List[Dict[str, Any]]:
    """Build one reportable row for every requested active canonical player.

    Existing date-partitioned dashboard records are metric overlays only; they
    never define the player population.
    """

    query = session.query(DashboardPlayer).filter(
        DashboardPlayer.is_active.is_(True),
        DashboardPlayer.identity_resolution_status == "resolved",
    )
    if player_ids is not None:
        query = query.filter(DashboardPlayer.mlb_player_id.in_([int(value) for value in player_ids]))
    players = query.order_by(DashboardPlayer.mlb_player_id.asc()).all()
    ids = {player.mlb_player_id for player in players}
    hitter_aggregates = _choose_latest(
        session.query(BatterAggregate).filter(BatterAggregate.batter_id.in_(ids)).all() if ids else [],
        "batter_id",
        snapshot_date,
    )
    pitcher_aggregates = _choose_latest(
        session.query(PitcherAggregate).filter(PitcherAggregate.pitcher_id.in_(ids)).all() if ids else [],
        "pitcher_id",
        snapshot_date,
    )
    overlays = _latest_dashboard_overlays(session, snapshot_date, {player.mlb_player_id: player.player_type for player in players})
    output: List[Dict[str, Any]] = []

    for player in players:
        sources = ["dashboard_players"]
        metrics: Dict[str, Any] = {}
        source_versions: Dict[str, Any] = {}
        row: Dict[str, Any] = {
            "mlb_player_id": player.mlb_player_id,
            "full_name": player.full_name,
            "player_type": player.player_type,
            "team_id": player.current_team_id,
            "team_name": player.current_team_name,
            "primary_position": player.primary_position,
            "is_active": True,
            "lineup_status": player.active_status_reason,
        }
        row.update({field: None for field in SNAPSHOT_SCALARS})
        aggregate = hitter_aggregates.get(player.mlb_player_id) if player.player_type == "hitter" else pitcher_aggregates.get(player.mlb_player_id)
        if isinstance(aggregate, BatterAggregate):
            row.update(
                exit_velocity=aggregate.avg_exit_velocity,
                launch_angle=aggregate.avg_launch_angle,
                hard_hit_rate=aggregate.hard_hit_pct,
                barrel_rate=aggregate.barrel_pct,
                strikeout_rate=aggregate.k_pct,
                walk_rate=aggregate.bb_pct,
            )
            metrics.update({
                "AVG": aggregate.batting_avg,
                "EV": aggregate.avg_exit_velocity,
                "LA": aggregate.avg_launch_angle,
                "HardHit": aggregate.hard_hit_pct,
                "Barrel": aggregate.barrel_pct,
                "K%": aggregate.k_pct,
                "BB%": aggregate.bb_pct,
            })
            sources.append("batter_aggregates")
        elif isinstance(aggregate, PitcherAggregate):
            row.update(
                xwoba=aggregate.xwoba,
                xba=aggregate.xba,
                hard_hit_rate=aggregate.hard_hit_pct,
                strikeout_rate=aggregate.k_pct,
                walk_rate=aggregate.bb_pct,
            )
            metrics.update({
                "Velocity": aggregate.avg_velocity,
                "Spin Rate": aggregate.avg_spin_rate,
                "xwOBA Allowed": aggregate.xwoba,
                "xBA Allowed": aggregate.xba,
                "HardHit Allowed": aggregate.hard_hit_pct,
                "K%": aggregate.k_pct,
                "BB%": aggregate.bb_pct,
            })
            sources.append("pitcher_aggregates")
        if aggregate is not None:
            source_versions["aggregate"] = {"window": aggregate.window, "end_date": aggregate.end_date.isoformat()}

        overlay = overlays.get(player.mlb_player_id)
        if overlay is not None:
            overlay_metrics = dict(overlay.metrics_json or {})
            compatible = overlay.component in (HITTER_COMPONENTS if player.player_type == "hitter" else PITCHER_COMPONENTS)
            if compatible:
                row.update(
                    model_score=overlay.adjusted_score if overlay.adjusted_score is not None else overlay.score,
                    confidence=overlay.confidence,
                    xwoba=_coalesce(_metric(overlay_metrics, "xwOBA", "xwoba", "xwOBA Allowed"), row.get("xwoba")),
                    xba=_coalesce(_metric(overlay_metrics, "xBA", "xba", "xBA Allowed"), row.get("xba")),
                    exit_velocity=_coalesce(_metric(overlay_metrics, "EV", "Exit Velocity", "avg_exit_velocity"), row.get("exit_velocity")),
                    launch_angle=_coalesce(_metric(overlay_metrics, "LA", "Launch Angle", "avg_launch_angle"), row.get("launch_angle")),
                    hard_hit_rate=_coalesce(_metric(overlay_metrics, "HardHit", "HardHit%", "Hard Hit %", "HardHit Allowed"), row.get("hard_hit_rate")),
                    barrel_rate=_coalesce(_metric(overlay_metrics, "Barrel", "Barrel%", "barrel_pct"), row.get("barrel_rate")),
                    strikeout_rate=_coalesce(_metric(overlay_metrics, "K%", "k_pct"), row.get("strikeout_rate")),
                    walk_rate=_coalesce(_metric(overlay_metrics, "BB%", "bb_pct"), row.get("walk_rate")),
                    iso=_metric(overlay_metrics, "ISO"),
                    obp=_metric(overlay_metrics, "OBP"),
                    slg=_metric(overlay_metrics, "SLG"),
                    plate_appearances=_safe_int(overlay_metrics.get("PA")),
                )
                metrics.update(overlay_metrics)
                source_versions["my_dashboard_records"] = {
                    "dataset_date": overlay.dataset_date.isoformat(),
                    "dataset_version": overlay.dataset_version,
                    "component": overlay.component,
                }
                sources.append("my_dashboard_records_metric_overlay")

        row["metrics"] = metrics
        row["source_versions"] = source_versions
        row["provenance"] = {
            "sources": sources,
            "population_source": "dashboard_players",
            "population_policy_version": CANONICAL_POPULATION_POLICY_VERSION,
            "metrics_overlay_only": True,
        }
        output.append(row)
    return output


def _normalize_snapshot_row(row: Dict[str, Any], active_players: Dict[int, DashboardPlayer]) -> Dict[str, Any]:
    player_id = _safe_int(row.get("mlb_player_id") or row.get("player_id") or row.get("entity_id"))
    if player_id is None or player_id not in active_players:
        raise ValueError(f"Snapshot row has no active canonical player: {player_id}")
    player = active_players[player_id]
    normalized = {
        "mlb_player_id": player_id,
        "full_name": str(row.get("full_name") or player.full_name).strip(),
        "player_type": str(row.get("player_type") or player.player_type).strip().lower(),
        "team_id": _safe_int(row.get("team_id") or player.current_team_id),
        "team_name": row.get("team_name") or player.current_team_name,
        "primary_position": row.get("primary_position") or player.primary_position,
        "is_active": True,
        "opponent_team_id": _safe_int(row.get("opponent_team_id")),
        "game_pk": _safe_int(row.get("game_pk")),
        "lineup_status": row.get("lineup_status") or player.active_status_reason,
        "lineup_position": _safe_int(row.get("lineup_position")),
        "metrics": _json_safe(dict(row.get("metrics") or {})),
        "source_versions": _json_safe(dict(row.get("source_versions") or {})),
        "provenance": _json_safe(dict(row.get("provenance") or {})),
    }
    for field in SNAPSHOT_SCALARS:
        if field == "confidence":
            normalized[field] = row.get(field)
        elif field == "plate_appearances":
            normalized[field] = _safe_int(row.get(field))
        else:
            normalized[field] = _safe_float(row.get(field))
    return normalized


def _snapshot_values(
    row: Dict[str, Any],
    *,
    snapshot_date: dt.date,
    context: str,
    snapshot_version: str,
    batch_version: str,
    now: dt.datetime,
) -> Dict[str, Any]:
    return {
        "mlb_player_id": row["mlb_player_id"],
        "snapshot_date": snapshot_date,
        "analytical_context": context,
        "snapshot_version": snapshot_version,
        "team_id": row.get("team_id"),
        "team_name": row.get("team_name"),
        "opponent_team_id": row.get("opponent_team_id"),
        "game_pk": row.get("game_pk"),
        "lineup_status": row.get("lineup_status"),
        "lineup_position": row.get("lineup_position"),
        "model_score": row.get("model_score"),
        "confidence": row.get("confidence"),
        "xwoba": row.get("xwoba"),
        "xba": row.get("xba"),
        "exit_velocity": row.get("exit_velocity"),
        "launch_angle": row.get("launch_angle"),
        "hard_hit_rate": row.get("hard_hit_rate"),
        "barrel_rate": row.get("barrel_rate"),
        "strikeout_rate": row.get("strikeout_rate"),
        "walk_rate": row.get("walk_rate"),
        "iso": row.get("iso"),
        "obp": row.get("obp"),
        "slg": row.get("slg"),
        "plate_appearances": _safe_int(row.get("plate_appearances")),
        "metrics_json": row.get("metrics") or {},
        "source_versions_json": {**(row.get("source_versions") or {}), "projection_batch_version": batch_version},
        "provenance_json": row.get("provenance") or {},
        "generated_at": now,
        "refreshed_at": now,
        "is_approved": True,
    }


def _promote_current_values(
    row: Dict[str, Any],
    snapshot: DashboardPlayerSnapshot,
    *,
    batch_version: str,
    snapshot_date: dt.date,
    now: dt.datetime,
) -> Dict[str, Any]:
    values = {
        "snapshot_id": snapshot.id,
        "player_type": row["player_type"],
        "full_name": row["full_name"],
        "team_id": row.get("team_id"),
        "team_name": row.get("team_name"),
        "primary_position": row.get("primary_position"),
        "is_active": True,
        "metrics_json": row.get("metrics") or {},
        "projection_version": batch_version,
        "source_freshness_json": {
            "snapshot_date": snapshot_date.isoformat(),
            "snapshot_refreshed_at": now.isoformat(),
            "source_versions": row.get("source_versions") or {},
        },
        "provenance_json": row.get("provenance") or {},
        "promoted_at": now,
        "updated_at": now,
    }
    for field in SNAPSHOT_SCALARS:
        values[field] = row.get(field)
    return values


def refresh_player_projection(
    session: Any,
    *,
    snapshot_date: dt.date,
    row_builder: Optional[Callable[[], Sequence[Dict[str, Any]]]] = None,
    context: str = SNAPSHOT_CONTEXT_CURRENT,
    full_refresh: bool = True,
    promote_current: bool = True,
    allow_empty_population: bool = False,
    promotion_guard: Optional[Callable[[Dict[str, Any]], None]] = None,
    now: Optional[dt.datetime] = None,
) -> Dict[str, Any]:
    """Persist immutable snapshots and atomically promote the current object."""

    current_time = now or dt.datetime.utcnow()
    active_players = {
        row.mlb_player_id: row
        for row in session.query(DashboardPlayer).filter(
            DashboardPlayer.is_active.is_(True),
            DashboardPlayer.identity_resolution_status == "resolved",
        ).all()
    }
    built_rows = list(row_builder() if row_builder is not None else build_player_snapshot_rows(session, snapshot_date))
    normalized_rows: List[Dict[str, Any]] = []
    seen: set[int] = set()
    for raw in built_rows:
        normalized = _normalize_snapshot_row(dict(raw), active_players)
        if normalized["mlb_player_id"] in seen:
            raise ValueError(f"Duplicate snapshot player ID: {normalized['mlb_player_id']}")
        seen.add(normalized["mlb_player_id"])
        normalized_rows.append(normalized)
    expected_ids = set(active_players)
    if not normalized_rows and not allow_empty_population:
        raise ValueError("Refusing to promote an empty player projection")
    if full_refresh and seen != expected_ids:
        missing = sorted(expected_ids - seen)
        extra = sorted(seen - expected_ids)
        raise ValueError(f"Full projection coverage mismatch; missing={missing[:10]} extra={extra[:10]}")

    row_versions = {row["mlb_player_id"]: _hash(row) for row in normalized_rows}
    batch_version = _hash({"snapshot_date": snapshot_date, "context": context, "rows": row_versions})
    snapshots_created = snapshots_reused = current_created = current_updated = current_removed = 0
    staged_snapshots: Dict[int, DashboardPlayerSnapshot] = {}
    try:
        for row in normalized_rows:
            player_id = row["mlb_player_id"]
            version = row_versions[player_id]
            snapshot = session.query(DashboardPlayerSnapshot).filter(
                DashboardPlayerSnapshot.mlb_player_id == player_id,
                DashboardPlayerSnapshot.snapshot_date == snapshot_date,
                DashboardPlayerSnapshot.analytical_context == context,
                DashboardPlayerSnapshot.snapshot_version == version,
            ).one_or_none()
            if snapshot is None:
                snapshot = DashboardPlayerSnapshot(**_snapshot_values(
                    row,
                    snapshot_date=snapshot_date,
                    context=context,
                    snapshot_version=version,
                    batch_version=batch_version,
                    now=current_time,
                ))
                session.add(snapshot)
                snapshots_created += 1
            else:
                snapshots_reused += 1
            staged_snapshots[player_id] = snapshot
        session.flush()

        if promote_current:
            for row in normalized_rows:
                player_id = row["mlb_player_id"]
                snapshot = staged_snapshots[player_id]
                current = session.get(DashboardPlayerCurrent, player_id)
                values = _promote_current_values(
                    row,
                    snapshot,
                    batch_version=batch_version,
                    snapshot_date=snapshot_date,
                    now=current_time,
                )
                if current is None:
                    session.add(DashboardPlayerCurrent(mlb_player_id=player_id, **values))
                    current_created += 1
                elif current.snapshot_id != snapshot.id or current.projection_version != batch_version:
                    for key, value in values.items():
                        setattr(current, key, value)
                    current_updated += 1
            if full_refresh:
                current_removed = session.query(DashboardPlayerCurrent).filter(
                    DashboardPlayerCurrent.mlb_player_id.notin_(seen) if seen else True
                ).delete(synchronize_session=False)
        session.flush()
        staged_status = {
            "snapshot_date": snapshot_date.isoformat(),
            "context": context,
            "projection_version": batch_version,
            "row_count": len(normalized_rows),
            "snapshots_created": snapshots_created,
            "snapshots_reused": snapshots_reused,
            "current_created": current_created,
            "current_updated": current_updated,
            "current_removed": current_removed,
            "promote_current": promote_current,
            "full_refresh": full_refresh,
        }
        if promotion_guard is not None:
            promotion_guard(dict(staged_status))
        session.commit()
    except Exception:
        session.rollback()
        raise

    return {
        **staged_status,
        "current_row_count": session.query(DashboardPlayerCurrent).count(),
        "historical_snapshot_count": session.query(DashboardPlayerSnapshot).count(),
        "refreshed_at": current_time.isoformat(),
        "query_source": "dashboard_player_current",
    }


def backfill_player_projection(
    session: Any,
    *,
    dates: Sequence[dt.date],
    row_builder: Optional[Callable[[dt.date], Sequence[Dict[str, Any]]]] = None,
    context: str = SNAPSHOT_CONTEXT_CURRENT,
    continue_on_error: bool = False,
) -> Dict[str, Any]:
    target_dates = sorted(set(dates))
    results: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []
    for index, target_date in enumerate(target_dates):
        try:
            result = refresh_player_projection(
                session,
                snapshot_date=target_date,
                row_builder=(lambda value=target_date: row_builder(value)) if row_builder is not None else None,
                context=context,
                promote_current=index == len(target_dates) - 1,
            )
            results.append(result)
        except Exception as exc:
            failures.append({"snapshot_date": target_date.isoformat(), "error": str(exc), "error_type": exc.__class__.__name__})
            if not continue_on_error:
                break
    return {
        "requested_date_count": len(target_dates),
        "successful_date_count": len(results),
        "failed_date_count": len(failures),
        "results": results,
        "failures": failures,
        "snapshot_rows_created": sum(item["snapshots_created"] for item in results),
        "final_current_row_count": session.query(DashboardPlayerCurrent).count(),
        "historical_snapshot_count": session.query(DashboardPlayerSnapshot).count(),
    }
