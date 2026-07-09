from __future__ import annotations

from typing import Any, Dict, Iterable, List

from sqlalchemy import func
from sqlalchemy.orm import Session

from .database import BatterAggregate, BatterPitchTypeMatchup, PlayerSplit, StatcastEvent, TeamSplit
from .db_utils import TERMINAL_EVENTS


def _row_to_dict(row: Any, columns: Iterable[str]) -> Dict[str, Any]:
    out = {column: getattr(row, column) for column in columns}
    out["duplicate_count"] = int(getattr(row, "duplicate_count") or 0)
    return out


def _duplicate_query(session: Session, model: Any, columns: List[Any], limit: int) -> List[Dict[str, Any]]:
    labels = [column.key for column in columns]
    rows = (
        session.query(*columns, func.count().label("duplicate_count"))
        .group_by(*columns)
        .having(func.count() > 1)
        .order_by(func.count().desc())
        .limit(limit)
        .all()
    )
    return [_row_to_dict(row, labels) for row in rows]


def build_duplicate_audit(session: Session, limit: int = 50) -> Dict[str, Any]:
    limit = max(1, min(int(limit or 50), 500))

    statcast_pitch_rows = (
        session.query(
            StatcastEvent.game_pk,
            StatcastEvent.at_bat_number,
            StatcastEvent.pitch_number,
            StatcastEvent.pitcher_id,
            StatcastEvent.batter_id,
            func.count().label("duplicate_count"),
        )
        .filter(
            StatcastEvent.game_pk.isnot(None),
            StatcastEvent.at_bat_number.isnot(None),
            StatcastEvent.pitch_number.isnot(None),
            StatcastEvent.pitcher_id.isnot(None),
            StatcastEvent.batter_id.isnot(None),
        )
        .group_by(
            StatcastEvent.game_pk,
            StatcastEvent.at_bat_number,
            StatcastEvent.pitch_number,
            StatcastEvent.pitcher_id,
            StatcastEvent.batter_id,
        )
        .having(func.count() > 1)
        .order_by(func.count().desc())
        .limit(limit)
        .all()
    )

    statcast_pa_rows = (
        session.query(
            StatcastEvent.game_pk,
            StatcastEvent.at_bat_number,
            StatcastEvent.pitcher_id,
            StatcastEvent.batter_id,
            func.count().label("duplicate_count"),
        )
        .filter(
            StatcastEvent.game_pk.isnot(None),
            StatcastEvent.at_bat_number.isnot(None),
            StatcastEvent.pitcher_id.isnot(None),
            StatcastEvent.batter_id.isnot(None),
            StatcastEvent.events.in_(TERMINAL_EVENTS),
        )
        .group_by(
            StatcastEvent.game_pk,
            StatcastEvent.at_bat_number,
            StatcastEvent.pitcher_id,
            StatcastEvent.batter_id,
        )
        .having(func.count() > 1)
        .order_by(func.count().desc())
        .limit(limit)
        .all()
    )

    audit = {
        "statcast_pitch_identity": [
            {
                "game_pk": row.game_pk,
                "at_bat_number": row.at_bat_number,
                "pitch_number": row.pitch_number,
                "pitcher_id": row.pitcher_id,
                "batter_id": row.batter_id,
                "duplicate_count": int(row.duplicate_count or 0),
            }
            for row in statcast_pitch_rows
        ],
        "statcast_terminal_pa_identity": [
            {
                "game_pk": row.game_pk,
                "at_bat_number": row.at_bat_number,
                "pitcher_id": row.pitcher_id,
                "batter_id": row.batter_id,
                "duplicate_count": int(row.duplicate_count or 0),
            }
            for row in statcast_pa_rows
        ],
        "batter_pitch_type_matchups": _duplicate_query(
            session,
            BatterPitchTypeMatchup,
            [
                BatterPitchTypeMatchup.target_date,
                BatterPitchTypeMatchup.game_pk,
                BatterPitchTypeMatchup.batter_id,
                BatterPitchTypeMatchup.opposing_pitcher_id,
                BatterPitchTypeMatchup.pitch_type,
            ],
            limit,
        ),
        "batter_aggregates": _duplicate_query(
            session,
            BatterAggregate,
            [BatterAggregate.batter_id, BatterAggregate.window, BatterAggregate.end_date],
            limit,
        ),
        "player_splits": _duplicate_query(
            session,
            PlayerSplit,
            [PlayerSplit.player_id, PlayerSplit.season, PlayerSplit.split],
            limit,
        ),
        "team_splits": _duplicate_query(
            session,
            TeamSplit,
            [TeamSplit.team_id, TeamSplit.season, TeamSplit.split],
            limit,
        ),
    }
    audit["has_duplicates"] = any(bool(rows) for rows in audit.values() if isinstance(rows, list))
    audit["limits"] = {"per_table": limit}
    audit["recommendation"] = "Clean duplicate keys, then add DB-level uniqueness or conflict-safe upserts for every audited identity."
    return audit
