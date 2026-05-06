#!/usr/bin/env python3
"""Uncapped targeted hittingMatchups refresh with same-day batter skip.

Use this when a late game is missing Batter vs Arsenal cards and you need to
populate real stored 365 data without reprocessing batters already refreshed
today.

Example:
    HITTING_MATCHUPS_TARGET_DATE=2026-05-06 \
    HITTING_MATCHUPS_TARGET_GAME_PK=825091 \
    DATABASE_URL=$DATABASE_URL \
    python scripts/run_hitting_matchups_refresh_uncapped_skip_existing.py

Behavior:
- Reuses the production refresh logic from run_hitting_matchups_refresh.py.
- Removes the practical batter cap by setting HITTING_MATCHUPS_MAX_BATTERS to a
  very high value before target collection.
- Skips an entire batter for the target date when that batter already has any
  stored 365 BatterPitchTypeMatchup rows refreshed today.
- Only writes missing same-day batter rows for the target game/date.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from mlb_app.database import BatterPitchTypeMatchup, create_tables, get_engine, get_session
from mlb_app.hitting_matchups import build_batter_pitch_type_summary

import scripts.run_hitting_matchups_refresh as base

# Effectively uncap. The base script treats MAX_BATTERS as a hard slice, so do
# not set this to 0. A huge ceiling preserves the original collection logic
# while preventing the late game from being dropped off the end of the list.
base.MAX_BATTERS = int(os.getenv("HITTING_MATCHUPS_MAX_BATTERS", "999999"))


def _already_refreshed_today(session, target: Dict[str, Any]) -> bool:
    """Return True when this batter already has stored 365 rows for the target date today.

    This intentionally keys on batter_id + target_date + days_back, not game_pk,
    because the operational goal is to avoid rewriting anything for a batter
    once that batter's stored 365 profile has been generated today.
    """
    target_date = base._parse_iso_date(target.get("date"))
    if target_date is None:
        return False

    today_start = dt.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)

    record = (
        session.query(BatterPitchTypeMatchup)
        .filter(
            BatterPitchTypeMatchup.batter_id == target["batter_id"],
            BatterPitchTypeMatchup.target_date == target_date,
            BatterPitchTypeMatchup.days_back == base.DAYS_BACK,
            BatterPitchTypeMatchup.refreshed_at >= today_start,
        )
        .first()
    )

    return record is not None


def run() -> Dict[str, Any]:
    base._validate_runtime()

    engine = get_engine(base.DATABASE_URL)
    create_tables(engine)
    Session = get_session(engine)

    targets = base._collect_targets()
    base._log(f"Collected {len(targets)} uncapped batter/pitcher targets")

    output: Dict[str, Any] = {
        "generated_at": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "database": base._database_label(),
        "targeted_mode": base.TARGETED_MODE,
        "target_date": base.TARGET_DATE or None,
        "target_game_pk": base.TARGET_GAME_PK or None,
        "days_back": base.DAYS_BACK,
        "max_batters_effective": base.MAX_BATTERS,
        "target_count": len(targets),
        "processed_targets": 0,
        "skipped_already_refreshed_today": 0,
        "upserted_rows": 0,
        "skipped_no_arsenal": 0,
        "rows_with_raw_statcast": 0,
        "rows_without_raw_statcast": 0,
        "rows": [],
    }

    with Session() as session:
        for target in targets:
            if _already_refreshed_today(session, target):
                output["skipped_already_refreshed_today"] += 1
                base._log(
                    "Skipping already refreshed batter today: "
                    f"batter={target['batter_id']} name={target.get('batter_name')} "
                    f"date={target.get('date')} game_pk={target.get('game_pk')}"
                )
                continue

            output["processed_targets"] += 1
            season = int(str(target["date"])[:4])
            pitch_types = base._pitch_types_for_pitcher(session, target["opposing_pitcher_id"], season)
            if not pitch_types:
                output["skipped_no_arsenal"] += 1
                base._log(
                    f"No arsenal or raw pitcher pitch types found pitcher={target['opposing_pitcher_id']} "
                    f"batter={target['batter_id']} game_pk={target.get('game_pk')}"
                )
                continue

            for pitch_type in pitch_types:
                summary = build_batter_pitch_type_summary(
                    session=session,
                    batter_id=target["batter_id"],
                    pitch_type=pitch_type,
                    days_back=base.DAYS_BACK,
                )
                record = base._upsert_matchup_row(session, target, summary)
                output["upserted_rows"] += 1

                if int(summary.get("raw_rows") or 0) > 0:
                    output["rows_with_raw_statcast"] += 1
                else:
                    output["rows_without_raw_statcast"] += 1

                output["rows"].append({
                    **target,
                    **summary,
                    "aggregate_id": getattr(record, "id", None),
                    "skipped_existing_same_day": False,
                })

        session.commit()

    output_path = Path(os.getenv("HITTING_MATCHUPS_OUTPUT_PATH", "hitting_matchups_refresh_uncapped_skip_existing.json"))
    if output_path.parent != Path("."):
        output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(output, indent=2, sort_keys=True), encoding="utf-8")

    base._log(
        "Uncapped skip-existing refresh complete: "
        f"targets={output['target_count']}; "
        f"processed={output['processed_targets']}; "
        f"skipped_existing_today={output['skipped_already_refreshed_today']}; "
        f"upserted_rows={output['upserted_rows']}; "
        f"rows_with_raw_statcast={output['rows_with_raw_statcast']}; "
        f"rows_without_raw_statcast={output['rows_without_raw_statcast']}; "
        f"skipped_no_arsenal={output['skipped_no_arsenal']}; "
        f"artifact={output_path}"
    )

    if base.TARGETED_MODE and output["upserted_rows"] == 0 and output["skipped_already_refreshed_today"] == 0:
        raise RuntimeError(
            "Targeted uncapped refresh completed with zero upserted rows and no same-day skips. "
            "Check target date, game_pk, probable pitchers, and pitcher pitch types."
        )

    return output


def main() -> int:
    try:
        run()
        return 0
    except Exception as exc:
        base._log(f"uncapped hittingMatchups refresh failed: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
