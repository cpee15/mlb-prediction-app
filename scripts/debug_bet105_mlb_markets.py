#!/usr/bin/env python3
"""Probe Bet105 MLB market rows for selected KIBL MLB fixtures.

Diagnostic-only. Does not run in the web route. The goal is to discover the
specific KIBL info/markets request body that returns Bet105 rows for MLB
sport_id=2, league_id=7, feed_source_id=171.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from mlb_app.kibl_bet105_repository import KiblBet105Repository
from mlb_app.kibl_client import find_rows


SPORT_ID = "2"
LEAGUE_ID = "7"
FEED_SOURCE_ID = 171
PREMATCH_BETTING_TYPE_ID = 1


def _clean(body: Dict[str, Any]) -> Dict[str, Any]:
    return {key: value for key, value in body.items() if value not in (None, "", [], {})}


def _get(row: Dict[str, Any], key: str) -> Any:
    if key in row:
        return row.get(key)
    info = row.get("info") if isinstance(row.get("info"), dict) else {}
    return info.get(key)


def _unique(rows: Iterable[Dict[str, Any]], keys: Tuple[str, ...], limit: int = 25) -> List[str]:
    out: List[str] = []
    for row in rows:
        value = None
        for key in keys:
            value = _get(row, key)
            if value not in (None, ""):
                break
        if value in (None, ""):
            continue
        text = str(value)
        if text not in out:
            out.append(text)
        if len(out) >= limit:
            break
    return out


def _request(repo: KiblBet105Repository, label: str, body: Dict[str, Any]) -> Dict[str, Any]:
    clean = _clean(body)
    try:
        rows = find_rows(repo.client.post("info/markets", clean))
    except Exception as exc:  # noqa: BLE001
        return {"label": label, "body": clean, "row_count": 0, "error": str(exc)}
    return {
        "label": label,
        "body": clean,
        "row_count": len(rows),
        "fixture_ids": _unique(rows, ("fixture_id", "fixtureId", "event_id", "id")),
        "market_ids": _unique(rows, ("market_id", "marketId")),
        "market_type_ids": _unique(rows, ("market_type_id", "marketTypeId")),
        "fixture_participant_ids": _unique(rows, ("fixture_participant_id", "fixtureParticipantId")),
        "participant_ids": _unique(rows, ("participant_id", "participantId")),
        "prices": _unique(rows, ("price_american", "price_decimal"), limit=10),
        "routing_keys": _unique(rows, ("routing_key", "routingKey"), limit=10),
        "first_rows": rows[:5],
    }


def _fixture_rows(repo: KiblBet105Repository, date: str) -> tuple[Dict[str, Any], List[Dict[str, Any]]]:
    filters = repo.build_filters(date=date, live_only=False)
    notes: List[str] = []
    fixtures = repo.fetch_fixture_summary(filters, notes)
    return filters, fixtures


def _market_bodies(filters: Dict[str, Any], fixture_ids: List[str]) -> List[tuple[str, Dict[str, Any]]]:
    start = filters.get("start_date") or filters.get("from")
    end = filters.get("end_date") or filters.get("to")
    base = {
        "feed_source_id": FEED_SOURCE_ID,
        "betting_type_id": PREMATCH_BETTING_TYPE_ID,
        "sport_id": SPORT_ID,
        "league_id": LEAGUE_ID,
        "start_date": start,
        "end_date": end,
        "from": start,
        "to": end,
        "offset": 0,
        "limit": 250,
    }
    core = {key: value for key, value in base.items() if key not in {"start_date", "end_date", "from", "to"}}
    bodies: List[tuple[str, Dict[str, Any]]] = [("base_dated", base), ("base_core", core)]
    for fixture_id in fixture_ids:
        bodies.extend(
            [
                (f"fixture_id:{fixture_id}:dated", {**base, "fixture_id": fixture_id}),
                (f"fixture_id:{fixture_id}:core", {**core, "fixture_id": fixture_id}),
                (f"event_id:{fixture_id}:dated", {**base, "event_id": fixture_id}),
                (f"event_id:{fixture_id}:core", {**core, "event_id": fixture_id}),
                (f"id:{fixture_id}:dated", {**base, "id": fixture_id}),
                (f"id:{fixture_id}:core", {**core, "id": fixture_id}),
            ]
        )
    if fixture_ids:
        bodies.extend(
            [
                ("fixture_ids_list:dated", {**base, "fixture_ids": fixture_ids}),
                ("fixture_ids_list:core", {**core, "fixture_ids": fixture_ids}),
                ("fixture_ids_csv:dated", {**base, "fixture_ids": ",".join(fixture_ids)}),
                ("fixture_ids_csv:core", {**core, "fixture_ids": ",".join(fixture_ids)}),
                ("event_ids_list:dated", {**base, "event_ids": fixture_ids}),
                ("ids_list:dated", {**base, "ids": fixture_ids}),
            ]
        )
    seen: set[str] = set()
    out: List[tuple[str, Dict[str, Any]]] = []
    for label, body in bodies:
        clean = _clean(body)
        fp = repr(sorted((key, str(value)) for key, value in clean.items()))
        if fp not in seen:
            seen.add(fp)
            out.append((label, clean))
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Probe Bet105 MLB market request shapes.")
    parser.add_argument("--date", default="2026-06-14", help="Slate date, YYYY-MM-DD")
    parser.add_argument("--fixture-id", action="append", default=[], help="Specific fixture ID to probe; can repeat")
    parser.add_argument("--max-fixtures", type=int, default=3, help="Max discovered fixture IDs to probe when --fixture-id is omitted")
    args = parser.parse_args()

    repo = KiblBet105Repository()
    filters, fixtures = _fixture_rows(repo, args.date)
    fixture_ids = args.fixture_id or [str(row.get("fixture_id")) for row in fixtures if row.get("fixture_id")][: args.max_fixtures]
    results = [_request(repo, label, body) for label, body in _market_bodies(filters, fixture_ids)]
    winners = [result for result in results if int(result.get("row_count") or 0) > 0]
    payload = {
        "status": "ok",
        "date": args.date,
        "scope": "bet105_mlb_market_probe",
        "fixture_count": len(fixtures),
        "fixture_ids_probed": fixture_ids,
        "request_count": len(results),
        "winner_count": len(winners),
        "winners": winners,
        "results_summary": [
            {
                "label": result.get("label"),
                "rows": result.get("row_count"),
                "fixtures": result.get("fixture_ids"),
                "markets": result.get("market_ids"),
                "types": result.get("market_type_ids"),
                "routing": result.get("routing_keys"),
                "error": result.get("error"),
            }
            for result in results
        ],
    }
    print(json.dumps(payload, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
