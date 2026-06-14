#!/usr/bin/env python3
"""Fast baseball fixture/list contract smoke test.

Diagnostic-only. This intentionally tests a tiny set of likely fixture-list calls so
Railway shells do not get stuck on a giant endpoint matrix.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from mlb_app.kibl_bet105_repository import KiblBet105Repository
from mlb_app.kibl_client import KiblClient, find_rows


BASEBALL_LEAGUE_IDS = ("20", "643")
PATHS = ("info/fixtures", "info/events", "info/markets")


def _clean(body: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in body.items() if v not in (None, "", [], {})}


def _get(row: Dict[str, Any], key: str) -> Any:
    if key in row:
        return row.get(key)
    info = row.get("info") if isinstance(row.get("info"), dict) else {}
    return info.get(key)


def _unique(rows: Iterable[Dict[str, Any]], keys: Tuple[str, ...], limit: int = 20) -> List[str]:
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


def _keys(rows: List[Dict[str, Any]]) -> List[str]:
    seen: List[str] = []
    for row in rows[:3]:
        for key in row.keys():
            if key not in seen:
                seen.append(key)
    return seen


def _request(client: KiblClient, path: str, label: str, body: Dict[str, Any]) -> Dict[str, Any]:
    clean = _clean(body)
    try:
        payload = client.post(path, clean)
        rows = find_rows(payload)
        return {
            "label": label,
            "path": path,
            "body": clean,
            "row_count": len(rows),
            "fixture_ids": _unique(rows, ("fixture_id", "fixtureId", "id", "event_id", "eventId")),
            "market_ids": _unique(rows, ("market_id", "marketId")),
            "league_ids": _unique(rows, ("league_id", "leagueId", "competition_id", "competitionId")),
            "start_times": _unique(rows, ("start_date", "startDate", "start_time", "startTime", "event_start", "game_date", "date")),
            "routing_keys": _unique(rows, ("routing_key", "routingKey"), limit=5),
            "first_row_keys": _keys(rows),
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "label": label,
            "path": path,
            "body": clean,
            "row_count": 0,
            "error": str(exc),
        }


def _specs(filters: Dict[str, Any], slate_date: str | None) -> List[Tuple[str, Dict[str, Any]]]:
    feed = filters.get("feed_source_id")
    betting = filters.get("betting_type_id")
    start = filters.get("start_date") or filters.get("from")
    end = filters.get("end_date") or filters.get("to")
    combined = ",".join(BASEBALL_LEAGUE_IDS)
    specs: List[Tuple[str, Dict[str, Any]]] = []

    for league_id in BASEBALL_LEAGUE_IDS:
        specs.extend(
            [
                (f"league{league_id}_date", {"league_id": league_id, "start_date": start, "end_date": end, "offset": 0, "limit": 250}),
                (f"league{league_id}_book_date", {"feed_source_id": feed, "betting_type_id": betting, "league_id": league_id, "start_date": start, "end_date": end, "offset": 0, "limit": 250}),
                (f"league{league_id}_from_to", {"league_id": league_id, "from": start, "to": end, "offset": 0, "limit": 250}),
                (f"league{league_id}_book_from_to", {"feed_source_id": feed, "betting_type_id": betting, "league_id": league_id, "from": start, "to": end, "offset": 0, "limit": 250}),
                (f"league{league_id}_sport5_date", {"feed_source_id": feed, "betting_type_id": betting, "sport_id": 5, "league_id": league_id, "start_date": start, "end_date": end, "offset": 0, "limit": 250}),
                (f"league{league_id}_date_only", {"league_id": league_id, "date": slate_date, "offset": 0, "limit": 250}),
            ]
        )

    specs.extend(
        [
            ("combined_book_date", {"feed_source_id": feed, "betting_type_id": betting, "league_id": combined, "start_date": start, "end_date": end, "offset": 0, "limit": 250}),
            ("combined_book_from_to", {"feed_source_id": feed, "betting_type_id": betting, "league_id": combined, "from": start, "to": end, "offset": 0, "limit": 250}),
        ]
    )
    return specs


def main() -> int:
    parser = argparse.ArgumentParser(description="Fast baseball fixture-list smoke test.")
    parser.add_argument("--date", default=os.getenv("BET105_DEBUG_DATE"), help="Slate date, YYYY-MM-DD")
    parser.add_argument("--live", action="store_true", help="Use live type id")
    args = parser.parse_args()

    repo = KiblBet105Repository()
    client: KiblClient = repo.client
    filters = repo.build_filters(date=args.date, live_only=args.live)

    results: List[Dict[str, Any]] = []
    for path in PATHS:
        for label, body in _specs(filters, args.date):
            results.append(_request(client, path, f"{path}:{label}", body))

    winners = [r for r in results if int(r.get("row_count") or 0) > 1]
    payload = {
        "status": "ok",
        "scope": "baseball_fixture_discovery_fast",
        "date": args.date,
        "live": bool(args.live),
        "request_count": len(results),
        "winner_count": len(winners),
        "winners": winners,
        "results_summary": [
            {
                "label": r.get("label"),
                "rows": r.get("row_count"),
                "fixtures": r.get("fixture_ids"),
                "markets": r.get("market_ids"),
                "routing": r.get("routing_keys"),
                "error": r.get("error"),
            }
            for r in results
        ],
    }
    print(json.dumps(payload, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
