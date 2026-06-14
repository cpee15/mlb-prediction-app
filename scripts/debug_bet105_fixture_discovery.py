#!/usr/bin/env python3
"""Discover the baseball fixture/list contract for KIBL without touching app routes.

This is intentionally diagnostic-only. It searches for the request shape that returns
multiple baseball fixtures before any production normalization changes are made.
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
PATHS = (
    "info/fixtures",
    "info/fixture",
    "fixtures",
    "fixture",
    "info/events",
    "info/event",
    "events",
    "event",
    "info/markets",
)
DATE_KEY_SETS = (
    ("start_date", "end_date"),
    ("from", "to"),
    ("startDate", "endDate"),
    ("date_from", "date_to"),
    ("date",),
)
SPORT_KEYS = ("sport_id", "sport_type_id", "sport", "sportId")
LEAGUE_KEYS = ("league_id", "leagueId", "competition_id", "competitionId")


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
    for row in rows[:10]:
        for key in row.keys():
            if key not in seen:
                seen.append(key)
    return seen


def _date_values(filters: Dict[str, Any], key_set: Tuple[str, ...], slate_date: str | None) -> Dict[str, Any]:
    if key_set == ("date",):
        return {"date": slate_date}
    src = {
        "start_date": filters.get("start_date"),
        "end_date": filters.get("end_date"),
        "from": filters.get("from"),
        "to": filters.get("to"),
    }
    first = src.get("start_date") or src.get("from")
    second = src.get("end_date") or src.get("to")
    return {key_set[0]: first, key_set[1]: second}


def _request(client: KiblClient, path: str, label: str, body: Dict[str, Any]) -> Dict[str, Any]:
    clean = _clean(body)
    try:
        payload = client.post(path, clean)
        rows = find_rows(payload)
        return {
            "label": label,
            "path": path,
            "body": clean,
            "body_keys": sorted(clean.keys()),
            "row_count": len(rows),
            "fixture_ids": _unique(rows, ("fixture_id", "fixtureId", "id", "event_id", "eventId")),
            "market_ids": _unique(rows, ("market_id", "marketId")),
            "league_ids": _unique(rows, ("league_id", "leagueId", "competition_id", "competitionId")),
            "sport_ids": _unique(rows, ("sport_id", "sportId", "sport_type_id", "sportTypeId")),
            "start_times": _unique(rows, ("start_date", "startDate", "start_time", "startTime", "event_start", "game_date", "date")),
            "routing_keys": _unique(rows, ("routing_key", "routingKey"), limit=5),
            "first_row_keys": _keys(rows),
            "first_row_sample": rows[:1],
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "label": label,
            "path": path,
            "body": clean,
            "body_keys": sorted(clean.keys()),
            "row_count": 0,
            "error": str(exc),
        }


def _body_specs(filters: Dict[str, Any], slate_date: str | None) -> List[Tuple[str, Dict[str, Any]]]:
    specs: List[Tuple[str, Dict[str, Any]]] = []
    feed = filters.get("feed_source_id")
    betting = filters.get("betting_type_id")

    for league_id in BASEBALL_LEAGUE_IDS:
        for league_key in LEAGUE_KEYS:
            league_part = {league_key: league_id}
            for date_keys in DATE_KEY_SETS:
                dates = _date_values(filters, date_keys, slate_date)
                specs.extend(
                    [
                        (f"{league_key}{league_id}_{'_'.join(date_keys)}", {**league_part, **dates, "offset": 0, "limit": 250}),
                        (f"book_{league_key}{league_id}_{'_'.join(date_keys)}", {"feed_source_id": feed, "betting_type_id": betting, **league_part, **dates, "offset": 0, "limit": 250}),
                    ]
                )
                for sport_key in SPORT_KEYS:
                    # Routing keys observed baseball rows with a sport-ish value of 5.
                    specs.append((f"sport5_{league_key}{league_id}_{'_'.join(date_keys)}", {"feed_source_id": feed, "betting_type_id": betting, sport_key: 5, **league_part, **dates, "offset": 0, "limit": 250}))

            specs.extend(
                [
                    (f"{league_key}{league_id}_core", {**league_part, "offset": 0, "limit": 250}),
                    (f"book_{league_key}{league_id}_core", {"feed_source_id": feed, "betting_type_id": betting, **league_part, "offset": 0, "limit": 250}),
                ]
            )

    # Keep the existing combined league body as a control, but still baseball-scoped.
    for date_keys in DATE_KEY_SETS:
        dates = _date_values(filters, date_keys, slate_date)
        specs.append((f"combined_league_{'_'.join(date_keys)}", {"feed_source_id": feed, "betting_type_id": betting, "league_id": ",".join(BASEBALL_LEAGUE_IDS), **dates, "offset": 0, "limit": 250}))

    seen: set[str] = set()
    out: List[Tuple[str, Dict[str, Any]]] = []
    for label, body in specs:
        clean = _clean(body)
        # Guardrail: every request must include a baseball league value under a known league key.
        if not any(str(clean.get(key, "")) in BASEBALL_LEAGUE_IDS or str(clean.get(key, "")) == ",".join(BASEBALL_LEAGUE_IDS) for key in LEAGUE_KEYS):
            continue
        fp = repr(sorted((k, str(v)) for k, v in clean.items()))
        if fp not in seen:
            seen.add(fp)
            out.append((label, clean))
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Discover baseball fixture/list request shapes.")
    parser.add_argument("--date", default=os.getenv("BET105_DEBUG_DATE"), help="Slate date, YYYY-MM-DD")
    parser.add_argument("--live", action="store_true", help="Use live type id")
    parser.add_argument("--raw-samples", action="store_true", help="Include first-row samples in output")
    args = parser.parse_args()

    repo = KiblBet105Repository()
    client: KiblClient = repo.client
    filters = repo.build_filters(date=args.date, live_only=args.live)

    results: List[Dict[str, Any]] = []
    specs = _body_specs(filters, args.date)
    for path in PATHS:
        for label, body in specs:
            results.append(_request(client, path, f"{path}:{label}", body))

    if not args.raw_samples:
        for result in results:
            result.pop("first_row_sample", None)

    winners = [r for r in results if int(r.get("row_count") or 0) > 1]
    fixture_winners = [r for r in winners if r.get("fixture_ids")]
    payload = {
        "status": "ok",
        "scope": "baseball_fixture_discovery",
        "baseball_league_ids": list(BASEBALL_LEAGUE_IDS),
        "date": args.date,
        "live": bool(args.live),
        "request_count": len(results),
        "winner_count": len(winners),
        "fixture_winner_count": len(fixture_winners),
        "winners": winners[:50],
        "fixture_winners": fixture_winners[:50],
        "results": results,
    }
    print(json.dumps(payload, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
