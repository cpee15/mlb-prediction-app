#!/usr/bin/env python3
"""Capture the KIBL Bet105 request/response contract without touching the app route.

Run this only in an environment that already has KIBL credentials configured.
It intentionally prints compact, redacted summaries rather than full payloads.
"""

from __future__ import annotations

import argparse
import json
import os
from typing import Any, Dict, Iterable, List, Tuple

from mlb_app.kibl_bet105_repository import KiblBet105Repository
from mlb_app.kibl_client import KiblClient, find_rows


MARKET_PATH = "info/markets"
FIXTURE_PATH = "info/fixtures"


def _clean_body(body: Dict[str, Any]) -> Dict[str, Any]:
    return {key: value for key, value in body.items() if value not in (None, "", [], {})}


def _row_value(row: Dict[str, Any], key: str) -> Any:
    if key in row:
        return row.get(key)
    info = row.get("info") if isinstance(row.get("info"), dict) else {}
    return info.get(key)


def _unique(rows: Iterable[Dict[str, Any]], key: str, limit: int = 12) -> List[str]:
    values: List[str] = []
    for row in rows:
        value = _row_value(row, key)
        if value in (None, ""):
            continue
        text = str(value)
        if text not in values:
            values.append(text)
        if len(values) >= limit:
            break
    return values


def _keys(rows: List[Dict[str, Any]]) -> List[str]:
    seen: List[str] = []
    for row in rows[:10]:
        for key in row.keys():
            if key not in seen:
                seen.append(key)
    return seen


def _request(client: KiblClient, path: str, label: str, body: Dict[str, Any]) -> Dict[str, Any]:
    clean = _clean_body(body)
    try:
        payload = client.post(path, clean)
        rows = find_rows(payload)
        return {
            "label": label,
            "path": path,
            "body_keys": sorted(clean.keys()),
            "row_count": len(rows),
            "fixture_ids": _unique(rows, "fixture_id"),
            "market_ids": _unique(rows, "market_id"),
            "market_type_ids": _unique(rows, "market_type_id"),
            "participant_ids": _unique(rows, "participant_id"),
            "fixture_participant_ids": _unique(rows, "fixture_participant_id"),
            "line_ids": _unique(rows, "line_id"),
            "contestant_ids": _unique(rows, "contestant_id"),
            "side_ids": _unique(rows, "side_id"),
            "sides": _unique(rows, "side"),
            "first_row_keys": _keys(rows),
            "first_row_sample": rows[:1],
        }
    except Exception as exc:  # noqa: BLE001 - debug capture should report every failed shape.
        return {
            "label": label,
            "path": path,
            "body_keys": sorted(clean.keys()),
            "row_count": 0,
            "error": str(exc),
        }


def _seeded_market_bodies(filters: Dict[str, Any], seed: Dict[str, Any]) -> List[Tuple[str, Dict[str, Any]]]:
    base = dict(filters)
    body_specs: List[Tuple[str, Dict[str, Any]]] = [("base", base)]

    # Direct row-derived fields from the one known-good Bet105 market row.
    for key in (
        "fixture_id",
        "event_id",
        "market_id",
        "market_type_id",
        "segment_id",
        "participant_id",
        "fixture_participant_id",
        "side_id",
        "line_id",
        "contestant_id",
    ):
        value = _row_value(seed, key)
        if value not in (None, ""):
            body_specs.append((key, {**base, key: value}))

    fixture_id = _row_value(seed, "fixture_id")
    market_id = _row_value(seed, "market_id")
    market_type_id = _row_value(seed, "market_type_id")
    line_id = _row_value(seed, "line_id")
    contestant_id = _row_value(seed, "contestant_id")

    combos = [
        ("fixture_id+market_id", {"fixture_id": fixture_id, "market_id": market_id}),
        ("fixture_id+market_type_id", {"fixture_id": fixture_id, "market_type_id": market_type_id}),
        ("fixture_id+line_id", {"fixture_id": fixture_id, "line_id": line_id}),
        ("fixture_id+contestant_id", {"fixture_id": fixture_id, "contestant_id": contestant_id}),
        ("market_id+line_id", {"market_id": market_id, "line_id": line_id}),
    ]
    for label, extra in combos:
        clean_extra = _clean_body(extra)
        if clean_extra:
            body_specs.append((label, {**base, **clean_extra}))

    # Check whether offset acts as a cursor without multiplying production requests.
    for offset in range(0, int(os.getenv("BET105_CONTRACT_PROBE_OFFSETS", "6"))):
        body_specs.append((f"base_offset_{offset}", {**base, "offset": offset, "limit": 250}))

    seen: set[str] = set()
    out: List[Tuple[str, Dict[str, Any]]] = []
    for label, body in body_specs:
        fp = repr(sorted((key, str(value)) for key, value in _clean_body(body).items()))
        if fp not in seen:
            seen.add(fp)
            out.append((label, body))
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Capture KIBL Bet105 request contract summaries.")
    parser.add_argument("--date", default=os.getenv("BET105_DEBUG_DATE"), help="Slate date, YYYY-MM-DD")
    parser.add_argument("--live", action="store_true", help="Use live betting_type_id")
    parser.add_argument("--raw-samples", action="store_true", help="Include first-row samples in output")
    args = parser.parse_args()

    repo = KiblBet105Repository()
    client = repo.client
    filters = repo.build_filters(date=args.date, live_only=args.live)

    base_result = _request(client, MARKET_PATH, "base", {**filters, "offset": 0, "limit": 250})
    results = [base_result]
    seed_rows = base_result.get("first_row_sample") or []
    seed = seed_rows[0] if seed_rows and isinstance(seed_rows[0], dict) else {}

    if seed:
        for label, body in _seeded_market_bodies(filters, seed):
            if label == "base":
                continue
            results.append(_request(client, MARKET_PATH, label, body))

        fixture_id = _row_value(seed, "fixture_id")
        if fixture_id not in (None, ""):
            results.append(_request(client, FIXTURE_PATH, "fixture_by_fixture_id", {**filters, "fixture_id": fixture_id, "offset": 0, "limit": 250}))
            results.append(_request(client, FIXTURE_PATH, "fixture_by_id", {**filters, "id": fixture_id, "offset": 0, "limit": 250}))

    if not args.raw_samples:
        for result in results:
            result.pop("first_row_sample", None)

    payload = {
        "status": "ok" if results else "empty",
        "date": args.date,
        "live": bool(args.live),
        "base_filters": filters,
        "result_count": len(results),
        "results": results,
    }
    print(json.dumps(payload, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
