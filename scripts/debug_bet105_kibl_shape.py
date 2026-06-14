#!/usr/bin/env python3
"""Capture redacted Bet105/KIBL payload shape summaries.

This script is intentionally diagnostic-only. It authenticates with the same
KIBL provider code used by production, calls fixtures and markets directly, and
prints/saves shape metadata without writing tokens or passwords.

Usage:
    python scripts/debug_bet105_kibl_shape.py --date 2026-06-14 --live false
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List

from mlb_app import kibl_bet105_provider as base


def _walk_dicts(value: Any) -> Iterable[Dict[str, Any]]:
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _walk_dicts(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk_dicts(child)


def _shape(value: Any, limit: int = 8) -> Any:
    if isinstance(value, dict):
        return {str(key): _shape(child, limit=limit) for key, child in list(value.items())[:limit]}
    if isinstance(value, list):
        return [_shape(value[0], limit=limit)] if value else []
    return type(value).__name__


def _first_rows(payload: Any, limit: int = 3) -> List[Dict[str, Any]]:
    rows = base._find_list_payload(payload)
    return [row for row in rows[:limit] if isinstance(row, dict)]


def _summarize_payload(kind: str, path: str, request_body: Dict[str, Any], payload: Any) -> Dict[str, Any]:
    rows = _first_rows(payload, limit=5)
    all_dicts = list(_walk_dicts(payload))
    return {
        "kind": kind,
        "path": path,
        "request_body": base._redact(request_body),
        "top_level_type": type(payload).__name__,
        "top_level_keys": list(payload.keys()) if isinstance(payload, dict) else None,
        "list_item_count": len(base._find_list_payload(payload)),
        "sample_shape": _shape(payload),
        "sample_rows": base._redact(rows),
        "all_keys_sample": sorted({key for item in all_dicts[:100] for key in item.keys()})[:200],
        "fixture_id_fields_seen": sorted(
            {
                key
                for item in all_dicts[:100]
                for key in item.keys()
                if "fixture" in str(key).lower() or str(key).lower() in {"event_id", "eventid", "id"}
            }
        ),
        "market_type_ids_seen": sorted(
            {
                str(base._extract_first(item, ("market_type_id", "marketTypeId", "marketTypeID")))
                for item in all_dicts
                if base._extract_first(item, ("market_type_id", "marketTypeId", "marketTypeID")) is not None
            }
        )[:50],
        "side_ids_seen": sorted(
            {
                str(base._extract_first(item, ("participant_side_id", "side_id", "sideId", "participantSideId")))
                for item in all_dicts
                if base._extract_first(item, ("participant_side_id", "side_id", "sideId", "participantSideId")) is not None
            }
        )[:50],
        "price_fields_seen": sorted(
            {
                key
                for item in all_dicts[:100]
                for key in item.keys()
                if "price" in str(key).lower() or "odds" in str(key).lower() or str(key).lower() in {"american", "decimal"}
            }
        ),
    }


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Slate date in YYYY-MM-DD.")
    parser.add_argument("--live", default="false", choices=("true", "false"))
    parser.add_argument("--out-dir", default="docs/debug")
    args = parser.parse_args()

    live_only = args.live == "true"
    scope = "live" if live_only else "events"
    params = base.build_kibl_bet105_request_params(scope, date=args.date, live_only=live_only)

    output_dir = Path(args.out_dir)
    summaries: List[Dict[str, Any]] = []

    for kind in ("fixtures", "markets"):
        payload, path = base._fetch_kibl_payload(scope, params, kind=kind)
        summary = _summarize_payload(kind, path, params, payload)
        summaries.append(summary)
        _write_json(output_dir / f"bet105_{args.date}_{kind}_shape.json", summary)

    combined = {"date": args.date, "live": live_only, "summaries": summaries}
    _write_json(output_dir / f"bet105_{args.date}_shape_summary.json", combined)
    print(json.dumps(combined, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
