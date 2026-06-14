from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

from . import kibl_bet105_provider as legacy
from .kibl_bet105_types import Bet105RawBoard
from .kibl_client import KiblClient, find_rows


class KiblBet105Repository:
    market_summary_path = "info/markets"
    fixture_paths = ("info/fixtures", "fixtures", "events", "info/events", "info/games", "info/matches")
    metadata_paths = ("info/fixture_participants", "info/participants", "info/contestants", "info/competitors", "info/teams")

    def __init__(self, client: Optional[KiblClient] = None) -> None:
        self.client = client or KiblClient()

    def build_filters(self, date: Optional[str] = None, live_only: Optional[bool] = None, event_id: Optional[str] = None) -> Dict[str, Any]:
        filters = legacy.build_kibl_bet105_request_params(
            "live" if live_only else "events",
            date=date,
            live_only=live_only,
            event_id=event_id,
            include_markets=False,
        )
        filters.pop("from_cache", None)
        return filters

    def fetch_market_summary(self, filters: Dict[str, Any], notes: List[str]) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        limit = int(os.getenv("KIBL_SUMMARY_LIMIT", "1000"))
        max_pages = int(os.getenv("KIBL_SUMMARY_MAX_PAGES", "5"))
        for page in range(max_pages):
            offset = page * limit
            payload = self.client.post_summary(self.market_summary_path, filters, offset=offset, limit=limit)
            page_rows = find_rows(payload)
            notes.append(f"market_summary:{self.market_summary_path}:offset={offset}:limit={limit}:rows={len(page_rows)}")
            rows.extend(page_rows)
            if len(page_rows) < limit:
                break
        return rows

    @staticmethod
    def _add(ids: Dict[str, List[str]], key: str, value: Any) -> None:
        if value in (None, ""):
            return
        text = str(value)
        if text not in ids.setdefault(key, []):
            ids[key].append(text)

    def extract_ids(self, market_rows: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        ids: Dict[str, List[str]] = {key: [] for key in ("fixture_id", "market_id", "participant_id", "fixture_participant_id", "contestant_id", "line_id")}
        for row in market_rows:
            info = row.get("info") if isinstance(row.get("info"), dict) else {}
            for key in ("fixture_id", "market_id", "participant_id", "fixture_participant_id"):
                self._add(ids, key, row.get(key))
            self._add(ids, "contestant_id", info.get("contestant_id"))
            self._add(ids, "line_id", info.get("line_id"))
        return ids

    def _clean(self, filters: Dict[str, Any], compact: bool = False) -> Dict[str, Any]:
        drop = {"from_cache", "path", "combined_market_candidates"}
        if compact:
            drop.update({"start_date", "end_date", "from", "to"})
        return {key: value for key, value in filters.items() if key not in drop and value not in (None, "")}

    def _detail_bodies(self, filters: Dict[str, Any], ids: Dict[str, List[str]], source_keys: List[str]) -> List[tuple[str, Dict[str, Any]]]:
        bodies: List[tuple[str, Dict[str, Any]]] = []
        for root in (self._clean(filters), self._clean(filters, compact=True)):
            for source_key in source_keys:
                values = ids.get(source_key) or []
                if not values:
                    continue
                for body_key in (source_key, f"{source_key}s", "ids"):
                    bodies.append((f"{source_key}->{body_key}", {**root, body_key: values[:500]}))
        seen: set[str] = set()
        out: List[tuple[str, Dict[str, Any]]] = []
        for label, body in bodies:
            fingerprint = repr(sorted((key, str(value)) for key, value in body.items()))
            if fingerprint not in seen:
                seen.add(fingerprint)
                out.append((label, body))
        return out

    def fetch_details(self, paths: tuple[str, ...], filters: Dict[str, Any], ids: Dict[str, List[str]], keys: List[str], notes: List[str], label: str) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for path in paths:
            for body_label, body in self._detail_bodies(filters, ids, keys):
                try:
                    payload = self.client.post(path, body)
                    found = find_rows(payload)
                    notes.append(f"{label}_detail:{path}:{body_label}:rows={len(found)}")
                    if found:
                        rows.extend(found)
                        return rows
                except Exception as exc:
                    notes.append(f"{label}_detail_error:{path}:{body_label}:{str(exc)[:120]}")
        return rows

    def fetch_board(self, date: Optional[str] = None, live_only: Optional[bool] = None, event_id: Optional[str] = None) -> Bet105RawBoard:
        filters = self.build_filters(date=date, live_only=live_only, event_id=event_id)
        board = Bet105RawBoard(filters=filters)
        board.market_rows = self.fetch_market_summary(filters, board.notes)
        board.ids = self.extract_ids(board.market_rows)
        board.fixture_rows = self.fetch_details(self.fixture_paths, filters, board.ids, ["fixture_id"], board.notes, "fixture")
        board.participant_rows = self.fetch_details(
            self.metadata_paths,
            filters,
            board.ids,
            ["fixture_participant_id", "participant_id", "contestant_id", "line_id"],
            board.notes,
            "metadata",
        )
        return board
