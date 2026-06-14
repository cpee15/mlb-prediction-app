from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Tuple

from . import kibl_bet105_provider as legacy
from .kibl_bet105_types import Bet105RawBoard
from .kibl_client import KiblClient, find_rows


class KiblBet105Repository:
    fixture_summary_path = "info/fixtures"
    market_summary_path = "info/markets"

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

    @staticmethod
    def _csv_values(value: Any) -> set[str]:
        if value in (None, ""):
            return set()
        if isinstance(value, (list, tuple, set)):
            return {str(item).strip() for item in value if item not in (None, "")}
        return {piece.strip() for piece in str(value).split(",") if piece.strip()}

    @staticmethod
    def _safe_text(value: Any) -> Optional[str]:
        if value in (None, ""):
            return None
        return str(value).strip()

    @staticmethod
    def _add(ids: Dict[str, List[str]], key: str, value: Any) -> None:
        if value in (None, ""):
            return
        text = str(value)
        if text not in ids.setdefault(key, []):
            ids[key].append(text)

    def _row_matches_requested_feed(self, row: Dict[str, Any], filters: Dict[str, Any]) -> bool:
        expected_feed = self._safe_text(filters.get("feed_source_id"))
        expected_betting = self._safe_text(filters.get("betting_type_id"))
        row_feed = self._safe_text(row.get("feed_source_id"))
        row_betting = self._safe_text(row.get("betting_type_id"))
        if expected_feed and row_feed and row_feed != expected_feed:
            return False
        if expected_betting and row_betting and row_betting != expected_betting:
            return False
        return True

    def _row_matches_requested_league(self, row: Dict[str, Any], filters: Dict[str, Any]) -> bool:
        requested = self._csv_values(filters.get("league_id"))
        if not requested:
            return True
        row_values: set[str] = set()
        for key in ("league_id", "leagueId", "competition_id", "competitionId", "sport_id", "sportId"):
            row_values.update(self._csv_values(row.get(key)))
        if not row_values:
            return True
        return bool(row_values.intersection(requested))

    def _filter_rows(self, rows: List[Dict[str, Any]], filters: Dict[str, Any], notes: List[str], label: str) -> List[Dict[str, Any]]:
        kept = [row for row in rows if self._row_matches_requested_feed(row, filters) and self._row_matches_requested_league(row, filters)]
        notes.append(
            f"{label}_filter:raw={len(rows)}:kept={len(kept)}:feed={filters.get('feed_source_id')}:betting={filters.get('betting_type_id')}:league={filters.get('league_id')}"
        )
        return kept

    def _summary_rows(self, path: str, body: Dict[str, Any], notes: List[str], label: str) -> List[Dict[str, Any]]:
        payload = self.client.post(path, body)
        rows = find_rows(payload)
        filtered = self._filter_rows(rows, body, notes, label)
        notes.append(f"{label}_request:{path}:keys={','.join(sorted(body.keys()))}:raw={len(rows)}:kept={len(filtered)}")
        return filtered

    def fetch_fixture_summary(self, filters: Dict[str, Any], notes: List[str]) -> List[Dict[str, Any]]:
        limit = int(os.getenv("KIBL_SUMMARY_LIMIT", "250"))
        body = {**filters, "offset": 0, "limit": limit}
        rows = self._summary_rows(self.fixture_summary_path, body, notes, "fixture")
        notes.append(f"fixture_summary:{self.fixture_summary_path}:offset=0:limit={limit}:rows={len(rows)}")
        return rows

    def fixture_ids_from_fixtures(self, fixture_rows: List[Dict[str, Any]]) -> List[str]:
        values: List[str] = []
        for row in fixture_rows:
            for key in ("fixture_id", "event_id", "id"):
                for value in self._csv_values(row.get(key)):
                    if value and value not in values:
                        values.append(value)
        return values

    def market_request_bodies(self, filters: Dict[str, Any], fixture_ids: List[str]) -> List[Tuple[str, Dict[str, Any]]]:
        clean = {key: value for key, value in filters.items() if key not in {"from_cache", "path", "combined_market_candidates"} and value not in (None, "")}
        core = {key: value for key, value in clean.items() if key not in {"start_date", "end_date", "from", "to"}}
        limit = int(os.getenv("KIBL_SUMMARY_LIMIT", "250"))
        roots = (("dated", clean), ("core", core))
        bodies: List[Tuple[str, Dict[str, Any]]] = []
        for root_label, root in roots:
            base = {**root, "offset": 0, "limit": limit}
            bodies.append((f"{root_label}:base", base))
            if fixture_ids:
                bodies.append((f"{root_label}:fixture_ids", {**base, "fixture_ids": fixture_ids[:100]}))
                bodies.append((f"{root_label}:event_ids", {**base, "event_ids": fixture_ids[:100]}))
                bodies.append((f"{root_label}:ids", {**base, "ids": fixture_ids[:100]}))
                bodies.append((f"{root_label}:fixture_ids_csv", {**base, "fixture_ids": ",".join(fixture_ids[:100])}))
                for value in fixture_ids[:20]:
                    bodies.append((f"{root_label}:fixture_id", {**base, "fixture_id": value}))
                    bodies.append((f"{root_label}:event_id", {**base, "event_id": value}))
                    bodies.append((f"{root_label}:id", {**base, "id": value}))
        seen: set[str] = set()
        out: List[Tuple[str, Dict[str, Any]]] = []
        for label, body in bodies:
            fp = repr(sorted((key, str(value)) for key, value in body.items()))
            if fp not in seen:
                seen.add(fp)
                out.append((label, body))
        return out

    def fetch_market_summary(self, filters: Dict[str, Any], fixture_ids: List[str], notes: List[str]) -> List[Dict[str, Any]]:
        best_label = "none"
        best_rows: List[Dict[str, Any]] = []
        candidates = self.market_request_bodies(filters, fixture_ids)
        for label, body in candidates:
            rows = self._summary_rows(self.market_summary_path, body, notes, f"market_{label}")
            notes.append(f"market_candidate:{label}:rows={len(rows)}")
            if len(rows) > len(best_rows):
                best_label = label
                best_rows = rows
        notes.append(f"market_selected:{best_label}:rows={len(best_rows)}:candidates={len(candidates)}")
        return best_rows

    def extract_ids(self, market_rows: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        ids: Dict[str, List[str]] = {key: [] for key in ("fixture_id", "market_id", "participant_id", "fixture_participant_id", "contestant_id", "line_id")}
        for row in market_rows:
            info = row.get("info") if isinstance(row.get("info"), dict) else {}
            for key in ("fixture_id", "market_id", "participant_id", "fixture_participant_id"):
                self._add(ids, key, row.get(key))
            self._add(ids, "contestant_id", info.get("contestant_id"))
            self._add(ids, "line_id", info.get("line_id"))
        return ids

    def fetch_board(self, date: Optional[str] = None, live_only: Optional[bool] = None, event_id: Optional[str] = None) -> Bet105RawBoard:
        filters = self.build_filters(date=date, live_only=live_only, event_id=event_id)
        board = Bet105RawBoard(filters=filters)
        board.fixture_rows = self.fetch_fixture_summary(filters, board.notes)
        fixture_ids = self.fixture_ids_from_fixtures(board.fixture_rows)
        board.notes.append(f"fixture_ids_from_summary:{len(fixture_ids)}")
        board.market_rows = self.fetch_market_summary(filters, fixture_ids, board.notes)
        board.ids = self.extract_ids(board.market_rows)
        board.notes.append(
            f"market_ids:fixtures={len(board.ids.get('fixture_id') or [])}:participants={len(board.ids.get('participant_id') or [])}:fixture_participants={len(board.ids.get('fixture_participant_id') or [])}:markets={len(board.ids.get('market_id') or [])}"
        )
        if not board.fixture_rows and board.ids.get("fixture_id"):
            board.notes.append("fixture_summary_empty:market_rows_have_fixture_ids")
        board.participant_rows = []
        return board
