from __future__ import annotations

import os
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from . import kibl_bet105_provider as legacy
from .kibl_bet105_types import Bet105RawBoard
from .kibl_client import KiblClient, find_rows


class KiblBet105Repository:
    fixture_summary_path = "info/fixtures"
    market_summary_path = "info/markets"
    mlb_sport_id = "2"
    mlb_league_id = "7"
    game_fixture_type_id = "1"
    eastern_tz = ZoneInfo("America/New_York")
    game_line_market_type_ids = ("1", "2", "3")
    fixture_excluded_filter_keys = {
        "feed_source_id",
        "betting_type_id",
        "from_cache",
        "path",
        "combined_market_candidates",
        "markets",
        "league_id",
        "sport_id",
    }

    def __init__(self, client: Optional[KiblClient] = None) -> None:
        self.client = client or KiblClient()
        self.markets_meta: Dict[str, Any] = {"fixture_request_summaries": []}

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

    @staticmethod
    def _info(row: Dict[str, Any]) -> Dict[str, Any]:
        return row.get("info") if isinstance(row.get("info"), dict) else {}

    @classmethod
    def _field(cls, row: Dict[str, Any], *keys: str) -> Any:
        info = cls._info(row)
        for key in keys:
            if row.get(key) not in (None, ""):
                return row.get(key)
            if info.get(key) not in (None, ""):
                return info.get(key)
        return None

    @classmethod
    def _unique(cls, rows: List[Dict[str, Any]], *keys: str, limit: int = 25) -> List[str]:
        values: List[str] = []
        for row in rows:
            value = cls._field(row, *keys)
            if value in (None, ""):
                continue
            text = str(value)
            if text not in values:
                values.append(text)
            if len(values) >= limit:
                break
        return values

    @classmethod
    def _distribution(cls, rows: List[Dict[str, Any]], *keys: str) -> Dict[str, int]:
        counts: Counter[str] = Counter()
        for row in rows:
            value = cls._field(row, *keys)
            if value not in (None, ""):
                counts[str(value)] += 1
        return dict(counts)

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
        requested_leagues = self._csv_values(filters.get("league_id"))
        requested_sports = self._csv_values(filters.get("sport_id"))
        if not requested_leagues and not requested_sports:
            return True
        row_leagues: set[str] = set()
        row_sports: set[str] = set()
        for key in ("league_id", "leagueId", "competition_id", "competitionId"):
            row_leagues.update(self._csv_values(row.get(key)))
        for key in ("sport_id", "sportId"):
            row_sports.update(self._csv_values(row.get(key)))
        if requested_leagues and row_leagues and not row_leagues.intersection(requested_leagues):
            return False
        if requested_sports and row_sports and not row_sports.intersection(requested_sports):
            return False
        return True

    def _filter_rows(self, rows: List[Dict[str, Any]], filters: Dict[str, Any], notes: List[str], label: str) -> List[Dict[str, Any]]:
        kept = [row for row in rows if self._row_matches_requested_feed(row, filters) and self._row_matches_requested_league(row, filters)]
        notes.append(
            f"{label}_filter:raw={len(rows)}:kept={len(kept)}:feed={filters.get('feed_source_id')}:betting={filters.get('betting_type_id')}:sport={filters.get('sport_id')}:league={filters.get('league_id')}"
        )
        return kept

    def _summary_rows(self, path: str, body: Dict[str, Any], notes: List[str], label: str) -> List[Dict[str, Any]]:
        payload = self.client.post(path, body)
        rows = find_rows(payload)
        filtered = self._filter_rows(rows, body, notes, label)
        notes.append(f"{label}_request:{path}:keys={','.join(sorted(body.keys()))}:raw={len(rows)}:kept={len(filtered)}")
        return filtered

    def _paged_summary_rows(self, path: str, body: Dict[str, Any], notes: List[str], label: str) -> List[Dict[str, Any]]:
        limit = int(os.getenv("KIBL_SUMMARY_LIMIT", "250"))
        max_pages = int(os.getenv("KIBL_SUMMARY_MAX_PAGES", "20"))
        rows: List[Dict[str, Any]] = []
        for page in range(max_pages):
            offset = page * limit
            page_body = {**body, "offset": offset, "limit": limit}
            page_rows = self._summary_rows(path, page_body, notes, f"{label}:page{page}")
            notes.append(f"{label}_page:{path}:offset={offset}:limit={limit}:rows={len(page_rows)}")
            rows.extend(page_rows)
            if len(page_rows) < limit:
                break
        return rows

    def _date_body(self, filters: Dict[str, Any]) -> Dict[str, Any]:
        return {key: value for key, value in filters.items() if key not in self.fixture_excluded_filter_keys and value not in (None, "")}

    def _fixture_request_bodies(self, filters: Dict[str, Any]) -> List[Tuple[str, Dict[str, Any]]]:
        body = {**self._date_body(filters), "sport_id": self.mlb_sport_id, "league_id": self.mlb_league_id}
        return [("mlb_sport2_league7", body)]

    def _selected_eastern_date(self, filters: Dict[str, Any]) -> Optional[str]:
        for key in ("start_date", "from", "date"):
            value = self._safe_text(filters.get(key))
            if value:
                return value[:10]
        return None

    def _parse_utc_start(self, value: Any) -> Optional[datetime]:
        text = self._safe_text(value)
        if not text:
            return None
        normalized = text.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            try:
                parsed = datetime.strptime(text[:19], "%Y-%m-%dT%H:%M:%S")
            except ValueError:
                return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    def _is_selected_mlb_game_fixture(self, row: Dict[str, Any], selected_date: Optional[str]) -> bool:
        if self._safe_text(row.get("sport_id")) != self.mlb_sport_id:
            return False
        if self._safe_text(row.get("league_id")) != self.mlb_league_id:
            return False
        if self._safe_text(row.get("fixture_type_id")) != self.game_fixture_type_id:
            return False
        if selected_date:
            start = self._parse_utc_start(row.get("start_time") or row.get("start_date") or row.get("date"))
            if not start:
                return False
            if start.astimezone(self.eastern_tz).date().isoformat() != selected_date:
                return False
        return True

    def _filter_fixture_rows_to_selected_games(self, rows: List[Dict[str, Any]], filters: Dict[str, Any], notes: List[str]) -> List[Dict[str, Any]]:
        selected_date = self._selected_eastern_date(filters)
        kept = [row for row in rows if self._is_selected_mlb_game_fixture(row, selected_date)]
        notes.append(f"fixture_game_filter:selected_date={selected_date}:raw={len(rows)}:kept={len(kept)}:sport_id=2:league_id=7:fixture_type_id=1")
        return kept

    def _dedupe_fixture_rows(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        seen: set[str] = set()
        out: List[Dict[str, Any]] = []
        for idx, row in enumerate(rows):
            fixture_id = None
            for key in ("fixture_id", "event_id", "id"):
                values = self._csv_values(row.get(key))
                if values:
                    fixture_id = sorted(values)[0]
                    break
            fp = fixture_id or f"row:{idx}:{repr(sorted(row.items()))[:200]}"
            if fp not in seen:
                seen.add(fp)
                out.append(row)
        return out

    def fetch_fixture_summary(self, filters: Dict[str, Any], notes: List[str]) -> List[Dict[str, Any]]:
        all_rows: List[Dict[str, Any]] = []
        for label, body in self._fixture_request_bodies(filters):
            rows = self._paged_summary_rows(self.fixture_summary_path, body, notes, f"fixture:{label}")
            notes.append(f"fixture_candidate:{label}:keys={','.join(sorted(body.keys()))}:rows={len(rows)}:book_filters_removed={not any(key in body for key in ('feed_source_id', 'betting_type_id'))}")
            all_rows.extend(rows)
        game_rows = self._filter_fixture_rows_to_selected_games(all_rows, filters, notes)
        deduped = self._dedupe_fixture_rows(game_rows)
        notes.append(f"fixture_summary:{self.fixture_summary_path}:raw={len(all_rows)}:games={len(game_rows)}:deduped={len(deduped)}:scope=sport_id=2,league_id=7")
        return deduped

    def fixture_ids_from_fixtures(self, fixture_rows: List[Dict[str, Any]]) -> List[str]:
        values: List[str] = []
        for row in fixture_rows:
            for key in ("fixture_id", "event_id", "id"):
                for value in self._csv_values(row.get(key)):
                    if value and value not in values:
                        values.append(value)
        return values

    def _market_rows_matching_fixtures(self, rows: List[Dict[str, Any]], fixture_ids: List[str], notes: List[str], label: str) -> List[Dict[str, Any]]:
        if not fixture_ids:
            return rows
        allowed = set(str(value) for value in fixture_ids)
        kept = [row for row in rows if str(row.get("fixture_id") or row.get("event_id") or row.get("id") or "") in allowed]
        dropped = len(rows) - len(kept)
        if dropped:
            notes.append(f"{label}:dropped_non_selected_mlb_market_rows={dropped}:allowed_fixture_ids={len(allowed)}")
        return kept

    def _clean_market_body(self, filters: Dict[str, Any]) -> Dict[str, Any]:
        clean = {key: value for key, value in filters.items() if key not in {"from_cache", "path", "combined_market_candidates", "league_id", "sport_id"} and value not in (None, "")}
        clean.update({"sport_id": self.mlb_sport_id, "league_id": self.mlb_league_id})
        return clean

    def market_request_bodies(self, filters: Dict[str, Any], fixture_ids: List[str]) -> List[Tuple[str, Dict[str, Any]]]:
        clean = self._clean_market_body(filters)
        if not fixture_ids:
            return [("dated:base", clean)]
        fixture_id_limit = int(os.getenv("KIBL_MARKET_FIXTURE_ID_LIMIT", str(len(fixture_ids))))
        limited_fixture_ids = fixture_ids[: max(0, fixture_id_limit)]
        bodies: List[Tuple[str, Dict[str, Any]]] = []
        for fixture_id in limited_fixture_ids:
            root = {**clean, "fixture_id": fixture_id}
            bodies.append((f"dated:fixture_id:{fixture_id}", root))
            for market_type_id in self.game_line_market_type_ids:
                bodies.append((f"dated:fixture_id:{fixture_id}:market_type_id:{market_type_id}", {**root, "market_type_id": int(market_type_id)}))
            bodies.append((f"dated:fixture_id:{fixture_id}:market_type_ids:list", {**root, "market_type_ids": [1, 2, 3]}))
            bodies.append((f"dated:fixture_id:{fixture_id}:market_type_ids:csv", {**root, "market_type_ids": "1,2,3"}))
        seen: set[str] = set()
        out: List[Tuple[str, Dict[str, Any]]] = []
        for label, body in bodies:
            fp = repr(sorted((key, str(value)) for key, value in body.items()))
            if fp not in seen:
                seen.add(fp)
                out.append((label, body))
        return out

    def _market_fingerprint(self, row: Dict[str, Any]) -> str:
        info = row.get("info") if isinstance(row.get("info", {}), dict) else {}
        parts = [row.get("fixture_id"), row.get("market_id"), row.get("participant_id"), row.get("fixture_participant_id"), row.get("market_type_id"), row.get("segment_id"), row.get("point"), row.get("side_id"), info.get("line_id"), info.get("contestant_id"), row.get("price_american"), row.get("price_decimal")]
        return "|".join(str(part) for part in parts)

    def _dedupe_market_rows(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        seen: set[str] = set()
        out: List[Dict[str, Any]] = []
        for row in rows:
            fp = self._market_fingerprint(row)
            if fp not in seen:
                seen.add(fp)
                out.append(row)
        return out

    def _market_rows_for_body(self, body: Dict[str, Any], notes: List[str], label: str, fixture_ids: List[str]) -> List[Dict[str, Any]]:
        payload = self.client.post(self.market_summary_path, body)
        raw_rows = find_rows(payload)
        feed_rows = self._filter_rows(raw_rows, body, notes, f"market_full:{label}")
        kept_rows = self._market_rows_matching_fixtures(feed_rows, fixture_ids, notes, f"market_full:{label}")
        fixture_id = str(body.get("fixture_id")) if body.get("fixture_id") not in (None, "") else None
        summary = {
            "fixture_id": fixture_id,
            "request_label": label,
            "request_keys": sorted(body.keys()),
            "raw_rows": len(raw_rows),
            "feed_kept_rows": len(feed_rows),
            "kept_rows": len(kept_rows),
            "market_type_ids": self._distribution(kept_rows, "market_type_id", "marketTypeId"),
            "market_ids": self._unique(kept_rows, "market_id", "marketId"),
            "participant_ids": self._unique(kept_rows, "participant_id", "participantId"),
            "fixture_participant_ids": self._unique(kept_rows, "fixture_participant_id", "fixtureParticipantId"),
            "price_american_count": sum(1 for row in kept_rows if self._field(row, "price_american", "american", "price") not in (None, "")),
            "routing_keys_sample": self._unique(kept_rows, "routing_key", "routingKey", limit=10),
        }
        self.markets_meta.setdefault("fixture_request_summaries", []).append(summary)
        notes.append(f"fixture_market_summary:fixture_id={fixture_id}:label={label}:raw={len(raw_rows)}:kept={len(kept_rows)}:types={summary['market_type_ids']}")
        return kept_rows

    def fetch_market_candidates(self, filters: Dict[str, Any], fixture_ids: List[str], notes: List[str], stage: str) -> List[Dict[str, Any]]:
        all_rows: List[Dict[str, Any]] = []
        candidates = self.market_request_bodies(filters, fixture_ids)
        notes.append(f"fixture_scoped_market_requests:enabled:fixture_ids_available={len(fixture_ids)}:requests={len(candidates)}:market_type_probes=1,2,3")
        for label, body in candidates:
            try:
                rows = self._market_rows_for_body(body, notes, f"{stage}:{label}", fixture_ids)
            except Exception as exc:
                notes.append(f"fixture_market_summary_error:label={stage}:{label}:error={exc}")
                continue
            notes.append(f"market_candidate:{stage}:{label}:rows={len(rows)}")
            all_rows.extend(rows)
        deduped = self._dedupe_market_rows(all_rows)
        self.markets_meta["fixture_count"] = len(fixture_ids)
        self.markets_meta["request_count"] = len(candidates)
        self.markets_meta["raw_union_rows"] = len(all_rows)
        self.markets_meta["deduped_rows"] = len(deduped)
        self.markets_meta["market_type_ids"] = self._distribution(deduped, "market_type_id", "marketTypeId")
        notes.append(f"market_union:{stage}:raw={len(all_rows)}:deduped={len(deduped)}:candidates={len(candidates)}")
        return deduped

    def fetch_market_summary(self, filters: Dict[str, Any], fixture_ids: List[str], notes: List[str]) -> List[Dict[str, Any]]:
        rows = self.fetch_market_candidates(filters, fixture_ids, notes, "fixture_id")
        notes.append(f"market_selected:fixture_id_rows={len(rows)}:types={self.markets_meta.get('market_type_ids', {})}")
        return rows

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
        board.notes.append(f"market_ids:fixtures={len(board.ids.get('fixture_id') or [])}:participants={len(board.ids.get('participant_id') or [])}:fixture_participants={len(board.ids.get('fixture_participant_id') or [])}:markets={len(board.ids.get('market_id') or [])}")
        if not board.fixture_rows and board.ids.get("fixture_id"):
            board.notes.append("fixture_summary_empty:market_rows_have_fixture_ids")
        if board.fixture_rows and not board.market_rows:
            board.notes.append("fixture_summary_present:market_rows_empty")
        board.participant_rows = []
        setattr(board, "markets_meta", self.markets_meta)
        return board
