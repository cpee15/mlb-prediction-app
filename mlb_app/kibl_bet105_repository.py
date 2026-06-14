from __future__ import annotations

import os
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
        return {
            key: value
            for key, value in filters.items()
            if key not in self.fixture_excluded_filter_keys and value not in (None, "")
        }

    def _fixture_request_bodies(self, filters: Dict[str, Any]) -> List[Tuple[str, Dict[str, Any]]]:
        body = {
            **self._date_body(filters),
            "sport_id": self.mlb_sport_id,
            "league_id": self.mlb_league_id,
        }
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
        # KIBL routing shows fixture_type_id=1 for real games; 3=props, 4=futures/outrights, 6=other alternates.
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
        notes.append(
            f"fixture_game_filter:selected_date={selected_date}:raw={len(rows)}:kept={len(kept)}:sport_id=2:league_id=7:fixture_type_id=1"
        )
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
        bodies = self._fixture_request_bodies(filters)
        for label, body in bodies:
            rows = self._paged_summary_rows(self.fixture_summary_path, body, notes, f"fixture:{label}")
            notes.append(
                f"fixture_candidate:{label}:keys={','.join(sorted(body.keys()))}:rows={len(rows)}:book_filters_removed={not any(key in body for key in ('feed_source_id', 'betting_type_id'))}"
            )
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

    def fixture_ids_from_markets(self, market_rows: List[Dict[str, Any]]) -> List[str]:
        values: List[str] = []
        for row in market_rows:
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

    def market_request_bodies(self, filters: Dict[str, Any], fixture_ids: List[str]) -> List[Tuple[str, Dict[str, Any]]]:
        clean = {
            key: value
            for key, value in filters.items()
            if key not in {"from_cache", "path", "combined_market_candidates", "league_id", "sport_id"} and value not in (None, "")
        }
        clean.update({"sport_id": self.mlb_sport_id, "league_id": self.mlb_league_id})
        core = {key: value for key, value in clean.items() if key not in {"start_date", "end_date", "from", "to"}}
        roots = (("dated", clean), ("core", core))
        fixture_seeded_enabled = os.getenv("KIBL_ENABLE_FIXTURE_SEEDED_MARKETS", "false").strip().lower() in {"1", "true", "yes", "on"}
        fixture_id_limit = int(os.getenv("KIBL_MARKET_FIXTURE_ID_LIMIT", "0"))
        batch_limit = int(os.getenv("KIBL_MARKET_FIXTURE_BATCH_LIMIT", "0"))
        limited_fixture_ids = fixture_ids[: max(0, fixture_id_limit)]
        batched_fixture_ids = fixture_ids[: max(0, batch_limit)]
        bodies: List[Tuple[str, Dict[str, Any]]] = []
        for root_label, root in roots:
            bodies.append((f"{root_label}:base", root))
            if fixture_ids and fixture_seeded_enabled:
                if batched_fixture_ids:
                    bodies.append((f"{root_label}:fixture_ids", {**root, "fixture_ids": batched_fixture_ids}))
                    bodies.append((f"{root_label}:event_ids", {**root, "event_ids": batched_fixture_ids}))
                    bodies.append((f"{root_label}:ids", {**root, "ids": batched_fixture_ids}))
                    bodies.append((f"{root_label}:fixture_ids_csv", {**root, "fixture_ids": ",".join(batched_fixture_ids)}))
                for value in limited_fixture_ids:
                    bodies.append((f"{root_label}:fixture_id", {**root, "fixture_id": value}))
                    bodies.append((f"{root_label}:event_id", {**root, "event_id": value}))
                    bodies.append((f"{root_label}:id", {**root, "id": value}))
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
        parts = [
            row.get("fixture_id"),
            row.get("market_id"),
            row.get("participant_id"),
            row.get("fixture_participant_id"),
            row.get("market_type_id"),
            row.get("segment_id"),
            row.get("point"),
            row.get("side_id"),
            info.get("line_id"),
            info.get("contestant_id"),
            row.get("price_american"),
            row.get("price_decimal"),
        ]
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

    def fetch_market_candidates(self, filters: Dict[str, Any], fixture_ids: List[str], notes: List[str], stage: str) -> List[Dict[str, Any]]:
        all_rows: List[Dict[str, Any]] = []
        candidates = self.market_request_bodies(filters, fixture_ids)
        if fixture_ids and os.getenv("KIBL_ENABLE_FIXTURE_SEEDED_MARKETS", "false").strip().lower() not in {"1", "true", "yes", "on"}:
            notes.append(
                "fixture_seeded_market_requests_disabled:set_KIBL_ENABLE_FIXTURE_SEEDED_MARKETS=true_to_enable:"
                f"fixture_ids_available={len(fixture_ids)}"
            )
        for label, body in candidates:
            rows = self._paged_summary_rows(self.market_summary_path, body, notes, f"market_{stage}:{label}")
            rows = self._market_rows_matching_fixtures(rows, fixture_ids, notes, f"market_{stage}:{label}")
            notes.append(f"market_candidate:{stage}:{label}:rows={len(rows)}")
            all_rows.extend(rows)
        deduped = self._dedupe_market_rows(all_rows)
        notes.append(f"market_union:{stage}:raw={len(all_rows)}:deduped={len(deduped)}:candidates={len(candidates)}")
        return deduped

    def fetch_market_summary(self, filters: Dict[str, Any], fixture_ids: List[str], notes: List[str]) -> List[Dict[str, Any]]:
        base_rows = self.fetch_market_candidates(filters, fixture_ids, notes, "base")
        seeded_ids = self.fixture_ids_from_markets(base_rows)
        for value in seeded_ids:
            if value not in fixture_ids:
                fixture_ids.append(value)
        notes.append(f"fixture_ids_from_market_rows:{len(seeded_ids)}:total_fixture_ids={len(fixture_ids)}")
        if seeded_ids:
            retry_rows = self.fetch_market_candidates(filters, fixture_ids, notes, "seeded")
            combined = self._dedupe_market_rows(base_rows + retry_rows)
        else:
            combined = base_rows
        notes.append(f"market_selected:union:rows={len(combined)}")
        return combined

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
        if board.fixture_rows and not board.market_rows:
            board.notes.append("fixture_summary_present:market_rows_empty")
        board.participant_rows = []
        return board
