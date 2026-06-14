from __future__ import annotations

import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

from .kibl_bet105_full_market_repository import KiblBet105Repository as FullMarketRepository


class KiblBet105Repository(FullMarketRepository):
    """Production-fast Bet105 repository.

    The full fixture-scoped discovery contract remains available for debug, but
    normal board requests only use the verified fixture_id + market_type_id 1/2/3
    calls. Market calls are fetched with bounded concurrency and expose compact
    timing/call-count metadata for verification.
    """

    def __init__(self, *args: Any, discovery_probes: Optional[bool] = None, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        if discovery_probes is None:
            discovery_probes = str(os.getenv("KIBL_BET105_ENABLE_DISCOVERY_PROBES", "false")).lower() in {"1", "true", "yes", "on"}
        self.discovery_probes = bool(discovery_probes)
        self.performance_meta: Dict[str, Any] = {
            "mode": "discovery" if self.discovery_probes else "fast",
            "cache_hit": False,
            "kibl_call_count": 0,
            "fixture_count": 0,
            "market_request_count": 0,
            "total_ms": 0,
            "fixtures_ms": 0,
            "markets_ms": 0,
        }

    @staticmethod
    def _elapsed_ms(start: float) -> int:
        return int(round((time.perf_counter() - start) * 1000))

    @staticmethod
    def _worker_count() -> int:
        try:
            configured = int(os.getenv("KIBL_BET105_MARKET_WORKERS", "8"))
        except ValueError:
            configured = 8
        return max(1, min(configured, 12))

    def _increment_call_count(self) -> None:
        self.performance_meta["kibl_call_count"] = int(self.performance_meta.get("kibl_call_count") or 0) + 1

    def _summary_rows(self, path: str, body: Dict[str, Any], notes: List[str], label: str) -> List[Dict[str, Any]]:
        self._increment_call_count()
        return super()._summary_rows(path, body, notes, label)

    def _market_rows_for_body(self, body: Dict[str, Any], notes: List[str], label: str, fixture_ids: List[str]) -> List[Dict[str, Any]]:
        self._increment_call_count()
        return super()._market_rows_for_body(body, notes, label, fixture_ids)

    def market_request_bodies(self, filters: Dict[str, Any], fixture_ids: List[str]) -> List[Tuple[str, Dict[str, Any]]]:
        clean = self._clean_market_body(filters)
        if not fixture_ids:
            return [("dated:base", clean)] if self.discovery_probes else []

        fixture_id_limit = int(os.getenv("KIBL_MARKET_FIXTURE_ID_LIMIT", str(len(fixture_ids))))
        limited_fixture_ids = fixture_ids[: max(0, fixture_id_limit)]
        bodies: List[Tuple[str, Dict[str, Any]]] = []
        for fixture_id in limited_fixture_ids:
            root = {**clean, "fixture_id": fixture_id}
            if self.discovery_probes:
                bodies.append((f"dated:fixture_id:{fixture_id}", root))
            for market_type_id in self.game_line_market_type_ids:
                bodies.append((f"dated:fixture_id:{fixture_id}:market_type_id:{market_type_id}", {**root, "market_type_id": int(market_type_id)}))
            if self.discovery_probes:
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

    def fetch_fixture_summary(self, filters: Dict[str, Any], notes: List[str]) -> List[Dict[str, Any]]:
        start = time.perf_counter()
        try:
            return super().fetch_fixture_summary(filters, notes)
        finally:
            self.performance_meta["fixtures_ms"] = self._elapsed_ms(start)

    def fetch_market_candidates(self, filters: Dict[str, Any], fixture_ids: List[str], notes: List[str], stage: str) -> List[Dict[str, Any]]:
        start = time.perf_counter()
        all_rows: List[Dict[str, Any]] = []
        candidates = self.market_request_bodies(filters, fixture_ids)
        workers = min(self._worker_count(), max(1, len(candidates) or 1))
        self.performance_meta["fixture_count"] = len(fixture_ids)
        self.performance_meta["market_request_count"] = len(candidates)
        self.performance_meta["market_workers"] = workers
        notes.append(
            f"fixture_scoped_market_requests:enabled:mode={self.performance_meta['mode']}:fixture_ids_available={len(fixture_ids)}:requests={len(candidates)}:workers={workers}:market_type_probes=1,2,3"
        )

        if not candidates:
            self.markets_meta["fixture_count"] = len(fixture_ids)
            self.markets_meta["request_count"] = 0
            self.markets_meta["raw_union_rows"] = 0
            self.markets_meta["deduped_rows"] = 0
            self.markets_meta["market_type_ids"] = {}
            self.performance_meta["markets_ms"] = self._elapsed_ms(start)
            return []

        results: List[List[Dict[str, Any]]] = [[] for _ in candidates]
        with ThreadPoolExecutor(max_workers=workers) as pool:
            future_map = {
                pool.submit(self._market_rows_for_body, body, notes, f"{stage}:{label}", fixture_ids): (idx, label)
                for idx, (label, body) in enumerate(candidates)
            }
            for future in as_completed(future_map):
                idx, label = future_map[future]
                try:
                    rows = future.result()
                except Exception as exc:
                    notes.append(f"fixture_market_summary_error:label={stage}:{label}:error={exc}")
                    rows = []
                results[idx] = rows
                notes.append(f"market_candidate:{stage}:{label}:rows={len(rows)}")

        for rows in results:
            all_rows.extend(rows)
        deduped = self._dedupe_market_rows(all_rows)
        self.markets_meta["fixture_count"] = len(fixture_ids)
        self.markets_meta["request_count"] = len(candidates)
        self.markets_meta["raw_union_rows"] = len(all_rows)
        self.markets_meta["deduped_rows"] = len(deduped)
        self.markets_meta["market_type_ids"] = self._distribution(deduped, "market_type_id", "marketTypeId")
        self.markets_meta["mode"] = self.performance_meta["mode"]
        self.markets_meta["market_workers"] = workers
        self.performance_meta["markets_ms"] = self._elapsed_ms(start)
        notes.append(f"market_union:{stage}:mode={self.performance_meta['mode']}:raw={len(all_rows)}:deduped={len(deduped)}:candidates={len(candidates)}")
        return deduped

    def fetch_board(self, date: Optional[str] = None, live_only: Optional[bool] = None, event_id: Optional[str] = None):
        start = time.perf_counter()
        board = super().fetch_board(date=date, live_only=live_only, event_id=event_id)
        self.performance_meta["total_ms"] = self._elapsed_ms(start)
        self.performance_meta["fixture_count"] = len(board.fixture_rows)
        self.performance_meta["market_row_count"] = len(board.market_rows)
        self.performance_meta["mode"] = "discovery" if self.discovery_probes else "fast"
        setattr(board, "performance_meta", dict(self.performance_meta))
        setattr(board, "markets_meta", self.markets_meta)
        board.notes.append(
            f"performance:mode={self.performance_meta['mode']}:total_ms={self.performance_meta['total_ms']}:fixtures_ms={self.performance_meta['fixtures_ms']}:markets_ms={self.performance_meta['markets_ms']}:kibl_calls={self.performance_meta['kibl_call_count']}:market_requests={self.performance_meta['market_request_count']}"
        )
        return board
