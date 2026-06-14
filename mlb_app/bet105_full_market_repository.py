from __future__ import annotations

from collections import Counter
from typing import Any, Dict, List, Tuple

from .kibl_bet105_repository import KiblBet105Repository
from .kibl_client import find_rows


class KiblBet105FullMarketRepository(KiblBet105Repository):
    """Bet105 repository extension for the full fixture-scoped MLB board.

    The winning production contract is still one dated info/markets request per
    selected fixture_id. This wrapper keeps that contract as the source of truth,
    adds fixture-scoped market_type_id probes for game-line markets, and records
    the raw/kept shape so production can prove whether KIBL returned only
    Moneyline or additional rows that the normalizer should surface.
    """

    game_line_market_type_ids = ("1", "2", "3")

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.markets_meta: Dict[str, Any] = {"fixture_request_summaries": []}

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

    @staticmethod
    def _fixture_id_from_label(label: str, body: Dict[str, Any]) -> str | None:
        if body.get("fixture_id") not in (None, ""):
            return str(body.get("fixture_id"))
        marker = "fixture_id:"
        if marker in label:
            return label.split(marker, 1)[1].split(":", 1)[0]
        return None

    def _clean_market_body(self, filters: Dict[str, Any]) -> Dict[str, Any]:
        clean = {
            key: value
            for key, value in filters.items()
            if key not in {"from_cache", "path", "combined_market_candidates", "league_id", "sport_id"} and value not in (None, "")
        }
        clean.update({"sport_id": self.mlb_sport_id, "league_id": self.mlb_league_id})
        return clean

    def market_request_bodies(self, filters: Dict[str, Any], fixture_ids: List[str]) -> List[Tuple[str, Dict[str, Any]]]:
        clean = self._clean_market_body(filters)
        if not fixture_ids:
            return [("dated:base", clean)]

        fixture_id_limit = int(__import__("os").getenv("KIBL_MARKET_FIXTURE_ID_LIMIT", str(len(fixture_ids))))
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

    def _market_rows_for_body(self, body: Dict[str, Any], notes: List[str], label: str, fixture_ids: List[str]) -> List[Dict[str, Any]]:
        payload = self.client.post(self.market_summary_path, body)
        raw_rows = find_rows(payload)
        feed_rows = self._filter_rows(raw_rows, body, notes, f"market_full:{label}")
        kept_rows = self._market_rows_matching_fixtures(feed_rows, fixture_ids, notes, f"market_full:{label}")
        fixture_id = self._fixture_id_from_label(label, body)
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
        notes.append(
            "fixture_market_summary:"
            f"fixture_id={fixture_id}:label={label}:raw={len(raw_rows)}:kept={len(kept_rows)}:"
            f"types={summary['market_type_ids']}"
        )
        return kept_rows

    def fetch_market_candidates(self, filters: Dict[str, Any], fixture_ids: List[str], notes: List[str], stage: str) -> List[Dict[str, Any]]:
        all_rows: List[Dict[str, Any]] = []
        candidates = self.market_request_bodies(filters, fixture_ids)
        notes.append(f"fixture_scoped_market_requests:enabled:fixture_ids_available={len(fixture_ids)}:requests={len(candidates)}:market_type_probes=1,2,3")
        for label, body in candidates:
            try:
                rows = self._market_rows_for_body(body, notes, f"{stage}:{label}", fixture_ids)
            except Exception as exc:  # noqa: BLE001 - diagnostics should record KIBL shape failures without hiding other fixtures.
                notes.append(f"fixture_market_summary_error:label={stage}:{label}:error={exc}")
                continue
            notes.append(f"market_candidate:{stage}:{label}:rows={len(rows)}")
            all_rows.extend(rows)
        deduped = self._dedupe_market_rows(all_rows)
        notes.append(f"market_union:{stage}:raw={len(all_rows)}:deduped={len(deduped)}:candidates={len(candidates)}")
        self.markets_meta["fixture_count"] = len(fixture_ids)
        self.markets_meta["request_count"] = len(candidates)
        self.markets_meta["raw_union_rows"] = len(all_rows)
        self.markets_meta["deduped_rows"] = len(deduped)
        self.markets_meta["market_type_ids"] = self._distribution(deduped, "market_type_id", "marketTypeId")
        return deduped

    def fetch_market_summary(self, filters: Dict[str, Any], fixture_ids: List[str], notes: List[str]) -> List[Dict[str, Any]]:
        rows = self.fetch_market_candidates(filters, fixture_ids, notes, "fixture_id")
        notes.append(f"market_selected:fixture_id_rows={len(rows)}:types={self.markets_meta.get('market_type_ids', {})}")
        return rows

    def fetch_board(self, *args: Any, **kwargs: Any):
        board = super().fetch_board(*args, **kwargs)
        setattr(board, "markets_meta", self.markets_meta)
        return board
