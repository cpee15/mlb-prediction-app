from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Tuple

from . import kibl_bet105_provider as legacy


class KiblClient:
    """Small KIBL HTTP client: auth lives in the legacy provider, request shape lives here."""

    def __init__(self, base_url: Optional[str] = None, timeout_seconds: Optional[int] = None) -> None:
        self.base_url = (base_url or os.getenv("KIBL_BASE_URL", legacy._DEFAULT_BASE_URL)).rstrip("/")
        self.timeout_seconds = int(timeout_seconds or os.getenv("KIBL_TIMEOUT_SECONDS", str(legacy._DEFAULT_TIMEOUT_SECONDS)))

    def configured(self) -> bool:
        return legacy._configured()

    def post(self, path: str, body: Dict[str, Any]) -> Any:
        url = f"{self.base_url}/{path.strip('/')}/"
        return legacy._post_kibl_json(url, body, self.timeout_seconds)

    def post_summary(self, path: str, filters: Dict[str, Any], offset: int = 0, limit: int = 1000) -> Any:
        body = {**filters, "offset": int(offset), "limit": int(limit)}
        return self.post(path, body)

    def post_detail(self, path: str, ids: List[Any], filters: Optional[Dict[str, Any]] = None, id_key: str = "ids") -> Any:
        body = {**(filters or {}), id_key: [str(value) for value in ids if value not in (None, "")]}
        return self.post(path, body)

    def first_non_empty(self, requests: List[Tuple[str, Dict[str, Any]]]) -> Tuple[Any, str, int]:
        first_payload: Any = None
        first_path = ""
        for path, body in requests:
            payload = self.post(path, body)
            count = len(legacy._find_list_payload(payload))
            if first_payload is None:
                first_payload = payload
                first_path = path
            if count:
                return payload, path, count
        return first_payload if first_payload is not None else {}, first_path, 0


def find_rows(payload: Any) -> List[Dict[str, Any]]:
    return legacy._find_list_payload(payload)
