from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import APIRouter, Query

router = APIRouter(tags=["landing-v2"])


@router.get("/landing-v2/snapshot")
def landing_v2_snapshot(date: Optional[str] = Query(default=None), mode: str = Query(default="pre")) -> Dict[str, Any]:
    return {"date": date, "mode": mode, "status": "ok"}
