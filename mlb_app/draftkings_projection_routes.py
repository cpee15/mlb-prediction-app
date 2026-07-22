"""Read-only DraftKings projection preview routes."""

from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from mlb_app.simulation.projections import (
    ingest_draftkings_salary_csv,
    match_canonical_projections_to_draftkings,
)


router = APIRouter(
    prefix="/dfs/draftkings",
    tags=["draftkings-dfs"],
)


class DraftKingsProjectionPreviewRequest(BaseModel):
    projection_payload: Dict[str, Any]
    salary_csv: str = Field(
        min_length=1,
    )
    source_filename: Optional[str] = None


@router.post("/projections/preview")
def preview_draftkings_projections(
    request: DraftKingsProjectionPreviewRequest,
) -> Dict[str, Any]:
    """
    Return one non-persistent DraftKings projection preview.

    This endpoint does not persist salaries, generate lineups, alter
    projection authority, or perform fuzzy identity matching.
    """

    try:
        slate = ingest_draftkings_salary_csv(
            request.salary_csv,
            source_filename=(
                request.source_filename
            ),
        )

        result = (
            match_canonical_projections_to_draftkings(
                projection_payload=(
                    request.projection_payload
                ),
                slate=slate,
            )
        )
    except (
        TypeError,
        ValueError,
    ) as exc:
        raise HTTPException(
            status_code=422,
            detail=str(exc),
        ) from exc

    return {
        **result,
        "preview": True,
        "persistent": False,
        "lineup_generation_applied": False,
    }
