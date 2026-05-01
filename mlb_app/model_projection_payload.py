from __future__ import annotations

from typing import Any, Dict

from sqlalchemy.orm import Session

from .model_projections import build_model_projection_payload as _build_model_projection_payload


def build_model_projection_payload(session: Session, target_date: str) -> Dict[str, Any]:
    """Compatibility wrapper for the isolated model projections payload builder."""
    return _build_model_projection_payload(session, target_date)


__all__ = ["build_model_projection_payload"]
