"""JSON-compatible canonical-shadow serialization."""

from __future__ import annotations

from dataclasses import fields, is_dataclass
from typing import Any, Dict

from .contracts import CanonicalShadowDiagnostics


def shadow_diagnostics_to_dict(
    diagnostics: CanonicalShadowDiagnostics,
) -> Dict[str, Any]:
    serialized = _serialize(diagnostics)

    if not isinstance(serialized, dict):
        raise TypeError(
            "shadow diagnostics must serialize "
            "to a dictionary"
        )

    return serialized


def _serialize(value):
    if is_dataclass(value):
        return {
            field.name: _serialize(
                getattr(value, field.name)
            )
            for field in fields(value)
        }

    if isinstance(value, tuple):
        return [
            _serialize(item)
            for item in value
        ]

    if isinstance(value, list):
        return [
            _serialize(item)
            for item in value
        ]

    if isinstance(value, dict):
        return {
            str(key): _serialize(item)
            for key, item in sorted(
                value.items(),
                key=lambda pair: str(pair[0]),
            )
        }

    return value
