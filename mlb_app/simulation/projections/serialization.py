"""JSON-compatible canonical projection serialization."""

from __future__ import annotations

from dataclasses import fields, is_dataclass
from enum import Enum
from typing import Any, Dict

from .contracts import CanonicalProjectionPayload


def projection_payload_to_dict(
    payload: CanonicalProjectionPayload,
) -> Dict[str, Any]:
    """Serialize with lists and deterministic field ordering."""

    serialized = _serialize(payload)

    if not isinstance(serialized, dict):
        raise TypeError(
            "canonical payload must serialize to a dictionary"
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

    if isinstance(value, Enum):
        return value.value

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
