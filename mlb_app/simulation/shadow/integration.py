"""Fail-open canonical-shadow integration."""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict

from .comparator import compare_shadow_payloads
from .contracts import CanonicalShadowDiagnostics
from .serialization import shadow_diagnostics_to_dict


def attach_canonical_shadow(
    *,
    legacy_result: Dict[str, Any],
    enabled: bool = False,
    canonical_payload=None,
) -> Dict[str, Any]:
    """Attach shadow diagnostics without mutating legacy data."""

    if not isinstance(legacy_result, dict):
        raise TypeError(
            "legacy_result must be a dictionary"
        )

    output = deepcopy(legacy_result)
    diagnostics = output.setdefault(
        "diagnostics",
        {},
    )

    if not isinstance(diagnostics, dict):
        diagnostics = {
            "legacy_diagnostics": deepcopy(
                diagnostics
            )
        }
        output["diagnostics"] = diagnostics

    if not enabled:
        shadow = CanonicalShadowDiagnostics(
            status="disabled",
            enabled=False,
            canonical_available=False,
            authoritative_source="legacy",
            warnings=(
                "canonical_shadow_disabled",
            ),
        )
    elif canonical_payload is None:
        shadow = CanonicalShadowDiagnostics(
            status="unavailable",
            enabled=True,
            canonical_available=False,
            authoritative_source="legacy",
            warnings=(
                "canonical_payload_unavailable",
            ),
        )
    else:
        try:
            shadow = compare_shadow_payloads(
                legacy_result=legacy_result,
                canonical_payload=canonical_payload,
            )
        except Exception as exc:
            shadow = CanonicalShadowDiagnostics(
                status="error",
                enabled=True,
                canonical_available=True,
                authoritative_source="legacy",
                warnings=(
                    "canonical_shadow_comparison_failed",
                ),
                error_type=exc.__class__.__name__,
                error_message=str(exc),
            )

    diagnostics["canonical_shadow"] = (
        shadow_diagnostics_to_dict(shadow)
    )

    return output
