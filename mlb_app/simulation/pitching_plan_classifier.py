"""
Pure pitching-plan classification.

This module has no production-route side effects. It converts explicit
pregame pitching-plan evidence into a deterministic diagnostic payload.

Production activation is intentionally outside this module.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, Iterable, Mapping, Optional


PLAN_TRADITIONAL_STARTER = "traditional_starter"
PLAN_OPENER_BULK = "opener_bulk"
PLAN_TANDEM = "tandem"
PLAN_BULLPEN_GAME = "bullpen_game"
PLAN_WORKLOAD_CAPPED_STARTER = "workload_capped_starter"
PLAN_UNKNOWN_FALLBACK = "unknown_fallback"

ALLOWED_PLAN_TYPES = {
    PLAN_TRADITIONAL_STARTER,
    PLAN_OPENER_BULK,
    PLAN_TANDEM,
    PLAN_BULLPEN_GAME,
    PLAN_WORKLOAD_CAPPED_STARTER,
    PLAN_UNKNOWN_FALLBACK,
}

SOURCE_VERIFIED = "verified"
SOURCE_INFERRED = "inferred"
SOURCE_FALLBACK = "fallback"
SOURCE_CONTRADICTORY = "contradictory"

ALLOWED_SOURCE_STATUSES = {
    SOURCE_VERIFIED,
    SOURCE_INFERRED,
    SOURCE_FALLBACK,
    SOURCE_CONTRADICTORY,
}

CAP_TYPES = {
    "pitches",
    "batters",
    "innings",
}


def _clean_id(value: Any) -> Optional[str]:
    if value is None:
        return None

    cleaned = str(value).strip()
    return cleaned or None


def _clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None

    cleaned = str(value).strip().lower()
    return cleaned or None


def _clean_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value

    if isinstance(value, str):
        return value.strip().lower() in {
            "1",
            "true",
            "yes",
            "y",
        }

    return bool(value)


def _clean_availability(
    value: Any,
) -> Dict[str, bool]:
    if not isinstance(value, Mapping):
        return {}

    cleaned: Dict[str, bool] = {}

    for key, available in value.items():
        pitcher_id = _clean_id(key)
        if pitcher_id is None:
            continue

        cleaned[pitcher_id] = _clean_bool(
            available
        )

    return cleaned


def _is_available(
    pitcher_id: Optional[str],
    availability: Mapping[str, bool],
) -> bool:
    if pitcher_id is None:
        return False

    if pitcher_id not in availability:
        return True

    return bool(availability[pitcher_id])


def _normalize_workload_cap(
    value: Any,
) -> Optional[Dict[str, Any]]:
    if not isinstance(value, Mapping):
        return None

    cap_type = _clean_text(
        value.get("type")
        or value.get("cap_type")
    )

    if cap_type not in CAP_TYPES:
        return None

    raw_value = (
        value.get("value")
        if value.get("value") is not None
        else value.get("cap")
    )

    try:
        cap_value = float(raw_value)
    except (TypeError, ValueError):
        return None

    if cap_value <= 0:
        return None

    source = (
        _clean_text(value.get("source"))
        or "unspecified"
    )

    return {
        "type": cap_type,
        "value": cap_value,
        "source": source,
    }


def _dedupe_sequence(
    rows: Iterable[Dict[str, Any]],
) -> list[Dict[str, Any]]:
    seen: set[tuple[str, str]] = set()
    result: list[Dict[str, Any]] = []

    for row in rows:
        role = _clean_text(row.get("role"))
        pitcher_id = _clean_id(
            row.get("pitcher_id")
        )

        if role is None or pitcher_id is None:
            continue

        key = (role, pitcher_id)

        if key in seen:
            continue

        seen.add(key)

        result.append(
            {
                "order": len(result) + 1,
                "role": role,
                "pitcher_id": pitcher_id,
            }
        )

    return result


def _unknown_payload(
    *,
    listed_starter_id: Optional[str],
    primary_pitcher_id: Optional[str],
    bulk_pitcher_id: Optional[str],
    workload_cap: Optional[Dict[str, Any]],
    source_status: str,
    reasons: list[str],
    source_provenance: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "plan_type": PLAN_UNKNOWN_FALLBACK,
        "confidence": 0.20,
        "source_status": source_status,
        "source_provenance": source_provenance,
        "listed_starter_id": listed_starter_id,
        "primary_pitcher_id": primary_pitcher_id,
        "bulk_pitcher_id": bulk_pitcher_id,
        "planned_sequence": [],
        "workload_cap": workload_cap,
        "fallback_used": True,
        "diagnostics": {
            "rule_id": "PP-R06",
            "reasons": sorted(set(reasons)),
            "classifier_version": (
                "pitching-plan-classifier-v1"
            ),
            "production_activation": False,
            "canonical_probability_authority_changed": (
                False
            ),
        },
    }


def classify_pitching_plan(
    evidence: Optional[Mapping[str, Any]],
) -> Dict[str, Any]:
    """
    Classify a pregame pitching plan using explicit evidence.

    The function is deterministic and side-effect free. It does not
    alter the supplied mapping.
    """

    raw = deepcopy(dict(evidence or {}))

    listed_starter_id = _clean_id(
        raw.get("listed_starter_id")
    )

    expected_primary_pitcher_id = _clean_id(
        raw.get("expected_primary_pitcher_id")
    )

    expected_bulk_pitcher_id = _clean_id(
        raw.get("expected_bulk_pitcher_id")
    )

    announced_plan = _clean_text(
        raw.get("announced_pitching_plan")
    )

    bullpen_game_indicator = _clean_bool(
        raw.get("team_bullpen_game_indicator")
    )

    workload_cap = _normalize_workload_cap(
        raw.get("workload_cap")
        or raw.get("starter_recent_workload")
    )

    availability = _clean_availability(
        raw.get("roster_and_availability_state")
    )

    contradictory = _clean_bool(
        raw.get("contradictory_sources")
    )

    source_name = (
        _clean_text(raw.get("source_name"))
        or "unspecified"
    )

    source_timestamp = _clean_text(
        raw.get("source_timestamp")
    )

    source_provenance = {
        "source_name": source_name,
        "source_timestamp": source_timestamp,
        "availability_supplied": bool(
            availability
        ),
        "announcement_supplied": (
            announced_plan is not None
        ),
    }

    primary_pitcher_id = (
        expected_primary_pitcher_id
        or listed_starter_id
    )

    reasons: list[str] = []

    for label, pitcher_id in [
        ("listed_starter_unavailable", listed_starter_id),
        (
            "primary_pitcher_unavailable",
            primary_pitcher_id,
        ),
        (
            "bulk_pitcher_unavailable",
            expected_bulk_pitcher_id,
        ),
    ]:
        if (
            pitcher_id is not None
            and not _is_available(
                pitcher_id,
                availability,
            )
        ):
            reasons.append(label)

    unavailable_identity = bool(reasons)

    if contradictory:
        reasons.append(
            "contradictory_sources"
        )

    if contradictory or unavailable_identity:
        return _unknown_payload(
            listed_starter_id=listed_starter_id,
            primary_pitcher_id=(
                primary_pitcher_id
                if _is_available(
                    primary_pitcher_id,
                    availability,
                )
                else None
            ),
            bulk_pitcher_id=(
                expected_bulk_pitcher_id
                if _is_available(
                    expected_bulk_pitcher_id,
                    availability,
                )
                else None
            ),
            workload_cap=workload_cap,
            source_status=(
                SOURCE_CONTRADICTORY
                if contradictory
                else SOURCE_FALLBACK
            ),
            reasons=reasons,
            source_provenance=source_provenance,
        )

    if announced_plan in {
        "opener_bulk",
        "opener",
        "opener_and_bulk",
    }:
        if (
            listed_starter_id is None
            or expected_bulk_pitcher_id is None
        ):
            return _unknown_payload(
                listed_starter_id=(
                    listed_starter_id
                ),
                primary_pitcher_id=(
                    primary_pitcher_id
                ),
                bulk_pitcher_id=(
                    expected_bulk_pitcher_id
                ),
                workload_cap=workload_cap,
                source_status=SOURCE_FALLBACK,
                reasons=[
                    "opener_bulk_identity_incomplete"
                ],
                source_provenance=(
                    source_provenance
                ),
            )

        if (
            listed_starter_id
            == expected_bulk_pitcher_id
        ):
            return _unknown_payload(
                listed_starter_id=(
                    listed_starter_id
                ),
                primary_pitcher_id=(
                    listed_starter_id
                ),
                bulk_pitcher_id=None,
                workload_cap=workload_cap,
                source_status=SOURCE_FALLBACK,
                reasons=[
                    "opener_bulk_identity_not_distinct"
                ],
                source_provenance=(
                    source_provenance
                ),
            )

        sequence = _dedupe_sequence(
            [
                {
                    "role": "opener",
                    "pitcher_id": (
                        listed_starter_id
                    ),
                },
                {
                    "role": "bulk_follower",
                    "pitcher_id": (
                        expected_bulk_pitcher_id
                    ),
                },
            ]
        )

        return {
            "plan_type": PLAN_OPENER_BULK,
            "confidence": 0.95,
            "source_status": SOURCE_VERIFIED,
            "source_provenance": source_provenance,
            "listed_starter_id": (
                listed_starter_id
            ),
            "primary_pitcher_id": (
                expected_bulk_pitcher_id
            ),
            "bulk_pitcher_id": (
                expected_bulk_pitcher_id
            ),
            "planned_sequence": sequence,
            "workload_cap": workload_cap,
            "fallback_used": False,
            "diagnostics": {
                "rule_id": "PP-R01",
                "reasons": [
                    "verified_opener_bulk_plan"
                ],
                "classifier_version": (
                    "pitching-plan-classifier-v1"
                ),
                "production_activation": False,
                (
                    "canonical_probability_"
                    "authority_changed"
                ): False,
            },
        }

    if announced_plan in {
        "tandem",
        "tandem_starter",
    }:
        if (
            listed_starter_id is None
            or expected_bulk_pitcher_id is None
            or (
                listed_starter_id
                == expected_bulk_pitcher_id
            )
        ):
            return _unknown_payload(
                listed_starter_id=(
                    listed_starter_id
                ),
                primary_pitcher_id=(
                    primary_pitcher_id
                ),
                bulk_pitcher_id=(
                    expected_bulk_pitcher_id
                ),
                workload_cap=workload_cap,
                source_status=SOURCE_FALLBACK,
                reasons=[
                    "tandem_identity_incomplete"
                ],
                source_provenance=(
                    source_provenance
                ),
            )

        sequence = _dedupe_sequence(
            [
                {
                    "role": "tandem_primary",
                    "pitcher_id": (
                        listed_starter_id
                    ),
                },
                {
                    "role": "tandem_secondary",
                    "pitcher_id": (
                        expected_bulk_pitcher_id
                    ),
                },
            ]
        )

        return {
            "plan_type": PLAN_TANDEM,
            "confidence": 0.95,
            "source_status": SOURCE_VERIFIED,
            "source_provenance": source_provenance,
            "listed_starter_id": (
                listed_starter_id
            ),
            "primary_pitcher_id": (
                listed_starter_id
            ),
                     "bulk_pitcher_id": (
                expected_bulk_pitcher_id
            ),
   "planned_sequence": sequence,
            "workload_cap": workload_cap,
            "fallback_used": False,
            "diagnostics": {
                "rule_id": "PP-R02",
                "reasons": [
                    "verified_tandem_plan"
                ],
                "classifier_version": (
                    "pitching-plan-classifier-v1"
                ),
                "production_activation": False,
                (
                    "canonical_probability_"
                    "authority_changed"
                ): False,
            },
        }

    if (
        announced_plan == "bullpen_game"
        or bullpen_game_indicator
    ):
        return {
            "plan_type": PLAN_BULLPEN_GAME,
            "confidence": (
                0.95
                if announced_plan == "bullpen_game"
                else 0.75
            ),
            "source_status": (
                SOURCE_VERIFIED
                if announced_plan == "bullpen_game"
                else SOURCE_INFERRED
            ),
            "source_provenance": source_provenance,
            "listed_starter_id": (
                listed_starter_id
            ),
            "primary_pitcher_id": None,
            "bulk_pitcher_id": None,
            "planned_sequence": [],
            "workload_cap": workload_cap,
            "fallback_used": False,
            "diagnostics": {
                "rule_id": "PP-R03",
                "reasons": [
                    "bullpen_game_indicator"
                ],
                "classifier_version": (
                    "pitching-plan-classifier-v1"
                ),
                "production_activation": False,
                (
                    "canonical_probability_"
                    "authority_changed"
                ): False,
            },
        }

    if (
        listed_starter_id is not None
        and expected_primary_pitcher_id is not None
        and (
            listed_starter_id
            != expected_primary_pitcher_id
        )
    ):
        return _unknown_payload(
            listed_starter_id=(
                listed_starter_id
            ),
            primary_pitcher_id=(
                expected_primary_pitcher_id
            ),
            bulk_pitcher_id=(
                expected_bulk_pitcher_id
            ),
            workload_cap=workload_cap,
            source_status=SOURCE_FALLBACK,
            reasons=[
                (
                    "different_primary_requires_"
                    "explicit_plan"
                )
            ],
            source_provenance=(
                source_provenance
            ),
        )

    if (
        listed_starter_id is not None
        and workload_cap is not None
    ):
        sequence = _dedupe_sequence(
            [
                {
                    "role": "starter",
                    "pitcher_id": (
                        listed_starter_id
                    ),
                }
            ]
        )

        return {
            "plan_type": (
                PLAN_WORKLOAD_CAPPED_STARTER
            ),
            "confidence": 0.85,
            "source_status": SOURCE_VERIFIED,
            "source_provenance": source_provenance,
            "listed_starter_id": (
                listed_starter_id
            ),
            "primary_pitcher_id": (
                listed_starter_id
            ),
            "bulk_pitcher_id": None,
            "planned_sequence": sequence,
            "workload_cap": workload_cap,
            "fallback_used": False,
            "diagnostics": {
                "rule_id": "PP-R04",
                "reasons": [
                    "verified_workload_cap"
                ],
                "classifier_version": (
                    "pitching-plan-classifier-v1"
                ),
                "production_activation": False,
                (
                    "canonical_probability_"
                    "authority_changed"
                ): False,
            },
        }

    if listed_starter_id is not None:
        sequence = _dedupe_sequence(
            [
                {
                    "role": "starter",
                    "pitcher_id": (
                        listed_starter_id
                    ),
                }
            ]
        )

        return {
            "plan_type": (
                PLAN_TRADITIONAL_STARTER
            ),
            "confidence": 0.65,
            "source_status": SOURCE_INFERRED,
            "source_provenance": source_provenance,
            "listed_starter_id": (
                listed_starter_id
            ),
            "primary_pitcher_id": (
                primary_pitcher_id
            ),
            "bulk_pitcher_id": None,
            "planned_sequence": sequence,
            "workload_cap": None,
            "fallback_used": False,
            "diagnostics": {
                "rule_id": "PP-R05",
                "reasons": [
                    "listed_starter_no_conflict"
                ],
                "classifier_version": (
                    "pitching-plan-classifier-v1"
                ),
                "production_activation": False,
                (
                    "canonical_probability_"
                    "authority_changed"
                ): False,
            },
        }

    return _unknown_payload(
        listed_starter_id=None,
        primary_pitcher_id=None,
        bulk_pitcher_id=None,
        workload_cap=workload_cap,
        source_status=SOURCE_FALLBACK,
        reasons=[
            "insufficient_pitching_plan_evidence"
        ],
        source_provenance=source_provenance,
    )


def validate_pitching_plan_payload(
    payload: Mapping[str, Any],
) -> Dict[str, Any]:
    required_fields = {
        "plan_type",
        "confidence",
        "source_status",
        "source_provenance",
        "listed_starter_id",
        "primary_pitcher_id",
        "bulk_pitcher_id",
        "planned_sequence",
        "workload_cap",
        "fallback_used",
        "diagnostics",
    }

    missing_fields = sorted(
        required_fields - set(payload.keys())
    )

    errors: list[str] = []

    if missing_fields:
        errors.append(
            "missing_fields:"
            + ",".join(missing_fields)
        )

    if payload.get("plan_type") not in (
        ALLOWED_PLAN_TYPES
    ):
        errors.append("invalid_plan_type")

    if payload.get("source_status") not in (
        ALLOWED_SOURCE_STATUSES
    ):
        errors.append("invalid_source_status")

    confidence = payload.get("confidence")

    if not isinstance(confidence, (int, float)):
        errors.append("invalid_confidence_type")
    elif not 0.0 <= float(confidence) <= 1.0:
        errors.append("invalid_confidence_range")

    sequence = payload.get("planned_sequence")

    if not isinstance(sequence, list):
        errors.append("invalid_sequence_type")
    else:
        expected_order = list(
            range(1, len(sequence) + 1)
        )
        actual_order = [
            row.get("order")
            for row in sequence
            if isinstance(row, Mapping)
        ]

        if actual_order != expected_order:
            errors.append(
                "invalid_sequence_order"
            )

    diagnostics = payload.get("diagnostics")

    if not isinstance(diagnostics, Mapping):
        errors.append("invalid_diagnostics")
    else:
        if diagnostics.get(
            "production_activation"
        ) is not False:
            errors.append(
                "production_activation_not_false"
            )

        if diagnostics.get(
            "canonical_probability_"
            "authority_changed"
        ) is not False:
            errors.append(
                "probability_authority_changed"
            )

    return {
        "valid": not errors,
        "errors": errors,
        "missing_fields": missing_fields,
    }
