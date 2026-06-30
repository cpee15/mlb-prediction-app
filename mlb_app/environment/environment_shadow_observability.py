"""
Environment shadow-observability contract.

This module creates deterministic, redacted, bounded shadow-observability
records from environment diagnostic composition payloads.

It does not activate environment effects, alter production outputs, join
historical outcomes, calculate accuracy, tune parameters, price markets,
or detect betting edges.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict, dataclass
from datetime import datetime
import hashlib
import json
from typing import Any, Mapping


RECORD_SCHEMA_VERSION = "7N-v1"
SUPPORTED_RETENTION_CLASSES = {
    "ephemeral",
    "diagnostic_short",
}
MAX_RECORD_BYTES = 32_768

COMPONENT_PAYLOAD_KEYS = (
    "venue_resolution",
    "weather_resolution",
    "vector_resolution",
    "carry_resolution",
)

PROVENANCE_ALLOWLIST = {
    "stage",
    "payload_present",
    "provider_present",
    "source",
    "source_class",
    "source_record_id",
    "pressure_source",
    "composition_status",
    "component_stage_count",
    "distance_mapping_applied",
}

SENSITIVE_KEY_FRAGMENTS = (
    "password",
    "secret",
    "credential",
    "authorization",
    "cookie",
    "token",
    "api_key",
    "apikey",
    "header",
)


@dataclass(frozen=True)
class ShadowObservabilityInput:
    game_id: str
    game_start_time_utc: datetime
    shadow_enabled: bool
    sampling_rate: float
    retention_class: str
    generated_at_utc: datetime
    baseline_projection_fingerprint: str | None = None
    shadow_projection_fingerprint: str | None = None


@dataclass(frozen=True)
class ShadowObservabilityRecord:
    shadow_record_id: str
    record_schema_version: str
    generated_at_utc: str
    game_id: str
    game_start_time_utc: str
    canonical_venue_id: str | None
    shadow_enabled: bool
    sampling_eligible: bool
    sampling_selected: bool
    sampling_key: str
    sampling_rate: float
    composition_status: str
    stage_statuses: dict[str, str]
    resolved_stage_count: int
    neutral_stage_count: int
    unavailable_stage_count: int
    invalid_stage_count: int
    diagnostic_codes: tuple[str, ...]
    validation_errors: tuple[str, ...]
    component_payload_hashes: dict[str, str | None]
    composition_payload_hash: str
    baseline_projection_fingerprint: str | None
    shadow_projection_fingerprint: str | None
    projection_fingerprints_equal: bool | None
    production_output_changed: bool
    production_authority: bool
    retention_class: str
    redaction_applied: bool

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["diagnostic_codes"] = list(
            self.diagnostic_codes
        )
        payload["validation_errors"] = list(
            self.validation_errors
        )
        return payload


@dataclass(frozen=True)
class ShadowObservabilityResult:
    emitted: bool
    reason: str
    record: ShadowObservabilityRecord | None
    metrics: dict[str, int]
    diagnostic_codes: tuple[str, ...]
    production_output_changed: bool = False
    production_authority: bool = False
    historical_outcome_joined: bool = False
    accuracy_metrics_generated: bool = False
    tuning_executed: bool = False
    pricing_or_edge_work_executed: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "emitted": self.emitted,
            "reason": self.reason,
            "record": (
                self.record.to_dict()
                if self.record is not None
                else None
            ),
            "metrics": dict(self.metrics),
            "diagnostic_codes": list(
                self.diagnostic_codes
            ),
            "production_output_changed": (
                self.production_output_changed
            ),
            "production_authority": (
                self.production_authority
            ),
            "historical_outcome_joined": (
                self.historical_outcome_joined
            ),
            "accuracy_metrics_generated": (
                self.accuracy_metrics_generated
            ),
            "tuning_executed": (
                self.tuning_executed
            ),
            "pricing_or_edge_work_executed": (
                self.pricing_or_edge_work_executed
            ),
        }


def _sorted_unique_strings(
    values: Any,
) -> tuple[str, ...]:
    if isinstance(values, str):
        candidates = [values]
    elif isinstance(
        values,
        (list, tuple, set),
    ):
        candidates = list(values)
    else:
        candidates = []

    return tuple(
        sorted(
            {
                item
                for item in candidates
                if isinstance(item, str)
                and item
            }
        )
    )


def semantic_json(
    value: Any,
) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def semantic_hash(
    value: Any,
) -> str:
    return hashlib.sha256(
        semantic_json(value).encode(
            "utf-8"
        )
    ).hexdigest()


def deterministic_sampling_key(
    *,
    game_id: str,
    game_start_time_utc: datetime,
    record_schema_version: str = RECORD_SCHEMA_VERSION,
) -> str:
    return "|".join(
        [
            record_schema_version,
            game_id.strip(),
            game_start_time_utc.isoformat(),
        ]
    )


def deterministic_sample_selected(
    *,
    sampling_key: str,
    sampling_rate: float,
) -> bool:
    if not 0.0 <= sampling_rate <= 1.0:
        raise ValueError(
            "sampling_rate_valid"
        )

    if sampling_rate == 0.0:
        return False

    if sampling_rate == 1.0:
        return True

    digest = hashlib.sha256(
        sampling_key.encode("utf-8")
    ).digest()

    bucket = int.from_bytes(
        digest[:8],
        byteorder="big",
        signed=False,
    )

    fraction = bucket / float(
        2**64
    )

    return fraction < sampling_rate


def _is_sensitive_key(
    key: str,
) -> bool:
    lowered = key.lower()

    return any(
        fragment in lowered
        for fragment in SENSITIVE_KEY_FRAGMENTS
    )


def redact_payload(
    value: Any,
    *,
    in_provenance: bool = False,
) -> Any:
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}

        for raw_key, raw_value in value.items():
            key = str(raw_key)

            if _is_sensitive_key(key):
                continue

            if (
                in_provenance
                and key not in PROVENANCE_ALLOWLIST
            ):
                continue

            result[key] = redact_payload(
                raw_value,
                in_provenance=(
                    in_provenance
                    or key == "provenance"
                ),
            )

        return result

    if isinstance(value, (list, tuple)):
        return [
            redact_payload(
                item,
                in_provenance=in_provenance,
            )
            for item in value
        ]

    if isinstance(
        value,
        (str, int, float, bool),
    ) or value is None:
        return value

    return str(type(value).__name__)


def _extract_int(
    payload: Mapping[str, Any],
    key: str,
) -> int:
    value = payload.get(key, 0)

    if isinstance(value, bool):
        return int(value)

    if isinstance(value, int):
        return value

    return 0


def extract_metrics(
    *,
    composition_payload: Mapping[str, Any],
    sampled: bool,
    fingerprint_mismatch: bool,
) -> dict[str, int]:
    metrics: dict[str, int] = {
        "records_seen": 1,
        "records_eligible": 1,
        "records_sampled": int(sampled),
        "payload_hash_repeat_count": 0,
        "projection_fingerprint_mismatch_count": int(
            fingerprint_mismatch
        ),
        "production_output_change_count": 0,
        "redaction_failure_count": 0,
    }

    composition_status = str(
        composition_payload.get(
            "composition_status",
            "unavailable",
        )
    )

    metrics[
        f"composition_status_count:{composition_status}"
    ] = 1

    stage_statuses = composition_payload.get(
        "stage_statuses",
        {},
    )

    if isinstance(stage_statuses, Mapping):
        for stage_name, status in sorted(
            stage_statuses.items()
        ):
            metrics[
                f"stage_status_count:{stage_name}:{status}"
            ] = 1

    for code in _sorted_unique_strings(
        composition_payload.get(
            "diagnostic_codes",
            [],
        )
    ):
        metrics[
            f"diagnostic_code_count:{code}"
        ] = 1

        if "exception" in code:
            metrics[
                f"provider_exception_count:{code}"
            ] = 1

    for error in _sorted_unique_strings(
        composition_payload.get(
            "validation_errors",
            [],
        )
    ):
        metrics[
            f"validation_error_count:{error}"
        ] = 1

    return metrics


def _fingerprint_comparison(
    baseline: str | None,
    shadow: str | None,
) -> tuple[bool | None, tuple[str, ...]]:
    if (
        baseline is None
        or shadow is None
    ):
        return (
            None,
            (
                "projection_fingerprint_missing",
            ),
        )

    equal = baseline == shadow

    if equal:
        return (
            True,
            (),
        )

    return (
        False,
        (
            "projection_fingerprint_mismatch",
        ),
    )


def build_shadow_observability_record(
    *,
    observability_input: ShadowObservabilityInput,
    composition_payload: Mapping[str, Any] | None,
) -> ShadowObservabilityResult:
    base_metrics = {
        "records_seen": 1,
        "records_eligible": 0,
        "records_sampled": 0,
        "production_output_change_count": 0,
        "redaction_failure_count": 0,
    }

    if not observability_input.shadow_enabled:
        return ShadowObservabilityResult(
            emitted=False,
            reason="shadow_disabled",
            record=None,
            metrics=base_metrics,
            diagnostic_codes=(
                "environment_shadow_observability_disabled",
            ),
        )

    if not observability_input.game_id.strip():
        return ShadowObservabilityResult(
            emitted=False,
            reason="invalid_game_identity",
            record=None,
            metrics=base_metrics,
            diagnostic_codes=(
                "environment_shadow_game_identity_invalid",
            ),
        )

    if not (
        0.0
        <= observability_input.sampling_rate
        <= 1.0
    ):
        return ShadowObservabilityResult(
            emitted=False,
            reason="invalid_sampling_rate",
            record=None,
            metrics=base_metrics,
            diagnostic_codes=(
                "environment_shadow_sampling_rate_invalid",
            ),
        )

    if (
        observability_input.retention_class
        not in SUPPORTED_RETENTION_CLASSES
    ):
        return ShadowObservabilityResult(
            emitted=False,
            reason="unsupported_retention_class",
            record=None,
            metrics=base_metrics,
            diagnostic_codes=(
                "environment_shadow_retention_class_invalid",
            ),
        )

    sampling_key = deterministic_sampling_key(
        game_id=observability_input.game_id,
        game_start_time_utc=(
            observability_input.game_start_time_utc
        ),
    )

    selected = deterministic_sample_selected(
        sampling_key=sampling_key,
        sampling_rate=(
            observability_input.sampling_rate
        ),
    )

    eligibility_metrics = dict(base_metrics)
    eligibility_metrics[
        "records_eligible"
    ] = 1

    if not selected:
        return ShadowObservabilityResult(
            emitted=False,
            reason="sample_not_selected",
            record=None,
            metrics=eligibility_metrics,
            diagnostic_codes=(
                "environment_shadow_sample_not_selected",
            ),
        )

    if composition_payload is None:
        composition_payload = {
            "composition_status": "unavailable",
            "stage_statuses": {},
            "resolved_stage_count": 0,
            "neutral_stage_count": 0,
            "unavailable_stage_count": 0,
            "invalid_stage_count": 0,
            "diagnostic_codes": [
                "environment_composition_missing"
            ],
            "validation_errors": [],
            "canonical_venue_id": None,
            "venue_resolution": None,
            "weather_resolution": None,
            "vector_resolution": None,
            "carry_resolution": None,
        }

    copied_payload = deepcopy(
        dict(composition_payload)
    )

    try:
        redacted_payload = redact_payload(
            copied_payload
        )

        composition_payload_hash = (
            semantic_hash(
                redacted_payload
            )
        )

        component_hashes: dict[
            str,
            str | None,
        ] = {}

        for key in COMPONENT_PAYLOAD_KEYS:
            component_value = (
                redacted_payload.get(key)
            )

            component_hashes[key] = (
                semantic_hash(component_value)
                if component_value is not None
                else None
            )

    except (
        TypeError,
        ValueError,
        OverflowError,
    ):
        failure_metrics = dict(
            eligibility_metrics
        )
        failure_metrics[
            "records_sampled"
        ] = 1

        return ShadowObservabilityResult(
            emitted=False,
            reason="hash_failure",
            record=None,
            metrics=failure_metrics,
            diagnostic_codes=(
                "environment_shadow_hash_failure",
            ),
        )

    (
        fingerprints_equal,
        fingerprint_codes,
    ) = _fingerprint_comparison(
        observability_input.baseline_projection_fingerprint,
        observability_input.shadow_projection_fingerprint,
    )

    mismatch = (
        fingerprints_equal is False
    )

    metrics = extract_metrics(
        composition_payload=redacted_payload,
        sampled=True,
        fingerprint_mismatch=mismatch,
    )

    diagnostic_codes = (
        _sorted_unique_strings(
            [
                *_sorted_unique_strings(
                    redacted_payload.get(
                        "diagnostic_codes",
                        [],
                    )
                ),
                *fingerprint_codes,
            ]
        )
    )

    validation_errors = (
        _sorted_unique_strings(
            redacted_payload.get(
                "validation_errors",
                [],
            )
        )
    )

    stage_statuses_raw = (
        redacted_payload.get(
            "stage_statuses",
            {},
        )
    )

    stage_statuses = (
        {
            str(key): str(value)
            for key, value
            in sorted(
                stage_statuses_raw.items()
            )
        }
        if isinstance(
            stage_statuses_raw,
            Mapping,
        )
        else {}
    )

    record_identity_payload = {
        "record_schema_version": (
            RECORD_SCHEMA_VERSION
        ),
        "game_id": (
            observability_input.game_id
        ),
        "game_start_time_utc": (
            observability_input.game_start_time_utc.isoformat()
        ),
        "sampling_key": sampling_key,
        "composition_payload_hash": (
            composition_payload_hash
        ),
    }

    record = ShadowObservabilityRecord(
        shadow_record_id=semantic_hash(
            record_identity_payload
        ),
        record_schema_version=(
            RECORD_SCHEMA_VERSION
        ),
        generated_at_utc=(
            observability_input.generated_at_utc.isoformat()
        ),
        game_id=(
            observability_input.game_id
        ),
        game_start_time_utc=(
            observability_input.game_start_time_utc.isoformat()
        ),
        canonical_venue_id=(
            str(
                redacted_payload.get(
                    "canonical_venue_id"
                )
            )
            if redacted_payload.get(
                "canonical_venue_id"
            )
            is not None
            else None
        ),
        shadow_enabled=True,
        sampling_eligible=True,
        sampling_selected=True,
        sampling_key=sampling_key,
        sampling_rate=(
            observability_input.sampling_rate
        ),
        composition_status=str(
            redacted_payload.get(
                "composition_status",
                "unavailable",
            )
        ),
        stage_statuses=stage_statuses,
        resolved_stage_count=_extract_int(
            redacted_payload,
            "resolved_stage_count",
        ),
        neutral_stage_count=_extract_int(
            redacted_payload,
            "neutral_stage_count",
        ),
        unavailable_stage_count=_extract_int(
            redacted_payload,
            "unavailable_stage_count",
        ),
        invalid_stage_count=_extract_int(
            redacted_payload,
            "invalid_stage_count",
        ),
        diagnostic_codes=diagnostic_codes,
        validation_errors=validation_errors,
        component_payload_hashes=(
            component_hashes
        ),
        composition_payload_hash=(
            composition_payload_hash
        ),
        baseline_projection_fingerprint=(
            observability_input.baseline_projection_fingerprint
        ),
        shadow_projection_fingerprint=(
            observability_input.shadow_projection_fingerprint
        ),
        projection_fingerprints_equal=(
            fingerprints_equal
        ),
        production_output_changed=False,
        production_authority=False,
        retention_class=(
            observability_input.retention_class
        ),
        redaction_applied=True,
    )

    serialized_record = semantic_json(
        record.to_dict()
    )

    if (
        len(
            serialized_record.encode(
                "utf-8"
            )
        )
        > MAX_RECORD_BYTES
    ):
        metrics[
            "redaction_failure_count"
        ] = 1

        return ShadowObservabilityResult(
            emitted=False,
            reason="payload_size_exceeded",
            record=None,
            metrics=metrics,
            diagnostic_codes=(
                "environment_shadow_redaction_failure",
            ),
        )

    return ShadowObservabilityResult(
        emitted=True,
        reason="record_emitted",
        record=record,
        metrics=metrics,
        diagnostic_codes=diagnostic_codes,
    )
