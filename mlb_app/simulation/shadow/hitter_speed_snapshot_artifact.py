"""Immutable cutoff-safe hitter speed snapshot artifacts."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any

from mlb_app.simulation.shadow.hitter_speed_source_adapter import (
    parse_savant_sprint_speed_csv,
)


ARTIFACT_CONTRACT_VERSION = (
    "hitter_speed_snapshot_artifact_v1"
)
ARTIFACT_TYPE = "hitter_speed_snapshot"


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _sha256_payload(value: Any) -> str:
    return hashlib.sha256(
        _canonical_json(value).encode("utf-8")
    ).hexdigest()


def build_hitter_speed_snapshot_artifact(
    csv_text: str,
    *,
    season: int,
    as_of_date,
    source_updated_at,
    source_url: str = (
        "https://baseballsavant.mlb.com/"
        "leaderboard/sprint_speed"
    ),
    source_version: str = (
        "savant_sprint_speed_csv_v1"
    ),
    minimum_competitive_runs: int = 10,
) -> dict[str, Any]:
    """Build a deterministic artifact without persistence."""

    adapter_result = parse_savant_sprint_speed_csv(
        csv_text,
        season=season,
        as_of_date=as_of_date,
        source_updated_at=source_updated_at,
        source_url=source_url,
        source_version=source_version,
        minimum_competitive_runs=(
            minimum_competitive_runs
        ),
    )

    records = list(
        adapter_result.get("records") or []
    )
    blockers = sorted(
        set(adapter_result.get("blockers") or [])
    )

    artifact = {
        "artifact_contract_version":
            ARTIFACT_CONTRACT_VERSION,
        "artifact_type": ARTIFACT_TYPE,
        "status": adapter_result.get("status"),
        "season": adapter_result.get("season"),
        "as_of_date":
            adapter_result.get("as_of_date"),
        "source_updated_at":
            adapter_result.get(
                "source_updated_at"
            ),
        "source_provider":
            adapter_result.get(
                "source_provider"
            ),
        "source_dataset":
            adapter_result.get(
                "source_dataset"
            ),
        "source_url":
            adapter_result.get("source_url"),
        "source_version":
            adapter_result.get(
                "source_version"
            ),
        "raw_source_sha256":
            adapter_result.get(
                "raw_source_sha256"
            ),
        "normalized_snapshot_sha256":
            adapter_result.get(
                "snapshot_sha256"
            ),
        "adapter_contract_version":
            adapter_result.get(
                "adapter_contract_version"
            ),
        "adapter_result_sha256":
            _sha256_payload(adapter_result),
        "minimum_competitive_runs": (
            adapter_result.get(
                "minimum_competitive_runs"
            )
        ),
        "record_count": len(records),
        "rejected_row_count":
            adapter_result.get(
                "rejected_row_count",
                0,
            ),
        "records": records,
        "rejected_rows": list(
            adapter_result.get(
                "rejected_rows"
            )
            or []
        ),
        "missing_required_headers": list(
            adapter_result.get(
                "missing_required_headers"
            )
            or []
        ),
        "blockers": blockers,
        "artifact_ready": (
            adapter_result.get("status")
            == "ready"
            and not blockers
            and bool(records)
        ),
        "external_fetch_performed": False,
        "database_writes_performed": False,
        "production_model_modified": False,
        "simulation_authority_changed": False,
        "parameter_selected": False,
        "production_authority_changed": False,
        "shadow_only": True,
    }

    digest_payload = dict(artifact)
    artifact["artifact_sha256"] = (
        _sha256_payload(digest_payload)
    )
    return artifact


def serialize_hitter_speed_snapshot_artifact(
    artifact: dict[str, Any],
) -> str:
    """Serialize an artifact deterministically."""

    return (
        json.dumps(
            artifact,
            allow_nan=False,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )


def validate_hitter_speed_snapshot_artifact(
    artifact: dict[str, Any],
) -> dict[str, Any]:
    """Validate identity, readiness, and isolation."""

    supplied_digest = artifact.get(
        "artifact_sha256"
    )
    digest_payload = dict(artifact)
    digest_payload.pop(
        "artifact_sha256",
        None,
    )
    expected_digest = _sha256_payload(
        digest_payload
    )

    blockers = []

    if (
        artifact.get(
            "artifact_contract_version"
        )
        != ARTIFACT_CONTRACT_VERSION
    ):
        blockers.append(
            "artifact_contract_version_mismatch"
        )

    if artifact.get("artifact_type") != ARTIFACT_TYPE:
        blockers.append(
            "artifact_type_mismatch"
        )

    if supplied_digest != expected_digest:
        blockers.append(
            "artifact_sha256_mismatch"
        )

    if not artifact.get("artifact_ready"):
        blockers.append(
            "artifact_not_ready"
        )

    if artifact.get("status") != "ready":
        blockers.append(
            "adapter_result_not_ready"
        )

    if artifact.get("blockers"):
        blockers.append(
            "adapter_blockers_present"
        )

    if not artifact.get("records"):
        blockers.append(
            "artifact_records_missing"
        )

    prohibited_truthy_flags = [
        "external_fetch_performed",
        "database_writes_performed",
        "production_model_modified",
        "simulation_authority_changed",
        "parameter_selected",
        "production_authority_changed",
    ]

    if any(
        artifact.get(field) is not False
        for field in prohibited_truthy_flags
    ):
        blockers.append(
            "production_isolation_violation"
        )

    if artifact.get("shadow_only") is not True:
        blockers.append(
            "shadow_only_flag_missing"
        )

    blockers = sorted(set(blockers))

    return {
        "status": (
            "ready"
            if not blockers
            else "blocked"
        ),
        "artifact_valid": not blockers,
        "artifact_sha256":
            supplied_digest,
        "expected_artifact_sha256":
            expected_digest,
        "record_count": len(
            artifact.get("records") or []
        ),
        "blockers": blockers,
        "external_fetch_performed": False,
        "database_writes_performed": False,
        "production_authority_changed": False,
    }


def materialize_hitter_speed_snapshot_artifact(
    artifact: dict[str, Any],
    output_path,
    *,
    overwrite: bool = False,
) -> dict[str, Any]:
    """Atomically persist one validated artifact."""

    validation = (
        validate_hitter_speed_snapshot_artifact(
            artifact
        )
    )
    destination = Path(output_path)

    if destination.exists() and not overwrite:
        return {
            "status": "blocked",
            "artifact_file_write_performed":
                False,
            "output_path": str(destination),
            "artifact_sha256":
                artifact.get(
                    "artifact_sha256"
                ),
            "blockers": [
                "output_path_exists",
            ],
            "external_fetch_performed": False,
            "database_writes_performed": False,
            "production_authority_changed":
                False,
        }

    if not validation["artifact_valid"]:
        return {
            "status": "blocked",
            "artifact_file_write_performed":
                False,
            "output_path": str(destination),
            "artifact_sha256":
                artifact.get(
                    "artifact_sha256"
                ),
            "blockers": validation["blockers"],
            "external_fetch_performed": False,
            "database_writes_performed": False,
            "production_authority_changed":
                False,
        }

    destination.parent.mkdir(
        parents=True,
        exist_ok=True,
    )
    serialized = (
        serialize_hitter_speed_snapshot_artifact(
            artifact
        )
    )

    temporary_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=destination.parent,
            prefix=f".{destination.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            handle.write(serialized)
            handle.flush()
            os.fsync(handle.fileno())
            temporary_path = Path(handle.name)

        os.replace(
            temporary_path,
            destination,
        )
        temporary_path = None
    finally:
        if (
            temporary_path is not None
            and temporary_path.exists()
        ):
            temporary_path.unlink()

    return {
        "status": "written",
        "artifact_file_write_performed": True,
        "output_path": str(destination),
        "artifact_sha256":
            artifact["artifact_sha256"],
        "serialized_sha256": hashlib.sha256(
            serialized.encode("utf-8")
        ).hexdigest(),
        "record_count":
            artifact["record_count"],
        "external_fetch_performed": False,
        "database_writes_performed": False,
        "production_authority_changed": False,
    }
