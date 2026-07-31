#!/usr/bin/env python3
"""Materialize an offline hitter speed snapshot artifact."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

from mlb_app.simulation.shadow.hitter_speed_snapshot_artifact import (
    build_hitter_speed_snapshot_artifact,
    materialize_hitter_speed_snapshot_artifact,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Materialize an explicitly supplied "
            "Baseball Savant Sprint Speed CSV "
            "as a deterministic shadow artifact."
        )
    )
    parser.add_argument(
        "--input-csv",
        required=True,
        type=Path,
    )
    parser.add_argument(
        "--output-json",
        required=True,
        type=Path,
    )
    parser.add_argument(
        "--season",
        required=True,
        type=int,
    )
    parser.add_argument(
        "--as-of-date",
        required=True,
    )
    parser.add_argument(
        "--source-updated-at",
        required=True,
    )
    parser.add_argument(
        "--source-url",
        default=(
            "https://baseballsavant.mlb.com/"
            "leaderboard/sprint_speed"
        ),
    )
    parser.add_argument(
        "--source-version",
        default="savant_sprint_speed_csv_v1",
    )
    parser.add_argument(
        "--minimum-competitive-runs",
        default=10,
        type=int,
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
    )
    return parser


def _summary(
    *,
    status: str,
    input_path: Path,
    output_path: Path,
    artifact=None,
    materialization=None,
    blockers=None,
) -> dict:
    artifact = artifact or {}
    materialization = materialization or {}

    return {
        "status": status,
        "input_path": str(input_path),
        "output_path": str(output_path),
        "artifact_contract_version":
            artifact.get(
                "artifact_contract_version"
            ),
        "artifact_sha256":
            artifact.get("artifact_sha256"),
        "raw_source_sha256":
            artifact.get(
                "raw_source_sha256"
            ),
        "record_count":
            artifact.get("record_count", 0),
        "artifact_ready":
            artifact.get(
                "artifact_ready",
                False,
            ),
        "artifact_file_write_performed": (
            materialization.get(
                "artifact_file_write_performed",
                False,
            )
        ),
        "serialized_sha256":
            materialization.get(
                "serialized_sha256"
            ),
        "blockers": sorted(
            set(
                blockers
                or materialization.get(
                    "blockers"
                )
                or artifact.get("blockers")
                or []
            )
        ),
        "external_fetch_performed": False,
        "database_writes_performed": False,
        "production_model_modified": False,
        "simulation_authority_changed": False,
        "parameter_selected": False,
        "production_authority_changed": False,
        "shadow_only": True,
    }


def main(
    argv: Sequence[str] | None = None,
) -> int:
    args = _parser().parse_args(argv)

    input_path = args.input_csv
    output_path = args.output_json

    try:
        same_path = (
            input_path.resolve()
            == output_path.resolve()
        )
    except OSError:
        same_path = False

    if same_path:
        payload = _summary(
            status="blocked",
            input_path=input_path,
            output_path=output_path,
            blockers=[
                "input_and_output_paths_match",
            ],
        )
        print(
            json.dumps(
                payload,
                indent=2,
                sort_keys=True,
            )
        )
        return 2

    try:
        csv_text = input_path.read_text(
            encoding="utf-8"
        )
    except OSError as exc:
        payload = _summary(
            status="blocked",
            input_path=input_path,
            output_path=output_path,
            blockers=[
                "input_csv_unreadable",
                type(exc).__name__,
            ],
        )
        print(
            json.dumps(
                payload,
                indent=2,
                sort_keys=True,
            )
        )
        return 2

    artifact = (
        build_hitter_speed_snapshot_artifact(
            csv_text,
            season=args.season,
            as_of_date=args.as_of_date,
            source_updated_at=(
                args.source_updated_at
            ),
            source_url=args.source_url,
            source_version=args.source_version,
            minimum_competitive_runs=(
                args.minimum_competitive_runs
            ),
        )
    )

    materialization = (
        materialize_hitter_speed_snapshot_artifact(
            artifact,
            output_path,
            overwrite=args.overwrite,
        )
    )

    success = (
        materialization.get("status")
        == "written"
    )
    payload = _summary(
        status=(
            "written"
            if success
            else "blocked"
        ),
        input_path=input_path,
        output_path=output_path,
        artifact=artifact,
        materialization=materialization,
    )
    print(
        json.dumps(
            payload,
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if success else 2


if __name__ == "__main__":
    raise SystemExit(main())
