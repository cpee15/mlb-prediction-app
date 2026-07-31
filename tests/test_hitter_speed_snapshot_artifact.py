import json

from mlb_app.simulation.shadow.hitter_speed_snapshot_artifact import (
    build_hitter_speed_snapshot_artifact,
    materialize_hitter_speed_snapshot_artifact,
    serialize_hitter_speed_snapshot_artifact,
    validate_hitter_speed_snapshot_artifact,
)


CSV = """last_name,first_name,player_id,competitive_runs,sprint_speed,season
Witt,Bobby,677951,182,30.4,2025
Turner,Trea,607208,165,29.5,2025
"""


def build(text=CSV, **overrides):
    values = {
        "season": 2025,
        "as_of_date": "2025-07-31",
        "source_updated_at":
            "2025-07-31T23:59:00+00:00",
    }
    values.update(overrides)
    return build_hitter_speed_snapshot_artifact(
        text,
        **values,
    )


def test_builds_deterministic_artifact():
    first = build()
    second = build()

    assert first == second
    assert first["status"] == "ready"
    assert first["artifact_ready"] is True
    assert first["record_count"] == 2
    assert [
        row["player_id"]
        for row in first["records"]
    ] == [607208, 677951]
    assert len(first["artifact_sha256"]) == 64
    assert len(
        first["adapter_result_sha256"]
    ) == 64


def test_serialization_is_deterministic():
    artifact = build()

    first = (
        serialize_hitter_speed_snapshot_artifact(
            artifact
        )
    )
    second = (
        serialize_hitter_speed_snapshot_artifact(
            artifact
        )
    )

    assert first == second
    assert first.endswith("\n")
    assert json.loads(first) == artifact


def test_validates_artifact_identity():
    artifact = build()
    result = (
        validate_hitter_speed_snapshot_artifact(
            artifact
        )
    )

    assert result["status"] == "ready"
    assert result["artifact_valid"] is True
    assert result["blockers"] == []


def test_tampering_invalidates_artifact():
    artifact = build()
    artifact["records"][0][
        "sprint_speed"
    ] = 15.0

    result = (
        validate_hitter_speed_snapshot_artifact(
            artifact
        )
    )

    assert result["status"] == "blocked"
    assert result["artifact_valid"] is False
    assert "artifact_sha256_mismatch" in (
        result["blockers"]
    )


def test_blocked_adapter_cannot_be_materialized(
    tmp_path,
):
    artifact = build(
        "player_id,competitive_runs\n"
        "677951,182\n"
    )
    output_path = (
        tmp_path / "blocked.json"
    )

    result = (
        materialize_hitter_speed_snapshot_artifact(
            artifact,
            output_path,
        )
    )

    assert result["status"] == "blocked"
    assert (
        result["artifact_file_write_performed"]
        is False
    )
    assert not output_path.exists()


def test_materializes_atomically_and_replays(
    tmp_path,
):
    artifact = build()
    output_path = (
        tmp_path
        / "snapshots"
        / "hitter-speed-2025-07-31.json"
    )

    result = (
        materialize_hitter_speed_snapshot_artifact(
            artifact,
            output_path,
        )
    )

    assert result["status"] == "written"
    assert (
        result["artifact_file_write_performed"]
        is True
    )
    assert output_path.exists()

    loaded = json.loads(
        output_path.read_text(
            encoding="utf-8"
        )
    )
    assert loaded == artifact
    assert (
        validate_hitter_speed_snapshot_artifact(
            loaded
        )["artifact_valid"]
        is True
    )

    temporary_files = list(
        output_path.parent.glob("*.tmp")
    )
    assert temporary_files == []


def test_repeated_materialization_is_identical(
    tmp_path,
):
    artifact = build()
    first_path = tmp_path / "first.json"
    second_path = tmp_path / "second.json"

    first = (
        materialize_hitter_speed_snapshot_artifact(
            artifact,
            first_path,
        )
    )
    second = (
        materialize_hitter_speed_snapshot_artifact(
            artifact,
            second_path,
        )
    )

    assert first["serialized_sha256"] == (
        second["serialized_sha256"]
    )
    assert first_path.read_bytes() == (
        second_path.read_bytes()
    )


def test_artifact_has_no_production_authority():
    artifact = build()

    assert (
        artifact["external_fetch_performed"]
        is False
    )
    assert (
        artifact["database_writes_performed"]
        is False
    )
    assert (
        artifact["production_model_modified"]
        is False
    )
    assert (
        artifact["simulation_authority_changed"]
        is False
    )
    assert artifact["parameter_selected"] is False
    assert (
        artifact["production_authority_changed"]
        is False
    )
    assert artifact["shadow_only"] is True
