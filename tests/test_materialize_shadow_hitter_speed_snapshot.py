import json
import subprocess
import sys
from pathlib import Path

from mlb_app.simulation.shadow.hitter_speed_snapshot_artifact import (
    build_hitter_speed_snapshot_artifact,
    materialize_hitter_speed_snapshot_artifact,
)


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = (
    ROOT
    / "scripts"
    / "materialize_shadow_hitter_speed_snapshot.py"
)
CSV = """player_id,competitive_runs,sprint_speed,season
677951,182,30.4,2025
607208,165,29.5,2025
"""


def command(input_path, output_path, *extra):
    return [
        sys.executable,
        str(SCRIPT),
        "--input-csv",
        str(input_path),
        "--output-json",
        str(output_path),
        "--season",
        "2025",
        "--as-of-date",
        "2025-07-31",
        "--source-updated-at",
        "2025-07-31T23:59:00+00:00",
        *extra,
    ]


def run_cli(input_path, output_path, *extra):
    return subprocess.run(
        command(
            input_path,
            output_path,
            *extra,
        ),
        cwd=ROOT,
        capture_output=True,
        check=False,
        text=True,
    )


def artifact():
    return build_hitter_speed_snapshot_artifact(
        CSV,
        season=2025,
        as_of_date="2025-07-31",
        source_updated_at=(
            "2025-07-31T23:59:00+00:00"
        ),
    )


def test_cli_materializes_supplied_csv(tmp_path):
    input_path = tmp_path / "speed.csv"
    output_path = tmp_path / "speed.json"
    input_path.write_text(
        CSV,
        encoding="utf-8",
    )

    result = run_cli(
        input_path,
        output_path,
    )

    assert result.returncode == 0
    payload = json.loads(result.stdout)
    assert payload["status"] == "written"
    assert payload["record_count"] == 2
    assert (
        payload["artifact_file_write_performed"]
        is True
    )
    assert output_path.exists()

    written = json.loads(
        output_path.read_text(
            encoding="utf-8"
        )
    )
    assert written["artifact_ready"] is True
    assert written["record_count"] == 2


def test_cli_refuses_existing_output(tmp_path):
    input_path = tmp_path / "speed.csv"
    output_path = tmp_path / "speed.json"
    input_path.write_text(
        CSV,
        encoding="utf-8",
    )
    output_path.write_text(
        '{"preserve": true}\n',
        encoding="utf-8",
    )

    result = run_cli(
        input_path,
        output_path,
    )

    assert result.returncode == 2
    payload = json.loads(result.stdout)
    assert payload["status"] == "blocked"
    assert "output_path_exists" in (
        payload["blockers"]
    )
    assert json.loads(
        output_path.read_text(
            encoding="utf-8"
        )
    ) == {"preserve": True}


def test_cli_overwrite_is_explicit(tmp_path):
    input_path = tmp_path / "speed.csv"
    output_path = tmp_path / "speed.json"
    input_path.write_text(
        CSV,
        encoding="utf-8",
    )
    output_path.write_text(
        '{"old": true}\n',
        encoding="utf-8",
    )

    result = run_cli(
        input_path,
        output_path,
        "--overwrite",
    )

    assert result.returncode == 0
    assert json.loads(
        output_path.read_text(
            encoding="utf-8"
        )
    )["artifact_ready"] is True


def test_cli_rejects_same_input_and_output(
    tmp_path,
):
    path = tmp_path / "speed.csv"
    path.write_text(
        CSV,
        encoding="utf-8",
    )
    original = path.read_bytes()

    result = run_cli(path, path)

    assert result.returncode == 2
    payload = json.loads(result.stdout)
    assert "input_and_output_paths_match" in (
        payload["blockers"]
    )
    assert path.read_bytes() == original


def test_cli_rejects_blocked_adapter_result(
    tmp_path,
):
    input_path = tmp_path / "speed.csv"
    output_path = tmp_path / "speed.json"
    input_path.write_text(
        "player_id,competitive_runs\n"
        "677951,182\n",
        encoding="utf-8",
    )

    result = run_cli(
        input_path,
        output_path,
    )

    assert result.returncode == 2
    payload = json.loads(result.stdout)
    assert payload["status"] == "blocked"
    assert (
        payload["artifact_file_write_performed"]
        is False
    )
    assert not output_path.exists()


def test_materializer_refuses_existing_path(
    tmp_path,
):
    output_path = tmp_path / "speed.json"
    output_path.write_text(
        "preserve\n",
        encoding="utf-8",
    )

    result = (
        materialize_hitter_speed_snapshot_artifact(
            artifact(),
            output_path,
        )
    )

    assert result["status"] == "blocked"
    assert result["blockers"] == [
        "output_path_exists",
    ]
    assert output_path.read_text(
        encoding="utf-8"
    ) == "preserve\n"


def test_cli_reports_production_isolation(
    tmp_path,
):
    input_path = tmp_path / "speed.csv"
    output_path = tmp_path / "speed.json"
    input_path.write_text(
        CSV,
        encoding="utf-8",
    )

    result = run_cli(
        input_path,
        output_path,
    )
    payload = json.loads(result.stdout)

    assert (
        payload["external_fetch_performed"]
        is False
    )
    assert (
        payload["database_writes_performed"]
        is False
    )
    assert (
        payload["production_model_modified"]
        is False
    )
    assert (
        payload["simulation_authority_changed"]
        is False
    )
    assert payload["parameter_selected"] is False
    assert (
        payload["production_authority_changed"]
        is False
    )
    assert payload["shadow_only"] is True
