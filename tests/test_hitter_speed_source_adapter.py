from mlb_app.simulation.shadow.hitter_speed_source_adapter import (
    parse_savant_sprint_speed_csv,
)


CSV = """last_name,first_name,player_id,team_id,position_name,competitive_runs,bolts,hp_to_1b,sprint_speed,season
Witt,Bobby,677951,118,SS,182,44,4.12,30.4,2025
Turner,Trea,607208,143,SS,165,31,4.19,29.5,2025
"""


def parse(text=CSV, **overrides):
    values = {
        "season": 2025,
"as_of_date": "2025-07-31",
"source_updated_at":
"2025-07-31T23:59:00+00:00",
    }
    values.update(overrides)
    return parse_savant_sprint_speed_csv(
        text,
        **values,
    )


def test_parses_cutoff_safe_snapshot():
    result = parse()

    assert result["status"] == "ready"
    assert result["record_count"] == 2
    assert result["rejected_row_count"] == 0
    assert [
        row["player_id"]
        for row in result["records"]
    ] == [607208, 677951]
    assert result["records"][1][
"sprint_speed"
    ] == 30.4
    assert result["records"][1][
"competitive_runs"
    ] == 182
    assert result["records"][1][
"bolt_count"
    ] == 44
    assert result["records"][1][
"home_to_first"
    ] == 4.12


def test_snapshot_is_deterministic_and_hashed():
    first = parse()
    second = parse()

    assert first == second
    assert len(first["raw_source_sha256"]) == 64
    assert len(first["snapshot_sha256"]) == 64
    assert all(
        len(row["record_sha256"]) == 64
        for row in first["records"]
    )


def test_header_aliases_are_supported():
    text = """Last Name,First Name,mlbam_id,Competitive Run,Sprint Speed
Witt,Bobby,677951,182,30.4
"""
    result = parse(text)

    assert result["status"] == "ready"
    assert result["record_count"] == 1
    assert result["records"][0][
"player_id"
    ] == 677951


def test_missing_required_headers_blocks():
    result = parse(
        "player_id,competitive_runs\n"
        "677951,182\n"
    )

    assert result["status"] == "blocked"
    assert "sprint_speed" in (
        result["missing_required_headers"]
    )
    assert (
        "missing_required_csv_headers"
        in result["blockers"]
    )


def test_rejects_invalid_and_unqualified_rows():
    text = """player_id,competitive_runs,sprint_speed
677951,9,30.4
607208,100,50.0
bad,100,29.0
"""
    result = parse(text)

    assert result["status"] == "blocked"
    assert result["record_count"] == 0
    assert result["rejected_row_count"] == 3
    assert "csv_rows_rejected" in (
        result["blockers"]
    )
    assert "no_eligible_speed_records" in (
        result["blockers"]
    )


def test_duplicate_player_identity_blocks_snapshot():
    text = """player_id,competitive_runs,sprint_speed
677951,100,30.4
677951,101,30.5
"""
    result = parse(text)

    assert result["status"] == "blocked"
    assert result["record_count"] == 1
    assert (
        result["rejected_rows"][0]["reasons"]
        == ["duplicate_player_id"]
    )


def test_row_season_must_match_snapshot_season():
    text = """player_id,competitive_runs,sprint_speed,year
677951,100,30.4,2024
"""
    result = parse(text)

    assert result["status"] == "blocked"
    assert "row_season_mismatch" in (
        result["rejected_rows"][0]["reasons"]
    )


def test_snapshot_date_must_match_capture_date():
    result = parse(
        source_updated_at=
            "2025-08-01T00:01:00+00:00"
    )

    assert result["status"] == "blocked"
    assert (
        "snapshot_date_does_not_match_capture_date"
        in result["blockers"]
    )


def test_naive_capture_timestamp_is_rejected():
    result = parse(
        source_updated_at="2025-07-31T23:59:00"
    )

    assert result["status"] == "blocked"
    assert "invalid_source_updated_at" in (
        result["blockers"]
    )


def test_adapter_has_no_fetch_write_or_authority():
    result = parse()

    assert (
        result["external_fetch_performed"]
        is False
    )
    assert (
        result["database_writes_performed"]
        is False
    )
    assert result["parameter_selected"] is False
    assert (
        result["production_authority_changed"]
        is False
    )
    assert result["shadow_only"] is True
