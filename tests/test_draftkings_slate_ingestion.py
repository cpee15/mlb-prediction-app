import io

import pytest

from mlb_app.simulation.projections import (
    DRAFTKINGS_SLATE_SCHEMA_VERSION,
    draftkings_slate_to_dict,
    ingest_draftkings_salary_csv,
)


HEADER = (
    "Position,Name + ID,Name,ID,"
    "Roster Position,Salary,Game Info,"
    "TeamAbbrev,AvgPointsPerGame,"
    "Status,Starting\n"
)


def csv_text():
    return HEADER + (
        'OF,"Mookie Betts (123)",'
        "Mookie Betts,123,OF/UTIL,6200,"
        "LAD@CHC 07/22/2026 08:05PM ET,"
        "LAD,10.4,,Yes\n"
        'SP,"Pitcher One (456)",'
        "Pitcher One,456,P,9400,"
        "LAD@CHC 07/22/2026 08:05PM ET,"
        "CHC,18.7,Q,\n"
    )


def test_salary_csv_normalizes_slate():
    slate = ingest_draftkings_salary_csv(
        csv_text(),
        source_filename="DKSalaries.csv",
    )

    assert slate.schema_version == (
        DRAFTKINGS_SLATE_SCHEMA_VERSION
    )
    assert slate.source == (
        "draftkings_salary_csv"
    )
    assert slate.source_filename == (
        "DKSalaries.csv"
    )
    assert slate.player_count == 2
    assert len(slate.slate_id) == 64

    batter = next(
        player
        for player in slate.players
        if player.dk_player_id == "123"
    )

    assert batter.player_name == (
        "Mookie Betts"
    )
    assert batter.position == "OF"
    assert batter.roster_positions == (
        "OF",
        "UTIL",
    )
    assert batter.salary == 6200
    assert batter.away_team == "LAD"
    assert batter.home_team == "CHC"
    assert batter.team_abbrev == "LAD"
    assert batter.starting is True


def test_serialization_is_plain_json_shape():
    serialized = draftkings_slate_to_dict(
        ingest_draftkings_salary_csv(
            csv_text()
        )
    )

    assert isinstance(
        serialized["players"],
        list,
    )
    batter = next(
        player
        for player in serialized["players"]
        if player["dk_player_id"] == "123"
    )

    assert batter["roster_positions"] == [
        "OF",
        "UTIL",
    ]


def test_stream_input_is_supported():
    stream = io.StringIO(
        csv_text()
    )
    stream.name = "tonight.csv"

    slate = ingest_draftkings_salary_csv(
        stream
    )

    assert slate.source_filename == (
        "tonight.csv"
    )


def test_missing_required_column_is_rejected():
    malformed = (
        "Name,ID,Salary\n"
        "Player,1,5000\n"
    )

    with pytest.raises(
        ValueError,
        match="missing required DraftKings columns",
    ):
        ingest_draftkings_salary_csv(
            malformed
        )


def test_duplicate_player_id_is_rejected():
    duplicated = csv_text() + (
        'OF,"Duplicate (123)",'
        "Duplicate,123,OF,4000,"
        "LAD@CHC 07/22/2026 08:05PM ET,"
        "LAD,5.0,,No\n"
    )

    with pytest.raises(
        ValueError,
        match="duplicate DraftKings player ID",
    ):
        ingest_draftkings_salary_csv(
            duplicated
        )


def test_invalid_game_info_is_rejected():
    malformed = csv_text().replace(
        "LAD@CHC",
        "invalid",
        1,
    )

    with pytest.raises(
        ValueError,
        match="invalid Game Info matchup",
    ):
        ingest_draftkings_salary_csv(
            malformed
        )


def test_empty_slate_is_rejected():
    with pytest.raises(
        ValueError,
        match="contains no players",
    ):
        ingest_draftkings_salary_csv(
            HEADER
        )
