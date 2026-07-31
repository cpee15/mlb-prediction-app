import importlib.util
from pathlib import Path
from urllib.parse import parse_qs, urlparse


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = (
    ROOT
    / "scripts"
    / "audit_shadow_hitter_speed_snapshot_acquisition.py"
)

SPEC = importlib.util.spec_from_file_location(
    "hitter_speed_acquisition_audit",
    SCRIPT,
)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


def csv_bytes(season):
    speeds = {
        2024: (29.6, 30.5),
        2025: (30.3, 30.2),
        2026: (29.8, 30.3),
    }
    first, second = speeds[season]
    text = (
        '"last_name, first_name",player_id,'
        'team_id,team,position,age,'
        'competitive_runs,bolts,hp_to_1b,'
        'sprint_speed\n'
        f'"Turner, Trea",607208,143,PHI,SS,'
        f'31,200,10,4.1,{first}\n'
        f'"Witt, Bobby",677951,118,KC,SS,'
        f'25,220,20,4.0,{second}\n'
    )
    return text.encode("utf-8")


def season_from_url(url):
    query = parse_qs(
        urlparse(url).query
    )
    return int(query["year"][0])


def response(url, body):
    return {
        "http_status": 200,
        "headers": {
            "content-type":
                "text/csv; charset=utf-8",
            "content-disposition":
                "attachment;filename=sprint_speed.csv",
        },
        "body": body,
        "final_url": url,
    }


def deterministic_fetcher(url, *, timeout):
    del timeout
    season = season_from_url(url)
    return response(
        url,
        csv_bytes(season),
    )


def test_builds_exact_year_csv_url():
    url = MODULE.build_acquisition_url(2025)
    query = parse_qs(
        urlparse(url).query,
        keep_blank_values=True,
    )

    assert query["year"] == ["2025"]
    assert query["min"] == ["10"]
    assert query["csv"] == ["true"]
    assert "start_year" not in query
    assert "end_year" not in query


def test_observes_deterministic_response():
    result = MODULE.observe_season(
        2025,
        fetcher=deterministic_fetcher,
        timeout=1.0,
    )

    assert result["row_count"] == 2
    assert result["unique_player_count"] == 2
    assert (
        result["raw_replay_identical"]
        is True
    )
    assert (
        result["semantic_replay_identical"]
        is True
    )
    assert (
        result["duplicate_player_id_count"]
        == 0
    )
    assert (
        result["underqualified_row_count"]
        == 0
    )


def test_audit_confirms_prospective_path():
    result = MODULE.run_acquisition_audit(
        (2026, 2024, 2025),
        fetcher=deterministic_fetcher,
        timeout=1.0,
    )

    assert result["status"] == "ready"
    assert [
        row["season_requested"]
        for row in result["observations"]
    ] == [2024, 2025, 2026]

    evaluation = result["evaluation"]
    assert (
        evaluation["acquisition_supported"]
        is True
    )
    assert (
        evaluation[
            "prospective_collection_allowed"
        ]
        is True
    )
    assert (
        evaluation[
            "retrospective_predictive_"
            "evaluation_allowed"
        ]
        is False
    )


def test_changed_repeat_blocks_replay():
    calls = {}

    def changing_fetcher(url, *, timeout):
        del timeout
        season = season_from_url(url)
        calls[season] = calls.get(season, 0) + 1
        body = csv_bytes(season)

        if (
            season == 2025
            and calls[season] == 2
        ):
            body = body.replace(
                b"30.3",
                b"30.4",
                1,
            )

        return response(url, body)

    result = MODULE.run_acquisition_audit(
        (2024, 2025, 2026),
        fetcher=changing_fetcher,
        timeout=1.0,
    )

    assert result["status"] == "blocked"
    assert 2025 in (
        result["evaluation"][
            "replay_failure_seasons"
        ]
    )


def test_rejects_duplicate_identity():
    duplicate = (
        '"last_name, first_name",player_id,'
        'competitive_runs,sprint_speed\n'
        '"Witt, Bobby",677951,200,30.2\n'
        '"Witt, Bobby",677951,210,30.3\n'
    ).encode("utf-8")

    def duplicate_fetcher(url, *, timeout):
        del timeout
        return response(url, duplicate)

    result = MODULE.run_acquisition_audit(
        (2025,),
        fetcher=duplicate_fetcher,
        timeout=1.0,
    )

    assert result["status"] == "blocked"
    assert (
        "player_identity_contract_failed"
        in result["evaluation"]["blockers"]
    )


def test_audit_reports_only_external_read():
    result = MODULE.run_acquisition_audit(
        (2024, 2025, 2026),
        fetcher=deterministic_fetcher,
        timeout=1.0,
    )

    assert (
        result["external_fetch_performed"]
        is True
    )
    assert (
        result["database_writes_performed"]
        is False
    )
    assert (
        result["artifact_file_write_performed"]
        is False
    )
    assert (
        result["production_model_modified"]
        is False
    )
    assert (
        result["simulation_authority_changed"]
        is False
    )
    assert result["parameter_selected"] is False
    assert (
        result["production_authority_changed"]
        is False
    )
    assert result["shadow_only"] is True
