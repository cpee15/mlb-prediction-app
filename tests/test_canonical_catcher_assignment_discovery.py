from mlb_app.simulation.shadow import (
    CANONICAL_CATCHER_ASSIGNMENT_DISCOVERY_VERSION,
    CONFIRMED_CATCHER_ASSIGNMENT_SOURCE_VERSION,
    discover_confirmed_catcher_assignments,
)


def lineup():
    return {
        "away": [
            {
                "batter_id": 101,
                "position": "SS",
                "position_code": "6",
            },
            {
                "batter_id": 102,
                "position": "C",
                "position_code": "2",
            },
        ],
        "home": [
            {
                "batter_id": 201,
                "position": "Catcher",
                "position_code": "2",
            },
            {
                "batter_id": 202,
                "position": "1B",
                "position_code": "3",
            },
        ],
    }


def discover(payload=None):
    value = lineup() if payload is None else payload

    return discover_confirmed_catcher_assignments(
        game_pk=824406,
        lineup_fetcher=lambda game_pk: value,
    )


def test_discovers_exact_confirmed_catchers():
    result = discover()

    assert result.status == "ready"
    assert result.ready is True
    assert result.away_candidate_count == 1
    assert result.home_candidate_count == 1

    assert tuple(
        (
            value.catcher_id,
            value.team_side,
            value.assignment_source_version,
        )
        for value in result.assignments
    ) == (
        (
            "102",
            "away",
            CONFIRMED_CATCHER_ASSIGNMENT_SOURCE_VERSION,
        ),
        (
            "201",
            "home",
            CONFIRMED_CATCHER_ASSIGNMENT_SOURCE_VERSION,
        ),
    )


def test_position_code_is_exact_catcher_evidence():
    result = discover(
        {
            "away": [
                {
                    "batter_id": "301",
                    "position": None,
                    "position_code": 2,
                },
            ],
            "home": [
                {
                    "batter_id": "401",
                    "position": None,
                    "position_code": "2",
                },
            ],
        }
    )

    assert result.ready is True
    assert tuple(
        value.catcher_id
        for value in result.assignments
    ) == ("301", "401")


def test_roster_membership_does_not_infer_catcher():
    result = discover(
        {
            "away": [
                {
                    "batter_id": 101,
                    "position": "SS",
                },
            ],
            "home": [
                {
                    "batter_id": 201,
                    "position": "1B",
                },
            ],
        }
    )

    assert result.status == "unavailable"
    assert result.ready is False
    assert result.assignments == ()


def test_missing_one_side_is_partial():
    result = discover(
        {
            "away": [
                {
                    "batter_id": 102,
                    "position": "C",
                },
            ],
            "home": [],
        }
    )

    assert result.status == "partial"
    assert result.ready is False
    assert len(result.assignments) == 1
    assert result.assignments[0].team_side == "away"


def test_ambiguous_catcher_assignment_fails_open():
    result = discover(
        {
            "away": [
                {
                    "batter_id": 101,
                    "position": "C",
                },
                {
                    "batter_id": 102,
                    "position": "Catcher",
                },
            ],
            "home": [
                {
                    "batter_id": 201,
                    "position": "C",
                },
            ],
        }
    )

    assert result.status == "blocked"
    assert result.ready is False
    assert result.assignments == ()
    assert result.error_type == (
        "ambiguous_catcher_assignment"
    )


def test_fetch_failure_fails_open():
    def unavailable(game_pk):
        raise RuntimeError("lineup source unavailable")

    result = discover_confirmed_catcher_assignments(
        game_pk=824406,
        lineup_fetcher=unavailable,
    )

    assert result.status == "error"
    assert result.ready is False
    assert result.error_type == "RuntimeError"
    assert result.error_message == (
        "lineup source unavailable"
    )


def test_invalid_payload_fails_open():
    result = discover_confirmed_catcher_assignments(
        game_pk=824406,
        lineup_fetcher=lambda game_pk: object(),
    )

    assert result.status == "blocked"
    assert result.ready is False
    assert result.error_type == "invalid_payload"


def test_missing_game_pk_is_blocked_without_fetch():
    result = discover_confirmed_catcher_assignments(
        game_pk=None,
        lineup_fetcher=lambda game_pk: lineup(),
    )

    assert result.status == "blocked"
    assert result.ready is False
    assert result.error_type == "missing_game_pk"


def test_discovery_version_is_explicit():
    assert discover().discovery_version == (
        CANONICAL_CATCHER_ASSIGNMENT_DISCOVERY_VERSION
    )
