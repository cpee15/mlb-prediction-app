from mlb_app import kibl_bet105_sportsbook_enrichment as enrichment


def test_fixture_metadata_extracts_deep_nested_participants_and_fixture_id():
    item = {
        "fixture": {
            "id": 777001,
            "scheduled_time": "2026-06-14 19:10:00",
            "competition": {"name": "MLB"},
            "participants": [
                {"side": "away", "team": {"displayName": "Toronto Blue Jays"}},
                {"side": "home", "team": {"displayName": "Philadelphia Phillies"}},
            ],
        }
    }

    meta = enrichment.fixture_metadata_from_item(item)

    assert meta["fixture_id"] == "777001"
    assert meta["event_id"] == "777001"
    assert meta["away_team"] == {"name": "Toronto Blue Jays"}
    assert meta["home_team"] == {"name": "Philadelphia Phillies"}
    assert meta["name"] == "Toronto Blue Jays @ Philadelphia Phillies"
    assert meta["start_time"] == "2026-06-14T23:10:00Z"


def test_market_event_enrichment_can_recover_team_names_from_market_rows():
    market_events = [
        {
            "event_id": "777002",
            "name": "Away @ Home",
            "away_team": {"name": None},
            "home_team": {"name": None},
            "markets": [
                {
                    "market_id": "m1",
                    "market_type_id": 1,
                    "market_name": "1",
                    "selections": [
                        {
                            "name": "Away",
                            "description": "Away",
                            "team": "Away",
                            "price": -120,
                            "raw": {
                                "fixture_id": 777002,
                                "participant_side_id": 1,
                                "participant": {"name": "Milwaukee Brewers"},
                                "price_american": -120,
                                "market_type_id": 1,
                            },
                        },
                        {
                            "name": "Home",
                            "description": "Home",
                            "team": "Home",
                            "price": 110,
                            "raw": {
                                "fixture_id": 777002,
                                "participant_side_id": 2,
                                "participant": {"name": "Cincinnati Reds"},
                                "price_american": 110,
                                "market_type_id": 1,
                            },
                        },
                    ],
                    "raw": {
                        "rows": [
                            {"fixture_id": 777002, "participant_side_id": 1, "participant": {"name": "Milwaukee Brewers"}, "market_type_id": 1},
                            {"fixture_id": 777002, "participant_side_id": 2, "participant": {"name": "Cincinnati Reds"}, "market_type_id": 1},
                        ]
                    },
                }
            ],
            "raw": {
                "rows": [
                    {"fixture_id": 777002, "participant_side_id": 1, "participant": {"name": "Milwaukee Brewers"}},
                    {"fixture_id": 777002, "participant_side_id": 2, "participant": {"name": "Cincinnati Reds"}},
                ]
            },
        }
    ]

    enriched = enrichment.enrich_market_events_with_fixture_metadata(market_events, [], [])

    event = enriched[0]
    assert event["name"] == "Milwaukee Brewers @ Cincinnati Reds"
    assert event["away_team"] == {"name": "Milwaukee Brewers"}
    assert event["home_team"] == {"name": "Cincinnati Reds"}
    market = event["markets"][0]
    assert market["market_key"] == "h2h"
    assert market["market_name"] == "Moneyline"
    assert [selection["name"] for selection in market["selections"]] == ["Milwaukee Brewers", "Cincinnati Reds"]
    assert [selection["description"] for selection in market["selections"]] == ["Milwaukee Brewers", "Cincinnati Reds"]
