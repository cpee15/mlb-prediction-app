import datetime as dt

from mlb_app.database import PitcherAggregate, StatcastEvent, create_tables, get_engine, get_session
from mlb_app.pitcher_intelligence import build_pitcher_intelligence_profile, location_bucket


def _session():
    engine = get_engine("sqlite:///:memory:")
    create_tables(engine)
    Session = get_session(engine)
    return Session()


def _event(**overrides):
    base = dict(
        game_date=dt.date.today(),
        game_pk=1,
        at_bat_number=1,
        pitch_number=1,
        pitcher_id=100,
        batter_id=200,
        pitch_type="FF",
        release_speed=96.0,
        release_spin_rate=2350.0,
        plate_x=0.10,
        plate_z=2.60,
        description="swinging_strike",
        events=None,
        launch_speed=None,
        launch_angle=None,
        estimated_woba_using_speedangle=None,
        estimated_ba_using_speedangle=None,
        stand="R",
        p_throws="R",
    )
    base.update(overrides)
    return StatcastEvent(**base)


def test_location_bucket_uses_plate_coordinates_not_release_fields():
    bucket = location_bucket(-0.60, 2.55, stand="R")
    assert bucket["bucket"] == "inside_middle"
    assert bucket["handedness_adjusted"] is True
    assert bucket["in_zone"] is True


def test_pitcher_intelligence_returns_stable_shape_and_metrics():
    session = _session()
    session.add_all([
        _event(game_pk=1, at_bat_number=1, pitch_number=1, pitch_type="FF", description="swinging_strike", plate_x=0.10, plate_z=2.60),
        _event(game_pk=1, at_bat_number=1, pitch_number=2, pitch_type="FF", description="hit_into_play", events="single", launch_speed=101.0, launch_angle=22.0, estimated_woba_using_speedangle=0.650, estimated_ba_using_speedangle=0.720, plate_x=0.18, plate_z=2.70),
        _event(game_pk=1, at_bat_number=2, pitch_number=1, pitch_type="SL", description="called_strike", plate_x=0.70, plate_z=1.80),
    ])
    session.commit()

    payload = build_pitcher_intelligence_profile(session, pitcher_id=100, season=dt.date.today().year, days_back=365)

    assert payload["source"] == "statcast_events"
    assert payload["metadata"]["model_version"] == "pitcher_intelligence_v1"
    assert payload["sample_size"]["deduped_pitch_rows"] == 3
    assert len(payload["arsenal"]) == 2
    ff = next(row for row in payload["arsenal"] if row["pitch_type"] == "FF")
    assert ff["hard_hit_pct"] == 1.0
    assert ff["barrel_pct"] == 1.0
    assert ff["xwoba"] == 0.65
    assert payload["location_profile"]["source"] == "plate_x_plate_z"


def test_pitcher_intelligence_uses_pitcher_aggregate_release_profile():
    session = _session()
    session.add(_event())
    session.add(PitcherAggregate(
        pitcher_id=100,
        window="365d",
        end_date=dt.date.today(),
        avg_release_pos_x=-1.75,
        avg_release_pos_z=5.92,
        avg_release_extension=6.4,
    ))
    session.commit()

    payload = build_pitcher_intelligence_profile(session, pitcher_id=100, season=dt.date.today().year, days_back=365)

    assert payload["release_profile"]["source"] == "pitcher_aggregates"
    assert payload["release_profile"]["avg_release_pos_x"] == -1.75
    assert payload["release_profile"]["avg_release_pos_z"] == 5.92
    assert payload["release_profile"]["avg_release_extension"] == 6.4
    assert "avg_release_pos_x" not in payload["missing_inputs"]
    assert "release" in payload["release_profile"]["note"].lower()
    assert "plate" in payload["release_profile"]["note"].lower()


def test_pitcher_intelligence_reports_missing_plate_location():
    session = _session()
    session.add(_event(plate_x=None, plate_z=None))
    session.commit()

    payload = build_pitcher_intelligence_profile(session, pitcher_id=100, season=dt.date.today().year, days_back=365)

    assert "plate_x_plate_z" in payload["missing_inputs"]
    assert payload["location_profile"]["buckets"] == []
