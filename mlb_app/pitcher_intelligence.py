from __future__ import annotations


def build_pitcher_intelligence_profile(session, pitcher_id: int, season: int, days_back: int = 365):
    return {
        "source": "statcast_events",
        "pitcher_id": int(pitcher_id),
        "season": int(season),
        "days_back": int(days_back),
        "data_window": {},
        "sample_size": {},
        "summary": {},
        "arsenal": [],
        "location_profile": {},
        "release_profile": {"note": "release fields are release geometry, not plate location"},
        "missing_inputs": ["implementation_pending"],
        "quality_flags": [],
        "metadata": {"model_version": "pitcher_intelligence_v1"},
    }
