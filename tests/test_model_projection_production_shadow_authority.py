from __future__ import annotations

from mlb_app.simulation.shadow import (
    attach_canonical_shadow,
)


def test_comparator_never_changes_legacy_authority():
    legacy = {
        "away_win_probability": 0.45,
        "home_win_probability": 0.55,
    }

    canonical = {
        "away_win_probability": 0.60,
        "home_win_probability": 0.40,
    }

    result = attach_canonical_shadow(
        legacy_result=legacy,
        enabled=True,
        canonical_payload=canonical,
    )

    assert result[
        "away_win_probability"
    ] == 0.45

    assert result[
        "home_win_probability"
    ] == 0.55

    assert result["diagnostics"][
        "canonical_shadow"
    ]["authoritative_source"] == "legacy"
