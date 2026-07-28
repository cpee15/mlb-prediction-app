import copy

from mlb_app.simulation.pa_outcome_model import (
    build_pa_outcome_probabilities,
)
from mlb_app.simulation.shadow.combined_hitter_profile import (
    COMBINED_HIT_SKILL_POLICY_VERSION,
    COMBINED_SHADOW_HITTER_PROFILE_VERSION,
    build_combined_shadow_hitter_profile,
    compare_combined_shadow_hitter_pa_outcomes,
)


def actual_blend():
    return {
        "schema_version": "shadow_player_profile_blend_v1",
        "status": "ready",
        "player_id": 7,
        "season": 2026,
        "split": "vsR",
        "window_policy": "disjoint_current_prior_career",
        "windows": {
            "current_season": {"pa": 100},
            "prior_season": {"pa": 200},
            "career_pre_prior": {"pa": 400},
        },
        "blended_actual_metrics": {
            "k_pct": 0.22,
            "bb_pct": 0.09,
            "batting_avg": 0.260,
            "iso": 0.170,
        },
        "contact_quality_context": {},
        "warnings": ["missing_batter_contact_aggregate"],
    }


def expected_components():
    return {
        "schema_version": "shadow_hitter_expected_components_v1",
        "status": "ready",
        "player_id": 7,
        "season": 2026,
        "split": "vsR",
        "source_latest_date": "2026-07-26",
        "source_age_days": 1,
        "blended_expected_metrics": {
            "xba": 0.280,
            "xwoba": 0.360,
        },
        "warnings": [],
    }


def pitcher():
    return {
        "bat_missing": {"k_rate": 0.24},
        "command_control": {"bb_rate": 0.08},
        "contact_management": {
            "barrel_rate_allowed": 0.08,
            "hard_hit_rate_allowed": 0.38,
            "xba_allowed": 0.250,
        },
    }


def environment():
    return {
        "run_environment": {
            "hr_boost_index": 1.0,
            "hit_boost_index": 1.0,
            "run_scoring_index": 1.0,
        }
    }


def test_combines_actual_average_and_xba_without_applying_xwoba():
    result = build_combined_shadow_hitter_profile(
        actual_blend=actual_blend(),
        expected_components=expected_components(),
    )

    assert result["schema_version"] == COMBINED_SHADOW_HITTER_PROFILE_VERSION
    assert result["status"] == "ready"
    assert result["shadow_only"] is True
    assert result["production_authority_changed"] is False
    assert result["hit_skill_policy"]["version"] == (
        COMBINED_HIT_SKILL_POLICY_VERSION
    )
    assert result["hit_skill_policy"]["combined_value"] == 0.270
    assert result["hit_skill_policy"]["parameter_selected"] is False
    assert result["candidate_profile"]["contact_skill"]["hit_skill"] == 0.270
    assert result["candidate_profile"]["contact_skill"]["contact_rate"] is None
    assert result["expected_evidence"]["xwoba"] == 0.360
    assert result["expected_evidence"]["xwoba_applied"] is False


def test_blocks_unready_or_mismatched_evidence():
    stale = expected_components()
    stale["status"] = "blocked"
    stale["blockers"] = ["stale_statcast_source"]
    mismatched = expected_components()
    mismatched["player_id"] = 99

    stale_result = build_combined_shadow_hitter_profile(
        actual_blend=actual_blend(),
        expected_components=stale,
    )
    mismatch_result = build_combined_shadow_hitter_profile(
        actual_blend=actual_blend(),
        expected_components=mismatched,
    )

    assert stale_result["status"] == "blocked"
    assert "expected_components_not_ready" in stale_result["blockers"]
    assert mismatch_result["status"] == "blocked"
    assert "profile_evidence_identity_mismatch" in mismatch_result["blockers"]


def test_optional_hit_skill_is_backward_compatible_and_observable():
    profile = {
        "contact_skill": {"k_rate": 0.22, "contact_rate": None},
        "plate_discipline": {"bb_rate": 0.09},
        "power": {"iso": 0.170},
    }
    explicit_none = copy.deepcopy(profile)
    explicit_none["contact_skill"]["hit_skill"] = None
    candidate = copy.deepcopy(profile)
    candidate["contact_skill"]["hit_skill"] = 0.270

    baseline = build_pa_outcome_probabilities(
        profile, pitcher(), environment()
    )
    unchanged = build_pa_outcome_probabilities(
        explicit_none, pitcher(), environment()
    )
    adjusted = build_pa_outcome_probabilities(
        candidate, pitcher(), environment()
    )

    assert baseline["probabilities"] == unchanged["probabilities"]
    assert baseline["inputs_used"]["batter_hit_skill"] is None
    assert adjusted["inputs_used"]["batter_hit_skill"] == 0.270
    assert adjusted["probabilities"]["single"] != (
        baseline["probabilities"]["single"]
    )


def test_paired_comparison_preserves_production_profile():
    production_profile = {
        "contact_skill": {"k_rate": 0.25, "contact_rate": 0.72},
        "plate_discipline": {"bb_rate": 0.08},
        "power": {
            "iso": 0.150,
            "barrel_rate": 0.07,
            "hard_hit_rate": 0.36,
        },
    }
    original = copy.deepcopy(production_profile)

    result = compare_combined_shadow_hitter_pa_outcomes(
        actual_blend=actual_blend(),
        expected_components=expected_components(),
        production_batter_profile=production_profile,
        pitcher_profile=pitcher(),
        environment_profile=environment(),
    )

    assert result["status"] == "ready"
    assert result["shadow_only"] is True
    assert result["production_authority_changed"] is False
    assert production_profile == original
    assert result["shadow_inputs_used"]["batter_hit_skill"] == 0.270
    assert result["maximum_absolute_probability_delta"] > 0
    assert abs(sum(result["production_probabilities"].values()) - 1.0) <= 0.0005
    assert abs(sum(result["shadow_probabilities"].values()) - 1.0) <= 0.0005


def test_player_id_loader_combines_matching_persisted_evidence(monkeypatch):
    import mlb_app.simulation.shadow.hitter_expected_components as expected_mod
    import mlb_app.simulation.shadow.player_profile_blend as actual_mod
    from mlb_app.simulation.shadow.combined_hitter_profile import (
        load_combined_shadow_hitter_profile,
    )

    calls = {}

    def load_actual(session, **kwargs):
        calls["actual"] = (session, kwargs)
        result = actual_blend()
        result["storage_evidence"] = {"player_split_row_count": 4}
        return result

    def load_expected(session, **kwargs):
        calls["expected"] = (session, kwargs)
        result = expected_components()
        result["storage_evidence"] = {"raw_row_count": 700}
        return result

    monkeypatch.setattr(
        actual_mod, "load_shadow_hitter_profile_blend", load_actual
    )
    monkeypatch.setattr(
        expected_mod, "load_shadow_hitter_expected_components", load_expected
    )
    session = object()
    result = load_combined_shadow_hitter_profile(
        session,
        player_id=7,
        season=2026,
        split="vsR",
        as_of_date="2026-07-27",
        career_start_season=2023,
    )

    assert result["status"] == "ready"
    assert result["player_id"] == 7
    assert result["evidence_status"] == {
        "actual": "ready",
        "expected": "ready",
    }
    assert result["storage_evidence"]["actual"] == {
        "player_split_row_count": 4
    }
    assert result["storage_evidence"]["expected"] == {
        "raw_row_count": 700
    }
    assert calls["actual"][1]["player_id"] == 7
    assert calls["expected"][1]["career_start_season"] == 2023
