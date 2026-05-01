from mlb_app.model_projection_formulas import bullpen_collapse_index, pitching_volatility_score


def test_bullpen_collapse_index_requires_all_inputs():
    model = bullpen_collapse_index({"era": 4.50, "bb_per_9": 3.2})
    assert model["score"] is None
    assert model["status"] == "missing_inputs"
    assert "whip" in model["missing_inputs"]


def test_bullpen_collapse_index_formula():
    model = bullpen_collapse_index({"era": 4.50, "bb_per_9": 3.2, "whip": 1.35})
    assert model["score"] == 15.75
    assert model["status"] == "calculated"


def test_pitching_volatility_contract_shape():
    model = pitching_volatility_score(
        {"k_pct": 0.27, "bb_pct": 0.08, "xwoba": 0.310, "xba": 0.230, "hard_hit_pct": 0.38, "avg_velocity": 94.5, "avg_spin_rate": 2350},
        {"FF": {"usage_pct": 0.45, "whiff_pct": 0.29, "xwoba": 0.315}},
    )
    for key in ["model_name", "status", "score", "formula", "inputs", "calculation_steps", "missing_inputs", "data_confidence", "source_notes"]:
        assert key in model
    assert model["score"] is not None
