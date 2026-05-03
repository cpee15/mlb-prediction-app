from typing import Dict, Any


def build_formula_map(shared_payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(shared_payload, dict):
        return {"error": "invalid_shared_payload"}

    meta = shared_payload.get("meta", {})
    derived = shared_payload.get("derived_outputs", {}) or {}
    inputs = shared_payload.get("inputs", {}) or {}

    game_sim = derived.get("game_simulation", {}) or {}
    bullpen_sim = derived.get("bullpen_adjusted_game_simulation", {}) or {}

    projection_sim = bullpen_sim or game_sim

    return {
        "directSimInputs": _build_direct_inputs(inputs),
        "derivedSimOutputs": _build_derived_outputs(projection_sim),
        "context": _build_context(shared_payload),
        "diagnostic": _build_diagnostic(shared_payload, meta),
    }


def _build_derived_outputs(sim: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "expectedRuns": {
            "away": sim.get("away_runs_mean"),
            "home": sim.get("home_runs_mean"),
            "total": sim.get("total_runs_mean"),
        },
        "winProbability": {
            "away": sim.get("away_win_probability"),
            "home": sim.get("home_win_probability"),
        },
        "totals": {
            "over": sim.get("over_probability"),
            "under": sim.get("under_probability"),
            "push": sim.get("push_probability"),
        },
    }


def _build_diagnostic(shared_payload: Dict[str, Any], meta: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "modelVersion": meta.get("model_version"),
        "sourceBuilder": meta.get("source_builder"),
        "simulationCount": shared_payload.get("simulation_count"),
        "seed": shared_payload.get("seed"),
        "starterExitEnabled": shared_payload.get("starter_exit_enabled"),
        "engineVersion": meta.get("engine_version"),
    }


def _build_direct_inputs(inputs: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "offense": inputs.get("offense_profiles"),
        "pitching": {
            "starter": inputs.get("starter_profiles"),
            "bullpen": inputs.get("bullpen_profiles"),
        },
        "environment": inputs.get("environment"),
        "plateAppearanceModel": _build_pa_model(inputs),
    }


def _build_pa_model(inputs: Dict[str, Any]) -> Dict[str, Any]:
    pa = inputs.get("pa_outcome_model") or {}

    return {
        "strikeout": {
            "offenseK": pa.get("offense_k_rate"),
            "starterK": pa.get("starter_k_rate"),
            "bullpenK": pa.get("bullpen_k_rate"),
            "modelProbability": pa.get("k_probability"),
        },
        "walk": {
            "offenseBB": pa.get("offense_bb_rate"),
            "starterBB": pa.get("starter_bb_rate"),
            "bullpenBB": pa.get("bullpen_bb_rate"),
            "modelProbability": pa.get("bb_probability"),
        },
        "hit": {
            "offenseBA": pa.get("offense_ba"),
            "starterXBA": pa.get("starter_xba_allowed"),
            "bullpenXBA": pa.get("bullpen_xba_allowed"),
            "modelProbability": pa.get("hit_probability"),
        },
        "power": {
            "offenseISO": pa.get("offense_iso"),
            "starterHardHit": pa.get("starter_hard_hit_allowed"),
            "bullpenHardHit": pa.get("bullpen_hard_hit_allowed"),
            "modelProbability": pa.get("xbh_probability"),
        },
    }


def _build_context(shared_payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "notes": shared_payload.get("notes"),
        "missingInputs": shared_payload.get("missing_inputs"),
    }