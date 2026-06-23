"""
Shared game simulation builder.

Goal:
    Make /models/projections and /matchup/:game_pk use the same simulation
    source of truth so expected runs, win probabilities, totals, and metadata
    are traceable and consistent.

This file is intentionally defensive because the sandbox engine may expose
slightly different function names while we migrate it into mlb_app/simulation.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, Optional


MODEL_VERSION = "shared-simulation-v1"
SOURCE_BUILDER = "mlb_app.simulation.game_simulation_builder"
CALIBRATION_VERSION = "calibration-v1"


def _load_sandbox_engine():
    """
    Temporary bridge to the sandbox engine.

    Long-term target:
        Move sandbox_matchup_engine_full.py into:
            mlb_app/simulation/game_engine_v2.py

        Then replace this loader with:
            from mlb_app.simulation.game_engine_v2 import ...
    """
    try:
        from mlb_app.simulation.game_engine_v2 import run_full_game_simulation as engine
        return engine
    except Exception as exc:
        raise RuntimeError(
            "Could not import sandbox_matchup_engine_full.py. "
            "Make sure it exists in the repo root while we migrate the engine."
        ) from exc


def _call_engine(engine: Any, game_pk: int, config: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Try common sandbox engine entrypoints without assuming the exact function name.
    """

    candidate_names = [
        "build_matchup_payload",
        "build_matchup",
        "build_game_payload",
        "run_full_game_simulation",
        "run_game_simulation",
        "simulate_game",
        "main",
    ]

    for name in candidate_names:
        fn = getattr(engine, name, None)
        if callable(fn):
            try:
                result = fn(game_pk=game_pk, config=config or {})
            except TypeError:
                try:
                    result = fn(game_pk)
                except TypeError:
                    continue

            if isinstance(result, dict):
                return result

            return {"raw_result": result}

    raise RuntimeError(
        "No supported sandbox engine entrypoint found. "
        "Expected one of: " + ", ".join(candidate_names)
    )


def _normalize_metadata(
    payload: Dict[str, Any],
    *,
    game_pk: int,
    config: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Adds the roadmap source-of-truth contract without destroying existing data.
    """

    existing_meta = payload.get("meta") or payload.get("metadata") or {}

    simulation_count = (
        payload.get("simulation_count")
        or existing_meta.get("simulation_count")
        or (config or {}).get("simulation_count")
    )

    seed = (
        payload.get("seed")
        or existing_meta.get("seed")
        or (config or {}).get("seed")
    )

    starter_quality_score = (
        payload.get("starter_quality_score")
        or existing_meta.get("starter_quality_score")
    )

    starter_quality_label = (
        payload.get("starter_quality_label")
        or existing_meta.get("starter_quality_label")
    )

    metadata = {
        **existing_meta,
        "game_pk": game_pk,
        "model_version": existing_meta.get("model_version", MODEL_VERSION),
        "source_builder": existing_meta.get("source_builder", SOURCE_BUILDER),
        "simulation_count": simulation_count,
        "seed": seed,
        "starter_exit_enabled": existing_meta.get("starter_exit_enabled", True),
        "starter_quality_score": starter_quality_score,
        "starter_quality_label": starter_quality_label,
        "calibration_version": existing_meta.get("calibration_version", CALIBRATION_VERSION),
        "offense_source": existing_meta.get("offense_source", payload.get("offense_source")),
        "pitcher_source": existing_meta.get("pitcher_source", payload.get("pitcher_source")),
        "bullpen_source": existing_meta.get("bullpen_source", payload.get("bullpen_source")),
        "environment_source": existing_meta.get("environment_source", payload.get("environment_source")),
    }

    payload["meta"] = metadata
    payload["metadata"] = metadata

    return payload


def _attach_pitching_plan_diagnostics(
    payload: Dict[str, Any],
    *,
    config: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Append optional pitching-plan diagnostics without changing simulation
    inputs, engine behavior, or canonical probability authority.

    The classifier import is intentionally lazy so the disabled path neither
    imports nor calls the classifier.
    """

    config_snapshot = deepcopy(
        dict(config or {})
    )

    if not config_snapshot.get(
        "pitching_plan_diagnostics_enabled",
        False,
    ):
        return payload

    version = str(
        config_snapshot.get(
            "pitching_plan_diagnostics_version",
            "pitching-plan-diagnostics-v1",
        )
    )

    evidence = deepcopy(
        config_snapshot.get(
            "pitching_plan_evidence"
        )
        or {}
    )

    diagnostic_payload: Dict[str, Any] = {
        "enabled": True,
        "status": "error",
        "version": version,
        "classification": None,
        "validation": None,
        "error": None,
        "behavioral_effect": "none",
        (
            "canonical_probability_"
            "authority_changed"
        ): False,
        "production_activation": False,
    }

    try:
        from mlb_app.simulation.pitching_plan_classifier import (
            classify_pitching_plan,
            validate_pitching_plan_payload,
        )

        classification = classify_pitching_plan(
            evidence
        )

        validation = (
            validate_pitching_plan_payload(
                classification
            )
        )

        diagnostic_payload[
            "classification"
        ] = classification

        diagnostic_payload[
            "validation"
        ] = validation

        diagnostic_payload["status"] = (
            "classified"
            if validation.get("valid") is True
            else "validation_failed"
        )
    except Exception as exc:
        diagnostic_payload["status"] = "error"

        diagnostic_payload["error"] = {
            "type": type(exc).__name__,
            "message": str(exc),
        }

    metadata = (
        payload.get("meta")
        or payload.get("metadata")
        or {}
    )

    metadata = {
        **metadata,
        "pitching_plan_diagnostics": (
            diagnostic_payload
        ),
    }

    payload["meta"] = metadata
    payload["metadata"] = metadata

    return payload


def _attach_starter_hook_diagnostics(
    payload: Dict[str, Any],
    *,
    config: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Append optional starter-hook diagnostics without changing simulation
    inputs, engine behavior, starter usage, or canonical probability authority.

    The evaluator import is intentionally lazy so the disabled path neither
    imports nor calls the evaluator.
    """

    config_snapshot = deepcopy(
        dict(config or {})
    )

    if not config_snapshot.get(
        "starter_hook_diagnostics_enabled",
        False,
    ):
        return payload

    version = str(
        config_snapshot.get(
            "starter_hook_diagnostics_version",
            "starter-hook-diagnostics-v1",
        )
    )

    state = deepcopy(
        config_snapshot.get(
            "starter_hook_state"
        )
        or {}
    )

    diagnostic_payload: Dict[str, Any] = {
        "enabled": True,
        "status": "error",
        "version": version,
        "evaluation": None,
        "validation": None,
        "error": None,
        "behavioral_effect": "none",
        (
            "canonical_probability_"
            "authority_changed"
        ): False,
        "production_activation": False,
    }

    try:
        from mlb_app.simulation.starter_hook_evaluator import (
            evaluate_starter_hook,
            validate_starter_hook_evaluation,
        )

        evaluation = evaluate_starter_hook(
            state
        )

        validation = (
            validate_starter_hook_evaluation(
                evaluation
            )
        )

        diagnostic_payload[
            "evaluation"
        ] = evaluation

        diagnostic_payload[
            "validation"
        ] = validation

        diagnostic_payload["status"] = (
            "evaluated"
            if validation.get("valid") is True
            else "validation_failed"
        )
    except Exception as exc:
        diagnostic_payload["status"] = "error"

        diagnostic_payload["error"] = {
            "type": type(exc).__name__,
            "message": str(exc),
        }

    metadata = (
        payload.get("meta")
        or payload.get("metadata")
        or {}
    )

    metadata = {
        **metadata,
        "starter_hook_diagnostics": (
            diagnostic_payload
        ),
    }

    payload["meta"] = metadata
    payload["metadata"] = metadata

    return payload


def _attach_bullpen_sequence_diagnostics(
    payload: Dict[str, Any],
    *,
    config: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Append optional bullpen-sequence diagnostics without changing simulation
    inputs, engine behavior, pitcher usage, or canonical probability authority.

    The evaluator import is intentionally lazy so the disabled path neither
    imports nor calls the evaluator.
    """

    config_snapshot = deepcopy(
        dict(config or {})
    )

    if not config_snapshot.get(
        "bullpen_sequence_diagnostics_enabled",
        False,
    ):
        return payload

    version = str(
        config_snapshot.get(
            "bullpen_sequence_diagnostics_version",
            "bullpen-sequence-diagnostics-v1",
        )
    )

    state = deepcopy(
        config_snapshot.get(
            "bullpen_sequence_state"
        )
        or {}
    )

    diagnostic_payload: Dict[str, Any] = {
        "enabled": True,
        "status": "error",
        "version": version,
        "evaluation": None,
        "validation": None,
        "error": None,
        "behavioral_effect": "none",
        (
            "canonical_probability_"
            "authority_changed"
        ): False,
        "production_activation": False,
    }

    try:
        from mlb_app.simulation.bullpen_sequence_evaluator import (
            evaluate_bullpen_sequence,
            validate_bullpen_sequence_evaluation,
        )

        evaluation = evaluate_bullpen_sequence(
            state
        )

        validation = (
            validate_bullpen_sequence_evaluation(
                evaluation
            )
        )

        diagnostic_payload[
            "evaluation"
        ] = evaluation

        diagnostic_payload[
            "validation"
        ] = validation

        diagnostic_payload["status"] = (
            "evaluated"
            if validation.get("valid") is True
            else "validation_failed"
        )
    except Exception as exc:
        diagnostic_payload["status"] = "error"

        diagnostic_payload["error"] = {
            "type": type(exc).__name__,
            "message": str(exc),
        }

    metadata = (
        payload.get("meta")
        or payload.get("metadata")
        or {}
    )

    metadata = {
        **metadata,
        "bullpen_sequence_diagnostics": (
            diagnostic_payload
        ),
    }

    payload["meta"] = metadata
    payload["metadata"] = metadata

    return payload


def build_game_simulation(
    game_pk: int,
    config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    import traceback

    engine = _load_sandbox_engine()
    config_snapshot = deepcopy(
        dict(config or {})
    )

    engine_config = {
        key: deepcopy(value)
        for key, value in config_snapshot.items()
        if key
        not in {
            "pitching_plan_diagnostics_enabled",
            "pitching_plan_evidence",
            "pitching_plan_diagnostics_version",
            "starter_hook_diagnostics_enabled",
            "starter_hook_diagnostics_version",
            "starter_hook_state",
            "bullpen_sequence_diagnostics_enabled",
            "bullpen_sequence_diagnostics_version",
            "bullpen_sequence_state",
        }
    }

    try:
        payload = engine(
            int(game_pk),
            engine_config,
        )

        normalized_payload = _normalize_metadata(
            payload,
            game_pk=int(game_pk),
            config=engine_config,
        )

        pitching_plan_payload = (
            _attach_pitching_plan_diagnostics(
                normalized_payload,
                config=config_snapshot,
            )
        )

        starter_hook_payload = (
            _attach_starter_hook_diagnostics(
                pitching_plan_payload,
                config=config_snapshot,
            )
        )

        return _attach_bullpen_sequence_diagnostics(
            starter_hook_payload,
            config=config_snapshot,
        )
    except Exception as exc:
        return {
            "status": "error",
            "error": str(exc),
            "traceback": traceback.format_exc(),  # 🔥 THIS IS THE KEY
        }
