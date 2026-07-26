"""Select calibrated baserunning with an immediate legacy rollback."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
import os
from typing import Any, Dict, Mapping, Optional

from .live_baserunning_shadow_execution import (
    CanonicalLiveBaserunningShadowExecution,
)
from .production_execution import (
    CanonicalProductionShadowExecution,
)


CANONICAL_CALIBRATED_BASERUNNING_ACTIVATION_VERSION = (
    "canonical_calibrated_baserunning_activation_v1"
)
CALIBRATED_BASERUNNING_ENABLED_ENV = (
    "MLB_CALIBRATED_BASERUNNING_ENABLED"
)
_FALSE_VALUES = {
    "",
    "0",
    "false",
    "no",
    "off",
}


def calibrated_baserunning_enabled(
    value: Optional[Any] = None,
) -> bool:
    """
    Resolve explicit override, environment rollback, then active default.

    The calibrated model is active after this release unless the rollback
    flag is explicitly disabled.
    """

    raw = (
        value
        if value is not None
        else os.getenv(
            CALIBRATED_BASERUNNING_ENABLED_ENV,
            "true",
        )
    )

    if isinstance(raw, bool):
        return raw

    return (
        str(raw).strip().lower()
        not in _FALSE_VALUES
    )


@dataclass(frozen=True)
class CanonicalCalibratedBaserunningActivation:
    fallback_execution: CanonicalProductionShadowExecution
    paired_execution: (
        CanonicalLiveBaserunningShadowExecution
    )
    activation_requested: bool
    activation_version: str = (
        CANONICAL_CALIBRATED_BASERUNNING_ACTIVATION_VERSION
    )

    def __post_init__(self) -> None:
        if not isinstance(
            self.fallback_execution,
            CanonicalProductionShadowExecution,
        ):
            raise TypeError(
                "fallback_execution must be canonical"
            )

        if not isinstance(
            self.paired_execution,
            CanonicalLiveBaserunningShadowExecution,
        ):
            raise TypeError(
                "paired_execution must be canonical"
            )

        if not isinstance(
            self.activation_requested,
            bool,
        ):
            raise TypeError(
                "activation_requested must be boolean"
            )

        if self.activation_version != (
            CANONICAL_CALIBRATED_BASERUNNING_ACTIVATION_VERSION
        ):
            raise ValueError(
                "unsupported calibrated baserunning "
                "activation version"
            )

    @property
    def calibrated_ready(self) -> bool:
        return (
            self.paired_execution
            .calibrated_execution
            .executed
            and self.paired_execution.observation.ready
            and self.paired_execution
            .observation
            .input_parity_verified
            and self.paired_execution
            .observation
            .seed_parity_verified
        )

    @property
    def activated(self) -> bool:
        return (
            self.activation_requested
            and self.calibrated_ready
        )

    @property
    def production_execution(
        self,
    ) -> CanonicalProductionShadowExecution:
        if self.activated:
            return (
                self.paired_execution
                .calibrated_execution
            )

        return self.fallback_execution

    @property
    def fallback_reason(self) -> Optional[str]:
        if self.activated:
            return None
        if not self.activation_requested:
            return "rollback_flag_disabled"
        return "calibrated_baserunning_unavailable"

    def to_diagnostics(self) -> Dict[str, Any]:
        return {
            "schema_version": self.activation_version,
            "activation_requested": (
                self.activation_requested
            ),
            "calibrated_ready": (
                self.calibrated_ready
            ),
            "production_activation": self.activated,
            "fallback_used": not self.activated,
            "fallback_reason": self.fallback_reason,
            "selected_execution": (
                "calibrated"
                if self.activated
                else "legacy_fallback"
            ),
            "probability_transform": {
                "attempt_probability_multiplier": 0.52,
                "success_rate_adjustment": 0.09,
                "transform_digest": (
                    self.paired_execution
                    .observation
                    .calibrated_transform_digest
                ),
                "frozen_during_monitoring_window": True,
            },
            "post_activation_monitoring_target": {
                "game_count": 100,
                "parameters_may_reselect": False,
            },
            "rollback_environment_variable": (
                CALIBRATED_BASERUNNING_ENABLED_ENV
            ),
            "production_authority_changed": (
                self.activated
            ),
            "authoritative_source": (
                "canonical_calibrated_baserunning"
                if self.activated
                else "legacy"
            ),
        }


def activate_calibrated_baserunning(
    *,
    fallback_execution: (
        CanonicalProductionShadowExecution
    ),
    paired_execution: (
        CanonicalLiveBaserunningShadowExecution
    ),
    enabled: Optional[Any] = None,
) -> CanonicalCalibratedBaserunningActivation:
    return CanonicalCalibratedBaserunningActivation(
        fallback_execution=fallback_execution,
        paired_execution=paired_execution,
        activation_requested=(
            calibrated_baserunning_enabled(enabled)
        ),
    )



def _distribution_mean(
    values: Mapping[str, Any],
) -> float:
    if not isinstance(values, Mapping) or not values:
        raise ValueError(
            "canonical run distribution is required"
        )

    return round(
        sum(
            float(run_value) * float(probability)
            for run_value, probability
            in values.items()
        ),
        6,
    )


def apply_calibrated_baserunning_production_authority(
    *,
    legacy_result: Dict[str, Any],
    activation: CanonicalCalibratedBaserunningActivation,
) -> Dict[str, Any]:
    """
    Promote a ready calibrated canonical execution into production.

    The existing production envelope and direct inputs are preserved.
    Only the simulation outputs selected by Model Projections are replaced.
    Rollback or unavailable canonical evidence preserves legacy outputs.
    """

    if not isinstance(legacy_result, dict):
        raise TypeError(
            "legacy_result must be a dictionary"
        )
    if not isinstance(
        activation,
        CanonicalCalibratedBaserunningActivation,
    ):
        raise TypeError(
            "activation must be canonical"
        )

    output = deepcopy(legacy_result)
    diagnostics = output.setdefault(
        "diagnostics",
        {},
    )

    if not isinstance(diagnostics, dict):
        diagnostics = {
            "legacy_diagnostics": deepcopy(
                diagnostics
            )
        }
        output["diagnostics"] = diagnostics

    activation_diagnostics = (
        activation.to_diagnostics()
    )
    diagnostics[
        "calibrated_baserunning_activation"
    ] = activation_diagnostics

    if not activation.activated:
        return output

    material = (
        activation.production_execution.material
    )

    if material is None:
        raise RuntimeError(
            "activated canonical execution has no material"
        )

    canonical_payload = material.canonical_payload
    outcomes = canonical_payload.get("outcomes")

    if not isinstance(outcomes, Mapping):
        raise ValueError(
            "canonical production outcomes are required"
        )

    away_distribution = outcomes.get(
        "away_run_distribution"
    )
    home_distribution = outcomes.get(
        "home_run_distribution"
    )
    total_distribution = outcomes.get(
        "total_run_distribution"
    )

    away_expected_runs = _distribution_mean(
        away_distribution
    )
    home_expected_runs = _distribution_mean(
        home_distribution
    )
    total_expected_runs = _distribution_mean(
        total_distribution
    )

    canonical_simulation = {
        "simulation_count": int(
            outcomes["simulation_count"]
        ),
        "away_win_probability": float(
            outcomes["away_win_probability"]
        ),
        "home_win_probability": float(
            outcomes["home_win_probability"]
        ),
        "tie_probability": float(
            outcomes["tie_probability"]
        ),
        "extra_innings_probability": float(
            outcomes[
                "extra_innings_probability"
            ]
        ),
        "walk_off_probability": float(
            outcomes["walk_off_probability"]
        ),
        "away_expected_runs": away_expected_runs,
        "home_expected_runs": home_expected_runs,
        "total_expected_runs": total_expected_runs,
        "expected_total_runs": total_expected_runs,
        "away_run_distribution": dict(
            away_distribution
        ),
        "home_run_distribution": dict(
            home_distribution
        ),
        "total_run_distribution": dict(
            total_distribution
        ),
        "team_total_probabilities": dict(
            outcomes["team_total_probabilities"]
        ),
        "total_probabilities": dict(
            outcomes["total_probabilities"]
        ),
        "model_version": canonical_payload[
            "model_version"
        ],
        "run_id": canonical_payload["run_id"],
        "source": (
            "canonical_event_driven_simulation"
        ),
        "production_activation": True,
        "production_authority_changed": True,
        "authoritative_source": (
            "canonical_event_driven_"
            "calibrated_baserunning"
        ),
    }

    derived_outputs = output.setdefault(
        "derived_outputs",
        {},
    )

    if not isinstance(derived_outputs, dict):
        derived_outputs = {}
        output["derived_outputs"] = derived_outputs

    legacy_game = derived_outputs.get(
        "game_simulation"
    )
    legacy_bullpen = derived_outputs.get(
        "bullpen_adjusted_game_simulation"
    )

    diagnostics[
        "pre_activation_legacy_simulation"
    ] = {
        "game_simulation_model_version": (
            legacy_game.get("model_version")
            if isinstance(legacy_game, dict)
            else None
        ),
        "bullpen_adjusted_model_version": (
            legacy_bullpen.get("model_version")
            if isinstance(legacy_bullpen, dict)
            else None
        ),
        "preserved_for_rollback": True,
    }

    derived_outputs[
        "game_simulation"
    ] = deepcopy(canonical_simulation)
    derived_outputs[
        "bullpen_adjusted_game_simulation"
    ] = deepcopy(canonical_simulation)

    metadata = output.get("meta")

    if not isinstance(metadata, dict):
        metadata = {}
        output["meta"] = metadata

    metadata.update(
        {
            "model_version": canonical_payload[
                "model_version"
            ],
            "canonical_run_id": canonical_payload[
                "run_id"
            ],
            "production_activation": True,
            "authoritative_source": (
                "canonical_event_driven_"
                "calibrated_baserunning"
            ),
        }
    )

    canonical_shadow = diagnostics.get(
        "canonical_shadow"
    )

    if isinstance(canonical_shadow, dict):
        canonical_shadow[
            "authoritative_source"
        ] = (
            "canonical_event_driven_"
            "calibrated_baserunning"
        )
        canonical_shadow[
            "production_activation"
        ] = True

        player_projections = (
            canonical_shadow.get(
                "player_projections"
            )
        )

        if isinstance(player_projections, dict):
            player_projections[
                "authoritative"
            ] = True
            player_projections[
                "authoritative_source"
            ] = (
                "canonical_event_driven_"
                "calibrated_baserunning"
            )

    return output
