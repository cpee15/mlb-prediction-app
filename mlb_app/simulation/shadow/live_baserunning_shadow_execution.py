"""Execute paired legacy and calibrated live baserunning shadows."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from typing import Any, Dict

from mlb_app.simulation.game import (
    CanonicalBaserunningProbabilityTransform,
)

from .baserunning_evidence_discovery import (
    CanonicalShadowBaserunningEvidenceDiscovery,
)
from .baserunning_output_validation import (
    validate_canonical_baserunning_shadow_outputs,
)
from .baserunning_production_execution import (
    run_canonical_production_shadow_with_baserunning_discovery,
)
from .historical_baserunning_holdout_validation import (
    HISTORICAL_BASERUNNING_SELECTED_ATTEMPT_MULTIPLIER,
    HISTORICAL_BASERUNNING_SELECTED_SUCCESS_ADJUSTMENT,
)
from .live_baserunning_shadow_monitoring import (
    CanonicalLiveBaserunningShadowObservation,
)
from .production_execution import (
    CanonicalProductionShadowExecution,
)


CANONICAL_LIVE_BASERUNNING_SHADOW_EXECUTION_VERSION = (
    "canonical_live_baserunning_shadow_execution_v1"
)


def _sha256(value: Dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


@dataclass(frozen=True)
class CanonicalLiveBaserunningShadowExecution:
    legacy_execution: CanonicalProductionShadowExecution
    calibrated_execution: CanonicalProductionShadowExecution
    observation: CanonicalLiveBaserunningShadowObservation
    execution_version: str = (
        CANONICAL_LIVE_BASERUNNING_SHADOW_EXECUTION_VERSION
    )

    def __post_init__(self) -> None:
        for field_name in (
            "legacy_execution",
            "calibrated_execution",
        ):
            if not isinstance(
                getattr(self, field_name),
                CanonicalProductionShadowExecution,
            ):
                raise TypeError(
                    f"{field_name} must be canonical"
                )

        if not isinstance(
            self.observation,
            CanonicalLiveBaserunningShadowObservation,
        ):
            raise TypeError(
                "observation must be canonical"
            )

        if self.execution_version != (
            CANONICAL_LIVE_BASERUNNING_SHADOW_EXECUTION_VERSION
        ):
            raise ValueError(
                "unsupported live baserunning shadow "
                "execution version"
            )

    @property
    def production_execution(
        self,
    ) -> CanonicalProductionShadowExecution:
        return self.legacy_execution

    def to_diagnostics(self) -> Dict[str, Any]:
        return {
            "schema_version": self.execution_version,
            "legacy_execution": (
                self.legacy_execution.to_diagnostics()
            ),
            "calibrated_execution": (
                self.calibrated_execution.to_diagnostics()
            ),
            "observation": (
                self.observation.to_diagnostics()
            ),
            "production_result": "legacy_execution",
            "activation_permitted": False,
            "production_activation": False,
            "production_authority_changed": False,
            "authoritative_source": "legacy",
        }


def execute_live_baserunning_shadow_pair(
    *,
    game_date: str,
    baserunning_evidence_discovery: (
        CanonicalShadowBaserunningEvidenceDiscovery
    ),
    **production_inputs: Any,
) -> CanonicalLiveBaserunningShadowExecution:
    if "baserunning_probability_transform" in production_inputs:
        raise ValueError(
            "live paired execution owns the calibrated "
            "probability transform"
        )

    game_pk = int(production_inputs.get("game_pk"))
    simulation_count = int(
        production_inputs.get(
            "simulation_count",
            25,
        )
    )

    transform = CanonicalBaserunningProbabilityTransform(
        attempt_probability_multiplier=(
            HISTORICAL_BASERUNNING_SELECTED_ATTEMPT_MULTIPLIER
        ),
        success_rate_adjustment=(
            HISTORICAL_BASERUNNING_SELECTED_SUCCESS_ADJUSTMENT
        ),
    )

    legacy_execution = (
        run_canonical_production_shadow_with_baserunning_discovery(
            baserunning_evidence_discovery=(
                baserunning_evidence_discovery
            ),
            **production_inputs,
        )
    )
    calibrated_execution = (
        run_canonical_production_shadow_with_baserunning_discovery(
            baserunning_evidence_discovery=(
                baserunning_evidence_discovery
            ),
            baserunning_probability_transform=transform,
            **production_inputs,
        )
    )

    legacy_validation = (
        validate_canonical_baserunning_shadow_outputs(
            legacy_execution
        )
    )
    calibrated_validation = (
        validate_canonical_baserunning_shadow_outputs(
            calibrated_execution
        )
    )

    legacy_diagnostics = (
        legacy_execution.to_diagnostics()
    )
    calibrated_diagnostics = (
        calibrated_execution.to_diagnostics()
    )

    parity_fields = (
        "provider_identity",
        "exact_artifact_digest",
        "fallback_catalog_digest",
        "baserunning_evidence_catalog_digest",
    )
    input_parity_verified = (
        legacy_execution.executed
        and calibrated_execution.executed
        and all(
            legacy_diagnostics.get(field_name)
            == calibrated_diagnostics.get(field_name)
            for field_name in parity_fields
        )
    )
    seed_parity_verified = (
        input_parity_verified
        and legacy_execution.simulation_count
        == calibrated_execution.simulation_count
        == simulation_count
    )

    paired_context_digest = _sha256(
        {
            "game_pk": game_pk,
            "game_date": game_date,
            "simulation_count": simulation_count,
            "provider_identity": (
                legacy_diagnostics.get(
                    "provider_identity"
                )
            ),
            "exact_artifact_digest": (
                legacy_diagnostics.get(
                    "exact_artifact_digest"
                )
            ),
            "fallback_catalog_digest": (
                legacy_diagnostics.get(
                    "fallback_catalog_digest"
                )
            ),
            "baserunning_evidence_catalog_digest": (
                legacy_diagnostics.get(
                    "baserunning_evidence_catalog_digest"
                )
            ),
            "seed_policy": (
                "canonical_trial_seed_same_game_config_v1"
            ),
        }
    )

    observation = (
        CanonicalLiveBaserunningShadowObservation(
            game_pk=game_pk,
            game_date=game_date,
            paired_context_digest=(
                paired_context_digest
            ),
            calibrated_transform_digest=(
                transform.digest
            ),
            legacy_validation=legacy_validation,
            calibrated_validation=(
                calibrated_validation
            ),
            input_parity_verified=(
                input_parity_verified
            ),
            seed_parity_verified=(
                seed_parity_verified
            ),
        )
    )

    return CanonicalLiveBaserunningShadowExecution(
        legacy_execution=legacy_execution,
        calibrated_execution=calibrated_execution,
        observation=observation,
    )
