"""Version and provenance contracts for runner advancement."""

from __future__ import annotations

from dataclasses import dataclass


BASELINE_RUNNER_ADVANCEMENT_MODEL_VERSION = (
    "baseline_runner_advancement_v1"
)


@dataclass(frozen=True)
class AdvancementModelMetadata:
    """Stable metadata attached to an advancement result."""

    model_version: str
    source: str
    calibrated: bool
    production_enabled: bool

    def __post_init__(self) -> None:
        if not self.model_version:
            raise ValueError("model_version is required")
        if not self.source:
            raise ValueError("source is required")


BASELINE_RUNNER_ADVANCEMENT_METADATA = AdvancementModelMetadata(
    model_version=BASELINE_RUNNER_ADVANCEMENT_MODEL_VERSION,
    source="versioned_initial_simulation_assumptions",
    calibrated=False,
    production_enabled=False,
)
