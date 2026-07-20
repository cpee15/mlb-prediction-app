"""Production-facing canonical trial-factory input contracts."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
from typing import Any, Dict, Mapping, Tuple


CANONICAL_TRIAL_FACTORY_INPUT_VERSION = (
    "canonical_trial_factory_input_v1"
)
CANONICAL_TRIAL_SEED_VERSION = (
    "canonical_trial_seed_v1"
)
DEFAULT_CANONICAL_SIMULATION_COUNT = 1000
DEFAULT_CANONICAL_MODEL_VERSION = (
    "canonical-event-model-v1"
)
MAX_CANONICAL_SEED = (2**63) - 1


FrozenConfigValue = Any
FrozenConfig = Tuple[
    Tuple[str, FrozenConfigValue],
    ...,
]


@dataclass(frozen=True)
class CanonicalTrialFactoryInput:
    """
    Stable inputs for constructing one canonical trial batch.

    Trial seeds are derived before any matchup-specific resolver is
    constructed. This keeps trial identity independent from probability
    generation, lineup selection, and pitcher-selection implementation.
    """

    game_pk: int
    simulation_count: int
    model_version: str
    base_seed: int
    seed_source: str
    trial_seeds: Tuple[int, ...]
    config: FrozenConfig
    schema_version: str = (
        CANONICAL_TRIAL_FACTORY_INPUT_VERSION
    )
    seed_version: str = (
        CANONICAL_TRIAL_SEED_VERSION
    )

    def __post_init__(self) -> None:
        if self.game_pk <= 0:
            raise ValueError(
                "game_pk must be positive"
            )
        if self.simulation_count <= 0:
            raise ValueError(
                "simulation_count must be positive"
            )
        if not self.model_version:
            raise ValueError(
                "model_version is required"
            )
        if not 0 <= self.base_seed <= MAX_CANONICAL_SEED:
            raise ValueError(
                "base_seed is outside the supported range"
            )
        if self.seed_source not in {
            "explicit",
            "derived",
        }:
            raise ValueError(
                "seed_source must be explicit or derived"
            )
        if len(self.trial_seeds) != self.simulation_count:
            raise ValueError(
                "trial seed count must match simulations"
            )
        if len(self.trial_seeds) != len(
            set(self.trial_seeds)
        ):
            raise ValueError(
                "trial seeds must be unique"
            )
        if any(
            not 0 <= seed <= MAX_CANONICAL_SEED
            for seed in self.trial_seeds
        ):
            raise ValueError(
                "trial seed is outside the supported range"
            )
        if self.schema_version != (
            CANONICAL_TRIAL_FACTORY_INPUT_VERSION
        ):
            raise ValueError(
                "unsupported canonical factory-input schema"
            )
        if self.seed_version != (
            CANONICAL_TRIAL_SEED_VERSION
        ):
            raise ValueError(
                "unsupported canonical seed version"
            )

        config_names = tuple(
            name for name, _ in self.config
        )

        if config_names != tuple(
            sorted(config_names)
        ):
            raise ValueError(
                "factory config keys must be ordered"
            )
        if len(config_names) != len(
            set(config_names)
        ):
            raise ValueError(
                "factory config keys must be unique"
            )

    def config_dict(self) -> Dict[str, Any]:
        """Return a detached mutable copy of ordinary factory inputs."""

        return {
            name: _thaw_config_value(value)
            for name, value in self.config
        }

    def seed_for_trial(
        self,
        trial_index: int,
    ) -> int:
        """Return the deterministic seed for one zero-based trial."""

        if (
            trial_index < 0
            or trial_index >= self.simulation_count
        ):
            raise IndexError(
                "trial_index is outside the batch"
            )

        return self.trial_seeds[trial_index]


def build_canonical_trial_factory_input(
    *,
    game_pk: int,
    config: Mapping[str, Any] | None = None,
) -> CanonicalTrialFactoryInput:
    """Normalize production builder inputs into a stable contract."""

    if isinstance(game_pk, bool):
        raise TypeError(
            "game_pk must be an integer"
        )

    try:
        normalized_game_pk = int(game_pk)
    except (TypeError, ValueError) as exc:
        raise TypeError(
            "game_pk must be an integer"
        ) from exc

    config_snapshot = dict(config or {})

    simulation_count = _positive_int(
        config_snapshot.get(
            "simulation_count",
            DEFAULT_CANONICAL_SIMULATION_COUNT,
        ),
        name="simulation_count",
    )

    model_version = str(
        config_snapshot.get(
            "canonical_model_version",
            DEFAULT_CANONICAL_MODEL_VERSION,
        )
    ).strip()

    if not model_version:
        raise ValueError(
            "canonical model version is required"
        )

    explicit_seed = config_snapshot.get("seed")

    if explicit_seed is None:
        base_seed = derive_canonical_base_seed(
            game_pk=normalized_game_pk,
            model_version=model_version,
        )
        seed_source = "derived"
    else:
        base_seed = _seed_int(
            explicit_seed,
            name="seed",
        )
        seed_source = "explicit"

    trial_seeds = tuple(
        derive_canonical_trial_seed(
            base_seed=base_seed,
            trial_index=trial_index,
        )
        for trial_index in range(
            simulation_count
        )
    )

    return CanonicalTrialFactoryInput(
        game_pk=normalized_game_pk,
        simulation_count=simulation_count,
        model_version=model_version,
        base_seed=base_seed,
        seed_source=seed_source,
        trial_seeds=trial_seeds,
        config=_freeze_config(config_snapshot),
    )


def derive_canonical_base_seed(
    *,
    game_pk: int,
    model_version: str,
) -> int:
    """
    Derive a stable game-level seed when no explicit seed is supplied.

    Simulation count is intentionally excluded so increasing the number of
    trials preserves the existing trial-seed prefix.
    """

    if game_pk <= 0:
        raise ValueError(
            "game_pk must be positive"
        )
    if not model_version:
        raise ValueError(
            "model_version is required"
        )

    return _hash_seed(
        CANONICAL_TRIAL_SEED_VERSION,
        "base",
        str(game_pk),
        model_version,
    )


def derive_canonical_trial_seed(
    *,
    base_seed: int,
    trial_index: int,
) -> int:
    """Derive one stable zero-based trial seed."""

    normalized_seed = _seed_int(
        base_seed,
        name="base_seed",
    )

    if isinstance(trial_index, bool):
        raise TypeError(
            "trial_index must be an integer"
        )

    try:
        normalized_index = int(trial_index)
    except (TypeError, ValueError) as exc:
        raise TypeError(
            "trial_index must be an integer"
        ) from exc

    if normalized_index < 0:
        raise ValueError(
            "trial_index cannot be negative"
        )

    return _hash_seed(
        CANONICAL_TRIAL_SEED_VERSION,
        "trial",
        str(normalized_seed),
        str(normalized_index),
    )


def _hash_seed(*parts: str) -> int:
    encoded = "\x1f".join(parts).encode(
        "utf-8"
    )
    digest = hashlib.sha256(encoded).digest()

    return int.from_bytes(
        digest[:8],
        byteorder="big",
        signed=False,
    ) & MAX_CANONICAL_SEED


def _positive_int(
    value,
    *,
    name: str,
) -> int:
    if isinstance(value, bool):
        raise TypeError(
            f"{name} must be an integer"
        )

    try:
        normalized = int(value)
    except (TypeError, ValueError) as exc:
        raise TypeError(
            f"{name} must be an integer"
        ) from exc

    if normalized <= 0:
        raise ValueError(
            f"{name} must be positive"
        )

    return normalized


def _seed_int(
    value,
    *,
    name: str,
) -> int:
    if isinstance(value, bool):
        raise TypeError(
            f"{name} must be an integer"
        )

    try:
        normalized = int(value)
    except (TypeError, ValueError) as exc:
        raise TypeError(
            f"{name} must be an integer"
        ) from exc

    if not 0 <= normalized <= MAX_CANONICAL_SEED:
        raise ValueError(
            f"{name} is outside the supported range"
        )

    return normalized


def _freeze_config(
    config: Mapping[str, Any],
) -> FrozenConfig:
    return tuple(
        (
            str(name),
            _freeze_config_value(value),
        )
        for name, value in sorted(
            config.items(),
            key=lambda item: str(item[0]),
        )
    )


def _freeze_config_value(
    value,
) -> FrozenConfigValue:
    if value is None or isinstance(
        value,
        (bool, int, float, str),
    ):
        return value

    if isinstance(value, Mapping):
        return (
            "__mapping__",
            tuple(
                (
                    str(name),
                    _freeze_config_value(item),
                )
                for name, item in sorted(
                    value.items(),
                    key=lambda pair: str(
                        pair[0]
                    ),
                )
            ),
        )

    if isinstance(value, (list, tuple)):
        return (
            "__sequence__",
            tuple(
                _freeze_config_value(item)
                for item in value
            ),
        )

    raise TypeError(
        "canonical factory config values must "
        "contain only mappings, sequences, and "
        "JSON-compatible scalar values"
    )


def _thaw_config_value(
    value: FrozenConfigValue,
):
    if (
        isinstance(value, tuple)
        and len(value) == 2
        and value[0] == "__mapping__"
    ):
        return {
            name: _thaw_config_value(item)
            for name, item in value[1]
        }

    if (
        isinstance(value, tuple)
        and len(value) == 2
        and value[0] == "__sequence__"
    ):
        return [
            _thaw_config_value(item)
            for item in value[1]
        ]

    return value
