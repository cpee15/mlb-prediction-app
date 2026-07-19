"""Versioned baseline batted-ball context contracts and sampling."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import random
from typing import Mapping, Optional, Sequence, Tuple, TypeVar


BASELINE_BATTED_BALL_MODEL_VERSION = (
    "baseline_batted_ball_context_v1"
)


class BattedBallType(str, Enum):
    """Canonical batted-ball classifications."""

    GROUND_BALL = "ground_ball"
    LINE_DRIVE = "line_drive"
    FLY_BALL = "fly_ball"
    POPUP = "popup"


class SprayDirection(str, Enum):
    """Coarse horizontal field direction."""

    PULL = "pull"
    CENTER = "center"
    OPPOSITE = "opposite"


class BattedBallDepth(str, Enum):
    """Coarse batted-ball depth."""

    SHALLOW = "shallow"
    MEDIUM = "medium"
    DEEP = "deep"


class ContactQuality(str, Enum):
    """Coarse quality-of-contact classification."""

    SOFT = "soft"
    MEDIUM = "medium"
    HARD = "hard"


@dataclass(frozen=True)
class BattedBallContext:
    """Context attached to a ball put into play.

    These fields describe the simulated contact. They do not decide
    runner advancement or scoring outcomes; those belong to later
    transition layers.
    """

    batted_ball_type: BattedBallType
    direction: SprayDirection
    depth: BattedBallDepth
    contact_quality: ContactQuality
    model_version: str = BASELINE_BATTED_BALL_MODEL_VERSION

    def __post_init__(self) -> None:
        if not self.model_version:
            raise ValueError("model_version is required")


WeightedValue = Tuple[object, float]
T = TypeVar("T")


def _validate_distribution(
    name: str,
    distribution: Sequence[Tuple[T, float]],
) -> None:
    if not distribution:
        raise ValueError(f"{name} distribution cannot be empty")

    seen = set()
    total = 0.0

    for value, probability in distribution:
        if value in seen:
            raise ValueError(
                f"{name} distribution contains duplicate value: {value}"
            )
        seen.add(value)

        if not isinstance(probability, (int, float)):
            raise TypeError(
                f"{name} probability must be numeric: {value}"
            )

        probability = float(probability)

        if probability < 0.0:
            raise ValueError(
                f"{name} probability cannot be negative: {value}"
            )

        total += probability

    if abs(total - 1.0) > 1e-9:
        raise ValueError(
            f"{name} probabilities must sum to 1.0; received {total}"
        )


def _sample_weighted(
    rng: random.Random,
    distribution: Sequence[Tuple[T, float]],
) -> T:
    roll = rng.random()
    cumulative = 0.0

    for value, probability in distribution:
        cumulative += probability
        if roll < cumulative:
            return value

    # Floating-point protection after validated unit-sum distributions.
    return distribution[-1][0]


# These are explicit initial simulation assumptions. They are versioned
# so future historical calibration can replace them without changing
# the BattedBallContext contract.
DEFAULT_TYPE_DISTRIBUTION = (
    (BattedBallType.GROUND_BALL, 0.43),
    (BattedBallType.LINE_DRIVE, 0.21),
    (BattedBallType.FLY_BALL, 0.27),
    (BattedBallType.POPUP, 0.09),
)

LINEOUT_POPOUT_TYPE_DISTRIBUTION = (
    (BattedBallType.LINE_DRIVE, 0.70),
    (BattedBallType.POPUP, 0.30),
)

HOME_RUN_TYPE_DISTRIBUTION = (
    (BattedBallType.LINE_DRIVE, 0.20),
    (BattedBallType.FLY_BALL, 0.80),
)

DIRECTION_DISTRIBUTION = (
    (SprayDirection.PULL, 0.40),
    (SprayDirection.CENTER, 0.35),
    (SprayDirection.OPPOSITE, 0.25),
)

DEPTH_DISTRIBUTIONS: Mapping[
    BattedBallType,
    Sequence[Tuple[BattedBallDepth, float]],
] = {
    BattedBallType.GROUND_BALL: (
        (BattedBallDepth.SHALLOW, 0.80),
        (BattedBallDepth.MEDIUM, 0.20),
        (BattedBallDepth.DEEP, 0.00),
    ),
    BattedBallType.LINE_DRIVE: (
        (BattedBallDepth.SHALLOW, 0.20),
        (BattedBallDepth.MEDIUM, 0.55),
        (BattedBallDepth.DEEP, 0.25),
    ),
    BattedBallType.FLY_BALL: (
        (BattedBallDepth.SHALLOW, 0.10),
        (BattedBallDepth.MEDIUM, 0.35),
        (BattedBallDepth.DEEP, 0.55),
    ),
    BattedBallType.POPUP: (
        (BattedBallDepth.SHALLOW, 0.55),
        (BattedBallDepth.MEDIUM, 0.40),
        (BattedBallDepth.DEEP, 0.05),
    ),
}

DEFAULT_CONTACT_QUALITY_DISTRIBUTION = (
    (ContactQuality.SOFT, 0.30),
    (ContactQuality.MEDIUM, 0.50),
    (ContactQuality.HARD, 0.20),
)

CONTACT_QUALITY_BY_OUTCOME: Mapping[
    str,
    Sequence[Tuple[ContactQuality, float]],
] = {
    "single": (
        (ContactQuality.SOFT, 0.15),
        (ContactQuality.MEDIUM, 0.60),
        (ContactQuality.HARD, 0.25),
    ),
    "double": (
        (ContactQuality.SOFT, 0.05),
        (ContactQuality.MEDIUM, 0.45),
        (ContactQuality.HARD, 0.50),
    ),
    "triple": (
        (ContactQuality.SOFT, 0.05),
        (ContactQuality.MEDIUM, 0.40),
        (ContactQuality.HARD, 0.55),
    ),
    "hr": (
        (ContactQuality.SOFT, 0.01),
        (ContactQuality.MEDIUM, 0.14),
        (ContactQuality.HARD, 0.85),
    ),
    "reached_on_error": (
        (ContactQuality.SOFT, 0.30),
        (ContactQuality.MEDIUM, 0.50),
        (ContactQuality.HARD, 0.20),
    ),
    "groundout": (
        (ContactQuality.SOFT, 0.40),
        (ContactQuality.MEDIUM, 0.45),
        (ContactQuality.HARD, 0.15),
    ),
    "flyout": (
        (ContactQuality.SOFT, 0.25),
        (ContactQuality.MEDIUM, 0.55),
        (ContactQuality.HARD, 0.20),
    ),
    "lineout_popout": (
        (ContactQuality.SOFT, 0.25),
        (ContactQuality.MEDIUM, 0.50),
        (ContactQuality.HARD, 0.25),
    ),
    "other_out": (
        (ContactQuality.SOFT, 0.35),
        (ContactQuality.MEDIUM, 0.50),
        (ContactQuality.HARD, 0.15),
    ),
}

NON_BATTED_BALL_OUTCOMES = frozenset(
    {
        "bb",
        "hbp",
        "k",
        "strikeout",
    }
)

VALID_PRIMARY_OUTCOMES = frozenset(
    {
        "single",
        "double",
        "triple",
        "hr",
        "reached_on_error",
        "out",
    }
)

VALID_OUTCOME_SUBTYPES = frozenset(
    {
        "groundout",
        "flyout",
        "lineout_popout",
        "other_out",
    }
)


def validate_baseline_batted_ball_distributions() -> None:
    """Validate every versioned baseline probability distribution."""

    _validate_distribution(
        "default batted-ball type",
        DEFAULT_TYPE_DISTRIBUTION,
    )
    _validate_distribution(
        "lineout/popout batted-ball type",
        LINEOUT_POPOUT_TYPE_DISTRIBUTION,
    )
    _validate_distribution(
        "home-run batted-ball type",
        HOME_RUN_TYPE_DISTRIBUTION,
    )
    _validate_distribution(
        "spray direction",
        DIRECTION_DISTRIBUTION,
    )
    _validate_distribution(
        "default contact quality",
        DEFAULT_CONTACT_QUALITY_DISTRIBUTION,
    )

    for batted_ball_type, distribution in DEPTH_DISTRIBUTIONS.items():
        _validate_distribution(
            f"{batted_ball_type.value} depth",
            distribution,
        )

    for outcome, distribution in CONTACT_QUALITY_BY_OUTCOME.items():
        _validate_distribution(
            f"{outcome} contact quality",
            distribution,
        )


class BaselineBattedBallContextProvider:
    """Sample baseline context for a primary plate-appearance result.

    The provider consumes the existing primary outcome and optional
    out subtype. It does not modify outcome probabilities, runner
    advancement, outs, scores, or box-score accounting.
    """

    model_version = BASELINE_BATTED_BALL_MODEL_VERSION

    def __init__(
        self,
        *,
        rng: Optional[random.Random] = None,
    ) -> None:
        validate_baseline_batted_ball_distributions()
        self._rng = rng or random.Random()

    def sample(
        self,
        *,
        primary_outcome: str,
        outcome_subtype: Optional[str] = None,
    ) -> Optional[BattedBallContext]:
        normalized_outcome = primary_outcome.strip().lower()
        normalized_subtype = (
            outcome_subtype.strip().lower()
            if outcome_subtype is not None
            else None
        )

        if normalized_outcome in NON_BATTED_BALL_OUTCOMES:
            if normalized_subtype is not None:
                raise ValueError(
                    "non-batted-ball outcomes cannot have "
                    "an outcome_subtype"
                )
            return None

        if normalized_outcome not in VALID_PRIMARY_OUTCOMES:
            raise ValueError(
                f"unsupported primary_outcome: {primary_outcome}"
            )

        if (
            normalized_subtype is not None
            and normalized_subtype not in VALID_OUTCOME_SUBTYPES
        ):
            raise ValueError(
                f"unsupported outcome_subtype: {outcome_subtype}"
            )

        if (
            normalized_subtype is not None
            and normalized_outcome != "out"
        ):
            raise ValueError(
                "outcome_subtype is only valid when "
                "primary_outcome is 'out'"
            )

        batted_ball_type = self._sample_type(
            primary_outcome=normalized_outcome,
            outcome_subtype=normalized_subtype,
        )

        contact_key = normalized_subtype or normalized_outcome
        contact_distribution = CONTACT_QUALITY_BY_OUTCOME.get(
            contact_key,
            DEFAULT_CONTACT_QUALITY_DISTRIBUTION,
        )

        return BattedBallContext(
            batted_ball_type=batted_ball_type,
            direction=_sample_weighted(
                self._rng,
                DIRECTION_DISTRIBUTION,
            ),
            depth=_sample_weighted(
                self._rng,
                DEPTH_DISTRIBUTIONS[batted_ball_type],
            ),
            contact_quality=_sample_weighted(
                self._rng,
                contact_distribution,
            ),
            model_version=self.model_version,
        )

    def _sample_type(
        self,
        *,
        primary_outcome: str,
        outcome_subtype: Optional[str],
    ) -> BattedBallType:
        if outcome_subtype == "groundout":
            return BattedBallType.GROUND_BALL

        if outcome_subtype == "flyout":
            return BattedBallType.FLY_BALL

        if outcome_subtype == "lineout_popout":
            return _sample_weighted(
                self._rng,
                LINEOUT_POPOUT_TYPE_DISTRIBUTION,
            )

        if primary_outcome == "hr":
            return _sample_weighted(
                self._rng,
                HOME_RUN_TYPE_DISTRIBUTION,
            )

        return _sample_weighted(
            self._rng,
            DEFAULT_TYPE_DISTRIBUTION,
        )


validate_baseline_batted_ball_distributions()
