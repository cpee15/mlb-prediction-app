from dataclasses import asdict
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Set

from mlb_app.simulation.bullpen_state import (
    BullpenState,
)
from mlb_app.simulation.bullpen_state import (
    validate_bullpen_state,
)


VALID_ROLES = {
    "closer",
    "setup",
    "middle_relief",
    "long_relief",
    "low_leverage",
}


VALID_HANDEDNESS = {
    "L",
    "R",
    "S",
    "unknown",
}


def _safe_handedness(
    value: Optional[str],
) -> str:

    if not value:
        return "unknown"

    cleaned = str(value).upper()

    if cleaned in {
        "L",
        "R",
        "S",
    }:
        return cleaned

    return "unknown"


def _recursive_pitcher_extract(
    obj: Any,
    results: List[Dict[str, Any]],
):

    if isinstance(obj, dict):

        lowered_keys = {
            str(k).lower()
            for k in obj.keys()
        }

        if (
            "pitcher_id" in lowered_keys
            or "player_id" in lowered_keys
            or "id" in lowered_keys
        ):
            results.append(obj)

        for value in obj.values():
            _recursive_pitcher_extract(
                value,
                results,
            )

    elif isinstance(obj, list):

        for item in obj:
            _recursive_pitcher_extract(
                item,
                results,
            )


def _derive_pitcher_id(
    pitcher: Dict[str, Any],
    idx: int,
) -> str:

    for key in [
        "pitcher_id",
        "player_id",
        "id",
    ]:

        if pitcher.get(key):
            return str(
                pitcher.get(key)
            )

    return f"heuristic_pitcher_{idx}"


def _infer_role(
    idx: int,
) -> str:

    if idx == 0:
        return "closer"

    if idx == 1:
        return "setup"

    if idx <= 4:
        return "middle_relief"

    return "low_leverage"


def initialize_candidate_bullpen_state(
    payload: dict,
    team_key: str,
) -> Dict[str, Any]:

    extracted = []

    _recursive_pitcher_extract(
        payload,
        extracted,
    )

    probable_starter = None

    for key in [
        "probable_pitcher_id",
        "starter_id",
    ]:

        if payload.get(key):
            probable_starter = str(
                payload.get(key)
            )

    available_relievers = []

    fatigue_by_pitcher = {}

    role_by_pitcher = {}

    handedness_by_pitcher = {}

    used_ids: Set[str] = set()

    starter_removed = False

    for idx, pitcher in enumerate(
        extracted
    ):

        pitcher_id = (
            _derive_pitcher_id(
                pitcher,
                idx,
            )
        )

        if (
            probable_starter
            and pitcher_id
            == probable_starter
        ):

            starter_removed = True
            continue

        if pitcher_id in used_ids:
            continue

        used_ids.add(
            pitcher_id
        )

        available_relievers.append(
            pitcher_id
        )

        fatigue_by_pitcher[
            pitcher_id
        ] = 0.0

        role_by_pitcher[
            pitcher_id
        ] = _infer_role(idx)

        handedness_by_pitcher[
            pitcher_id
        ] = _safe_handedness(
            pitcher.get(
                "handedness"
            )
        )

    initialization_quality = (
        "empty"
    )

    if available_relievers:
        initialization_quality = (
            "partial"
        )

    bullpen_state = BullpenState(
        team_id=team_key,
        available_relievers=(
            available_relievers
        ),
        used_pitchers=[],
        current_pitcher=None,
        fatigue_by_pitcher=(
            fatigue_by_pitcher
        ),
        role_by_pitcher=(
            role_by_pitcher
        ),
        handedness_by_pitcher=(
            handedness_by_pitcher
        ),
        usage_log=[],
    )

    return {
        "state": bullpen_state,
        "initialization_quality": (
            initialization_quality
        ),
        "starter_removed": (
            starter_removed
        ),
        "reliever_count": (
            len(
                available_relievers
            )
        ),
    }


def validate_initialized_bullpen_state(
    bullpen_state: BullpenState,
) -> bool:

    if not validate_bullpen_state(
        bullpen_state
    ):
        return False

    seen = set()

    for pitcher_id in (
        bullpen_state.available_relievers
    ):

        if pitcher_id in seen:
            return False

        seen.add(
            pitcher_id
        )

        role = (
            bullpen_state.role_by_pitcher.get(
                pitcher_id
            )
        )

        if role not in VALID_ROLES:
            return False

        handedness = (
            bullpen_state.handedness_by_pitcher.get(
                pitcher_id
            )
        )

        if (
            handedness
            not in VALID_HANDEDNESS
        ):
            return False

        fatigue = (
            bullpen_state.fatigue_by_pitcher.get(
                pitcher_id
            )
        )

        if (
            fatigue is None
            or fatigue < 0
            or fatigue > 1
        ):
            return False

    return True


def bullpen_state_to_dict(
    bullpen_state: BullpenState,
) -> dict:

    return asdict(
        bullpen_state
    )
