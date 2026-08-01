"""Feature-flagged hitter-profile shadow canary."""

from __future__ import annotations

import copy
from collections.abc import Mapping
from typing import Any

from mlb_app.simulation.pa_outcome_model import (
    build_pa_outcome_probabilities,
)
from mlb_app.simulation.shadow.hitter_hit_type_allocation_parameterization import (
    resolve_hitter_hit_type_allocation,
)
from mlb_app.simulation.shadow.hitter_power_skill_parameterization import (
    resolve_hitter_iso,
)
from mlb_app.simulation.shadow.hitter_profile_activation_readiness import (
    synthesize_hitter_profile_activation_readiness,
)
from mlb_app.simulation.shadow.hitter_strikeout_skill_parameterization import (
    resolve_hitter_strikeout_rate,
)
from mlb_app.simulation.shadow.hitter_walk_skill_parameterization import (
    resolve_hitter_walk_rate,
)


CANARY_SCHEMA_VERSION = (
    "hitter_profile_shadow_canary_v1"
)
HIT_OUTCOME_KEYS = (
    "single",
    "double",
    "triple",
    "hr",
)


def _nested(
    profile: Mapping[str, Any],
    section: str,
    key: str,
) -> Any:
    return (
        profile.get(section) or {}
    ).get(key)


def _disabled_result() -> dict[str, Any]:
    return {
        "schema_version":
            CANARY_SCHEMA_VERSION,
        "status": "disabled",
        "executed": False,
        "feature_flag_enabled": False,
        "shadow_only": True,
        "parameter_selected": True,
        "production_authority_changed": False,
        "production_inputs_unchanged": True,
        "candidate_profile": None,
        "candidate_probabilities": None,
        "production_probabilities": None,
        "probability_deltas": None,
        "fallback_telemetry": None,
        "blockers": [],
    }


def run_hitter_profile_shadow_canary(
    *,
    enabled: bool = False,
    production_batter_profile: Mapping[
        str,
        Any,
    ],
    pitcher_profile: Mapping[
        str,
        Any,
    ]
    | None = None,
    environment_profile: Mapping[
        str,
        Any,
    ]
    | None = None,
    candidate_signals: Mapping[
        str,
        Any,
    ]
    | None = None,
    readiness: Mapping[
        str,
        Any,
    ]
    | None = None,
) -> dict[str, Any]:
    """Run a paired PA comparison without changing authority."""

    if enabled is not True:
        return _disabled_result()

    production_original = copy.deepcopy(
        dict(production_batter_profile)
    )
    production_input = copy.deepcopy(
        production_original
    )
    signal_payload = dict(
        candidate_signals or {}
    )
    signal_envelope = isinstance(
        signal_payload.get("signals"),
        Mapping,
    )
    signals = (
        dict(signal_payload["signals"])
        if signal_envelope
        else signal_payload
    )
    readiness_payload = dict(
        readiness
        or synthesize_hitter_profile_activation_readiness()
    )

    blockers = []
    if signal_envelope:
        if (
            signal_payload.get("status")
            != "ready"
        ):
            blockers.append(
                "candidate_signals_not_ready"
            )
        if (
            signal_payload.get("cutoff_safe")
            is not True
        ):
            blockers.append(
                "candidate_signals_not_cutoff_safe"
            )

    if (
        readiness_payload.get("status")
        != "ready_for_activation"
        or readiness_payload.get(
            "first_activation_ready"
        )
        is not True
        or readiness_payload.get(
            "parameter_selected"
        )
        is not True
    ):
        blockers.append(
            "hitter_profile_activation_not_ready"
        )

    pitcher = dict(
        pitcher_profile or {}
    )
    environment = dict(
        environment_profile or {}
    )
    production_model = (
        build_pa_outcome_probabilities(
            production_input,
            pitcher,
            environment,
        )
    )
    production_probabilities = dict(
        production_model["probabilities"]
    )
    production_hit_mass = sum(
        production_probabilities[key]
        for key in HIT_OUTCOME_KEYS
    )
    production_hit_allocation = (
        {
            "single":
                production_probabilities[
                    "single"
                ]
                / production_hit_mass,
            "double":
                production_probabilities[
                    "double"
                ]
                / production_hit_mass,
            "triple":
                production_probabilities[
                    "triple"
                ]
                / production_hit_mass,
            "home_run":
                production_probabilities[
                    "hr"
                ]
                / production_hit_mass,
        }
        if production_hit_mass > 0.0
        else None
    )

    strikeout = resolve_hitter_strikeout_rate(
        actual_k_rate=_nested(
            production_input,
            "contact_skill",
            "k_rate",
        ),
        whiff_rate=signals.get(
            "whiff_rate"
        ),
    )
    walk = resolve_hitter_walk_rate(
        called_ball_rate=signals.get(
            "called_ball_rate"
        ),
        actual_walk_rate=_nested(
            production_input,
            "plate_discipline",
            "bb_rate",
        ),
    )
    power = resolve_hitter_iso(
        expected_damage_per_ab=signals.get(
            "expected_damage_per_ab"
        ),
        actual_iso=_nested(
            production_input,
            "power",
            "iso",
        ),
    )
    allocation = (
        resolve_hitter_hit_type_allocation(
            expected_damage_per_bbe=signals.get(
                "expected_damage_per_bbe"
            ),
            conservative_triple_probability=(
                signals.get(
                    "conservative_triple_probability",
                    (
                        production_hit_allocation[
                            "triple"
                        ]
                        if production_hit_allocation
                        is not None
                        else None
                    ),
                )
            ),
            actual_allocation=(
                signals.get(
                    "actual_allocation"
                )
                or production_hit_allocation
            ),
        )
    )

    parameter_results = {
        "strikeout_skill": strikeout,
        "walk_skill": walk,
        "power_skill": power,
        "hit_type_allocation": allocation,
    }

    for name, result in (
        parameter_results.items()
    ):
        if result.get("status") != "ready":
            blockers.append(
                f"{name}_not_ready"
            )

    if blockers:
        return {
            "schema_version":
                CANARY_SCHEMA_VERSION,
            "status": "blocked",
            "executed": False,
            "feature_flag_enabled": True,
            "shadow_only": True,
            "parameter_selected": True,
            "production_authority_changed":
                False,
            "production_inputs_unchanged":
                (
                    dict(production_batter_profile)
                    == production_original
                ),
            "candidate_profile": None,
            "candidate_probabilities": None,
            "production_probabilities": None,
            "probability_deltas": None,
            "parameter_results":
                parameter_results,
            "fallback_telemetry": None,
            "blockers": sorted(
                set(blockers)
            ),
        }

    candidate_profile = copy.deepcopy(
        production_input
    )
    candidate_profile.setdefault(
        "contact_skill",
        {},
    )["k_rate"] = strikeout[
        "strikeout_rate"
    ]
    candidate_profile.setdefault(
        "plate_discipline",
        {},
    )["bb_rate"] = walk[
        "walk_rate"
    ]
    candidate_profile.setdefault(
        "power",
        {},
    )["iso"] = power["iso"]
    candidate_profile.setdefault(
        "metadata",
        {},
    ).update({
        "source_type":
            "selected_hitter_profile_shadow_canary",
        "canary_schema_version":
            CANARY_SCHEMA_VERSION,
        "shadow_only": True,
        "feature_flag_enabled": True,
        "production_authority_changed":
            False,
        "hit_skill_policy":
            "retain_current_policy",
    })

    candidate_model = (
        build_pa_outcome_probabilities(
            candidate_profile,
            pitcher,
            environment,
        )
    )
    candidate_probabilities = dict(
        candidate_model["probabilities"]
    )

    candidate_hit_mass = sum(
        candidate_probabilities[key]
        for key in HIT_OUTCOME_KEYS
    )
    selected_allocation = allocation[
        "allocation"
    ]

    candidate_probabilities.update({
        "single":
            candidate_hit_mass
            * selected_allocation[
                "single"
            ],
        "double":
            candidate_hit_mass
            * selected_allocation[
                "double"
            ],
        "triple":
            candidate_hit_mass
            * selected_allocation[
                "triple"
            ],
        "hr":
            candidate_hit_mass
            * selected_allocation[
                "home_run"
            ],
    })

    probability_keys = sorted(
        set(production_probabilities)
        | set(candidate_probabilities)
    )
    deltas = {
        key: (
            candidate_probabilities.get(
                key,
                0.0,
            )
            - production_probabilities.get(
                key,
                0.0,
            )
        )
        for key in probability_keys
    }

    fallback_by_signal = {
        name: {
            "fallback_used":
                result["fallback_used"],
            "fallback_reason":
                result["fallback_reason"],
            "source":
                result["source"],
        }
        for name, result
        in parameter_results.items()
    }
    fallback_count = sum(
        item["fallback_used"]
        for item
        in fallback_by_signal.values()
    )

    return {
        "schema_version":
            CANARY_SCHEMA_VERSION,
        "status": "ready",
        "executed": True,
        "feature_flag_enabled": True,
        "shadow_only": True,
        "parameter_selected": True,
        "production_authority_changed": False,
        "production_inputs_unchanged": (
            dict(production_batter_profile)
            == production_original
        ),
        "production_model_version":
            production_model[
                "model_version"
            ],
        "candidate_model_version":
            candidate_model[
                "model_version"
            ],
        "production_profile":
            production_input,
        "candidate_profile":
            candidate_profile,
        "production_probabilities":
            production_probabilities,
        "candidate_probabilities":
            candidate_probabilities,
        "probability_deltas": deltas,
        "maximum_absolute_probability_delta":
            max(
                abs(value)
                for value in deltas.values()
            ),
        "candidate_probability_sum":
            sum(
                candidate_probabilities.values()
            ),
        "candidate_hit_probability":
            candidate_hit_mass,
        "selected_hit_type_allocation":
            selected_allocation,
        "parameter_results":
            parameter_results,
        "fallback_telemetry": {
            "signal_count":
                len(parameter_results),
            "fallback_count":
                fallback_count,
            "fallback_rate":
                fallback_count
                / len(parameter_results),
            "by_signal":
                fallback_by_signal,
        },
        "readiness_status":
            readiness_payload.get(
                "status"
            ),
        "blockers": [],
    }
