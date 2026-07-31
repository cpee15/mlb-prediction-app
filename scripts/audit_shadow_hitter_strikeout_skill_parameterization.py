"""Audit the selected shadow hitter strikeout parameterization."""

from __future__ import annotations

import json
import math

from mlb_app.model_projection_routes import (
    _session_factory,
)
from mlb_app.simulation.shadow.hitter_strikeout_skill_parameterization import (
    ACTUAL_K_COEFFICIENT,
    BOOTSTRAP_CI_95,
    BOOTSTRAP_PROBABILITY_OF_IMPROVEMENT,
    INTERCEPT,
    MAXIMUM_ACTUAL_K_RATE,
    MAXIMUM_WHIFF_RATE,
    MINIMUM_ACTUAL_K_RATE,
    MINIMUM_WHIFF_RATE,
    WHIFF_COEFFICIENT,
    selected_hitter_strikeout_skill_parameterization,
)
from mlb_app.simulation.shadow.hitter_strikeout_skill_validation import (
    _clean,
    _fit,
    bootstrap_hitter_strikeout_skill_differences,
    evaluate_hitter_strikeout_skill_models,
)
from scripts.audit_shadow_hitter_strikeout_skill import (
    build_samples,
)


TOLERANCE = 1e-12


def main() -> None:
    factory = _session_factory()
    session = factory()

    try:
        raw_samples, coverage = build_samples(
            session
        )
    finally:
        session.close()

    samples = _clean(raw_samples)
    validation = (
        evaluate_hitter_strikeout_skill_models(
            samples
        )
    )
    bootstrap = (
        bootstrap_hitter_strikeout_skill_differences(
            samples
        )
    )

    pooled = _fit(
        samples,
        (
            "pre_actual_k_rate",
            "pre_whiff_rate",
        ),
    )

    actual_k_rates = sorted(
        row["pre_actual_k_rate"]
        for row in samples
    )
    whiff_rates = sorted(
        row["pre_whiff_rate"]
        for row in samples
    )

    blend_evidence = bootstrap[
        "comparisons"
    ][
        "blend_increment_over_best_univariate"
    ]

    checks = {
        "validation_ready":
            validation["status"] == "ready",
        "bootstrap_ready":
            bootstrap["status"] == "ready",
        "intercept_reproduced":
            math.isclose(
                pooled[0],
                INTERCEPT,
                rel_tol=0.0,
                abs_tol=TOLERANCE,
            ),
        "actual_k_coefficient_reproduced":
            math.isclose(
                pooled[1],
                ACTUAL_K_COEFFICIENT,
                rel_tol=0.0,
                abs_tol=TOLERANCE,
            ),
        "whiff_coefficient_reproduced":
            math.isclose(
                pooled[2],
                WHIFF_COEFFICIENT,
                rel_tol=0.0,
                abs_tol=TOLERANCE,
            ),
        "minimum_actual_k_reproduced":
            math.isclose(
                actual_k_rates[0],
                MINIMUM_ACTUAL_K_RATE,
                rel_tol=0.0,
                abs_tol=TOLERANCE,
            ),
        "maximum_actual_k_reproduced":
            math.isclose(
                actual_k_rates[-1],
                MAXIMUM_ACTUAL_K_RATE,
                rel_tol=0.0,
                abs_tol=TOLERANCE,
            ),
        "minimum_whiff_reproduced":
            math.isclose(
                whiff_rates[0],
                MINIMUM_WHIFF_RATE,
                rel_tol=0.0,
                abs_tol=TOLERANCE,
            ),
        "maximum_whiff_reproduced":
            math.isclose(
                whiff_rates[-1],
                MAXIMUM_WHIFF_RATE,
                rel_tol=0.0,
                abs_tol=TOLERANCE,
            ),
        "bootstrap_probability_reproduced":
            math.isclose(
                blend_evidence[
                    "probability_of_improvement"
                ],
                BOOTSTRAP_PROBABILITY_OF_IMPROVEMENT,
                rel_tol=0.0,
                abs_tol=TOLERANCE,
            ),
        "bootstrap_lower_reproduced":
            math.isclose(
                blend_evidence[
                    "mse_improvement_ci_95"
                ]["lower"],
                BOOTSTRAP_CI_95[0],
                rel_tol=0.0,
                abs_tol=TOLERANCE,
            ),
        "bootstrap_upper_reproduced":
            math.isclose(
                blend_evidence[
                    "mse_improvement_ci_95"
                ]["upper"],
                BOOTSTRAP_CI_95[1],
                rel_tol=0.0,
                abs_tol=TOLERANCE,
            ),
    }

    blockers = sorted(
        name
        for name, passed in checks.items()
        if not passed
    )

    print(
        json.dumps(
            {
                "schema_version":
                    "shadow_hitter_strikeout_skill_parameterization_audit_v1",
                "status":
                    "ready"
                    if not blockers
                    else "blocked",
                "shadow_only": True,
                "parameter_selected":
                    not blockers,
                "production_authority_changed":
                    False,
                "selected_parameterization":
                    selected_hitter_strikeout_skill_parameterization(),
                "recomputed_mapping": {
                    "intercept": pooled[0],
                    "actual_k_coefficient":
                        pooled[1],
                    "whiff_coefficient":
                        pooled[2],
                    "minimum_actual_k_rate":
                        actual_k_rates[0],
                    "maximum_actual_k_rate":
                        actual_k_rates[-1],
                    "minimum_whiff_rate":
                        whiff_rates[0],
                    "maximum_whiff_rate":
                        whiff_rates[-1],
                },
                "sample_count": len(samples),
                "holdout_pa": sum(
                    row["holdout_pa"]
                    for row in samples
                ),
                "seasons":
                    validation["seasons"],
                "window_count":
                    len(coverage),
                "checks": checks,
                "blockers": blockers,
                "activation": {
                    "activation_eligible":
                        not blockers,
                    "feature_flag_required":
                        True,
                    "shadow_canary_required":
                        True,
                    "production_enabled":
                        False,
                },
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
