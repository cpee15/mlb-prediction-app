"""Audit the selected shadow hitter hit-type allocation."""

from __future__ import annotations

import json
import math

from mlb_app.model_projection_routes import (
    _session_factory,
)
from mlb_app.simulation.shadow.hitter_hit_type_allocation_parameterization import (
    EXPECTED_BOOTSTRAP_CI_95,
    EXPECTED_BOOTSTRAP_PROBABILITY,
    EXPECTED_DAMAGE_COEFFICIENTS,
    MAXIMUM_EXPECTED_DAMAGE_PER_BBE,
    MINIMUM_EXPECTED_DAMAGE_PER_BBE,
    selected_hitter_hit_type_allocation_parameterization,
)
from mlb_app.simulation.shadow.hitter_hit_type_allocation_validation import (
    MODEL_FEATURES,
    _clean_samples,
    _fit_model,
    bootstrap_hitter_hit_type_allocation_differences,
    evaluate_hitter_hit_type_allocation_models,
)
from scripts.audit_shadow_hitter_hit_type_allocation import (
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

    samples = _clean_samples(raw_samples)
    validation = (
        evaluate_hitter_hit_type_allocation_models(
            samples
        )
    )
    bootstrap = (
        bootstrap_hitter_hit_type_allocation_differences(
            samples
        )
    )

    pooled = _fit_model(
        samples,
        MODEL_FEATURES["expected_damage"],
    )
    expected_damage_values = sorted(
        row["pre_expected_damage_per_bbe"]
        for row in samples
    )
    expected_evidence = bootstrap[
        "comparisons"
    ]["expected_minus_league_prior"]

    checks = {
        "validation_ready":
            validation["status"] == "ready",
        "bootstrap_ready":
            bootstrap["status"] == "ready",
        "minimum_input_reproduced":
            math.isclose(
                expected_damage_values[0],
                MINIMUM_EXPECTED_DAMAGE_PER_BBE,
                rel_tol=0.0,
                abs_tol=TOLERANCE,
            ),
        "maximum_input_reproduced":
            math.isclose(
                expected_damage_values[-1],
                MAXIMUM_EXPECTED_DAMAGE_PER_BBE,
                rel_tol=0.0,
                abs_tol=TOLERANCE,
            ),
        "bootstrap_probability_reproduced":
            math.isclose(
                expected_evidence[
                    "probability_of_improvement"
                ],
                EXPECTED_BOOTSTRAP_PROBABILITY,
                rel_tol=0.0,
                abs_tol=TOLERANCE,
            ),
        "bootstrap_lower_reproduced":
            math.isclose(
                expected_evidence[
                    "log_loss_improvement_ci_95"
                ]["lower"],
                EXPECTED_BOOTSTRAP_CI_95[0],
                rel_tol=0.0,
                abs_tol=TOLERANCE,
            ),
        "bootstrap_upper_reproduced":
            math.isclose(
                expected_evidence[
                    "log_loss_improvement_ci_95"
                ]["upper"],
                EXPECTED_BOOTSTRAP_CI_95[1],
                rel_tol=0.0,
                abs_tol=TOLERANCE,
            ),
    }

    for hit_type, selected in (
        EXPECTED_DAMAGE_COEFFICIENTS.items()
    ):
        recomputed = pooled[hit_type]
        checks[
            f"{hit_type}_intercept_reproduced"
        ] = math.isclose(
            recomputed[0],
            selected[0],
            rel_tol=0.0,
            abs_tol=TOLERANCE,
        )
        checks[
            f"{hit_type}_coefficient_reproduced"
        ] = math.isclose(
            recomputed[1],
            selected[1],
            rel_tol=0.0,
            abs_tol=TOLERANCE,
        )

    blockers = sorted(
        name
        for name, passed in checks.items()
        if not passed
    )

    print(
        json.dumps(
            {
                "schema_version":
                    "shadow_hitter_hit_type_allocation_parameterization_audit_v1",
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
                    selected_hitter_hit_type_allocation_parameterization(),
                "recomputed_mapping": {
                    hit_type: {
                        "intercept":
                            coefficients[0],
                        "expected_damage_coefficient":
                            coefficients[1],
                    }
                    for hit_type, coefficients
                    in pooled.items()
                    if hit_type
                    in EXPECTED_DAMAGE_COEFFICIENTS
                },
                "supported_input_range": {
                    "minimum_expected_damage_per_bbe":
                        expected_damage_values[0],
                    "maximum_expected_damage_per_bbe":
                        expected_damage_values[-1],
                },
                "sample_count": len(samples),
                "holdout_hits": sum(
                    row["holdout_hits"]
                    for row in samples
                ),
                "seasons":
                    validation["seasons"],
                "window_count":
                    len(coverage),
                "checks": checks,
                "blockers": blockers,
                "triple_policy": {
                    "selected_model_controls_triples":
                        False,
                    "policy":
                        "retain_current_conservative_triple_probability",
                },
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
