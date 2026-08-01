"""Audit the selected shadow hitter power parameterization."""

from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import math
from pathlib import Path

from mlb_app.simulation.shadow.hitter_power_incremental_validation import (
    _clean_samples,
    _fit,
)
from mlb_app.simulation.shadow.hitter_power_skill_parameterization import (
    BOOTSTRAP_CI_95,
    BOOTSTRAP_PROBABILITY_OF_IMPROVEMENT,
    EXPECTED_DAMAGE_COEFFICIENT,
    INTERCEPT,
    MAXIMUM_EXPECTED_DAMAGE_PER_AB,
    MINIMUM_EXPECTED_DAMAGE_PER_AB,
    selected_hitter_power_skill_parameterization,
)


TOLERANCE = 1e-12


def _run_source_audit() -> tuple[
    dict,
    list[dict],
]:
    source_path = (
        Path(__file__).resolve().parent
        / "audit_shadow_hitter_power_incremental_value.py"
    )
    spec = importlib.util.spec_from_file_location(
        "power_incremental_audit",
        source_path,
    )
    audit = importlib.util.module_from_spec(
        spec
    )
    spec.loader.exec_module(audit)

    captured_samples = []
    original_evaluate = (
        audit.evaluate_hitter_power_incremental_models
    )

    def capture_evaluation(
        samples,
        *args,
        **kwargs,
    ):
        captured_samples.extend(
            dict(row)
            for row in samples
        )
        return original_evaluate(
            samples,
            *args,
            **kwargs,
        )

    audit.evaluate_hitter_power_incremental_models = (
        capture_evaluation
    )

    stdout = io.StringIO()
    with contextlib.redirect_stdout(stdout):
        audit.main()

    return (
        json.loads(stdout.getvalue()),
        captured_samples,
    )


def main() -> None:
    source_audit, raw_samples = (
        _run_source_audit()
    )
    samples = _clean_samples(raw_samples)

    pooled = _fit(
        samples,
        (
            "pre_expected_damage_per_ab",
        ),
    )
    expected_damage_values = sorted(
        row["pre_expected_damage_per_ab"]
        for row in samples
    )

    bootstrap = source_audit[
        "clustered_bootstrap"
    ]
    expected_evidence = bootstrap[
        "comparisons"
    ][
        "expected_damage_minus_actual_iso"
    ]

    checks = {
        "validation_ready":
            source_audit["status"] == "ready",
        "bootstrap_ready":
            bootstrap["status"] == "ready",
        "intercept_reproduced":
            math.isclose(
                pooled[0],
                INTERCEPT,
                rel_tol=0.0,
                abs_tol=TOLERANCE,
            ),
        "expected_damage_coefficient_reproduced":
            math.isclose(
                pooled[1],
                EXPECTED_DAMAGE_COEFFICIENT,
                rel_tol=0.0,
                abs_tol=TOLERANCE,
            ),
        "minimum_input_reproduced":
            math.isclose(
                expected_damage_values[0],
                MINIMUM_EXPECTED_DAMAGE_PER_AB,
                rel_tol=0.0,
                abs_tol=TOLERANCE,
            ),
        "maximum_input_reproduced":
            math.isclose(
                expected_damage_values[-1],
                MAXIMUM_EXPECTED_DAMAGE_PER_AB,
                rel_tol=0.0,
                abs_tol=TOLERANCE,
            ),
        "bootstrap_probability_reproduced":
            math.isclose(
                expected_evidence[
                    "probability_of_improvement"
                ],
                BOOTSTRAP_PROBABILITY_OF_IMPROVEMENT,
                rel_tol=0.0,
                abs_tol=TOLERANCE,
            ),
        "bootstrap_lower_reproduced":
            math.isclose(
                expected_evidence[
                    "mse_improvement_ci_95"
                ]["lower"],
                BOOTSTRAP_CI_95[0],
                rel_tol=0.0,
                abs_tol=TOLERANCE,
            ),
        "bootstrap_upper_reproduced":
            math.isclose(
                expected_evidence[
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
                    "shadow_hitter_power_skill_parameterization_audit_v1",
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
                    selected_hitter_power_skill_parameterization(),
                "recomputed_mapping": {
                    "intercept": pooled[0],
                    "expected_damage_coefficient":
                        pooled[1],
                    "minimum_expected_damage_per_ab":
                        expected_damage_values[0],
                    "maximum_expected_damage_per_ab":
                        expected_damage_values[-1],
                },
                "sample_count": len(samples),
                "holdout_ab": sum(
                    row["holdout_ab"]
                    for row in samples
                ),
                "seasons":
                    source_audit["seasons"],
                "window_count":
                    len(
                        source_audit[
                            "window_coverage"
                        ]
                    ),
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
