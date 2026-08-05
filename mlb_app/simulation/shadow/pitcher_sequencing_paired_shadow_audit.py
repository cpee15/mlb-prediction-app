"""Paired audit for canonical pitcher sequencing plans."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from mlb_app.simulation.shadow.hitter_profile_paired_simulation_shadow_audit import (
    compare_hitter_profile_simulation_shadow_payloads,
)


SCHEMA_VERSION = (
    "pitcher_sequencing_paired_shadow_audit_v1"
)

STABLE_INPUT_DIAGNOSTIC_KEYS = (
    "provider_identity",
    "exact_artifact_digest",
    "fallback_catalog_digest",
    "baserunning_evidence_catalog_digest",
    "canonical_model_version",
)

SEQUENCE_BLOCKER_NAMES = frozenset({
    "planned_starter_not_first",
    "planned_starter_used_in_relief",
    "preferred_follower_skipped",
    "pitcher_reentry",
    "pitcher_outside_plan",
})


def _mapping(value: Any) -> dict[str, Any]:
    return (
        dict(value)
        if isinstance(value, Mapping)
        else {}
    )


def _execution_payload(
    execution: Any,
) -> dict[str, Any] | None:
    material = getattr(
        execution,
        "material",
        None,
    )
    payload = getattr(
        material,
        "canonical_payload",
        None,
    )

    return (
        dict(payload)
        if isinstance(payload, Mapping)
        else None
    )


def _execution_diagnostics(
    execution: Any,
) -> dict[str, Any]:
    method = getattr(
        execution,
        "to_diagnostics",
        None,
    )

    return (
        dict(method())
        if callable(method)
        else {}
    )


def _sequence_audit(
    diagnostics: Mapping[str, Any],
) -> dict[str, Any]:
    return _mapping(
        diagnostics.get(
            "pitcher_appearance_sequence_audit"
        )
    )


def _role_mean_innings(
    sequence_audit: Mapping[str, Any],
    role: str,
) -> float | None:
    role_summaries = _mapping(
        sequence_audit.get("role_summaries")
    )
    role_summary = _mapping(
        role_summaries.get(role)
    )
    innings_summary = _mapping(
        role_summary.get("innings_equivalent")
    )

    try:
        value = float(
            innings_summary.get("mean")
        )
    except (TypeError, ValueError):
        return None

    return value


def _opener_bulk_workload_inverted(
    sequence_audit: Mapping[str, Any],
) -> bool:
    """
    Detect role inversion without selecting a fixed threshold.

    An opener must have a shorter average workload than its bulk
    follower. This relational invariant avoids hard-coding an innings
    limit while still detecting traditional-starter treatment of an
    opener.
    """

    opener_mean = _role_mean_innings(
        sequence_audit,
        "opener",
    )
    bulk_mean = _role_mean_innings(
        sequence_audit,
        "bulk_follower",
    )

    return (
        opener_mean is not None
        and bulk_mean is not None
        and opener_mean >= bulk_mean
    )


def _sequence_blockers(
    sequence_audit: Mapping[str, Any],
) -> tuple[str, ...]:
    anomaly_counts = _mapping(
        sequence_audit.get("anomaly_counts")
    )
    blockers = set()

    for raw_name, raw_count in (
        anomaly_counts.items()
    ):
        try:
            count = int(raw_count)
        except (TypeError, ValueError):
            continue

        if count <= 0:
            continue

        name = str(raw_name)
        normalized_name = name.rsplit(
            ":",
            1,
        )[-1]

        if normalized_name in (
            SEQUENCE_BLOCKER_NAMES
        ):
            blockers.add(normalized_name)

    if (
        sequence_audit.get(
            "starter_relief_detected"
        )
        is True
    ):
        blockers.add(
            "planned_starter_used_in_relief"
        )

    if _opener_bulk_workload_inverted(
        sequence_audit
    ):
        blockers.add(
            "opener_bulk_workload_order_invalid"
        )

    return tuple(sorted(blockers))


def _stable_inputs_match(
    baseline: Mapping[str, Any],
    candidate: Mapping[str, Any],
) -> bool:
    return all(
        baseline.get(key) == candidate.get(key)
        for key in STABLE_INPUT_DIAGNOSTIC_KEYS
    )


def _sequence_summary(
    sequence_audit: Mapping[str, Any],
) -> dict[str, Any]:
    role_summaries = _mapping(
        sequence_audit.get("role_summaries")
    )

    return {
        "status": sequence_audit.get("status"),
        "audited": sequence_audit.get(
            "audited"
        ),
        "trial_count": sequence_audit.get(
            "trial_count"
        ),
        "appearance_count":
            sequence_audit.get(
                "appearance_count"
            ),
        "affected_trial_count":
            sequence_audit.get(
                "affected_trial_count"
            ),
        "affected_trial_rate":
            sequence_audit.get(
                "affected_trial_rate"
            ),
        "starter_relief_appearance_count":
            sequence_audit.get(
                "starter_relief_appearance_count"
            ),
        "starter_relief_detected":
            sequence_audit.get(
                "starter_relief_detected"
            ),
        "anomaly_counts": _mapping(
            sequence_audit.get("anomaly_counts")
        ),
        "role_summaries": {
            role: {
                "appearance_count":
                    _mapping(summary).get(
                        "appearance_count"
                    ),
                "appearance_rate":
                    _mapping(summary).get(
                        "appearance_rate"
                    ),
                "team_trial_appearance_count":
                    _mapping(summary).get(
                        "team_trial_appearance_count"
                    ),
                "outs_recorded":
                    _mapping(summary).get(
                        "outs_recorded"
                    ),
                "innings_equivalent":
                    _mapping(summary).get(
                        "innings_equivalent"
                    ),
            }
            for role, summary
            in sorted(role_summaries.items())
        },
    }


@dataclass(frozen=True)
class PitcherSequencingPairedShadowAudit:
    """Read-only paired sequencing audit result."""

    status: str
    baseline_execution: Any = None
    candidate_execution: Any = None
    comparison: Mapping[str, Any] | None = None
    blockers: tuple[str, ...] = ()
    enabled: bool = False
    audit_version: str = SCHEMA_VERSION

    @property
    def production_execution(self) -> Any:
        return self.baseline_execution

    def to_diagnostics(self) -> dict[str, Any]:
        baseline_diagnostics = (
            _execution_diagnostics(
                self.baseline_execution
            )
        )
        candidate_diagnostics = (
            _execution_diagnostics(
                self.candidate_execution
            )
        )
        baseline_sequence = _sequence_audit(
            baseline_diagnostics
        )
        candidate_sequence = _sequence_audit(
            candidate_diagnostics
        )
        comparison = _mapping(self.comparison)

        return {
            "schema_version": self.audit_version,
            "status": self.status,
            "enabled": self.enabled,
            "blockers": list(self.blockers),
            "baseline_execution": {
                "status":
                    baseline_diagnostics.get(
                        "status"
                    ),
                "executed":
                    baseline_diagnostics.get(
                        "executed"
                    ),
                "simulation_count":
                    baseline_diagnostics.get(
                        "simulation_count"
                    ),
            },
            "candidate_execution": {
                "status":
                    candidate_diagnostics.get(
                        "status"
                    ),
                "executed":
                    candidate_diagnostics.get(
                        "executed"
                    ),
                "simulation_count":
                    candidate_diagnostics.get(
                        "simulation_count"
                    ),
            },
            "baseline_sequence": (
                _sequence_summary(
                    baseline_sequence
                )
            ),
            "candidate_sequence": (
                _sequence_summary(
                    candidate_sequence
                )
            ),
            "comparison": {
                key: value
                for key, value
                in comparison.items()
                if key != "records"
            },
            "safety_checks": {
                "production_inputs_unchanged": (
                    _stable_inputs_match(
                        baseline_diagnostics,
                        candidate_diagnostics,
                    )
                    if (
                        baseline_diagnostics
                        and candidate_diagnostics
                    )
                    else False
                ),
                "simulation_counts_match": (
                    baseline_diagnostics.get(
                        "simulation_count"
                    )
                    == candidate_diagnostics.get(
                        "simulation_count"
                    )
                ),
                "database_writes_performed":
                    False,
                "production_authority_changed":
                    False,
            },
            "decision": {
                "pitcher_sequence_activation_allowed":
                    False,
                "production_activation_allowed":
                    False,
                "recommended_next_slice": (
                    "correct_canonical_opener_bulk_"
                    "workload_policy"
                    if (
                        "opener_bulk_workload_"
                        "order_invalid"
                        in self.blockers
                    )
                    else (
                        "audit_pitcher_profile_skill_"
                        "and_role_calibration"
                        if self.status == "observed"
                        else
                        "run_paired_pitcher_"
                        "sequencing_shadow_audit"
                    )
                ),
            },
            "database_writes_performed": False,
            "production_authority_changed": False,
        }


def run_paired_pitcher_sequencing_shadow_audit(
    *,
    enabled: bool = False,
    away_pitching_plan_classification: (
        Mapping[str, Any] | None
    ) = None,
    home_pitching_plan_classification: (
        Mapping[str, Any] | None
    ) = None,
    execution_runner: Callable[..., Any] | None = None,
    **production_inputs: Any,
) -> PitcherSequencingPairedShadowAudit:
    """
    Run baseline and candidate plans with identical inputs.

    The baseline omits classification evidence and therefore uses the
    safe traditional-starter materialization. The candidate receives
    the supplied classifications. Neither result becomes authoritative.
    """

    if enabled is not True:
        return PitcherSequencingPairedShadowAudit(
            status="disabled",
            enabled=False,
        )

    if (
        not isinstance(
            away_pitching_plan_classification,
            Mapping,
        )
        and not isinstance(
            home_pitching_plan_classification,
            Mapping,
        )
    ):
        return PitcherSequencingPairedShadowAudit(
            status="blocked",
            blockers=(
                "candidate_plan_classification_unavailable",
            ),
            enabled=True,
        )

    if execution_runner is None:
        from mlb_app.simulation.shadow.production_execution import (
            run_canonical_production_shadow,
        )

        execution_runner = (
            run_canonical_production_shadow
        )

    baseline = execution_runner(
        **production_inputs,
        away_pitching_plan_classification=None,
        home_pitching_plan_classification=None,
    )
    candidate = execution_runner(
        **production_inputs,
        away_pitching_plan_classification=(
            away_pitching_plan_classification
        ),
        home_pitching_plan_classification=(
            home_pitching_plan_classification
        ),
    )

    baseline_payload = _execution_payload(
        baseline
    )
    candidate_payload = _execution_payload(
        candidate
    )
    baseline_diagnostics = (
        _execution_diagnostics(baseline)
    )
    candidate_diagnostics = (
        _execution_diagnostics(candidate)
    )
    baseline_sequence = _sequence_audit(
        baseline_diagnostics
    )
    candidate_sequence = _sequence_audit(
        candidate_diagnostics
    )

    blockers = []
    comparison = None

    if baseline_payload is None:
        blockers.append(
            "baseline_execution_not_ready"
        )

    if candidate_payload is None:
        blockers.append(
            "candidate_execution_not_ready"
        )

    if (
        baseline_diagnostics.get(
            "simulation_count"
        )
        != candidate_diagnostics.get(
            "simulation_count"
        )
    ):
        blockers.append(
            "paired_execution_count_mismatch"
        )

    if not _stable_inputs_match(
        baseline_diagnostics,
        candidate_diagnostics,
    ):
        blockers.append(
            "production_inputs_changed"
        )

    if (
        baseline_sequence.get("status")
        != "observed"
    ):
        blockers.append(
            "baseline_sequence_audit_not_observed"
        )

    if (
        candidate_sequence.get("status")
        != "observed"
    ):
        blockers.append(
            "candidate_sequence_audit_not_observed"
        )
    else:
        blockers.extend(
            _sequence_blockers(
                candidate_sequence
            )
        )

    if not blockers:
        comparison = (
            compare_hitter_profile_simulation_shadow_payloads(
                baseline_payload=baseline_payload,
                candidate_payload=candidate_payload,
            )
        )

        if comparison.get("status") != "ready":
            blockers.extend(
                comparison.get("blockers") or ()
            )

    return PitcherSequencingPairedShadowAudit(
        status=(
            "observed"
            if not blockers
            else "blocked"
        ),
        baseline_execution=baseline,
        candidate_execution=candidate,
        comparison=comparison,
        blockers=tuple(sorted(set(blockers))),
        enabled=True,
    )
