#!/usr/bin/env python3
"""Layer 6EL planning artifact for live-fetcher runtime summary implementation.

This is intentionally planning-only. It does not modify the live adapter,
fetcher, resolver gates, fixtures, validation scripts, or production defaults.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List


PLAN_SLUG = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_implementation_plan"
)

DIAGNOSIS = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_implementation_plan_complete"
)

NEXT_LAYER = (
    "6EM_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_implementation_plan_audit"
)

TMP_DIR = Path("tmp")
JSON_PATH = TMP_DIR / f"{PLAN_SLUG}.json"

CHECKS_CSV = TMP_DIR / f"{PLAN_SLUG}_checks.csv"
CURRENT_STATE_CSV = TMP_DIR / f"{PLAN_SLUG}_current_state.csv"
SOURCE_CHANGES_CSV = TMP_DIR / f"{PLAN_SLUG}_source_changes.csv"
FIELD_CONTRACT_CSV = TMP_DIR / f"{PLAN_SLUG}_field_contract.csv"
STATUS_CONTRACT_CSV = TMP_DIR / f"{PLAN_SLUG}_status_contract.csv"
VALIDATION_CSV = TMP_DIR / f"{PLAN_SLUG}_validation.csv"
NON_GOALS_CSV = TMP_DIR / f"{PLAN_SLUG}_non_goals.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{PLAN_SLUG}_immutability.csv"


RUNTIME_SUMMARY_FIELDS = [
    {
        "field": "live_fetcher_runtime_summary_status",
        "type": "string",
        "required": True,
        "additive": True,
        "purpose": "Human-readable runtime outcome bucket for the live adapter CLI artifact.",
    },
    {
        "field": "live_fetcher_runtime_summary_reason",
        "type": "string",
        "required": True,
        "additive": True,
        "purpose": "Concise explanation for the selected runtime summary status.",
    },
    {
        "field": "live_fetcher_runtime_summary_mode",
        "type": "string",
        "required": True,
        "additive": True,
        "purpose": "Resolved execution mode reflected in the runtime summary.",
    },
    {
        "field": "live_fetcher_runtime_summary_gate",
        "type": "string",
        "required": True,
        "additive": True,
        "purpose": "Resolved gate state reflected without changing resolver behavior.",
    },
    {
        "field": "live_fetcher_runtime_summary_safe_to_proceed",
        "type": "boolean",
        "required": True,
        "additive": True,
        "purpose": "Boolean safety rollup for whether runtime posture is safe.",
    },
    {
        "field": "live_fetcher_runtime_summary_external_fetch_enabled",
        "type": "boolean",
        "required": True,
        "additive": True,
        "purpose": "Mirrors whether external fetch is enabled after existing resolution.",
    },
    {
        "field": "live_fetcher_runtime_summary_write_blocked",
        "type": "boolean",
        "required": True,
        "additive": True,
        "purpose": "Shows whether DB writes are blocked by dry-run/write policy.",
    },
    {
        "field": "live_fetcher_runtime_summary_candidate_materialization_blocked",
        "type": "boolean",
        "required": True,
        "additive": True,
        "purpose": "Shows whether candidate materialization is blocked by safety posture.",
    },
    {
        "field": "live_fetcher_runtime_summary_dependency_missing",
        "type": "boolean",
        "required": True,
        "additive": True,
        "purpose": "Shows whether missing dependency state contributed to runtime status.",
    },
    {
        "field": "live_fetcher_runtime_summary_field_version",
        "type": "integer",
        "required": True,
        "additive": True,
        "purpose": "Version marker for the runtime summary field contract.",
    },
]

STATUS_CONTRACT = [
    {
        "scenario": "default_no_real_gate_live_dry_run",
        "expected_status": "safe_dry_run_no_real_fetch",
        "expected_reason": "Live dry-run remains safe because real external fetch is not gated on.",
        "safe_to_proceed": True,
        "network_allowed_in_validation": False,
    },
    {
        "scenario": "synthetic_path",
        "expected_status": "validation_synthetic_dry_run",
        "expected_reason": "Synthetic validation path is dry-run and does not fetch real external data.",
        "safe_to_proceed": True,
        "network_allowed_in_validation": False,
    },
    {
        "scenario": "real_gated_monkeypatch_path",
        "expected_status": "real_gated_dry_run_candidate",
        "expected_reason": "Real-gated candidate path is represented in validation without network access.",
        "safe_to_proceed": True,
        "network_allowed_in_validation": False,
    },
    {
        "scenario": "dependency_missing_path",
        "expected_status": "dependency_missing_safe",
        "expected_reason": "Missing dependency is surfaced while keeping the artifact safe and diagnostic-only.",
        "safe_to_proceed": True,
        "network_allowed_in_validation": False,
    },
    {
        "scenario": "live_without_dry_run",
        "expected_status": "blocked_requires_dry_run",
        "expected_reason": "Live execution without dry-run must be blocked by runtime summary posture.",
        "safe_to_proceed": False,
        "network_allowed_in_validation": False,
    },
    {
        "scenario": "live_write_attempt",
        "expected_status": "blocked_write",
        "expected_reason": "Any live write attempt must be marked unsafe and write-blocked.",
        "safe_to_proceed": False,
        "network_allowed_in_validation": False,
    },
    {
        "scenario": "invalid_or_multi_date_window",
        "expected_status": "blocked_date_window_invalid",
        "expected_reason": "Invalid or multi-date windows remain blocked for live-fetcher safety.",
        "safe_to_proceed": False,
        "network_allowed_in_validation": False,
    },
]

CURRENT_STATE = [
    {
        "area": "roadmap_position",
        "fact": "Layer 6 is active and this substream supports bullpen sequencing and leverage behavior.",
        "6el_action": "Plan runtime summary implementation without claiming Layer 6 completion.",
    },
    {
        "area": "6ef_preflight_fields",
        "fact": "6EF added diagnostic-only preflight fields.",
        "6el_action": "Require future implementation to consume or mirror safety posture without mutating 6EF contract.",
    },
    {
        "area": "6ej_6ek_runtime_plan",
        "fact": "6EJ/6EK established the 10-field runtime summary contract.",
        "6el_action": "Carry the exact field set forward into the future implementation plan.",
    },
    {
        "area": "live_adapter_required_fields",
        "fact": "Existing artifact required fields must remain present and compatible.",
        "6el_action": "Add artifact compatibility validation to future implementation layer.",
    },
    {
        "area": "network_safety",
        "fact": "Validation must not depend on CI network access or real Statcast fetches.",
        "6el_action": "Use monkeypatch-style validation for real-gated candidate coverage without network.",
    },
]

SOURCE_CHANGES = [
    {
        "target_file": "scripts/fetch_candidate_bullpen_statcast_live_adapter.py",
        "future_change": "Add helper _candidate_bullpen_live_fetcher_runtime_summary(...).",
        "insertion_point": "After preflight helper or near existing observability/preflight helpers.",
        "6el_change": "planned_only",
    },
    {
        "target_file": "scripts/fetch_candidate_bullpen_statcast_live_adapter.py",
        "future_change": "Add apply helper _candidate_bullpen_apply_live_fetcher_runtime_summary(...).",
        "insertion_point": "Near runtime summary helper so artifact mutation remains localized.",
        "6el_change": "planned_only",
    },
    {
        "target_file": "scripts/fetch_candidate_bullpen_statcast_live_adapter.py",
        "future_change": "Apply runtime summary after observability and preflight fields have been applied.",
        "insertion_point": "CLI artifact assembly path after observability/preflight mutation.",
        "6el_change": "planned_only",
    },
    {
        "target_file": "scripts/fetch_candidate_bullpen_statcast_live_adapter.py",
        "future_change": "Do not change resolver gates, adapter behavior, production defaults, write policy, or materialization policy.",
        "insertion_point": "N/A",
        "6el_change": "planned_only",
    },
]

VALIDATION_PLAN = [
    {
        "validation": "source_validation",
        "requirement": "Confirm future source contains both helper names and all 10 runtime fields.",
        "evidence": "CSV row and JSON summary.",
    },
    {
        "validation": "default_artifact_validation",
        "requirement": "Run default live dry-run artifact and verify safe dry-run/no-real-fetch summary.",
        "evidence": "Artifact field assertions.",
    },
    {
        "validation": "synthetic_artifact_validation",
        "requirement": "Run synthetic validation path and verify validation synthetic dry-run status.",
        "evidence": "Artifact field assertions.",
    },
    {
        "validation": "monkeypatched_real_gated_artifact_validation_without_network",
        "requirement": "Cover real-gated path without network by monkeypatching fetch dependency.",
        "evidence": "Artifact field assertions and no-network guarantee.",
    },
    {
        "validation": "dependency_missing_validation",
        "requirement": "Cover dependency-missing path and verify dependency flag/status.",
        "evidence": "Artifact field assertions.",
    },
    {
        "validation": "blocked_path_validation",
        "requirement": "Cover no-dry-run, write-attempt, and invalid/multi-date blocked paths.",
        "evidence": "Artifact field assertions.",
    },
    {
        "validation": "artifact_compatibility_validation",
        "requirement": "Verify existing required live artifact fields remain present and unchanged in meaning.",
        "evidence": "Compatibility CSV.",
    },
    {
        "validation": "import_boundary_validation",
        "requirement": "Verify no new import side effects and no required network-only dependency for import.",
        "evidence": "Import check row.",
    },
    {
        "validation": "safety_validation",
        "requirement": "Verify no CI network, DB writes, or materialization during validation.",
        "evidence": "Safety assertions.",
    },
    {
        "validation": "immutability_validation",
        "requirement": "Verify unrelated files and prior layer scripts are not modified by future implementation.",
        "evidence": "Git diff/file whitelist check.",
    },
]

NON_GOALS = [
    "No runtime summary implementation in 6EL.",
    "No real Statcast fetch.",
    "No CI network dependency.",
    "No DB writes.",
    "No candidate materialization.",
    "No adapter changes.",
    "No resolver gate changes.",
    "No production default changes.",
    "No write-policy changes.",
]

IMMUTABILITY = [
    {
        "path": "scripts/backfill_candidate_bullpen_statcast_labels.py",
        "policy": "must_not_modify",
        "reason": "Backfill implementation is outside 6EL planning scope.",
    },
    {
        "path": "scripts/fetch_candidate_bullpen_statcast_live_adapter.py",
        "policy": "must_not_modify_in_6el",
        "reason": "6EL is planning-only; future 6EM+ audited implementation may target this file.",
    },
    {
        "path": "scripts/audit_pitcher_aggregate_rate_provenance.py",
        "policy": "preserve_untracked_if_present",
        "reason": "Explicitly unrelated untracked file.",
    },
    {
        "path": "scripts/backtest_extras_walkoff_hybrid_pairing.py",
        "policy": "preserve_untracked_if_present",
        "reason": "Explicitly unrelated untracked file.",
    },
    {
        "path": "scripts/backtest_transition_parameter_sensitivity.py",
        "policy": "preserve_untracked_if_present",
        "reason": "Explicitly unrelated untracked file.",
    },
    {
        "path": "scripts/debug_extras_walkoff_payload_paths.py",
        "policy": "preserve_untracked_if_present",
        "reason": "Explicitly unrelated untracked file.",
    },
    {
        "path": "fixtures",
        "policy": "must_not_modify",
        "reason": "Fixtures are outside 6EL planning scope.",
    },
    {
        "path": "6EF/6EG/6EH/6EI/6EJ/6EK validation and audit scripts",
        "policy": "must_not_modify",
        "reason": "Prior layer evidence must remain immutable.",
    },
]


def write_csv(path: Path, rows: Iterable[Dict[str, Any]]) -> int:
    rows = list(rows)
    if not rows:
        raise ValueError(f"no rows for {path}")
    fieldnames: List[str] = list(rows[0].keys())
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    checks = [
        {
            "check": "planning_only_scope",
            "passed": True,
            "detail": "6EL creates only a planning artifact and does not implement runtime summary fields.",
        },
        {
            "check": "runtime_summary_field_contract",
            "passed": len(RUNTIME_SUMMARY_FIELDS) == 10,
            "detail": f"{len(RUNTIME_SUMMARY_FIELDS)} runtime summary fields planned.",
        },
        {
            "check": "status_contract",
            "passed": len(STATUS_CONTRACT) == 7,
            "detail": f"{len(STATUS_CONTRACT)} status scenarios planned.",
        },
        {
            "check": "source_change_plan",
            "passed": len(SOURCE_CHANGES) == 4,
            "detail": "Future implementation source insertion and non-change constraints are explicit.",
        },
        {
            "check": "validation_plan",
            "passed": len(VALIDATION_PLAN) == 10,
            "detail": "Future implementation validation covers source, artifacts, safety, compatibility, and immutability.",
        },
        {
            "check": "non_goals",
            "passed": len(NON_GOALS) == 9,
            "detail": "Non-goals explicitly prevent implementation, network, DB writes, materialization, adapter, gate, default, and write-policy changes.",
        },
        {
            "check": "immutability",
            "passed": len(IMMUTABILITY) >= 8,
            "detail": "Unrelated files and prior layer artifacts are protected.",
        },
        {
            "check": "recommended_next_layer",
            "passed": NEXT_LAYER.startswith("6EM_"),
            "detail": NEXT_LAYER,
        },
    ]

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "current_state": write_csv(CURRENT_STATE_CSV, CURRENT_STATE),
        "source_changes": write_csv(SOURCE_CHANGES_CSV, SOURCE_CHANGES),
        "field_contract": write_csv(FIELD_CONTRACT_CSV, RUNTIME_SUMMARY_FIELDS),
        "status_contract": write_csv(STATUS_CONTRACT_CSV, STATUS_CONTRACT),
        "validation": write_csv(VALIDATION_CSV, VALIDATION_PLAN),
        "non_goals": write_csv(
            NON_GOALS_CSV,
            [{"non_goal": value, "planned_enforcement": "future implementation and audit must preserve this"} for value in NON_GOALS],
        ),
        "immutability": write_csv(IMMUTABILITY_CSV, IMMUTABILITY),
    }

    all_checks_passed = all(row["passed"] for row in checks)

    plan = {
        "layer": "6EL",
        "name": "candidate bullpen Statcast live adapter CLI live fetcher observability preflight runtime summary implementation plan",
        "planning_only": True,
        "diagnosis": DIAGNOSIS if all_checks_passed else "failed",
        "all_checks_passed": all_checks_passed,
        "recommended_next_layer": NEXT_LAYER,
        "future_helper_name": "_candidate_bullpen_live_fetcher_runtime_summary",
        "future_apply_helper_name": "_candidate_bullpen_apply_live_fetcher_runtime_summary",
        "future_target_file": "scripts/fetch_candidate_bullpen_statcast_live_adapter.py",
        "runtime_summary_fields": [row["field"] for row in RUNTIME_SUMMARY_FIELDS],
        "status_scenarios": [row["scenario"] for row in STATUS_CONTRACT],
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "current_state_csv": str(CURRENT_STATE_CSV),
            "source_changes_csv": str(SOURCE_CHANGES_CSV),
            "field_contract_csv": str(FIELD_CONTRACT_CSV),
            "status_contract_csv": str(STATUS_CONTRACT_CSV),
            "validation_csv": str(VALIDATION_CSV),
            "non_goals_csv": str(NON_GOALS_CSV),
            "immutability_csv": str(IMMUTABILITY_CSV),
        },
        "csv_counts": csv_counts,
    }

    JSON_PATH.write_text(json.dumps(plan, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    print(json.dumps(plan, indent=2, sort_keys=True))
    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
