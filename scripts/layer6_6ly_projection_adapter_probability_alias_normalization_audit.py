#!/usr/bin/env python3
"""Layer 6LY projection adapter probability alias normalization audit.

Audit the 6LX normalized probability surface artifact.

This script is audit-only. It does not:
- execute adapter calls
- compute metrics
- run backtests
- fetch live data
- write databases
- modify production code
- activate mechanics
- grant Layer 6 exit
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any


LAYER = "6LY"
LAYER_SLUG = "layer6_6ly_projection_adapter_probability_alias_normalization_audit"

TMP = Path("tmp")

PREDECESSOR_JSON = TMP / "layer6_6lx_projection_adapter_probability_alias_normalization_implementation.json"
NORMALIZED_SURFACE_JSON = TMP / "layer6_6lx_projection_adapter_probability_alias_normalization_implementation_normalized_surface.json"

OUT_JSON = TMP / f"{LAYER_SLUG}.json"
OUT_CHECKS = TMP / f"{LAYER_SLUG}_checks.csv"
OUT_PREDECESSOR = TMP / f"{LAYER_SLUG}_predecessor.csv"
OUT_INPUT_ARTIFACTS = TMP / f"{LAYER_SLUG}_input_artifacts.csv"
OUT_DECISION = TMP / f"{LAYER_SLUG}_decision.csv"
OUT_SAFETY = TMP / f"{LAYER_SLUG}_safety_boundaries.csv"
OUT_RECOMMENDED = TMP / f"{LAYER_SLUG}_recommended_path.csv"

EXPECTED_PREDECESSOR_LAYER = "6LX"

PASS_DIAGNOSIS = "probability_alias_normalization_artifact_audited"
FAIL_DIAGNOSIS = "probability_alias_normalization_audit_blocked_or_failed"

PASS_NEXT_LAYER = "6LZ_layer_6_projection_adapter_probability_surface_metric_plan"
PASS_RECOMMENDED_PATH = "plan_probability_surface_metric_on_audited_normalized_surface"

FAIL_NEXT_LAYER = "6LY_layer_6_projection_adapter_probability_alias_normalization_audit_repair"
FAIL_RECOMMENDED_PATH = "restore_or_repair_6lx_normalized_surface_artifact_before_audit"

SAFETY_BOUNDARIES = {
    "adapter_calls_allowed": False,
    "metrics_allowed": False,
    "backtests_allowed": False,
    "live_data_fetch_allowed": False,
    "database_writes_allowed": False,
    "production_code_changes_allowed": False,
    "mechanics_activation_allowed": False,
    "layer_6_exit_allowed": False,
}


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def first_dict_row(payload: Any) -> dict[str, Any]:
    """Accept a few common non-production artifact shapes without guessing values."""
    if isinstance(payload, list):
        dict_rows = [row for row in payload if isinstance(row, dict)]
        return dict_rows[0] if dict_rows else {}

    if isinstance(payload, dict):
        for key in ("rows", "games", "surface", "normalized_surface", "data"):
            value = payload.get(key)
            if isinstance(value, list):
                dict_rows = [row for row in value if isinstance(row, dict)]
                return dict_rows[0] if dict_rows else {}
        return payload

    return {}


def row_count(payload: Any) -> int:
    if isinstance(payload, list):
        return len([row for row in payload if isinstance(row, dict)])

    if isinstance(payload, dict):
        for key in ("rows", "games", "surface", "normalized_surface", "data"):
            value = payload.get(key)
            if isinstance(value, list):
                return len([row for row in value if isinstance(row, dict)])
        return 1

    return 0


def check_row(name: str, passed: bool, detail: Any) -> dict[str, Any]:
    return {
        "check": name,
        "passed": bool(passed),
        "detail": detail,
    }


def main() -> None:
    TMP.mkdir(exist_ok=True)

    predecessor_exists = PREDECESSOR_JSON.exists()
    surface_exists = NORMALIZED_SURFACE_JSON.exists()

    predecessor = read_json(PREDECESSOR_JSON) if predecessor_exists else {}
    surface_payload = read_json(NORMALIZED_SURFACE_JSON) if surface_exists else None

    row = first_dict_row(surface_payload)
    rows = row_count(surface_payload)

    run_fields = [
        "home_expected_runs",
        "away_expected_runs",
        "total_expected_runs",
        "projected_total",
    ]

    run_surface_gap_remains = bool(row) and all(row.get(field) is None for field in run_fields)

    non_production = (
        predecessor.get("non_production") is True
        or predecessor.get("artifact_scope") == "non_production"
        or row.get("non_production") is True
        or row.get("artifact_scope") == "non_production"
    )

    not_a_backtest_surface = (
        predecessor.get("not_a_backtest_surface") is True
        or predecessor.get("is_backtest_surface") is False
        or row.get("not_a_backtest_surface") is True
        or row.get("is_backtest_surface") is False
    )

    predecessor_layer = predecessor.get("layer") or predecessor.get("layer_id")

    checks = [
        check_row("predecessor_json_exists", predecessor_exists, str(PREDECESSOR_JSON)),
        check_row("predecessor_layer_is_6lx", predecessor_layer == EXPECTED_PREDECESSOR_LAYER, predecessor_layer),
        check_row("normalized_surface_artifact_exists", surface_exists, str(NORMALIZED_SURFACE_JSON)),
        check_row("row_count_equals_1", rows == 1, rows),
        check_row("game_pk_present", "game_pk" in row, row.get("game_pk")),
        check_row("home_win_probability_present", "home_win_probability" in row, row.get("home_win_probability")),
        check_row("away_win_probability_present", "away_win_probability" in row, row.get("away_win_probability")),
        check_row("home_win_prob_preserved", "home_win_prob" in row, row.get("home_win_prob")),
        check_row("away_win_prob_preserved", "away_win_prob" in row, row.get("away_win_prob")),
        check_row("non_production_true", non_production, non_production),
        check_row("not_a_backtest_surface_true", not_a_backtest_surface, not_a_backtest_surface),
        check_row(
            "run_surface_gap_remains",
            run_surface_gap_remains,
            {field: row.get(field) for field in run_fields},
        ),
        check_row("no_metrics_ready_yet", True, "audit layer does not compute metrics"),
        check_row("no_adapter_calls_occurred", True, "script contains no adapter invocation"),
        check_row("no_production_code_changed", True, "script writes only tmp audit artifacts"),
        check_row("no_activation_occurred", True, "activation forbidden"),
        check_row("layer_6_exit_remains_blocked", True, "Layer 6 exit forbidden"),
    ]

    all_checks_passed = all(item["passed"] for item in checks)

    diagnosis = PASS_DIAGNOSIS if all_checks_passed else FAIL_DIAGNOSIS
    recommended_next_layer = PASS_NEXT_LAYER if all_checks_passed else FAIL_NEXT_LAYER
    recommended_path = PASS_RECOMMENDED_PATH if all_checks_passed else FAIL_RECOMMENDED_PATH

    failed_checks = [item["check"] for item in checks if not item["passed"]]

    blockers = []
    blockers.extend(failed_checks)
    blockers.extend([
        "run_surface_gap_remains",
        "real_backtest_metrics_not_run",
        "layer6_exit_not_allowed",
    ])

    result = {
        "layer": LAYER,
        "layer_slug": LAYER_SLUG,
        "all_checks_passed": all_checks_passed,
        "diagnosis": diagnosis,
        "recommended_next_layer": recommended_next_layer,
        "recommended_path": recommended_path,
        "predecessor_artifact": str(PREDECESSOR_JSON),
        "normalized_surface_artifact": str(NORMALIZED_SURFACE_JSON),
        "row_count": rows,
        "probability_surface_normalized_and_audited": all_checks_passed,
        "probability_metric_ready_after_audit": False,
        "runs_metric_ready_after_audit": False,
        "any_backtest_metric_ready_after_audit": False,
        "run_surface_gap_remains": run_surface_gap_remains,
        "layer_6_exit_recommended": False,
        "failed_checks": failed_checks,
        "blockers": blockers,
        "safety_boundaries": SAFETY_BOUNDARIES,
        "checks": checks,
    }

    OUT_JSON.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    write_csv(OUT_CHECKS, checks, ["check", "passed", "detail"])

    write_csv(
        OUT_PREDECESSOR,
        [{
            "artifact": str(PREDECESSOR_JSON),
            "exists": predecessor_exists,
            "layer": predecessor_layer,
            "expected_layer": EXPECTED_PREDECESSOR_LAYER,
        }],
        ["artifact", "exists", "layer", "expected_layer"],
    )

    write_csv(
        OUT_INPUT_ARTIFACTS,
        [
            {"artifact": str(PREDECESSOR_JSON), "exists": predecessor_exists, "required": True},
            {"artifact": str(NORMALIZED_SURFACE_JSON), "exists": surface_exists, "required": True},
        ],
        ["artifact", "exists", "required"],
    )

    write_csv(
        OUT_DECISION,
        [{
            "all_checks_passed": all_checks_passed,
            "diagnosis": diagnosis,
            "probability_surface_normalized_and_audited": all_checks_passed,
            "run_surface_gap_remains": run_surface_gap_remains,
            "layer_6_exit_recommended": False,
        }],
        [
            "all_checks_passed",
            "diagnosis",
            "probability_surface_normalized_and_audited",
            "run_surface_gap_remains",
            "layer_6_exit_recommended",
        ],
    )

    write_csv(
        OUT_SAFETY,
        [{"boundary": key, "allowed": value} for key, value in SAFETY_BOUNDARIES.items()],
        ["boundary", "allowed"],
    )

    write_csv(
        OUT_RECOMMENDED,
        [{
            "recommended_next_layer": recommended_next_layer,
            "recommended_path": recommended_path,
        }],
        ["recommended_next_layer", "recommended_path"],
    )

    print(json.dumps({
        "layer": LAYER,
        "all_checks_passed": all_checks_passed,
        "diagnosis": diagnosis,
        "recommended_next_layer": recommended_next_layer,
        "recommended_path": recommended_path,
        "failed_checks": failed_checks,
        "blockers": blockers,
    }, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
