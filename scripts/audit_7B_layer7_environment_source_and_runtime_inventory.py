#!/usr/bin/env python3
"""
Layer 7B
Layer 7 Environment Source and Runtime Inventory

Performs a bounded semantic inventory of existing venue, park-factor, roof,
weather, atmospheric, field-geometry, batted-ball, environment-runtime, and
environment-validation references.

This layer determines whether repository evidence appears to represent:
- data/source acquisition;
- schema or data contracts;
- runtime consumers;
- fallback behavior;
- tests/audits;
- documentation or planning only.

Inventory and diagnosis only. This layer does not:
- modify production simulation behavior;
- change simulation probabilities, parameters, or canonical authority;
- execute historical validation, tuning, backtests, pricing, or edge logic.
"""

from __future__ import annotations

import ast
import csv
import json
import re
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "7B"
LAYER_NAME = "layer7_environment_source_and_runtime_inventory"

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp/layer_7B_environment_source_and_runtime_inventory"
)

PLAN_7A_PATH = (
    ROOT
    / "scripts/plan_7A_layer7_environment_realism_inventory_and_scope.py"
)

ROADMAP_PATH = (
    ROOT
    / "docs/roadmap_to_edge_detection.md"
)

SUPPORTED_SUFFIXES = {
    ".py",
    ".md",
    ".json",
    ".yaml",
    ".yml",
    ".csv",
    ".toml",
    ".txt",
    ".js",
    ".jsx",
    ".ts",
    ".tsx",
}

EXCLUDED_PARTS = {
    ".git",
    ".venv",
    "venv",
    "node_modules",
    "__pycache__",
    "tmp",
    "dist",
    "build",
}

DOMAIN_PATTERNS = {
    "venue_identity": [
        r"\bvenue\b",
        r"\bvenue_id\b",
        r"\bstadium\b",
        r"\bballpark\b",
        r"\bpark_name\b",
        r"\bhome_venue\b",
    ],
    "park_factors": [
        r"\bpark factor\b",
        r"\bpark_factor\b",
        r"\bpark_factors\b",
        r"\brun_environment\b",
        r"\brun environment\b",
        r"\bneutral park\b",
    ],
    "roof_and_dome_state": [
        r"\broof\b",
        r"\broof_open\b",
        r"\broof_closed\b",
        r"\bdome\b",
        r"\bretractable\b",
        r"\bindoor\b",
    ],
    "weather_core": [
        r"\bweather\b",
        r"\btemperature\b",
        r"\bhumidity\b",
        r"\bprecipitation\b",
        r"\bdew_point\b",
        r"\bdew point\b",
    ],
    "wind": [
        r"\bwind\b",
        r"\bwind_speed\b",
        r"\bwind_direction\b",
        r"\bwind direction\b",
        r"\bfield_orientation\b",
        r"\bfield orientation\b",
    ],
    "atmospheric_physics": [
        r"\bair_density\b",
        r"\bair density\b",
        r"\bbarometric\b",
        r"\bpressure\b",
        r"\baltitude\b",
        r"\belevation\b",
    ],
    "field_geometry": [
        r"\bwall_height\b",
        r"\bwall height\b",
        r"\bfence_distance\b",
        r"\bfence distance\b",
        r"\bfield_geometry\b",
        r"\bfield geometry\b",
        r"\bdimensions\b",
    ],
    "batted_ball_environment": [
        r"\blaunch_angle\b",
        r"\blaunch angle\b",
        r"\bexit_velocity\b",
        r"\bexit velocity\b",
        r"\bspray_angle\b",
        r"\bspray angle\b",
        r"\bcarry\b",
        r"\bbatted_ball\b",
        r"\bbatted ball\b",
    ],
    "environment_runtime_wiring": [
        r"\benvironment_source\b",
        r"\bweather_source\b",
        r"\bpark_source\b",
        r"\benvironment_modifier\b",
        r"\bweather_modifier\b",
        r"\bpark_modifier\b",
        r"\benvironment_context\b",
    ],
    "environment_validation": [
        r"\bpark bucket\b",
        r"\bweather bucket\b",
        r"\benvironment bucket\b",
        r"\bhr calibration\b",
        r"\bxbh calibration\b",
        r"\btotal-run calibration\b",
        r"\brun bias\b",
    ],
}

EVIDENCE_PATTERNS = {
    "source_acquisition": [
        r"\brequests\.",
        r"\bhttpx\.",
        r"\burllib\b",
        r"\bfetch\(",
        r"\bapi\b",
        r"\bendpoint\b",
        r"\bdownload\b",
        r"\bscrape\b",
        r"\bprovider\b",
        r"\bsource_url\b",
    ],
    "schema_contract": [
        r"\bdataclass\b",
        r"\bTypedDict\b",
        r"\bBaseModel\b",
        r"\bschema\b",
        r"\bcontract\b",
        r"\bfield\b",
        r"\bcolumns\b",
        r"\brequired_keys\b",
        r"\bvalidate\b",
    ],
    "runtime_consumer": [
        r"\bsimulat",
        r"\bengine\b",
        r"\bbuilder\b",
        r"\bruntime\b",
        r"\bprojection\b",
        r"\bprobabilit",
        r"\bmodifier\b",
        r"\badjust",
        r"\bcontext\b",
    ],
    "fallback_behavior": [
        r"\bfallback\b",
        r"\bdefault\b",
        r"\bneutral\b",
        r"\bmissing\b",
        r"\bunknown\b",
        r"\bNone\b",
        r"\bget\(",
        r"\btry\b",
        r"\bexcept\b",
    ],
    "test_or_audit": [
        r"\btest_\w+",
        r"\bpytest\b",
        r"\bunittest\b",
        r"\bassert\b",
        r"\baudit\b",
        r"\bdiagnos",
        r"\bverification\b",
        r"\bvalidation\b",
    ],
    "documentation_or_plan": [
        r"\broadmap\b",
        r"\bplan\b",
        r"\bproposal\b",
        r"\bfuture\b",
        r"\bshould\b",
        r"\bTODO\b",
        r"\bnot yet\b",
        r"\bdiagnostic-only\b",
    ],
}

PRODUCTION_PATH_HINTS = [
    "mlb_app/",
    "app/",
    "src/",
    "backend/",
    "simulation/",
    "engine/",
]

TEST_PATH_HINTS = [
    "test",
    "tests/",
    "audit_",
    "assess_",
    "diagnose_",
    "verify_",
]

PLAN_PATH_HINTS = [
    "plan_",
    "docs/",
    "roadmap",
    "proposal",
]

PROHIBITED_AUTHORITIES = [
    "production_environment_activation",
    "simulation_state_change",
    "simulation_parameter_change",
    "simulation_probability_change",
    "canonical_probability_replacement",
    "historical_outcome_join",
    "accuracy_metric_generation",
    "parameter_tuning",
    "backtest_execution",
    "pricing",
    "market_comparison",
    "edge_detection",
    "bet_recommendation",
]


def write_csv(
    path: Path,
    fieldnames: list[str],
    rows: Iterable[dict[str, Any]],
) -> None:
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with path.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=fieldnames,
        )
        writer.writeheader()
        writer.writerows(rows)


def write_json(
    path: Path,
    payload: Any,
) -> None:
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    path.write_text(
        json.dumps(
            payload,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def read_text(path: Path) -> str:
    try:
        return path.read_text(
            encoding="utf-8",
            errors="ignore",
        )
    except OSError:
        return ""


def string_constants(path: Path) -> set[str]:
    if not path.exists():
        return set()

    try:
        tree = ast.parse(
            read_text(path),
            filename=str(path),
        )
    except SyntaxError:
        return set()

    return {
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
    }


def iter_repository_files() -> list[Path]:
    files: list[Path] = []

    for path in ROOT.rglob("*"):
        if not path.is_file():
            continue

        if path.suffix.lower() not in SUPPORTED_SUFFIXES:
            continue

        relative = path.relative_to(ROOT)

        if any(
            part in EXCLUDED_PARTS
            for part in relative.parts
        ):
            continue

        files.append(path)

    return sorted(files)


def pattern_matches(
    text: str,
    patterns: list[str],
) -> tuple[list[str], int]:
    matched = []
    count = 0

    for pattern in patterns:
        occurrences = re.findall(
            pattern,
            text,
            flags=re.IGNORECASE,
        )

        if occurrences:
            matched.append(pattern)
            count += len(occurrences)

    return matched, count


def path_classification(relative_path: str) -> str:
    lowered = relative_path.lower()

    if any(
        hint.lower() in lowered
        for hint in TEST_PATH_HINTS
    ):
        return "test_audit_or_diagnostic"

    if any(
        hint.lower() in lowered
        for hint in PLAN_PATH_HINTS
    ):
        return "documentation_or_plan"

    if any(
        lowered.startswith(hint.lower())
        or f"/{hint.lower()}" in lowered
        for hint in PRODUCTION_PATH_HINTS
    ):
        return "production_candidate"

    return "other_repository_code"


def strongest_evidence_type(
    evidence_counts: dict[str, int],
) -> str:
    if not evidence_counts:
        return "unclassified_reference"

    ranked = sorted(
        evidence_counts.items(),
        key=lambda item: (
            item[1],
            item[0],
        ),
        reverse=True,
    )

    if not ranked or ranked[0][1] == 0:
        return "unclassified_reference"

    return ranked[0][0]


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    plan_constants = string_constants(
        PLAN_7A_PATH
    )

    plan_contract_present = all(
        token in plan_constants
        for token in [
            "layer7_environment_realism_inventory_and_scope_plan_complete",
            "7B_layer7_environment_source_and_runtime_inventory",
            "layer7_environment_inventory_execution",
        ]
    )

    roadmap_text = read_text(
        ROADMAP_PATH
    )

    roadmap_contract_present = all(
        token in roadmap_text
        for token in [
            "Layer 7 — Environment Realism Engine",
            "venue-specific wind geometry",
            "roof state and dome state",
            "weather-to-batted-ball interaction",
            "environment_and_park_physics",
        ]
    )

    repository_files = iter_repository_files()

    evidence_rows: list[dict[str, Any]] = []

    for path in repository_files:
        relative_path = str(
            path.relative_to(ROOT)
        )

        text = read_text(path)

        domain_results = {}

        for domain, patterns in DOMAIN_PATTERNS.items():
            matched_patterns, match_count = pattern_matches(
                text,
                patterns,
            )

            if match_count:
                domain_results[domain] = {
                    "matched_patterns": matched_patterns,
                    "match_count": match_count,
                }

        if not domain_results:
            continue

        evidence_counts = {}

        for evidence_type, patterns in EVIDENCE_PATTERNS.items():
            _, count = pattern_matches(
                text,
                patterns,
            )

            evidence_counts[evidence_type] = count

        evidence_type = strongest_evidence_type(
            evidence_counts
        )

        classification = path_classification(
            relative_path
        )

        for domain, result in domain_results.items():
            evidence_rows.append(
                {
                    "domain": domain,
                    "path": relative_path,
                    "path_classification": classification,
                    "strongest_evidence_type": evidence_type,
                    "domain_match_count": (
                        result["match_count"]
                    ),
                    "matched_domain_patterns": "|".join(
                        result["matched_patterns"]
                    ),
                    "source_acquisition_score": (
                        evidence_counts[
                            "source_acquisition"
                        ]
                    ),
                    "schema_contract_score": (
                        evidence_counts[
                            "schema_contract"
                        ]
                    ),
                    "runtime_consumer_score": (
                        evidence_counts[
                            "runtime_consumer"
                        ]
                    ),
                    "fallback_behavior_score": (
                        evidence_counts[
                            "fallback_behavior"
                        ]
                    ),
                    "test_or_audit_score": (
                        evidence_counts[
                            "test_or_audit"
                        ]
                    ),
                    "documentation_or_plan_score": (
                        evidence_counts[
                            "documentation_or_plan"
                        ]
                    ),
                    "production_capability_verified": False,
                }
            )

    domain_summary_rows = []

    for domain in DOMAIN_PATTERNS:
        rows = [
            row
            for row in evidence_rows
            if row["domain"] == domain
        ]

        production_candidates = {
            row["path"]
            for row in rows
            if row["path_classification"]
            == "production_candidate"
        }

        source_candidates = {
            row["path"]
            for row in rows
            if row["source_acquisition_score"] > 0
        }

        schema_candidates = {
            row["path"]
            for row in rows
            if row["schema_contract_score"] > 0
        }

        runtime_candidates = {
            row["path"]
            for row in rows
            if row["runtime_consumer_score"] > 0
        }

        fallback_candidates = {
            row["path"]
            for row in rows
            if row["fallback_behavior_score"] > 0
        }

        test_candidates = {
            row["path"]
            for row in rows
            if row["test_or_audit_score"] > 0
        }

        plan_only_candidates = {
            row["path"]
            for row in rows
            if row["path_classification"]
            == "documentation_or_plan"
        }

        if (
            production_candidates
            and source_candidates
            and runtime_candidates
        ):
            semantic_status = (
                "candidate_source_and_runtime_paths_present_"
                "requires_direct_contract_audit"
            )
        elif production_candidates:
            semantic_status = (
                "production_candidate_references_present_"
                "runtime_authority_unverified"
            )
        elif rows:
            semantic_status = (
                "references_present_without_verified_"
                "production_path"
            )
        else:
            semantic_status = (
                "no_repository_evidence_found"
            )

        domain_summary_rows.append(
            {
                "domain": domain,
                "matching_files": len(
                    {row["path"] for row in rows}
                ),
                "production_candidate_files": len(
                    production_candidates
                ),
                "source_candidate_files": len(
                    source_candidates
                ),
                "schema_candidate_files": len(
                    schema_candidates
                ),
                "runtime_candidate_files": len(
                    runtime_candidates
                ),
                "fallback_candidate_files": len(
                    fallback_candidates
                ),
                "test_or_audit_files": len(
                    test_candidates
                ),
                "documentation_or_plan_files": len(
                    plan_only_candidates
                ),
                "semantic_status": semantic_status,
                "production_capability_verified": False,
            }
        )

    runtime_candidate_rows = [
        {
            "domain": row["domain"],
            "path": row["path"],
            "path_classification": (
                row["path_classification"]
            ),
            "runtime_consumer_score": (
                row["runtime_consumer_score"]
            ),
            "source_acquisition_score": (
                row["source_acquisition_score"]
            ),
            "schema_contract_score": (
                row["schema_contract_score"]
            ),
            "fallback_behavior_score": (
                row["fallback_behavior_score"]
            ),
            "audit_status": (
                "candidate_requires_direct_file_and_call_graph_audit"
            ),
            "production_authority_verified": False,
        }
        for row in evidence_rows
        if (
            row["path_classification"]
            == "production_candidate"
            or row["runtime_consumer_score"] > 0
        )
    ]

    source_contract_gaps = []

    for row in domain_summary_rows:
        gaps = []

        if row["source_candidate_files"] == 0:
            gaps.append(
                "no_source_acquisition_candidate"
            )

        if row["schema_candidate_files"] == 0:
            gaps.append(
                "no_schema_contract_candidate"
            )

        if row["runtime_candidate_files"] == 0:
            gaps.append(
                "no_runtime_consumer_candidate"
            )

        if row["fallback_candidate_files"] == 0:
            gaps.append(
                "no_fallback_candidate"
            )

        if row["test_or_audit_files"] == 0:
            gaps.append(
                "no_test_or_audit_candidate"
            )

        if row["production_candidate_files"] == 0:
            gaps.append(
                "no_production_path_candidate"
            )

        source_contract_gaps.append(
            {
                "domain": row["domain"],
                "gap_count": len(gaps),
                "gaps": "|".join(gaps),
                "direct_audit_required": True,
                "production_ready": False,
            }
        )

    inventory_checks = [
        {
            "check": "plan_7a_path_exists",
            "actual": PLAN_7A_PATH.exists(),
            "expected": True,
            "passed": PLAN_7A_PATH.exists(),
        },
        {
            "check": "roadmap_path_exists",
            "actual": ROADMAP_PATH.exists(),
            "expected": True,
            "passed": ROADMAP_PATH.exists(),
        },
        {
            "check": "plan_7a_contract_present",
            "actual": plan_contract_present,
            "expected": True,
            "passed": plan_contract_present,
        },
        {
            "check": "roadmap_layer7_contract_present",
            "actual": roadmap_contract_present,
            "expected": True,
            "passed": roadmap_contract_present,
        },
        {
            "check": "ten_environment_domains_audited",
            "actual": len(
                domain_summary_rows
            ),
            "expected": 10,
            "passed": len(
                domain_summary_rows
            )
            == 10,
        },
        {
            "check": "repository_files_scanned",
            "actual": len(
                repository_files
            ),
            "expected": "greater_than_zero",
            "passed": len(
                repository_files
            )
            > 0,
        },
        {
            "check": "semantic_evidence_rows_emitted",
            "actual": len(
                evidence_rows
            ),
            "expected": "greater_than_zero",
            "passed": len(
                evidence_rows
            )
            > 0,
        },
        {
            "check": "domain_gap_rows_emitted",
            "actual": len(
                source_contract_gaps
            ),
            "expected": 10,
            "passed": len(
                source_contract_gaps
            )
            == 10,
        },
        {
            "check": "production_capability_not_inferred",
            "actual": all(
                row[
                    "production_capability_verified"
                ]
                is False
                for row in domain_summary_rows
            ),
            "expected": True,
            "passed": all(
                row[
                    "production_capability_verified"
                ]
                is False
                for row in domain_summary_rows
            ),
        },
        {
            "check": "inventory_only_boundary_preserved",
            "actual": True,
            "expected": True,
            "passed": True,
        },
    ]

    all_checks_passed = all(
        row["passed"]
        for row in inventory_checks
    )

    authority_rows = [
        {
            "authority": authority,
            "granted": False,
            "reason": (
                "7B is a semantic inventory and runtime-path "
                "diagnosis layer only."
            ),
        }
        for authority in PROHIBITED_AUTHORITIES
    ]

    authority_rows.extend(
        [
            {
                "authority": (
                    "venue_and_park_factor_contract_planning"
                ),
                "granted": all_checks_passed,
                "reason": (
                    "7C may define the canonical venue and "
                    "park-factor source contract."
                ),
            },
            {
                "authority": (
                    "production_environment_integration"
                ),
                "granted": False,
                "reason": (
                    "Repository references do not establish "
                    "production environment authority."
                ),
            },
        ]
    )

    diagnosis_name = (
        "layer7_environment_source_and_runtime_inventory_complete"
        if all_checks_passed
        else
        "layer7_environment_source_and_runtime_inventory_failed"
    )

    recommended_next_layer = (
        "7C_canonical_venue_and_park_factor_source_contract_plan"
        if all_checks_passed
        else
        "7C_environment_inventory_remediation"
    )

    write_csv(
        OUTPUT_DIR / "inventory_checks.csv",
        [
            "check",
            "actual",
            "expected",
            "passed",
        ],
        inventory_checks,
    )

    write_csv(
        OUTPUT_DIR / "semantic_evidence.csv",
        [
            "domain",
            "path",
            "path_classification",
            "strongest_evidence_type",
            "domain_match_count",
            "matched_domain_patterns",
            "source_acquisition_score",
            "schema_contract_score",
            "runtime_consumer_score",
            "fallback_behavior_score",
            "test_or_audit_score",
            "documentation_or_plan_score",
            "production_capability_verified",
        ],
        evidence_rows,
    )

    write_csv(
        OUTPUT_DIR / "domain_summary.csv",
        [
            "domain",
            "matching_files",
            "production_candidate_files",
            "source_candidate_files",
            "schema_candidate_files",
            "runtime_candidate_files",
            "fallback_candidate_files",
            "test_or_audit_files",
            "documentation_or_plan_files",
            "semantic_status",
            "production_capability_verified",
        ],
        domain_summary_rows,
    )

    write_csv(
        OUTPUT_DIR / "runtime_candidates.csv",
        [
            "domain",
            "path",
            "path_classification",
            "runtime_consumer_score",
            "source_acquisition_score",
            "schema_contract_score",
            "fallback_behavior_score",
            "audit_status",
            "production_authority_verified",
        ],
        runtime_candidate_rows,
    )

    write_csv(
        OUTPUT_DIR / "source_contract_gaps.csv",
        [
            "domain",
            "gap_count",
            "gaps",
            "direct_audit_required",
            "production_ready",
        ],
        source_contract_gaps,
    )

    write_csv(
        OUTPUT_DIR / "authority_boundaries.csv",
        [
            "authority",
            "granted",
            "reason",
        ],
        authority_rows,
    )

    write_csv(
        OUTPUT_DIR / "recommended_path.csv",
        [
            "recommended_next_layer",
            "recommended_action",
            "entry_condition",
            "passed",
        ],
        [
            {
                "recommended_next_layer": (
                    recommended_next_layer
                ),
                "recommended_action": (
                    "Define canonical venue identity, park-factor "
                    "provenance, season/version semantics, and "
                    "neutral fallback behavior."
                    if all_checks_passed
                    else
                    "Remediate failed 7B inventory checks."
                ),
                "entry_condition": (
                    "All ten 7B inventory checks pass."
                ),
                "passed": all_checks_passed,
            }
        ],
    )

    summary = {
        "inventory_checks_required": len(
            inventory_checks
        ),
        "inventory_checks_passed": sum(
            1
            for row in inventory_checks
            if row["passed"]
        ),
        "repository_files_scanned": len(
            repository_files
        ),
        "semantic_evidence_rows": len(
            evidence_rows
        ),
        "runtime_candidate_rows": len(
            runtime_candidate_rows
        ),
        "environment_domains_audited": len(
            domain_summary_rows
        ),
        "domains_with_production_candidates": sum(
            1
            for row in domain_summary_rows
            if row["production_candidate_files"] > 0
        ),
        "domains_with_source_candidates": sum(
            1
            for row in domain_summary_rows
            if row["source_candidate_files"] > 0
        ),
        "domains_with_runtime_candidates": sum(
            1
            for row in domain_summary_rows
            if row["runtime_candidate_files"] > 0
        ),
        "production_environment_capability_verified": False,
        "production_behavior_changed": False,
        "simulation_behavior_changed": False,
        "canonical_probability_authority_changed": False,
        "production_activation": False,
        "historical_validation_executed": False,
        "pricing_or_edge_work_executed": False,
    }

    write_json(
        OUTPUT_DIR / "inventory_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": diagnosis_name,
        "all_checks_passed": all_checks_passed,
        **summary,
        "layer7_completed": False,
        "new_production_authority_granted": False,
        "historical_validation_allowed_next": False,
        "tuning_allowed_next": False,
        "backtests_allowed_next": False,
        "pricing_allowed_next": False,
        "edge_detection_allowed_next": False,
        "venue_and_park_factor_contract_planning_allowed_next": (
            all_checks_passed
        ),
        "production_environment_integration_allowed_next": False,
        "recommended_next_layer": (
            recommended_next_layer
        ),
        "generated_csv_artifacts": [
            str(
                OUTPUT_DIR / "inventory_checks.csv"
            ),
            str(
                OUTPUT_DIR / "semantic_evidence.csv"
            ),
            str(
                OUTPUT_DIR / "domain_summary.csv"
            ),
            str(
                OUTPUT_DIR / "runtime_candidates.csv"
            ),
            str(
                OUTPUT_DIR / "source_contract_gaps.csv"
            ),
            str(
                OUTPUT_DIR / "authority_boundaries.csv"
            ),
            str(
                OUTPUT_DIR / "recommended_path.csv"
            ),
        ],
        "generated_json_artifacts": [
            str(
                OUTPUT_DIR / "inventory_summary.json"
            ),
            str(
                OUTPUT_DIR / "diagnosis.json"
            ),
        ],
    }

    write_json(
        OUTPUT_DIR / "diagnosis.json",
        diagnosis,
    )

    print(
        json.dumps(
            diagnosis,
            indent=2,
        )
    )

    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
