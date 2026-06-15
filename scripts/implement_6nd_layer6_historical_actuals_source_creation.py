#!/usr/bin/env python3
"""Create local historical actuals source from existing local repo datasets, then rerun 6NA."""

from __future__ import annotations

import csv
import json
import subprocess
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable


SLUG = "layer6_6nd_historical_actuals_source_creation"
TMP_DIR = Path("tmp")

SCRIPT_6NC = Path("scripts/audit_6nc_layer6_historical_actuals_source_preparation_plan.py")
SCRIPT_6NA = Path("scripts/implement_6na_layer6_historical_actuals_source_validation.py")
JSON_6NC = TMP_DIR / "layer6_6nc_historical_actuals_source_preparation_plan_audit.json"
JSON_6NA = TMP_DIR / "layer6_6na_historical_actuals_source_validation.json"

TARGET_ACTUALS = Path("data/local/historical_actuals.csv")

REQUIRED_INPUTS = [
    JSON_6NC,
    TMP_DIR / "layer6_6nc_historical_actuals_source_preparation_plan_audit_checks.csv",
    TMP_DIR / "layer6_6nc_historical_actuals_source_preparation_plan_audit_predecessor.csv",
    TMP_DIR / "layer6_6nc_historical_actuals_source_preparation_plan_audit_input_artifacts.csv",
    TMP_DIR / "layer6_6nc_historical_actuals_source_preparation_plan_audit_target_output_review.csv",
    TMP_DIR / "layer6_6nc_historical_actuals_source_preparation_plan_audit_required_schema_review.csv",
    TMP_DIR / "layer6_6nc_historical_actuals_source_preparation_plan_audit_allowed_source_families_review.csv",
    TMP_DIR / "layer6_6nc_historical_actuals_source_preparation_plan_audit_source_preparation_checks_review.csv",
    TMP_DIR / "layer6_6nc_historical_actuals_source_preparation_plan_audit_row_requirements_review.csv",
    TMP_DIR / "layer6_6nc_historical_actuals_source_preparation_plan_audit_provenance_requirements_review.csv",
    TMP_DIR / "layer6_6nc_historical_actuals_source_preparation_plan_audit_rerun_commands_review.csv",
    TMP_DIR / "layer6_6nc_historical_actuals_source_preparation_plan_audit_blocking_conditions_review.csv",
    TMP_DIR / "layer6_6nc_historical_actuals_source_preparation_plan_audit_allowed_operations_next.csv",
    TMP_DIR / "layer6_6nc_historical_actuals_source_preparation_plan_audit_forbidden_operations_next.csv",
    TMP_DIR / "layer6_6nc_historical_actuals_source_preparation_plan_audit_future_6nd_contract.csv",
    TMP_DIR / "layer6_6nc_historical_actuals_source_preparation_plan_audit_decision.csv",
    TMP_DIR / "layer6_6nc_historical_actuals_source_preparation_plan_audit_safety_boundaries.csv",
    TMP_DIR / "layer6_6nc_historical_actuals_source_preparation_plan_audit_recommended_path.csv",
    SCRIPT_6NC,
    SCRIPT_6NA,
]

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
PREDECESSOR_CSV = TMP_DIR / f"{SLUG}_predecessor.csv"
INPUT_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_input_artifacts.csv"
CANDIDATE_FILES_CSV = TMP_DIR / f"{SLUG}_candidate_files.csv"
CANDIDATE_SCORES_CSV = TMP_DIR / f"{SLUG}_candidate_scores.csv"
SELECTED_SOURCE_CSV = TMP_DIR / f"{SLUG}_selected_source.csv"
SCHEMA_MAPPING_CSV = TMP_DIR / f"{SLUG}_schema_mapping.csv"
NORMALIZED_SAMPLE_CSV = TMP_DIR / f"{SLUG}_normalized_sample.csv"
SOURCE_PROVENANCE_CSV = TMP_DIR / f"{SLUG}_source_provenance.csv"
SAMPLE_SUFFICIENCY_CSV = TMP_DIR / f"{SLUG}_sample_sufficiency.csv"
RERUN_6NA_CSV = TMP_DIR / f"{SLUG}_rerun_6na_summary.csv"
MONEYLINE_BOUNDARIES_CSV = TMP_DIR / f"{SLUG}_moneyline_deferral_boundaries.csv"
ALLOWED_NEXT_CSV = TMP_DIR / f"{SLUG}_allowed_operations_next.csv"
FORBIDDEN_NEXT_CSV = TMP_DIR / f"{SLUG}_forbidden_operations_next.csv"
FUTURE_6NE_CSV = TMP_DIR / f"{SLUG}_future_6ne_contract.csv"
DECISION_CSV = TMP_DIR / f"{SLUG}_decision.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety_boundaries.csv"
RECOMMENDED_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6NC = "layer_6_historical_actuals_source_preparation_plan_audit_complete"
DIAGNOSIS_COMPLETE = "layer_6_historical_actuals_source_creation_implementation_complete"
DIAGNOSIS_BLOCKED = "layer_6_historical_actuals_source_creation_blocked"
RECOMMENDED_NEXT_COMPLETE = "6NE_layer_6_historical_actuals_source_creation_audit"
RECOMMENDED_PATH_COMPLETE = "audit_created_historical_actuals_source_before_actuals_only_metrics_plan"
RECOMMENDED_PATH_BLOCKED = "supply_local_historical_actuals_source_then_rerun_6nd"

ALIASES = {
    "game_pk": ["game_pk", "game_id", "mlb_game_pk", "gamepk", "id"],
    "game_date": ["game_date", "date", "official_date", "game_day"],
    "home_team": ["home_team", "home_name", "home", "home_team_name", "home_abbrev", "home_team_abbrev"],
    "away_team": ["away_team", "away_name", "away", "away_team_name", "away_abbrev", "away_team_abbrev"],
    "home_score": ["home_score", "home_runs", "home_final", "home_team_score", "home_score_actual"],
    "away_score": ["away_score", "away_runs", "away_final", "away_team_score", "away_score_actual"],
    "home_win_binary": ["home_win_binary", "home_win", "home_won", "home_team_win"],
    "status": ["status", "game_status", "abstract_game_state", "detailed_state"],
}

SEARCH_ROOTS = [
    Path("data"),
    Path("data/local"),
    Path("backfills"),
    Path("exports"),
    Path("artifacts"),
    Path("tmp"),
    Path("tmp/statsapi_cache/schedule"),
]

EXCLUDE_PARTS = {
    ".git",
    ".venv",
    "venv",
    "node_modules",
    "__pycache__",
}


def read_csv_rows(path: Path, max_rows: int | None = None) -> list[dict[str, str]]:
    if not path.exists():
        return []
    rows: list[dict[str, str]] = []
    with path.open(newline="", encoding="utf-8", errors="ignore") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            rows.append(dict(row))
            if max_rows is not None and len(rows) >= max_rows:
                break
    return rows


def read_jsonish_rows(path: Path, max_rows: int | None = None) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    text = path.read_text(encoding="utf-8", errors="ignore").strip()
    if not text:
        return []
    rows: list[dict[str, Any]] = []
    if path.suffix.lower() == ".jsonl":
        for line in text.splitlines():
            if not line.strip():
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            if isinstance(obj, dict):
                rows.append(obj)
            if max_rows is not None and len(rows) >= max_rows:
                break
        return rows
    try:
        obj = json.loads(text)
    except Exception:
        return []
    if isinstance(obj, list):
        rows = [x for x in obj if isinstance(x, dict)]
    elif isinstance(obj, dict):
        for key in ["rows", "data", "games", "records", "results"]:
            if isinstance(obj.get(key), list):
                rows = [x for x in obj[key] if isinstance(x, dict)]
                break
        if not rows:
            rows = [obj]
    return rows[:max_rows] if max_rows is not None else rows


def write_csv(path: Path, rows: Iterable[dict[str, Any]]) -> int:
    rows = list(rows)
    if not rows:
        rows = [{"empty": True}]
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        parsed = json.loads(path.read_text(encoding="utf-8", errors="ignore"))
        return parsed if isinstance(parsed, dict) else {"root_type": type(parsed).__name__}
    except Exception:
        return {}


def syntax_compile() -> tuple[int, str]:
    failures: list[str] = []
    for root in [Path("mlb_app"), Path("scripts")]:
        if not root.exists():
            continue
        for path in sorted(root.rglob("*.py")):
            try:
                compile(path.read_text(encoding="utf-8", errors="ignore"), str(path), "exec")
            except Exception as exc:
                failures.append(f"{path}: {type(exc).__name__}: {exc}")
    return (0 if not failures else 1, "\n".join(failures))


def boolish(value: Any) -> bool:
    return value is True or str(value).lower() == "true"


def all_passed(rows: list[dict[str, Any]]) -> bool:
    return all(boolish(row.get("passed", "")) for row in rows)


def parse_int(value: Any) -> int | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        return int(float(str(value).strip()))
    except Exception:
        return None


def parse_date(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d", "%Y%m%d"):
        try:
            return datetime.strptime(text[:10] if fmt == "%Y-%m-%d" else text, fmt)
        except Exception:
            pass
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return None


def map_schema(fieldnames: list[str]) -> dict[str, str | None]:
    normalized = {str(name).strip().lower(): str(name) for name in fieldnames if name is not None}
    mapping: dict[str, str | None] = {}
    for canonical, aliases in ALIASES.items():
        match = None
        for alias in aliases:
            if alias.lower() in normalized:
                match = normalized[alias.lower()]
                break
        mapping[canonical] = match
    return mapping


def get_value(row: dict[str, Any], column: str | None) -> str:
    if not column:
        return ""
    return str(row.get(column, "")).strip()


def discover_candidate_files() -> list[Path]:
    candidates: list[Path] = []
    suffixes = {".csv", ".json", ".jsonl"}
    for root in SEARCH_ROOTS:
        if not root.exists():
            continue
        if root.is_file() and root.suffix.lower() in suffixes:
            candidates.append(root)
            continue
        for path in root.rglob("*"):
            if any(part in EXCLUDE_PARTS for part in path.parts):
                continue
            if path.is_file() and path.suffix.lower() in suffixes:
                if path == TARGET_ACTUALS:
                    continue
                candidates.append(path)
    seen: set[str] = set()
    unique: list[Path] = []
    for path in candidates:
        key = str(path.resolve())
        if key not in seen:
            seen.add(key)
            unique.append(path)
    return sorted(unique)


def extract_statsapi_schedule_games(path: Path, max_rows: int | None = None) -> list[dict[str, Any]]:
    """Extract final-score game rows from local cached StatsAPI schedule JSON."""
    try:
        obj = json.loads(path.read_text(encoding="utf-8", errors="ignore"))
    except Exception:
        return []

    dates = obj.get("dates")
    if not isinstance(dates, list):
        return []

    rows: list[dict[str, Any]] = []
    for date_block in dates:
        if not isinstance(date_block, dict):
            continue
        games = date_block.get("games")
        if not isinstance(games, list):
            continue

        for game in games:
            if not isinstance(game, dict):
                continue

            status = game.get("status") if isinstance(game.get("status"), dict) else {}
            detailed_state = str(status.get("detailedState", "")).strip().lower()
            abstract_state = str(status.get("abstractGameState", "")).strip().lower()
            coded_state = str(status.get("codedGameState", "")).strip().lower()

            is_final = (
                detailed_state == "final"
                or abstract_state == "final"
                or coded_state == "f"
            )
            if not is_final:
                continue

            teams = game.get("teams") if isinstance(game.get("teams"), dict) else {}
            home = teams.get("home") if isinstance(teams.get("home"), dict) else {}
            away = teams.get("away") if isinstance(teams.get("away"), dict) else {}
            home_team = home.get("team") if isinstance(home.get("team"), dict) else {}
            away_team = away.get("team") if isinstance(away.get("team"), dict) else {}

            rows.append(
                {
                    "game_pk": game.get("gamePk"),
                    "game_date": game.get("officialDate") or game.get("gameDate") or date_block.get("date"),
                    "home_team": home_team.get("name") or home_team.get("abbreviation") or home_team.get("teamName"),
                    "away_team": away_team.get("name") or away_team.get("abbreviation") or away_team.get("teamName"),
                    "home_score": home.get("score"),
                    "away_score": away.get("score"),
                    "home_win_binary": None,
                    "source_artifact": str(path),
                }
            )

    return rows[:max_rows] if max_rows is not None else rows


def load_rows(path: Path, max_rows: int | None = None) -> list[dict[str, Any]]:
    if path.suffix.lower() == ".csv":
        return read_csv_rows(path, max_rows=max_rows)
    if path.suffix.lower() in {".json", ".jsonl"}:
        if "statsapi_cache/schedule" in str(path):
            return extract_statsapi_schedule_games(path, max_rows=max_rows)
        return read_jsonish_rows(path, max_rows=max_rows)
    return []


def normalize_rows(path: Path, rows: list[dict[str, Any]], mapping: dict[str, str | None]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
    normalized_rows: list[dict[str, Any]] = []
    invalid_rows: list[dict[str, Any]] = []
    dates: list[datetime] = []

    for idx, row in enumerate(rows, start=2):
        game_pk = get_value(row, mapping.get("game_pk"))
        game_date_raw = get_value(row, mapping.get("game_date"))
        game_date = parse_date(game_date_raw)
        home_team = get_value(row, mapping.get("home_team"))
        away_team = get_value(row, mapping.get("away_team"))
        home_score = parse_int(get_value(row, mapping.get("home_score")))
        away_score = parse_int(get_value(row, mapping.get("away_score")))
        home_win_binary_raw = get_value(row, mapping.get("home_win_binary"))
        home_win_binary = parse_int(home_win_binary_raw) if home_win_binary_raw != "" else None

        reasons: list[str] = []
        if not game_pk:
            reasons.append("missing_game_pk")
        if game_date is None:
            reasons.append("invalid_game_date")
        if not home_team:
            reasons.append("missing_home_team")
        if not away_team:
            reasons.append("missing_away_team")
        if home_score is None or home_score < 0:
            reasons.append("invalid_home_score")
        if away_score is None or away_score < 0:
            reasons.append("invalid_away_score")
        if home_score is not None and away_score is not None and home_score == away_score:
            reasons.append("tie_game_blocked")
        if home_win_binary is None and home_score is not None and away_score is not None:
            home_win_binary = int(home_score > away_score)
        if home_win_binary not in (0, 1):
            reasons.append("invalid_home_win_binary")
        if home_score is not None and away_score is not None and home_win_binary in (0, 1):
            if home_win_binary != int(home_score > away_score):
                reasons.append("home_win_binary_mismatch")

        if reasons:
            invalid_rows.append({"source_file": str(path), "source_row_number": idx, "game_pk": game_pk, "reasons": "|".join(reasons)})
            continue

        assert game_date is not None
        dates.append(game_date)
        normalized_rows.append(
            {
                "game_pk": game_pk,
                "game_date": game_date.strftime("%Y-%m-%d"),
                "home_team": home_team,
                "away_team": away_team,
                "home_score": home_score,
                "away_score": away_score,
                "home_win_binary": home_win_binary,
                "source_artifact": str(path),
            }
        )

    duplicate_game_pk = sum(count for count in Counter(row["game_pk"] for row in normalized_rows).values() if count > 1)
    if duplicate_game_pk:
        seen: set[str] = set()
        deduped: list[dict[str, Any]] = []
        for row in normalized_rows:
            if row["game_pk"] in seen:
                invalid_rows.append({"source_file": str(path), "source_row_number": "", "game_pk": row["game_pk"], "reasons": "duplicate_game_pk"})
                continue
            seen.add(row["game_pk"])
            deduped.append(row)
        normalized_rows = deduped

    date_span_days = 0
    if dates:
        date_span_days = (max(dates).date() - min(dates).date()).days + 1

    return normalized_rows, invalid_rows, date_span_days


def score_candidate(path: Path) -> dict[str, Any]:
    try:
        rows = load_rows(path)
    except Exception as exc:
        return {"path": str(path), "readable": False, "score": 0, "reason": f"{type(exc).__name__}: {exc}", "passed": False}

    if not rows:
        return {"path": str(path), "readable": True, "row_count": 0, "score": 0, "reason": "no_rows", "passed": False}

    fieldnames = list(rows[0].keys())
    mapping = map_schema(fieldnames)
    required = ["game_pk", "game_date", "home_team", "away_team", "home_score", "away_score"]
    schema_coverage = sum(1 for field in required if mapping.get(field))
    normalized_rows, invalid_rows, date_span_days = normalize_rows(path, rows, mapping)
    normalized_count = len(normalized_rows)
    invalid_count = len(invalid_rows)

    filename_bonus = 0
    lower_name = str(path).lower()
    for token in ["actual", "backfill", "result", "final", "score", "game"]:
        if token in lower_name:
            filename_bonus += 5

    score = schema_coverage * 100 + normalized_count + min(date_span_days, 365) + filename_bonus - invalid_count * 2
    sample_classification = "real_evaluation_candidate" if date_span_days > 21 and normalized_count >= 100 else "smoke_test_only"

    return {
        "path": str(path),
        "readable": True,
        "row_count": len(rows),
        "normalized_row_count": normalized_count,
        "invalid_row_count": invalid_count,
        "schema_coverage": schema_coverage,
        "date_span_days": date_span_days,
        "sample_classification": sample_classification,
        "score": score,
        "reason": "candidate_scored",
        "passed": normalized_count > 0 and schema_coverage == len(required),
    }


def run_6na() -> tuple[int | None, dict[str, Any]]:
    if not SCRIPT_6NA.exists():
        return None, {}
    completed = subprocess.run(
        [sys.executable, str(SCRIPT_6NA)],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    return completed.returncode, load_json(JSON_6NA)


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    compile_returncode, compile_errors = syntax_compile()
    json_6nc = load_json(JSON_6NC)

    input_rows = [
        {
            "artifact_path": str(path),
            "exists": path.exists(),
            "row_count": len(read_csv_rows(path)) if path.suffix == ".csv" else "",
            "passed": path.exists(),
        }
        for path in REQUIRED_INPUTS
    ]

    predecessor_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6nc_script_exists", "expected": True, "actual": SCRIPT_6NC.exists(), "passed": SCRIPT_6NC.exists()},
        {"check": "6na_script_exists", "expected": True, "actual": SCRIPT_6NA.exists(), "passed": SCRIPT_6NA.exists()},
        {"check": "6nc_json_exists", "expected": True, "actual": JSON_6NC.exists(), "passed": JSON_6NC.exists()},
        {"check": "6nc_all_checks_passed", "expected": True, "actual": json_6nc.get("all_checks_passed"), "passed": json_6nc.get("all_checks_passed") is True},
        {"check": "6nc_diagnosis", "expected": DIAGNOSIS_6NC, "actual": json_6nc.get("diagnosis"), "passed": json_6nc.get("diagnosis") == DIAGNOSIS_6NC},
        {"check": "historical_actuals_source_creation_allowed_next", "expected": True, "actual": json_6nc.get("historical_actuals_source_creation_allowed_next"), "passed": json_6nc.get("historical_actuals_source_creation_allowed_next") is True},
        {"check": "metric_execution_allowed_next", "expected": False, "actual": json_6nc.get("metric_execution_allowed_next"), "passed": json_6nc.get("metric_execution_allowed_next") is False},
        {"check": "backtest_execution_allowed_next", "expected": False, "actual": json_6nc.get("backtest_execution_allowed_next"), "passed": json_6nc.get("backtest_execution_allowed_next") is False},
    ]

    schedule_files = sorted(Path("tmp/statsapi_cache/schedule").glob("*.json")) if Path("tmp/statsapi_cache/schedule").exists() else []
    generic_files = [] if schedule_files else discover_candidate_files()

    candidate_files = []
    seen_candidate_paths = set()
    for candidate_path in schedule_files + generic_files:
        key = str(candidate_path)
        if key in seen_candidate_paths:
            continue
        seen_candidate_paths.add(key)
        candidate_files.append(candidate_path)

    candidate_file_rows = [
        {
            "candidate_path": str(path),
            "suffix": path.suffix,
            "size_bytes": path.stat().st_size,
            "priority": "statsapi_schedule_cache" if "statsapi_cache/schedule" in str(path) else "generic",
            "passed": True,
        }
        for path in candidate_files
    ]

    candidate_score_rows = [score_candidate(path) for path in candidate_files]
    valid_candidates = [row for row in candidate_score_rows if boolish(row.get("passed"))]
    valid_candidates.sort(
        key=lambda row: (
            "statsapi_cache/schedule" in str(row.get("path", "")),
            row.get("sample_classification") == "real_evaluation_candidate",
            int(row.get("normalized_row_count", 0) or 0),
            int(row.get("date_span_days", 0) or 0),
            int(row.get("score", 0) or 0),
        ),
        reverse=True,
    )

    selected_score = valid_candidates[0] if valid_candidates else {}
    selected_source_path = selected_score.get("path", "")
    selected_rows: list[dict[str, Any]] = []
    selected_mapping: dict[str, str | None] = {}
    normalized_rows: list[dict[str, Any]] = []
    invalid_rows: list[dict[str, Any]] = []
    selected_date_span_days = 0

    if selected_source_path:
        selected_path = Path(str(selected_source_path))
        selected_rows = load_rows(selected_path)
        selected_mapping = map_schema(list(selected_rows[0].keys()) if selected_rows else [])
        normalized_rows, invalid_rows, selected_date_span_days = normalize_rows(selected_path, selected_rows, selected_mapping)

    target_created = False
    if normalized_rows:
        TARGET_ACTUALS.parent.mkdir(parents=True, exist_ok=True)
        write_csv(TARGET_ACTUALS, normalized_rows)
        target_created = TARGET_ACTUALS.exists()

    rerun_6na_exit_code: int | None = None
    rerun_6na_json: dict[str, Any] = {}
    if target_created:
        rerun_6na_exit_code, rerun_6na_json = run_6na()

    selected_source_row_count = len(normalized_rows)
    sample_classification = "real_evaluation_candidate" if selected_date_span_days > 21 and selected_source_row_count >= 100 else "smoke_test_only"
    sufficient_for_real_historical_evaluation = sample_classification == "real_evaluation_candidate"

    schema_mapping_rows = [
        {
            "canonical_field": canonical,
            "matched_source_column": selected_mapping.get(canonical) or "",
            "passed": bool(selected_mapping.get(canonical)) or canonical in {"home_win_binary", "status"},
        }
        for canonical in ["game_pk", "game_date", "home_team", "away_team", "home_score", "away_score", "home_win_binary", "status"]
    ]

    selected_source_rows = [
        {
            "selected_source_path": selected_source_path,
            "selected_source_row_count": selected_source_row_count,
            "selected_source_date_span_days": selected_date_span_days,
            "target_actuals_output_path": str(TARGET_ACTUALS),
            "target_actuals_file_created": target_created,
            "passed": target_created,
        }
    ]

    provenance_rows = [
        {
            "target_actuals_output_path": str(TARGET_ACTUALS),
            "source_artifact": selected_source_path,
            "source_family": "repo_existing_local_raw_results" if selected_source_path else "",
            "rows_created": selected_source_row_count,
            "passed": target_created,
        }
    ]

    sufficiency_rows = [
        {
            "sample_classification": sample_classification if target_created else "no_valid_source",
            "selected_source_row_count": selected_source_row_count,
            "selected_source_date_span_days": selected_date_span_days,
            "sufficient_for_real_historical_evaluation": sufficient_for_real_historical_evaluation,
            "smoke_test_only_allowed_for_6na_validation": target_created and not sufficient_for_real_historical_evaluation,
            "passed": target_created,
        }
    ]

    rerun_rows = [
        {
            "command": f"{sys.executable} {SCRIPT_6NA}",
            "rerun_6na_exit_code": rerun_6na_exit_code if rerun_6na_exit_code is not None else "",
            "rerun_6na_all_checks_passed": rerun_6na_json.get("all_checks_passed", ""),
            "rerun_6na_diagnosis": rerun_6na_json.get("diagnosis", ""),
            "passed": rerun_6na_exit_code == 0 and rerun_6na_json.get("all_checks_passed") is True,
        }
    ]

    moneyline_boundary_rows = [
        {"boundary": "historical_moneyline_validation", "status": "deferred", "passed": True},
        {"boundary": "market_comparison_metrics", "status": "blocked", "passed": True},
        {"boundary": "roi_clv_market_edge_claims", "status": "blocked", "passed": True},
        {"boundary": "actuals_only_metrics", "status": "blocked_until_6ne_audit_and_followup_plan", "passed": True},
    ]

    complete = (
        all_passed(predecessor_rows)
        and all_passed(input_rows)
        and target_created
        and selected_source_row_count > 0
        and rerun_6na_exit_code == 0
        and rerun_6na_json.get("all_checks_passed") is True
    )
    blocked = not complete
    diagnosis = DIAGNOSIS_COMPLETE if complete else DIAGNOSIS_BLOCKED
    recommended_next_layer = RECOMMENDED_NEXT_COMPLETE if complete else ""
    recommended_path = RECOMMENDED_PATH_COMPLETE if complete else RECOMMENDED_PATH_BLOCKED

    allowed_next_rows = [
        {
            "operation": "audit_created_historical_actuals_source",
            "allowed_next": complete,
            "scope": "6NE audit only" if complete else "blocked_until_valid_local_actuals_source_exists",
            "passed": True,
        }
    ]

    forbidden_next_rows = [
        {"operation": "source_ingestion", "allowed_next": False, "passed": True},
        {"operation": "metric_execution", "allowed_next": False, "passed": True},
        {"operation": "historical_backtest", "allowed_next": False, "passed": True},
        {"operation": "tuning", "allowed_next": False, "passed": True},
        {"operation": "mechanics_activation", "allowed_next": False, "passed": True},
        {"operation": "layer_6_exit", "allowed_next": False, "passed": True},
        {"operation": "live_data_fetches", "allowed_next": False, "passed": True},
        {"operation": "remote_api_calls", "allowed_next": False, "passed": True},
        {"operation": "production_table_creation", "allowed_next": False, "passed": True},
        {"operation": "real_historical_evaluation_claims_if_smoke_test_only", "allowed_next": False, "passed": True},
    ]

    future_6ne_rows = [
        {"contract": "audit_created_actuals_source", "required_if_complete": True, "passed": True},
        {"contract": "audit_6na_rerun_passed", "required_if_complete": True, "passed": True},
        {"contract": "audit_sample_sufficiency_classification", "required_if_complete": True, "passed": True},
        {"contract": "preserve_no_metrics_backtests_tuning_activation_exit", "required_if_complete": True, "passed": True},
    ]

    decision_rows = [
        {"decision": "6nc_passed", "expected": True, "actual": json_6nc.get("all_checks_passed"), "passed": json_6nc.get("all_checks_passed") is True},
        {"decision": "all_required_6nc_artifacts_exist", "expected": True, "actual": all_passed(input_rows), "passed": all_passed(input_rows)},
        {"decision": "candidate_files_found", "expected": ">0", "actual": len(candidate_files), "passed": len(candidate_files) > 0},
        {"decision": "valid_candidate_selected", "expected": True, "actual": bool(selected_source_path), "passed": bool(selected_source_path)},
        {"decision": "target_actuals_file_created", "expected": True, "actual": target_created, "passed": target_created},
        {"decision": "source_rows_created_positive", "expected": True, "actual": selected_source_row_count > 0, "passed": selected_source_row_count > 0},
        {"decision": "rerun_6na_passed", "expected": True, "actual": rerun_6na_json.get("all_checks_passed"), "passed": rerun_6na_json.get("all_checks_passed") is True},
        {"decision": "sample_sufficiency_classified", "expected": True, "actual": bool(sample_classification), "passed": bool(sample_classification)},
        {"decision": "do_not_execute_metrics_backtest_tune_activate_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "complete", "expected": True, "actual": complete, "passed": complete},
        {"decision": "blocked", "expected": False, "actual": blocked, "passed": not blocked},
    ]

    safety_rows = [
        {"boundary": "source_creation_historical_actuals_validation_only", "expected": True, "actual": True, "passed": True},
        {"boundary": "local_candidate_files_checked_by_6nd", "expected": True, "actual": True, "passed": True},
        {"boundary": "local_candidate_files_read_by_6nd", "expected": bool(candidate_files), "actual": bool(candidate_files), "passed": True},
        {"boundary": "source_rows_ingested_by_6nd", "expected": False, "actual": False, "passed": True},
        {"boundary": "normalized_source_tables_created_for_production_by_6nd", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_code_modified_by_6nd", "expected": False, "actual": False, "passed": True},
        {"boundary": "adapter_call_executed_by_6nd", "expected": False, "actual": False, "passed": True},
        {"boundary": "metric_execution_run_by_6nd", "expected": False, "actual": False, "passed": True},
        {"boundary": "backtest_execution_run_by_6nd", "expected": False, "actual": False, "passed": True},
        {"boundary": "full_batch_adapter_call_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "real_historical_evaluation_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "production_simulations_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "activation_execution_allowed_after_this_layer", "expected": False, "actual": False, "passed": True},
        {"boundary": "mechanics_activated_by_this_layer", "expected": False, "actual": False, "passed": True},
        {"boundary": "layer_6_exit_recommended", "expected": False, "actual": False, "passed": True},
        {"boundary": "layer_6_exit_credit", "expected": False, "actual": False, "passed": True},
        {"boundary": "database_writes_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "live_data_fetches_run", "expected": False, "actual": False, "passed": True},
        {"boundary": "remote_api_calls_run", "expected": False, "actual": False, "passed": True},
    ]

    recommended_rows = [
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_COMPLETE if complete else "", "actual": recommended_next_layer, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH_COMPLETE if complete else RECOMMENDED_PATH_BLOCKED, "actual": recommended_path, "passed": True},
        {"decision": "do_not_recommend_metric_execution", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_real_backtest", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_tuning", "expected": True, "actual": True, "passed": True},
        {"decision": "do_not_recommend_activation_or_exit", "expected": True, "actual": True, "passed": True},
        {"decision": "diagnosis", "expected": DIAGNOSIS_COMPLETE if complete else DIAGNOSIS_BLOCKED, "actual": diagnosis, "passed": True},
    ]

    checks = [
        {"check": "predecessor", "passed": all_passed(predecessor_rows), "detail": f"{sum(1 for r in predecessor_rows if r['passed'])}/{len(predecessor_rows)}"},
        {"check": "input_artifacts", "passed": all_passed(input_rows), "detail": f"{sum(1 for r in input_rows if r['passed'])}/{len(input_rows)}"},
        {"check": "candidate_files", "passed": len(candidate_files) > 0, "detail": f"candidate_files={len(candidate_files)}"},
        {"check": "candidate_scores", "passed": bool(valid_candidates), "detail": f"valid_candidates={len(valid_candidates)}"},
        {"check": "selected_source", "passed": bool(selected_source_path), "detail": selected_source_path},
        {"check": "schema_mapping", "passed": all_passed(schema_mapping_rows), "detail": f"{sum(1 for r in schema_mapping_rows if r['passed'])}/{len(schema_mapping_rows)}"},
        {"check": "target_actuals_created", "passed": target_created, "detail": str(TARGET_ACTUALS)},
        {"check": "sample_sufficiency", "passed": bool(sample_classification), "detail": sample_classification},
        {"check": "rerun_6na", "passed": rerun_6na_exit_code == 0 and rerun_6na_json.get("all_checks_passed") is True, "detail": f"exit={rerun_6na_exit_code}"},
        {"check": "moneyline_deferral_boundaries", "passed": all_passed(moneyline_boundary_rows), "detail": f"{sum(1 for r in moneyline_boundary_rows if r['passed'])}/{len(moneyline_boundary_rows)}"},
        {"check": "allowed_operations_next", "passed": all_passed(allowed_next_rows), "detail": f"{sum(1 for r in allowed_next_rows if r['passed'])}/{len(allowed_next_rows)}"},
        {"check": "forbidden_operations_next", "passed": all_passed(forbidden_next_rows), "detail": f"{sum(1 for r in forbidden_next_rows if r['passed'])}/{len(forbidden_next_rows)}"},
        {"check": "future_6ne_contract", "passed": all_passed(future_6ne_rows), "detail": f"{sum(1 for r in future_6ne_rows if r['passed'])}/{len(future_6ne_rows)}"},
        {"check": "decision", "passed": all_passed(decision_rows), "detail": f"{sum(1 for r in decision_rows if r['passed'])}/{len(decision_rows)}"},
        {"check": "safety_boundaries", "passed": all_passed(safety_rows), "detail": f"{sum(1 for r in safety_rows if r['passed'])}/{len(safety_rows)}"},
        {"check": "recommended_path", "passed": all_passed(recommended_rows), "detail": f"{sum(1 for r in recommended_rows if r['passed'])}/{len(recommended_rows)}"},
    ]

    all_checks_passed = complete

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "predecessor": write_csv(PREDECESSOR_CSV, predecessor_rows),
        "input_artifacts": write_csv(INPUT_ARTIFACTS_CSV, input_rows),
        "candidate_files": write_csv(CANDIDATE_FILES_CSV, candidate_file_rows),
        "candidate_scores": write_csv(CANDIDATE_SCORES_CSV, candidate_score_rows),
        "selected_source": write_csv(SELECTED_SOURCE_CSV, selected_source_rows),
        "schema_mapping": write_csv(SCHEMA_MAPPING_CSV, schema_mapping_rows),
        "normalized_sample": write_csv(NORMALIZED_SAMPLE_CSV, normalized_rows[:25]),
        "source_provenance": write_csv(SOURCE_PROVENANCE_CSV, provenance_rows),
        "sample_sufficiency": write_csv(SAMPLE_SUFFICIENCY_CSV, sufficiency_rows),
        "rerun_6na_summary": write_csv(RERUN_6NA_CSV, rerun_rows),
        "moneyline_deferral_boundaries": write_csv(MONEYLINE_BOUNDARIES_CSV, moneyline_boundary_rows),
        "allowed_operations_next": write_csv(ALLOWED_NEXT_CSV, allowed_next_rows),
        "forbidden_operations_next": write_csv(FORBIDDEN_NEXT_CSV, forbidden_next_rows),
        "future_6ne_contract": write_csv(FUTURE_6NE_CSV, future_6ne_rows),
        "decision": write_csv(DECISION_CSV, decision_rows),
        "safety_boundaries": write_csv(SAFETY_CSV, safety_rows),
        "recommended_path": write_csv(RECOMMENDED_CSV, recommended_rows),
    }

    summary = {
        "layer": "6ND",
        "layer_type": "game_mechanics_realism",
        "source_creation_historical_actuals_validation_only": True,
        "all_checks_passed": all_checks_passed,
        "blocked": blocked,
        "diagnosis": diagnosis,
        "recommended_next_layer": recommended_next_layer,
        "recommended_path": recommended_path,
        "predecessor_layer": "6NC",
        "predecessor_diagnosis": json_6nc.get("diagnosis"),
        "predecessor_all_checks_passed": json_6nc.get("all_checks_passed") is True,
        "source_family": "historical_actuals_source_creation",
        "local_candidate_files_checked_by_6nd": True,
        "local_candidate_files_read_by_6nd": bool(candidate_files),
        "target_actuals_file_created_by_6nd": target_created,
        "target_actuals_output_path": str(TARGET_ACTUALS),
        "source_rows_created_by_6nd": selected_source_row_count,
        "source_rows_ingested_by_6nd": False,
        "normalized_source_tables_created_for_production_by_6nd": False,
        "production_code_modified_by_6nd": False,
        "adapter_call_executed_by_6nd": False,
        "reran_6na_validation": target_created,
        "rerun_6na_exit_code": rerun_6na_exit_code,
        "rerun_6na_all_checks_passed": rerun_6na_json.get("all_checks_passed"),
        "selected_source_path": selected_source_path,
        "selected_source_row_count": selected_source_row_count,
        "selected_source_date_span_days": selected_date_span_days,
        "sample_classification": sample_classification if target_created else "no_valid_source",
        "sufficient_for_real_historical_evaluation": sufficient_for_real_historical_evaluation,
        "metric_execution_allowed_next": False,
        "metric_execution_run_by_6nd": False,
        "backtest_execution_allowed_next": False,
        "backtest_execution_run_by_6nd": False,
        "full_batch_adapter_call_run": False,
        "real_historical_evaluation_run": False,
        "production_simulations_run": False,
        "activation_execution_allowed_after_this_layer": False,
        "mechanics_activated_by_this_layer": False,
        "layer_6_exit_recommended": False,
        "layer_6_exit_credit": False,
        "database_writes_run": False,
        "live_data_fetches_run": False,
        "remote_api_calls_run": False,
        "games_evaluated": 0,
        "moneyline_deferral_boundaries_preserved": True,
        "historical_actuals_source_creation_audit_allowed_next": complete,
        "compile_errors": compile_errors,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "predecessor_csv": str(PREDECESSOR_CSV),
            "input_artifacts_csv": str(INPUT_ARTIFACTS_CSV),
            "candidate_files_csv": str(CANDIDATE_FILES_CSV),
            "candidate_scores_csv": str(CANDIDATE_SCORES_CSV),
            "selected_source_csv": str(SELECTED_SOURCE_CSV),
            "schema_mapping_csv": str(SCHEMA_MAPPING_CSV),
            "normalized_sample_csv": str(NORMALIZED_SAMPLE_CSV),
            "source_provenance_csv": str(SOURCE_PROVENANCE_CSV),
            "sample_sufficiency_csv": str(SAMPLE_SUFFICIENCY_CSV),
            "rerun_6na_summary_csv": str(RERUN_6NA_CSV),
            "moneyline_deferral_boundaries_csv": str(MONEYLINE_BOUNDARIES_CSV),
            "allowed_operations_next_csv": str(ALLOWED_NEXT_CSV),
            "forbidden_operations_next_csv": str(FORBIDDEN_NEXT_CSV),
            "future_6ne_contract_csv": str(FUTURE_6NE_CSV),
            "decision_csv": str(DECISION_CSV),
            "safety_boundaries_csv": str(SAFETY_CSV),
            "recommended_path_csv": str(RECOMMENDED_CSV),
        },
    }

    JSON_PATH.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if complete else 1


if __name__ == "__main__":
    raise SystemExit(main())
