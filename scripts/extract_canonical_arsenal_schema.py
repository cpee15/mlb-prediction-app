from __future__ import annotations

import csv
import json
import os
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from mlb_app.matchup_generator import generate_matchups_for_date


DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///mlb.db")
BACKTEST_START = os.getenv("BACKTEST_START", "2026-04-20")
MAX_GAMES = int(os.getenv("MAX_GAMES", "5"))


PITCH_TYPE_MAP = {
    "FF": "four_seam",
    "FA": "four_seam",
    "SI": "sinker",
    "FT": "sinker",
    "SL": "slider",
    "CU": "curveball",
    "CH": "changeup",
    "KC": "knuckle_curve",
    "FC": "cutter",
}


PITCH_METRIC_MAP = {
    "usage_pct": "canonical_usage_pct",
    "whiff_pct": "canonical_whiff_pct",
    "strikeout_pct": "canonical_strikeout_pct",
    "rv_per_100": "canonical_rv_per_100",
    "xwoba": "canonical_xwoba",
    "xba": "canonical_xba",
    "hard_hit_pct": "canonical_hard_hit_pct",
}


FEATURE_METRIC_MAP = {
    "xwoba": "canonical_xwoba",
    "xba": "canonical_xba",
    "hard_hit_pct": "canonical_hard_hit_pct",
}


CANONICAL_COLUMNS = [
    "game_pk",
    "game_date",
    "team_side",
    "role",
    "raw_pitch_type",
    "canonical_pitch_type",
    "canonical_usage_pct",
    "canonical_whiff_pct",
    "canonical_strikeout_pct",
    "canonical_rv_per_100",
    "canonical_xwoba",
    "canonical_xba",
    "canonical_hard_hit_pct",
    "canonical_pitch_mix_weight",
    "canonical_damage_proxy",
    "validation_flags",
]


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        path.write_text("")
        return

    fields = sorted({key for row in rows for key in row.keys()})

    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def normalize_pitch_type(raw_pitch_type: Optional[str]) -> tuple[str, List[str]]:
    flags = []

    if raw_pitch_type is None:
        return "", ["missing_pitch_type"]

    raw = str(raw_pitch_type).upper()

    if raw in PITCH_TYPE_MAP:
        return PITCH_TYPE_MAP[raw], flags

    flags.append("unknown_pitch_type")
    return raw.lower(), flags


def normalize_percent(value: Any) -> tuple[Optional[float], List[str]]:
    flags = []

    if value is None:
        return None, ["missing_value"]

    try:
        val = float(value)
    except (TypeError, ValueError):
        return None, ["non_numeric_value"]

    if 0.0 <= val <= 1.0:
        return val, flags

    if 1.0 < val <= 100.0:
        flags.append("converted_percent_scale")
        return val / 100.0, flags

    flags.append("invalid_percent_scale")
    return None, flags


def normalize_rv(value: Any) -> tuple[Optional[float], List[str]]:
    if value is None:
        return None, ["missing_value"]

    try:
        return float(value), []
    except (TypeError, ValueError):
        return None, ["non_numeric_value"]


def damage_proxy(row: Dict[str, Any]) -> Optional[float]:
    components = []

    rv = row.get("canonical_rv_per_100")
    xwoba = row.get("canonical_xwoba")
    hard_hit = row.get("canonical_hard_hit_pct")
    xba = row.get("canonical_xba")

    if rv is not None:
        # Compress run value into a small comparable proxy contribution.
        components.append(float(rv) / 10.0)

    if xwoba is not None:
        components.append(float(xwoba))

    if hard_hit is not None:
        components.append(float(hard_hit))

    if xba is not None:
        components.append(float(xba))

    if not components:
        return None

    return sum(components) / len(components)


def blank_row(
    *,
    game_pk: Any,
    game_date: Any,
    team_side: str,
    role: str,
    raw_pitch_type: str = "",
) -> Dict[str, Any]:
    row = {column: None for column in CANONICAL_COLUMNS}

    row["game_pk"] = game_pk
    row["game_date"] = game_date
    row["team_side"] = team_side
    row["role"] = role
    row["raw_pitch_type"] = raw_pitch_type

    canonical_pitch, flags = normalize_pitch_type(raw_pitch_type) if raw_pitch_type else ("", [])

    row["canonical_pitch_type"] = canonical_pitch
    row["validation_flags"] = "|".join(flags)

    return row


def append_flags(row: Dict[str, Any], flags: List[str]) -> None:
    existing = row.get("validation_flags") or ""
    current = [x for x in existing.split("|") if x]
    current.extend(flags)
    row["validation_flags"] = "|".join(sorted(set(current)))


def extract_pitch_arsenal_rows(
    matchup: Dict[str, Any],
    team_side: str,
) -> List[Dict[str, Any]]:
    key = f"{team_side}_pitch_arsenal"
    arsenal = matchup.get(key) or {}

    if not isinstance(arsenal, dict):
        return []

    rows = []

    for raw_pitch_type, metrics in arsenal.items():
        if not isinstance(metrics, dict):
            continue

        row = blank_row(
            game_pk=matchup.get("game_pk"),
            game_date=matchup.get("game_date"),
            team_side=team_side,
            role="starter_arsenal",
            raw_pitch_type=str(raw_pitch_type),
        )

        for raw_metric, target_field in PITCH_METRIC_MAP.items():
            if raw_metric not in metrics:
                continue

            if raw_metric == "rv_per_100":
                value, flags = normalize_rv(metrics.get(raw_metric))
            else:
                value, flags = normalize_percent(metrics.get(raw_metric))

            row[target_field] = value
            append_flags(row, flags)

        row["canonical_pitch_mix_weight"] = row.get("canonical_usage_pct")
        row["canonical_damage_proxy"] = damage_proxy(row)

        rows.append(row)

    return rows


def extract_pitcher_feature_row(
    matchup: Dict[str, Any],
    team_side: str,
) -> List[Dict[str, Any]]:
    key = f"{team_side}_pitcher_features"
    features = matchup.get(key) or {}

    if not isinstance(features, dict):
        return []

    row = blank_row(
        game_pk=matchup.get("game_pk"),
        game_date=matchup.get("game_date"),
        team_side=team_side,
        role="pitcher_features",
        raw_pitch_type="aggregate",
    )

    for raw_metric, target_field in FEATURE_METRIC_MAP.items():
        if raw_metric not in features:
            continue

        value, flags = normalize_percent(features.get(raw_metric))
        row[target_field] = value
        append_flags(row, flags)

    row["canonical_damage_proxy"] = damage_proxy(row)

    return [row]


def load_matchups() -> List[Dict[str, Any]]:
    engine = create_engine(DATABASE_URL)
    SessionLocal = sessionmaker(bind=engine)

    with SessionLocal() as session:
        matchups = generate_matchups_for_date(
            session,
            BACKTEST_START,
        )

    if not matchups:
        raise RuntimeError("No real matchups returned")

    sliced = matchups[:MAX_GAMES]

    for matchup in sliced:
        if not isinstance(matchup, dict):
            raise RuntimeError("Non-dict matchup payload returned")

        if "_fallback_error" in matchup:
            raise RuntimeError("Fallback payload detected")

    return sliced


def summarize(rows: List[Dict[str, Any]], games_scanned: int) -> Dict[str, Any]:
    coverage = {}

    for field in CANONICAL_COLUMNS:
        if field in {
            "game_pk",
            "game_date",
            "team_side",
            "role",
            "raw_pitch_type",
            "canonical_pitch_type",
            "validation_flags",
        }:
            continue

        present = sum(1 for row in rows if row.get(field) is not None)
        coverage[field] = {
            "present_count": present,
            "coverage_rate": round(present / max(len(rows), 1), 3),
        }

    flags = Counter()

    for row in rows:
        for flag in str(row.get("validation_flags") or "").split("|"):
            if flag:
                flags[flag] += 1

    required_sets = {
        "pit_k_shadow": [
            "canonical_whiff_pct",
            "canonical_usage_pct",
            "canonical_strikeout_pct",
        ],
        "pit_xwoba_shadow": [
            "canonical_xwoba",
            "canonical_rv_per_100",
        ],
        "pit_hard_hit_shadow": [
            "canonical_hard_hit_pct",
            "canonical_xwoba",
        ],
        "pit_hr_shadow": [
            "canonical_damage_proxy",
        ],
    }

    readiness = []

    for composite, fields in required_sets.items():
        available = {
            field: coverage.get(field, {}).get("coverage_rate", 0.0)
            for field in fields
        }

        if all(rate > 0 for rate in available.values()):
            status = "ready"
        elif any(rate > 0 for rate in available.values()):
            status = "partial"
        else:
            status = "not_ready"

        readiness.append(
            {
                "shadow_composite": composite,
                "status": status,
                "field_coverage": json.dumps(available),
            }
        )

    ready_count = sum(1 for item in readiness if item["status"] == "ready")
    partial_count = sum(1 for item in readiness if item["status"] == "partial")

    if rows and ready_count >= 2 and not flags.get("invalid_percent_scale"):
        diagnosis = "canonical_arsenal_extraction_ready_for_shadow_calculators"
    elif rows and (ready_count > 0 or partial_count > 0):
        diagnosis = "canonical_arsenal_extraction_partial_needs_mapping_work"
    else:
        diagnosis = "canonical_arsenal_extraction_failed_real_payloads"

    return {
        "diagnosis": diagnosis,
        "loader_mode": "real_generate_matchups_for_date",
        "used_fallback": False,
        "games_scanned": games_scanned,
        "rows_extracted": len(rows),
        "coverage": coverage,
        "validation_flags": dict(flags),
        "shadow_composite_readiness": readiness,
    }


def main() -> None:
    matchups = load_matchups()

    rows = []

    for matchup in matchups:
        for team_side in ["home", "away"]:
            rows.extend(
                extract_pitch_arsenal_rows(
                    matchup,
                    team_side,
                )
            )
            rows.extend(
                extract_pitcher_feature_row(
                    matchup,
                    team_side,
                )
            )

    summary = summarize(
        rows,
        games_scanned=len(matchups),
    )

    out_dir = Path("tmp")
    out_dir.mkdir(exist_ok=True)

    json_path = out_dir / "canonical_arsenal_schema.json"
    rows_csv = out_dir / "canonical_arsenal_rows.csv"
    summary_csv = out_dir / "canonical_arsenal_validation_summary.csv"

    json_path.write_text(json.dumps(summary, indent=2))
    write_csv(rows_csv, rows)
    write_csv(summary_csv, summary["shadow_composite_readiness"])

    print(json.dumps(summary, indent=2))
    print(f"Wrote {json_path}")
    print(f"Wrote {rows_csv}")
    print(f"Wrote {summary_csv}")


if __name__ == "__main__":
    main()
