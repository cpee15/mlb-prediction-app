from __future__ import annotations

import csv
import json
import os
import sqlite3
from pathlib import Path
from typing import Any, Dict, List

from mlb_app.database import get_engine, get_session
from mlb_app.matchup_generator import generate_matchups_for_date


BACKTEST_START = os.getenv("BACKTEST_START", "2026-04-20")
BACKTEST_END = os.getenv("BACKTEST_END", "2026-05-03")
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///mlb.db")


KEYWORDS = [
    "score",
    "run",
    "result",
    "status",
    "final",
    "winner",
    "loser",
    "home",
    "away",
    "game_pk",
    "game_date",
]


def daterange(start: str, end: str):

    import datetime as dt

    current = dt.date.fromisoformat(start)
    stop = dt.date.fromisoformat(end)

    while current <= stop:
        yield current.isoformat()
        current += dt.timedelta(days=1)


def write_csv(path: Path, rows: List[Dict[str, Any]]):

    if not rows:
        path.write_text("")
        return

    fieldnames = sorted({
        key
        for row in rows
        for key in row.keys()
    })

    with path.open("w", newline="") as handle:

        writer = csv.DictWriter(
            handle,
            fieldnames=fieldnames,
        )

        writer.writeheader()
        writer.writerows(rows)


def flatten(obj: Any, prefix: str = "") -> Dict[str, Any]:

    out = {}

    if isinstance(obj, dict):

        for key, value in obj.items():

            path = f"{prefix}.{key}" if prefix else key

            out[path] = value

            if isinstance(value, (dict, list)):
                out.update(flatten(value, path))

    elif isinstance(obj, list):

        for idx, value in enumerate(obj[:5]):

            path = f"{prefix}[{idx}]"

            out[path] = value

            if isinstance(value, (dict, list)):
                out.update(flatten(value, path))

    return out


def main():

    sqlite_path = DATABASE_URL.replace(
        "sqlite:///",
        "",
    )

    conn = sqlite3.connect(sqlite_path)

    cursor = conn.cursor()

    table_rows = []
    column_rows = []
    sample_rows = []
    matchup_rows = []
    cache_rows = []

    likely_tables = []

    tables = cursor.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type='table'
        ORDER BY name
        """
    ).fetchall()

    table_names = [row[0] for row in tables]

    for table_name in table_names:

        try:

            columns = cursor.execute(
                f"PRAGMA table_info({table_name})"
            ).fetchall()

            col_names = [col[1] for col in columns]

            row_count = cursor.execute(
                f"SELECT COUNT(*) FROM {table_name}"
            ).fetchone()[0]

            matching_cols = [
                col
                for col in col_names
                if any(
                    keyword in col.lower()
                    for keyword in KEYWORDS
                )
            ]

            table_rows.append(
                {
                    "table_name": table_name,
                    "row_count": row_count,
                    "matching_columns": ",".join(
                        matching_cols
                    ),
                }
            )

            if matching_cols:

                likely_tables.append(table_name)

            for col in col_names:

                if any(
                    keyword in col.lower()
                    for keyword in KEYWORDS
                ):

                    column_rows.append(
                        {
                            "table_name": table_name,
                            "column_name": col,
                        }
                    )

            lower_cols = {
                col.lower()
                for col in col_names
            }

            has_date = any(
                "date" in col
                for col in lower_cols
            )

            has_score = any(
                (
                    "score" in col
                    or "run" in col
                )
                for col in lower_cols
            )

            if has_date and has_score:

                try:

                    result = cursor.execute(
                        f"""
                        SELECT *
                        FROM {table_name}
                        LIMIT 5
                        """
                    ).fetchall()

                    for row in result:

                        sample_rows.append(
                            {
                                "table_name": table_name,
                                "sample_row": str(row)[:2000],
                            }
                        )

                except Exception:
                    pass

            if (
                "cache" in table_name.lower()
                or "statsapi" in table_name.lower()
            ):

                cache_rows.append(
                    {
                        "table_name": table_name,
                        "row_count": row_count,
                    }
                )

        except Exception as exc:

            table_rows.append(
                {
                    "table_name": table_name,
                    "error": str(exc),
                }
            )

    conn.close()

    engine = get_engine(DATABASE_URL)
    SessionLocal = get_session(engine)

    with SessionLocal() as session:

        for game_date in daterange(
            BACKTEST_START,
            BACKTEST_END,
        ):

            try:

                matchups = generate_matchups_for_date(
                    session,
                    game_date,
                )

            except Exception:
                continue

            for matchup in matchups[:5]:

                flat = flatten(matchup)

                for path, value in flat.items():

                    lower = path.lower()

                    if any(
                        keyword in lower
                        for keyword in KEYWORDS
                    ):

                        if isinstance(
                            value,
                            (
                                int,
                                float,
                                str,
                                bool,
                                type(None),
                            ),
                        ):

                            matchup_rows.append(
                                {
                                    "game_date": game_date,
                                    "path": path,
                                    "value": value,
                                }
                            )

    diagnosis = "actual_result_source_unclear"
    recommended_source = None

    score_tables = []

    for row in sample_rows:

        text = row["sample_row"].lower()

        if any(
            token in text
            for token in [
                "final",
                "score",
                "runs",
            ]
        ):

            score_tables.append(
                row["table_name"]
            )

    if score_tables:

        diagnosis = "actual_scores_available_in_db"

        recommended_source = score_tables[0]

    elif matchup_rows:

        if any(
            (
                "score" in row["path"].lower()
                or "run" in row["path"].lower()
            )
            for row in matchup_rows
        ):

            diagnosis = (
                "actual_scores_available_under_different_matchup_keys"
            )

            recommended_source = (
                "matchup_payload"
            )

    elif cache_rows:

        diagnosis = (
            "actual_scores_available_in_cache"
        )

        recommended_source = (
            cache_rows[0]["table_name"]
        )

    payload = {
        "tables_inspected": len(table_names),
        "likely_result_tables": likely_tables[:25],
        "likely_score_columns": column_rows[:100],
        "backtest_rows_with_scores": len(
            sample_rows
        ),
        "sample_actual_result_rows": sample_rows[:25],
        "matchup_actual_score_paths": matchup_rows[:100],
        "cache_result_sources": cache_rows,
        "recommended_actual_result_source": recommended_source,
        "diagnosis": diagnosis,
        "recommended_next_step": (
            "Patch calibration harness "
            "to consume discovered source."
        ),
    }

    out_dir = Path("tmp")
    out_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    prefix = (
        f"actual_result_sources_"
        f"{BACKTEST_START}_to_{BACKTEST_END}"
    )

    json_path = out_dir / f"{prefix}.json"

    json_path.write_text(
        json.dumps(
            payload,
            indent=2,
            sort_keys=True,
        )
    )

    write_csv(
        out_dir / f"{prefix}_tables.csv",
        table_rows,
    )

    write_csv(
        out_dir / f"{prefix}_columns.csv",
        column_rows,
    )

    write_csv(
        out_dir / f"{prefix}_sample_rows.csv",
        sample_rows,
    )

    write_csv(
        out_dir / f"{prefix}_matchup_paths.csv",
        matchup_rows,
    )

    write_csv(
        out_dir / f"{prefix}_cache_sources.csv",
        cache_rows,
    )

    print(
        json.dumps(
            payload,
            indent=2,
            sort_keys=True,
        )
    )

    print(f"Wrote {json_path}")


if __name__ == "__main__":
    main()
