from __future__ import annotations

import csv
import json
import os
import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List


BACKTEST_START = os.getenv("BACKTEST_START", "2026-04-20")
BACKTEST_END = os.getenv("BACKTEST_END", "2026-05-03")
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///mlb.db")


SCORE_TERMS = [
    "score",
    "bat_score",
    "fld_score",
    "post_bat_score",
    "post_fld_score",
]

RUN_TERMS = [
    "run",
    "rbi",
    "earned",
]

TEAM_TERMS = [
    "team",
    "inning",
    "topbot",
    "home",
    "away",
]


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


def daterange(start: str, end: str):

    import datetime as dt

    current = dt.date.fromisoformat(start)
    stop = dt.date.fromisoformat(end)

    while current <= stop:
        yield current.isoformat()
        current += dt.timedelta(days=1)


def safe_int(value):

    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def main():

    sqlite_path = DATABASE_URL.replace(
        "sqlite:///",
        "",
    )

    conn = sqlite3.connect(sqlite_path)

    conn.row_factory = sqlite3.Row

    cursor = conn.cursor()

    columns = cursor.execute(
        """
        PRAGMA table_info(statcast_events)
        """
    ).fetchall()

    column_names = [
        row["name"]
        for row in columns
    ]

    score_like_columns = [
        col
        for col in column_names
        if any(
            term in col.lower()
            for term in SCORE_TERMS
        )
    ]

    run_like_columns = [
        col
        for col in column_names
        if any(
            term in col.lower()
            for term in RUN_TERMS
        )
    ]

    team_like_columns = [
        col
        for col in column_names
        if any(
            term in col.lower()
            for term in TEAM_TERMS
        )
    ]

    games_query = """
    SELECT DISTINCT game_pk
    FROM statcast_events
    WHERE game_date >= ?
      AND game_date <= ?
      AND game_pk IS NOT NULL
    """

    game_rows = cursor.execute(
        games_query,
        (
            BACKTEST_START,
            BACKTEST_END,
        ),
    ).fetchall()

    game_pks = sorted({
        row["game_pk"]
        for row in game_rows
    })

    derived_rows = []
    compare_rows = []
    reconstruction_rows = []

    direct_score_games = 0
    reconstructable_games = 0

    winner_matches = 0
    winner_total = 0

    for game_pk in game_pks[:500]:

        rows = cursor.execute(
            """
            SELECT *
            FROM statcast_events
            WHERE game_pk = ?
            ORDER BY
                game_date,
                at_bat_number,
                pitch_number
            """,
            (game_pk,),
        ).fetchall()

        if not rows:
            continue

        latest = rows[-1]

        found_direct = False

        derived_home = None
        derived_away = None

        direct_home_candidates = [
            "home_score",
            "post_home_score",
            "fld_score",
        ]

        direct_away_candidates = [
            "away_score",
            "post_away_score",
            "bat_score",
        ]

        for home_col in direct_home_candidates:

            if home_col in column_names:

                val = safe_int(latest[home_col])

                if val is not None:
                    derived_home = val
                    found_direct = True
                    break

        for away_col in direct_away_candidates:

            if away_col in column_names:

                val = safe_int(latest[away_col])

                if val is not None:
                    derived_away = val
                    found_direct = True
                    break

        if found_direct:
            direct_score_games += 1

        reconstructed_runs = 0

        for row in rows:

            rbi = None

            if "rbi" in column_names:
                rbi = safe_int(row["rbi"])

            if rbi:
                reconstructed_runs += rbi

        if reconstructed_runs > 0:
            reconstructable_games += 1

        derived_rows.append(
            {
                "game_pk": game_pk,
                "rows": len(rows),
                "derived_home_score": derived_home,
                "derived_away_score": derived_away,
                "found_direct_scores": found_direct,
                "reconstructed_runs": reconstructed_runs,
            }
        )

        reconstruction_rows.append(
            {
                "game_pk": game_pk,
                "reconstructed_runs": reconstructed_runs,
            }
        )

        matchup_columns = []

        try:

            matchup_schema = cursor.execute(
                "PRAGMA table_info(matchups)"
            ).fetchall()

            matchup_columns = [
                row["name"]
                for row in matchup_schema
            ]

        except Exception:
            matchup_columns = []

        matchup = None

        if "game_pk" in matchup_columns:

            matchup_query = """
            SELECT *
            FROM matchups
            WHERE game_pk = ?
            LIMIT 1
            """

            try:

                matchup = cursor.execute(
                    matchup_query,
                    (game_pk,),
                ).fetchone()

            except Exception:
                matchup = None

        if matchup:

            home_prob = matchup["home_win_prob"] \
                if "home_win_prob" in matchup.keys() \
                else None

            away_prob = matchup["away_win_prob"] \
                if "away_win_prob" in matchup.keys() \
                else None

            derived_winner = None

            if (
                derived_home is not None
                and derived_away is not None
            ):

                derived_winner = (
                    "home"
                    if derived_home > derived_away
                    else "away"
                )

            prob_winner = None

            if (
                home_prob is not None
                and away_prob is not None
            ):

                prob_winner = (
                    "home"
                    if float(home_prob) > float(away_prob)
                    else "away"
                )

            consistent = (
                derived_winner == prob_winner
                if (
                    derived_winner
                    and prob_winner
                )
                else None
            )

            if consistent is not None:
                winner_total += 1

                if consistent:
                    winner_matches += 1

            compare_rows.append(
                {
                    "game_pk": game_pk,
                    "derived_winner": derived_winner,
                    "prob_winner": prob_winner,
                    "winner_consistent": consistent,
                    "home_win_prob": home_prob,
                    "away_win_prob": away_prob,
                }
            )

    conn.close()

    if direct_score_games > 0:

        diagnosis = (
            "actual_scores_derivable_from_statcast_direct_columns"
        )

        recommended_source = (
            "statcast_direct_score_columns"
        )

    elif reconstructable_games > 0:

        diagnosis = (
            "actual_scores_reconstructable_from_statcast_events"
        )

        recommended_source = (
            "statcast_event_reconstruction"
        )

    elif game_pks:

        diagnosis = (
            "statcast_events_have_game_rows_but_no_score_derivation"
        )

        recommended_source = None

    else:

        diagnosis = (
            "statcast_events_missing_for_backtest_games"
        )

        recommended_source = None

    if (
        diagnosis
        == "statcast_events_have_game_rows_but_no_score_derivation"
    ):

        recommended_next_step = (
            "StatsAPI final-score backfill likely required."
        )

    else:

        recommended_next_step = (
            "Patch calibration harness to consume "
            "derived final scores."
        )

    payload = {
        "statcast_columns_inspected": len(
            column_names
        ),
        "score_like_columns": score_like_columns,
        "run_like_columns": run_like_columns,
        "team_like_columns": team_like_columns,
        "games_with_statcast_rows": len(
            game_pks
        ),
        "games_with_direct_score_derivation": direct_score_games,
        "games_with_reconstructable_runs": reconstructable_games,
        "derived_score_examples": derived_rows[:10],
        "winner_consistency_vs_matchup_probs": (
            round(
                winner_matches / winner_total,
                4,
            )
            if winner_total > 0
            else None
        ),
        "recommended_actual_score_source": (
            recommended_source
        ),
        "diagnosis": diagnosis,
        "recommended_next_step": (
            recommended_next_step
        ),
    }

    out_dir = Path("tmp")

    out_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    prefix = (
        f"statcast_final_score_derivation_"
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
        out_dir / f"{prefix}_games.csv",
        derived_rows,
    )

    write_csv(
        out_dir / f"{prefix}_columns.csv",
        [
            {"column_name": col}
            for col in column_names
        ],
    )

    write_csv(
        out_dir / f"{prefix}_examples.csv",
        derived_rows,
    )

    write_csv(
        out_dir / f"{prefix}_matchup_compare.csv",
        compare_rows,
    )

    write_csv(
        out_dir / f"{prefix}_reconstruction.csv",
        reconstruction_rows,
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
