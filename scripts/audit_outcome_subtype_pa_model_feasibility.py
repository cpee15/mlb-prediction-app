import csv
import json
import os
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy import inspect as sqlalchemy_inspect
from sqlalchemy.orm import sessionmaker

from mlb_app.matchup_generator import (
    generate_matchups_for_date,
)

from mlb_app.simulation.game_engine_v2 import (
    run_full_game_simulation,
)

TMP_DIR = Path("tmp")

OUTPUT_JSON = (
    TMP_DIR
    / "outcome_subtype_pa_model_feasibility.json"
)

OUTPUT_PAYLOAD = (
    TMP_DIR
    / "outcome_subtype_payload_key_matches.csv"
)

OUTPUT_REPO = (
    TMP_DIR
    / "outcome_subtype_repo_term_matches.csv"
)

OUTPUT_DB = (
    TMP_DIR
    / "outcome_subtype_db_schema_matches.csv"
)

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "sqlite:///mlb.db",
)

BACKTEST_START = os.environ.get(
    "BACKTEST_START",
    "2026-04-20",
)

MAX_GAMES = int(
    os.environ.get(
        "MAX_GAMES",
        3,
    )
)

SIMS_PER_GAME = int(
    os.environ.get(
        "SIMS_PER_GAME",
        50,
    )
)

SEED = int(
    os.environ.get(
        "SEED",
        42,
    )
)

MAX_MATCHES = 500

SEARCH_TERMS = [
    "ground",
    "ground_ball",
    "gb",
    "fly",
    "fly_ball",
    "fb",
    "line_drive",
    "ld",
    "popup",
    "pop_up",
    "launch",
    "launch_angle",
    "spray",
    "batted_ball",
    "contact",
    "double_play",
    "sac_fly",
]

TEXT_EXTENSIONS = {
    ".py",
    ".sql",
    ".json",
    ".md",
    ".txt",
    ".yaml",
    ".yml",
}

SKIP_DIRS = {
    ".git",
    "__pycache__",
    "node_modules",
    "venv",
    ".venv",
    "tmp",
}

def write_csv(
    path,
    rows,
):

    if not rows:

        rows = [
            {
                "status":
                    "no_matches"
            }
        ]

    with open(
        path,
        "w",
        newline="",
    ) as f:

        writer = csv.DictWriter(
            f,
            fieldnames=list(
                rows[0].keys()
            ),
        )

        writer.writeheader()
        writer.writerows(rows)

def recursively_find_terms(
    obj,
    terms,
    path="root",
    matches=None,
):

    if matches is None:

        matches = []

    if len(matches) >= MAX_MATCHES:

        return matches

    try:

        if isinstance(obj, dict):

            for key, value in (
                obj.items()
            ):

                key_lower = str(
                    key
                ).lower()

                for term in terms:

                    if (
                        term.lower()
                        in key_lower
                    ):

                        matches.append(
                            {
                                "source":
                                    "payload_key",

                                "path":
                                    (
                                        f"{path}."
                                        f"{key}"
                                    ),

                                "term":
                                    term,

                                "value_preview":
                                    str(value)[:200],
                            }
                        )

                recursively_find_terms(
                    value,
                    terms,
                    (
                        f"{path}."
                        f"{key}"
                    ),
                    matches,
                )

        elif isinstance(obj, list):

            for idx, item in enumerate(obj):

                recursively_find_terms(
                    item,
                    terms,
                    (
                        f"{path}[{idx}]"
                    ),
                    matches,
                )

        else:

            obj_str = str(obj).lower()

            for term in terms:

                if (
                    term.lower()
                    in obj_str
                ):

                    matches.append(
                        {
                            "source":
                                "payload_value",

                            "path":
                                path,

                            "term":
                                term,

                            "value_preview":
                                str(obj)[:200],
                        }
                    )

    except Exception:

        pass

    return matches

def audit_payloads():

    engine = create_engine(
        DATABASE_URL
    )

    Session = sessionmaker(
        bind=engine
    )

    games_sampled = 0

    payload_matches = []

    parser_blocked = False

    parser_details = {}

    with Session() as session:

        matchups = (
            generate_matchups_for_date(
                session,
                BACKTEST_START,
            )
        )

        for matchup in (
            matchups[:MAX_GAMES]
        ):

            game_pk = (
                matchup.get(
                    "game_pk"
                )
                or matchup.get(
                    "gamePk"
                )
            )

            try:

                result = (
                    run_full_game_simulation(
                        int(game_pk),
                        config={
                            "simulation_count":
                                SIMS_PER_GAME,

                            "seed":
                                (
                                    SEED
                                    + games_sampled
                                ),

                            "date":
                                BACKTEST_START,

                            "matchup": {
                                "raw":
                                    matchup,

                                "game_date":
                                    BACKTEST_START,
                            },
                        },
                    )
                )

            except Exception as e:

                parser_blocked = True

                parser_details = {
                    "exception":
                        str(e)
                }

                continue

            games_sampled += 1

            recursively_find_terms(
                result,
                SEARCH_TERMS,
                matches=payload_matches,
            )

    return {
        "games_sampled":
            games_sampled,

        "payload_matches":
            payload_matches[
                :MAX_MATCHES
            ],

        "parser_blocked":
            parser_blocked,

        "parser_details":
            parser_details,
    }

def audit_repo():

    rows = []

    for path in Path(".").rglob("*"):

        if any(
            skip in path.parts
            for skip in SKIP_DIRS
        ):

            continue

        if (
            path.suffix
            not in TEXT_EXTENSIONS
        ):

            continue

        try:

            text = path.read_text(
                errors="ignore"
            )

        except Exception:

            continue

        text_lower = text.lower()

        for term in SEARCH_TERMS:

            count = (
                text_lower.count(
                    term.lower()
                )
            )

            if count > 0:

                rows.append(
                    {
                        "file":
                            str(path),

                        "term":
                            term,

                        "match_count":
                            count,
                    }
                )

        if len(rows) >= MAX_MATCHES:

            break

    return rows[:MAX_MATCHES]

def audit_db_schema():

    rows = []

    try:

        engine = create_engine(
            DATABASE_URL
        )

        inspector = (
            sqlalchemy_inspect(
                engine
            )
        )

        tables = (
            inspector.get_table_names()
        )

        for table in tables:

            table_lower = (
                table.lower()
            )

            for term in SEARCH_TERMS:

                if (
                    term.lower()
                    in table_lower
                ):

                    rows.append(
                        {
                            "match_type":
                                "table",

                            "table":
                                table,

                            "column":
                                "",

                            "term":
                                term,
                        }
                    )

            try:

                columns = (
                    inspector.get_columns(
                        table
                    )
                )

            except Exception:

                continue

            for column in columns:

                col_name = (
                    column["name"]
                )

                col_lower = (
                    col_name.lower()
                )

                for term in SEARCH_TERMS:

                    if (
                        term.lower()
                        in col_lower
                    ):

                        rows.append(
                            {
                                "match_type":
                                    "column",

                                "table":
                                    table,

                                "column":
                                    col_name,

                                "term":
                                    term,
                            }
                        )

            if len(rows) >= MAX_MATCHES:

                break

    except Exception as e:

        rows.append(
            {
                "match_type":
                    "db_error",

                "table":
                    "",

                "column":
                    "",

                "term":
                    str(e),
            }
        )

    return rows[:MAX_MATCHES]

def determine_feasibility(
    payload_matches,
    repo_matches,
    db_matches,
):

    payload_terms = {
        row["term"]
        for row in payload_matches
    }

    subtype_payload_hits = any(
        [
            "ground" in payload_terms,
            "fly" in payload_terms,
            "line_drive"
            in payload_terms,
            "popup"
            in payload_terms,
        ]
    )

    if subtype_payload_hits:

        return (
            "outcome_subtype_pa_model_"
            "ready_from_payload"
        )

    db_terms = {
        row["term"]
        for row in db_matches
    }

    subtype_db_hits = any(
        [
            "ground" in db_terms,
            "fly" in db_terms,
            "line_drive"
            in db_terms,
            "popup"
            in db_terms,
        ]
    )

    if subtype_db_hits:

        return (
            "outcome_subtype_pa_model_"
            "ready_from_database"
        )

    repo_terms = {
        row["term"]
        for row in repo_matches
    }

    subtype_repo_hits = any(
        [
            "launch" in repo_terms,
            "batted_ball"
            in repo_terms,
            "ground_ball"
            in repo_terms,
            "fly_ball"
            in repo_terms,
            "line_drive"
            in repo_terms,
        ]
    )

    if subtype_repo_hits:

        return (
            "outcome_subtype_pa_model_"
            "derivable_from_repo_features"
        )

    return (
        "outcome_subtype_pa_model_"
        "requires_external_data"
    )

def main():

    payload_results = (
        audit_payloads()
    )

    repo_matches = (
        audit_repo()
    )

    db_matches = (
        audit_db_schema()
    )

    write_csv(
        OUTPUT_PAYLOAD,
        payload_results[
            "payload_matches"
        ],
    )

    write_csv(
        OUTPUT_REPO,
        repo_matches,
    )

    write_csv(
        OUTPUT_DB,
        db_matches,
    )

    if (
        payload_results[
            "parser_blocked"
        ]
        and payload_results[
            "games_sampled"
        ]
        == 0
    ):

        summary = {
            "diagnosis":
                (
                    "outcome_subtype_pa_model_"
                    "parser_blocked"
                ),

            "feasibility_classification":
                (
                    "not_feasible"
                ),

            "parser_details":
                payload_results[
                    "parser_details"
                ],

            "recommended_next_step":
                (
                    "layer_6t_"
                    "batted_ball_prior_"
                    "design_audit"
                ),
        }

        with open(
            OUTPUT_JSON,
            "w",
        ) as f:

            json.dump(
                summary,
                f,
                indent=2,
            )

        print(
            json.dumps(
                summary,
                indent=2,
            )
        )

        return

    feasibility = (
        determine_feasibility(
            payload_results[
                "payload_matches"
            ],
            repo_matches,
            db_matches,
        )
    )

    diagnosis_map = {
        (
            "outcome_subtype_pa_model_"
            "ready_from_payload"
        ):
            (
                "outcome_subtype_pa_model_"
                "ready_from_payload"
            ),

        (
            "outcome_subtype_pa_model_"
            "ready_from_database"
        ):
            (
                "outcome_subtype_pa_model_"
                "ready_from_database"
            ),

        (
            "outcome_subtype_pa_model_"
            "derivable_from_repo_features"
        ):
            (
                "outcome_subtype_pa_model_"
                "derivable_from_repo_features"
            ),

        (
            "outcome_subtype_pa_model_"
            "requires_external_data"
        ):
            (
                "outcome_subtype_pa_model_"
                "requires_external_data"
            ),
    }

    missing_signals = [
        "ground_ball_probability",
        "fly_ball_probability",
        "line_drive_probability",
        "popup_probability",
    ]

    if (
        feasibility
        == (
            "outcome_subtype_pa_model_"
            "derivable_from_repo_features"
        )
    ):

        strategy = (
            "derive_subtype_priors_"
            "from_existing_repo_"
            "features"
        )

        next_step = (
            "layer_6t_"
            "subtype_probability_"
            "derivation_prototype"
        )

    elif (
        feasibility
        == (
            "outcome_subtype_pa_model_"
            "ready_from_payload"
        )
    ):

        strategy = (
            "prototype_subtype_"
            "transition_engine"
        )

        next_step = (
            "layer_6t_"
            "outcome_subtype_"
            "transition_prototype"
        )

    else:

        strategy = (
            "derive_or_ingest_"
            "batted_ball_priors_"
            "before_transition_"
            "prototype"
        )

        next_step = (
            "layer_6t_"
            "batted_ball_prior_"
            "design_audit"
        )

    summary = {
        "diagnosis":
            diagnosis_map[
                feasibility
            ],

        "feasibility_classification":
            feasibility,

        "games_sampled":
            payload_results[
                "games_sampled"
            ],

        "payload_matches_found":
            len(
                payload_results[
                    "payload_matches"
                ]
            ),

        "repo_matches_found":
            len(
                repo_matches
            ),

        "db_schema_matches_found":
            len(
                db_matches
            ),

        "candidate_existing_sources":
            sorted(
                {
                    row["term"]
                    for row in repo_matches
                }
            )[:50],

        "missing_required_signals":
            missing_signals,

        "recommended_subtype_model_strategy":
            strategy,

        "recommended_next_step":
            next_step,
    }

    with open(
        OUTPUT_JSON,
        "w",
    ) as f:

        json.dump(
            summary,
            f,
            indent=2,
        )

    print(
        json.dumps(
            summary,
            indent=2,
        )
    )

if __name__ == "__main__":

    main()
