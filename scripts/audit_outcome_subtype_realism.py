import csv
import json
import os
from pathlib import Path

from sqlalchemy import create_engine
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
    / "outcome_subtype_realism.json"
)

OUTPUT_SOURCE = (
    TMP_DIR
    / "outcome_subtype_realism_source_checks.csv"
)

OUTPUT_PA_KEYS = (
    TMP_DIR
    / "outcome_subtype_realism_pa_keys.csv"
)

OUTPUT_MATRIX = (
    TMP_DIR
    / "outcome_subtype_transition_matrix.csv"
)

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "sqlite:///mlb.db",
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

SOURCE_FILES = [
    (
        "mlb_app/simulation/"
        "inning_simulator.py"
    ),
    (
        "mlb_app/simulation/"
        "transition_profiles.py"
    ),
]

SOURCE_CHECKS = {
    "k_separate": [
        '"k"',
        "'k'",
    ],

    "generic_out": [
        '"out"',
        "'out'",
    ],

    "groundout_modeled": [
        "groundout",
    ],

    "flyout_modeled": [
        "flyout",
    ],

    "lineout_popout_modeled": [
        "lineout",
        "popup",
        "popout",
    ],

    "sac_fly_inside_generic_out": [
        "sac_fly",
    ],

    "double_play_inside_generic_out": [
        "double_play",
    ],
}

EXPECTED_PA_KEYS = [
    "k",
    "bb",
    "hbp",
    "single",
    "double",
    "triple",
    "hr",
    "reached_on_error",
    "out",
    "groundout",
    "flyout",
    "lineout",
    "popup",
    "double_play",
    "sac_fly",
]

TRANSITION_MATRIX = [
    {
        "subtype":
            "strikeout",

        "runner_advancement":
            "low",

        "gdp_risk":
            "no",

        "sac_fly_risk":
            "no",

        "tag_up":
            "no",

        "current_support":
            "partial_via_k",
    },

    {
        "subtype":
            "generic_out",

        "runner_advancement":
            "mixed",

        "gdp_risk":
            "probabilistic",

        "sac_fly_risk":
            "probabilistic",

        "tag_up":
            "partial",

        "current_support":
            "yes",
    },

    {
        "subtype":
            "groundout",

        "runner_advancement":
            "medium",

        "gdp_risk":
            "high",

        "sac_fly_risk":
            "no",

        "tag_up":
            "sometimes",

        "current_support":
            "missing",
    },

    {
        "subtype":
            "flyout",

        "runner_advancement":
            "medium",

        "gdp_risk":
            "low",

        "sac_fly_risk":
            "high",

        "tag_up":
            "yes",

        "current_support":
            "missing",
    },

    {
        "subtype":
            "lineout_popout",

        "runner_advancement":
            "low_medium",

        "gdp_risk":
            "low",

        "sac_fly_risk":
            "low",

        "tag_up":
            "rare",

        "current_support":
            "missing",
    },
]

def write_csv(
    path,
    rows,
):

    if not rows:

        return

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

def audit_source_capabilities():

    combined_text = ""

    for path in SOURCE_FILES:

        p = Path(path)

        if p.exists():

            combined_text += (
                p.read_text()
                + "\n"
            )

    results = {}

    rows = []

    for capability, terms in (
        SOURCE_CHECKS.items()
    ):

        found = any(
            term in combined_text
            for term in terms
        )

        results[
            capability
        ] = found

        rows.append(
            {
                "capability":
                    capability,

                "found":
                    found,

                "search_terms":
                    ",".join(terms),
            }
        )

    return results, rows

def inspect_pa_models():

    engine = create_engine(
        DATABASE_URL
    )

    Session = sessionmaker(
        bind=engine
    )

    games_sampled = 0

    found_keys = set()

    pa_model_paths_found = 0

    parser_blocked = False

    parser_details = {}

    with Session() as session:

        matchups = (
            generate_matchups_for_date(
                session,
                "2026-04-20",
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
                                "2026-04-20",

                            "matchup": {
                                "raw":
                                    matchup,

                                "game_date":
                                    "2026-04-20",
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

            pa_models = result.get(
                "pa_models",
                {},
            )

            if not pa_models:

                parser_blocked = True

                parser_details = {
                    "status":
                        result.get(
                            "status"
                        ),

                    "error":
                        result.get(
                            "error"
                        ),

                    "top_level_keys":
                        sorted(
                            result.keys()
                        ),
                }

                continue

            games_sampled += 1

            for (
                model_name,
                model_payload,
            ) in pa_models.items():

                if not isinstance(
                    model_payload,
                    dict,
                ):

                    continue

                probs = (
                    model_payload.get(
                        (
                            "lineup_average_"
                            "probabilities"
                        ),
                        {},
                    )
                )

                if not isinstance(
                    probs,
                    dict,
                ):

                    continue

                pa_model_paths_found += 1

                for key in probs:

                    found_keys.add(key)

    key_rows = []

    key_summary = {}

    for key in EXPECTED_PA_KEYS:

        exists = (
            key in found_keys
        )

        key_summary[
            key
        ] = exists

        key_rows.append(
            {
                "pa_key":
                    key,

                "found":
                    exists,
            }
        )

    return {
        "games_sampled":
            games_sampled,

        "pa_model_paths_found":
            pa_model_paths_found,

        "found_keys":
            sorted(
                found_keys
            ),

        "key_summary":
            key_summary,

        "key_rows":
            key_rows,

        "parser_blocked":
            parser_blocked,

        "parser_details":
            parser_details,
    }

def determine_feasibility(
    source_capabilities,
    pa_key_summary,
):

    subtype_keys_exist = any(
        [
            pa_key_summary.get(
                "groundout",
                False,
            ),

            pa_key_summary.get(
                "flyout",
                False,
            ),

            pa_key_summary.get(
                "lineout",
                False,
            ),

            pa_key_summary.get(
                "popup",
                False,
            ),
        ]
    )

    if subtype_keys_exist:

        return (
            "outcome_subtype_ready_"
            "for_prototype"
        )

    if (
        source_capabilities[
            "k_separate"
        ]
        and source_capabilities[
            "generic_out"
        ]
    ):

        return (
            "outcome_subtype_needs_"
            "pa_model_extension"
        )

    return (
        "outcome_subtype_"
        "not_feasible"
    )

def main():

    source_capabilities, source_rows = (
        audit_source_capabilities()
    )

    pa_results = (
        inspect_pa_models()
    )

    write_csv(
        OUTPUT_SOURCE,
        source_rows,
    )

    write_csv(
        OUTPUT_PA_KEYS,
        pa_results[
            "key_rows"
        ],
    )

    write_csv(
        OUTPUT_MATRIX,
        TRANSITION_MATRIX,
    )

    if (
        pa_results[
            "parser_blocked"
        ]
        and pa_results[
            "games_sampled"
        ]
        == 0
    ):

        summary = {
            "diagnosis":
                (
                    "outcome_subtype_"
                    "realism_parser_blocked"
                ),

            "feasibility_decision":
                (
                    "outcome_subtype_"
                    "not_feasible"
                ),

            "parser_details":
                pa_results[
                    "parser_details"
                ],

            "recommended_next_step":
                (
                    "layer_6s_"
                    "outcome_subtype_"
                    "pa_model_feasibility"
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
            source_capabilities,
            pa_results[
                "key_summary"
            ],
        )
    )

    diagnosis_map = {
        (
            "outcome_subtype_"
            "ready_for_prototype"
        ):
            (
                "outcome_subtype_"
                "realism_ready_for_"
                "prototype"
            ),

        (
            "outcome_subtype_"
            "needs_pa_model_extension"
        ):
            (
                "outcome_subtype_"
                "realism_needs_"
                "pa_model_extension"
            ),

        (
            "outcome_subtype_"
            "not_feasible"
        ):
            (
                "outcome_subtype_"
                "realism_not_feasible"
            ),
    }

    blockers = []

    if not pa_results[
        "key_summary"
    ].get(
        "groundout",
        False,
    ):

        blockers.append(
            (
                "pa_model_does_not_emit_"
                "groundout_probability"
            )
        )

    if not pa_results[
        "key_summary"
    ].get(
        "flyout",
        False,
    ):

        blockers.append(
            (
                "pa_model_does_not_emit_"
                "flyout_probability"
            )
        )

    if (
        source_capabilities[
            "generic_out"
        ]
        and not pa_results[
            "key_summary"
        ].get(
            "sac_fly",
            False,
        )
    ):

        blockers.append(
            (
                "generic_out_blends_"
                "sac_fly_and_double_play_"
                "contexts"
            )
        )

    summary = {
        "diagnosis":
            diagnosis_map[
                feasibility
            ],

        "feasibility_decision":
            feasibility,

        "games_sampled":
            pa_results[
                "games_sampled"
            ],

        "pa_model_paths_found":
            pa_results[
                "pa_model_paths_found"
            ],

        "source_capabilities":
            source_capabilities,

        "pa_key_summary":
            pa_results[
                "key_summary"
            ],

        "largest_blockers":
            blockers,

        "recommended_next_step":
            (
                "layer_6s_"
                "outcome_subtype_"
                "pa_model_feasibility"
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

if __name__ == "__main__":

    main()
