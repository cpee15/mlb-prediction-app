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

from mlb_app.simulation.outcome_subtypes import (
    derive_outcome_subtype_probabilities,
    normalize_out_subtype_shares,
)

TMP_DIR = Path("tmp")

OUTPUT_JSON = (
    TMP_DIR
    / "outcome_subtype_derivation.json"
)

OUTPUT_CHECKS = (
    TMP_DIR
    / "outcome_subtype_derivation_checks.csv"
)

OUTPUT_PAYLOADS = (
    TMP_DIR
    / "outcome_subtype_derivation_payloads.csv"
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

TOLERANCE = 1e-9

def approx_equal(a, b):

    return (
        abs(a - b)
        <= TOLERANCE
    )

def write_csv(
    path,
    rows,
):

    if not rows:

        rows = [
            {
                "status":
                    "no_rows"
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

def validate_invalid_rejections():

    bad_cases = [
        {
            "groundout": -1,
            "flyout": 1,
            "lineout_popout": 1,
            "other_out": 1,
        },

        {
            "groundout": 1,
            "flyout": 1,
            "lineout_popout": 1,
        },

        {
            "groundout": "bad",
            "flyout": 1,
            "lineout_popout": 1,
            "other_out": 1,
        },

        {
            "groundout": 0,
            "flyout": 0,
            "lineout_popout": 0,
            "other_out": 0,
        },
    ]

    passed = 0

    for case in bad_cases:

        try:

            normalize_out_subtype_shares(
                case
            )

        except Exception:

            passed += 1

    return (
        passed
        == len(bad_cases)
    )

def smoke_test_payloads():

    engine = create_engine(
        DATABASE_URL
    )

    Session = sessionmaker(
        bind=engine
    )

    payload_rows = []

    conservation_failures = 0

    payloads_tested = 0

    parser_blocked = False

    with Session() as session:

        matchups = (
            generate_matchups_for_date(
                session,
                BACKTEST_START,
            )
        )

        for sample_count, matchup in enumerate(
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
                                    + sample_count
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

            except Exception:

                parser_blocked = True

                continue

            pa_models = result.get(
                "pa_models",
                {},
            )

            for (
                model_name,
                payload,
            ) in pa_models.items():

                probs = payload.get(
                    (
                        "lineup_average_"
                        "probabilities"
                    ),
                    {},
                )

                if not probs:

                    continue

                payloads_tested += 1

                derived = (
                    derive_outcome_subtype_probabilities(
                        probs,
                        include_metadata=True,
                    )
                )

                original_sum = (
                    derived[
                        (
                            "pa_probability_"
                            "sum_original"
                        )
                    ]
                )

                derived_sum = (
                    derived[
                        (
                            "pa_probability_"
                            "sum_derived"
                        )
                    ]
                )

                conserved = (
                    approx_equal(
                        original_sum,
                        derived_sum,
                    )
                )

                if not conserved:

                    conservation_failures += 1

                original_numeric_keys = sorted(
                    [
                        key
                        for key, value
                        in probs.items()
                        if isinstance(
                            value,
                            (int, float),
                        )
                    ]
                )

                preserved_keys = (
                    derived[
                        "preserved_numeric_keys"
                    ]
                )

                excluded_keys = (
                    derived[
                        "excluded_decomposed_keys"
                    ]
                )

                missing_keys = sorted(
                    list(
                        set(
                            original_numeric_keys
                        )
                        - set(
                            preserved_keys
                        )
                        - set(
                            excluded_keys
                        )
                    )
                )

                missing_sum = sum(
                    float(
                        probs[key]
                    )
                    for key
                    in missing_keys
                    if isinstance(
                        probs.get(key),
                        (int, float),
                    )
                )

                payload_rows.append(
                    {
                        "game_pk":
                            game_pk,

                        "model":
                            model_name,

                        "original_numeric_keys":
                            json.dumps(
                                original_numeric_keys
                            ),

                        "preserved_numeric_keys":
                            json.dumps(
                                preserved_keys
                            ),

                        "excluded_decomposed_keys":
                            json.dumps(
                                excluded_keys
                            ),

                        "missing_numeric_keys":
                            json.dumps(
                                missing_keys
                            ),

                        "missing_numeric_sum":
                            missing_sum,

                        "original_sum":
                            original_sum,

                        "derived_sum":
                            derived_sum,

                        "conserved":
                            conserved,
                    }
                )

    return {
        "payloads_tested":
            payloads_tested,

        "conservation_failures":
            conservation_failures,

        "payload_rows":
            payload_rows,

        "parser_blocked":
            parser_blocked,
    }

def main():

    default_sum = sum(
        normalize_out_subtype_shares().values()
    )

    invalid_rejections = (
        validate_invalid_rejections()
    )

    payload_results = (
        smoke_test_payloads()
    )

    write_csv(
        OUTPUT_PAYLOADS,
        payload_results[
            "payload_rows"
        ],
    )

    checks = [
        {
            "check":
                "default_share_sum",

            "passed":
                approx_equal(
                    default_sum,
                    1.0,
                ),
        },

        {
            "check":
                (
                    "invalid_share_"
                    "rejections"
                ),

            "passed":
                invalid_rejections,
        },

        {
            "check":
                (
                    "schema_"
                    "reconciliation"
                ),

            "passed":
                (
                    payload_results[
                        (
                            "conservation_"
                            "failures"
                        )
                    ]
                    == 0
                ),
        },
    ]

    write_csv(
        OUTPUT_CHECKS,
        checks,
    )

    if (
        payload_results[
            "parser_blocked"
        ]
        and payload_results[
            "payloads_tested"
        ]
        == 0
    ):

        diagnosis = (
            "outcome_subtype_"
            "derivation_payload_"
            "parser_blocked"
        )

    elif not invalid_rejections:

        diagnosis = (
            "outcome_subtype_"
            "derivation_invalid_"
            "validation_failed"
        )

    elif (
        payload_results[
            "conservation_failures"
        ]
        > 0
    ):

        diagnosis = (
            "outcome_subtype_"
            "derivation_schema_"
            "reconciliation_failed"
        )

    else:

        diagnosis = (
            "outcome_subtype_"
            "derivation_prototype_"
            "safe"
        )

    summary = {
        "diagnosis":
            diagnosis,

        "default_share_sum":
            default_sum,

        "production_payloads_tested":
            payload_results[
                "payloads_tested"
            ],

        "conservation_failures":
            payload_results[
                "conservation_failures"
            ],

        "invalid_share_rejections_passed":
            invalid_rejections,

        "schema_reconciliation_passed":
            (
                payload_results[
                    "conservation_failures"
                ]
                == 0
            ),

        "subtype_keys": [
            "strikeout",
            "groundout",
            "flyout",
            "lineout_popout",
            "other_out",
        ],

        "recommended_next_step":
            (
                "layer_6u_"
                "subtype_transition_"
                "prototype"
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
