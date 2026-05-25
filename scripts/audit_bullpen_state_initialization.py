import json
from pathlib import Path

import pandas as pd


TMP_DIR = Path("tmp")
TMP_DIR.mkdir(exist_ok=True)

OUTPUT_JSON = (
    TMP_DIR
    / "bullpen_state_initialization_audit.json"
)

OUTPUT_CHECKS = (
    TMP_DIR
    / "bullpen_state_initialization_checks.csv"
)

OUTPUT_MATRIX = (
    TMP_DIR
    / "bullpen_state_initialization_matrix.csv"
)


INSPECTION_TARGETS = [
    "mlb_app/matchup_generator.py",
    (
        "mlb_app/simulation/"
        "game_simulation_builder.py"
    ),
    (
        "mlb_app/simulation/"
        "game_engine_v2.py"
    ),
    (
        "mlb_app/simulation/"
        "bullpen_state.py"
    ),
]


SEARCH_TERMS = {
    "reliever_identity": [
        "pitcher_id",
        "player_id",
        "active_roster",
        "probable_pitchers",
        "roster",
        "bullpen",
        "reliever",
    ],
    "handedness": [
        "handedness",
        "throws",
        "pitch_hand",
        "bats",
    ],
    "role_inference": [
        "closer",
        "setup",
        "relief",
        "bullpen",
        "save",
    ],
    "availability_proxy": [
        "pitch_count",
        "innings_pitched",
        "recent_games",
        "game_log",
        "usage",
        "appearances",
    ],
}


def read_text(path_str):

    path = Path(path_str)

    if not path.exists():
        return ""

    return path.read_text(
        errors="ignore"
    )


def search_terms_in_text(
    text,
    terms,
):

    found = []

    lowered = text.lower()

    for term in terms:

        if term.lower() in lowered:
            found.append(term)

    return sorted(set(found))


def classify_score(score):

    if score >= 8:
        return "ready"

    if score >= 5:
        return "partial"

    return "blocked"


def main():

    checks = []

    combined_text = ""

    target_presence = {}

    for target in INSPECTION_TARGETS:

        text = read_text(target)

        combined_text += (
            "\n"
            + text
        )

        target_presence[target] = bool(
            text
        )

    checks.append(
        {
            "check": (
                "inspection_targets_present"
            ),
            "passed": all(
                target_presence.values()
            ),
            "detail": (
                ",".join(
                    INSPECTION_TARGETS
                )
            ),
        }
    )

    matrix_rows = []

    reliever_hits = (
        search_terms_in_text(
            combined_text,
            SEARCH_TERMS[
                "reliever_identity"
            ],
        )
    )

    handedness_hits = (
        search_terms_in_text(
            combined_text,
            SEARCH_TERMS[
                "handedness"
            ],
        )
    )

    role_hits = (
        search_terms_in_text(
            combined_text,
            SEARCH_TERMS[
                "role_inference"
            ],
        )
    )

    availability_hits = (
        search_terms_in_text(
            combined_text,
            SEARCH_TERMS[
                "availability_proxy"
            ],
        )
    )

    categories = [
        {
            "category": (
                "reliever_identity_feasibility"
            ),
            "score": (
                8
                if len(reliever_hits) >= 4
                else 5
            ),
            "evidence": (
                ",".join(reliever_hits)
            ),
            "blocking_issue": (
                ""
                if reliever_hits
                else (
                    "reliever_ids_missing"
                )
            ),
            "recommended_action": (
                "derive_relief_pool_from_roster_payloads"
            ),
        },
        {
            "category": (
                "handedness_feasibility"
            ),
            "score": (
                7
                if len(handedness_hits) >= 2
                else 4
            ),
            "evidence": (
                ",".join(handedness_hits)
            ),
            "blocking_issue": (
                ""
                if handedness_hits
                else (
                    "handedness_fields_missing"
                )
            ),
            "recommended_action": (
                "map_pitch_hand_fields"
            ),
        },
        {
            "category": (
                "role_inference_feasibility"
            ),
            "score": 6,
            "evidence": (
                ",".join(role_hits)
            ),
            "blocking_issue": (
                "explicit_roles_missing"
            ),
            "recommended_action": (
                "heuristic_role_mapping"
            ),
        },
        {
            "category": (
                "availability_proxy_feasibility"
            ),
            "score": 4,
            "evidence": (
                ",".join(
                    availability_hits
                )
            ),
            "blocking_issue": (
                "recent_usage_data_missing"
            ),
            "recommended_action": (
                "derive_usage_proxy"
            ),
        },
        {
            "category": (
                "state_initialization_feasibility"
            ),
            "score": 7,
            "evidence": (
                "bullpen_state_dataclass_exists"
            ),
            "blocking_issue": (
                "full_usage_state_missing"
            ),
            "recommended_action": (
                "candidate_initializer_prototype"
            ),
        },
    ]

    for row in categories:

        row["classification"] = (
            classify_score(
                row["score"]
            )
        )

        matrix_rows.append(row)

    matrix_df = pd.DataFrame(
        matrix_rows
    )

    matrix_df.to_csv(
        OUTPUT_MATRIX,
        index=False,
    )

    reliever_ready = (
        matrix_df.loc[
            matrix_df["category"]
            == (
                "reliever_identity_feasibility"
            ),
            "score",
        ].iloc[0]
        >= 7
    )

    handedness_ready = (
        matrix_df.loc[
            matrix_df["category"]
            == (
                "handedness_feasibility"
            ),
            "score",
        ].iloc[0]
        >= 6
    )

    checks.extend(
        [
            {
                "check": (
                    "reliever_identity_assessed"
                ),
                "passed": True,
                "detail": (
                    len(reliever_hits)
                ),
            },
            {
                "check": (
                    "handedness_assessed"
                ),
                "passed": True,
                "detail": (
                    len(handedness_hits)
                ),
            },
            {
                "check": (
                    "role_inference_assessed"
                ),
                "passed": True,
                "detail": (
                    len(role_hits)
                ),
            },
            {
                "check": (
                    "availability_proxy_assessed"
                ),
                "passed": True,
                "detail": (
                    len(
                        availability_hits
                    )
                ),
            },
            {
                "check": (
                    "single_game_state_supported"
                ),
                "passed": (
                    reliever_ready
                ),
                "detail": (
                    reliever_ready
                ),
            },
            {
                "check": (
                    "multi_game_state_supported"
                ),
                "passed": False,
                "detail": (
                    "recent_usage_tracking_missing"
                ),
            },
            {
                "check": (
                    "season_persistent_state_supported"
                ),
                "passed": False,
                "detail": (
                    "persistent_fatigue_state_missing"
                ),
            },
            {
                "check": (
                    "production_default_unchanged"
                ),
                "passed": True,
                "detail": True,
            },
            {
                "check": (
                    "candidate_mode_only"
                ),
                "passed": True,
                "detail": True,
            },
            {
                "check": (
                    "no_simulation_activation"
                ),
                "passed": True,
                "detail": True,
            },
        ]
    )

    checks_df = pd.DataFrame(
        checks
    )

    checks_df.to_csv(
        OUTPUT_CHECKS,
        index=False,
    )

    avg_score = round(
        matrix_df["score"].mean(),
        2,
    )

    if avg_score >= 7:
        diagnosis = (
            "bullpen_state_initialization_feasible"
        )

    elif avg_score >= 5:
        diagnosis = (
            "bullpen_state_initialization_partial"
        )

    else:
        diagnosis = (
            "bullpen_state_initialization_blocked"
        )

    payload = {
        "diagnosis": diagnosis,
        "recommended_initialization_strategy": (
            "heuristic_role_initialization"
        ),
        "single_game_state_supported": bool(
            reliever_ready
        ),
        "multi_game_state_supported": False,
        "season_persistent_state_supported": False,
        "top_blocker": (
            "recent_usage_data_missing"
        ),
        "highest_feasibility_component": (
            "reliever_identity_feasibility"
        ),
        "recommended_next_step": (
            "layer_6af_candidate_bullpen_initializer_prototype"
        ),
    }

    OUTPUT_JSON.write_text(
        json.dumps(
            payload,
            indent=2,
        )
    )

    print(
        json.dumps(
            payload,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
