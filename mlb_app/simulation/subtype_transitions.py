import random

DEFAULT_SUBTYPE_TRANSITION_RATES = {
    "groundout_double_play_rate": 0.11,
    "groundout_runner_third_scores_rate": 0.25,
    "groundout_runner_second_advances_rate": 0.35,
    "flyout_runner_third_scores_rate": 0.38,
    "flyout_runner_second_advances_rate": 0.22,
    "lineout_popout_runner_third_scores_rate": 0.05,
    "lineout_popout_runner_second_advances_rate": 0.05,
}

VALID_SUBTYPES = {
    "strikeout",
    "groundout",
    "flyout",
    "lineout_popout",
    "other_out",
}

def resolve_subtype_transition_rates(
    transition_rates=None,
):

    resolved = dict(
        DEFAULT_SUBTYPE_TRANSITION_RATES
    )

    if transition_rates is None:

        return resolved

    if not isinstance(
        transition_rates,
        dict,
    ):

        raise TypeError(
            "transition_rates must be dict"
        )

    for key, value in (
        transition_rates.items()
    ):

        if (
            key
            not in DEFAULT_SUBTYPE_TRANSITION_RATES
        ):

            raise ValueError(
                f"unknown transition rate: {key}"
            )

        if not isinstance(
            value,
            (int, float),
        ):

            raise ValueError(
                f"rate must be numeric: {key}"
            )

        value = float(value)

        if value < 0.0 or value > 1.0:

            raise ValueError(
                f"rate out of bounds: {key}"
            )

        resolved[key] = value

    return resolved

def _roll(
    rng,
    probability,
):

    return (
        rng.random()
        < probability
    )

def advance_runners_by_subtype(
    bases,
    outcome_subtype,
    outs,
    rng=None,
    transition_rates=None,
):

    if (
        outcome_subtype
        not in VALID_SUBTYPES
    ):

        raise ValueError(
            (
                "invalid outcome subtype: "
                f"{outcome_subtype}"
            )
        )

    if rng is None:

        rng = random.Random()

    rates = (
        resolve_subtype_transition_rates(
            transition_rates
        )
    )

    first, second, third = bases

    runs_scored = 0

    outs_added = 1

    #
    # STRIKEOUT
    #

    if (
        outcome_subtype
        == "strikeout"
    ):

        return (
            (
                first,
                second,
                third,
            ),
            0,
            1,
        )

    #
    # OTHER OUT
    #

    if (
        outcome_subtype
        == "other_out"
    ):

        return (
            (
                first,
                second,
                third,
            ),
            0,
            1,
        )

    #
    # GROUNDOUT
    #

    if (
        outcome_subtype
        == "groundout"
    ):

        #
        # Double play case
        #

        if (
            first
            and outs < 2
            and _roll(
                rng,
                rates[
                    (
                        "groundout_"
                        "double_play_rate"
                    )
                ],
            )
        ):

            outs_added = 2

            return (
                (
                    False,
                    second,
                    third,
                ),
                0,
                outs_added,
            )

        #
        # Standard groundout
        #

        new_first = False
        new_second = second
        new_third = third

        #
        # Runner scores from third
        #

        if (
            third
            and outs < 2
            and _roll(
                rng,
                rates[
                    (
                        "groundout_runner_"
                        "third_scores_rate"
                    )
                ],
            )
        ):

            runs_scored += 1
            new_third = False

        #
        # Runner advances from second
        #

        if (
            second
            and outs < 2
            and _roll(
                rng,
                rates[
                    (
                        "groundout_runner_"
                        "second_advances_rate"
                    )
                ],
            )
        ):

            new_second = False

            if not new_third:

                new_third = True

        return (
            (
                new_first,
                new_second,
                new_third,
            ),
            runs_scored,
            outs_added,
        )

    #
    # FLYOUT
    #

    if (
        outcome_subtype
        == "flyout"
    ):

        new_first = first
        new_second = second
        new_third = third

        if (
            third
            and outs < 2
            and _roll(
                rng,
                rates[
                    (
                        "flyout_runner_"
                        "third_scores_rate"
                    )
                ],
            )
        ):

            runs_scored += 1
            new_third = False

        if (
            second
            and outs < 2
            and _roll(
                rng,
                rates[
                    (
                        "flyout_runner_"
                        "second_advances_rate"
                    )
                ],
            )
        ):

            new_second = False

            if not new_third:

                new_third = True

        return (
            (
                new_first,
                new_second,
                new_third,
            ),
            runs_scored,
            outs_added,
        )

    #
    # LINEOUT / POPOUT
    #

    if (
        outcome_subtype
        == "lineout_popout"
    ):

        new_first = first
        new_second = second
        new_third = third

        if (
            third
            and outs < 2
            and _roll(
                rng,
                rates[
                    (
                        "lineout_popout_"
                        "runner_third_"
                        "scores_rate"
                    )
                ],
            )
        ):

            runs_scored += 1
            new_third = False

        if (
            second
            and outs < 2
            and _roll(
                rng,
                rates[
                    (
                        "lineout_popout_"
                        "runner_second_"
                        "advances_rate"
                    )
                ],
            )
        ):

            new_second = False

            if not new_third:

                new_third = True

        return (
            (
                new_first,
                new_second,
                new_third,
            ),
            runs_scored,
            outs_added,
        )

    raise RuntimeError(
        "unreachable subtype path"
    )
