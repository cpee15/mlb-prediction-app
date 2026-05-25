from copy import deepcopy

DEFAULT_OUT_SUBTYPE_SHARES = {
    "groundout": 0.44,
    "flyout": 0.31,
    "lineout_popout": 0.18,
    "other_out": 0.07,
}

REQUIRED_SUBTYPE_KEYS = [
    "groundout",
    "flyout",
    "lineout_popout",
    "other_out",
]

STRIKEOUT_KEYS = [
    "k",
    "strikeout",
    "strikeouts",
]

OUT_KEYS = [
    "out",
    "generic_out",
]

EXCLUDED_KEYS = set(
    STRIKEOUT_KEYS
    + OUT_KEYS
    + [
        "strikeout",
        "groundout",
        "flyout",
        "lineout_popout",
        "other_out",
        "generic_out_original",
        "out_subtype_share_sum",
        "pa_probability_sum_original",
        "pa_probability_sum_derived",
        "preserved_numeric_keys",
        "excluded_decomposed_keys",
        "missing_numeric_keys",
    ]
)

def _is_numeric(value):

    return isinstance(
        value,
        (int, float),
    )

def _first_numeric(
    probabilities,
    keys,
    default=0.0,
):

    for key in keys:

        value = probabilities.get(
            key
        )

        if _is_numeric(value):

            return float(value)

    return float(default)

def normalize_out_subtype_shares(
    shares=None,
):

    if shares is None:

        shares = deepcopy(
            DEFAULT_OUT_SUBTYPE_SHARES
        )

    if not isinstance(
        shares,
        dict,
    ):

        raise TypeError(
            "shares must be dict"
        )

    normalized = {}

    for key in REQUIRED_SUBTYPE_KEYS:

        if key not in shares:

            raise ValueError(
                f"missing share key: {key}"
            )

        value = shares[key]

        if not _is_numeric(
            value
        ):

            raise TypeError(
                f"share must be numeric: {key}"
            )

        value = float(value)

        if value < 0:

            raise ValueError(
                f"share cannot be negative: {key}"
            )

        normalized[key] = value

    total = sum(
        normalized.values()
    )

    if total <= 0:

        raise ValueError(
            "share total must be > 0"
        )

    return {
        key: value / total
        for key, value
        in normalized.items()
    }

def derive_outcome_subtype_probabilities(
    probabilities,
    shares=None,
    include_metadata=True,
):

    if not isinstance(
        probabilities,
        dict,
    ):

        raise TypeError(
            "probabilities must be dict"
        )

    normalized_shares = (
        normalize_out_subtype_shares(
            shares
        )
    )

    derived = {}

    original_numeric_keys = []

    preserved_numeric_keys = []

    excluded_decomposed_keys = []

    missing_numeric_keys = []

    for key, value in (
        probabilities.items()
    ):

        if not _is_numeric(
            value
        ):

            continue

        original_numeric_keys.append(
            key
        )

        if key in EXCLUDED_KEYS:

            excluded_decomposed_keys.append(
                key
            )

            continue

        derived[key] = float(value)

        preserved_numeric_keys.append(
            key
        )

    strikeout_prob = (
        _first_numeric(
            probabilities,
            STRIKEOUT_KEYS,
        )
    )

    out_prob = (
        _first_numeric(
            probabilities,
            OUT_KEYS,
        )
    )

    derived[
        "strikeout"
    ] = strikeout_prob

    derived[
        "groundout"
    ] = (
        out_prob
        * normalized_shares[
            "groundout"
        ]
    )

    derived[
        "flyout"
    ] = (
        out_prob
        * normalized_shares[
            "flyout"
        ]
    )

    derived[
        "lineout_popout"
    ] = (
        out_prob
        * normalized_shares[
            "lineout_popout"
        ]
    )

    derived[
        "other_out"
    ] = (
        out_prob
        * normalized_shares[
            "other_out"
        ]
    )

    derived[
        "generic_out_original"
    ] = out_prob

    derived[
        "out_subtype_share_sum"
    ] = sum(
        normalized_shares.values()
    )

    original_sum = sum(
        float(v)
        for v in probabilities.values()
        if _is_numeric(v)
    )

    derived_sum = sum(
        float(v)
        for k, v in derived.items()
        if (
            _is_numeric(v)
            and k
            not in {
                "generic_out_original",
                "out_subtype_share_sum",
                "pa_probability_sum_original",
                "pa_probability_sum_derived",
            }
        )
    )

    derived[
        "pa_probability_sum_original"
    ] = original_sum

    derived[
        "pa_probability_sum_derived"
    ] = derived_sum

    if include_metadata:

        derived[
            "preserved_numeric_keys"
        ] = sorted(
            preserved_numeric_keys
        )

        derived[
            "excluded_decomposed_keys"
        ] = sorted(
            excluded_decomposed_keys
        )

        derived[
            "missing_numeric_keys"
        ] = sorted(
            missing_numeric_keys
        )

    return derived
