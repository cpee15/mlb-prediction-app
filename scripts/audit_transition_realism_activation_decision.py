import csv
import json
from pathlib import Path
from typing import Any, Dict

TMP_DIR = Path("tmp")
TMP_DIR.mkdir(exist_ok=True)

FILES = {
    "base_out_transition_realism":
        TMP_DIR / "base_out_transition_realism.json",

    "transition_realism_prototype":
        TMP_DIR / "transition_realism_prototype.json",

    "transition_candidate_equivalence":
        TMP_DIR / "transition_candidate_equivalence.json",

    "transition_realism_candidate":
        TMP_DIR / "transition_realism_candidate.json",

    "transition_parameter_calibration":
        TMP_DIR / "transition_parameter_calibration.json",
}

def safe_load_json(path: Path):

    if not path.exists():
        return None

    try:

        with open(path) as f:
            return json.load(f)

    except Exception:
        return None


def unwrap_summary(payload):

    if payload is None:
        return None

    if (
        isinstance(payload, dict)
        and "summary" in payload
        and isinstance(payload["summary"], dict)
    ):
        return payload["summary"]

    return payload

def write_csv(path, rows):

    if not rows:
        return

    with open(
        path,
        "w",
        newline="",
    ) as f:

        writer = csv.DictWriter(
            f,
            fieldnames=rows[0].keys(),
        )

        writer.writeheader()
        writer.writerows(rows)

raw_evidence = {
    key: safe_load_json(path)
    for key, path in FILES.items()
}

evidence = {
    key: unwrap_summary(value)
    for key, value in raw_evidence.items()
}

dimension_scores = {}
risks = []
required_before_default_activation = []

confidence = 0.0

#
# architecture safety
#

calibration = evidence.get(
    "transition_parameter_calibration"
)

if calibration:

    parser_missing = calibration.get(
        "parser_missing_count",
        999,
    )

    actual_missing = calibration.get(
        "actual_missing_count",
        999,
    )

    if (
        parser_missing == 0
        and actual_missing == 0
    ):

        dimension_scores[
            "architecture_safety"
        ] = "pass"

        confidence += 0.15

    else:

        dimension_scores[
            "architecture_safety"
        ] = "fail"

        confidence -= 0.15

        risks.append(
            "parser_or_actual_result_failures_present"
        )

else:

    dimension_scores[
        "architecture_safety"
    ] = "missing"

    confidence -= 0.10

#
# default equivalence
#

equivalence = evidence.get(
    "transition_candidate_equivalence"
)

if equivalence:

    if (
        equivalence.get(
            "differences_found"
        ) == 0
        and equivalence.get(
            "invalid_mode_passed"
        ) is True
    ):

        dimension_scores[
            "default_equivalence"
        ] = "pass"

        confidence += 0.15

    else:

        dimension_scores[
            "default_equivalence"
        ] = "fail"

        confidence -= 0.25

        risks.append(
            "default_equivalence_regression"
        )

else:

    dimension_scores[
        "default_equivalence"
    ] = "missing"

    confidence -= 0.10

#
# mechanics need
#

transition_realism = evidence.get(
    "base_out_transition_realism"
)

if transition_realism:

    bottlenecks = (
        transition_realism.get(
            "largest_remaining_bottlenecks"
        )
        or transition_realism.get(
            "largest_missing_realism_effects"
        )
        or transition_realism.get(
            "bottlenecks"
        )
        or []
    )

    if bottlenecks:

        dimension_scores[
            "mechanics_need"
        ] = "pass"

        confidence += 0.15

    else:

        dimension_scores[
            "mechanics_need"
        ] = "warn"

else:

    dimension_scores[
        "mechanics_need"
    ] = "missing"

    confidence -= 0.10

#
# prototype success
#

prototype = evidence.get(
    "transition_realism_prototype"
)

if prototype:

    repairs = prototype.get(
        "prototype_repairs",
        0,
    )

    large_shift_count = prototype.get(
        "large_shift_count",
        999,
    )

    if (
        repairs >= 2
        and large_shift_count == 0
    ):

        dimension_scores[
            "prototype_success"
        ] = "pass"

        confidence += 0.15

    else:

        dimension_scores[
            "prototype_success"
        ] = "warn"

        risks.append(
            "prototype_not_fully_stable"
        )

else:

    dimension_scores[
        "prototype_success"
    ] = "missing"

    confidence -= 0.10

#
# candidate safety
#

candidate = evidence.get(
    "transition_realism_candidate"
)

if candidate:

    large_shift_count = candidate.get(
        "large_shift_count",
        999,
    )

    candidate_repair = candidate.get(
        "candidate_repair",
        False,
    )

    if (
        large_shift_count == 0
        and candidate_repair is True
    ):

        dimension_scores[
            "candidate_safety"
        ] = "pass"

        confidence += 0.15

    else:

        dimension_scores[
            "candidate_safety"
        ] = "warn"

        confidence -= 0.10

        risks.append(
            "candidate_shift_risk_present"
        )

else:

    dimension_scores[
        "candidate_safety"
    ] = "missing"

    confidence -= 0.10

#
# calibration support
#

best_config = None

if calibration:

    best_config = calibration.get(
        "best_config"
    )

    if best_config:

        dimension_scores[
            "calibration_support"
        ] = "pass"

        confidence += 0.15

    else:

        dimension_scores[
            "calibration_support"
        ] = "warn"

        risks.append(
            "calibration_no_clear_winner"
        )

else:

    dimension_scores[
        "calibration_support"
    ] = "missing"

    confidence -= 0.10

#
# parameter stability
#

if best_config == "low_advancement":

    dimension_scores[
        "parameter_stability"
    ] = "warn"

    confidence -= 0.05

    risks.append(
        "smoke_full_parameter_divergence_detected"
    )

    required_before_default_activation.append(
        "stabilize_transition_parameter_selection"
    )

else:

    dimension_scores[
        "parameter_stability"
    ] = "pass"

    confidence += 0.10

#
# activation risk
#

if (
    dimension_scores.get(
        "architecture_safety"
    ) == "pass"
    and dimension_scores.get(
        "default_equivalence"
    ) == "pass"
):

    dimension_scores[
        "activation_risk"
    ] = "warn"

else:

    dimension_scores[
        "activation_risk"
    ] = "fail"

#
# decision
#

decision = (
    "hold_for_parameter_stabilization"
)

diagnosis = (
    "transition_activation_decision_hold_for_stabilization"
)

if (
    dimension_scores.get(
        "architecture_safety"
    ) == "pass"
    and dimension_scores.get(
        "default_equivalence"
    ) == "pass"
    and dimension_scores.get(
        "candidate_safety"
    ) == "pass"
    and best_config == "low_advancement"
):

    decision = (
        "activate_low_advancement_candidate_only"
    )

    diagnosis = (
        "transition_activation_decision_candidate_only_supported"
    )

#
# clamp confidence
#

confidence = max(
    0.0,
    min(
        1.0,
        round(
            confidence,
            4,
        ),
    ),
)

summary = {
    "diagnosis":
        diagnosis,

    "decision":
        decision,

    "confidence":
        confidence,

    "evidence_summary": {
        "available_evidence": [
            key
            for key, value
            in evidence.items()
            if value is not None
        ],

        "missing_evidence": [
            key
            for key, value
            in evidence.items()
            if value is None
        ],

        "best_config":
            best_config,
    },

    "dimension_scores":
        dimension_scores,

    "activation_recommendation": {
        "default_transition_mode":
            "current",

        "candidate_transition_mode":
            "realism_candidate",

        "preferred_candidate_profile":
            best_config,

        "activate_by_default":
            False,
    },

    "risks":
        risks,

    "required_before_default_activation":
        required_before_default_activation,

    "recommended_next_step":
        "layer_6m_candidate_profile_plumbing_audit",
}

with open(
    TMP_DIR
    / "transition_realism_activation_decision.json",
    "w",
) as f:

    json.dump(
        summary,
        f,
        indent=2,
    )

dimension_rows = []

for key, value in (
    dimension_scores.items()
):

    dimension_rows.append(
        {
            "dimension": key,
            "score": value,
        }
    )

risk_rows = []

for risk in risks:

    risk_rows.append(
        {
            "risk": risk
        }
    )

write_csv(
    TMP_DIR
    / "transition_realism_activation_decision_dimensions.csv",
    dimension_rows,
)

write_csv(
    TMP_DIR
    / "transition_realism_activation_decision_risks.csv",
    risk_rows,
)

print(
    json.dumps(
        summary,
        indent=2,
    )
)
