from __future__ import annotations

import csv
import json
import random
from pathlib import Path

from mlb_app.simulation.inning_simulator import (
    simulate_half_inning,
)

OUTPUT_DIR = Path("tmp")
OUTPUT_DIR.mkdir(exist_ok=True)

PROBABILITIES = {
    "k": 0.225,
    "bb": 0.085,
    "hbp": 0.011,
    "single": 0.145,
    "double": 0.045,
    "triple": 0.004,
    "hr": 0.030,
    "reached_on_error": 0.007,
    "out": 0.448,
}

rows = []
differences = []

for seed in range(250):

    baseline = simulate_half_inning(
        PROBABILITIES,
        rng=random.Random(seed),
    )

    explicit_current = simulate_half_inning(
        PROBABILITIES,
        rng=random.Random(seed),
        transition_mode="current",
    )

    equivalent = baseline == explicit_current

    row = {
        "seed": seed,
        "equivalent": equivalent,
        "baseline_runs": baseline["runs"],
        "explicit_runs": explicit_current["runs"],
        "baseline_pas": baseline["plate_appearances"],
        "explicit_pas": explicit_current["plate_appearances"],
    }

    rows.append(row)

    if not equivalent:
        differences.append(row)

invalid_mode_passed = False

try:

    simulate_half_inning(
        PROBABILITIES,
        transition_mode="bad_mode",
    )

except ValueError:
    invalid_mode_passed = True

if differences:
    diagnosis = "transition_candidate_default_regression"
elif not invalid_mode_passed:
    diagnosis = "transition_candidate_invalid_mode_failed"
else:
    diagnosis = "transition_candidate_default_equivalent"

summary = {
    "diagnosis": diagnosis,
    "rows_tested": len(rows),
    "differences_found": len(differences),
    "invalid_mode_passed": invalid_mode_passed,
}

with open(
    OUTPUT_DIR / "transition_candidate_equivalence.json",
    "w",
) as handle:
    json.dump(
        {
            "summary": summary,
            "differences": differences,
        },
        handle,
        indent=2,
    )

with open(
    OUTPUT_DIR / "transition_candidate_equivalence.csv",
    "w",
    newline="",
) as handle:

    writer = csv.DictWriter(
        handle,
        fieldnames=rows[0].keys(),
    )

    writer.writeheader()
    writer.writerows(rows)

print(json.dumps(summary, indent=2))
