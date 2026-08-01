"""Run hitter-profile canary evidence through acceptance gates."""

from __future__ import annotations

import argparse
import json

from mlb_app.simulation.shadow.hitter_profile_canary_acceptance_gate import (
    evaluate_hitter_profile_canary_acceptance,
)
from scripts.audit_shadow_hitter_profile_canary import (
    run_live_audit,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--season",
        type=int,
    )
    parser.add_argument(
        "--as-of-date",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=250,
    )
    args = parser.parse_args()

    audit = run_live_audit(
        season=args.season,
        as_of_date=args.as_of_date,
        limit=args.limit,
    )
    result = (
        evaluate_hitter_profile_canary_acceptance(
            audit
        )
    )
    result["audit_provenance"] = {
        "schema_version":
            audit.get("schema_version"),
        "season":
            audit.get("season"),
        "as_of_date":
            audit.get("as_of_date"),
        "audit_limit":
            audit.get("audit_limit"),
        "candidate_population_count":
            audit.get(
                "candidate_population_count"
            ),
    }

    print(
        json.dumps(
            result,
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
