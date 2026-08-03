"""Collect multi-date hitter-profile simulation-shadow evidence."""

from __future__ import annotations

import argparse
import json
import os

from mlb_app.database import (
    create_tables,
    get_engine,
    get_session,
)
from mlb_app.simulation.shadow.hitter_profile_canary_acceptance_gate import (
    evaluate_hitter_profile_canary_acceptance,
)
from mlb_app.simulation.shadow.hitter_profile_simulation_shadow_evidence_collection import (
    collect_hitter_profile_simulation_shadow_evidence,
)
from scripts.audit_shadow_hitter_profile_canary import (
    run_live_audit,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--target-date",
        action="append",
        required=True,
        dest="target_dates",
    )
    parser.add_argument(
        "--game-limit",
        type=int,
        default=5,
    )
    parser.add_argument(
        "--simulation-count",
        type=int,
        default=1000,
    )
    parser.add_argument(
        "--canary-limit",
        type=int,
        default=250,
    )
    args = parser.parse_args()

    target_dates = sorted(
        set(args.target_dates)
    )
    gates_by_date = {}
    canary_provenance = {}

    for target_date in target_dates:
        season = int(target_date[:4])
        audit = run_live_audit(
            season=season,
            as_of_date=target_date,
            limit=args.canary_limit,
        )
        gate = (
            evaluate_hitter_profile_canary_acceptance(
                audit
            )
        )
        gates_by_date[target_date] = gate
        canary_provenance[target_date] = {
            "season": audit.get("season"),
            "as_of_date":
                audit.get("as_of_date"),
            "audit_limit":
                audit.get("audit_limit"),
            "audit_status":
                audit.get("status"),
            "gate_status":
                gate.get("status"),
            "gate_passed":
                gate.get("gate_passed"),
            "gate_blockers":
                gate.get("blockers"),
        }

    engine = get_engine(
        os.getenv(
            "DATABASE_URL",
            "sqlite:///mlb.db",
        )
    )
    create_tables(engine)
    Session = get_session(engine)
    session = Session()

    try:
        result = (
            collect_hitter_profile_simulation_shadow_evidence(
                session,
                enabled=True,
                target_dates=target_dates,
                acceptance_gates_by_date=(
                    gates_by_date
                ),
                simulation_count=(
                    args.simulation_count
                ),
                game_limit=args.game_limit,
            )
        )
    finally:
        session.close()

    result["canary_provenance_by_date"] = (
        canary_provenance
    )
    result[
        "database_writes_performed"
    ] = False
    result[
        "production_authority_changed"
    ] = False

    print(
        json.dumps(
            result,
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
