"""Evaluate live hitter-profile simulation-shadow acceptance."""

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
from mlb_app.simulation.shadow.hitter_profile_live_simulation_shadow_window import (
    run_hitter_profile_live_simulation_shadow_window,
)
from mlb_app.simulation.shadow.hitter_profile_simulation_shadow_acceptance_gate import (
    evaluate_hitter_profile_simulation_shadow_acceptance,
)
from scripts.audit_shadow_hitter_profile_canary import (
    run_live_audit,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--target-date",
        required=True,
    )
    parser.add_argument(
        "--season",
        type=int,
    )
    parser.add_argument(
        "--game-limit",
        type=int,
        default=15,
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

    season = (
        args.season
        if args.season is not None
        else int(args.target_date[:4])
    )

    canary_audit = run_live_audit(
        season=season,
        as_of_date=args.target_date,
        limit=args.canary_limit,
    )
    canary_gate = (
        evaluate_hitter_profile_canary_acceptance(
            canary_audit
        )
    )

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
        window = (
            run_hitter_profile_live_simulation_shadow_window(
                session,
                enabled=True,
                target_date=args.target_date,
                acceptance_gate=canary_gate,
                simulation_count=(
                    args.simulation_count
                ),
                game_limit=args.game_limit,
            )
        )
    finally:
        session.close()

    result = (
        evaluate_hitter_profile_simulation_shadow_acceptance(
            window
        )
    )
    result["audit_provenance"] = {
        "target_date": args.target_date,
        "season": season,
        "game_limit": args.game_limit,
        "simulation_count":
            args.simulation_count,
        "canary_limit": args.canary_limit,
        "canary_audit_status":
            canary_audit.get("status"),
        "canary_gate_status":
            canary_gate.get("status"),
        "canary_gate_passed":
            canary_gate.get("gate_passed"),
        "window_schema_version":
            window.get("schema_version"),
        "window_status":
            window.get("status"),
    }
    result["database_writes_performed"] = False
    result["production_authority_changed"] = False

    print(
        json.dumps(
            result,
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
