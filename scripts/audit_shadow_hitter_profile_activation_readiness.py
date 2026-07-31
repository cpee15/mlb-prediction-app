#!/usr/bin/env python3
"""Report synthesized hitter-profile activation readiness."""

from __future__ import annotations

import json

from mlb_app.simulation.shadow.hitter_profile_activation_readiness import (
    synthesize_hitter_profile_activation_readiness,
)


def main() -> int:
    result = (
        synthesize_hitter_profile_activation_readiness()
    )
    print(
        json.dumps(
            result,
            allow_nan=False,
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
