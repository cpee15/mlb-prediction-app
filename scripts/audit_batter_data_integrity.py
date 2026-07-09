#!/usr/bin/env python
from __future__ import annotations

import json
import os

from mlb_app.database import create_tables, get_engine, get_session
from mlb_app.data_integrity_audit import build_duplicate_audit


def main() -> int:
    database_url = os.getenv("DATABASE_URL", "sqlite:///mlb.db")
    engine = get_engine(database_url)
    create_tables(engine)
    Session = get_session(engine)
    with Session() as session:
        payload = build_duplicate_audit(session)
    print(json.dumps(payload, indent=2, default=str))
    return 1 if payload.get("has_duplicates") else 0


if __name__ == "__main__":
    raise SystemExit(main())
