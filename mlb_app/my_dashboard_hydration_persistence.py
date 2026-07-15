from __future__ import annotations

import os
from typing import Any, Dict

from .database import create_tables, get_engine, get_session
from .my_dashboard_dataset import dashboard_dataset_status, hydrate_dashboard_dataset


def _session_factory():
    database_url = os.getenv("DATABASE_URL", "sqlite:///mlb.db")
    engine = get_engine(database_url)
    create_tables(engine)
    return get_session(engine)


def persist_hydration_payload(run: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
    """Persist completed batch hydration payloads into My Dashboard-owned datasets.

    The route already builds the authoritative, unfiltered component payloads. This
    function promotes those same rows into the persisted Workbench dataset without
    rerunning any analytical service. Empty payloads never replace a valid dataset.
    """

    target_date = str(run.get("target_date") or payload.get("date") or "")[:10]
    active_lineups = bool(run.get("active_lineups", payload.get("active_lineups", False)))
    force = bool(run.get("force_requested", False))
    results = payload.get("results") if isinstance(payload.get("results"), dict) else {}
    persisted: Dict[str, Any] = {}

    factory = _session_factory()
    with factory() as session:
        for component, component_payload in results.items():
            if not isinstance(component_payload, dict):
                persisted[component] = {
                    "hydrated": False,
                    "skipped": True,
                    "reason": "invalid_component_payload",
                }
                continue

            rows = component_payload.get("items") or component_payload.get("records") or []
            if not rows:
                status = dashboard_dataset_status(
                    session=session,
                    date=target_date,
                    component=component,
                    active_lineups=active_lineups,
                )
                persisted[component] = {
                    **status,
                    "hydrated": False,
                    "skipped": True,
                    "reason": "empty_payload_preserved_previous_dataset",
                }
                continue

            status = dashboard_dataset_status(
                session=session,
                date=target_date,
                component=component,
                active_lineups=active_lineups,
            )
            if status.get("ready") and not force:
                persisted[component] = {
                    **status,
                    "hydrated": False,
                    "skipped": True,
                    "reason": "current_dataset_already_ready",
                }
                continue

            persisted[component] = hydrate_dashboard_dataset(
                session=session,
                date=target_date,
                component=component,
                payload_builder=lambda value=component_payload: value,
                active_lineups=active_lineups,
                force=force,
                ttl_seconds=None,
                solver_version="my_dashboard_solver_v1",
            )

    return {
        "dataset_source": "my_dashboard_records",
        "dataset_date": target_date,
        "dataset_mode": "active_lineups" if active_lineups else "standard",
        "force_requested": force,
        "components": persisted,
        "component_count": len(persisted),
        "hydrated_component_count": sum(1 for value in persisted.values() if value.get("hydrated")),
        "skipped_component_count": sum(1 for value in persisted.values() if value.get("skipped")),
        "dataset_row_count": sum(int(value.get("dataset_row_count") or 0) for value in persisted.values()),
    }
