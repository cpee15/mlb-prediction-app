"""Owner-only, read-only MLBGPT Control Center APIs."""

from __future__ import annotations

from typing import Any, Dict, List

from fastapi import APIRouter, Depends

from .admin_access import (
    DashboardPrincipal,
    access_payload_for_user,
    dashboard_session_factory,
    require_capability,
)
from .application_registry import list_application_surfaces
from .dashboard_report_types import list_report_types
from .database import AppUser, AppUserPreference
from .my_dashboard_observability import latest_hydration_status

router = APIRouter(prefix="/admin", tags=["admin-control-center"])

LOCKED_SECTIONS = [
    {
        "key": "settings",
        "label": "Settings",
        "status": "locked",
        "next_phase": "Allowlisted settings and feature controls",
    },
    {
        "key": "operations",
        "label": "Operations",
        "status": "locked",
        "next_phase": "Authenticated operational actions and cron-safe controls",
    },
    {
        "key": "workbench",
        "label": "Workbench",
        "status": "locked",
        "next_phase": "Constrained MLBGPT report language and compiler",
    },
    {
        "key": "audit",
        "label": "Audit Log",
        "status": "locked",
        "next_phase": "Administrative action history",
    },
]


def _session_factory():
    return dashboard_session_factory()


def _safe_hydration_summary() -> Dict[str, Any]:
    latest = latest_hydration_status() or {}
    if not isinstance(latest, dict):
        latest = {}
    components = latest.get("components")
    warnings = latest.get("warnings")
    return {
        "status": latest.get("status", "unknown"),
        "target_date": latest.get("target_date"),
        "started_at": latest.get("started_at"),
        "completed_at": latest.get("completed_at"),
        "duration_ms": latest.get("duration_ms"),
        "component_count": len(components) if isinstance(components, dict) else 0,
        "warning_count": len(warnings) if isinstance(warnings, list) else 0,
        "has_error": bool(latest.get("error")),
    }


def _object_manager_descriptors() -> List[Dict[str, Any]]:
    objects = list_report_types()
    for item in objects:
        fields = item.get("fields") if isinstance(item.get("fields"), list) else []
        queryable = bool(item.get("queryable"))
        filterable_fields = [field for field in fields if field.get("filterable")]
        sortable_fields = [field for field in fields if field.get("sortable")]
        item["freshness"] = sorted({
            str(field.get("freshness"))
            for field in fields
            if field.get("freshness")
        })
        item["filtering"] = {
            "supported": queryable and bool(filterable_fields),
            "field_count": len(filterable_fields) if queryable else 0,
        }
        item["sorting"] = {
            "supported": queryable and bool(sortable_fields),
            "field_count": len(sortable_fields) if queryable else 0,
        }
    return objects


def _admin_user_payload(session, user: AppUser) -> Dict[str, Any]:
    prefs = (
        session.query(AppUserPreference)
        .filter(AppUserPreference.user_id == user.id)
        .first()
    )
    access = access_payload_for_user(session, user)
    return {
        "id": user.id,
        "username": user.username,
        "email": user.email,
        "role": access["role"],
        "plan": prefs.plan_type if prefs and prefs.plan_type else "free",
        "capabilities": list(access["capabilities"]),
        "created_at": user.created_at.isoformat() if user.created_at else None,
        "updated_at": user.updated_at.isoformat() if user.updated_at else None,
    }


@router.get("/overview")
def admin_overview(
    principal: DashboardPrincipal = Depends(require_capability("admin.portal.access")),
) -> Dict[str, Any]:
    objects = _object_manager_descriptors()
    apps = list_application_surfaces()
    Session = _session_factory()
    with Session() as session:
        user_count = session.query(AppUser).count()
    return {
        "administrator": {
            "id": principal.user_id,
            "username": principal.username,
            "email": principal.email,
            "role": principal.role,
            "capabilities": list(principal.capabilities),
        },
        "counts": {
            "capabilities": len(principal.capabilities),
            "objects": len(objects),
            "queryable_objects": sum(bool(item.get("queryable")) for item in objects),
            "application_surfaces": len(apps),
            "users": user_count,
        },
        "operations": {
            "hydration": _safe_hydration_summary(),
            "mode": "read_only",
        },
        "locked_sections": LOCKED_SECTIONS,
    }


@router.get("/objects")
def admin_objects(
    principal: DashboardPrincipal = Depends(require_capability("admin.objects.read")),
) -> Dict[str, Any]:
    del principal
    objects = _object_manager_descriptors()
    return {
        "objects": objects,
        "totalSize": len(objects),
        "queryableSize": sum(bool(item.get("queryable")) for item in objects),
        "source": "dashboard_report_types",
    }


@router.get("/apps")
def admin_apps(
    principal: DashboardPrincipal = Depends(require_capability("admin.apps.read")),
) -> Dict[str, Any]:
    del principal
    apps = list_application_surfaces()
    return {
        "apps": apps,
        "totalSize": len(apps),
        "source": "application_registry",
    }


@router.get("/users")
def admin_users(
    principal: DashboardPrincipal = Depends(require_capability("admin.users.read")),
) -> Dict[str, Any]:
    del principal
    Session = _session_factory()
    with Session() as session:
        users: List[AppUser] = (
            session.query(AppUser)
            .order_by(AppUser.created_at.asc(), AppUser.id.asc())
            .all()
        )
        payload = [_admin_user_payload(session, user) for user in users]
    return {
        "users": payload,
        "totalSize": len(payload),
    }
