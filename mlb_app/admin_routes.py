"""Private MLBGPT Control Center APIs."""

from __future__ import annotations

import datetime as dt
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field, StrictBool

from .admin_access import (
    DashboardPrincipal,
    access_payload_for_user,
    capabilities_for_role,
    dashboard_session_factory,
    require_capability,
)
from .admin_configuration import (
    FEATURE_FLAG_DEFINITIONS,
    PROFILE_DEFINITIONS,
    SETTING_DEFINITIONS,
    ensure_profile_catalog,
    get_or_create_directory_profile,
    profile_key_for_role,
    record_audit_event,
    serialize_feature_flags,
    serialize_profile_catalog,
    serialize_settings,
    validate_setting_value,
    validate_target_profiles,
)
from .application_registry import list_application_surfaces
from .dashboard_report_types import list_report_types
from .database import (
    AppAdminAuditEvent,
    AppFeatureFlag,
    AppGlobalSetting,
    AppSession,
    AppUser,
    AppUserDirectoryProfile,
    AppUserPreference,
    FederatedIdentity,
)
from .my_dashboard_observability import latest_hydration_status

router = APIRouter(prefix="/admin", tags=["admin-control-center"])

LOCKED_SECTIONS = [
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
]


class AdminUserUpdateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    username: Optional[str] = Field(default=None, max_length=80)
    first_name: Optional[str] = Field(default=None, max_length=80)
    last_name: Optional[str] = Field(default=None, max_length=80)
    display_name: Optional[str] = Field(default=None, max_length=160)
    alias: Optional[str] = Field(default=None, max_length=80)
    title: Optional[str] = Field(default=None, max_length=120)
    company: Optional[str] = Field(default=None, max_length=160)
    locale: Optional[str] = Field(default=None, min_length=2, max_length=32)
    language: Optional[str] = Field(default=None, min_length=2, max_length=16)
    timezone: Optional[str] = Field(default=None, min_length=1, max_length=64)
    is_active: Optional[StrictBool] = None
    is_locked: Optional[StrictBool] = None


class AdminSettingUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    namespace: str = Field(min_length=1, max_length=64)
    key: str = Field(min_length=1, max_length=128)
    value: Any


class AdminSettingsPatchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    updates: List[AdminSettingUpdate] = Field(min_length=1, max_length=25)


class AdminFeatureFlagUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    key: str = Field(min_length=1, max_length=128)
    enabled: StrictBool
    target_profiles: List[str] = Field(default_factory=list, max_length=10)


class AdminFeatureFlagsPatchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    updates: List[AdminFeatureFlagUpdate] = Field(min_length=1, max_length=25)


def _session_factory():
    return dashboard_session_factory()


def _utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)


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
        selectable_fields = [field for field in fields if field.get("selectable", True)]
        sortable_fields = [field for field in fields if field.get("sortable")]
        item["freshness"] = sorted({
            str(field.get("freshness"))
            for field in fields
            if field.get("freshness")
        })
        item["filtering"] = {
            "supported": queryable and bool(filterable_fields),
            "field_count": len(filterable_fields) if queryable else 0,
            "selectable_field_count": len(selectable_fields) if queryable else 0,
            "logic": ["and", "or"] if queryable else [],
        }
        item["sorting"] = {
            "supported": queryable and bool(sortable_fields),
            "field_count": len(sortable_fields) if queryable else 0,
        }
    return objects


def _admin_user_payload(session, user: AppUser) -> Dict[str, Any]:
    """Preserve the Phase 1 minimized list contract."""

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


def _directory_payload(profile: AppUserDirectoryProfile) -> Dict[str, Any]:
    return {
        "public_id": profile.public_id,
        "first_name": profile.first_name,
        "last_name": profile.last_name,
        "display_name": profile.display_name,
        "alias": profile.alias,
        "title": profile.title,
        "company": profile.company,
        "is_active": bool(profile.is_active),
        "is_locked": bool(profile.is_locked),
        "locale": profile.locale,
        "language": profile.language,
        "timezone": profile.timezone,
        "session_version": profile.session_version,
        "last_login_at": profile.last_login_at.isoformat() if profile.last_login_at else None,
        "created_at": profile.created_at.isoformat() if profile.created_at else None,
        "updated_at": profile.updated_at.isoformat() if profile.updated_at else None,
    }


def _admin_user_detail(session, user: AppUser, *, actor_user_id: int) -> Dict[str, Any]:
    directory = get_or_create_directory_profile(
        session,
        user.id,
        actor_user_id=actor_user_id,
    )
    access = access_payload_for_user(session, user)
    identities = (
        session.query(FederatedIdentity)
        .filter(FederatedIdentity.user_id == user.id)
        .all()
    )
    return {
        "id": user.id,
        "username": user.username,
        "email": user.email,
        "role": access["role"],
        "profile_key": profile_key_for_role(access["role"]),
        "capabilities": list(access["capabilities"]),
        "directory": _directory_payload(directory),
        "federation": {
            "identity_count": len(identities),
            "providers": sorted({identity.provider for identity in identities}),
        },
        "created_at": user.created_at.isoformat() if user.created_at else None,
        "updated_at": user.updated_at.isoformat() if user.updated_at else None,
    }


def _clean_optional_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    cleaned = value.strip()
    return cleaned or None


def _validate_timezone(value: str) -> str:
    cleaned = value.strip()
    try:
        ZoneInfo(cleaned)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise HTTPException(status_code=400, detail="Unknown timezone") from exc
    return cleaned


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
            "profile_key": profile_key_for_role(principal.role),
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
            "mode": "configuration_foundation",
        },
        "locked_sections": LOCKED_SECTIONS,
    }


@router.get("/me")
def admin_me(
    principal: DashboardPrincipal = Depends(require_capability("admin.portal.access")),
) -> Dict[str, Any]:
    Session = _session_factory()
    with Session() as session:
        user = session.get(AppUser, principal.user_id)
        payload = _admin_user_detail(session, user, actor_user_id=principal.user_id)
        session.commit()
        return {"user": payload}


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
    return {"users": payload, "totalSize": len(payload)}


@router.get("/users/{user_id}")
def admin_user_detail(
    user_id: int,
    principal: DashboardPrincipal = Depends(require_capability("admin.users.read")),
) -> Dict[str, Any]:
    Session = _session_factory()
    with Session() as session:
        user = session.get(AppUser, user_id)
        if user is None:
            raise HTTPException(status_code=404, detail="User not found")
        payload = _admin_user_detail(session, user, actor_user_id=principal.user_id)
        session.commit()
        return {"user": payload}


@router.patch("/users/{user_id}")
def admin_user_update(
    user_id: int,
    request: AdminUserUpdateRequest,
    principal: DashboardPrincipal = Depends(require_capability("admin.users.manage")),
) -> Dict[str, Any]:
    values = request.model_dump(exclude_unset=True)
    if not values:
        raise HTTPException(status_code=400, detail="At least one profile field is required")
    Session = _session_factory()
    with Session() as session:
        user = session.get(AppUser, user_id)
        if user is None:
            raise HTTPException(status_code=404, detail="User not found")
        directory = get_or_create_directory_profile(
            session,
            user.id,
            actor_user_id=principal.user_id,
        )
        if user.id == principal.user_id and (
            values.get("is_active") is False or values.get("is_locked") is True
        ):
            raise HTTPException(status_code=400, detail="The active owner cannot deactivate or lock itself")
        before = _admin_user_detail(session, user, actor_user_id=principal.user_id)
        now = _utcnow()
        security_changed = False
        if "username" in values:
            username = _clean_optional_text(values.pop("username"))
            if not username:
                raise HTTPException(status_code=400, detail="Username is required")
            user.username = username
        for field_name in (
            "first_name", "last_name", "display_name", "alias", "title", "company",
        ):
            if field_name in values:
                setattr(directory, field_name, _clean_optional_text(values[field_name]))
        for field_name in ("locale", "language"):
            if field_name in values:
                cleaned = _clean_optional_text(values[field_name])
                if not cleaned:
                    raise HTTPException(status_code=400, detail=f"{field_name} is required")
                setattr(directory, field_name, cleaned)
        if "timezone" in values:
            directory.timezone = _validate_timezone(values["timezone"] or "")
        for field_name in ("is_active", "is_locked"):
            if field_name in values and getattr(directory, field_name) != values[field_name]:
                setattr(directory, field_name, values[field_name])
                security_changed = True
        if security_changed:
            directory.session_version = int(directory.session_version or 0) + 1
            session.query(AppSession).filter(AppSession.user_id == user.id).delete(
                synchronize_session=False
            )
        user.updated_at = now
        directory.updated_by_user_id = principal.user_id
        directory.updated_at = now
        session.flush()
        after = _admin_user_detail(session, user, actor_user_id=principal.user_id)
        record_audit_event(
            session,
            actor_user_id=principal.user_id,
            actor_session_id=principal.session_id,
            action="admin.user.updated",
            target_type="app_user",
            target_identifier=str(user.id),
            before=before,
            after=after,
        )
        session.commit()
        return {"ok": True, "user": after, "sessions_revoked": security_changed}


@router.get("/profiles")
def admin_profiles(
    principal: DashboardPrincipal = Depends(require_capability("admin.profiles.read")),
) -> Dict[str, Any]:
    del principal
    Session = _session_factory()
    with Session() as session:
        ensure_profile_catalog(session)
        session.commit()
    profiles = serialize_profile_catalog(capabilities_for_role)
    return {"profiles": profiles, "totalSize": len(profiles), "source": "server_capability_registry"}


@router.get("/settings")
def admin_settings(
    principal: DashboardPrincipal = Depends(require_capability("admin.settings.read")),
) -> Dict[str, Any]:
    del principal
    Session = _session_factory()
    with Session() as session:
        settings = serialize_settings(session)
    return {"settings": settings, "totalSize": len(settings), "source": "settings_registry"}


@router.patch("/settings")
def admin_settings_update(
    request: AdminSettingsPatchRequest,
    principal: DashboardPrincipal = Depends(require_capability("admin.settings.manage")),
) -> Dict[str, Any]:
    Session = _session_factory()
    with Session() as session:
        now = _utcnow()
        for update in request.updates:
            namespace = update.namespace.strip()
            key = update.key.strip()
            value = validate_setting_value(namespace, key, update.value)
            definition = SETTING_DEFINITIONS[(namespace, key)]
            row = (
                session.query(AppGlobalSetting)
                .filter(
                    AppGlobalSetting.namespace == namespace,
                    AppGlobalSetting.setting_key == key,
                )
                .first()
            )
            before = {"namespace": namespace, "key": key, "value": row.value_json if row else definition["default"]}
            if row is None:
                row = AppGlobalSetting(
                    namespace=namespace,
                    setting_key=key,
                    value_type=definition["value_type"],
                    default_value_json=definition["default"],
                    validation_json=definition.get("validation", {}),
                    description=definition["description"],
                    environment_override=bool(definition.get("environment_variable")),
                    created_at=now,
                )
                session.add(row)
            row.value_json = value
            row.updated_by_user_id = principal.user_id
            row.updated_at = now
            session.flush()
            record_audit_event(
                session,
                actor_user_id=principal.user_id,
                actor_session_id=principal.session_id,
                action="admin.setting.updated",
                target_type="global_setting",
                target_identifier=f"{namespace}.{key}",
                before=before,
                after={"namespace": namespace, "key": key, "value": value},
            )
        session.commit()
        return {"ok": True, "settings": serialize_settings(session)}


@router.get("/feature-flags")
def admin_feature_flags(
    principal: DashboardPrincipal = Depends(require_capability("admin.settings.read")),
) -> Dict[str, Any]:
    del principal
    Session = _session_factory()
    with Session() as session:
        flags = serialize_feature_flags(session)
    return {"feature_flags": flags, "totalSize": len(flags), "source": "feature_flag_registry"}


@router.patch("/feature-flags")
def admin_feature_flags_update(
    request: AdminFeatureFlagsPatchRequest,
    principal: DashboardPrincipal = Depends(require_capability("admin.settings.manage")),
) -> Dict[str, Any]:
    Session = _session_factory()
    with Session() as session:
        now = _utcnow()
        for update in request.updates:
            key = update.key.strip()
            if key not in FEATURE_FLAG_DEFINITIONS:
                raise HTTPException(status_code=400, detail=f"Unknown feature flag: {key}")
            targets = validate_target_profiles(update.target_profiles)
            row = (
                session.query(AppFeatureFlag)
                .filter(AppFeatureFlag.flag_key == key)
                .first()
            )
            before = {
                "key": key,
                "enabled": bool(row.enabled) if row else False,
                "target_profiles": list(row.target_profiles_json or []) if row else [],
            }
            if row is None:
                row = AppFeatureFlag(flag_key=key, created_at=now)
                session.add(row)
            row.enabled = update.enabled
            row.target_profiles_json = targets
            row.updated_by_user_id = principal.user_id
            row.updated_at = now
            session.flush()
            record_audit_event(
                session,
                actor_user_id=principal.user_id,
                actor_session_id=principal.session_id,
                action="admin.feature_flag.updated",
                target_type="feature_flag",
                target_identifier=key,
                before=before,
                after={"key": key, "enabled": update.enabled, "target_profiles": targets},
            )
        session.commit()
        return {"ok": True, "feature_flags": serialize_feature_flags(session)}


@router.get("/audit-events")
def admin_audit_events(
    limit: int = Query(default=50, ge=1, le=100),
    principal: DashboardPrincipal = Depends(require_capability("admin.audit.read")),
) -> Dict[str, Any]:
    del principal
    Session = _session_factory()
    with Session() as session:
        events = (
            session.query(AppAdminAuditEvent)
            .order_by(AppAdminAuditEvent.created_at.desc(), AppAdminAuditEvent.id.desc())
            .limit(limit)
            .all()
        )
        actor_ids = {event.actor_user_id for event in events}
        actors = {
            user.id: user.username
            for user in session.query(AppUser).filter(AppUser.id.in_(actor_ids)).all()
        } if actor_ids else {}
        payload = [{
            "id": event.public_id,
            "actor": {"id": event.actor_user_id, "username": actors.get(event.actor_user_id)},
            "action": event.action,
            "target_type": event.target_type,
            "target_identifier": event.target_identifier,
            "before": event.before_json,
            "after": event.after_json,
            "source": event.source,
            "created_at": event.created_at.isoformat() if event.created_at else None,
        } for event in events]
    return {"audit_events": payload, "totalSize": len(payload), "limit": limit}
