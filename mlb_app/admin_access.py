"""Server-owned MyDashboard identity, role, and capability resolution."""

from __future__ import annotations

import datetime as dt
import os
import re
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

from fastapi import Cookie, Depends, Header, HTTPException

from .database import (
    AppSession,
    AppUser,
    AppUserRole,
    create_tables,
    get_engine,
    get_session,
)

ROLE_USER = "user"
ROLE_ADMIN = "admin"
VALID_ROLES = frozenset({ROLE_USER, ROLE_ADMIN})

USER_CAPABILITIES: Tuple[str, ...] = tuple(sorted({
    "dashboard.export",
    "dashboard.folders.manage",
    "dashboard.reports.filter",
    "dashboard.reports.manage",
    "dashboard.reports.paginate",
    "dashboard.reports.run",
    "dashboard.reports.sort",
}))

ADMIN_CAPABILITIES: Tuple[str, ...] = tuple(sorted({
    *USER_CAPABILITIES,
    "admin.apps.read",
    "admin.audit.read",
    "admin.objects.read",
    "admin.operations.read",
    "admin.portal.access",
    "admin.settings.read",
    "admin.users.read",
    "workbench.advanced",
}))


@dataclass(frozen=True)
class DashboardPrincipal:
    user_id: int
    email: str
    username: str
    role: str
    capabilities: Tuple[str, ...]
    session_id: int
    session_created_at: dt.datetime
    session_expires_at: dt.datetime

    def has_capability(self, capability: str) -> bool:
        return capability in self.capabilities

    def access_payload(self) -> Dict[str, object]:
        return {
            "role": self.role,
            "capabilities": list(self.capabilities),
        }


def _utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)


def dashboard_session_factory():
    database_url = (
        os.getenv("DATABASE_URL")
        or os.getenv("SQLALCHEMY_DATABASE_URL")
        or os.getenv("POSTGRES_URL")
        or "sqlite:///mlb.db"
    )
    engine = get_engine(database_url)
    create_tables(engine)
    return get_session(engine)


def normalize_email(value: str) -> str:
    return str(value or "").strip().lower()


def configured_admin_emails(raw: Optional[str] = None) -> Tuple[str, ...]:
    source = os.getenv("MLBGPT_ADMIN_EMAILS", "") if raw is None else raw
    values = {
        normalize_email(value)
        for value in re.split(r"[,;\s]+", source or "")
        if normalize_email(value)
    }
    return tuple(sorted(values))


def is_configured_admin_email(email: str, raw: Optional[str] = None) -> bool:
    return normalize_email(email) in configured_admin_emails(raw)


def capabilities_for_role(role: str) -> Tuple[str, ...]:
    return ADMIN_CAPABILITIES if role == ROLE_ADMIN else USER_CAPABILITIES


def role_assignment_for_user(session, user_id: int) -> Optional[AppUserRole]:
    return (
        session.query(AppUserRole)
        .filter(AppUserRole.user_id == user_id)
        .first()
    )


def resolved_role_for_user(
    session,
    user: AppUser,
    *,
    session_created_at: Optional[dt.datetime] = None,
) -> str:
    assignment = role_assignment_for_user(session, user.id)
    if not assignment or assignment.role not in VALID_ROLES:
        return ROLE_USER
    if assignment.role != ROLE_ADMIN:
        return ROLE_USER
    if not user.password_hash:
        return ROLE_USER
    if (
        session_created_at is not None
        and assignment.assigned_at is not None
        and session_created_at < assignment.assigned_at
    ):
        return ROLE_USER
    return ROLE_ADMIN


def access_payload_for_user(
    session,
    user: AppUser,
    *,
    session_created_at: Optional[dt.datetime] = None,
) -> Dict[str, object]:
    role = resolved_role_for_user(
        session,
        user,
        session_created_at=session_created_at,
    )
    return {
        "role": role,
        "capabilities": list(capabilities_for_role(role)),
    }


def ensure_owner_admin_role(
    session,
    user: AppUser,
    *,
    verified_at: dt.datetime,
) -> bool:
    """Promote an existing password-verified allowlisted owner and revoke stale sessions."""

    if not is_configured_admin_email(user.email):
        return False
    if not user.password_hash:
        raise HTTPException(
            status_code=409,
            detail="Account recovery is required before administrator access can be enabled.",
        )

    assignment = role_assignment_for_user(session, user.id)
    if assignment and assignment.role == ROLE_ADMIN:
        return False

    # Sessions created before the verified role grant must never inherit admin access.
    session.query(AppSession).filter(AppSession.user_id == user.id).delete(
        synchronize_session=False
    )

    if assignment is None:
        assignment = AppUserRole(
            user_id=user.id,
            role=ROLE_ADMIN,
            assignment_source="owner_email_allowlist_verified_login",
            assigned_at=verified_at,
            verified_at=verified_at,
            updated_at=verified_at,
        )
        session.add(assignment)
    else:
        assignment.role = ROLE_ADMIN
        assignment.assignment_source = "owner_email_allowlist_verified_login"
        assignment.assigned_at = verified_at
        assignment.verified_at = verified_at
        assignment.updated_at = verified_at
    session.flush()
    return True


def resolve_session_token(
    cookie_token: Optional[str],
    header_token: Optional[str],
) -> Optional[str]:
    return header_token or cookie_token


def resolve_principal(session, token: Optional[str]) -> Optional[DashboardPrincipal]:
    if not token:
        return None
    now = _utcnow()
    db_session = (
        session.query(AppSession)
        .filter(AppSession.session_token == token, AppSession.expires_at > now)
        .order_by(AppSession.id.desc())
        .first()
    )
    if not db_session:
        return None
    user = session.query(AppUser).filter(AppUser.id == db_session.user_id).first()
    if not user:
        return None
    db_session.last_seen_at = now
    role = resolved_role_for_user(
        session,
        user,
        session_created_at=db_session.created_at,
    )
    return DashboardPrincipal(
        user_id=user.id,
        email=user.email,
        username=user.username,
        role=role,
        capabilities=capabilities_for_role(role),
        session_id=db_session.id,
        session_created_at=db_session.created_at,
        session_expires_at=db_session.expires_at,
    )


def current_dashboard_principal(
    mlb_dashboard_session: Optional[str] = Cookie(default=None),
    x_dashboard_session: Optional[str] = Header(
        default=None,
        alias="X-Dashboard-Session",
    ),
) -> DashboardPrincipal:
    Session = dashboard_session_factory()
    with Session() as session:
        principal = resolve_principal(
            session,
            resolve_session_token(mlb_dashboard_session, x_dashboard_session),
        )
        if not principal:
            raise HTTPException(status_code=401, detail="Dashboard sign-in required")
        session.commit()
        return principal


def require_capability(capability: str):
    def dependency(
        principal: DashboardPrincipal = Depends(current_dashboard_principal),
    ) -> DashboardPrincipal:
        if not principal.has_capability(capability):
            raise HTTPException(status_code=403, detail="Administrator access required")
        return principal

    return dependency
