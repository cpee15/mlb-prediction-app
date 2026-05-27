from __future__ import annotations

import base64
import datetime as dt
import hashlib
import hmac
import json
import os
import secrets
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Cookie, Header, HTTPException, Query, Response
from pydantic import BaseModel, Field

from .database import (
    AppDashboardFolder,
    AppDashboardItem,
    AppSession,
    AppUser,
    AppUserPreference,
    create_tables,
    get_engine,
    get_session,
)
from .model_tracker import (
    list_tracker_rows,
    refresh_tracker_results,
)
from .model_tracker_safe_snapshot import build_tracker_snapshot_safe

router = APIRouter(tags=["model-tracker", "my-dashboard"])

DASHBOARD_SESSION_COOKIE = "mlb_dashboard_session"
DASHBOARD_SESSION_HOURS = 6
FEATURE_CHOICES = [
    "Matchups",
    "Daily Odds",
    "Model Projections",
    "News",
    "Props",
    "Pitchers",
    "Batters",
]
DEFAULT_COMPONENTS = [
    {
        "key": "hitters",
        "title": "My Top Hitters Today",
        "description": "Unique hitter board from Batter vs Arsenal, pitch usage, damage quality, and model context.",
        "source_type": "seeded_component",
    },
    {
        "key": "pitchers",
        "title": "My Top Pitchers Today",
        "description": "Pitcher lean board using K profile, contact suppression, opponent offense, and arsenal context.",
        "source_type": "seeded_component",
    },
    {
        "key": "teams",
        "title": "My Top Teams Today",
        "description": "Team board from model side edge, expected runs, offense profile, and opponent weaknesses.",
        "source_type": "seeded_component",
    },
    {
        "key": "totals",
        "title": "Game total watchlist from projected runs, run environment, and simulation context.",
        "description": "Game total watchlist from projected runs, run environment, and simulation context.",
        "source_type": "seeded_component",
    },
    {
        "key": "overall_players",
        "title": "My Top Overall Players Today",
        "description": "Combined unique player board blending hitter and pitcher model-solver scores.",
        "source_type": "seeded_component",
    },
]
DEFAULT_FILTER_FIELDS = ["search_text", "team", "opponent", "min_score", "max_score", "min_confidence", "pitch_type", "category", "source"]


class DashboardProfileRequest(BaseModel):
    email: str
    username: str
    password: Optional[str] = None
    feature_interests: List[str] = Field(default_factory=list)
    wants_newsletter: bool = False
    plan_type: Optional[str] = "free"


class DashboardItemCreateRequest(BaseModel):
    folder_id: int
    source_tab: str
    source_type: str
    title: str
    subtitle: Optional[str] = None
    payload_json: Dict[str, Any]
    filter_json: Optional[Dict[str, Any]] = None
    sort_json: Optional[Dict[str, Any]] = None
    pin_order: Optional[int] = None
    notes: Optional[str] = None


class DashboardFolderCreateRequest(BaseModel):
    folder_name: str
    folder_date: Optional[str] = None
    is_default: bool = False



def _session_factory():
    database_url = os.getenv("DATABASE_URL") or os.getenv("SQLALCHEMY_DATABASE_URL") or os.getenv("POSTGRES_URL") or "sqlite:///mlb.db"
    engine = get_engine(database_url)
    create_tables(engine)
    return get_session(engine)



def _target_date(value: Optional[str]) -> str:
    target = (value or dt.date.today().isoformat())[:10]
    try:
        dt.date.fromisoformat(target)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid date: {value}") from exc
    return target



def _utcnow() -> dt.datetime:
    return dt.datetime.utcnow()



def _cookie_settings() -> Dict[str, Any]:
    same_site = str(os.getenv("DASHBOARD_COOKIE_SAMESITE", "none") or "none").lower()
    if same_site not in {"lax", "strict", "none"}:
        same_site = "none"
    secure_default = same_site == "none"
    secure = str(os.getenv("DASHBOARD_COOKIE_SECURE", "1" if secure_default else "0")).lower() in {"1", "true", "yes", "on"}
    if same_site == "none":
        secure = True
    return {
        "httponly": True,
        "samesite": same_site,
        "secure": secure,
        "max_age": DASHBOARD_SESSION_HOURS * 60 * 60,
        "path": "/",
    }



def _normalize_email(email: str) -> str:
    return (email or "").strip().lower()



def _normalize_username(username: str) -> str:
    return (username or "").strip()



def _hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    derived = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 310000)
    return f"pbkdf2_sha256${base64.b64encode(salt).decode()}${base64.b64encode(derived).decode()}"



def _verify_password(password: str, stored: Optional[str]) -> bool:
    if not stored or "$" not in stored:
        return False
    try:
        _, salt_b64, hash_b64 = stored.split("$", 2)
        salt = base64.b64decode(salt_b64.encode())
        expected = base64.b64decode(hash_b64.encode())
        candidate = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 310000)
        return hmac.compare_digest(candidate, expected)
    except Exception:
        return False



def _resolve_session_token(cookie_token: Optional[str], header_token: Optional[str]) -> Optional[str]:
    return header_token or cookie_token



def _serialize_user(user: AppUser, prefs: Optional[AppUserPreference]) -> Dict[str, Any]:
    return {
        "id": user.id,
        "email": user.email,
        "username": user.username,
        "created_at": user.created_at.isoformat() if user.created_at else None,
        "updated_at": user.updated_at.isoformat() if user.updated_at else None,
        "preferences": {
            "wants_newsletter": prefs.wants_newsletter if prefs else False,
            "feature_interests": prefs.feature_interests_json or [],
            "plan_type": prefs.plan_type if prefs else "free",
        },
    }



def _serialize_folder(folder: AppDashboardFolder, items: List[AppDashboardItem]) -> Dict[str, Any]:
    return {
        "id": folder.id,
        "folder_name": folder.folder_name,
        "folder_date": folder.folder_date.isoformat() if folder.folder_date else None,
        "is_default": folder.is_default,
        "created_at": folder.created_at.isoformat() if folder.created_at else None,
        "updated_at": folder.updated_at.isoformat() if folder.updated_at else None,
        "item_count": len(items),
        "items": [_serialize_item(item) for item in items],
    }



def _serialize_item(item: AppDashboardItem) -> Dict[str, Any]:
    return {
        "id": item.id,
        "user_id": item.user_id,
        "folder_id": item.folder_id,
        "source_tab": item.source_tab,
        "source_type": item.source_type,
        "title": item.title,
        "subtitle": item.subtitle,
        "payload_json": item.payload_json,
        "filter_json": item.filter_json,
        "sort_json": item.sort_json,
        "pin_order": item.pin_order,
        "notes": item.notes,
        "created_at": item.created_at.isoformat() if item.created_at else None,
        "updated_at": item.updated_at.isoformat() if item.updated_at else None,
    }



def _build_seed_payload(component: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "component_key": component["key"],
        "title": component["title"],
        "description": component["description"],
        "seeded_from": "current_my_dashboard_page",
        "save_ready": {
            "source_tabs_supported": [
                "Matchups",
                "Daily Odds",
                "Model Projections",
                "News",
                "My Dashboard",
                "Pitcher",
                "Batter",
                "Team",
                "Model Tracker",
            ],
            "available_fields": ["title", "subtitle", "metrics", "reasoning", "game_pk", "entity_id", "entity_type", "notes"],
            "conditions": ["equals", "contains", "min", "max", "in"],
            "max_filters": 10,
            "default_filter_fields": DEFAULT_FILTER_FIELDS,
            "logic_mode": "AND",
        },
    }



def _get_or_create_today_folder(session, user_id: int) -> AppDashboardFolder:
    today = dt.date.today()
    folder = (
        session.query(AppDashboardFolder)
        .filter(AppDashboardFolder.user_id == user_id, AppDashboardFolder.folder_date == today)
        .order_by(AppDashboardFolder.id.asc())
        .first()
    )
    if folder:
        return folder
    now = _utcnow()
    folder = AppDashboardFolder(
        user_id=user_id,
        folder_name=today.isoformat(),
        folder_date=today,
        is_default=False,
        created_at=now,
        updated_at=now,
    )
    session.add(folder)
    session.flush()
    return folder



def _get_or_create_default_folder(session, user_id: int) -> AppDashboardFolder:
    folder = (
        session.query(AppDashboardFolder)
        .filter(AppDashboardFolder.user_id == user_id, AppDashboardFolder.is_default.is_(True))
        .order_by(AppDashboardFolder.id.asc())
        .first()
    )
    if folder:
        return folder
    now = _utcnow()
    folder = AppDashboardFolder(
        user_id=user_id,
        folder_name="Default Dashboard",
        folder_date=None,
        is_default=True,
        created_at=now,
        updated_at=now,
    )
    session.add(folder)
    session.flush()
    return folder



def _seed_default_dashboard(session, user_id: int, folder_id: int) -> None:
    existing = (
        session.query(AppDashboardItem)
        .filter(AppDashboardItem.user_id == user_id, AppDashboardItem.folder_id == folder_id)
        .count()
    )
    if existing:
        return
    now = _utcnow()
    for index, component in enumerate(DEFAULT_COMPONENTS, start=1):
        session.add(
            AppDashboardItem(
                user_id=user_id,
                folder_id=folder_id,
                source_tab="my-dashboard",
                source_type=component["source_type"],
                title=component["title"],
                subtitle=component["description"],
                payload_json=_build_seed_payload(component),
                filter_json={
                    "default_filter_fields": DEFAULT_FILTER_FIELDS,
                    "max_filters": 10,
                },
                sort_json={"mode": "manual_seed_order", "position": index},
                pin_order=index,
                created_at=now,
                updated_at=now,
            )
        )



def _upsert_preferences(session, user_id: int, request: DashboardProfileRequest) -> AppUserPreference:
    prefs = session.query(AppUserPreference).filter(AppUserPreference.user_id == user_id).first()
    now = _utcnow()
    feature_interests = [choice for choice in request.feature_interests if choice in FEATURE_CHOICES]
    if prefs is None:
        prefs = AppUserPreference(
            user_id=user_id,
            wants_newsletter=request.wants_newsletter,
            feature_interests_json=feature_interests,
            plan_type=request.plan_type or "free",
            created_at=now,
            updated_at=now,
        )
        session.add(prefs)
    else:
        prefs.wants_newsletter = request.wants_newsletter
        prefs.feature_interests_json = feature_interests
        prefs.plan_type = request.plan_type or prefs.plan_type or "free"
        prefs.updated_at = now
    return prefs



def _create_session(session, user_id: int) -> AppSession:
    now = _utcnow()
    expires_at = now + dt.timedelta(hours=DASHBOARD_SESSION_HOURS)
    token = secrets.token_urlsafe(32)
    db_session = AppSession(
        user_id=user_id,
        session_token=token,
        expires_at=expires_at,
        created_at=now,
        last_seen_at=now,
    )
    session.add(db_session)
    session.flush()
    return db_session



def _get_active_user(session, token: Optional[str]) -> Optional[AppUser]:
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
    db_session.last_seen_at = now
    user = session.query(AppUser).filter(AppUser.id == db_session.user_id).first()
    return user



def _get_workspace_payload(session, user: AppUser) -> Dict[str, Any]:
    prefs = session.query(AppUserPreference).filter(AppUserPreference.user_id == user.id).first()
    default_folder = _get_or_create_default_folder(session, user.id)
    today_folder = _get_or_create_today_folder(session, user.id)
    _seed_default_dashboard(session, user.id, default_folder.id)
    session.flush()

    folders = (
        session.query(AppDashboardFolder)
        .filter(AppDashboardFolder.user_id == user.id)
        .order_by(AppDashboardFolder.is_default.desc(), AppDashboardFolder.folder_date.desc().nullslast(), AppDashboardFolder.id.desc())
        .all()
    )
    items = (
        session.query(AppDashboardItem)
        .filter(AppDashboardItem.user_id == user.id)
        .order_by(AppDashboardItem.folder_id.asc(), AppDashboardItem.pin_order.asc().nullslast(), AppDashboardItem.id.asc())
        .all()
    )
    items_by_folder: Dict[int, List[AppDashboardItem]] = {}
    for item in items:
        items_by_folder.setdefault(item.folder_id, []).append(item)

    return {
        "user": _serialize_user(user, prefs),
        "folders": [_serialize_folder(folder, items_by_folder.get(folder.id, [])) for folder in folders],
        "default_folder_id": default_folder.id,
        "today_folder_id": today_folder.id,
        "feature_choices": FEATURE_CHOICES,
        "seeded_components": [component["key"] for component in DEFAULT_COMPONENTS],
    }


@router.get("/model-tracker/health")
def model_tracker_health() -> Dict[str, Any]:
    return {
        "status": "ok",
        "component": "model_tracker",
        "persistence": "model_tracker_snapshots",
        "safe_mode": "additive_snapshot_layer",
    }


@router.post("/model-tracker/snapshot")
def model_tracker_snapshot(date: Optional[str] = Query(default=None)) -> Dict[str, Any]:
    target = _target_date(date)
    try:
        Session = _session_factory()
        with Session() as session:
            return build_tracker_snapshot_safe(session, target)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail={"message": "Model Tracker snapshot failed", "error": str(exc)}) from exc


@router.get("/model-tracker")
def model_tracker_list(date: Optional[str] = Query(default=None)) -> Dict[str, Any]:
    target = _target_date(date)
    try:
        Session = _session_factory()
        with Session() as session:
            return list_tracker_rows(session, target)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail={"message": "Model Tracker list failed", "error": str(exc)}) from exc


@router.get("/model-tracker/game/{game_pk}")
def model_tracker_game(game_pk: int, date: Optional[str] = Query(default=None)) -> Dict[str, Any]:
    target = _target_date(date)
    try:
        Session = _session_factory()
        with Session() as session:
            payload = list_tracker_rows(session, target, game_pk=game_pk)
            payload["game_pk"] = game_pk
            return payload
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail={"message": "Model Tracker game lookup failed", "error": str(exc)}) from exc


@router.post("/model-tracker/results/refresh")
def model_tracker_results_refresh(date: Optional[str] = Query(default=None)) -> Dict[str, Any]:
    target = _target_date(date)
    try:
        Session = _session_factory()
        with Session() as session:
            return refresh_tracker_results(session, target)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail={"message": "Model Tracker result refresh failed", "error": str(exc)}) from exc


@router.post("/my-dashboard/profile")
def my_dashboard_profile_create(request: DashboardProfileRequest, response: Response) -> Dict[str, Any]:
    email = _normalize_email(request.email)
    username = _normalize_username(request.username)
    if not email or "@" not in email:
        raise HTTPException(status_code=400, detail="A valid email is required")
    if not username:
        raise HTTPException(status_code=400, detail="Username is required")

    Session = _session_factory()
    with Session() as session:
        user = session.query(AppUser).filter(AppUser.email == email).first()
        now = _utcnow()
        if user is None:
            user = AppUser(
                email=email,
                username=username,
                password_hash=_hash_password(request.password) if request.password else None,
                created_at=now,
                updated_at=now,
            )
            session.add(user)
            session.flush()
        else:
            if user.password_hash and request.password and not _verify_password(request.password, user.password_hash):
                raise HTTPException(status_code=403, detail="Password does not match existing account")
            user.username = username or user.username
            if request.password and not user.password_hash:
                user.password_hash = _hash_password(request.password)
            user.updated_at = now

        prefs = _upsert_preferences(session, user.id, request)
        default_folder = _get_or_create_default_folder(session, user.id)
        _seed_default_dashboard(session, user.id, default_folder.id)
        _get_or_create_today_folder(session, user.id)
        db_session = _create_session(session, user.id)
        session.commit()

        response.set_cookie(
            key=DASHBOARD_SESSION_COOKIE,
            value=db_session.session_token,
            **_cookie_settings(),
        )
        return {
            "ok": True,
            "user": _serialize_user(user, prefs),
            "default_folder_id": default_folder.id,
            "session_expires_at": db_session.expires_at.isoformat(),
            "session_token": db_session.session_token,
            "cookie_settings": _cookie_settings(),
        }


@router.get("/my-dashboard/profile")
def my_dashboard_profile_get(
    mlb_dashboard_session: Optional[str] = Cookie(default=None),
    x_dashboard_session: Optional[str] = Header(default=None, alias="X-Dashboard-Session"),
) -> Dict[str, Any]:
    Session = _session_factory()
    with Session() as session:
        user = _get_active_user(session, _resolve_session_token(mlb_dashboard_session, x_dashboard_session))
        if not user:
            return {"authenticated": False}
        prefs = session.query(AppUserPreference).filter(AppUserPreference.user_id == user.id).first()
        session.commit()
        return {
            "authenticated": True,
            "user": _serialize_user(user, prefs),
            "feature_choices": FEATURE_CHOICES,
        }


@router.get("/my-dashboard/workspace")
def my_dashboard_workspace(
    mlb_dashboard_session: Optional[str] = Cookie(default=None),
    x_dashboard_session: Optional[str] = Header(default=None, alias="X-Dashboard-Session"),
) -> Dict[str, Any]:
    Session = _session_factory()
    with Session() as session:
        user = _get_active_user(session, _resolve_session_token(mlb_dashboard_session, x_dashboard_session))
        if not user:
            raise HTTPException(status_code=401, detail="Dashboard sign-in required")
        payload = _get_workspace_payload(session, user)
        session.commit()
        return payload


@router.post("/my-dashboard/folders/today/ensure")
def my_dashboard_ensure_today_folder(
    mlb_dashboard_session: Optional[str] = Cookie(default=None),
    x_dashboard_session: Optional[str] = Header(default=None, alias="X-Dashboard-Session"),
) -> Dict[str, Any]:
    Session = _session_factory()
    with Session() as session:
        user = _get_active_user(session, _resolve_session_token(mlb_dashboard_session, x_dashboard_session))
        if not user:
            raise HTTPException(status_code=401, detail="Dashboard sign-in required")
        folder = _get_or_create_today_folder(session, user.id)
        session.commit()
        return {
            "ok": True,
            "folder": _serialize_folder(folder, []),
        }


@router.post("/my-dashboard/folders")
def my_dashboard_create_folder(
    request: DashboardFolderCreateRequest,
    mlb_dashboard_session: Optional[str] = Cookie(default=None),
    x_dashboard_session: Optional[str] = Header(default=None, alias="X-Dashboard-Session"),
) -> Dict[str, Any]:
    Session = _session_factory()
    with Session() as session:
        user = _get_active_user(session, _resolve_session_token(mlb_dashboard_session, x_dashboard_session))
        if not user:
            raise HTTPException(status_code=401, detail="Dashboard sign-in required")
        now = _utcnow()
        folder_date = dt.date.fromisoformat(request.folder_date) if request.folder_date else None
        folder = AppDashboardFolder(
            user_id=user.id,
            folder_name=request.folder_name.strip(),
            folder_date=folder_date,
            is_default=bool(request.is_default),
            created_at=now,
            updated_at=now,
        )
        session.add(folder)
        session.commit()
        return {"ok": True, "folder": _serialize_folder(folder, [])}


@router.get("/my-dashboard/items")
def my_dashboard_items(
    folder_id: Optional[int] = Query(default=None),
    mlb_dashboard_session: Optional[str] = Cookie(default=None),
    x_dashboard_session: Optional[str] = Header(default=None, alias="X-Dashboard-Session"),
) -> Dict[str, Any]:
    Session = _session_factory()
    with Session() as session:
        user = _get_active_user(session, _resolve_session_token(mlb_dashboard_session, x_dashboard_session))
        if not user:
            raise HTTPException(status_code=401, detail="Dashboard sign-in required")
        query = session.query(AppDashboardItem).filter(AppDashboardItem.user_id == user.id)
        if folder_id is not None:
            query = query.filter(AppDashboardItem.folder_id == folder_id)
        items = query.order_by(AppDashboardItem.pin_order.asc().nullslast(), AppDashboardItem.id.asc()).all()
        session.commit()
        return {"items": [_serialize_item(item) for item in items]}


@router.post("/my-dashboard/items")
def my_dashboard_create_item(
    request: DashboardItemCreateRequest,
    mlb_dashboard_session: Optional[str] = Cookie(default=None),
    x_dashboard_session: Optional[str] = Header(default=None, alias="X-Dashboard-Session"),
) -> Dict[str, Any]:
    Session = _session_factory()
    with Session() as session:
        user = _get_active_user(session, _resolve_session_token(mlb_dashboard_session, x_dashboard_session))
        if not user:
            raise HTTPException(status_code=401, detail="Dashboard sign-in required")
        folder = session.query(AppDashboardFolder).filter(AppDashboardFolder.id == request.folder_id, AppDashboardFolder.user_id == user.id).first()
        if not folder:
            raise HTTPException(status_code=404, detail="Folder not found")
        now = _utcnow()
        item = AppDashboardItem(
            user_id=user.id,
            folder_id=folder.id,
            source_tab=request.source_tab,
            source_type=request.source_type,
            title=request.title,
            subtitle=request.subtitle,
            payload_json=request.payload_json,
            filter_json=request.filter_json,
            sort_json=request.sort_json,
            pin_order=request.pin_order,
            notes=request.notes,
            created_at=now,
            updated_at=now,
        )
        session.add(item)
        session.commit()
        return {"ok": True, "item": _serialize_item(item)}
