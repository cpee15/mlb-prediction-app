import copy
import datetime as dt

import pytest
from fastapi import HTTPException
from pydantic import ValidationError
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from mlb_app import model_tracker_routes as routes
from mlb_app.database import AppDashboardFolder, AppDashboardItem, AppUser, Base


@pytest.fixture()
def dashboard_store(tmp_path, monkeypatch):
    engine = create_engine(f"sqlite:///{tmp_path / 'dashboard-renames.db'}")
    Base.metadata.create_all(
        engine,
        tables=[AppUser.__table__, AppDashboardFolder.__table__, AppDashboardItem.__table__],
    )
    Session = sessionmaker(bind=engine)
    now = dt.datetime(2026, 7, 22, 12, 0, 0)
    with Session() as session:
        owner = AppUser(email="owner@example.com", username="owner", created_at=now, updated_at=now)
        other = AppUser(email="other@example.com", username="other", created_at=now, updated_at=now)
        session.add_all([owner, other])
        session.flush()
        folder = AppDashboardFolder(
            user_id=owner.id,
            folder_name="Original Folder",
            folder_date=dt.date(2026, 7, 22),
            is_default=False,
            created_at=now,
            updated_at=now,
        )
        session.add(folder)
        session.flush()
        item = AppDashboardItem(
            user_id=owner.id,
            folder_id=folder.id,
            source_tab="my-dashboard",
            source_type="report_view",
            title="Original Report",
            subtitle="Saved subtitle",
            payload_json={"snapshot": {"records": [{"entity_id": 1}]}},
            filter_json={"team": "CHC"},
            sort_json={"by": "score", "direction": "desc"},
            pin_order=4,
            notes="Saved notes",
            created_at=now,
            updated_at=now,
        )
        session.add(item)
        session.commit()
        ids = {"owner": owner.id, "other": other.id, "folder": folder.id, "item": item.id}

    monkeypatch.setattr(routes, "_session_factory", lambda: Session)

    def active_user(session, token):
        user_id = ids.get(token or "")
        return session.get(AppUser, user_id) if user_id else None

    monkeypatch.setattr(routes, "_get_active_user", active_user)
    return Session, ids


def test_owner_can_rename_folder_and_only_name_changes(dashboard_store):
    Session, ids = dashboard_store
    with Session() as session:
        before = session.get(AppDashboardFolder, ids["folder"])
        original = (before.folder_date, before.is_default, before.created_at)

    result = routes.my_dashboard_rename_folder(
        ids["folder"],
        routes.DashboardFolderRenameRequest(folder_name="  Scouting Notes  "),
        x_dashboard_session="owner",
        mlb_dashboard_session=None,
    )

    assert result["folder"]["folder_name"] == "Scouting Notes"
    with Session() as session:
        renamed = session.get(AppDashboardFolder, ids["folder"])
        assert (renamed.folder_date, renamed.is_default, renamed.created_at) == original
        assert renamed.updated_at > original[2]


def test_owner_can_rename_report_without_mutating_saved_snapshot(dashboard_store):
    Session, ids = dashboard_store
    with Session() as session:
        item = session.get(AppDashboardItem, ids["item"])
        preserved = copy.deepcopy({
            "folder_id": item.folder_id,
            "subtitle": item.subtitle,
            "payload_json": item.payload_json,
            "filter_json": item.filter_json,
            "sort_json": item.sort_json,
            "pin_order": item.pin_order,
            "notes": item.notes,
        })

    result = routes.my_dashboard_rename_item(
        ids["item"],
        routes.DashboardItemRenameRequest(title="  Confirmed Hitters  "),
        x_dashboard_session="owner",
        mlb_dashboard_session=None,
    )

    assert result["item"]["title"] == "Confirmed Hitters"
    with Session() as session:
        item = session.get(AppDashboardItem, ids["item"])
        assert {
            "folder_id": item.folder_id,
            "subtitle": item.subtitle,
            "payload_json": item.payload_json,
            "filter_json": item.filter_json,
            "sort_json": item.sort_json,
            "pin_order": item.pin_order,
            "notes": item.notes,
        } == preserved


@pytest.mark.parametrize(
    ("rename", "payload"),
    [
        (routes.my_dashboard_rename_folder, routes.DashboardFolderRenameRequest(folder_name="Other Folder")),
        (routes.my_dashboard_rename_item, routes.DashboardItemRenameRequest(title="Other Report")),
    ],
)
def test_user_cannot_rename_another_users_saved_records(dashboard_store, rename, payload):
    _, ids = dashboard_store
    record_id = ids["folder"] if rename is routes.my_dashboard_rename_folder else ids["item"]
    with pytest.raises(HTTPException) as exc:
        rename(record_id, payload, x_dashboard_session="other", mlb_dashboard_session=None)
    assert exc.value.status_code == 404


@pytest.mark.parametrize(
    ("rename", "record_key", "payload"),
    [
        (routes.my_dashboard_rename_folder, "folder", routes.DashboardFolderRenameRequest(folder_name="   ")),
        (routes.my_dashboard_rename_item, "item", routes.DashboardItemRenameRequest(title="   ")),
    ],
)
def test_blank_names_are_rejected(dashboard_store, rename, record_key, payload):
    _, ids = dashboard_store
    with pytest.raises(HTTPException) as exc:
        rename(ids[record_key], payload, x_dashboard_session="owner", mlb_dashboard_session=None)
    assert exc.value.status_code == 400


def test_rename_models_enforce_database_length_limit():
    with pytest.raises(ValidationError):
        routes.DashboardFolderRenameRequest(folder_name="x" * 256)
    with pytest.raises(ValidationError):
        routes.DashboardItemRenameRequest(title="x" * 256)
