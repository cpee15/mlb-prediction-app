import datetime as dt

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from mlb_app import admin_access, admin_routes
from mlb_app.database import AppSession, AppUser, AppUserRole, Base


def _principal(user_id: int):
    now = dt.datetime.utcnow()
    return admin_access.DashboardPrincipal(
        user_id=user_id,
        email="owner@example.com",
        username="owner",
        role=admin_access.ROLE_ADMIN,
        capabilities=admin_access.ADMIN_CAPABILITIES,
        session_id=1,
        session_created_at=now,
        session_expires_at=now + dt.timedelta(hours=6),
    )


def test_admin_user_checkbox_promotes_role_and_revokes_existing_sessions(tmp_path, monkeypatch):
    engine = create_engine(f"sqlite:///{tmp_path / 'admin-role.db'}")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    with Session() as session:
        owner = AppUser(email="owner@example.com", username="owner", password_hash="hash")
        user = AppUser(email="user@example.com", username="user", password_hash="hash")
        session.add_all([owner, user])
        session.flush()
        session.add(AppSession(
            user_id=user.id,
            session_token="old-user-session",
            expires_at=dt.datetime.utcnow() + dt.timedelta(hours=6),
        ))
        session.commit()
        owner_id, user_id = owner.id, user.id

    monkeypatch.setattr(admin_routes, "_session_factory", lambda: Session)
    response = admin_routes.admin_user_update(
        user_id,
        admin_routes.AdminUserUpdateRequest(is_admin=True),
        _principal(owner_id),
    )

    assert response["ok"] is True
    assert response["sessions_revoked"] is True
    assert response["user"]["role"] == "admin"
    with Session() as session:
        assignment = session.query(AppUserRole).filter_by(user_id=user_id).one()
        assert assignment.role == "admin"
        assert assignment.assignment_source == "admin_control_center"
        assert session.query(AppSession).filter_by(user_id=user_id).count() == 0
