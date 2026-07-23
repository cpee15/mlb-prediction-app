import datetime as dt

import pytest
from fastapi import HTTPException
from pydantic import ValidationError
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from mlb_app import admin_access, my_dashboard_routes, workbench_query
from mlb_app.database import AppFeatureFlag, AppSession, AppUser, AppUserRole, Base


EXAMPLE = """SELECT full_name, team_name, model_score
FROM all_active_hitters
WHERE model_score >= 0.5 AND confidence = 'high'
ORDER BY model_score DESC
LIMIT 50"""


def _principal(*, role="admin", capabilities=None):
    now = admin_access._utcnow()
    return admin_access.DashboardPrincipal(
        user_id=1,
        email="owner@example.com",
        username="owner",
        role=role,
        capabilities=tuple(capabilities or admin_access.capabilities_for_role(role)),
        session_id=10,
        session_created_at=now,
        session_expires_at=now + dt.timedelta(hours=6),
    )


def test_example_compiles_to_a_structured_allowlisted_plan():
    plan = workbench_query.parse_workbench_statement(EXAMPLE)
    assert plan.logical_object == "all_active_hitters"
    assert plan.selected_fields == ["full_name", "team_name", "model_score"]
    assert plan.filters == [
        {"field": "model_score", "operator": "gte", "value": 0.5},
        {"field": "confidence", "operator": "eq", "value": "high"},
    ]
    assert plan.sort_by == "model_score"
    assert plan.sort_direction == "desc"
    assert plan.page_size == 50
    assert plan.as_dict()["authored_sql_executed"] is False


@pytest.mark.parametrize(
    "statement, message",
    [
        ("SELECT * FROM all_active_hitters LIMIT 10", "wildcards"),
        ("SELECT full_name FROM dashboard_player_current LIMIT 10", "logical object"),
        ("SELECT full_name FROM all_active_hitters", "Use SELECT fields"),
        ("SELECT full_name FROM all_active_hitters LIMIT 251", "LIMIT must be"),
        ("SELECT full_name FROM all_active_hitters; DROP TABLE users", "semicolons"),
        ("SELECT full_name FROM all_active_hitters -- comment LIMIT 10", "Comments"),
        ("DELETE FROM all_active_hitters LIMIT 10", "DELETE"),
        ("SELECT full_name FROM all_active_hitters JOIN secrets LIMIT 10", "JOIN"),
        ("SELECT full_name FROM all_active_hitters WHERE confidence = 'high' OR confidence = 'low' LIMIT 10", "OR"),
        ("SELECT full_name FROM all_active_hitters WHERE secret = 1 LIMIT 10", "filter field"),
        ("SELECT full_name FROM all_active_hitters WHERE model_score CONTAINS '5' LIMIT 10", "operator"),
    ],
)
def test_unsafe_or_unsupported_statements_are_rejected(statement, message):
    with pytest.raises(ValueError, match=message):
        workbench_query.parse_workbench_statement(statement)


def test_typed_literals_and_null_operators_use_field_contracts():
    plan = workbench_query.parse_workbench_statement(
        "SELECT batter_name, batter_team_id, xwoba FROM hitters_arsenal_splits "
        "WHERE batter_team_id = 112 AND xwoba IS NOT NULL ORDER BY xwoba ASC LIMIT 25"
    )
    assert plan.filters == [
        {"field": "batter_team_id", "operator": "eq", "value": 112},
        {"field": "xwoba", "operator": "is_not_null"},
    ]
    assert plan.sort_direction == "asc"


def test_execution_passes_only_the_normalized_contract_to_existing_service(monkeypatch):
    captured = {}

    def query(session, report_type, **options):
        captured.update({"session": session, "report_type": report_type, **options})
        return {"records": [], "items": [], "totalSize": 0}

    monkeypatch.setattr(workbench_query, "query_player_report", query)
    plan = workbench_query.parse_workbench_statement(EXAMPLE)
    result = workbench_query.execute_workbench_plan(object(), plan, page_number=2)
    assert captured["report_type"] == "all_active_hitters"
    assert captured["filters"] == plan.filters
    assert captured["page_number"] == 2
    assert captured["page_size"] == 50
    assert "statement" not in captured
    assert result["workbench_plan"]["execution_boundary"] == "structured_report_service"


def test_metadata_is_derived_and_excludes_physical_schema_details():
    objects = workbench_query.queryable_objects()
    assert {item["api_name"] for item in objects} == {
        "all_active_hitters",
        "all_active_pitchers",
        "hitters_arsenal_splits",
        "players_lineup_history",
    }
    assert all("base_object" not in item for item in objects)
    assert all("source_object" not in field for item in objects for field in item["fields"])


def test_request_contract_rejects_submitted_authorization_fields():
    with pytest.raises(ValidationError):
        my_dashboard_routes.QueryStudioRequest(
            statement=EXAMPLE,
            role="admin",
            capabilities=["workbench.execute"],
        )


def test_feature_flag_defaults_locked_and_honors_profile_target(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'workbench-flags.db'}")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    owner = _principal()
    with Session() as session:
        with pytest.raises(HTTPException) as locked:
            my_dashboard_routes._require_query_studio_enabled(session, owner)
        assert locked.value.status_code == 403
        session.add(AppFeatureFlag(
            flag_key="workbench_query_enabled",
            enabled=True,
            target_profiles_json=["standard_user"],
        ))
        session.commit()
        with pytest.raises(HTTPException):
            my_dashboard_routes._require_query_studio_enabled(session, owner)
        flag = session.query(AppFeatureFlag).one()
        flag.target_profiles_json = ["owner_administrator"]
        session.commit()
        my_dashboard_routes._require_query_studio_enabled(session, owner)


def test_direct_execute_tampering_still_requires_both_capabilities():
    principal = _principal(capabilities=("workbench.execute",))
    with pytest.raises(HTTPException) as denied:
        my_dashboard_routes.my_dashboard_query_studio_execute(
            my_dashboard_routes.QueryStudioRequest(statement=EXAMPLE),
            principal,
        )
    assert denied.value.status_code == 403


def test_query_studio_auth_boundary_returns_401_403_and_owner_metadata(tmp_path, monkeypatch):
    engine = create_engine(f"sqlite:///{tmp_path / 'workbench-http.db'}")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    now = admin_access._utcnow()
    with Session() as session:
        owner = AppUser(email="owner@example.com", username="owner", password_hash="hash", created_at=now, updated_at=now)
        user = AppUser(email="user@example.com", username="user", password_hash="hash", created_at=now, updated_at=now)
        session.add_all([owner, user])
        session.flush()
        session.add(AppUserRole(
            user_id=owner.id,
            role="admin",
            assignment_source="test",
            assigned_at=now - dt.timedelta(minutes=1),
            verified_at=now - dt.timedelta(minutes=1),
            updated_at=now,
        ))
        session.add_all([
            AppSession(user_id=owner.id, session_token="owner-token", created_at=now, last_seen_at=now, expires_at=now + dt.timedelta(hours=6)),
            AppSession(user_id=user.id, session_token="user-token", created_at=now, last_seen_at=now, expires_at=now + dt.timedelta(hours=6)),
            AppFeatureFlag(flag_key="workbench_query_enabled", enabled=True, target_profiles_json=["owner_administrator"]),
        ])
        session.commit()

    monkeypatch.setattr(admin_access, "dashboard_session_factory", lambda: Session)
    monkeypatch.setattr(my_dashboard_routes, "session_factory", lambda: Session)
    with pytest.raises(HTTPException) as anonymous:
        admin_access.current_dashboard_principal(None, None)
    assert anonymous.value.status_code == 401

    standard = admin_access.current_dashboard_principal(None, "user-token")
    with pytest.raises(HTTPException) as denied:
        admin_access.require_capability("workbench.advanced")(standard)
    assert denied.value.status_code == 403

    owner_principal = admin_access.current_dashboard_principal(None, "owner-token")
    metadata = my_dashboard_routes.my_dashboard_query_studio_metadata(owner_principal)
    assert metadata["authored_sql_executed"] is False
    assert metadata["totalSize"] == 4
