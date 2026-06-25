from flask import Flask

from routes import connector_groups
from services import connector_groups as connector_groups_svc


def test_legacy_connector_group_migration_error_points_to_schema_screen():
    payload = connector_groups._legacy_cdc_migration_error()

    assert "Legacy connector-group migration flow is disabled" in payload["error"]
    assert "schema migration screen" in payload["error"]
    assert "queued and autostarted" in payload["error"]


def test_remove_group_table_returns_warning_when_sync_fails_after_delete(monkeypatch):
    calls = []

    monkeypatch.setattr(
        connector_groups_svc,
        "remove_table",
        lambda group_id, schema, table: calls.append(("remove", group_id, schema, table)),
    )
    monkeypatch.setattr(
        connector_groups_svc,
        "refresh_connector_tables",
        lambda group_id: calls.append(("refresh", group_id)) or (
            (_ for _ in ()).throw(RuntimeError("connect unavailable"))
        ),
    )

    app = Flask(__name__)
    app.register_blueprint(connector_groups.bp)

    res = app.test_client().delete("/api/connector-groups/gid-1/tables/TCBPAY/ALLORDERS")

    assert res.status_code == 200
    assert res.get_json() == {
        "removed": True,
        "sync_error": "CDC connector config sync failed: connect unavailable",
    }
    assert calls == [
        ("remove", "gid-1", "TCBPAY", "ALLORDERS"),
        ("refresh", "gid-1"),
    ]


def test_add_group_tables_returns_warning_when_sync_fails_after_insert(monkeypatch):
    calls = []

    monkeypatch.setattr(
        connector_groups_svc,
        "add_tables",
        lambda group_id, tables: calls.append(("add", group_id, tables)) or [
            {"source_schema": "TCBPAY", "source_table": "ALLORDERS"},
        ],
    )
    monkeypatch.setattr(
        connector_groups_svc,
        "refresh_connector_tables",
        lambda group_id: calls.append(("refresh", group_id)) or (
            (_ for _ in ()).throw(RuntimeError("connect unavailable"))
        ),
    )

    app = Flask(__name__)
    app.register_blueprint(connector_groups.bp)

    res = app.test_client().post(
        "/api/connector-groups/gid-1/tables",
        json={"tables": [{"source_schema": "TCBPAY", "source_table": "ALLORDERS"}]},
    )

    assert res.status_code == 201
    assert res.get_json() == {
        "tables": [{"source_schema": "TCBPAY", "source_table": "ALLORDERS"}],
        "migrations": [],
        "migrations_error": None,
        "sync_error": "CDC connector config sync failed: connect unavailable",
        "requested_count": 1,
        "added_count": 1,
        "already_present_count": 0,
    }
    assert calls == [
        ("add", "gid-1", [{"source_schema": "TCBPAY", "source_table": "ALLORDERS"}]),
        ("refresh", "gid-1"),
    ]


def test_add_group_tables_reports_existing_without_sync(monkeypatch):
    calls = []

    monkeypatch.setattr(
        connector_groups_svc,
        "add_tables",
        lambda group_id, tables: calls.append(("add", group_id, tables)) or [],
    )
    monkeypatch.setattr(
        connector_groups_svc,
        "refresh_connector_tables",
        lambda group_id: calls.append(("refresh", group_id)),
    )

    app = Flask(__name__)
    app.register_blueprint(connector_groups.bp)

    res = app.test_client().post(
        "/api/connector-groups/gid-1/tables",
        json={"tables": [{"source_schema": "TCBPAY", "source_table": "ALLORDERS"}]},
    )

    assert res.status_code == 200
    assert res.get_json() == {
        "tables": [],
        "migrations": [],
        "migrations_error": None,
        "sync_error": None,
        "requested_count": 1,
        "added_count": 0,
        "already_present_count": 1,
        "message": "Tables are already in CDC connector group",
    }
    assert calls == [
        ("add", "gid-1", [{"source_schema": "TCBPAY", "source_table": "ALLORDERS"}]),
    ]
