from flask import Flask

from routes import connector_groups
from routes import planner
from services import orchestrator
from services import connector_groups as connector_groups_svc


def test_legacy_connector_group_migration_error_points_to_schema_screen():
    payload = connector_groups._legacy_cdc_migration_error()

    assert "Legacy connector-group migration flow is disabled" in payload["error"]
    assert "schema migration screen" in payload["error"]
    assert "queued and autostarted" in payload["error"]


def test_legacy_connector_group_membership_error_points_to_schema_screen():
    payload = connector_groups._legacy_cdc_membership_error()

    assert "Direct connector-group table edits are disabled" in payload["error"]
    assert "schema migration screen" in payload["error"]
    assert "enters the queue" in payload["error"]
    assert "autostarted" in payload["error"]


def test_create_connector_group_rejects_direct_group_creation(monkeypatch):
    calls = []

    monkeypatch.setattr(
        connector_groups_svc,
        "create_group",
        lambda **kwargs: calls.append(("create", kwargs)),
    )

    app = Flask(__name__)
    app.register_blueprint(connector_groups.bp)

    res = app.test_client().post(
        "/api/connector-groups",
        json={
            "group_name": "CDC",
            "connector_name": "cdc_connector",
            "topic_prefix": "cdc",
        },
    )

    assert res.status_code == 400
    assert "Direct connector-group creation is disabled" in res.get_json()["error"]
    assert "schema migration screen" in res.get_json()["error"]
    assert calls == []


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


def test_add_group_tables_rejects_direct_membership_edits(monkeypatch):
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

    assert res.status_code == 400
    assert "Direct connector-group table edits are disabled" in res.get_json()["error"]
    assert calls == []


def test_connector_group_wizard_rejects_direct_membership_edits():
    app = Flask(__name__)
    app.register_blueprint(connector_groups.bp)

    res = app.test_client().post(
        "/api/connector-groups/wizard",
        json={
            "group_name": "CDC",
            "connector_name": "cdc_connector",
            "topic_prefix": "cdc",
            "tables": [{"source_schema": "TCBPAY", "source_table": "ALLORDERS"}],
        },
    )

    assert res.status_code == 400
    assert "Direct connector-group table edits are disabled" in res.get_json()["error"]


def test_refresh_tables_starts_pending_cdc_batches_after_sync(monkeypatch):
    calls = []

    monkeypatch.setattr(
        connector_groups_svc,
        "refresh_connector_tables",
        lambda group_id: calls.append(("refresh", group_id)),
    )
    monkeypatch.setattr(
        connector_groups,
        "_start_pending_cdc_plan_batches_for_group",
        lambda group_id: calls.append(("start-pending", group_id)) or [
            {"batch": 4, "started": ["mid-1"]},
        ],
    )

    app = Flask(__name__)
    app.register_blueprint(connector_groups.bp)

    res = app.test_client().post("/api/connector-groups/gid-1/refresh-tables")

    assert res.status_code == 200
    assert res.get_json() == {
        "ok": True,
        "plan_starts": [{"batch": 4, "started": ["mid-1"]}],
        "plan_start_error": None,
    }
    assert calls == [
        ("refresh", "gid-1"),
        ("start-pending", "gid-1"),
    ]


def test_start_group_starts_pending_cdc_batches_after_request_start(monkeypatch):
    calls = []

    monkeypatch.setattr(
        connector_groups_svc,
        "request_start",
        lambda group_id: calls.append(("request-start", group_id)) or {
            "group_id": group_id,
            "status": "RUNNING",
        },
    )
    monkeypatch.setattr(
        connector_groups,
        "_start_pending_cdc_plan_batches_for_group",
        lambda group_id: calls.append(("start-pending", group_id)) or [
            {"batch": 2, "started": ["mid-1"]},
        ],
    )
    monkeypatch.setitem(
        connector_groups._state,
        "broadcast",
        lambda event: calls.append(("broadcast", event["group_id"], event["status"])),
    )

    app = Flask(__name__)
    app.register_blueprint(connector_groups.bp)

    res = app.test_client().post("/api/connector-groups/gid-1/start")

    assert res.status_code == 200
    assert res.get_json() == {
        "group_id": "gid-1",
        "status": "RUNNING",
        "plan_starts": [{"batch": 2, "started": ["mid-1"]}],
        "plan_start_error": None,
    }
    assert calls == [
        ("request-start", "gid-1"),
        ("broadcast", "gid-1", "RUNNING"),
        ("start-pending", "gid-1"),
    ]


def test_start_pending_cdc_plan_batches_kicks_group(monkeypatch):
    calls = []

    monkeypatch.setattr(
        connector_groups,
        "_pending_cdc_plan_batches_for_group",
        lambda group_id: calls.append(("load", group_id)) or [(42, 2), (42, 3)],
    )
    monkeypatch.setattr(
        planner,
        "_start_next_plan_batch",
        lambda plan_id, **kwargs: calls.append(("start", plan_id, kwargs)) or {
            "batch": kwargs["batch_order"],
            "started": [f"mid-{kwargs['batch_order']}"],
        },
    )
    monkeypatch.setattr(orchestrator, "_update_queue_positions", lambda: calls.append(("queue",)))
    monkeypatch.setattr(
        orchestrator,
        "_kick_new_migrations_for_group",
        lambda group_id: calls.append(("kick", group_id)),
    )

    assert connector_groups._start_pending_cdc_plan_batches_for_group("gid-1") == [
        {"batch": 2, "started": ["mid-2"]},
        {"batch": 3, "started": ["mid-3"]},
    ]
    assert calls == [
        ("load", "gid-1"),
        ("start", 42, {
            "actor": "SYSTEM",
            "batch_order": 2,
            "allow_cdc_queue_when_blocked": True,
        }),
        ("start", 42, {
            "actor": "SYSTEM",
            "batch_order": 3,
            "allow_cdc_queue_when_blocked": True,
        }),
        ("queue",),
        ("kick", "gid-1"),
    ]
