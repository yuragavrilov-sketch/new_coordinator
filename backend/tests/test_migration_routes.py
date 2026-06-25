from flask import Flask

from routes import migrations


def test_legacy_direct_cdc_creation_error_points_to_schema_screen():
    message = migrations._legacy_cdc_creation_error()

    assert "Legacy direct CDC migration creation is disabled" in message
    assert "schema migration screen" in message
    assert "queued and autostarted" in message


def test_create_migration_rejects_direct_cdc_before_db(monkeypatch):
    app = Flask(__name__)
    app.register_blueprint(migrations.bp)

    monkeypatch.setitem(migrations._state, "db_available", {"value": True})

    def fail_get_conn():
        raise AssertionError("CDC direct reject must not open state DB connection")

    monkeypatch.setitem(migrations._state, "get_conn", fail_get_conn)

    res = app.test_client().post("/api/migrations", json={
        "migration_name": "TCBPAY.ALLORDERS",
        "strategy": "CDC_DIRECT",
        "source_schema": "TCBPAY",
        "source_table": "ALLORDERS",
        "target_schema": "TCBPAY",
        "target_table": "ALLORDERS",
        "group_id": "gid-1",
    })

    assert res.status_code == 400
    body = res.get_json()
    assert "Legacy direct CDC migration creation is disabled" in body["error"]
    assert "schema migration screen" in body["error"]
