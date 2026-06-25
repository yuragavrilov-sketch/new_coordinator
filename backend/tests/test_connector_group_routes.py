from routes import connector_groups


def test_legacy_connector_group_migration_error_points_to_schema_screen():
    payload = connector_groups._legacy_cdc_migration_error()

    assert "Legacy connector-group migration flow is disabled" in payload["error"]
    assert "schema migration screen" in payload["error"]
    assert "queued and autostarted" in payload["error"]
