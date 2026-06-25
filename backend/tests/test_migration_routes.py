from routes import migrations


def test_legacy_direct_cdc_creation_error_points_to_schema_screen():
    message = migrations._legacy_cdc_creation_error()

    assert "Legacy direct CDC migration creation is disabled" in message
    assert "schema migration screen" in message
    assert "queued and autostarted" in message
