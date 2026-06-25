from __future__ import annotations

import json

from routes import schema_migrations
from services import orchestrator


def test_schema_migration_cdc_key_derivation_prefers_pk():
    assert schema_migrations._derive_cdc_key_info({
        "pk_columns": ["ID"],
        "uk_constraints": [{"name": "UK_T", "columns": ["CODE"]}],
    }) == ("PRIMARY_KEY", "PK", ["ID"], True, True)


def test_schema_migration_cdc_key_derivation_uses_unique_key():
    assert schema_migrations._derive_cdc_key_info({
        "pk_columns": [],
        "uk_constraints": [{"name": "UK_T", "columns": ["CODE", "DATE_ID"]}],
    }) == ("UNIQUE_KEY", "UK", ["CODE", "DATE_ID"], False, True)


def test_orchestrator_source_key_derivation_returns_none_without_key():
    assert orchestrator._derive_source_key_from_info({
        "pk_columns": [],
        "uk_constraints": [],
    }) is None


def test_orchestrator_source_key_derivation_serializes_pk():
    result = orchestrator._derive_source_key_from_info({
        "pk_columns": ["ID"],
        "uk_constraints": [],
    })

    assert result is not None
    assert result["source_pk_exists"] is True
    assert result["effective_key_type"] == "PRIMARY_KEY"
    assert json.loads(result["effective_key_columns_json"]) == ["ID"]
