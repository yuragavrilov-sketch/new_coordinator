from __future__ import annotations

import json

from routes import schema_migrations
from routes import migrations
from services import orchestrator
from services import connector_groups


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


def test_cdc_group_table_keys_normalizes_names(monkeypatch):
    monkeypatch.setattr(
        connector_groups,
        "get_group_tables",
        lambda group_id: [
            {"source_schema": "tcbpay", "source_table": "allorders"},
            {"source_schema": "TCBPAY", "source_table": "Merchants#Orders"},
        ],
    )

    assert migrations._cdc_group_table_keys("gid") == {
        ("TCBPAY", "ALLORDERS"),
        ("TCBPAY", "MERCHANTS#ORDERS"),
    }


def test_orchestrator_treats_steady_state_as_plan_done():
    assert orchestrator._plan_item_status_for_phase("STEADY_STATE") == "DONE"


def test_orchestrator_keeps_plan_running_when_no_pending_but_active_items():
    assert orchestrator._plan_status_without_pending(
        active_count=1,
        failed_count=0,
        cancelled_count=0,
    ) == "RUNNING"


def test_orchestrator_closes_plan_when_no_pending_or_active_items():
    assert orchestrator._plan_status_without_pending(
        active_count=0,
        failed_count=0,
        cancelled_count=0,
    ) == "DONE"


def test_orchestrator_syncs_cdc_runtime_context_from_group(monkeypatch):
    updates = {}
    monkeypatch.setattr(
        orchestrator,
        "_update",
        lambda migration_id, fields: updates.update({"migration_id": migration_id, **fields}),
    )

    result = orchestrator._sync_cdc_runtime_context(
        "mid-1",
        {
            "source_schema": "TCBPAY",
            "source_table": "ALLORDERS",
            "connector_name": "",
            "topic_prefix": "",
            "consumer_group": "",
        },
        {
            "connector_name": "sm_tcbpay_pay_connector",
            "topic_prefix": "sm.tcbpay.pay",
            "consumer_group_prefix": "sm.tcbpay.pay",
            "run_id": "r123ab",
        },
    )

    assert updates == {
        "migration_id": "mid-1",
        "connector_name": "sm_tcbpay_pay_connector_r123ab",
        "topic_prefix": "sm.tcbpay.pay.r123ab",
        "consumer_group": "sm.tcbpay.pay_TCBPAY_ALLORDERS",
    }
    assert result["topic_prefix"] == "sm.tcbpay.pay.r123ab"
