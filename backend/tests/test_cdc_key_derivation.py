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


def test_schema_migration_cdc_autostart_recovers_missing_running_connector(monkeypatch):
    calls = []

    monkeypatch.setattr(
        schema_migrations,
        "_ensure_cdc_group_topics",
        lambda group_id: calls.append(("topics", group_id)) or [],
    )
    monkeypatch.setattr(
        connector_groups,
        "refresh_connector_tables",
        lambda group_id: calls.append(("refresh", group_id)) or (
            (_ for _ in ()).throw(
                ValueError("CDC connector cdc-main is marked RUNNING but is missing in Kafka Connect")
            )
        ),
    )
    monkeypatch.setattr(
        connector_groups,
        "request_start",
        lambda group_id: calls.append(("start", group_id)) or {
            "group_id": group_id,
            "status": "TOPICS_CREATING",
        },
    )

    result = schema_migrations._sync_and_request_cdc_connector_start("gid-1", "RUNNING")

    assert result == {"group_id": "gid-1", "status": "TOPICS_CREATING"}
    assert calls == [
        ("topics", "gid-1"),
        ("refresh", "gid-1"),
        ("start", "gid-1"),
    ]


def test_schema_migration_cdc_autostart_keeps_refresh_errors_blocking(monkeypatch):
    calls = []

    monkeypatch.setattr(
        connector_groups,
        "refresh_connector_tables",
        lambda group_id: calls.append(("refresh", group_id)) or (
            (_ for _ in ()).throw(ValueError("bad table.include.list"))
        ),
    )
    monkeypatch.setattr(
        connector_groups,
        "request_start",
        lambda group_id: calls.append(("start", group_id)),
    )

    try:
        schema_migrations._sync_and_request_cdc_connector_start("gid-1", "STOPPED")
    except ValueError as exc:
        assert "bad table.include.list" in str(exc)
    else:
        raise AssertionError("expected refresh error")

    assert calls == [("refresh", "gid-1")]


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


def test_schema_migration_starts_each_created_cdc_queue_position(monkeypatch):
    calls = []

    def fake_start(plan_id, **kwargs):
        calls.append((plan_id, kwargs))
        return {"batch": kwargs["batch_order"], "started": [f"mid-{kwargs['batch_order']}"]}

    import routes.planner as planner

    monkeypatch.setattr(planner, "_start_next_plan_batch", fake_start)

    result = schema_migrations._start_created_cdc_plan_batches(
        42,
        [
            {"batch_order": 3},
            {"batch_order": 1},
            {"batch_order": 3},
        ],
    )

    assert result == [
        {"batch": 1, "started": ["mid-1"]},
        {"batch": 3, "started": ["mid-3"]},
    ]
    assert calls == [
        (42, {
            "actor": "SYSTEM",
            "batch_order": 1,
            "allow_cdc_queue_when_blocked": True,
        }),
        (42, {
            "actor": "SYSTEM",
            "batch_order": 3,
            "allow_cdc_queue_when_blocked": True,
        }),
    ]


def test_schema_migration_kicks_cdc_group_after_queue_start(monkeypatch):
    calls = []

    monkeypatch.setattr(orchestrator, "_update_queue_positions", lambda: calls.append(("queue",)))
    monkeypatch.setattr(
        orchestrator,
        "_kick_new_migrations_for_group",
        lambda group_id: calls.append(("kick", group_id)),
    )

    schema_migrations._kick_cdc_group_best_effort("gid-1")

    assert calls == [
        ("queue",),
        ("kick", "gid-1"),
    ]


def test_schema_migration_ensures_cdc_group_topics(monkeypatch):
    from services import connector_groups

    monkeypatch.setattr(
        connector_groups,
        "create_group_topics",
        lambda group_id: [{"topic_name": "topic-1", "status": "ok"}],
    )

    assert schema_migrations._ensure_cdc_group_topics("gid-1") == [
        {"topic_name": "topic-1", "status": "ok"},
    ]


def test_schema_migration_ensure_cdc_group_topics_raises_on_error(monkeypatch):
    from services import connector_groups

    monkeypatch.setattr(
        connector_groups,
        "create_group_topics",
        lambda group_id: [{"topic_name": "topic-1", "status": "error", "error": "boom"}],
    )

    try:
        schema_migrations._ensure_cdc_group_topics("gid-1")
    except ValueError as exc:
        assert "CDC topic creation failed" in str(exc)
        assert "topic-1: boom" in str(exc)
    else:
        raise AssertionError("expected CDC topic creation failure")


def test_connector_group_topic_creation_uses_active_run_topic_names(monkeypatch):
    from services import kafka_topics

    created_topics = []

    class CursorStub:
        description = [
            ("id",),
            ("group_id",),
            ("source_schema",),
            ("source_table",),
            ("target_schema",),
            ("target_table",),
            ("effective_key_type",),
            ("effective_key_columns_json",),
            ("source_pk_exists",),
            ("source_uk_exists",),
            ("topic_name",),
            ("created_at",),
        ]

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, *_args):
            pass

        def fetchall(self):
            return [(
                "row-1",
                "gid-1",
                "TCBPAY",
                "ALLORDERS",
                "TCBPAY",
                "ALLORDERS",
                "PRIMARY_KEY",
                '["ID"]',
                True,
                False,
                "stale.topic.TCBPAY.ALLORDERS",
                None,
            )]

    class ConnStub:
        def cursor(self):
            return CursorStub()

        def close(self):
            pass

    monkeypatch.setattr(
        connector_groups,
        "get_group",
        lambda group_id: {
            "group_id": group_id,
            "topic_prefix": "base.topic",
            "run_id": "r123ab",
        },
    )
    monkeypatch.setitem(connector_groups._state, "get_conn", lambda: ConnStub())
    monkeypatch.setitem(connector_groups._state, "row_to_dict", lambda cur, row: {
        desc[0]: value for desc, value in zip(cur.description, row)
    })
    monkeypatch.setitem(connector_groups._state, "load_configs", lambda: {"kafka": {"bootstrap_servers": "k:9092"}})
    monkeypatch.setattr(
        kafka_topics,
        "create_topic",
        lambda bootstrap_servers, topic_name: created_topics.append((bootstrap_servers, topic_name)),
    )

    assert connector_groups.create_group_topics("gid-1") == [
        {"topic_name": "base.topic.r123ab.TCBPAY.ALLORDERS", "status": "ok"},
    ]
    assert created_topics == [
        (["k:9092"], "base.topic.r123ab.TCBPAY.ALLORDERS"),
    ]


class CursorStub:
    def __init__(self, row):
        self.row = row
        self.executed = []

    def execute(self, sql, params):
        self.executed.append((sql, params))

    def fetchone(self):
        return self.row


def test_schema_migration_active_cdc_table_lookup_finds_non_terminal():
    cur = CursorStub(("mid-1", "NEW"))

    assert schema_migrations._active_cdc_migration_for_group_table(
        cur, "gid-1", "tcbpay", "allorders",
    ) == ("mid-1", "NEW")
    sql, params = cur.executed[0]
    assert params == ("gid-1", "tcbpay", "allorders")
    assert "LEFT(COALESCE(strategy, ''), 4) = 'CDC_'" in sql
    assert "COALESCE(phase, '') NOT IN" in sql


def test_schema_migration_active_cdc_table_lookup_allows_absent():
    cur = CursorStub(None)

    assert schema_migrations._active_cdc_migration_for_group_table(
        cur, "gid-1", "TCBPAY", "ALLORDERS",
    ) is None


def test_schema_migration_reuses_plan_cdc_group_when_schema_group_is_empty():
    assert schema_migrations._resolve_cdc_connector_group_id(
        sm_group_id=None,
        plan_group_id="plan-gid",
        payload_group_id=None,
    ) == "plan-gid"


def test_schema_migration_rejects_conflicting_cdc_groups():
    try:
        schema_migrations._resolve_cdc_connector_group_id(
            sm_group_id="schema-gid",
            plan_group_id="plan-gid",
            payload_group_id=None,
        )
    except ValueError as exc:
        assert "only one CDC connector pack" in str(exc)
    else:
        raise AssertionError("expected CDC connector group conflict")


def test_schema_migration_accepts_existing_manual_cdc_key_columns():
    assert schema_migrations._validate_manual_cdc_key_columns(
        {"columns": [{"name": "ID"}, {"name": "DATE_ID"}]},
        ["ID", "DATE_ID"],
    ) == []


def test_schema_migration_rejects_missing_manual_cdc_key_columns():
    assert schema_migrations._validate_manual_cdc_key_columns(
        {"columns": [{"name": "ID"}]},
        ["ID", "MISSING_COL"],
    ) == ["MISSING_COL"]


def test_schema_migration_manual_cdc_key_requires_missing_pk_uk():
    try:
        schema_migrations._effective_cdc_key_info(
            {
                "columns": [{"name": "ID"}, {"name": "ALT_ID"}],
                "pk_columns": ["ID"],
                "uk_constraints": [],
            },
            ["ALT_ID"],
            "TCBPAY",
            "ALLORDERS",
        )
    except ValueError as exc:
        assert "allowed only when PK/UK is missing" in str(exc)
    else:
        raise AssertionError("expected manual CDC key rejection for PK table")


def test_schema_migration_manual_cdc_key_for_no_key_table_is_user_defined():
    assert schema_migrations._effective_cdc_key_info(
        {
            "columns": [{"name": "ID"}, {"name": "MERCHANT_ID"}],
            "pk_columns": [],
            "uk_constraints": [],
        },
        ["ID", "MERCHANT_ID"],
        "TCBPAY",
        "ALLORDERS",
    ) == ("USER_DEFINED", "USER", ["ID", "MERCHANT_ID"], False, False)


def test_schema_migration_normalizes_manual_cdc_key_columns_from_csv():
    assert schema_migrations._normalize_manual_cdc_key_columns(
        " id, ID, merchant_id, "
    ) == ["ID", "MERCHANT_ID"]


def test_schema_migration_normalizes_manual_cdc_key_columns_from_list():
    assert schema_migrations._normalize_manual_cdc_key_columns(
        ["id", " ", "ID", "date_id"]
    ) == ["ID", "DATE_ID"]


def test_orchestrator_treats_steady_state_as_plan_done():
    assert orchestrator._plan_item_status_for_phase("STEADY_STATE") == "DONE"


def test_orchestrator_index_enable_phase_holds_load_slot():
    assert "INDEXES_ENABLING" in orchestrator._HEAVY_PHASES


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


def test_orchestrator_refreshes_queue_when_group_becomes_running(monkeypatch):
    calls = []

    class ImmediateThread:
        def __init__(self, target, **_kwargs):
            self.target = target

        def start(self):
            self.target()

    monkeypatch.setattr(orchestrator.threading, "Thread", ImmediateThread)
    monkeypatch.setattr(
        orchestrator.connector_groups_svc,
        "do_create_topics",
        lambda group_id: calls.append(("topics", group_id)) or [{"topic_name": "t1", "status": "ok"}],
    )
    monkeypatch.setattr(
        orchestrator.connector_groups_svc,
        "do_start_connector",
        lambda group_id: calls.append(("start", group_id)) or {"name": "cdc-1"},
    )
    monkeypatch.setattr(
        orchestrator.connector_groups_svc,
        "refresh_connector_tables",
        lambda group_id: calls.append(("refresh", group_id)),
    )
    monkeypatch.setattr(
        orchestrator.connector_groups_svc,
        "transition_group",
        lambda group_id, status, message=None: calls.append(("transition", group_id, status)),
    )
    monkeypatch.setattr(
        orchestrator,
        "_update_queue_positions",
        lambda: calls.append(("queue",)),
    )
    monkeypatch.setattr(
        orchestrator,
        "_kick_new_migrations_for_group",
        lambda group_id: calls.append(("kick", group_id)),
    )
    monkeypatch.setitem(orchestrator._state, "broadcast", lambda event: calls.append(("broadcast", event["status"])))
    orchestrator._group_in_progress.clear()

    orchestrator._handle_group_connector_starting("gid-1")

    assert calls == [
        ("topics", "gid-1"),
        ("start", "gid-1"),
        ("refresh", "gid-1"),
        ("transition", "gid-1", "RUNNING"),
        ("queue",),
        ("kick", "gid-1"),
        ("broadcast", "RUNNING"),
    ]


def test_orchestrator_does_not_start_connector_when_topic_refresh_fails(monkeypatch):
    calls = []

    class ImmediateThread:
        def __init__(self, target, **_kwargs):
            self.target = target

        def start(self):
            self.target()

    monkeypatch.setattr(orchestrator.threading, "Thread", ImmediateThread)
    monkeypatch.setattr(
        orchestrator.connector_groups_svc,
        "do_create_topics",
        lambda group_id: calls.append(("topics", group_id)) or [
            {"topic_name": "t1", "status": "error", "error": "boom"}
        ],
    )
    monkeypatch.setattr(
        orchestrator.connector_groups_svc,
        "do_start_connector",
        lambda group_id: calls.append(("start", group_id)) or {"name": "cdc-1"},
    )
    monkeypatch.setattr(
        orchestrator.connector_groups_svc,
        "transition_group",
        lambda group_id, status, message=None: calls.append(("transition", group_id, status)),
    )
    monkeypatch.setitem(orchestrator._state, "broadcast", lambda event: calls.append(("broadcast", event["status"])))
    orchestrator._group_in_progress.clear()

    orchestrator._handle_group_connector_starting("gid-1")

    assert calls == [
        ("topics", "gid-1"),
        ("transition", "gid-1", "FAILED"),
        ("broadcast", "FAILED"),
    ]


def test_orchestrator_kicks_first_new_cdc_migration_for_group(monkeypatch):
    executed = []
    handled = []
    row = {
        "migration_id": "mid-1",
        "group_id": "gid-1",
        "phase": "NEW",
        "strategy": "CDC_DIRECT",
        "state_changed_at": "2026-06-25T00:00:00Z",
    }

    class CursorStub:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, query, params=None):
            executed.append((query, params))

        def fetchall(self):
            return [row]

    class ConnStub:
        def cursor(self):
            return CursorStub()

        def close(self):
            pass

    monkeypatch.setitem(orchestrator._state, "get_conn", lambda: ConnStub())
    monkeypatch.setattr(orchestrator, "row_to_dict", lambda _cur, value: dict(value))
    monkeypatch.setattr(orchestrator, "_handle_new", lambda mid, migration: handled.append((mid, migration)))

    orchestrator._kick_new_migrations_for_group("gid-1")

    assert handled == [("mid-1", row)]
    query, params = executed[0]
    assert params == ("gid-1",)
    assert "m.phase = 'NEW'" in query
    assert "LEFT(COALESCE(m.strategy, ''), 4) = 'CDC_'" in query
    assert "LIMIT  1" in query
