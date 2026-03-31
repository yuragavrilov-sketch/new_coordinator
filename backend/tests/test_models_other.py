from datetime import datetime, timezone
from uuid import uuid4

from models.connector_group import ConnectorGroup, GroupTable, GroupStateHistory
from models.catalog import DdlSnapshot, DdlObject, DdlCompareResult
from models.plan import MigrationPlan, MigrationPlanItem
from models.data_compare import DataCompareTask, DataCompareChunk
from models.checklist import ChecklistList, ChecklistItem
from models.enums import GroupStatus


def test_connector_group():
    g = ConnectorGroup.model_validate({
        "group_id": str(uuid4()),
        "group_name": "group-1",
        "source_connection_id": "oracle_source",
        "connector_name": "cdc-group-1",
        "topic_prefix": "cdc",
        "status": "RUNNING",
        "created_at": datetime.now(timezone.utc),
        "updated_at": datetime.now(timezone.utc),
    })
    assert g.status == GroupStatus.RUNNING


def test_group_table():
    t = GroupTable.model_validate({
        "id": str(uuid4()),
        "group_id": str(uuid4()),
        "source_schema": "HR",
        "source_table": "EMPLOYEES",
        "target_schema": "hr",
        "target_table": "employees",
        "created_at": datetime.now(timezone.utc),
    })
    assert t.source_schema == "HR"


def test_group_state_history():
    h = GroupStateHistory.model_validate({
        "id": 1,
        "group_id": str(uuid4()),
        "from_status": "PENDING",
        "to_status": "RUNNING",
        "created_at": datetime.now(timezone.utc),
    })
    assert h.to_status == "RUNNING"


def test_ddl_snapshot():
    s = DdlSnapshot.model_validate({
        "snapshot_id": 1,
        "src_schema": "HR",
        "tgt_schema": "hr",
        "loaded_at": datetime.now(timezone.utc),
    })
    assert s.snapshot_id == 1


def test_ddl_object():
    o = DdlObject.model_validate({
        "id": 1,
        "snapshot_id": 1,
        "db_side": "source",
        "object_type": "TABLE",
        "object_name": "EMPLOYEES",
    })
    assert o.object_type == "TABLE"


def test_ddl_compare_result():
    r = DdlCompareResult.model_validate({
        "id": 1,
        "snapshot_id": 1,
        "object_type": "TABLE",
        "object_name": "EMPLOYEES",
        "match_status": "MATCH",
    })
    assert r.match_status == "MATCH"


def test_migration_plan():
    p = MigrationPlan.model_validate({
        "plan_id": 1,
        "name": "plan-1",
        "src_schema": "HR",
        "tgt_schema": "hr",
        "status": "DRAFT",
    })
    assert p.status == "DRAFT"


def test_migration_plan_item():
    i = MigrationPlanItem.model_validate({
        "item_id": 1,
        "plan_id": 1,
        "table_name": "EMPLOYEES",
        "mode": "CDC",
        "batch_order": 1,
        "sort_order": 0,
        "status": "PENDING",
    })
    assert i.table_name == "EMPLOYEES"


def test_data_compare_task():
    t = DataCompareTask.model_validate({
        "task_id": str(uuid4()),
        "source_schema": "HR",
        "source_table": "EMPLOYEES",
        "target_schema": "hr",
        "target_table": "employees",
        "status": "PENDING",
        "created_at": datetime.now(timezone.utc),
    })
    assert t.status == "PENDING"


def test_data_compare_chunk():
    c = DataCompareChunk.model_validate({
        "chunk_id": str(uuid4()),
        "task_id": str(uuid4()),
        "side": "source",
        "chunk_seq": 1,
        "rowid_start": "AAABcAAEAAAAIjAAA",
        "rowid_end": "AAABcAAEAAAAIjAAZ",
        "status": "PENDING",
        "created_at": datetime.now(timezone.utc),
    })
    assert c.side == "source"


def test_checklist_list():
    cl = ChecklistList.model_validate({
        "list_id": 1,
        "name": "pre-migration",
        "created_at": datetime.now(timezone.utc),
    })
    assert cl.name == "pre-migration"


def test_checklist_item():
    ci = ChecklistItem.model_validate({
        "item_id": 1,
        "list_id": 1,
        "table_name": "EMPLOYEES",
        "decision": "migrate",
        "status": "pending",
        "created_at": datetime.now(timezone.utc),
    })
    assert ci.decision == "migrate"
