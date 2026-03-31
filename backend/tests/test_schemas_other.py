import pytest
from pydantic import ValidationError
from schemas.connector_schemas import CreateGroupRequest, AddGroupTableRequest
from schemas.catalog_schemas import SnapshotRequest


def test_create_group_minimal():
    req = CreateGroupRequest(
        group_name="group-1",
        connector_name="cdc-group-1",
        topic_prefix="cdc",
    )
    assert req.group_name == "group-1"
    assert req.source_connection_id == "oracle_source"


def test_create_group_name_required():
    with pytest.raises(ValidationError):
        CreateGroupRequest(
            group_name="",
            connector_name="cdc-group-1",
            topic_prefix="cdc",
        )


def test_add_group_table():
    req = AddGroupTableRequest(
        source_schema="HR",
        source_table="EMPLOYEES",
        target_schema="hr",
        target_table="employees",
    )
    assert req.source_schema == "HR"


def test_snapshot_request():
    req = SnapshotRequest(
        src_schema="HR",
        tgt_schema="hr",
    )
    assert req.src_schema == "HR"


def test_snapshot_request_required():
    with pytest.raises(ValidationError):
        SnapshotRequest(src_schema="HR")
