from __future__ import annotations
from datetime import datetime
from typing import Any
from pydantic import BaseModel, ConfigDict
from models.enums import GroupStatus


class ConnectorGroup(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    group_id: str
    group_name: str
    source_connection_id: str = "oracle_source"
    connector_name: str
    topic_prefix: str
    consumer_group_prefix: str = ""
    status: GroupStatus = GroupStatus.PENDING
    error_text: str | None = None
    connector_config_json: dict[str, Any] | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    run_id: str = ""


class GroupTable(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: str
    group_id: str
    source_schema: str
    source_table: str
    target_schema: str
    target_table: str
    effective_key_type: str = "NONE"
    effective_key_columns_json: str = "[]"
    source_pk_exists: bool = False
    source_uk_exists: bool = False
    topic_name: str = ""
    created_at: datetime | None = None


class GroupStateHistory(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    group_id: str
    from_status: str | None = None
    to_status: str
    message: str | None = None
    created_at: datetime | None = None
