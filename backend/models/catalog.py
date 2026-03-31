from __future__ import annotations
from datetime import datetime
from typing import Any
from pydantic import BaseModel, ConfigDict


class DdlSnapshot(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    snapshot_id: int
    src_schema: str
    tgt_schema: str
    loaded_at: datetime | None = None


class DdlObject(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    snapshot_id: int
    db_side: str
    object_type: str
    object_name: str
    oracle_status: str | None = None
    last_ddl_time: datetime | None = None
    metadata: dict[str, Any] | None = None


class DdlCompareResult(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    snapshot_id: int
    object_type: str
    object_name: str
    match_status: str = "UNKNOWN"
    diff: dict[str, Any] | None = None
