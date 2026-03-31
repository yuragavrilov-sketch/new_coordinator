from __future__ import annotations
from pydantic import BaseModel, Field


class SnapshotRequest(BaseModel):
    src_schema: str = Field(min_length=1)
    tgt_schema: str = Field(min_length=1)
    source_connection_id: str = "oracle_source"
    target_connection_id: str = "oracle_target"
