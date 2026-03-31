from __future__ import annotations
from datetime import datetime
from decimal import Decimal
from pydantic import BaseModel, ConfigDict


class DataCompareTask(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    task_id: str
    source_schema: str
    source_table: str
    target_schema: str
    target_table: str
    compare_mode: str = "full"
    last_n: int | None = None
    order_column: str | None = None
    chunk_size: int = 100000
    status: str = "PENDING"
    source_count: int | None = None
    target_count: int | None = None
    source_hash: Decimal | None = None
    target_hash: Decimal | None = None
    counts_match: bool | None = None
    hash_match: bool | None = None
    chunks_total: int = 0
    chunks_done: int = 0
    error_text: str | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    created_at: datetime | None = None


class DataCompareChunk(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    chunk_id: str
    task_id: str
    side: str
    chunk_seq: int
    rowid_start: str
    rowid_end: str
    status: str = "PENDING"
    row_count: int | None = None
    hash_sum: Decimal | None = None
    worker_id: str | None = None
    claimed_at: datetime | None = None
    completed_at: datetime | None = None
    error_text: str | None = None
    retry_count: int = 0
    created_at: datetime | None = None
