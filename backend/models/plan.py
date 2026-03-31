from __future__ import annotations
from datetime import datetime
from typing import Any
from pydantic import BaseModel, ConfigDict


class MigrationPlan(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    plan_id: int
    name: str
    src_schema: str
    tgt_schema: str
    connector_group_id: str | None = None
    defaults_json: dict[str, Any] | None = None
    status: str = "DRAFT"
    created_at: datetime | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None


class MigrationPlanItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    item_id: int
    plan_id: int
    table_name: str
    mode: str = "CDC"
    batch_order: int = 1
    sort_order: int = 0
    overrides_json: dict[str, Any] | None = None
    migration_id: str | None = None
    status: str = "PENDING"
