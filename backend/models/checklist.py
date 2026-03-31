from __future__ import annotations
from datetime import datetime
from pydantic import BaseModel, ConfigDict


class ChecklistList(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    list_id: int
    name: str
    created_at: datetime | None = None


class ChecklistItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    item_id: int
    list_id: int
    schema_name: str = ""
    table_name: str
    decision: str = "migrate"
    status: str = "pending"
    created_at: datetime | None = None
