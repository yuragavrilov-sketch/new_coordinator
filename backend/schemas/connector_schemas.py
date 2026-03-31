from __future__ import annotations
from pydantic import BaseModel, Field, field_validator


class CreateGroupRequest(BaseModel):
    group_name: str = Field(min_length=1, max_length=255)
    source_connection_id: str = "oracle_source"
    connector_name: str = Field(min_length=1, max_length=255)
    topic_prefix: str = Field(min_length=1, max_length=255)
    consumer_group_prefix: str = ""

    @field_validator("group_name", mode="before")
    @classmethod
    def _strip_name(cls, v: str) -> str:
        return v.strip() if isinstance(v, str) else v


class AddGroupTableRequest(BaseModel):
    source_schema: str = Field(min_length=1)
    source_table: str = Field(min_length=1)
    target_schema: str = Field(min_length=1)
    target_table: str = Field(min_length=1)
    effective_key_type: str = "NONE"
    effective_key_columns_json: str = "[]"
    source_pk_exists: bool = False
    source_uk_exists: bool = False
