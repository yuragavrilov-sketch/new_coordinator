from __future__ import annotations
from pydantic import BaseModel, field_validator


class PaginationParams(BaseModel):
    page: int = 1
    page_size: int = 100

    @field_validator("page", mode="after")
    @classmethod
    def _clamp_page(cls, v: int) -> int:
        return max(1, v)

    @field_validator("page_size", mode="after")
    @classmethod
    def _clamp_page_size(cls, v: int) -> int:
        return max(1, min(500, v))


class ErrorResponse(BaseModel):
    error: str


def validate_request_data(schema_cls: type[BaseModel], data: dict | None) -> BaseModel:
    if not isinstance(data, dict):
        raise ValueError("Request body must be a JSON object")
    return schema_cls.model_validate(data)
