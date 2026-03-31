"""Row serialization helpers for JSON-safe output."""

import decimal
from datetime import datetime


def _clean_value(v):
    if isinstance(v, datetime):
        if v.tzinfo is not None:
            from datetime import timezone
            v = v.astimezone(timezone.utc).replace(tzinfo=None)
        return v.isoformat() + "Z"
    if isinstance(v, decimal.Decimal):
        return str(v)
    return v


def clean_row(d: dict) -> dict:
    return {k: _clean_value(v) for k, v in d.items()}


def row_to_dict(cursor, row: tuple) -> dict:
    cols = [desc[0] for desc in cursor.description]
    return clean_row(dict(zip(cols, row)))
