"""
Compare DDL objects between source and target snapshots.
"""
import json


def _normalize_sql(text: str | None) -> str:
    """Normalize SQL for comparison: collapse whitespace, lowercase."""
    if not text:
        return ""
    return " ".join(text.lower().split())


def _normalize_code(text: str | None) -> str:
    """Normalize PL/SQL code for comparison.

    - Strip trailing whitespace per line
    - Strip leading/trailing blank lines
    - Collapse runs of blank lines into one
    - Lowercase (keywords and identifiers are case-insensitive in Oracle)
    """
    if not text:
        return ""
    lines = [line.rstrip() for line in text.splitlines()]
    # Strip leading/trailing blank lines
    while lines and not lines[0]:
        lines.pop(0)
    while lines and not lines[-1]:
        lines.pop()
    # Collapse multiple blank lines into one
    result: list[str] = []
    prev_blank = False
    for line in lines:
        if not line:
            if not prev_blank:
                result.append("")
            prev_blank = True
        else:
            result.append(line.lower())
            prev_blank = False
    return "\n".join(result)


def _diff_table(src_meta: dict, tgt_meta: dict) -> dict:
    """Compare table DDL. Uses same logic as planner._diff_summary."""
    src_cols = {c["name"]: c for c in src_meta.get("columns", [])}
    tgt_cols = {c["name"]: c for c in tgt_meta.get("columns", [])}

    cols_missing = [n for n in src_cols if n not in tgt_cols]
    cols_extra = [n for n in tgt_cols if n not in src_cols]
    cols_type = [
        n for n in src_cols
        if n in tgt_cols and src_cols[n].get("data_type") != tgt_cols[n].get("data_type")
    ]

    src_idx = {i["name"]: i for i in src_meta.get("indexes", [])}
    tgt_idx = {i["name"]: i for i in tgt_meta.get("indexes", [])}
    tgt_idx_keys = {(i["unique"], ",".join(i["columns"])) for i in tgt_meta.get("indexes", [])}
    idx_missing = [
        n for n, i in src_idx.items()
        if n not in tgt_idx and (i["unique"], ",".join(i["columns"])) not in tgt_idx_keys
    ]
    idx_disabled = [n for n, i in tgt_idx.items() if i.get("status") != "VALID"]

    src_con_keys = {(c["type_code"], ",".join(c["columns"])): c["name"] for c in src_meta.get("constraints", [])}
    tgt_con_keys = {(c["type_code"], ",".join(c["columns"])) for c in tgt_meta.get("constraints", [])}
    con_missing = [name for key, name in src_con_keys.items() if key not in tgt_con_keys]
    con_disabled = [
        c["name"] for c in tgt_meta.get("constraints", [])
        if c.get("status") == "DISABLED" and c.get("type_code") != "P"
    ]

    src_trg = {t["name"] for t in src_meta.get("triggers", [])}
    tgt_trg = {t["name"] for t in tgt_meta.get("triggers", [])}
    trg_missing = [n for n in src_trg if n not in tgt_trg]

    total = len(cols_missing) + len(cols_extra) + len(cols_type) + len(idx_missing) + len(idx_disabled) + len(con_missing) + len(con_disabled) + len(trg_missing)
    return {
        "ok": total == 0,
        "cols_missing": cols_missing,
        "cols_extra": cols_extra,
        "cols_type": cols_type,
        "idx_missing": idx_missing,
        "idx_disabled": idx_disabled,
        "con_missing": con_missing,
        "con_disabled": con_disabled,
        "trg_missing": trg_missing,
    }


def _diff_view(src_meta: dict, tgt_meta: dict) -> dict:
    src_sql = _normalize_sql(src_meta.get("sql_text"))
    tgt_sql = _normalize_sql(tgt_meta.get("sql_text"))
    sql_match = src_sql == tgt_sql
    status_match = src_meta.get("status") == tgt_meta.get("status")
    return {"ok": sql_match and status_match, "sql_match": sql_match, "status_match": status_match}


def _diff_mview(src_meta: dict, tgt_meta: dict) -> dict:
    src_sql = _normalize_sql(src_meta.get("sql_text"))
    tgt_sql = _normalize_sql(tgt_meta.get("sql_text"))
    sql_match = src_sql == tgt_sql
    refresh_match = src_meta.get("refresh_type") == tgt_meta.get("refresh_type")
    return {"ok": sql_match and refresh_match, "sql_match": sql_match, "refresh_match": refresh_match}


def _diff_code(src_meta: dict, tgt_meta: dict, obj_type: str) -> dict:
    if obj_type == "PACKAGE":
        spec_match = _normalize_code(src_meta.get("spec_source")) == _normalize_code(tgt_meta.get("spec_source"))
        body_match = _normalize_code(src_meta.get("body_source")) == _normalize_code(tgt_meta.get("body_source"))
        return {"ok": spec_match and body_match, "spec_match": spec_match, "body_match": body_match}
    elif obj_type == "TYPE":
        src_match = _normalize_code(src_meta.get("source")) == _normalize_code(tgt_meta.get("source"))
        body_match = _normalize_code(src_meta.get("body_source")) == _normalize_code(tgt_meta.get("body_source"))
        return {"ok": src_match and body_match, "source_match": src_match, "body_match": body_match}
    else:
        code_match = _normalize_code(src_meta.get("source_code")) == _normalize_code(tgt_meta.get("source_code"))
        return {"ok": code_match, "code_match": code_match}


def _diff_sequence(src_meta: dict, tgt_meta: dict) -> dict:
    fields = ["min_value", "max_value", "increment_by", "cache_size"]
    diffs = {f: (src_meta.get(f), tgt_meta.get(f)) for f in fields if src_meta.get(f) != tgt_meta.get(f)}
    return {"ok": len(diffs) == 0, "field_diffs": diffs}


def _diff_synonym(src_meta: dict, tgt_meta: dict) -> dict:
    fields = ["table_owner", "table_name", "db_link"]
    diffs = {f: (src_meta.get(f), tgt_meta.get(f)) for f in fields if src_meta.get(f) != tgt_meta.get(f)}
    return {"ok": len(diffs) == 0, "field_diffs": diffs}


# ── Public API ───────────────────────────────────────────────────────────────

_COMPARATORS = {
    "TABLE": _diff_table,
    "VIEW": _diff_view,
    "MATERIALIZED VIEW": _diff_mview,
    "FUNCTION": lambda s, t: _diff_code(s, t, "FUNCTION"),
    "PROCEDURE": lambda s, t: _diff_code(s, t, "PROCEDURE"),
    "PACKAGE": lambda s, t: _diff_code(s, t, "PACKAGE"),
    "TYPE": lambda s, t: _diff_code(s, t, "TYPE"),
    "SEQUENCE": _diff_sequence,
    "SYNONYM": _diff_synonym,
}


def compare_object(object_type: str, src_meta: dict, tgt_meta: dict) -> dict:
    """Compare a single object. Returns {ok: bool, ...diff_details}."""
    comparator = _COMPARATORS.get(object_type)
    if not comparator:
        return {"ok": True}
    return comparator(src_meta, tgt_meta)
