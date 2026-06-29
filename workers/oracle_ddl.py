"""Target-side DDL for enabling indexes/constraints after a bulk load.

Ported from backend/db/oracle_browser.py so the universal worker can run index
enabling as a claimed job (the workers package must not import backend).
Behaviour must match the backend version, including skipping partitioned indexes.
"""


def set_table_logging(conn, schema: str, table: str, nologging: bool) -> None:
    """Switch the target table between NOLOGGING and LOGGING mode.

    NOLOGGING before baseline load skips redo generation for direct-path
    inserts (APPEND hint), dramatically reducing I/O.  Restore to LOGGING
    afterwards so normal DML is protected.

    Skips silently for Global Temporary Tables — Oracle rejects
    ALTER TABLE LOGGING/NOLOGGING on GTT with ORA-14451.
    """
    s, t = schema.upper(), table.upper()
    if is_temporary_table(conn, s, t):
        print(f"[oracle_ddl] set_table_logging skipped: {s}.{t} is a temporary table")
        return
    mode = "NOLOGGING" if nologging else "LOGGING"
    with conn.cursor() as cur:
        cur.execute(f'ALTER TABLE "{s}"."{t}" {mode}')
    conn.commit()


def is_temporary_table(conn, schema: str, table: str) -> bool:
    """Return True if {schema}.{table} is a Global Temporary Table.

    Temp tables do not support LOGGING/NOLOGGING (ORA-14451), and their
    indexes have no UNUSABLE state in the normal sense — so callers skip
    rebuild and logging switches for them.
    """
    s, t = schema.upper(), table.upper()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT temporary FROM all_tables WHERE owner = :s AND table_name = :t",
            {"s": s, "t": t},
        )
        row = cur.fetchone()
    return bool(row) and (row[0] or "N").upper() == "Y"


def _list_indexes(conn, schema: str, table: str) -> list[dict]:
    """Return [{name, status, partitioned}] for all_indexes on schema.table.

    Status is normalised: 'N/A' (Oracle's placeholder for partitioned indexes
    which have per-partition status) is mapped to 'VALID' — matching the
    normalisation in oracle_browser.get_full_ddl_info.
    """
    s, t = schema.upper(), table.upper()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT index_name, status, partitioned
            FROM   all_indexes
            WHERE  owner      = :s
              AND  table_name = :t
            ORDER BY index_name
            """,
            {"s": s, "t": t},
        )
        return [
            {
                "name":        r[0],
                "status":      "VALID" if r[1] == "N/A" else r[1],
                "partitioned": r[2] == "YES",
            }
            for r in cur.fetchall()
        ]


def _list_constraints(conn, schema: str, table: str) -> list[dict]:
    """Return [{name, type_code, status}] for all_constraints on schema.table."""
    s, t = schema.upper(), table.upper()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT constraint_name, constraint_type, status
            FROM   all_constraints
            WHERE  owner           = :s
              AND  table_name      = :t
              AND  constraint_type IN ('P', 'U', 'R', 'C')
            ORDER BY constraint_type, constraint_name
            """,
            {"s": s, "t": t},
        )
        return [
            {
                "name":      r[0],
                "type_code": r[1],
                "status":    r[2],
            }
            for r in cur.fetchall()
        ]


def _constraint_backing_indexes(conn, schema: str, table: str) -> set[str]:
    """Return upper-cased names of indexes that back a PK or UNIQUE constraint.

    Uses the authoritative all_constraints.index_name column — Oracle's own
    mapping, covering all cases including non-unique indexes created with
    USING INDEX on a UNIQUE constraint.

    Also includes every index with uniqueness = 'UNIQUE' as a safety net.

    Oracle refuses to skip such indexes during INSERT even with
    SKIP_UNUSABLE_INDEXES = TRUE.  Marking them UNUSABLE causes ORA-26026.
    """
    s = schema.upper()
    t = table.upper()
    protected: set[str] = set()

    with conn.cursor() as cur:
        # Direct authoritative lookup
        cur.execute(
            """
            SELECT index_name
            FROM   all_constraints
            WHERE  owner = :s AND table_name = :t
              AND  constraint_type IN ('P', 'U')
              AND  index_name IS NOT NULL
            """,
            {"s": s, "t": t},
        )
        for row in cur.fetchall():
            protected.add(row[0].upper())

        # Safety net: any index flagged UNIQUE in all_indexes
        cur.execute(
            """
            SELECT index_name
            FROM   all_indexes
            WHERE  owner = :s AND table_name = :t
              AND  uniqueness = 'UNIQUE'
            """,
            {"s": s, "t": t},
        )
        for row in cur.fetchall():
            protected.add(row[0].upper())

    print(f"[oracle_ddl] protected indexes for {s}.{t}: {protected}")
    return protected


def enable_table_objects(conn, schema: str, table: str) -> dict:
    """Set the table back to LOGGING, rebuild UNUSABLE indexes (skipping
    partitioned ones), and enable DISABLED constraints. Returns the same shape
    as oracle_browser.enable_all_disabled_objects.

    Foreign keys are enabled NOVALIDATE so per-table migration does not fail
    on ORA-02298 while the referenced parent rows are loaded by another table
    in the same schema pack.

    Returns:
      {"enabled": {"indexes": [...], "constraints": [...], "fk_novalidate": [...]},
       "errors":  {"indexes": [...], "constraints": [...]}}
    """
    set_table_logging(conn, schema, table, nologging=False)
    s = schema.upper()
    t = table.upper()
    # NOLOGGING is not valid on GTT indexes (ORA-14451). Use plain REBUILD for
    # temporary tables.
    is_temp = is_temporary_table(conn, s, t)
    rebuild_clause = "REBUILD" if is_temp else "REBUILD NOLOGGING"
    enabled: dict = {"indexes": [], "constraints": [], "fk_novalidate": []}
    errors: dict = {"indexes": [], "constraints": []}

    with conn.cursor() as cur:
        for idx in _list_indexes(conn, s, t):
            if idx.get("partitioned"):
                # ORA-14086: a partitioned index can't be rebuilt as a whole;
                # it also never gets marked UNUSABLE by mark_indexes_unusable.
                continue
            if idx["status"] == "UNUSABLE":
                try:
                    cur.execute(
                        f'ALTER INDEX "{s}"."{idx["name"]}" {rebuild_clause}'
                    )
                    enabled["indexes"].append(idx["name"])
                except Exception as exc:
                    errors["indexes"].append({"name": idx["name"], "error": str(exc)})

        for con in _list_constraints(conn, s, t):
            if con["status"] == "DISABLED":
                try:
                    if con.get("type_code") == "R":
                        cur.execute(
                            f'ALTER TABLE "{s}"."{t}" ENABLE NOVALIDATE CONSTRAINT "{con["name"]}"'
                        )
                        enabled["fk_novalidate"].append(con["name"])
                        continue
                    cur.execute(
                        f'ALTER TABLE "{s}"."{t}" ENABLE CONSTRAINT "{con["name"]}"'
                    )
                    enabled["constraints"].append(con["name"])
                except Exception as exc:
                    errors["constraints"].append(
                        {"name": con["name"], "error": str(exc)}
                    )

    conn.commit()
    return {"enabled": enabled, "errors": errors}
