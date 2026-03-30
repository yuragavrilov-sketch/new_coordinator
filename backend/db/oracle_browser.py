"""Oracle DB browser — schemas, tables, columns/keys for migration wizard."""

_SYSTEM_SCHEMAS = frozenset([
    "SYS", "SYSTEM", "OUTLN", "DIP", "ORACLE_OCM", "DBSNMP", "APPQOSSYS",
    "WMSYS", "EXFSYS", "CTXSYS", "XDB", "ANONYMOUS", "ORDSYS", "ORDPLUGINS",
    "SI_INFORMTN_SCHEMA", "MDSYS", "OLAPSYS", "MDDATA", "XS$NULL",
    "APEX_PUBLIC_USER", "FLOWS_FILES", "DVSYS", "LBACSYS", "OJVMSYS",
    "GSMADMIN_INTERNAL", "GSMUSER", "AUDSYS", "DVF", "DBSFWUSER",
    "REMOTE_SCHEDULER_AGENT", "SYS$UMF", "SYSBACKUP", "SYSDG", "SYSKM", "SYSRAC",
])

_CATALOG_TYPES = frozenset([
    "TABLE", "VIEW", "MATERIALIZED VIEW",
    "FUNCTION", "PROCEDURE", "PACKAGE",
    "SEQUENCE", "SYNONYM", "TYPE",
])


def get_oracle_conn(db: str, configs: dict):
    """Open an Oracle connection. db = 'source' or 'target'."""
    cfg = configs.get(f"oracle_{db}", {})
    host         = cfg.get("host", "").strip()
    port         = cfg.get("port", 1521)
    service_name = cfg.get("service_name", "").strip()
    user         = cfg.get("user", "").strip()
    password     = cfg.get("password", "")
    if not host or not service_name or not user:
        raise ValueError(f"Oracle {db} не настроен — проверьте Настройки")
    try:
        import oracledb
    except ImportError:
        raise ImportError("oracledb не установлен (pip install oracledb)")
    return oracledb.connect(user=user, password=password, dsn=f"{host}:{port}/{service_name}")


def list_schemas(conn) -> list[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT owner FROM all_tables ORDER BY owner")
        return [r[0] for r in cur.fetchall() if r[0] not in _SYSTEM_SCHEMAS]


def list_tables(conn, schema: str) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT table_name FROM all_tables WHERE owner = :s ORDER BY table_name",
            {"s": schema},
        )
        return [r[0] for r in cur.fetchall()]


def list_all_objects(conn, schema: str) -> list[dict]:
    """List all DDL objects in schema (excluding PACKAGE BODY / TYPE BODY)."""
    placeholders = ",".join(f":t{i}" for i in range(len(_CATALOG_TYPES)))
    binds = {"s": schema}
    binds.update({f"t{i}": t for i, t in enumerate(sorted(_CATALOG_TYPES))})
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT object_name, object_type, status, last_ddl_time
            FROM   all_objects
            WHERE  owner = :s
              AND  object_type IN ({placeholders})
            ORDER  BY object_type, object_name
        """, binds)
        return [
            {
                "object_name": r[0],
                "object_type": r[1],
                "status": r[2],
                "last_ddl_time": r[3].isoformat() if r[3] else None,
            }
            for r in cur.fetchall()
        ]


def get_table_info(conn, schema: str, table: str) -> dict:
    with conn.cursor() as cur:
        # Columns
        cur.execute("""
            SELECT column_name, data_type, nullable
            FROM   all_tab_columns
            WHERE  owner = :s AND table_name = :t
            ORDER BY column_id
        """, {"s": schema, "t": table})
        columns = [{"name": r[0], "type": r[1], "nullable": r[2] == "Y"}
                   for r in cur.fetchall()]

        # PK columns
        cur.execute("""
            SELECT acc.column_name
            FROM   all_constraints  ac
            JOIN   all_cons_columns acc
                   ON  ac.constraint_name = acc.constraint_name
                   AND ac.owner           = acc.owner
            WHERE  ac.owner           = :s
              AND  ac.table_name      = :t
              AND  ac.constraint_type = 'P'
            ORDER BY acc.position
        """, {"s": schema, "t": table})
        pk_columns = [r[0] for r in cur.fetchall()]

        # UK constraints
        cur.execute("""
            SELECT ac.constraint_name, acc.column_name
            FROM   all_constraints  ac
            JOIN   all_cons_columns acc
                   ON  ac.constraint_name = acc.constraint_name
                   AND ac.owner           = acc.owner
            WHERE  ac.owner           = :s
              AND  ac.table_name      = :t
              AND  ac.constraint_type = 'U'
            ORDER BY ac.constraint_name, acc.position
        """, {"s": schema, "t": table})
        uk_map: dict = {}
        for cname, col in cur.fetchall():
            uk_map.setdefault(cname, []).append(col)

        # Unique indexes that are NOT already covered by a UK constraint
        # (CREATE UNIQUE INDEX without a matching CONSTRAINT UNIQUE)
        constraint_backed: set = set()
        cur.execute("""
            SELECT index_name
            FROM   all_constraints
            WHERE  owner      = :s
              AND  table_name = :t
              AND  constraint_type IN ('P', 'U')
              AND  index_name IS NOT NULL
        """, {"s": schema, "t": table})
        for row in cur.fetchall():
            constraint_backed.add(row[0])

        cur.execute("""
            SELECT ai.index_name,
                   LISTAGG(aic.column_name, ',')
                       WITHIN GROUP (ORDER BY aic.column_position) AS cols
            FROM   all_indexes     ai
            JOIN   all_ind_columns aic
                   ON  ai.index_name = aic.index_name
                   AND ai.owner      = aic.index_owner
            WHERE  ai.owner      = :s
              AND  ai.table_name = :t
              AND  ai.uniqueness = 'UNIQUE'
            GROUP BY ai.index_name
            ORDER BY ai.index_name
        """, {"s": schema, "t": table})
        for idx_name, cols_csv in cur.fetchall():
            if idx_name not in constraint_backed and idx_name not in uk_map:
                uk_map[idx_name] = cols_csv.split(",")

        uk_constraints = [{"name": k, "columns": v} for k, v in uk_map.items()]

    return {
        "columns":        columns,
        "pk_columns":     pk_columns,
        "uk_constraints": uk_constraints,
    }


# ── Full DDL info (for target-prep tab) ────────────────────────────────────

def get_full_ddl_info(conn, schema: str, table: str) -> dict:
    """Return full DDL snapshot: columns, constraints, indexes, triggers."""
    with conn.cursor() as cur:
        # Fetch columns without data_default (LONG type — cannot be used in
        # expressions; fetched separately below to avoid ORA-00997).
        cur.execute("""
            SELECT column_name, data_type, data_length, data_precision, data_scale,
                   nullable, column_id
            FROM   all_tab_columns
            WHERE  owner = :s AND table_name = :t
            ORDER BY column_id
        """, {"s": schema, "t": table})
        columns = [
            {
                "name":           r[0],
                "data_type":      r[1],
                "data_length":    r[2],
                "data_precision": r[3],
                "data_scale":     r[4],
                "nullable":       r[5] == "Y",
                "data_default":   None,   # populated below
                "column_id":      r[6],
            }
            for r in cur.fetchall()
        ]

    # Fetch data_default (LONG) one row at a time — the only safe way to read
    # Oracle LONG columns without risking ORA-00997.
    col_index = {c["name"]: c for c in columns}
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name, data_default
            FROM   all_tab_columns
            WHERE  owner = :s AND table_name = :t
            ORDER BY column_id
        """, {"s": schema, "t": table})
        for row in cur:
            col_name, default_val = row
            if col_name in col_index and default_val is not None:
                try:
                    col_index[col_name]["data_default"] = str(default_val).strip()
                except Exception:
                    pass   # ignore unreadable LONG values

    with conn.cursor() as cur:

        # Constraints (PK / UK / FK / CHECK) — subquery avoids outer-join NULLs
        cur.execute("""
            SELECT ac.constraint_name,
                   ac.constraint_type,
                   ac.status,
                   (SELECT LISTAGG(acc.column_name, ',')
                            WITHIN GROUP (ORDER BY acc.position)
                    FROM   all_cons_columns acc
                    WHERE  acc.constraint_name = ac.constraint_name
                      AND  acc.owner           = ac.owner) AS cols
            FROM   all_constraints ac
            WHERE  ac.owner           = :s
              AND  ac.table_name      = :t
              AND  ac.constraint_type IN ('P','U','R','C')
            ORDER BY ac.constraint_type, ac.constraint_name
        """, {"s": schema, "t": table})
        _ctype = {"P": "PRIMARY KEY", "U": "UNIQUE", "R": "FOREIGN KEY", "C": "CHECK"}
        constraints = [
            {
                "name":      r[0],
                "type":      _ctype.get(r[1], r[1]),
                "type_code": r[1],
                "status":    r[2],          # ENABLED / DISABLED
                "columns":   r[3].split(",") if r[3] else [],
            }
            for r in cur.fetchall()
        ]

        # Indexes
        # Note: partitioned indexes have status = 'N/A' in all_indexes;
        # real per-partition status lives in all_ind_partitions.
        # We treat 'N/A' as 'VALID' for comparison purposes.
        cur.execute("""
            SELECT ai.index_name, ai.uniqueness, ai.index_type,
                   ai.status, ai.partitioned,
                   LISTAGG(aic.column_name, ',')
                       WITHIN GROUP (ORDER BY aic.column_position) AS cols
            FROM   all_indexes     ai
            JOIN   all_ind_columns aic
                   ON  ai.index_name  = aic.index_name
                   AND ai.owner       = aic.index_owner
            WHERE  ai.owner      = :s
              AND  ai.table_name = :t
            GROUP BY ai.index_name, ai.uniqueness, ai.index_type,
                     ai.status, ai.partitioned
            ORDER BY ai.index_name
        """, {"s": schema, "t": table})
        indexes = [
            {
                "name":       r[0],
                "unique":     r[1] == "UNIQUE",
                "index_type": r[2],
                "status":     "VALID" if r[3] == "N/A" else r[3],
                "columns":    r[5].split(",") if r[5] else [],
            }
            for r in cur.fetchall()
        ]

        # Triggers
        cur.execute("""
            SELECT trigger_name, trigger_type, triggering_event, status
            FROM   all_triggers
            WHERE  owner = :s AND table_name = :t
            ORDER BY trigger_name
        """, {"s": schema, "t": table})
        triggers = [
            {"name": r[0], "trigger_type": r[1], "event": r[2], "status": r[3]}
            for r in cur.fetchall()
        ]

    return {
        "schema":      schema,
        "table":       table,
        "columns":     columns,
        "constraints": constraints,
        "indexes":     indexes,
        "triggers":    triggers,
    }


def execute_target_action(conn, action: str, schema: str, table: str, object_name: str) -> None:
    """Execute a DDL management action on the target database."""
    _valid = {
        "disable_index", "enable_index",
        "disable_trigger", "enable_trigger",
        "disable_constraint", "enable_constraint",
    }
    if action not in _valid:
        raise ValueError(f"Unknown action: {action}")

    # Build DDL statement — identifiers are double-quoted to preserve case
    s, t, o = schema.upper(), table.upper(), object_name
    if action == "disable_index":
        ddl = f'ALTER INDEX "{s}"."{o}" UNUSABLE'
    elif action == "enable_index":
        ddl = f'ALTER INDEX "{s}"."{o}" REBUILD'
    elif action == "disable_trigger":
        ddl = f'ALTER TRIGGER "{s}"."{o}" DISABLE'
    elif action == "enable_trigger":
        ddl = f'ALTER TRIGGER "{s}"."{o}" ENABLE'
    elif action == "disable_constraint":
        ddl = f'ALTER TABLE "{s}"."{t}" DISABLE CONSTRAINT "{o}"'
    else:  # enable_constraint
        ddl = f'ALTER TABLE "{s}"."{t}" ENABLE CONSTRAINT "{o}"'

    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()


def set_table_logging(conn, schema: str, table: str, nologging: bool) -> None:
    """Switch the target table between NOLOGGING and LOGGING mode.

    NOLOGGING before baseline load skips redo generation for direct-path
    inserts (APPEND hint), dramatically reducing I/O.  Restore to LOGGING
    afterwards so normal DML is protected.
    """
    s, t = schema.upper(), table.upper()
    mode = "NOLOGGING" if nologging else "LOGGING"
    with conn.cursor() as cur:
        cur.execute(f'ALTER TABLE "{s}"."{t}" {mode}')
    conn.commit()


def _constraint_backing_indexes(conn, schema: str, table: str) -> set[str]:
    """Return upper-cased names of indexes that back a PK or UNIQUE constraint.

    Uses the authoritative ``all_constraints.index_name`` column — this is
    Oracle's own mapping and covers all cases including non-unique indexes
    created with ``USING INDEX`` on a UNIQUE constraint.

    Also includes every index with ``uniqueness = 'UNIQUE'`` as a safety net.

    Oracle refuses to skip such indexes during INSERT even with
    SKIP_UNUSABLE_INDEXES = TRUE.  Marking them UNUSABLE causes ORA-26026.
    """
    s = schema.upper()
    t = table.upper()
    protected: set[str] = set()

    with conn.cursor() as cur:
        # Direct authoritative lookup
        cur.execute("""
            SELECT index_name
            FROM   all_constraints
            WHERE  owner = :s AND table_name = :t
              AND  constraint_type IN ('P', 'U')
              AND  index_name IS NOT NULL
        """, {"s": s, "t": t})
        for row in cur.fetchall():
            protected.add(row[0].upper())

        # Safety net: any index flagged UNIQUE in all_indexes
        cur.execute("""
            SELECT index_name
            FROM   all_indexes
            WHERE  owner = :s AND table_name = :t
              AND  uniqueness = 'UNIQUE'
        """, {"s": s, "t": t})
        for row in cur.fetchall():
            protected.add(row[0].upper())

    print(f"[oracle_browser] protected indexes for {s}.{t}: {protected}")
    return protected


def rebuild_unusable_constraint_indexes(conn, schema: str, table: str) -> list[str]:
    """Rebuild any UNUSABLE indexes that back PK/UNIQUE constraints.

    Recovers from a previous failed attempt that left such indexes in
    UNUSABLE state.  ORA-26026 is raised by INSERT if they are UNUSABLE,
    so they MUST be VALID before the load starts.

    Returns the list of index names that were rebuilt.
    """
    info = get_full_ddl_info(conn, schema, table)
    s = schema.upper()
    protected = _constraint_backing_indexes(conn, schema, table)
    rebuilt: list[str] = []
    with conn.cursor() as cur:
        for idx in info["indexes"]:
            if idx["name"].upper() in protected and idx["status"] == "UNUSABLE":
                try:
                    cur.execute(f'ALTER INDEX "{s}"."{idx["name"]}" REBUILD NOLOGGING')
                    rebuilt.append(idx["name"])
                except Exception as exc:
                    print(f"[oracle_browser] could not rebuild {idx['name']}: {exc}")
    conn.commit()
    return rebuilt


def mark_indexes_unusable(conn, schema: str, table: str, skip_pk: bool = True) -> list[str]:
    """Mark indexes on *table* as UNUSABLE so Oracle skips index maintenance
    during the subsequent bulk INSERT.

    SKIP_UNUSABLE_INDEXES = TRUE (Oracle session default) causes the INSERT to
    bypass UNUSABLE indexes — but ONLY for indexes that do NOT back a PK or
    UNIQUE constraint.  Marking such an index UNUSABLE causes ORA-26026.

    Detection uses ``all_constraints.index_name`` — the authoritative Oracle
    mapping — plus the ``uniqueness`` flag as a safety net.

    Returns the list of index names that were marked UNUSABLE.
    """
    info = get_full_ddl_info(conn, schema, table)
    s = schema.upper()
    protected = _constraint_backing_indexes(conn, schema, table)

    marked: list[str] = []
    with conn.cursor() as cur:
        for idx in info["indexes"]:
            if idx["status"] != "VALID":
                continue
            if idx["name"].upper() in protected:
                continue
            try:
                cur.execute(f'ALTER INDEX "{s}"."{idx["name"]}" UNUSABLE')
                marked.append(idx["name"])
            except Exception as exc:
                print(f"[oracle_browser] could not mark {idx['name']} UNUSABLE: {exc}")
    conn.commit()
    return marked


def disable_triggers(conn, schema: str, table: str) -> list[str]:
    """Disable all ENABLED triggers on *table* so they don't fire during bulk
    INSERT.  Returns the list of trigger names that were disabled.
    Re-enable them afterwards with enable_all_disabled_objects().
    """
    info = get_full_ddl_info(conn, schema, table)
    s = schema.upper()
    disabled: list[str] = []
    with conn.cursor() as cur:
        for trg in info["triggers"]:
            if trg["status"] == "ENABLED":
                try:
                    cur.execute(f'ALTER TRIGGER "{s}"."{trg["name"]}" DISABLE')
                    disabled.append(trg["name"])
                except Exception as exc:
                    print(f"[oracle_browser] could not disable trigger {trg['name']}: {exc}")
    conn.commit()
    return disabled


def enable_all_disabled_objects(conn, schema: str, table: str) -> dict:
    """
    Rebuild UNUSABLE indexes and re-enable DISABLED constraints on *table*.

    Triggers are NOT touched here — they must be re-enabled separately
    (e.g. via enable_triggers()) after CDC apply has fully caught up,
    otherwise triggers would fire on every replayed DML event.

    Returns a summary dict:
      { "enabled": {"indexes": [...], "constraints": [...]},
        "errors":  {"indexes": [...], "constraints": [...]} }

    Errors list items: {"name": str, "error": str}
    A single commit is issued after all statements.  Any per-object error is
    collected and returned rather than raised so the caller decides policy.
    """
    info = get_full_ddl_info(conn, schema, table)
    s = schema.upper()
    t = table.upper()
    enabled: dict = {"indexes": [], "constraints": []}
    errors:  dict = {"indexes": [], "constraints": []}

    with conn.cursor() as cur:
        for idx in info["indexes"]:
            if idx["status"] == "UNUSABLE":
                try:
                    cur.execute(f'ALTER INDEX "{s}"."{idx["name"]}" REBUILD NOLOGGING')
                    enabled["indexes"].append(idx["name"])
                except Exception as exc:
                    errors["indexes"].append({"name": idx["name"], "error": str(exc)})

        for con in info["constraints"]:
            if con["status"] == "DISABLED":
                try:
                    cur.execute(
                        f'ALTER TABLE "{s}"."{t}" ENABLE CONSTRAINT "{con["name"]}"'
                    )
                    enabled["constraints"].append(con["name"])
                except Exception as exc:
                    errors["constraints"].append({"name": con["name"], "error": str(exc)})

    conn.commit()
    return {"enabled": enabled, "errors": errors}


def enable_triggers(conn, schema: str, table: str) -> dict:
    """Re-enable all DISABLED triggers on *table*.

    Call this only after CDC apply has caught up — otherwise triggers fire
    on every replayed row.  Counterpart to disable_triggers().

    Returns {"enabled": [...], "errors": [...]}.
    """
    info = get_full_ddl_info(conn, schema, table)
    s = schema.upper()
    enabled: list[str] = []
    errors: list[dict] = []

    with conn.cursor() as cur:
        for trg in info["triggers"]:
            if trg["status"] == "DISABLED":
                try:
                    cur.execute(f'ALTER TRIGGER "{s}"."{trg["name"]}" ENABLE')
                    enabled.append(trg["name"])
                except Exception as exc:
                    errors.append({"name": trg["name"], "error": str(exc)})

    conn.commit()
    return {"enabled": enabled, "errors": errors}
