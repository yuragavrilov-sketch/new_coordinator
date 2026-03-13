"""Oracle SCN helpers and low-level connection factory."""


def open_oracle_conn(cfg: dict):
    """
    Open an Oracle connection from a service-config dict.
    cfg keys: host, port (opt, default 1521), service_name, user, password.
    """
    try:
        import oracledb
    except ImportError:
        raise ImportError("oracledb не установлен (pip install oracledb)")
    host         = cfg.get("host", "").strip()
    port         = int(cfg.get("port", 1521))
    service_name = cfg.get("service_name", "").strip()
    user         = cfg.get("user", "").strip()
    password     = cfg.get("password", "")
    if not host or not service_name or not user:
        raise ValueError("Oracle connection не настроен — проверьте Настройки")
    return oracledb.connect(
        user=user,
        password=password,
        dsn=f"{host}:{port}/{service_name}",
    )


def get_current_scn(cfg: dict) -> int:
    """Return the current SCN of an Oracle instance."""
    conn = open_oracle_conn(cfg)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT current_scn FROM v$database")
            row = cur.fetchone()
            if not row:
                raise RuntimeError("v$database returned no rows — проверьте привилегии")
            return int(row[0])
    finally:
        conn.close()


def check_supplemental_logging(cfg: dict, schema: str, table: str) -> bool:
    """
    Return True if the table has at least ALL COLUMNS supplemental logging enabled.
    Required for Debezium LogMiner connector to capture full row images.
    """
    conn = open_oracle_conn(cfg)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT log_mode FROM v$database
            """)
            row = cur.fetchone()
            if not row or row[0] != "ARCHIVELOG":
                raise RuntimeError(
                    f"Oracle не в режиме ARCHIVELOG (текущий: {row[0] if row else '?'}). "
                    "Debezium LogMiner требует ARCHIVELOG."
                )
            # Check supplemental logging at table level
            cur.execute("""
                SELECT supplemental_log_data_all
                FROM   all_tables
                WHERE  owner      = :s
                  AND  table_name = :t
            """, {"s": schema.upper(), "t": table.upper()})
            row = cur.fetchone()
            # supplemental_log_data_all = 'YES' means ALL COLUMNS logging is on
            return bool(row and row[0] == "YES")
    finally:
        conn.close()
