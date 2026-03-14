"""Baseline publish: TRUNCATE final table + INSERT /*+ APPEND */ FROM stage."""

from services.oracle_scn import open_oracle_conn


def publish_baseline(
    dst_cfg: dict,
    target_schema: str,
    target_table: str,
    stage_table: str,
    parallel_degree: int = 1,
) -> int:
    """
    1. TRUNCATE target_schema.target_table
    2. (optional) ALTER SESSION ENABLE PARALLEL DML
    3. INSERT /*+ APPEND [PARALLEL(<n>)] */ INTO target SELECT * FROM stage
    4. COMMIT

    parallel_degree controls Oracle PQ slaves for the INSERT SELECT.
    This is independent of max_parallel_workers (which controls bulk chunk
    workers). When parallel_degree=1 (default), uses APPEND only —
    direct-path insert without Oracle parallelism.

    Returns the number of rows inserted.
    Idempotent on restart (TRUNCATE makes it safe to retry).
    """
    conn = open_oracle_conn(dst_cfg)
    try:
        tgt = f'"{target_schema.upper()}"."{target_table.upper()}"'
        stg = f'"{target_schema.upper()}"."{stage_table.upper()}"'

        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {tgt}")

            if parallel_degree > 1:
                # Required: without this Oracle silently ignores PARALLEL
                # hint on DML and only parallelises the SELECT part.
                cur.execute("ALTER SESSION ENABLE PARALLEL DML")
                hint = f"APPEND PARALLEL({parallel_degree})"
            else:
                hint = "APPEND"

            cur.execute(
                f"INSERT /*+ {hint} */ INTO {tgt} SELECT * FROM {stg}"
            )
            rows_inserted = cur.rowcount

        conn.commit()
        return rows_inserted if rows_inserted >= 0 else 0
    finally:
        conn.close()
