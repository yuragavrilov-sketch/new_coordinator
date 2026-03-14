"""Baseline publish: TRUNCATE final table + INSERT /*+ APPEND */ FROM stage.

Uses ROWID-batched inserts to avoid ORA-01652 (TEMP tablespace exhaustion)
that occurs when Oracle needs to sort/hash the entire stage table in one shot.
Each batch commits independently which also limits undo usage.
"""

from services.oracle_scn import open_oracle_conn

# Rows per commit batch.  Configurable via caller; 500 000 is a safe default
# that keeps each INSERT small enough to avoid TEMP pressure while still being
# efficient (direct-path via APPEND_VALUES hint).
_DEFAULT_BATCH = 500_000


def publish_baseline(
    dst_cfg: dict,
    target_schema: str,
    target_table: str,
    stage_table: str,
    parallel_degree: int = 1,
    batch_size: int = _DEFAULT_BATCH,
) -> int:
    """
    1. TRUNCATE target_schema.target_table
    2. Fetch stage rows in ROWID-ordered batches of *batch_size*
    3. INSERT /*+ APPEND_VALUES */ each batch + COMMIT
    4. Return total rows inserted.

    Batching avoids ORA-01652 (TEMP exhaustion) that occurs when Oracle
    tries to process the entire stage table in a single INSERT SELECT.
    Each batch is a self-contained transaction so undo pressure is also low.

    parallel_degree is kept for API compatibility but ignored when
    batching — direct-path per-batch already saturates I/O on most systems.
    Idempotent on restart only if called after a fresh TRUNCATE (the
    orchestrator always TRUNCATEs first).
    """
    conn = open_oracle_conn(dst_cfg)
    try:
        tgt = f'"{target_schema.upper()}"."{target_table.upper()}"'
        stg = f'"{target_schema.upper()}"."{stage_table.upper()}"'

        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {tgt}")
        conn.commit()

        # Fetch column list once so INSERT is explicit (safer than SELECT *)
        conn2 = open_oracle_conn(dst_cfg)
        try:
            with conn2.cursor() as cur:
                cur.execute(f"SELECT * FROM {stg} WHERE 1=0")
                cols = [d[0] for d in cur.description]
        finally:
            conn2.close()

        col_list   = ", ".join(f'"{c}"' for c in cols)
        bind_names = [f":{i+1}" for i in range(len(cols))]
        insert_sql = (
            f'INSERT /*+ APPEND_VALUES */ INTO {tgt} ({col_list}) '
            f'VALUES ({", ".join(bind_names)})'
        )
        select_sql = f"SELECT {col_list} FROM {stg} ORDER BY ROWID"

        rows_inserted = 0
        with conn.cursor() as src_cur:
            src_cur.arraysize    = batch_size
            src_cur.prefetchrows = batch_size + 1
            src_cur.execute(select_sql)

            while True:
                batch = src_cur.fetchmany(batch_size)
                if not batch:
                    break
                with conn.cursor() as ins_cur:
                    ins_cur.executemany(insert_sql, batch)
                conn.commit()
                rows_inserted += len(batch)
                print(f"[baseline] {rows_inserted} rows inserted into {tgt}")

        return rows_inserted
    finally:
        conn.close()
