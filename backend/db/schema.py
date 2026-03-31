"""Schema initialisation — CREATE TABLE IF NOT EXISTS + column migrations."""

from db.pool import get_conn


def init_db() -> None:
    print("[state_db] running schema init / migrations...")
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS service_configs (
                    service_name VARCHAR(50) PRIMARY KEY,
                    config       JSONB NOT NULL DEFAULT '{}',
                    updated_at   TIMESTAMP DEFAULT NOW()
                )
            """)
            for svc in ("oracle_source", "oracle_target", "kafka", "kafka_connect"):
                cur.execute("""
                    INSERT INTO service_configs (service_name, config)
                    VALUES (%s, '{}'::jsonb)
                    ON CONFLICT (service_name) DO NOTHING
                """, (svc,))

            cur.execute("""
                CREATE TABLE IF NOT EXISTS migrations (
                    migration_id              uuid PRIMARY KEY,
                    migration_name            varchar(255) NOT NULL,
                    phase                     varchar(32)  NOT NULL DEFAULT 'DRAFT',
                    state_changed_at          timestamp    NOT NULL DEFAULT now(),

                    source_connection_id      varchar(64)  NOT NULL DEFAULT '',
                    target_connection_id      varchar(64)  NOT NULL DEFAULT '',

                    source_schema             varchar(128) NOT NULL DEFAULT '',
                    source_table              varchar(128) NOT NULL DEFAULT '',
                    target_schema             varchar(128) NOT NULL DEFAULT '',
                    target_table              varchar(128) NOT NULL DEFAULT '',

                    stage_table_name          varchar(128) NOT NULL DEFAULT '',
                    stage_tablespace          varchar(128) NOT NULL DEFAULT '',
                    connector_name            varchar(255) NOT NULL DEFAULT '',
                    topic_prefix              varchar(255) NOT NULL DEFAULT '',
                    consumer_group            varchar(255) NOT NULL DEFAULT '',

                    chunk_strategy            varchar(32)  NOT NULL DEFAULT '',
                    chunk_size                bigint       NOT NULL DEFAULT 10000,
                    max_parallel_workers      int          NOT NULL DEFAULT 1,
                    apply_mode                varchar(32)  NOT NULL DEFAULT '',

                    source_pk_exists          boolean      NOT NULL DEFAULT false,
                    source_uk_exists          boolean      NOT NULL DEFAULT false,

                    effective_key_type        varchar(32)  NOT NULL DEFAULT '',
                    effective_key_source      varchar(32)  NOT NULL DEFAULT '',
                    effective_key_columns_json text         NOT NULL DEFAULT '[]',

                    key_uniqueness_validated  boolean      NOT NULL DEFAULT false,
                    key_validation_status     varchar(32)  NULL,
                    key_validation_message    text         NULL,

                    start_scn                 numeric(20,0) NULL,
                    scn_fixed_at              timestamp    NULL,

                    created_by                varchar(128) NULL,
                    description               text         NULL,

                    created_at                timestamp    NOT NULL DEFAULT now(),
                    updated_at                timestamp    NOT NULL DEFAULT now(),

                    locked_by                 varchar(128) NULL,
                    lock_until                timestamp    NULL,

                    error_code                varchar(64)  NULL,
                    error_text                text         NULL,
                    failed_phase              varchar(32)  NULL,
                    retry_count               int          NOT NULL DEFAULT 0
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS migration_state_history (
                    id                bigserial    PRIMARY KEY,
                    migration_id      uuid         NOT NULL REFERENCES migrations(migration_id),

                    from_phase        varchar(32)  NULL,
                    to_phase          varchar(32)  NOT NULL,

                    transition_status varchar(16)  NOT NULL DEFAULT 'SUCCESS',
                    transition_reason varchar(64)  NULL,
                    message           text         NULL,

                    actor_type        varchar(32)  NOT NULL DEFAULT 'SYSTEM',
                    actor_id          varchar(128) NULL,
                    correlation_id    varchar(128) NULL,

                    created_at        timestamp    NOT NULL DEFAULT current_timestamp
                )
            """)

            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_msh_migration_created
                    ON migration_state_history(migration_id, created_at DESC)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_migrations_phase
                    ON migrations(phase)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_migrations_state_changed
                    ON migrations(state_changed_at DESC)
            """)

            # ── Column migrations on migrations table (idempotent) ────────────
            for col_sql in [
                "ALTER TABLE migrations ADD COLUMN IF NOT EXISTS total_rows                BIGINT",
                "ALTER TABLE migrations ADD COLUMN IF NOT EXISTS total_chunks              INTEGER",
                "ALTER TABLE migrations ADD COLUMN IF NOT EXISTS chunks_done               INTEGER NOT NULL DEFAULT 0",
                "ALTER TABLE migrations ADD COLUMN IF NOT EXISTS chunks_failed             INTEGER NOT NULL DEFAULT 0",
                "ALTER TABLE migrations ADD COLUMN IF NOT EXISTS validate_hash_sample      BOOLEAN NOT NULL DEFAULT FALSE",
                "ALTER TABLE migrations ADD COLUMN IF NOT EXISTS validation_result         JSONB",
                "ALTER TABLE migrations ADD COLUMN IF NOT EXISTS connector_status          VARCHAR(50)",
                "ALTER TABLE migrations ADD COLUMN IF NOT EXISTS kafka_lag                 BIGINT",
                "ALTER TABLE migrations ADD COLUMN IF NOT EXISTS kafka_lag_checked_at      TIMESTAMPTZ",
                "ALTER TABLE migrations ADD COLUMN IF NOT EXISTS rows_loaded               BIGINT NOT NULL DEFAULT 0",
                "ALTER TABLE migrations ADD COLUMN IF NOT EXISTS baseline_parallel_degree  INTEGER NOT NULL DEFAULT 1",
                "ALTER TABLE migrations ADD COLUMN IF NOT EXISTS baseline_batch_size       INTEGER NOT NULL DEFAULT 500000",
                "ALTER TABLE migrations ADD COLUMN IF NOT EXISTS migration_strategy        VARCHAR(32) NOT NULL DEFAULT 'STAGE'",
                "ALTER TABLE migrations ADD COLUMN IF NOT EXISTS baseline_chunks_total     INTEGER",
                "ALTER TABLE migrations ADD COLUMN IF NOT EXISTS baseline_chunks_done      INTEGER NOT NULL DEFAULT 0",
                "ALTER TABLE migrations ADD COLUMN IF NOT EXISTS stage_tablespace          VARCHAR(128) NOT NULL DEFAULT ''",
                "ALTER TABLE migrations ADD COLUMN IF NOT EXISTS queue_position            INTEGER",
                "ALTER TABLE migrations ADD COLUMN IF NOT EXISTS migration_mode            VARCHAR(32) NOT NULL DEFAULT 'CDC'",
                "ALTER TABLE migrations ADD COLUMN IF NOT EXISTS data_compare_task_id      UUID",
            ]:
                cur.execute(col_sql)
                col_name = col_sql.split("IF NOT EXISTS")[1].strip().split()[0]
                print(f"[state_db]   column ok: migrations.{col_name}")

            # ── migration_chunks ──────────────────────────────────────────
            cur.execute("""
                CREATE TABLE IF NOT EXISTS migration_chunks (
                    chunk_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    migration_id    UUID NOT NULL REFERENCES migrations(migration_id) ON DELETE CASCADE,
                    chunk_seq       INTEGER NOT NULL,
                    rowid_start     VARCHAR(20) NOT NULL,
                    rowid_end       VARCHAR(20) NOT NULL,
                    status          VARCHAR(20) NOT NULL DEFAULT 'PENDING',
                    rows_loaded     BIGINT NOT NULL DEFAULT 0,
                    worker_id       VARCHAR(200),
                    claimed_at      TIMESTAMPTZ,
                    started_at      TIMESTAMPTZ,
                    completed_at    TIMESTAMPTZ,
                    error_text      TEXT,
                    retry_count     INTEGER NOT NULL DEFAULT 0,
                    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (migration_id, chunk_seq)
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_chunks_migration_status
                    ON migration_chunks (migration_id, status)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_chunks_pending
                    ON migration_chunks (status, created_at)
                    WHERE status = 'PENDING'
            """)
            # chunk_type distinguishes bulk-load chunks from baseline-publish chunks
            cur.execute(
                "ALTER TABLE migration_chunks ADD COLUMN IF NOT EXISTS "
                "chunk_type VARCHAR(10) NOT NULL DEFAULT 'BULK'"
            )
            print("[state_db]   column ok: migration_chunks.chunk_type")

            # Fix unique constraint: (migration_id, chunk_seq) → (migration_id, chunk_type, chunk_seq)
            # so BULK and BASELINE chunks with the same seq don't collide.
            cur.execute("""
                DO $$
                BEGIN
                    IF EXISTS (
                        SELECT 1 FROM pg_constraint
                        WHERE conname = 'migration_chunks_migration_id_chunk_seq_key'
                    ) THEN
                        ALTER TABLE migration_chunks
                            DROP CONSTRAINT migration_chunks_migration_id_chunk_seq_key;
                        ALTER TABLE migration_chunks
                            ADD CONSTRAINT migration_chunks_migration_id_chunk_type_chunk_seq_key
                            UNIQUE (migration_id, chunk_type, chunk_seq);
                        RAISE NOTICE 'migration_chunks unique constraint upgraded';
                    END IF;
                END$$
            """)
            # Ensure constraint exists even on fresh installs that never had the old one
            cur.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_constraint
                        WHERE conname = 'migration_chunks_migration_id_chunk_type_chunk_seq_key'
                    ) THEN
                        ALTER TABLE migration_chunks
                            ADD CONSTRAINT migration_chunks_migration_id_chunk_type_chunk_seq_key
                            UNIQUE (migration_id, chunk_type, chunk_seq);
                    END IF;
                END$$
            """)

            # ── migration_cdc_state ───────────────────────────────────────
            cur.execute("""
                CREATE TABLE IF NOT EXISTS migration_cdc_state (
                    migration_id      UUID PRIMARY KEY REFERENCES migrations(migration_id) ON DELETE CASCADE,
                    consumer_group    VARCHAR(200) NOT NULL DEFAULT '',
                    topic             VARCHAR(200) NOT NULL DEFAULT '',
                    total_lag         BIGINT NOT NULL DEFAULT 0,
                    lag_by_partition  JSONB,
                    last_event_scn    NUMERIC,
                    last_event_ts     TIMESTAMPTZ,
                    apply_rate_rps    NUMERIC(10,2),
                    rows_applied      BIGINT NOT NULL DEFAULT 0,
                    worker_id         VARCHAR(200),
                    worker_heartbeat  TIMESTAMPTZ,
                    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            # Column migrations for migration_cdc_state (upgrades from older schema)
            cur.execute(
                "ALTER TABLE migration_cdc_state ADD COLUMN IF NOT EXISTS rows_applied BIGINT NOT NULL DEFAULT 0"
            )
            print("[state_db]   column ok: migration_cdc_state.rows_applied")

            # ── checklist tables ────────────────────────────────────────
            cur.execute("""
                CREATE TABLE IF NOT EXISTS checklist_lists (
                    list_id    SERIAL PRIMARY KEY,
                    name       VARCHAR(255) NOT NULL UNIQUE,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS checklist_items (
                    item_id      SERIAL PRIMARY KEY,
                    list_id      INTEGER NOT NULL REFERENCES checklist_lists(list_id) ON DELETE CASCADE,
                    schema_name  VARCHAR(128) NOT NULL DEFAULT '',
                    table_name   VARCHAR(128) NOT NULL,
                    decision     VARCHAR(20) NOT NULL DEFAULT 'migrate',
                    status       VARCHAR(20) NOT NULL DEFAULT 'pending',
                    created_at   TIMESTAMP NOT NULL DEFAULT NOW(),
                    UNIQUE (list_id, schema_name, table_name)
                )
            """)

            # ── connector_groups ──────────────────────────────────────
            cur.execute("""
                CREATE TABLE IF NOT EXISTS connector_groups (
                    group_id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    group_name              VARCHAR(255) NOT NULL,
                    source_connection_id    VARCHAR(64)  NOT NULL DEFAULT 'oracle_source',
                    connector_name          VARCHAR(255) NOT NULL,
                    topic_prefix            VARCHAR(255) NOT NULL,
                    consumer_group_prefix   VARCHAR(255) NOT NULL DEFAULT '',
                    status                  VARCHAR(32)  NOT NULL DEFAULT 'PENDING',
                    error_text              TEXT,
                    connector_config_json   JSONB,
                    created_at              TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
                    updated_at              TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
                    UNIQUE (connector_name)
                )
            """)
            cur.execute(
                "ALTER TABLE connector_groups ADD COLUMN IF NOT EXISTS "
                "run_id VARCHAR(8) NOT NULL DEFAULT ''"
            )
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_connector_groups_status
                    ON connector_groups(status)
            """)

            # ── group_tables (tables belonging to a group, decoupled from migrations) ──
            cur.execute("""
                CREATE TABLE IF NOT EXISTS group_tables (
                    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    group_id                UUID NOT NULL REFERENCES connector_groups(group_id) ON DELETE CASCADE,
                    source_schema           VARCHAR(128) NOT NULL,
                    source_table            VARCHAR(128) NOT NULL,
                    target_schema           VARCHAR(128) NOT NULL,
                    target_table            VARCHAR(128) NOT NULL,
                    effective_key_type      VARCHAR(32)  NOT NULL DEFAULT 'NONE',
                    effective_key_columns_json TEXT NOT NULL DEFAULT '[]',
                    source_pk_exists        BOOLEAN NOT NULL DEFAULT FALSE,
                    source_uk_exists        BOOLEAN NOT NULL DEFAULT FALSE,
                    topic_name              VARCHAR(512) NOT NULL DEFAULT '',
                    created_at              TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
                    UNIQUE (group_id, source_schema, source_table)
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_group_tables_group_id
                    ON group_tables(group_id)
            """)

            # ── group_state_history ───────────────────────────────────────
            cur.execute("""
                CREATE TABLE IF NOT EXISTS group_state_history (
                    id              BIGSERIAL PRIMARY KEY,
                    group_id        UUID NOT NULL REFERENCES connector_groups(group_id) ON DELETE CASCADE,
                    from_status     VARCHAR(32),
                    to_status       VARCHAR(32) NOT NULL,
                    message         TEXT,
                    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_gsh_group_created
                    ON group_state_history(group_id, created_at DESC)
            """)

            # ── data_compare_tasks ─────────────────────────────────────
            cur.execute("""
                CREATE TABLE IF NOT EXISTS data_compare_tasks (
                    task_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    source_schema  VARCHAR(128) NOT NULL,
                    source_table   VARCHAR(128) NOT NULL,
                    target_schema  VARCHAR(128) NOT NULL,
                    target_table   VARCHAR(128) NOT NULL,
                    compare_mode   VARCHAR(20)  NOT NULL DEFAULT 'full',
                    last_n         INTEGER,
                    order_column   VARCHAR(128),
                    chunk_size     INTEGER NOT NULL DEFAULT 100000,
                    status         VARCHAR(20)  NOT NULL DEFAULT 'PENDING',
                    source_count   BIGINT,
                    target_count   BIGINT,
                    source_hash    NUMERIC,
                    target_hash    NUMERIC,
                    counts_match   BOOLEAN,
                    hash_match     BOOLEAN,
                    chunks_total   INTEGER NOT NULL DEFAULT 0,
                    chunks_done    INTEGER NOT NULL DEFAULT 0,
                    error_text     TEXT,
                    started_at     TIMESTAMPTZ,
                    completed_at   TIMESTAMPTZ,
                    created_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
                )
            """)

            # Column migrations for data_compare_tasks (upgrade from older schema)
            for col_sql in [
                "ALTER TABLE data_compare_tasks ADD COLUMN IF NOT EXISTS chunk_size    INTEGER NOT NULL DEFAULT 100000",
                "ALTER TABLE data_compare_tasks ADD COLUMN IF NOT EXISTS chunks_total  INTEGER NOT NULL DEFAULT 0",
                "ALTER TABLE data_compare_tasks ADD COLUMN IF NOT EXISTS chunks_done   INTEGER NOT NULL DEFAULT 0",
            ]:
                cur.execute(col_sql)

            # ── data_compare_chunks ───────────────────────────────────
            cur.execute("""
                CREATE TABLE IF NOT EXISTS data_compare_chunks (
                    chunk_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    task_id      UUID NOT NULL REFERENCES data_compare_tasks(task_id) ON DELETE CASCADE,
                    side         VARCHAR(10)  NOT NULL,
                    chunk_seq    INTEGER      NOT NULL,
                    rowid_start  VARCHAR(20)  NOT NULL,
                    rowid_end    VARCHAR(20)  NOT NULL,
                    status       VARCHAR(20)  NOT NULL DEFAULT 'PENDING',
                    row_count    BIGINT,
                    hash_sum     NUMERIC,
                    worker_id    VARCHAR(200),
                    claimed_at   TIMESTAMPTZ,
                    completed_at TIMESTAMPTZ,
                    error_text   TEXT,
                    retry_count  INTEGER NOT NULL DEFAULT 0,
                    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (task_id, side, chunk_seq)
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_dcc_task_status
                    ON data_compare_chunks(task_id, status)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_dcc_pending
                    ON data_compare_chunks(status, created_at)
                    WHERE status = 'PENDING'
            """)

            # Column migrations for data_compare_chunks (compare-both-sides columns)
            for col_sql in [
                "ALTER TABLE data_compare_chunks ADD COLUMN IF NOT EXISTS source_count  BIGINT",
                "ALTER TABLE data_compare_chunks ADD COLUMN IF NOT EXISTS source_hash   NUMERIC",
                "ALTER TABLE data_compare_chunks ADD COLUMN IF NOT EXISTS target_count  BIGINT",
                "ALTER TABLE data_compare_chunks ADD COLUMN IF NOT EXISTS target_hash   NUMERIC",
                "ALTER TABLE data_compare_chunks ADD COLUMN IF NOT EXISTS counts_match  BOOLEAN",
                "ALTER TABLE data_compare_chunks ADD COLUMN IF NOT EXISTS hash_match    BOOLEAN",
            ]:
                cur.execute(col_sql)
            print("[state_db]   columns ok: data_compare_chunks.{source,target}_{count,hash}, {counts,hash}_match")

            # ── migration_plans ───────────────────────────────────
            cur.execute("""
                CREATE TABLE IF NOT EXISTS migration_plans (
                    plan_id         SERIAL PRIMARY KEY,
                    name            TEXT NOT NULL,
                    src_schema      TEXT NOT NULL,
                    tgt_schema      TEXT NOT NULL,
                    connector_group_id UUID REFERENCES connector_groups(group_id),
                    defaults_json   JSONB NOT NULL DEFAULT '{}',
                    status          TEXT NOT NULL DEFAULT 'DRAFT',
                    created_at      TIMESTAMPTZ DEFAULT NOW(),
                    started_at      TIMESTAMPTZ,
                    completed_at    TIMESTAMPTZ
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS migration_plan_items (
                    item_id         SERIAL PRIMARY KEY,
                    plan_id         INTEGER NOT NULL REFERENCES migration_plans(plan_id) ON DELETE CASCADE,
                    table_name      TEXT NOT NULL,
                    mode            TEXT NOT NULL DEFAULT 'CDC',
                    batch_order     INTEGER NOT NULL DEFAULT 1,
                    sort_order      INTEGER NOT NULL DEFAULT 0,
                    overrides_json  JSONB NOT NULL DEFAULT '{}',
                    migration_id    UUID REFERENCES migrations(migration_id),
                    status          TEXT NOT NULL DEFAULT 'PENDING'
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_mpi_plan_id
                    ON migration_plan_items(plan_id)
            """)

            # ── DDL Catalog cache ──────────────────────────────────────
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ddl_snapshots (
                    snapshot_id   SERIAL PRIMARY KEY,
                    src_schema    TEXT NOT NULL,
                    tgt_schema    TEXT NOT NULL,
                    loaded_at     TIMESTAMPTZ DEFAULT now()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ddl_objects (
                    id            SERIAL PRIMARY KEY,
                    snapshot_id   INT NOT NULL REFERENCES ddl_snapshots(snapshot_id) ON DELETE CASCADE,
                    db_side       TEXT NOT NULL,
                    object_type   TEXT NOT NULL,
                    object_name   TEXT NOT NULL,
                    oracle_status TEXT,
                    last_ddl_time TIMESTAMPTZ,
                    metadata      JSONB DEFAULT '{}'
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS ix_ddl_objects_snapshot
                ON ddl_objects(snapshot_id, db_side, object_type)
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ddl_compare_results (
                    id            SERIAL PRIMARY KEY,
                    snapshot_id   INT NOT NULL REFERENCES ddl_snapshots(snapshot_id) ON DELETE CASCADE,
                    object_type   TEXT NOT NULL,
                    object_name   TEXT NOT NULL,
                    match_status  TEXT NOT NULL DEFAULT 'UNKNOWN',
                    diff          JSONB DEFAULT '{}'
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS ix_ddl_compare_snapshot
                ON ddl_compare_results(snapshot_id, object_type)
            """)

            # ── group_id FK on migrations ────────────────────────────────
            cur.execute(
                "ALTER TABLE migrations ADD COLUMN IF NOT EXISTS "
                "group_id UUID REFERENCES connector_groups(group_id)"
            )
            print("[state_db]   column ok: migrations.group_id")
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_migrations_group_id
                    ON migrations(group_id)
            """)

        conn.commit()
        print("[state_db] schema init complete")
    finally:
        conn.close()
