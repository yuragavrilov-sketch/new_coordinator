"""Migration state helpers — used by orchestrator and routes."""

from db.serialization import row_to_dict


def get_active_migrations(conn) -> list[dict]:
    """Return all migrations in active (non-terminal) phases."""
    _ACTIVE_PHASES = (
        "NEW", "PREPARING", "SCN_FIXED",
        "CONNECTOR_STARTING", "CDC_BUFFERING",
        "TOPIC_CREATING",
        "CHUNKING", "BULK_LOADING", "BULK_LOADED",
        "STAGE_VALIDATING", "STAGE_VALIDATED",
        "BASELINE_PUBLISHING", "BASELINE_LOADING", "BASELINE_PUBLISHED",
        "STAGE_DROPPING", "INDEXES_ENABLING",
        "DATA_VERIFYING", "DATA_MISMATCH",
        "CDC_APPLY_STARTING", "CDC_APPLYING", "CDC_CATCHING_UP", "CDC_CAUGHT_UP",
        "STEADY_STATE",
        "CANCELLING",
    )
    placeholders = ",".join(["%s"] * len(_ACTIVE_PHASES))
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT * FROM migrations WHERE phase IN ({placeholders})",
            _ACTIVE_PHASES,
        )
        return [row_to_dict(cur, r) for r in cur.fetchall()]


def transition_phase(
    conn,
    migration_id: str,
    to_phase: str,
    *,
    actor_type: str = "SYSTEM",
    actor_id: str | None = None,
    message: str | None = None,
    error_code: str | None = None,
    error_text: str | None = None,
    extra_fields: dict | None = None,
) -> str:
    """
    Transition a migration to *to_phase*.
    Returns the previous phase.
    Caller must commit/rollback the connection.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT phase FROM migrations WHERE migration_id = %s FOR UPDATE",
            (migration_id,),
        )
        row = cur.fetchone()
        if not row:
            raise ValueError(f"Migration {migration_id} not found")
        from_phase = row[0]

        fields: dict = {
            "phase":            to_phase,
            "state_changed_at": "NOW()",
            "updated_at":       "NOW()",
        }
        if to_phase == "FAILED":
            if error_code:
                fields["error_code"] = error_code
            if error_text:
                fields["error_text"] = error_text
            fields["failed_phase"] = from_phase
        if extra_fields:
            fields.update(extra_fields)

        set_parts, values = [], []
        for k, v in fields.items():
            if v == "NOW()":
                set_parts.append(f"{k} = NOW()")
            else:
                set_parts.append(f"{k} = %s")
                values.append(v)
        values.append(migration_id)
        cur.execute(
            f"UPDATE migrations SET {', '.join(set_parts)} WHERE migration_id = %s",
            values,
        )
        cur.execute("""
            INSERT INTO migration_state_history
                (migration_id, from_phase, to_phase, message, actor_type, actor_id)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (migration_id, from_phase, to_phase, message, actor_type, actor_id))
    return from_phase


def update_migration_fields(conn, migration_id: str, fields: dict) -> None:
    """
    Update arbitrary columns on a migration row.
    Caller must commit the connection.
    """
    if not fields:
        return
    set_parts = [f"{k} = %s" for k in fields]
    values = list(fields.values()) + [migration_id]
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE migrations SET {', '.join(set_parts)}, updated_at = NOW() "
            f"WHERE migration_id = %s",
            values,
        )
