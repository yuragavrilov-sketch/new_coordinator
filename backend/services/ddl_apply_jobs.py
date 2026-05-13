"""DDL apply jobs — API-side service.

Queues jobs into ddl_apply_jobs; workers (workers/ddl_apply_worker.py) pick them
up via SELECT … FOR UPDATE SKIP LOCKED. Worker writes progress to
schema_migration_events; this module also writes a "queued" event on submit
so the user sees feedback before the worker picks anything up.
"""

import json
from typing import Iterable


# Actions supported by the worker.
_VALID_ACTIONS = {"create_missing", "sync_diff", "recreate"}

# Oracle types whose DDL can be re-applied via CREATE OR REPLACE.
# For other types sync_diff would need DROP+CREATE — refused by default.
REPLACEABLE_TYPES = {
    "VIEW", "PROCEDURE", "FUNCTION", "PACKAGE", "PACKAGE BODY",
    "TRIGGER", "TYPE", "TYPE BODY", "SYNONYM",
}


def _resolve_object_type(fe_or_oracle: str) -> str:
    """Accept either Oracle canonical (TABLE, MATERIALIZED VIEW, ...) or
    frontend alias (MVIEW, DBLINK). Return Oracle canonical."""
    fe_or_oracle = (fe_or_oracle or "").upper().strip()
    fe_map = {
        "MVIEW":  "MATERIALIZED VIEW",
        "DBLINK": "DATABASE LINK",
    }
    return fe_map.get(fe_or_oracle, fe_or_oracle)


def submit_jobs(
    conn,
    sm_id: str,
    action: str,
    objects: list[dict],
) -> dict:
    """Insert one ddl_apply_jobs row per object + a queued event.

    objects: [{"type": "<oracle or fe>", "name": "..."}]
    Returns {"queued": n, "skipped": [...]}.
    """
    if action not in _VALID_ACTIONS:
        raise ValueError(f"unknown action: {action}")
    queued: list[str] = []
    skipped: list[dict] = []

    with conn.cursor() as cur:
        # Verify the schema_migration exists (avoids cryptic FK error later)
        cur.execute(
            "SELECT 1 FROM schema_migrations WHERE schema_migration_id = %s",
            (sm_id,),
        )
        if not cur.fetchone():
            raise ValueError("schema_migration not found")

        for obj in objects:
            otype = _resolve_object_type(obj.get("type", ""))
            oname = (obj.get("name") or "").strip()
            if not otype or not oname:
                skipped.append({**obj, "reason": "missing type/name"})
                continue
            if action == "sync_diff" and otype not in REPLACEABLE_TYPES:
                skipped.append({**obj, "reason": f"sync_diff not supported for {otype}"})
                continue

            cur.execute("""
                INSERT INTO ddl_apply_jobs
                    (schema_migration_id, action, object_type, object_name)
                VALUES (%s, %s, %s, %s)
                RETURNING job_id
            """, (sm_id, action, otype, oname))
            job_id = cur.fetchone()[0]
            queued.append(str(job_id))

            cur.execute("""
                INSERT INTO schema_migration_events
                    (schema_migration_id, event_type, object_type, object_name,
                     level, message, job_id)
                VALUES (%s, 'ddl_apply.queued', %s, %s, 'info',
                        %s, %s)
            """, (sm_id, otype, oname,
                  f"queued for {action.replace('_', ' ')}", job_id))

        conn.commit()
    return {"queued": len(queued), "job_ids": queued, "skipped": skipped}


def cancel_pending(conn, sm_id: str) -> int:
    """Cancel only PENDING jobs (running ones are left alone — they'll finish).
    Returns count cancelled."""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE ddl_apply_jobs
            SET    state = 'CANCELLED', completed_at = NOW()
            WHERE  schema_migration_id = %s AND state = 'PENDING'
        """, (sm_id,))
        n = cur.rowcount
        if n:
            cur.execute("""
                INSERT INTO schema_migration_events
                    (schema_migration_id, event_type, level, message)
                VALUES (%s, 'ddl_apply.cancelled', 'info', %s)
            """, (sm_id, f"cancelled {n} pending job(s)"))
        conn.commit()
    return n


def list_jobs(conn, sm_id: str, limit: int = 100) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT job_id, action, object_type, object_name, state,
                   error_text, created_at, started_at, completed_at
            FROM   ddl_apply_jobs
            WHERE  schema_migration_id = %s
            ORDER BY created_at DESC
            LIMIT  %s
        """, (sm_id, limit))
        out = []
        for r in cur.fetchall():
            out.append({
                "job_id":       str(r[0]),
                "action":       r[1],
                "object_type":  r[2],
                "object_name":  r[3],
                "state":        r[4],
                "error_text":   r[5],
                "created_at":   r[6].isoformat() if r[6] else None,
                "started_at":   r[7].isoformat() if r[7] else None,
                "completed_at": r[8].isoformat() if r[8] else None,
            })
        return out
