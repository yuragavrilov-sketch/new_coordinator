"""Shared helpers for orchestrator modules.

All phase handlers import from here to access DB connections,
config, broadcasting, and in-progress tracking.
"""

import threading
from datetime import datetime

from db.state_db import (
    get_active_migrations,
    row_to_dict,
    transition_phase,
    update_migration_fields,
)

# Module-level state populated by init()
_state: dict = {}

# Track migrations running in a dedicated thread
_in_progress: set[str] = set()
_in_progress_lock = threading.Lock()

# Track groups running in a dedicated thread
_group_in_progress: set[str] = set()
_group_in_progress_lock = threading.Lock()


def init(get_conn_fn, load_configs_fn, broadcast_fn) -> None:
    _state["get_conn"] = get_conn_fn
    _state["load_configs"] = load_configs_fn
    _state["broadcast"] = broadcast_fn


def get_conn():
    return _state["get_conn"]()


def configs() -> dict:
    return _state["load_configs"]()


def oracle_cfg(connection_id: str) -> dict:
    return configs().get(connection_id, {})


def broadcast(event: dict) -> None:
    _state["broadcast"](event)


def transition(migration_id: str, to_phase: str,
               message: str | None = None,
               error_code: str | None = None,
               error_text: str | None = None,
               extra_fields: dict | None = None) -> None:
    conn = get_conn()
    try:
        from_phase = transition_phase(
            conn, migration_id, to_phase,
            message=message,
            error_code=error_code,
            error_text=error_text,
            extra_fields=extra_fields,
        )
        conn.commit()
    finally:
        conn.close()

    broadcast({
        "type":         "migration_phase",
        "migration_id": migration_id,
        "from_phase":   from_phase,
        "phase":        to_phase,
        "ts":           datetime.utcnow().isoformat() + "Z",
    })
    print(f"[orchestrator] {migration_id}: {from_phase} → {to_phase}"
          + (f" ({message})" if message else ""))


def fail(migration_id: str, error_text: str,
         error_code: str = "ORCHESTRATOR_ERROR") -> None:
    try:
        transition(
            migration_id, "FAILED",
            message=error_text[:500],
            error_code=error_code,
            error_text=error_text[:2000],
        )
    except Exception as exc:
        print(f"[orchestrator] could not set FAILED for {migration_id}: {exc}")


def update(migration_id: str, fields: dict) -> None:
    conn = get_conn()
    try:
        update_migration_fields(conn, migration_id, fields)
        conn.commit()
    finally:
        conn.close()


def current_phase(migration_id: str) -> str | None:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT phase FROM migrations WHERE migration_id = %s",
                (migration_id,),
            )
            row = cur.fetchone()
            return row[0] if row else None
    finally:
        conn.close()


def safe_transition(migration_id: str, expected_phase: str, to_phase: str,
                    **kwargs) -> bool:
    cur = current_phase(migration_id)
    if cur != expected_phase:
        print(f"[orchestrator] {migration_id}: skip transition {expected_phase}→{to_phase}, "
              f"current phase is {cur} (cancelled?)")
        return False
    transition(migration_id, to_phase, **kwargs)
    return True


def in_prog(migration_id: str) -> bool:
    with _in_progress_lock:
        return migration_id in _in_progress


def mark_in_prog(migration_id: str) -> None:
    with _in_progress_lock:
        _in_progress.add(migration_id)


def unmark_in_prog(migration_id: str) -> None:
    with _in_progress_lock:
        _in_progress.discard(migration_id)


def group_in_prog(group_id: str) -> bool:
    with _group_in_progress_lock:
        return group_id in _group_in_progress


def mark_group_in_prog(group_id: str) -> None:
    with _group_in_progress_lock:
        _group_in_progress.add(group_id)


def unmark_group_in_prog(group_id: str) -> None:
    with _group_in_progress_lock:
        _group_in_progress.discard(group_id)
