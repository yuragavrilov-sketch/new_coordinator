import threading

import services.debezium as debezium
import services.connector_groups as connector_groups_svc

from orchestrator.helpers import (
    get_conn, fail, broadcast,
    group_in_prog, mark_group_in_prog, unmark_group_in_prog,
)


def tick_groups() -> None:
    """Drive connector-group lifecycle phases."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT group_id, status, connector_name
                FROM   connector_groups
                WHERE  status IN ('TOPICS_CREATING', 'CONNECTOR_STARTING', 'STOPPING')
            """)
            groups = [{"group_id": r[0], "status": r[1], "connector_name": r[2]}
                      for r in cur.fetchall()]
    except Exception:
        return
    finally:
        conn.close()

    for g in groups:
        gid = g["group_id"]
        status = g["status"]
        try:
            if status == "TOPICS_CREATING":
                _handle_group_topics_creating(gid)
            elif status == "CONNECTOR_STARTING":
                _handle_group_connector_starting(gid)
            elif status == "STOPPING":
                _handle_group_stopping(gid)
        except Exception as exc:
            print(f"[orchestrator] group {gid} status {status} error: {exc}")
            connector_groups_svc.transition_group(gid, "FAILED", str(exc))
            broadcast({
                "type": "connector_group_status",
                "group_id": gid,
                "status": "FAILED",
            })


def _handle_group_topics_creating(group_id: str) -> None:
    """Create Kafka topics, then move to CONNECTOR_STARTING."""
    if group_in_prog(group_id):
        return
    mark_group_in_prog(group_id)

    def _run():
        try:
            results = connector_groups_svc.do_create_topics(group_id)
            errors = [r for r in results if r.get("status") == "error"]
            if errors:
                msg = "; ".join(f"{r['topic_name']}: {r.get('error','?')}" for r in errors)
                connector_groups_svc.transition_group(
                    group_id, "FAILED", f"Ошибка создания топиков: {msg}")
                broadcast({
                    "type": "connector_group_status",
                    "group_id": group_id, "status": "FAILED",
                })
                return

            ok_topics = [r["topic_name"] for r in results if r.get("status") == "ok"]
            connector_groups_svc.transition_group(
                group_id, "CONNECTOR_STARTING",
                f"Создано {len(ok_topics)} топиков")
            broadcast({
                "type": "connector_group_status",
                "group_id": group_id, "status": "CONNECTOR_STARTING",
            })
        except Exception as exc:
            connector_groups_svc.transition_group(
                group_id, "FAILED", str(exc))
            broadcast({
                "type": "connector_group_status",
                "group_id": group_id, "status": "FAILED",
            })
        finally:
            unmark_group_in_prog(group_id)

    threading.Thread(target=_run, daemon=True).start()


def _handle_group_connector_starting(group_id: str) -> None:
    """Create and start Debezium connector, then move to RUNNING."""
    if group_in_prog(group_id):
        return
    mark_group_in_prog(group_id)

    def _run():
        try:
            result = connector_groups_svc.do_start_connector(group_id)
            connector_groups_svc.transition_group(
                group_id, "RUNNING",
                f"Коннектор запущен: {result.get('name', '?')}")
            broadcast({
                "type": "connector_group_status",
                "group_id": group_id, "status": "RUNNING",
            })
        except Exception as exc:
            connector_groups_svc.transition_group(
                group_id, "FAILED", str(exc))
            broadcast({
                "type": "connector_group_status",
                "group_id": group_id, "status": "FAILED",
            })
        finally:
            unmark_group_in_prog(group_id)

    threading.Thread(target=_run, daemon=True).start()


def _handle_group_stopping(group_id: str) -> None:
    """Stop and delete Debezium connector, then move to STOPPED."""
    if group_in_prog(group_id):
        return
    mark_group_in_prog(group_id)

    def _run():
        try:
            connector_groups_svc.do_stop_connector(group_id)
            connector_groups_svc.transition_group(
                group_id, "STOPPED", "Коннектор остановлен")
            broadcast({
                "type": "connector_group_status",
                "group_id": group_id, "status": "STOPPED",
            })
        except Exception as exc:
            connector_groups_svc.transition_group(
                group_id, "FAILED", str(exc))
            broadcast({
                "type": "connector_group_status",
                "group_id": group_id, "status": "FAILED",
            })
        finally:
            unmark_group_in_prog(group_id)

    threading.Thread(target=_run, daemon=True).start()


# ---------------------------------------------------------------------------
# Group connector health check (runs every tick)
# ---------------------------------------------------------------------------

def check_group_connectors() -> None:
    """Poll all RUNNING connector groups.  If a group connector failed,
    fail all active CDC migrations in that group."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT group_id, connector_name, COALESCE(run_id, '') as run_id
                FROM   connector_groups
                WHERE  status = 'RUNNING'
            """)
            groups = cur.fetchall()
    except Exception:
        return  # table may not exist yet on first run
    finally:
        conn.close()

    for group_id, connector_name, run_id in groups:
        active_name = f"{connector_name}_{run_id}" if run_id else connector_name
        try:
            status = debezium.get_connector_status(active_name)
        except Exception as exc:
            print(f"[orchestrator] group {group_id} connector check error: {exc}")
            continue

        if status == "FAILED":
            print(f"[orchestrator] group connector {active_name} FAILED — failing group migrations")
            connector_groups_svc.update_group_status(group_id, "FAILED", "Connector FAILED")

            # Fail all active CDC migrations in this group
            conn = get_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT migration_id FROM migrations
                        WHERE  group_id = %s
                          AND  phase NOT IN ('DRAFT', 'COMPLETED', 'CANCELLED', 'FAILED')
                          AND  migration_mode != 'BULK_ONLY'
                    """, (group_id,))
                    for row in cur.fetchall():
                        fail(row[0],
                             f"Коннектор группы {connector_name} перешёл в FAILED",
                             "GROUP_CONNECTOR_FAILED")
            finally:
                conn.close()
