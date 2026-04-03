import threading

import services.debezium as debezium
import services.connector_groups as connector_groups_svc

from orchestrator.helpers import (
    get_conn, fail, transition, broadcast,
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
                WHERE  status IN ('PENDING', 'TOPICS_CREATING', 'CONNECTOR_STARTING', 'STOPPING')
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
            if status == "PENDING":
                _handle_group_pending(gid)
            elif status == "TOPICS_CREATING":
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


def _handle_group_pending(group_id: str) -> None:
    """PENDING → TOPICS_CREATING: start the group lifecycle."""
    print(f"[orchestrator] group {group_id}: PENDING → TOPICS_CREATING")
    connector_groups_svc.transition_group(
        group_id, "TOPICS_CREATING", "Начинаем создание топиков")
    broadcast({
        "type": "connector_group_status",
        "group_id": group_id,
        "status": "TOPICS_CREATING",
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
            # Auto-create migrations for all group_tables without active migration
            _auto_create_group_migrations(group_id)
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


def _auto_create_group_migrations(group_id: str) -> None:
    """Create migrations for all group_tables that don't have an active migration."""
    import uuid
    from datetime import datetime

    group = connector_groups_svc.get_group(group_id)
    if not group:
        return
    tables = connector_groups_svc.get_group_tables(group_id)
    if not tables:
        return

    connector_name = group["connector_name"]
    from services.connector_groups import _active_topic_prefix
    topic_prefix = _active_topic_prefix(group)
    prefix = group.get("consumer_group_prefix") or group["topic_prefix"]

    conn = get_conn()
    created = 0
    try:
        with conn.cursor() as cur:
            for tbl in tables:
                src_schema = tbl["source_schema"].upper()
                src_table = tbl["source_table"].upper()
                tgt_schema = tbl["target_schema"].upper()
                tgt_table = tbl["target_table"].upper()

                # Skip if active migration already exists
                cur.execute("""
                    SELECT 1 FROM migrations
                    WHERE  group_id = %s
                      AND  source_schema = %s AND source_table = %s
                      AND  phase NOT IN ('CANCELLED', 'FAILED', 'COMPLETED')
                    LIMIT 1
                """, (group_id, src_schema, src_table))
                if cur.fetchone():
                    continue

                ekt = tbl.get("effective_key_type", "NONE")
                ekc_json = tbl.get("effective_key_columns_json", "[]")
                pk_exists = tbl.get("source_pk_exists", False)
                uk_exists = tbl.get("source_uk_exists", False)
                key_source_map = {
                    "PRIMARY_KEY": "PK", "UNIQUE_KEY": "UK",
                    "USER_DEFINED": "USER", "NONE": "NONE",
                }
                consumer_group = f"{prefix}_{src_schema}_{src_table}"
                stage_name = f"STG_{src_schema}_{src_table}"
                migration_name = f"{src_schema}.{src_table}"
                mid = str(uuid.uuid4())
                now = datetime.utcnow()

                cur.execute("""
                    INSERT INTO migrations (
                        migration_id, migration_name, phase, state_changed_at,
                        source_connection_id, target_connection_id,
                        source_schema, source_table, target_schema, target_table,
                        stage_table_name, stage_tablespace,
                        connector_name, topic_prefix, consumer_group,
                        chunk_size, max_parallel_workers, baseline_parallel_degree,
                        source_pk_exists, source_uk_exists,
                        effective_key_type, effective_key_source, effective_key_columns_json,
                        migration_strategy, migration_mode,
                        group_id, created_at, updated_at
                    ) VALUES (
                        %s, %s, 'NEW', %s,
                        'oracle_source', 'oracle_target',
                        %s, %s, %s, %s,
                        %s, 'PAYSTAGE',
                        %s, %s, %s,
                        500000, 10, 4,
                        %s, %s,
                        %s, %s, %s,
                        'STAGE', 'CDC',
                        %s, %s, %s
                    )
                """, (
                    mid, migration_name, now,
                    src_schema, src_table, tgt_schema, tgt_table,
                    stage_name,
                    connector_name, topic_prefix, consumer_group,
                    pk_exists, uk_exists,
                    ekt, key_source_map.get(ekt, "NONE"), ekc_json,
                    group_id, now, now,
                ))
                cur.execute("""
                    INSERT INTO migration_state_history
                        (migration_id, from_phase, to_phase, message, actor_type)
                    VALUES (%s, NULL, 'NEW', 'Auto-created from connector group', 'SYSTEM')
                """, (mid,))
                created += 1

        conn.commit()
        if created:
            print(f"[orchestrator] group {group_id}: auto-created {created} migrations")
    except Exception as exc:
        conn.rollback()
        print(f"[orchestrator] group {group_id}: auto-create migrations error: {exc}")
    finally:
        conn.close()


def _cancel_group_migrations(group_id: str) -> int:
    """Cancel all active migrations belonging to this group.
    Returns the number of migrations cancelled."""
    _TERMINAL = ("DRAFT", "COMPLETED", "CANCELLED", "FAILED", "CANCELLING")
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT migration_id FROM migrations
                WHERE  group_id = %s
                  AND  phase NOT IN %s
            """, (group_id, _TERMINAL))
            mids = [row[0] for row in cur.fetchall()]
    finally:
        conn.close()

    for mid in mids:
        try:
            transition(mid, "CANCELLING",
                       message=f"Остановка группы коннекторов")
        except Exception as exc:
            print(f"[orchestrator] cancel migration {mid} on group stop failed: {exc}")
    return len(mids)


def _handle_group_stopping(group_id: str) -> None:
    """Stop and delete Debezium connector, cancel migrations, then move to STOPPED."""
    if group_in_prog(group_id):
        return
    mark_group_in_prog(group_id)

    def _run():
        try:
            cancelled = _cancel_group_migrations(group_id)
            if cancelled:
                print(f"[orchestrator] group {group_id}: cancelled {cancelled} migration(s)")

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
