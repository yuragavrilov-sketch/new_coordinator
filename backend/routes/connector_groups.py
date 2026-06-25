"""Connector Groups — CRUD + lifecycle + tables API."""

import json
import uuid
from datetime import datetime

from flask import Blueprint, jsonify, request
from services.strategy import Strategy

bp = Blueprint("connector_groups", __name__)

_state: dict = {}


def init(*, get_conn_fn, row_to_dict_fn, load_configs_fn, broadcast_fn):
    _state["get_conn"] = get_conn_fn
    _state["row_to_dict"] = row_to_dict_fn
    _state["load_configs"] = load_configs_fn
    _state["broadcast"] = broadcast_fn

    # Initialise the service layer
    from services.connector_groups import init as svc_init
    svc_init(
        get_conn_fn=get_conn_fn,
        row_to_dict_fn=row_to_dict_fn,
        load_configs_fn=load_configs_fn,
    )


def _r2d(cur, row):
    return _state["row_to_dict"](cur, row)


# ── CRUD ──────────────────────────────────────────────────────────────────────

@bp.get("/api/connector-groups")
def list_groups():
    from services.connector_groups import list_groups as svc_list
    groups = svc_list()
    return jsonify(groups)


@bp.get("/api/connector-groups/<group_id>")
def get_group(group_id: str):
    from services.connector_groups import (
        get_group as svc_get,
        get_group_tables,
        get_group_migrations,
    )
    group = svc_get(group_id)
    if not group:
        return jsonify({"error": "Группа не найдена"}), 404
    group["tables"] = get_group_tables(group_id)
    group["migrations"] = get_group_migrations(group_id)
    return jsonify(group)


@bp.post("/api/connector-groups")
def create_group():
    body = request.get_json(force=True)
    required = ("group_name", "connector_name", "topic_prefix")
    missing = [f for f in required if not body.get(f)]
    if missing:
        return jsonify({"error": f"Отсутствуют поля: {', '.join(missing)}"}), 400

    from services.connector_groups import create_group as svc_create
    try:
        group = svc_create(
            group_name=body["group_name"],
            source_connection_id=body.get("source_connection_id", "oracle_source"),
            connector_name=body["connector_name"],
            topic_prefix=body["topic_prefix"],
            consumer_group_prefix=body.get("consumer_group_prefix", body["topic_prefix"]),
        )
    except Exception as exc:
        return jsonify({"error": str(exc)}), 400
    return jsonify(group), 201


@bp.post("/api/connector-groups/wizard")
def create_group_wizard():
    """Create a connector group AND add tables atomically."""
    body = request.get_json(force=True)

    # ── validate group fields ──────────────────────────────────────────────
    required = ("group_name", "connector_name", "topic_prefix")
    missing = [f for f in required if not body.get(f)]
    if missing:
        return jsonify({"error": f"Отсутствуют поля: {', '.join(missing)}"}), 400

    tables = body.get("tables")
    if not tables or not isinstance(tables, list) or len(tables) == 0:
        return jsonify({"error": "Нужно выбрать хотя бы одну таблицу"}), 400

    source_conn_id = body.get("source_connection_id", "oracle_source")
    topic_prefix = body["topic_prefix"]
    consumer_prefix = body.get("consumer_group_prefix") or topic_prefix

    gid = str(uuid.uuid4())
    get_conn = _state["get_conn"]
    r2d = _state["row_to_dict"]

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # ── create group ───────────────────────────────────────────────
            cur.execute("""
                INSERT INTO connector_groups
                    (group_id, group_name, source_connection_id,
                     connector_name, topic_prefix, consumer_group_prefix)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING *
            """, (gid, body["group_name"], source_conn_id,
                  body["connector_name"], topic_prefix, consumer_prefix))
            group_row = r2d(cur, cur.fetchone())

            # ── add tables to group_tables ─────────────────────────────────
            table_rows = []
            for t in tables:
                tid = str(uuid.uuid4())
                src_schema = t.get("source_schema", "")
                src_table = t.get("source_table", "")
                tgt_schema = t.get("target_schema", src_schema)
                tgt_table = t.get("target_table", src_table)
                ekt = t.get("effective_key_type", "NONE")
                ekc = json.dumps(t.get("effective_key_columns", []))
                pk = t.get("source_pk_exists", False)
                uk = t.get("source_uk_exists", False)
                topic = f"{topic_prefix}.{src_schema.upper()}.{src_table.upper()}".replace("#", "_")

                cur.execute("""
                    INSERT INTO group_tables
                        (id, group_id, source_schema, source_table,
                         target_schema, target_table,
                         effective_key_type, effective_key_columns_json,
                         source_pk_exists, source_uk_exists, topic_name)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING *
                """, (tid, gid, src_schema, src_table,
                      tgt_schema, tgt_table,
                      ekt, ekc, pk, uk, topic))
                table_rows.append(r2d(cur, cur.fetchone()))

        conn.commit()
    except Exception as exc:
        conn.rollback()
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()

    _state["broadcast"]({
        "type": "connector_group_status",
        "group_id": gid,
        "status": "PENDING",
    })

    # ── optionally create migrations (NEW) so they queue up for orchestrator ──
    migrations_created: list[dict] = []
    if body.get("create_migrations"):
        try:
            strategy = Strategy.parse(body.get("strategy") or "CDC_STAGE")
        except ValueError as exc:
            return jsonify({
                "group": group_row, "tables": table_rows,
                "migrations": [],
                "migrations_error": f"Invalid strategy: {exc}",
            }), 207
        try:
            from services.connector_groups import create_migrations_for_group_tables
            migrations_created = create_migrations_for_group_tables(
                gid, table_ids=None,
                strategy=strategy,
                stage_tablespace=body.get("stage_tablespace") or "PAYSTAGE",
                truncate_target=bool(body.get("truncate_target", True)),
                chunk_size=int(body.get("chunk_size") or 1_000_000),
                max_parallel_workers=int(body.get("max_parallel_workers") or 1),
                baseline_parallel_degree=int(body.get("baseline_parallel_degree") or 4),
                baseline_batch_size=int(body.get("baseline_batch_size") or 500_000),
                validate_hash_sample=bool(body.get("validate_hash_sample", False)),
            )
        except Exception as exc:
            return jsonify({
                "group": group_row, "tables": table_rows,
                "migrations": [],
                "migrations_error": str(exc),
            }), 207

    return jsonify({
        "group":      group_row,
        "tables":     table_rows,
        "migrations": migrations_created,
    }), 201


@bp.delete("/api/connector-groups/<group_id>")
def delete_group(group_id: str):
    from services.connector_groups import delete_group as svc_delete
    force = request.args.get("force", "").lower() in ("1", "true", "yes")
    try:
        svc_delete(group_id, force=force)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    return "", 204


# ── Group tables ──────────────────────────────────────────────────────────────

@bp.get("/api/connector-groups/<group_id>/tables")
def list_group_tables(group_id: str):
    from services.connector_groups import get_group_tables
    return jsonify(get_group_tables(group_id))


@bp.post("/api/connector-groups/<group_id>/tables")
def add_group_tables(group_id: str):
    body = request.get_json(force=True)
    tables = body.get("tables", [])
    if not tables:
        return jsonify({"error": "Нужно указать хотя бы одну таблицу"}), 400
    from services.connector_groups import (
        add_tables, refresh_connector_tables,
        create_migrations_for_group_tables,
    )
    try:
        rows = add_tables(group_id, tables)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    # If the connector is already running, push table.include.list / key cols update
    try:
        refresh_connector_tables(group_id)
    except Exception as exc:
        return jsonify({"error": f"CDC connector config sync failed: {exc}"}), 409

    # ── optionally create migrations for the new tables ──────────────────
    migrations_created: list[dict] = []
    migrations_error: str | None = None
    if body.get("create_migrations"):
        try:
            strategy = Strategy.parse(body.get("strategy") or "CDC_STAGE")
            new_ids = [r["id"] for r in rows] if rows else None
            migrations_created = create_migrations_for_group_tables(
                group_id, table_ids=new_ids,
                strategy=strategy,
                stage_tablespace=body.get("stage_tablespace") or "PAYSTAGE",
                truncate_target=bool(body.get("truncate_target", True)),
                chunk_size=int(body.get("chunk_size") or 1_000_000),
                max_parallel_workers=int(body.get("max_parallel_workers") or 1),
                baseline_parallel_degree=int(body.get("baseline_parallel_degree") or 4),
                baseline_batch_size=int(body.get("baseline_batch_size") or 500_000),
                validate_hash_sample=bool(body.get("validate_hash_sample", False)),
            )
        except Exception as exc:
            migrations_error = str(exc)

    return jsonify({
        "tables":           rows,
        "migrations":       migrations_created,
        "migrations_error": migrations_error,
    }), 201


@bp.delete("/api/connector-groups/<group_id>/tables/<source_schema>/<source_table>")
def remove_group_table(group_id: str, source_schema: str, source_table: str):
    from services.connector_groups import remove_table, refresh_connector_tables
    remove_table(group_id, source_schema, source_table)
    try:
        refresh_connector_tables(group_id)
    except Exception as exc:
        return jsonify({"error": f"CDC connector config sync failed: {exc}"}), 409
    return "", 204


# ── Debezium config preview ──────────────────────────────────────────────────

@bp.get("/api/connector-groups/<group_id>/debezium-config")
def debezium_config(group_id: str):
    """Return the Debezium connector config that would be sent to Kafka Connect."""
    from services.connector_groups import build_connector_config
    try:
        cfg = build_connector_config(group_id)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 404
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    return jsonify(cfg)


# ── CDC readiness check ──────────────────────────────────────────────────────

@bp.post("/api/connector-groups/check-readiness")
def check_readiness():
    """Check ARCHIVELOG / supplemental logging readiness for a list of tables.

    Body: {"source_connection_id": "oracle_source",
           "tables": [{"source_schema": "X", "source_table": "Y"}, ...]}
    """
    body = request.get_json(force=True) or {}
    src_conn_id = body.get("source_connection_id", "oracle_source")
    tables = body.get("tables", [])
    if not tables:
        return jsonify({"error": "tables is required"}), 400

    from services.connector_groups import check_cdc_readiness
    try:
        result = check_cdc_readiness(src_conn_id, tables)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    return jsonify(result)


# ── Kafka topics ──────────────────────────────────────────────────────────────

@bp.post("/api/connector-groups/<group_id>/create-topics")
def create_topics(group_id: str):
    """Pre-create Kafka topics for all tables in the group."""
    from services.connector_groups import create_group_topics
    try:
        results = create_group_topics(group_id)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    return jsonify(results)


@bp.get("/api/connector-groups/<group_id>/topic-counts")
def topic_counts(group_id: str):
    """Return message counts for each topic in the group."""
    from services.connector_groups import get_topic_message_counts
    try:
        counts = get_topic_message_counts(group_id)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    return jsonify(counts)


# ── Lifecycle ─────────────────────────────────────────────────────────────────

@bp.post("/api/connector-groups/<group_id>/start")
def start_group(group_id: str):
    from services.connector_groups import request_start
    try:
        result = request_start(group_id)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500

    _state["broadcast"]({
        "type": "connector_group_status",
        "group_id": group_id,
        "status": result["status"],
    })
    return jsonify(result)


@bp.post("/api/connector-groups/<group_id>/stop")
def stop_group(group_id: str):
    from services.connector_groups import request_stop
    try:
        request_stop(group_id)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500

    _state["broadcast"]({
        "type": "connector_group_status",
        "group_id": group_id,
        "status": "STOPPING",
    })
    return jsonify({"status": "STOPPING"})


@bp.get("/api/connector-groups/<group_id>/status")
def group_status(group_id: str):
    from services.connector_groups import get_connector_status, get_group as svc_get
    status = get_connector_status(group_id)
    group = svc_get(group_id)
    return jsonify({"status": status, "group": group})


@bp.get("/api/connector-groups/<group_id>/history")
def group_history(group_id: str):
    from services.connector_groups import get_group_history
    return jsonify(get_group_history(group_id))


@bp.post("/api/connector-groups/<group_id>/refresh-tables")
def refresh_tables(group_id: str):
    """Re-sync table.include.list and message.key.columns from group_tables."""
    from services.connector_groups import refresh_connector_tables
    try:
        refresh_connector_tables(group_id)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    return jsonify({"ok": True})


# ── Create migration from group table ─────────────────────────────────────────

@bp.post("/api/connector-groups/<group_id>/create-migration")
def create_migration_from_table(group_id: str):
    """Create a migration for a specific table in the group and start it."""
    from services.connector_groups import (
        get_group as svc_get,
        get_group_tables,
        refresh_connector_tables,
    )

    body = request.get_json(force=True) or {}
    table_id = body.get("table_id", "").strip()
    if not table_id:
        return jsonify({"error": "table_id is required"}), 400

    # ── look up group ─────────────────────────────────────────────────────
    group = svc_get(group_id)
    if not group:
        return jsonify({"error": "Группа не найдена"}), 404

    # ── find the table in group_tables ────────────────────────────────────
    tables = get_group_tables(group_id)
    tbl = next((t for t in tables if t["id"] == table_id), None)
    if not tbl:
        return jsonify({"error": "Таблица не найдена в группе"}), 404

    src_schema = tbl["source_schema"].upper()
    src_table = tbl["source_table"].upper()
    tgt_schema = tbl["target_schema"].upper()
    tgt_table = tbl["target_table"].upper()

    # ── check for active duplicate ────────────────────────────────────────
    get_conn = _state["get_conn"]
    r2d = _state["row_to_dict"]
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT migration_id, migration_name, phase
                FROM   migrations
                WHERE  group_id = %s
                  AND  source_schema = %s AND source_table = %s
                  AND  phase NOT IN ('CANCELLED', 'FAILED', 'COMPLETED')
                LIMIT 1
            """, (group_id, src_schema, src_table))
            dup = cur.fetchone()
            if dup:
                d = r2d(cur, dup)
                return jsonify({
                    "error": f"Уже есть активная миграция \"{d['migration_name']}\" "
                             f"({d['phase']}) для {src_schema}.{src_table}"
                }), 409
    finally:
        conn.close()

    # ── derive fields ─────────────────────────────────────────────────────
    ekt = tbl.get("effective_key_type", "NONE")
    ekc_json = tbl.get("effective_key_columns_json", "[]")
    pk_exists = tbl.get("source_pk_exists", False)
    uk_exists = tbl.get("source_uk_exists", False)
    key_source_map = {
        "PRIMARY_KEY": "PK", "UNIQUE_KEY": "UK",
        "USER_DEFINED": "USER", "NONE": "NONE",
    }

    from services.connector_groups import _active_topic_prefix
    connector_name = group["connector_name"]
    topic_prefix = _active_topic_prefix(group)
    prefix = group.get("consumer_group_prefix") or group["topic_prefix"]
    consumer_group = f"{prefix}_{src_schema}_{src_table}"

    try:
        strategy = Strategy.parse(body.get("strategy"))
    except ValueError as exc:
        return jsonify({"error": f"Invalid strategy: {exc}"}), 400

    truncate_target = bool(body.get("truncate_target", True))
    if strategy.uses_stage and truncate_target is False:
        return jsonify({
            "error": "STAGE-стратегия требует TRUNCATE target (поведение неизменяемо). "
                     "Используйте DIRECT, если нужно сохранить существующие данные."
        }), 400

    if strategy.has_cdc:
        try:
            refresh_connector_tables(group_id)
        except Exception as exc:
            return jsonify({"error": f"CDC connector config sync failed: {exc}"}), 409

    stage_name       = f"STG_{src_schema}_{src_table}" if strategy.uses_stage else ""
    stage_tablespace = body.get("stage_tablespace", "PAYSTAGE") if strategy.uses_stage else ""

    migration_name = (body.get("migration_name", "").strip()
                      or f"{src_schema}.{src_table} → {tgt_schema}.{tgt_table}")

    mid = str(uuid.uuid4())
    now = datetime.utcnow()
    initial_phase = "NEW"

    # ── insert migration ──────────────────────────────────────────────────
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO migrations (
                    migration_id, migration_name, phase, state_changed_at,
                    source_connection_id, target_connection_id,
                    source_schema, source_table, target_schema, target_table,
                    stage_table_name, stage_tablespace,
                    connector_name, topic_prefix, consumer_group,
                    chunk_size, max_parallel_workers, baseline_parallel_degree,
                    baseline_batch_size,
                    validate_hash_sample,
                    source_pk_exists, source_uk_exists,
                    effective_key_type, effective_key_source, effective_key_columns_json,
                    strategy,
                    truncate_target,
                    group_id,
                    created_at, updated_at
                ) VALUES (
                    %s, %s, %s, %s,
                    %s, %s,
                    %s, %s, %s, %s,
                    %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s,
                    %s,
                    %s, %s,
                    %s, %s, %s,
                    %s,
                    %s,
                    %s,
                    %s, %s
                )
            """, (
                mid, migration_name, initial_phase, now,
                "oracle_source", "oracle_target",
                src_schema, src_table, tgt_schema, tgt_table,
                stage_name, stage_tablespace.strip().upper(),
                connector_name, topic_prefix, consumer_group,
                body.get("chunk_size", 1_000_000),
                max(1, int(body.get("max_parallel_workers", 1) or 1)),
                max(1, int(body.get("baseline_parallel_degree", 4) or 4)),
                max(1000, int(body.get("baseline_batch_size", 500_000) or 500_000)),
                body.get("validate_hash_sample", False),
                pk_exists, uk_exists,
                ekt, key_source_map.get(ekt, "NONE"), ekc_json,
                strategy.value,
                truncate_target,
                group_id,
                now, now,
            ))
            cur.execute("""
                INSERT INTO migration_state_history
                    (migration_id, from_phase, to_phase, message, actor_type)
                VALUES (%s, NULL, %s, %s, 'USER')
            """, (mid, initial_phase, "Migration created from connector group"))
        conn.commit()
    except Exception as exc:
        conn.rollback()
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()

    _state["broadcast"]({
        "type":         "migration_phase",
        "migration_id": mid,
        "phase":        initial_phase,
        "ts":           now.isoformat() + "Z",
    })

    return jsonify({
        "migration_id": mid,
        "migration_name": migration_name,
        "phase": initial_phase,
    }), 201
