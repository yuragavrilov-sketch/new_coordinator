"""Connector Groups — CRUD + lifecycle API."""

import json
import uuid
from datetime import datetime

from flask import Blueprint, jsonify, request

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
    from services.connector_groups import get_group as svc_get, get_group_migrations
    group = svc_get(group_id)
    if not group:
        return jsonify({"error": "Группа не найдена"}), 404
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
    """Create a connector group AND migrations for selected tables atomically."""
    body = request.get_json(force=True)

    # ── validate group fields ──────────────────────────────────────────────
    required = ("group_name", "connector_name", "topic_prefix")
    missing = [f for f in required if not body.get(f)]
    if missing:
        return jsonify({"error": f"Отсутствуют поля: {', '.join(missing)}"}), 400

    tables = body.get("tables")
    if not tables or not isinstance(tables, list) or len(tables) == 0:
        return jsonify({"error": "Нужно выбрать хотя бы одну таблицу"}), 400

    # ── common defaults ────────────────────────────────────────────────────
    strategy = (body.get("migration_strategy") or "STAGE").strip().upper()
    if strategy not in ("STAGE", "DIRECT"):
        strategy = "STAGE"
    chunk_size = body.get("chunk_size", 1_000_000)
    max_workers = max(1, int(body.get("max_parallel_workers", 1) or 1))
    baseline_deg = max(1, int(body.get("baseline_parallel_degree", 4) or 4))
    validate_hash = body.get("validate_hash_sample", False)
    stage_tablespace = (body.get("stage_tablespace") or "").strip().upper()
    source_conn_id = body.get("source_connection_id", "oracle_source")

    gid = str(uuid.uuid4())
    topic_prefix = body["topic_prefix"]
    consumer_prefix = body.get("consumer_group_prefix") or topic_prefix
    now = datetime.utcnow()

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

            # ── create migrations for each table ───────────────────────────
            migration_ids = []
            for t in tables:
                mid = str(uuid.uuid4())
                src_schema = t.get("source_schema", "")
                src_table = t.get("source_table", "")
                tgt_schema = t.get("target_schema", src_schema)
                tgt_table = t.get("target_table", src_table)

                t_strategy = (t.get("migration_strategy") or strategy).strip().upper()
                if t_strategy not in ("STAGE", "DIRECT"):
                    t_strategy = strategy
                t_stage_name = t.get("stage_table_name") or f"STG_{src_schema}_{src_table}".upper()

                connector_name = body["connector_name"]
                cg = f"{consumer_prefix}_{src_schema}_{src_table}".upper()

                key_source_map = {
                    "PRIMARY_KEY": "PK", "UNIQUE_KEY": "UK",
                    "USER_DEFINED": "USER", "NONE": "NONE",
                }
                ekt = t.get("effective_key_type", "")
                eks = key_source_map.get(ekt, "NONE")
                ekc = json.dumps(t.get("effective_key_columns", []))

                mig_name = t.get("migration_name") or f"{src_schema}.{src_table} → {tgt_schema}.{tgt_table}"

                cur.execute("""
                    INSERT INTO migrations (
                        migration_id, migration_name, phase, state_changed_at,
                        source_connection_id, target_connection_id,
                        source_schema, source_table, target_schema, target_table,
                        stage_table_name, stage_tablespace,
                        connector_name, topic_prefix, consumer_group,
                        chunk_size, max_parallel_workers, baseline_parallel_degree,
                        validate_hash_sample,
                        source_pk_exists, source_uk_exists,
                        effective_key_type, effective_key_source, effective_key_columns_json,
                        migration_strategy, migration_mode,
                        group_id,
                        created_at, updated_at
                    ) VALUES (
                        %s, %s, 'DRAFT', %s,
                        %s, 'oracle_target',
                        %s, %s, %s, %s,
                        %s, %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s,
                        %s, %s,
                        %s, %s, %s,
                        %s, 'CDC',
                        %s,
                        %s, %s
                    )
                """, (
                    mid, mig_name, now,
                    source_conn_id,
                    src_schema, src_table, tgt_schema, tgt_table,
                    t_stage_name if t_strategy == "STAGE" else "",
                    stage_tablespace if t_strategy == "STAGE" else "",
                    connector_name, topic_prefix, cg,
                    chunk_size, max_workers, baseline_deg,
                    validate_hash,
                    t.get("source_pk_exists", False),
                    t.get("source_uk_exists", False),
                    ekt, eks, ekc,
                    t_strategy,
                    gid,
                    now, now,
                ))
                cur.execute("""
                    INSERT INTO migration_state_history
                        (migration_id, from_phase, to_phase, message, actor_type)
                    VALUES (%s, NULL, 'DRAFT', %s, 'USER')
                """, (mid, f"Created via group wizard: {body['group_name']}"))
                migration_ids.append({"migration_id": mid, "source_table": f"{src_schema}.{src_table}"})

        conn.commit()
    except Exception as exc:
        conn.rollback()
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()

    # broadcast events
    _state["broadcast"]({
        "type": "connector_group_status",
        "group_id": gid,
        "status": "PENDING",
    })
    for m in migration_ids:
        _state["broadcast"]({
            "type": "migration_phase",
            "migration_id": m["migration_id"],
            "phase": "DRAFT",
            "ts": now.isoformat() + "Z",
        })

    return jsonify({"group": group_row, "migrations": migration_ids}), 201


@bp.delete("/api/connector-groups/<group_id>")
def delete_group(group_id: str):
    from services.connector_groups import delete_group as svc_delete
    try:
        svc_delete(group_id)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    return "", 204


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


# ── Lifecycle ─────────────────────────────────────────────────────────────────

@bp.post("/api/connector-groups/<group_id>/start")
def start_group(group_id: str):
    from services.connector_groups import start_connector
    try:
        result = start_connector(group_id)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500

    _state["broadcast"]({
        "type": "connector_group_status",
        "group_id": group_id,
        "status": "RUNNING",
    })
    return jsonify(result)


@bp.post("/api/connector-groups/<group_id>/stop")
def stop_group(group_id: str):
    from services.connector_groups import stop_connector
    try:
        stop_connector(group_id)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500

    _state["broadcast"]({
        "type": "connector_group_status",
        "group_id": group_id,
        "status": "STOPPED",
    })
    return "", 204


@bp.get("/api/connector-groups/<group_id>/status")
def group_status(group_id: str):
    from services.connector_groups import get_connector_status, get_group as svc_get
    status = get_connector_status(group_id)
    group = svc_get(group_id)
    return jsonify({"status": status, "group": group})


@bp.post("/api/connector-groups/<group_id>/refresh-tables")
def refresh_tables(group_id: str):
    """Re-sync table.include.list and message.key.columns from current migrations."""
    from services.connector_groups import refresh_connector_tables
    try:
        refresh_connector_tables(group_id)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    return jsonify({"ok": True})
