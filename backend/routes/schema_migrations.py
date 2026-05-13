"""Schema-migration dashboard API.

GET    /api/schema-migrations              → list (each with computed KPIs)
GET    /api/schema-migrations/:id          → detail (header + KPIs)
GET    /api/schema-migrations/:id/objects  → object table rows (SchemaObject[])
GET    /api/schema-migrations/:id/events   → recent events
GET    /api/schema-migrations/:id/metrics  → live metrics (placeholder)
POST   /api/schema-migrations              → create (Wizard submit)
POST   /api/schema-migrations/:id/pause    → pause whole schema migration
POST   /api/schema-migrations/:id/resume   → resume
POST   /api/schema-migrations/:id/rollback → cancel (mark child migrations)
"""

from flask import Blueprint, jsonify, request

import services.schema_migrations as svc
import services.ddl_apply_jobs as ddl_jobs

bp = Blueprint("schema_migrations", __name__)

_state: dict = {}


def init(*, get_conn_fn, db_available_ref, broadcast_fn):
    _state["get_conn"]     = get_conn_fn
    _state["db_available"] = db_available_ref
    _state["broadcast"]    = broadcast_fn


def _db_ok() -> bool:
    return _state["db_available"]["value"]


@bp.get("/api/schema-migrations")
def list_schema_migrations():
    if not _db_ok():
        return jsonify([])
    conn = _state["get_conn"]()
    try:
        return jsonify(svc.list_schema_migrations(conn))
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()


@bp.get("/api/schema-migrations/<sm_id>")
def get_schema_migration(sm_id: str):
    if not _db_ok():
        return jsonify({"error": "DB unavailable"}), 503
    conn = _state["get_conn"]()
    try:
        sm = svc.get_schema_migration(conn, sm_id)
        if not sm:
            return jsonify({"error": "Not found"}), 404
        return jsonify(sm)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()


@bp.get("/api/schema-migrations/<sm_id>/objects")
def get_objects(sm_id: str):
    if not _db_ok():
        return jsonify([])
    conn = _state["get_conn"]()
    try:
        return jsonify(svc.get_objects(conn, sm_id))
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()


@bp.get("/api/schema-migrations/<sm_id>/objects/<path:obj_id>/detail")
def get_object_detail(sm_id: str, obj_id: str):
    if not _db_ok():
        return jsonify({"error": "DB unavailable"}), 503
    conn = _state["get_conn"]()
    try:
        detail = svc.get_object_detail(conn, sm_id, obj_id)
        if detail is None:
            return jsonify({"error": "Not found"}), 404
        return jsonify(detail)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()


@bp.get("/api/schema-migrations/<sm_id>/events")
def get_events(sm_id: str):
    if not _db_ok():
        return jsonify([])
    limit = int(request.args.get("limit", 100))
    conn = _state["get_conn"]()
    try:
        return jsonify(svc.get_events(conn, sm_id, limit=limit))
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()


@bp.get("/api/schema-migrations/<sm_id>/metrics")
def get_metrics(sm_id: str):
    if not _db_ok():
        return jsonify({})
    conn = _state["get_conn"]()
    try:
        return jsonify(svc.get_metrics(conn, sm_id))
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()


@bp.post("/api/schema-migrations")
def create_schema_migration():
    if not _db_ok():
        return jsonify({"error": "DB unavailable"}), 503
    payload = request.get_json(silent=True) or {}
    if not payload.get("src_schema") or not payload.get("tgt_schema"):
        return jsonify({"error": "src_schema and tgt_schema required"}), 400
    conn = _state["get_conn"]()
    try:
        new_id = svc.create_schema_migration(conn, payload)
        _state["broadcast"]({"type": "schema_migration.created", "id": new_id})
        return jsonify({"schema_migration_id": new_id}), 201
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()


@bp.post("/api/schema-migrations/<sm_id>/pause")
def pause(sm_id: str):
    return _set_paused(sm_id, True)


@bp.post("/api/schema-migrations/<sm_id>/resume")
def resume(sm_id: str):
    return _set_paused(sm_id, False)


def _set_paused(sm_id: str, paused: bool):
    if not _db_ok():
        return jsonify({"error": "DB unavailable"}), 503
    conn = _state["get_conn"]()
    try:
        ok = svc.set_paused(conn, sm_id, paused)
        if not ok:
            return jsonify({"error": "Not found"}), 404
        _state["broadcast"]({
            "type": "schema_migration.paused" if paused else "schema_migration.resumed",
            "id": sm_id,
        })
        return jsonify({"ok": True, "paused": paused})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()


@bp.post("/api/schema-migrations/<sm_id>/ddl-apply")
def ddl_apply(sm_id: str):
    """Queue DDL apply jobs.

    Body: {"action": "create_missing"|"sync_diff"|"recreate",
           "objects": [{"type": "TABLE"|"MVIEW"|..., "name": "FOO"}]}
    """
    if not _db_ok():
        return jsonify({"error": "DB unavailable"}), 503
    payload = request.get_json(silent=True) or {}
    action  = payload.get("action") or ""
    objects = payload.get("objects") or []
    if not action:
        return jsonify({"error": "action required"}), 400
    if not isinstance(objects, list) or not objects:
        return jsonify({"error": "objects required"}), 400
    conn = _state["get_conn"]()
    try:
        result = ddl_jobs.submit_jobs(conn, sm_id, action, objects)
        _state["broadcast"]({
            "type":   "ddl_apply.queued",
            "sm_id":  sm_id,
            "action": action,
            "queued": result["queued"],
        })
        return jsonify(result), 202
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()


@bp.get("/api/schema-migrations/<sm_id>/ddl-jobs")
def list_ddl_jobs(sm_id: str):
    if not _db_ok():
        return jsonify([])
    limit = int(request.args.get("limit", 100))
    conn = _state["get_conn"]()
    try:
        return jsonify(ddl_jobs.list_jobs(conn, sm_id, limit=limit))
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()


@bp.post("/api/schema-migrations/<sm_id>/rollback")
def rollback(sm_id: str):
    """Mark all child migrations as CANCELLING. Best-effort: orchestrator
    drives them to CANCELLED."""
    if not _db_ok():
        return jsonify({"error": "DB unavailable"}), 503
    conn = _state["get_conn"]()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE migrations m
                SET phase = 'CANCELLING', state_changed_at = NOW(), updated_at = NOW()
                FROM migration_plan_items mpi, schema_migrations sm
                WHERE m.migration_id = mpi.migration_id
                  AND mpi.plan_id = sm.plan_id
                  AND sm.schema_migration_id = %s
                  AND m.phase NOT IN ('COMPLETED', 'CANCELLED', 'FAILED')
            """, (sm_id,))
            affected = cur.rowcount
            conn.commit()
        _state["broadcast"]({"type": "schema_migration.rollback", "id": sm_id, "affected": affected})
        return jsonify({"ok": True, "affected": affected})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()
