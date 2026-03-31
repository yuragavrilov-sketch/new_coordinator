"""Orchestrator engine — background thread driving migration state transitions."""

import threading
import time

import services.job_queue as job_queue
from db.state_db import get_active_migrations

from orchestrator.helpers import get_conn, fail
from orchestrator.groups import tick_groups, check_group_connectors

from orchestrator.phases import preparing, chunking, baseline, cleanup, cdc, data_verify
from orchestrator.phases import group_new
from orchestrator.phases import bulk_pipeline

TICK_INTERVAL = 5

_LEGACY_HANDLERS = {
    "NEW":                  lambda mid, m: preparing.handle_new(mid, m),
    "PREPARING":            lambda mid, m: preparing.handle_preparing(mid, m),
    "SCN_FIXED":            lambda mid, m: preparing.handle_scn_fixed(mid, m),
    "CONNECTOR_STARTING":   lambda mid, m: preparing.handle_connector_starting(mid, m),
    "CDC_BUFFERING":        lambda mid, m: preparing.handle_cdc_buffering(mid, m),
    "STRUCTURE_READY":      lambda mid, m: bulk_pipeline.handle_structure_ready(mid, m),
    "DATA_COMPARING":       lambda mid, m: bulk_pipeline.handle_data_comparing(mid, m),
    "TARGET_CLEARING":      lambda mid, m: bulk_pipeline.handle_target_clearing(mid, m),
    "CHUNKING":             lambda mid, m: chunking.handle_chunking(mid, m),
    "BULK_LOADING":         lambda mid, m: chunking.handle_bulk_loading(mid, m),
    "BULK_LOADED":          lambda mid, m: chunking.handle_bulk_loaded(mid, m),
    "STAGE_VALIDATING":     lambda mid, m: baseline.handle_stage_validating(mid, m),
    "STAGE_VALIDATED":      lambda mid, m: baseline.handle_stage_validated(mid, m),
    "BASELINE_PUBLISHING":  lambda mid, m: baseline.handle_baseline_publishing(mid, m),
    "BASELINE_LOADING":     lambda mid, m: baseline.handle_baseline_loading(mid, m),
    "BASELINE_PUBLISHED":   lambda mid, m: baseline.handle_baseline_published(mid, m),
    "STAGE_DROPPING":       lambda mid, m: baseline.handle_stage_dropping(mid, m),
    "INDEXES_ENABLING":     lambda mid, m: cleanup.handle_indexes_enabling(mid, m),
    "DATA_VERIFYING":       lambda mid, m: data_verify.handle_data_verifying(mid, m),
    "DATA_MISMATCH":        lambda mid, m: data_verify.handle_data_mismatch(mid, m),
    "CDC_APPLY_STARTING":   lambda mid, m: cdc.handle_cdc_apply_starting(mid, m),
    "CDC_CATCHING_UP":      lambda mid, m: cdc.handle_cdc_catching_up(mid, m),
    "CDC_CAUGHT_UP":        lambda mid, m: cdc.handle_cdc_caught_up(mid, m),
    "STEADY_STATE":         lambda mid, m: cdc.handle_steady_state(mid, m),
    "CANCELLING":           lambda mid, m: cleanup.handle_cancelling(mid, m),
}

_GROUP_HANDLERS = {
    "NEW":                  lambda mid, m: group_new.handle_new_group(mid, m),
    "TOPIC_CREATING":       lambda mid, m: group_new.handle_topic_creating(mid, m),
    "STRUCTURE_READY":      lambda mid, m: bulk_pipeline.handle_structure_ready(mid, m),
    "DATA_COMPARING":       lambda mid, m: bulk_pipeline.handle_data_comparing(mid, m),
    "TARGET_CLEARING":      lambda mid, m: bulk_pipeline.handle_target_clearing(mid, m),
    "CHUNKING":             lambda mid, m: chunking.handle_chunking(mid, m),
    "BULK_LOADING":         lambda mid, m: chunking.handle_bulk_loading(mid, m),
    "BULK_LOADED":          lambda mid, m: chunking.handle_bulk_loaded(mid, m),
    "STAGE_VALIDATING":     lambda mid, m: baseline.handle_stage_validating(mid, m),
    "STAGE_VALIDATED":      lambda mid, m: baseline.handle_stage_validated(mid, m),
    "BASELINE_PUBLISHING":  lambda mid, m: baseline.handle_baseline_publishing(mid, m),
    "BASELINE_LOADING":     lambda mid, m: baseline.handle_baseline_loading(mid, m),
    "BASELINE_PUBLISHED":   lambda mid, m: baseline.handle_baseline_published(mid, m),
    "STAGE_DROPPING":       lambda mid, m: baseline.handle_stage_dropping(mid, m),
    "INDEXES_ENABLING":     lambda mid, m: cleanup.handle_indexes_enabling_group(mid, m),
    "DATA_VERIFYING":       lambda mid, m: data_verify.handle_data_verifying(mid, m),
    "DATA_MISMATCH":        lambda mid, m: data_verify.handle_data_mismatch(mid, m),
    "CDC_APPLYING":         lambda mid, m: cdc.handle_cdc_applying(mid, m),
    "CDC_CATCHING_UP":      lambda mid, m: cdc.handle_cdc_catching_up(mid, m),
    "CDC_CAUGHT_UP":        lambda mid, m: cdc.handle_cdc_caught_up(mid, m),
    "STEADY_STATE":         lambda mid, m: cdc.handle_steady_state(mid, m),
    "CANCELLING":           lambda mid, m: cleanup.handle_cancelling(mid, m),
}

_orchestrator_started = False


def is_running() -> bool:
    return _orchestrator_started


def start_orchestrator() -> None:
    global _orchestrator_started
    if _orchestrator_started:
        return
    _orchestrator_started = True

    def _run():
        time.sleep(3)
        while True:
            try:
                _tick()
            except Exception as exc:
                print(f"[orchestrator] tick error: {exc}")
            time.sleep(TICK_INTERVAL)

    threading.Thread(target=_run, daemon=True, name="orchestrator").start()
    print("[orchestrator] started")


def _tick() -> None:
    conn = get_conn()
    try:
        job_queue.reset_stale_chunks(conn)
        migrations = get_active_migrations(conn)
    finally:
        conn.close()

    for m in migrations:
        mid = m["migration_id"]
        phase = m["phase"]
        try:
            _dispatch(mid, phase, m)
        except Exception as exc:
            print(f"[orchestrator] migration {mid} phase {phase} error: {exc}")
            fail(mid, str(exc))

    tick_groups()
    check_group_connectors()


def _dispatch(migration_id: str, phase: str, m: dict) -> None:
    if m.get("group_id"):
        handler = _GROUP_HANDLERS.get(phase)
    else:
        handler = _LEGACY_HANDLERS.get(phase)
    if handler:
        handler(migration_id, m)
