import threading

import services.oracle_scn as oracle_scn
import db.oracle_browser as oracle_browser

from orchestrator.helpers import (
    oracle_cfg, transition, fail, safe_transition,
    current_phase, in_prog, mark_in_prog, unmark_in_prog,
)


def handle_indexes_enabling(mid: str, m: dict) -> None:
    """Enable all UNUSABLE indexes, DISABLED constraints and triggers — runs in a thread."""
    if in_prog(mid):
        return
    mark_in_prog(mid)

    def _run():
        try:
            dst_cfg = oracle_cfg(m["target_connection_id"])
            conn = oracle_scn.open_oracle_conn(dst_cfg)
            try:
                oracle_browser.set_table_logging(
                    conn, m["target_schema"], m["target_table"], nologging=False,
                )
                result = oracle_browser.enable_all_disabled_objects(
                    conn, m["target_schema"], m["target_table"],
                )

                # Note: IDENTITY columns are restored via the Switchover tab,
                # not automatically here — user controls the timing.
            finally:
                conn.close()

            err_count = (
                len(result["errors"]["indexes"])
                + len(result["errors"]["constraints"])
            )
            if err_count:
                names = (
                    [e["name"] for e in result["errors"]["indexes"]]
                    + [e["name"] for e in result["errors"]["constraints"]]
                )
                err_detail = str(result["errors"])
                transition(
                    mid, "INDEXES_ENABLING",
                    message=(
                        f"Ошибка пересчёта: {', '.join(names)}. "
                        "Нажмите «Включить индексы» ещё раз для повторной попытки."
                    ),
                    extra_fields={
                        "error_code": "INDEXES_ENABLE_ERROR",
                        "error_text": err_detail[:2000],
                    },
                )
                return

            n_idx = len(result["enabled"]["indexes"])
            n_con = len(result["enabled"]["constraints"])

            mode = (m.get("migration_mode") or "CDC").upper()
            if mode == "BULK_ONLY":
                try:
                    oracle_browser.enable_triggers(
                        oracle_scn.open_oracle_conn(dst_cfg),
                        m["target_schema"], m["target_table"],
                    )
                except Exception as exc:
                    print(f"[orchestrator] {mid}: enable triggers warning: {exc}")

                msg = (
                    f"Включено: индексов={n_idx}, констрейнтов={n_con}. "
                    "Режим BULK_ONLY — запуск сверки данных"
                )
                safe_transition(
                    mid, "INDEXES_ENABLING", "DATA_VERIFYING",
                    message=msg,
                    extra_fields={"error_code": None, "error_text": None},
                )
            else:
                msg = (
                    f"Включено: индексов={n_idx}, констрейнтов={n_con}. "
                    "Триггеры остаются выключенными до завершения CDC. "
                    "Ожидание запуска CDC apply-worker"
                )
                safe_transition(
                    mid, "INDEXES_ENABLING", "CDC_APPLY_STARTING",
                    message=msg,
                    extra_fields={"error_code": None, "error_text": None},
                )
        except Exception as exc:
            if current_phase(mid) not in ("CANCELLING", "CANCELLED"):
                fail(mid, str(exc), "INDEXES_ENABLE_ERROR")
        finally:
            unmark_in_prog(mid)

    threading.Thread(target=_run, daemon=True).start()


def handle_indexes_enabling_group(mid: str, m: dict) -> None:
    """Same as legacy handle_indexes_enabling but routes to CDC_APPLYING
    instead of CDC_APPLY_STARTING for group-based migrations."""
    if in_prog(mid):
        return
    mark_in_prog(mid)

    def _run():
        try:
            dst_cfg = oracle_cfg(m["target_connection_id"])
            conn = oracle_scn.open_oracle_conn(dst_cfg)
            try:
                oracle_browser.set_table_logging(
                    conn, m["target_schema"], m["target_table"], nologging=False,
                )
                result = oracle_browser.enable_all_disabled_objects(
                    conn, m["target_schema"], m["target_table"],
                )

                id_restored = oracle_browser.restore_identity_always(
                    conn, m["target_schema"], m["target_table"],
                )
                if id_restored:
                    print(f"[orchestrator] {mid}: restored IDENTITY ALWAYS: {id_restored}")
            finally:
                conn.close()

            err_count = (
                len(result["errors"]["indexes"])
                + len(result["errors"]["constraints"])
            )
            if err_count:
                names = (
                    [e["name"] for e in result["errors"]["indexes"]]
                    + [e["name"] for e in result["errors"]["constraints"]]
                )
                err_detail = str(result["errors"])
                transition(
                    mid, "INDEXES_ENABLING",
                    message=(
                        f"Ошибка пересчёта: {', '.join(names)}. "
                        "Нажмите «Включить индексы» ещё раз для повторной попытки."
                    ),
                    extra_fields={
                        "error_code": "INDEXES_ENABLE_ERROR",
                        "error_text": err_detail[:2000],
                    },
                )
                return

            n_idx = len(result["enabled"]["indexes"])
            n_con = len(result["enabled"]["constraints"])

            mode = (m.get("migration_mode") or "CDC").upper()
            if mode == "BULK_ONLY":
                try:
                    oracle_browser.enable_triggers(
                        oracle_scn.open_oracle_conn(dst_cfg),
                        m["target_schema"], m["target_table"],
                    )
                except Exception as exc:
                    print(f"[orchestrator] {mid}: enable triggers warning: {exc}")

                safe_transition(
                    mid, "INDEXES_ENABLING", "DATA_VERIFYING",
                    message=(
                        f"Включено: индексов={n_idx}, констрейнтов={n_con}. "
                        "Режим BULK_ONLY — запуск сверки данных"
                    ),
                    extra_fields={"error_code": None, "error_text": None},
                )
            else:
                # Group-based CDC → CDC_APPLYING (not CDC_APPLY_STARTING)
                safe_transition(
                    mid, "INDEXES_ENABLING", "CDC_APPLYING",
                    message=(
                        f"Включено: индексов={n_idx}, констрейнтов={n_con}. "
                        "Ожидание CDC apply-worker"
                    ),
                    extra_fields={"error_code": None, "error_text": None},
                )
        except Exception as exc:
            if current_phase(mid) not in ("CANCELLING", "CANCELLED"):
                fail(mid, str(exc), "INDEXES_ENABLE_ERROR")
        finally:
            unmark_in_prog(mid)

    threading.Thread(target=_run, daemon=True).start()


def handle_cancelling(mid: str, m: dict) -> None:
    """Wait for any in-flight thread to finish, then transition to CANCELLED."""
    if in_prog(mid):
        return  # thread still running — wait for next tick
    transition(mid, "CANCELLED", message="Миграция отменена")
