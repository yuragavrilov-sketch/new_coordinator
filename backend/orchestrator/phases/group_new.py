import json
import threading

import services.oracle_scn as oracle_scn
import services.oracle_stage as oracle_stage
import services.kafka_topics as kafka_topics
import services.connector_groups as connector_groups_svc

from orchestrator.helpers import (
    oracle_cfg, configs, get_conn, transition, fail, update, safe_transition,
    current_phase, in_prog, mark_in_prog, unmark_in_prog,
)
from orchestrator.queue import check_loading_slot
from orchestrator.phases.preparing import create_chunks_and_transition


def handle_new_group(mid: str, m: dict) -> None:
    """Group migration: validate keys, queue gate, create stage, → TOPIC_CREATING.

    Unlike legacy NEW:
    - No SCN fixation
    - Connector already managed at group level
    """
    mode = (m.get("migration_mode") or "CDC").upper()
    pk = m.get("source_pk_exists", False)
    uk = m.get("source_uk_exists", False)
    key_cols = json.loads(m.get("effective_key_columns_json") or "[]")

    if mode != "BULK_ONLY" and not pk and not uk and not key_cols:
        fail(mid,
             "Таблица не имеет PK/UK и ключевые колонки не заданы.",
             "NO_KEY_COLUMNS")
        return

    # Queue gate (same as legacy)
    if not check_loading_slot(mid):
        return

    # Verify group connector is RUNNING (for CDC mode)
    mode = (m.get("migration_mode") or "CDC").upper()
    if mode != "BULK_ONLY":
        group = connector_groups_svc.get_group(m["group_id"])
        if not group:
            fail(mid, "Группа коннектора не найдена", "GROUP_NOT_FOUND")
            return
        if group["status"] != "RUNNING":
            fail(mid,
                 f"Коннектор группы не запущен (status={group['status']}). "
                 "Запустите коннектор группы перед миграцией.",
                 "GROUP_NOT_RUNNING")
            return

    update(mid, {"queue_position": None})

    # Create stage table (if STAGE strategy) — same as legacy PREPARING
    # but without SCN fixation
    if in_prog(mid):
        return
    mark_in_prog(mid)

    def _run():
        try:
            src_cfg = oracle_cfg(m["source_connection_id"])
            dst_cfg = oracle_cfg(m["target_connection_id"])
            strategy = (m.get("migration_strategy") or "STAGE").upper()

            if mode != "BULK_ONLY":
                try:
                    has_supp = oracle_scn.check_supplemental_logging(
                        src_cfg, m["source_schema"], m["source_table"]
                    )
                    if not has_supp:
                        print(
                            f"[orchestrator] WARNING: {m['source_schema']}.{m['source_table']} "
                            "does not have ALL COLUMNS supplemental logging."
                        )
                except Exception as exc:
                    print(f"[orchestrator] supplemental logging check failed: {exc}")

            if strategy == "STAGE":
                ts = m.get("stage_tablespace") or ""
                oracle_stage.create_stage_table(
                    src_cfg, dst_cfg,
                    m["source_schema"], m["source_table"],
                    m["target_schema"], m["stage_table_name"],
                    tablespace=ts,
                )
                stage_msg = "Stage table создана"
            else:
                stage_msg = "Прямая загрузка (без stage)"

            if mode == "BULK_ONLY":
                # Skip topic creation — go straight to chunking
                safe_transition(mid, "NEW", "CHUNKING",
                                message=f"{stage_msg}, BULK_ONLY → нарезка чанков")
                # Create chunks inline
                unmark_in_prog(mid)
                create_chunks_and_transition(mid, m)
                return
            else:
                safe_transition(mid, "NEW", "TOPIC_CREATING",
                                message=f"{stage_msg}, создание топика Kafka")
        except Exception as exc:
            if current_phase(mid) not in ("CANCELLING", "CANCELLED"):
                fail(mid, str(exc), "PREPARING_ERROR")
        finally:
            unmark_in_prog(mid)

    threading.Thread(target=_run, daemon=True).start()


def handle_topic_creating(mid: str, m: dict) -> None:
    """Pre-create the Kafka topic for this table, then create chunks."""
    if in_prog(mid):
        return
    mark_in_prog(mid)

    def _run():
        try:
            topic_prefix = m.get("topic_prefix", "")
            src_schema = m["source_schema"].upper()
            src_table = m["source_table"].upper()
            topic_name = f"{topic_prefix}.{src_schema}.{src_table}".replace("#", "_")

            # Get Kafka bootstrap servers
            cfg = configs()
            kafka_cfg = cfg.get("kafka", {})
            bootstrap = [
                s.strip()
                for s in (kafka_cfg.get("bootstrap_servers") or "kafka:9092").split(",")
            ]

            kafka_topics.create_topic(
                bootstrap_servers=bootstrap,
                topic_name=topic_name,
            )

            # Transition to chunking
            safe_transition(mid, "TOPIC_CREATING", "CHUNKING",
                            message=f"Топик {topic_name} создан, нарезка чанков")

            # Create chunks
            unmark_in_prog(mid)
            create_chunks_and_transition(mid, m)
            return
        except Exception as exc:
            if current_phase(mid) not in ("CANCELLING", "CANCELLED"):
                fail(mid, str(exc), "TOPIC_CREATE_ERROR")
        finally:
            unmark_in_prog(mid)

    threading.Thread(target=_run, daemon=True).start()
