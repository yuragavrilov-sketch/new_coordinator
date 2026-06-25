import type { FormData } from "./types";
import type { Strategy } from "../../types/migration";

export function toSnake(s: string): string {
  return s.replace(/[^a-zA-Z0-9]/g, "_").toLowerCase();
}

export function shortId(): string {
  return Math.random().toString(36).slice(2, 8);
}

export function autoFields(ss: string, st: string) {
  const s = toSnake(ss), tbl = toSnake(st), id = shortId();
  return {
    connector_name:   `${s}_${tbl}_${id}_connector`,
    topic_prefix:     `${s}.${tbl}.${id}`,
    consumer_group:   `${s}_${tbl}_${id}_cg`,
    stage_table_name: `STG_${ss.toUpperCase()}_${st.toUpperCase()}`,
  };
}

export const KEY_SOURCE_MAP: Record<string, string> = {
  PRIMARY_KEY:  "PK",
  UNIQUE_KEY:   "UK",
  USER_DEFINED: "USER",
  NONE:         "NONE",
};

export const INIT: FormData = {
  migration_name:           "",
  source_schema:            "",
  source_table:             "",
  target_schema:            "",
  target_table:             "",
  strategy:                 "BULK_DIRECT" as Strategy,
  truncate_target:          true,
  group_id:                 "",
  connector_name:           "",
  topic_prefix:             "",
  consumer_group:           "",
  stage_table_name:         "",
  stage_tablespace:         "PAYSTAGE",
  chunk_size:               1_000_000,
  max_parallel_workers:     1,
  baseline_parallel_degree: 4,
  validate_hash_sample:     false,
  effective_key_type:       "",
  effective_key_columns:    [],
  selected_uk_index:        0,
};
