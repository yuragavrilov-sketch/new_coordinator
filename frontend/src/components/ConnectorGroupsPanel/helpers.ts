import React from "react";
import type { GroupStatus, Strategy } from "../../types/migration";
import { t } from "../../theme";
import type { MigrateParams } from "./types";

export const STATUS_COLORS: Record<GroupStatus, { bg: string; text: string }> = {
  PENDING:            { bg: t.bg.s2,    text: t.text.secondary },
  TOPICS_CREATING:    { bg: t.bg.s3,    text: t.blue.fg },
  CONNECTOR_STARTING: { bg: t.bg.s3,    text: t.blue.fg },
  RUNNING:            { bg: t.green.bg, text: t.green.fg },
  STOPPING:           { bg: t.red.bg,  text: t.amber.fg },
  STOPPED:            { bg: t.bg.s2,  text: t.text.muted },
  FAILED:             { bg: t.red.bg,   text: t.red.fg },
};

export function actionBtn(bg: string, border: string): React.CSSProperties {
  return {
    background:   bg,
    border:       `1px solid ${border}`,
    borderRadius: t.radius.sm,
    color:        border,
    padding:      "2px 10px",
    fontSize:     t.size.sm,
    cursor:       "pointer",
  };
}

export const MIGRATE_DEFAULTS: MigrateParams = {
  strategy:                 "CDC_STAGE" as Strategy,
  truncate_target:          true,
  chunk_size:               1_000_000,
  max_parallel_workers:     1,
  baseline_parallel_degree: 4,
  baseline_batch_size:      500_000,
  stage_tablespace:         "PAYSTAGE",
  validate_hash_sample:     false,
};
