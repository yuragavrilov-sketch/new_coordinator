export type MigrationPhase =
  | "DRAFT" | "NEW" | "PREPARING" | "SCN_FIXED"
  | "CONNECTOR_STARTING" | "CDC_BUFFERING"
  | "CHUNKING" | "BULK_LOADING" | "BULK_LOADED"
  | "STAGE_VALIDATING" | "STAGE_VALIDATED"
  | "BASELINE_PUBLISHING" | "BASELINE_PUBLISHED"
  | "CDC_APPLY_STARTING" | "CDC_CATCHING_UP" | "CDC_CAUGHT_UP"
  | "STEADY_STATE" | "PAUSED"
  | "CANCELLING" | "CANCELLED"
  | "COMPLETED" | "FAILED";

export interface Migration {
  migration_id: string;
  migration_name: string;
  phase: MigrationPhase;
  state_changed_at: string;
  source_connection_id: string;
  target_connection_id: string;
  source_schema: string;
  source_table: string;
  target_schema: string;
  target_table: string;
  stage_table_name: string;
  connector_name: string;
  topic_prefix: string;
  consumer_group: string;
  chunk_strategy: string;
  chunk_size: number;
  apply_mode: string;
  source_pk_exists: boolean;
  source_uk_exists: boolean;
  effective_key_type: string;
  effective_key_source: string;
  effective_key_columns_json: string;
  key_uniqueness_validated: boolean;
  key_validation_status: string | null;
  key_validation_message: string | null;
  start_scn: string | null;
  scn_fixed_at: string | null;
  created_by: string | null;
  description: string | null;
  created_at: string;
  updated_at: string;
  locked_by: string | null;
  lock_until: string | null;
  error_code: string | null;
  error_text: string | null;
  failed_phase: string | null;
  retry_count: number;
  // ── Progress / monitoring (added stage 1) ────────────────────────────────
  total_rows: number | null;
  total_chunks: number | null;
  chunks_done: number;
  chunks_failed: number;
  validate_hash_sample: boolean;
  validation_result: Record<string, unknown> | null;
  connector_status: string | null;
  kafka_lag: number | null;
  kafka_lag_checked_at: string | null;
}

export interface MigrationSummary {
  migration_id: string;
  migration_name: string;
  phase: MigrationPhase;
  state_changed_at: string;
  source_schema: string;
  source_table: string;
  target_schema: string;
  target_table: string;
  created_at: string;
  updated_at: string;
  error_code: string | null;
  error_text: string | null;
  failed_phase: string | null;
  retry_count: number;
  description: string | null;
  created_by: string | null;
  total_rows: number | null;
  total_chunks: number | null;
  chunks_done: number;
  chunks_failed: number;
}

export interface StateHistoryEntry {
  id: number;
  migration_id: string;
  from_phase: string | null;
  to_phase: string;
  transition_status: string;
  transition_reason: string | null;
  message: string | null;
  actor_type: string;
  actor_id: string | null;
  correlation_id: string | null;
  created_at: string;
}

export interface MigrationDetail extends Migration {
  history: StateHistoryEntry[];
}

// ── Phase colours ────────────────────────────────────────────────────────────

interface PhaseColor { bg: string; text: string; border: string }

const PHASE_COLORS: Record<string, PhaseColor> = {
  DRAFT:               { bg: "#1e293b", text: "#94a3b8", border: "#334155" },
  NEW:                 { bg: "#1e3a5f", text: "#93c5fd", border: "#1d4ed8" },
  PREPARING:           { bg: "#1e3a5f", text: "#93c5fd", border: "#1d4ed8" },
  SCN_FIXED:           { bg: "#1e3a5f", text: "#93c5fd", border: "#1d4ed8" },
  CONNECTOR_STARTING:  { bg: "#2e1065", text: "#c4b5fd", border: "#7c3aed" },
  CDC_BUFFERING:       { bg: "#2e1065", text: "#c4b5fd", border: "#7c3aed" },
  CHUNKING:            { bg: "#3b2000", text: "#fcd34d", border: "#d97706" },
  BULK_LOADING:        { bg: "#3b2000", text: "#fcd34d", border: "#d97706" },
  BULK_LOADED:         { bg: "#3b2000", text: "#fcd34d", border: "#d97706" },
  STAGE_VALIDATING:    { bg: "#083344", text: "#67e8f9", border: "#0891b2" },
  STAGE_VALIDATED:     { bg: "#083344", text: "#67e8f9", border: "#0891b2" },
  BASELINE_PUBLISHING: { bg: "#2e1065", text: "#c4b5fd", border: "#7c3aed" },
  BASELINE_PUBLISHED:  { bg: "#2e1065", text: "#c4b5fd", border: "#7c3aed" },
  CDC_APPLY_STARTING:  { bg: "#431407", text: "#fdba74", border: "#ea580c" },
  CDC_CATCHING_UP:     { bg: "#431407", text: "#fdba74", border: "#ea580c" },
  CDC_CAUGHT_UP:       { bg: "#431407", text: "#fdba74", border: "#ea580c" },
  STEADY_STATE:        { bg: "#052e16", text: "#86efac", border: "#16a34a" },
  PAUSED:              { bg: "#1e293b", text: "#cbd5e1", border: "#475569" },
  CANCELLING:          { bg: "#450a0a", text: "#fca5a5", border: "#dc2626" },
  CANCELLED:           { bg: "#1c1917", text: "#78716c", border: "#57534e" },
  COMPLETED:           { bg: "#052e16", text: "#86efac", border: "#16a34a" },
  FAILED:              { bg: "#450a0a", text: "#fca5a5", border: "#dc2626" },
};

const FALLBACK_COLOR: PhaseColor = { bg: "#1e293b", text: "#94a3b8", border: "#334155" };

export function phaseColor(phase: string): PhaseColor {
  return PHASE_COLORS[phase] ?? FALLBACK_COLOR;
}

export const ORDERED_PHASES: MigrationPhase[] = [
  "DRAFT", "NEW", "PREPARING", "SCN_FIXED",
  "CONNECTOR_STARTING", "CDC_BUFFERING",
  "CHUNKING", "BULK_LOADING", "BULK_LOADED",
  "STAGE_VALIDATING", "STAGE_VALIDATED",
  "BASELINE_PUBLISHING", "BASELINE_PUBLISHED",
  "CDC_APPLY_STARTING", "CDC_CATCHING_UP", "CDC_CAUGHT_UP",
  "STEADY_STATE",
];
