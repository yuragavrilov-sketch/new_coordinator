export interface Column {
  name:     string;
  type:     string;
  nullable: boolean;
}

export interface UkConstraint {
  name:    string;
  columns: string[];
}

export interface TableInfo {
  columns:        Column[];
  pk_columns:     string[];
  uk_constraints: UkConstraint[];
}

export interface FormData {
  migration_name:           string;
  source_schema:            string;
  source_table:             string;
  target_schema:            string;
  target_table:             string;
  migration_mode:           "CDC" | "BULK_ONLY";
  migration_strategy:       "STAGE" | "DIRECT";
  group_id:                 string;
  connector_name:           string;
  topic_prefix:             string;
  consumer_group:           string;
  stage_table_name:         string;
  stage_tablespace:         string;
  chunk_size:               number;
  max_parallel_workers:     number;
  baseline_parallel_degree: number;
  validate_hash_sample:     boolean;
  effective_key_type:       string;
  effective_key_columns:    string[];
  selected_uk_index:        number;
}

export interface EnsureResult {
  created: boolean;
  columns: any;
  objects: any;
}
