import { Strategy } from "../../../types/migration";

export interface Column   { name: string; type: string; nullable: boolean }
export interface UkConstraint { name: string; columns: string[] }
export interface TableInfo {
  columns:        Column[];
  pk_columns:     string[];
  uk_constraints: UkConstraint[];
}

export interface TableKeyEntry {
  tableInfo:             TableInfo | null;
  loadingInfo:           boolean;
  infoError:             string;
  effective_key_type:    string;
  effective_key_columns: string[];
  selected_uk_index:     number;
}

export interface BatchItem {
  table:           string;
  strategy:        Strategy;
  truncate_target: boolean;
  chunk_size:      number;
  workers:         number;
}

export interface Batch {
  id:    number;
  items: BatchItem[];
}

export interface PlanDefaults {
  chunk_size:      number;
  workers:         number;
  strategy:        Strategy;
  truncate_target: boolean;
}

export interface FKDep {
  table:      string;
  depends_on: string[];
}

export interface ConnectorGroup {
  group_id:       string;
  id:             string;
  group_name:     string;
  connector_name: string;
  status:         string;
}

export interface PlanSummary {
  plan_id:    number;
  name:       string;
  status:     string;
  item_count: number;
  items_done: number;
}
