import type { Column, UkConstraint, TableInfo } from "../CreateMigrationModal/types";
export type { Column, UkConstraint, TableInfo };

export interface TableEntry {
  schema:                string;
  table:                 string;
  tableInfo:             TableInfo | null;
  loadingInfo:           boolean;
  infoError:             string;
  effective_key_type:    string;
  effective_key_columns: string[];
  selected_uk_index:     number;
  target_schema:         string;
  target_table:          string;
}

export interface GroupForm {
  group_name:     string;
  connector_name: string;
  topic_prefix:   string;
}
