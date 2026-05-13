/** API client for /api/schema-migrations/* endpoints. */
import type { SchemaInfo, SchemaObject, MigrationEvent, LiveMetrics } from "./types";

export interface SchemaMigrationListItem extends SchemaInfo {
  kpi: {
    totalObjects:  number;
    doneObjects:   number;
    errorObjects:  number;
    totalRows:     number;
    rowsDone:      number;
    progress:      number;
  };
  planId:  number | null;
  groupId: string | null;
  paused:  boolean;
}

export async function listSchemaMigrations(): Promise<SchemaMigrationListItem[]> {
  const r = await fetch("/api/schema-migrations");
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return r.json();
}

export async function getObjects(id: string): Promise<SchemaObject[]> {
  const r = await fetch(`/api/schema-migrations/${id}/objects`);
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return r.json();
}

export async function getEvents(id: string, limit = 100): Promise<MigrationEvent[]> {
  const r = await fetch(`/api/schema-migrations/${id}/events?limit=${limit}`);
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return r.json();
}

export async function getMetrics(id: string): Promise<LiveMetrics> {
  const r = await fetch(`/api/schema-migrations/${id}/metrics`);
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return r.json();
}

export interface CreatePayload {
  name?:           string;
  src_schema:      string;
  tgt_schema:      string;
  source_host?:    string;
  source_version?: string;
  target_host?:    string;
  target_version?: string;
  priority?:       "P0" | "P1" | "P2";
  owner?:          string;
  description?:    string;
  plan_id?:        number;
  group_id?:       string;
  window_at?:      string;
}

export async function createSchemaMigration(payload: CreatePayload): Promise<string> {
  const r = await fetch("/api/schema-migrations", {
    method:  "POST",
    headers: { "Content-Type": "application/json" },
    body:    JSON.stringify(payload),
  });
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  const j = await r.json();
  return j.schema_migration_id;
}

export async function pause(id: string, paused: boolean): Promise<void> {
  const r = await fetch(`/api/schema-migrations/${id}/${paused ? "pause" : "resume"}`, { method: "POST" });
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
}

export interface DdlSideInfo {
  metadata:      Record<string, unknown>;
  oracle_status: string | null;
  last_ddl_time: string | null;
}

export interface DdlDetailResp {
  kind:        "ddl";
  found:       boolean;
  object_type: string;       // canonical Oracle name (e.g. "MATERIALIZED VIEW")
  object_name: string;
  source?:     DdlSideInfo | null;
  target?:     DdlSideInfo | null;
  match_status?: "MATCH" | "DIFF" | "MISSING" | "EXTRA" | "UNKNOWN";
  diff?:       Record<string, unknown>;
}

export interface MigrationDetailResp {
  kind:         "migration";
  found:        boolean;
  migration_id?: string;
  migration?:   Record<string, unknown>;     // raw `migrations` row
  history?:     Array<Record<string, unknown>>;
  ddl_diff?:    DdlDetailResp | null;
}

export type ObjectDetailResp = DdlDetailResp | MigrationDetailResp;

export async function getObjectDetail(smId: string, objId: string): Promise<ObjectDetailResp> {
  const r = await fetch(`/api/schema-migrations/${smId}/objects/${encodeURIComponent(objId)}/detail`);
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return r.json();
}

export async function rollback(id: string): Promise<number> {
  const r = await fetch(`/api/schema-migrations/${id}/rollback`, { method: "POST" });
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  const j = await r.json();
  return j.affected as number;
}

export type DdlApplyAction = "create_missing" | "sync_diff" | "recreate";

export interface DdlApplyResp {
  queued:   number;
  job_ids:  string[];
  skipped:  Array<{ type: string; name: string; reason: string }>;
}

export async function applyDdl(
  smId:    string,
  action:  DdlApplyAction,
  objects: Array<{ type: string; name: string }>,
): Promise<DdlApplyResp> {
  const r = await fetch(`/api/schema-migrations/${smId}/ddl-apply`, {
    method:  "POST",
    headers: { "Content-Type": "application/json" },
    body:    JSON.stringify({ action, objects }),
  });
  if (!r.ok) {
    const t = await r.text();
    throw new Error(t || `HTTP ${r.status}`);
  }
  return r.json();
}

export interface DdlJob {
  job_id:       string;
  action:       DdlApplyAction;
  object_type:  string;
  object_name:  string;
  state:        "PENDING" | "CLAIMED" | "RUNNING" | "DONE" | "FAILED" | "CANCELLED";
  error_text:   string | null;
  created_at:   string | null;
  started_at:   string | null;
  completed_at: string | null;
}

export async function listDdlJobs(smId: string, limit = 100): Promise<DdlJob[]> {
  const r = await fetch(`/api/schema-migrations/${smId}/ddl-jobs?limit=${limit}`);
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return r.json();
}
