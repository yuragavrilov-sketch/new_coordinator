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

export async function rollback(id: string): Promise<number> {
  const r = await fetch(`/api/schema-migrations/${id}/rollback`, { method: "POST" });
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  const j = await r.json();
  return j.affected as number;
}
