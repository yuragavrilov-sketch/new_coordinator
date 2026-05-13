import type { FKDep } from "./types";

/** Topological sort (Kahn's algorithm) — returns table names ordered by FK deps. */
export function topoSort(tables: string[], deps: FKDep[]): string[] {
  const graph    = new Map<string, string[]>();
  const inDeg    = new Map<string, number>();
  const tableSet = new Set(tables);
  for (const t of tables) { graph.set(t, []); inDeg.set(t, 0); }
  for (const d of deps) {
    if (!tableSet.has(d.table)) continue;
    for (const p of d.depends_on) {
      if (!tableSet.has(p)) continue;
      graph.get(p)!.push(d.table);
      inDeg.set(d.table, (inDeg.get(d.table) || 0) + 1);
    }
  }
  const queue: string[] = [];
  for (const [t, deg] of inDeg) if (deg === 0) queue.push(t);
  const sorted: string[] = [];
  while (queue.length > 0) {
    const cur = queue.shift()!;
    sorted.push(cur);
    for (const next of graph.get(cur) || []) {
      const newDeg = (inDeg.get(next) || 1) - 1;
      inDeg.set(next, newDeg);
      if (newDeg === 0) queue.push(next);
    }
  }
  for (const t of tables) if (!sorted.includes(t)) sorted.push(t);
  return sorted;
}
