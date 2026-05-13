import type { GroupForm } from "./types";

export { toSnake, shortId } from "../CreateMigrationModal/helpers";

/** Wildcard filter: no `*` → prefix match, with `*` → glob (case-insensitive). */
export function matchesFilter(name: string, filter: string): boolean {
  if (!filter) return true;
  const f = filter.toUpperCase();
  const n = name.toUpperCase();
  if (!f.includes("*")) return n.startsWith(f);
  const re = new RegExp(
    "^" + f.replace(/[.+?^${}()|[\]\\]/g, "\\$&").replace(/\*/g, ".*") + "$",
  );
  return re.test(n);
}

export const INIT_FORM: GroupForm = {
  group_name:     "",
  connector_name: "",
  topic_prefix:   "",
};

export const STEP_LABELS = ["Группа", "Таблицы и ключи"];
