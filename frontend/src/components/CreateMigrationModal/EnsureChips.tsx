import { t } from "../../theme";
import { Chip } from "./ui";
import type { EnsureResult } from "./types";

interface Props {
  result: EnsureResult;
}

/**
 * Renders the summary chips after an `ensure-table` request returns:
 * — created flag, added/dropped columns, drop errors, type warnings,
 *   added objects, errors, or "identical" chip when nothing changed.
 */
export function EnsureChips({ result }: Props) {
  const colsAdded   = result.columns?.added?.length        || 0;
  const colsDropped = result.columns?.dropped?.length      || 0;
  const colsDropErr = result.columns?.drop_errors?.length  || 0;
  const colsWarn    = result.columns?.warnings?.length     || 0;
  const objAdded    = (result.objects?.constraints?.added?.length || 0)
                    + (result.objects?.indexes?.added?.length     || 0)
                    + (result.objects?.triggers?.added?.length    || 0);
  const objErrors   = (result.objects?.constraints?.errors?.length || 0)
                    + (result.objects?.indexes?.errors?.length     || 0)
                    + (result.objects?.triggers?.errors?.length    || 0);

  const noChanges = !result.created
    && colsAdded === 0 && colsDropped === 0 && colsWarn === 0
    && objAdded === 0 && objErrors === 0;

  return (
    <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
      {result.created && (
        <Chip label="Таблица создана" color={t.green.fg} bg={t.green.bg} />
      )}
      {colsAdded > 0 && (
        <Chip label={`+${colsAdded} колонок`} color={t.green.fg} bg={t.green.bg} />
      )}
      {colsDropped > 0 && (
        <Chip label={`−${colsDropped} лишних колонок`} color="#f97316" bg="#431407" />
      )}
      {colsDropErr > 0 && (
        <Chip label={`${colsDropErr} ошибок удаления колонок`} color={t.red.fg} bg={t.red.bg} />
      )}
      {colsWarn > 0 && (
        <Chip label={`${colsWarn} расхождений типов`} color="#fbbf24" bg="#422006" />
      )}
      {objAdded > 0 && (
        <Chip label={`+${objAdded} объектов`} color={t.green.fg} bg={t.green.bg} />
      )}
      {objErrors > 0 && (
        <Chip label={`${objErrors} ошибок`} color={t.red.fg} bg={t.red.bg} />
      )}
      {noChanges && (
        <Chip label="Таблицы идентичны" color={t.green.fg} bg={t.green.bg} />
      )}
    </div>
  );
}
