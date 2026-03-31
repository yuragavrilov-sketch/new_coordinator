/**
 * Shared formatting utilities.
 * Replaces duplicated fmtTs/fmtNum/fmtDuration across components.
 */

type TsFormat = "full" | "short" | "time";

/**
 * Format an ISO timestamp string for display.
 * - "full": 01.01.2025, 12:00:00 (date + time with seconds)
 * - "short": 01.01.2025, 12:00 (date + time without seconds)
 * - "time": 12:00:00 (time only)
 */
export function fmtTs(
  iso: string | null | undefined,
  format: TsFormat = "full",
): string {
  if (!iso) return "—";
  try {
    const opts: Intl.DateTimeFormatOptions =
      format === "time"
        ? { hour: "2-digit", minute: "2-digit", second: "2-digit" }
        : format === "short"
          ? { day: "2-digit", month: "2-digit", year: "numeric", hour: "2-digit", minute: "2-digit" }
          : { day: "2-digit", month: "2-digit", year: "numeric", hour: "2-digit", minute: "2-digit", second: "2-digit" };
    return new Date(iso).toLocaleString("ru-RU", opts);
  } catch {
    return iso ?? "—";
  }
}

/** Format a number with locale separators, or "—" if null/undefined. */
export function fmtNum(n: number | null | undefined): string {
  if (n == null) return "—";
  return n.toLocaleString("ru-RU");
}

/** Format a duration in milliseconds to a human-readable string. */
export function fmtDuration(ms: number): string {
  if (ms < 0) ms = 0;
  if (ms < 1000) return `${ms} мс`;
  const s = Math.floor(ms / 1000);
  if (s < 60) return `${s} сек`;
  const m = Math.floor(s / 60);
  const rs = s % 60;
  if (m < 60) return `${m} мин${rs > 0 ? ` ${rs} сек` : ""}`;
  const h = Math.floor(m / 60);
  const rm = m % 60;
  return `${h} ч${rm > 0 ? ` ${rm} мин` : ""}`;
}

/** Format bytes to human-readable string. */
export function fmtBytes(bytes: number): string {
  if (bytes === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  const val = bytes / Math.pow(1024, i);
  return `${val.toFixed(i === 0 ? 0 : 1)} ${units[i]}`;
}
