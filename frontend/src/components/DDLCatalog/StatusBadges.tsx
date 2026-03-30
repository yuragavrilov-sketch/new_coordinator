import React from "react";
import { S } from "./styles";

export function MatchBadge({ status }: { status: string }) {
  switch (status) {
    case "MATCH":
      return <span style={S.badge("#22c55e22", "#22c55e")}>Совпадает</span>;
    case "DIFF":
      return <span style={S.badge("#eab30822", "#eab308")}>Отличается</span>;
    case "MISSING":
      return <span style={S.badge("#ef444422", "#ef4444")}>Нет на таргете</span>;
    case "EXTRA":
      return <span style={S.badge("#8b5cf622", "#8b5cf6")}>Лишний</span>;
    default:
      return <span style={S.badge("#33415522", "#475569")}>Не проверено</span>;
  }
}

export function MigrationBadge({ status }: { status: string }) {
  switch (status) {
    case "PLANNED":
      return <span style={S.badge("#3b82f622", "#3b82f6")}>Запланирована</span>;
    case "IN_PROGRESS":
      return <span style={S.badge("#eab30822", "#eab308")}>В процессе</span>;
    case "COMPLETED":
      return <span style={S.badge("#22c55e22", "#22c55e")}>Завершена</span>;
    case "FAILED":
      return <span style={S.badge("#ef444422", "#ef4444")}>Ошибка</span>;
    default:
      return <span style={S.badge("#33415522", "#475569")}>Нет</span>;
  }
}
