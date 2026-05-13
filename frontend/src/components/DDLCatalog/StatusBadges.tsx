import React from "react";
import { S } from "./styles";
import { t } from "../../theme";

export function MatchBadge({ status }: { status: string }) {
  switch (status) {
    case "MATCH":
      return <span style={S.badge("#22c55e22", t.green.base)}>Совпадает</span>;
    case "DIFF":
      return <span style={S.badge("#eab30822", t.amber.base)}>Отличается</span>;
    case "MISSING":
      return <span style={S.badge("#ef444422", t.red.base)}>Нет на таргете</span>;
    case "EXTRA":
      return <span style={S.badge("#8b5cf622", "#8b5cf6")}>Лишний</span>;
    default:
      return <span style={S.badge("#33415522", t.text.disabled)}>Не проверено</span>;
  }
}

export function MigrationBadge({ status }: { status: string }) {
  switch (status) {
    case "PLANNED":
      return <span style={S.badge("#3b82f622", t.blue.base)}>Запланирована</span>;
    case "IN_PROGRESS":
      return <span style={S.badge("#eab30822", t.amber.base)}>В процессе</span>;
    case "COMPLETED":
      return <span style={S.badge("#22c55e22", t.green.base)}>Завершена</span>;
    case "FAILED":
      return <span style={S.badge("#ef444422", t.red.base)}>Ошибка</span>;
    default:
      return <span style={S.badge("#33415522", t.text.disabled)}>Нет</span>;
  }
}
