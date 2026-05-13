import { S } from "../styles";
import { Chip, KeyTypeBtn } from "../../CreateMigrationModal/ui";
import type { TableKeyEntry } from "./types";
import { t } from "../../../theme";

interface Props {
  entry:    TableKeyEntry;
  onChange: (upd: Partial<TableKeyEntry>) => void;
}

export function TableKeyConfig({ entry, onChange }: Props) {
  const info = entry.tableInfo;

  function setKeyType(kt: string) {
    let cols: string[] = [];
    if (kt === "PRIMARY_KEY") cols = info?.pk_columns ?? [];
    if (kt === "UNIQUE_KEY")  cols = info?.uk_constraints[entry.selected_uk_index]?.columns ?? [];
    onChange({ effective_key_type: kt, effective_key_columns: cols });
  }

  function toggleKeyCol(col: string, checked: boolean) {
    const cols = checked
      ? [...entry.effective_key_columns, col]
      : entry.effective_key_columns.filter(c => c !== col);
    onChange({ effective_key_columns: cols });
  }

  if (entry.loadingInfo) {
    return <div style={{ fontSize: 11, color: t.text.disabled, padding: "4px 0" }}>Загрузка информации...</div>;
  }
  if (entry.infoError) {
    return <div style={{ fontSize: 10, color: t.red.fg }}>{entry.infoError}</div>;
  }
  if (!info) return null;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
      {/* chips */}
      <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
        <Chip label={`${info.columns.length} кол.`} color={t.text.secondary} bg={t.bg.s2} />
        {info.pk_columns.length > 0
          ? <Chip label={`PK: ${info.pk_columns.join(", ")}`} color={t.green.fg} bg={t.green.bg} />
          : <Chip label="Нет PK" color={t.red.fg} bg={t.red.bg} />}
        {info.uk_constraints.length > 0 && (
          <Chip label={`UK: ${info.uk_constraints.length}`} color={t.purple.fg} bg={t.purple.bg} />
        )}
      </div>

      {/* key type selector */}
      <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
        <KeyTypeBtn label="PRIMARY KEY"
          active={entry.effective_key_type === "PRIMARY_KEY"}
          disabled={info.pk_columns.length === 0}
          onClick={() => setKeyType("PRIMARY_KEY")} />
        <KeyTypeBtn label="UNIQUE KEY"
          active={entry.effective_key_type === "UNIQUE_KEY"}
          disabled={info.uk_constraints.length === 0}
          onClick={() => setKeyType("UNIQUE_KEY")} />
        <KeyTypeBtn label="USER DEFINED"
          active={entry.effective_key_type === "USER_DEFINED"}
          onClick={() => setKeyType("USER_DEFINED")} />
        <KeyTypeBtn label="NONE"
          active={entry.effective_key_type === "NONE"}
          onClick={() => setKeyType("NONE")} />
      </div>

      {entry.effective_key_type === "PRIMARY_KEY" && (
        <div style={{ fontSize: 11, color: t.green.fg }}>
          Ключ: <strong>{info.pk_columns.join(", ")}</strong>
        </div>
      )}

      {entry.effective_key_type === "UNIQUE_KEY" && (
        <>
          {info.uk_constraints.length > 1 && (
            <select
              style={{ ...S.select, fontSize: 11 }}
              value={entry.selected_uk_index}
              onChange={e => {
                const idx = parseInt(e.target.value);
                onChange({
                  selected_uk_index: idx,
                  effective_key_columns: info.uk_constraints[idx]?.columns ?? [],
                });
              }}
            >
              {info.uk_constraints.map((uk, i) => (
                <option key={uk.name} value={i}>{uk.name} ({uk.columns.join(", ")})</option>
              ))}
            </select>
          )}
          <div style={{ fontSize: 11, color: t.purple.fg }}>
            Ключ: <strong>{info.uk_constraints[entry.selected_uk_index]?.columns.join(", ")}</strong>
          </div>
        </>
      )}

      {entry.effective_key_type === "USER_DEFINED" && (
        <div>
          <div style={{
            maxHeight: 140, overflowY: "auto",
            border: `1px solid ${t.border.base}`, borderRadius: 5, background: t.bg.s2,
          }}>
            {info.columns.map(col => (
              <label key={col.name} style={{
                display: "flex", alignItems: "center", gap: 8,
                padding: "4px 10px", cursor: "pointer",
                borderBottom: `1px solid ${t.bg.app}`,
              }}>
                <input type="checkbox"
                  checked={entry.effective_key_columns.includes(col.name)}
                  onChange={e => toggleKeyCol(col.name, e.target.checked)}
                />
                <span style={{ fontSize: 11, color: t.text.primary, fontFamily: "monospace" }}>{col.name}</span>
                <span style={{ fontSize: 9, color: t.text.disabled }}>{col.type}</span>
                {!col.nullable && (
                  <span style={{ fontSize: 9, color: t.red.base, marginLeft: "auto" }}>NOT NULL</span>
                )}
              </label>
            ))}
          </div>
          {entry.effective_key_columns.length > 0 && (
            <div style={{ fontSize: 10, color: t.text.disabled }}>Выбрано: {entry.effective_key_columns.join(", ")}</div>
          )}
          {entry.effective_key_columns.length === 0 && (
            <div style={{ fontSize: 10, color: t.red.fg }}>Выберите хотя бы один столбец</div>
          )}
        </div>
      )}
    </div>
  );
}
