import { t } from "../../theme";

interface Props {
  tables:      string[];
  schema:      string;
  groupMap:    Map<string, string>;  // "SCHEMA.TABLE" → group_name
  onAdd:       (table: string) => void;
  emptyText:   string;
}

/** Scrollable list of tables that can be added to the group. */
export function AvailableTablesList({ tables, schema, groupMap, onAdd, emptyText }: Props) {
  return (
    <div style={{
      height: 200, overflowY: "auto",
      border: `1px solid ${t.border.base}`,
      borderRadius: t.radius.md,
      background: t.bg.s2,
    }}>
      {tables.length === 0 && (
        <div style={{ padding: "8px 10px", color: t.text.faint, fontSize: t.size.base }}>
          {emptyText}
        </div>
      )}
      {tables.map(tbl => {
        const groupName = groupMap.get(`${schema}.${tbl}`);
        return (
          <div
            key={tbl}
            onClick={() => onAdd(tbl)}
            style={{
              padding: "6px 10px", fontSize: t.size.base, cursor: "pointer",
              color: t.text.primary, display: "flex", alignItems: "center", gap: 8,
              borderBottom: `1px solid ${t.bg.app}`,
            }}
            onMouseEnter={e => (e.currentTarget.style.background = t.border.base)}
            onMouseLeave={e => (e.currentTarget.style.background = "transparent")}
          >
            <span style={{ color: t.blue.base, fontSize: t.size.lg, fontWeight: 700 }}>+</span>
            <span style={{ fontFamily: t.font.mono }}>{tbl}</span>
            {groupName && (
              <span style={{
                marginLeft: "auto", fontSize: 9, fontWeight: 600,
                padding: "1px 6px", borderRadius: 3,
                background: "#431407", color: "#fdba74",
                border: "1px solid #ea580c44",
              }}>
                {groupName}
              </span>
            )}
          </div>
        );
      })}
    </div>
  );
}
