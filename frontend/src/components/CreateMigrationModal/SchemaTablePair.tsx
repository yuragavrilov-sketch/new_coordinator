import { useEffect, useRef, useState } from "react";
import { S } from "./styles";
import { Field } from "./ui";
import { SearchableSelect } from "./SearchableSelect";

interface Props {
  db:             "source" | "target";
  schema:         string;
  table:          string;
  onSchema:       (v: string) => void;
  onTable:        (v: string) => void;
  schemaErr?:     string;
  tableErr?:      string;
  excludeTables?: Set<string>;
}

export function SchemaTablePair({
  db, schema, table, onSchema, onTable,
  schemaErr, tableErr, excludeTables,
}: Props) {
  const [schemas, setSchemas] = useState<string[]>([]);
  const [tables,  setTables]  = useState<string[]>([]);
  const [lSch,    setLSch]    = useState(true);
  const [lTab,    setLTab]    = useState(false);
  const [eSch,    setESch]    = useState("");
  const [eTab,    setETab]    = useState("");

  useEffect(() => {
    setLSch(true);
    fetch(`/api/db/${db}/schemas`)
      .then(r => r.json())
      .then(d => { if (d.error) setESch(d.error); else setSchemas(d); })
      .catch(e => setESch(String(e)))
      .finally(() => setLSch(false));
  }, [db]);

  useEffect(() => {
    if (!schema) { setTables([]); return; }
    setLTab(true);
    setTables([]);
    fetch(`/api/db/${db}/tables?schema=${encodeURIComponent(schema)}`)
      .then(r => r.json())
      .then(d => { if (d.error) setETab(d.error); else setTables(d); })
      .catch(e => setETab(String(e)))
      .finally(() => setLTab(false));
  }, [db, schema]);

  const availableTables = excludeTables
    ? tables.filter(t => !excludeTables.has(t))
    : tables;

  const prevTables = useRef<string[]>([]);
  useEffect(() => {
    if (availableTables !== prevTables.current) {
      prevTables.current = availableTables;
      if (table && !availableTables.includes(table)) onTable("");
    }
  }, [availableTables]); // eslint-disable-line

  return (
    <div style={S.row2}>
      <Field label="Схема" required error={schemaErr || eSch}>
        <SearchableSelect
          items={schemas}
          value={schema}
          onChange={onSchema}
          disabled={lSch}
          placeholder={lSch ? "Загрузка…" : "Выберите схему"}
        />
      </Field>
      <Field label="Таблица" required error={tableErr || eTab}>
        <SearchableSelect
          items={availableTables}
          value={table}
          onChange={onTable}
          disabled={!schema || lTab}
          placeholder={!schema ? "Сначала схему" : lTab ? "Загрузка…" : "Выберите таблицу"}
        />
      </Field>
    </div>
  );
}
