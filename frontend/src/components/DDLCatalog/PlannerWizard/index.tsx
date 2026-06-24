import { useCallback, useEffect, useState } from "react";
import { S } from "../styles";
import type {
  Batch, BatchItem, ConnectorGroup, FKDep,
  PlanDefaults, PlanSummary, TableInfo, TableKeyEntry,
} from "./types";
import { hasCdc, type Strategy } from "../../../types/migration";
import { topoSort } from "./topoSort";
import { StepIndicator } from "./StepIndicator";
import { TableSelectionStep } from "./steps/TableSelectionStep";
import { OrderingStep }       from "./steps/OrderingStep";
import { ReviewStep }         from "./steps/ReviewStep";
import { t } from "../../../theme";

interface Props {
  selectedTables: string[];
  srcSchema:      string;
  tgtSchema:      string;
  onClose:        () => void;
}

export function PlannerWizard({ selectedTables, srcSchema, tgtSchema, onClose }: Props) {
  const [step, setStep] = useState(0);
  const [planMode, setPlanModeState] = useState<"historical" | "cdc">("historical");

  // Step 0 state
  const [defaults, setDefaults] = useState<PlanDefaults>({
    chunk_size: 50000, workers: 1, strategy: "BULK_DIRECT" as Strategy, truncate_target: true,
  });
  const [tableSettings, setTableSettings] = useState<Map<string, BatchItem>>(() => {
    const map = new Map<string, BatchItem>();
    for (const table of selectedTables) {
      map.set(table, {
        table, strategy: "BULK_DIRECT" as Strategy, truncate_target: true, chunk_size: 50000, workers: 1,
      });
    }
    return map;
  });
  const [groups,        setGroups]        = useState<ConnectorGroup[]>([]);
  const [selectedGroup, setSelectedGroup] = useState("");
  const [plans,         setPlans]         = useState<PlanSummary[]>([]);
  const [selectedPlanId, setSelectedPlanId] = useState("");

  const [tableKeyEntries, setTableKeyEntries] = useState<Map<string, TableKeyEntry>>(() => {
    const map = new Map<string, TableKeyEntry>();
    for (const table of selectedTables) {
      map.set(table, {
        tableInfo:             null,
        loadingInfo:           true,
        infoError:             "",
        effective_key_type:    "",
        effective_key_columns: [],
        selected_uk_index:     0,
      });
    }
    return map;
  });

  // Step 1
  const [batches,     setBatches]     = useState<Batch[]>([]);
  const [deps,        setDeps]        = useState<FKDep[]>([]);
  const [depsLoading, setDepsLoading] = useState(false);

  // Step 2
  const [executing,    setExecuting]    = useState(false);
  const [executeError, setExecuteError] = useState<string | null>(null);
  const [planId,       setPlanId]       = useState<string | null>(null);
  const [starting,     setStarting]     = useState(false);
  const [startError,   setStartError]   = useState<string | null>(null);

  // Load CDC packs
  useEffect(() => {
    fetch("/api/connector-groups")
      .then(r => r.ok ? r.json() : Promise.reject())
      .then((data: ConnectorGroup[]) => setGroups(data))
      .catch(() => {});
  }, []);

  useEffect(() => {
    fetch("/api/planner/plans")
      .then(r => r.ok ? r.json() : [])
      .then((data: PlanSummary[]) => setPlans(data))
      .catch(() => {});
  }, []);

  const selectedPlan = plans.find(p => String(p.plan_id) === selectedPlanId);

  // Load table info for all selected tables
  useEffect(() => {
    for (const table of selectedTables) {
      const p = `schema=${encodeURIComponent(srcSchema)}&table=${encodeURIComponent(table)}`;
      fetch(`/api/db/source/table-info?${p}`)
        .then(r => r.json())
        .then((d: TableInfo & { error?: string }) => {
          setTableKeyEntries(prev => {
            const next = new Map(prev);
            const cur = next.get(table);
            if (!cur) return next;
            if (d.error) {
              next.set(table, { ...cur, loadingInfo: false, infoError: d.error });
            } else {
              let keyType = "USER_DEFINED";
              let keyCols: string[] = [];
              if (d.pk_columns.length > 0) {
                keyType = "PRIMARY_KEY"; keyCols = d.pk_columns;
              } else if (d.uk_constraints.length > 0) {
                keyType = "UNIQUE_KEY"; keyCols = d.uk_constraints[0].columns;
              }
              next.set(table, {
                ...cur, tableInfo: d, loadingInfo: false,
                effective_key_type: keyType, effective_key_columns: keyCols,
              });
            }
            return next;
          });
        })
        .catch(e => {
          setTableKeyEntries(prev => {
            const next = new Map(prev);
            const cur = next.get(table);
            if (cur) next.set(table, { ...cur, loadingInfo: false, infoError: String(e) });
            return next;
          });
        });
    }
  }, [selectedTables, srcSchema]);

  const updateTableKeyEntry = (table: string, upd: Partial<TableKeyEntry>) => {
    setTableKeyEntries(prev => {
      const next = new Map(prev);
      const cur = next.get(table);
      if (cur) next.set(table, { ...cur, ...upd });
      return next;
    });
  };

  const updateTableSetting = (table: string, upd: Partial<BatchItem>) => {
    setTableSettings(prev => {
      const next = new Map(prev);
      const cur = next.get(table);
      if (cur) next.set(table, { ...cur, ...upd });
      return next;
    });
  };

  const setPlanMode = (mode: "historical" | "cdc") => {
    setPlanModeState(mode);
    const nextDefaults: PlanDefaults = mode === "historical"
      ? { ...defaults, strategy: "BULK_DIRECT", workers: 1, truncate_target: true }
      : { ...defaults, strategy: "CDC_STAGE", workers: Math.max(defaults.workers, 4), truncate_target: true };
    setDefaults(nextDefaults);
    if (mode === "historical") setSelectedGroup("");
    setTableSettings(prev => {
      const next = new Map(prev);
      for (const table of selectedTables) {
        const cur = next.get(table);
        next.set(table, {
          table,
          strategy: nextDefaults.strategy,
          truncate_target: nextDefaults.truncate_target,
          chunk_size: cur?.chunk_size ?? nextDefaults.chunk_size,
          workers: nextDefaults.workers,
        });
      }
      return next;
    });
  };

  // Step 0 → 1: load FK deps and build initial batches
  const initOrdering = useCallback(() => {
    if (selectedTables.length === 0) return;
    setDepsLoading(true);
    const qs = new URLSearchParams({ schema: srcSchema, tables: selectedTables.join(",") });
    fetch(`/api/planner/fk-dependencies?${qs}`)
      .then(r => r.ok ? r.json() : Promise.reject())
      .then((data: FKDep[]) => {
        setDeps(data);
        const sorted = topoSort(selectedTables, data);
        const items: BatchItem[] = sorted.map(table => {
          const ts = tableSettings.get(table);
          return ts ?? {
            table, strategy: defaults.strategy, truncate_target: defaults.truncate_target,
            chunk_size: defaults.chunk_size, workers: defaults.workers,
          };
        });
        setBatches(planMode === "historical"
          ? items.map((item, idx) => ({ id: idx + 1, items: [item] }))
          : [{ id: 1, items }]);
      })
      .catch(() => {
        setDeps([]);
        const items: BatchItem[] = selectedTables.map(table => {
          const ts = tableSettings.get(table);
          return ts ?? {
            table, strategy: defaults.strategy, truncate_target: defaults.truncate_target,
            chunk_size: defaults.chunk_size, workers: defaults.workers,
          };
        });
        setBatches(planMode === "historical"
          ? items.map((item, idx) => ({ id: idx + 1, items: [item] }))
          : [{ id: 1, items }]);
      })
      .finally(() => setDepsLoading(false));
  }, [selectedTables, srcSchema, tableSettings, defaults, planMode]);

  // Execute plan
  const doExecute = useCallback(() => {
    setExecuting(true); setExecuteError(null);
    const group = groups.find(g => g.group_name === selectedGroup);
    const hasCdcTables = batches.some(b => b.items.some(it => hasCdc(it.strategy)));
    const payload = {
      name:         `${planMode === "historical" ? "Historical bulk pack" : "CDC pack"} ${srcSchema}->${tgtSchema}`,
      src_schema: srcSchema,
      tgt_schema: tgtSchema,
      connector_group_id: hasCdcTables ? (group?.group_id ?? group?.id ?? null) : null,
      defaults: {
        chunk_size:           defaults.chunk_size,
        max_parallel_workers: defaults.workers,
        strategy:             defaults.strategy,
        truncate_target:      defaults.truncate_target,
      },
      batches: batches.map(b => ({
        order: b.id,
        tables: b.items.map(it => {
          return {
            table: it.table,
            mode: hasCdc(it.strategy) ? "CDC" : "BULK",
            overrides: {
              strategy:             it.strategy,
              truncate_target:      it.truncate_target,
              chunk_size:           it.chunk_size,
              max_parallel_workers: it.workers,
            },
          };
        }),
      })),
    };
    const endpoint = selectedPlanId
      ? `/api/planner/plans/${selectedPlanId}/items`
      : "/api/planner/execute";
    fetch(endpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    })
      .then(r => r.ok ? r.json() : r.json().then(d => Promise.reject(d.error || "Ошибка")))
      .then((data: { plan_id: string | number }) => setPlanId(String(data.plan_id ?? selectedPlanId)))
      .catch(e => setExecuteError(typeof e === "string" ? e : String(e)))
      .finally(() => setExecuting(false));
  }, [planMode, srcSchema, tgtSchema, selectedGroup, groups, defaults, batches, selectedPlanId]);

  // Start first batch
  const doStart = useCallback(() => {
    if (selectedPlanId && selectedPlan?.status === "RUNNING") {
      onClose();
      return;
    }
    if (!planId) return;
    setStarting(true); setStartError(null);
    fetch(`/api/planner/plans/${planId}/start`, { method: "POST" })
      .then(r => r.ok ? r.json() : r.json().then(d => Promise.reject(d.error || "Ошибка")))
      .then(() => { onClose(); })
      .catch(e => setStartError(typeof e === "string" ? e : String(e)))
      .finally(() => setStarting(false));
  }, [planId, selectedPlanId, selectedPlan, onClose]);

  const canNext = (): boolean => {
    if (step === 0) return true;
    if (step === 1) return batches.length > 0 && batches.some(b => b.items.length > 0);
    return false;
  };

  const goNext = () => {
    if (step === 0)      { initOrdering(); setStep(1); }
    else if (step === 1) { setStep(2); }
  };
  const goBack = () => { if (step > 0) setStep(step - 1); };

  return (
    <div style={{
      background: t.bg.app,
      border: `1px solid ${t.blue.base}`,
      borderRadius: 10,
      overflow: "hidden",
      display: "flex", flexDirection: "column",
    }}>
      <div style={{
        padding: "12px 16px",
        background: t.bg.s1,
        borderBottom: `1px solid ${t.border.subtle}`,
        display: "flex", alignItems: "center", justifyContent: "space-between",
      }}>
        <span style={{ fontSize: 14, fontWeight: 700, color: t.text.primary }}>
          Настройка миграции
          <span style={{ fontSize: 12, fontWeight: 400, color: t.text.muted, marginLeft: 8 }}>
            {selectedTables.length} таблиц
          </span>
        </span>
        <button
          onClick={onClose}
          style={{
            background: "none", border: "none", color: t.text.disabled,
            fontSize: 16, cursor: "pointer", padding: "2px 6px", lineHeight: 1,
          }}
          title="Закрыть"
        >
          ✕
        </button>
      </div>

      <div style={{ padding: 20, display: "flex", flexDirection: "column", gap: 0 }}>
        <StepIndicator current={step} />

        {executeError && (
          <div style={{
            background: `${t.red.border}22`, border: `1px solid ${t.red.border}`, borderRadius: 6,
            color: t.red.fg, padding: "8px 14px", fontSize: 12, marginBottom: 12,
          }}>
            {executeError}
          </div>
        )}
        {startError && (
          <div style={{
            background: `${t.red.border}22`, border: `1px solid ${t.red.border}`, borderRadius: 6,
            color: t.red.fg, padding: "8px 14px", fontSize: 12, marginBottom: 12,
          }}>
            {startError}
          </div>
        )}

        {step === 0 && (
          <TableSelectionStep
            selected={selectedTables}
            planMode={planMode}
            onPlanMode={setPlanMode}
            plans={plans}
            selectedPlanId={selectedPlanId}
            onSelectedPlanId={setSelectedPlanId}
            defaults={defaults} onDefaults={setDefaults}
            tableSettings={tableSettings} onTableSetting={updateTableSetting}
            groups={groups} selectedGroup={selectedGroup}
            onSelectGroup={setSelectedGroup}
            tableKeyEntries={tableKeyEntries}
            onTableKeyEntry={updateTableKeyEntry}
          />
        )}

        {step === 1 && (
          <OrderingStep
            batches={batches} onBatches={setBatches}
            deps={deps} depsLoading={depsLoading}
            planMode={planMode}
          />
        )}

        {step === 2 && (
          <ReviewStep
            srcSchema={srcSchema} tgtSchema={tgtSchema}
            selectedGroup={selectedGroup}
            planMode={planMode}
            selectedPlan={selectedPlan}
            defaults={defaults} batches={batches}
            executing={executing} onExecute={doExecute}
            planId={planId} starting={starting} onStart={doStart}
            tableKeyEntries={tableKeyEntries}
          />
        )}

        <div style={{
          display: "flex", justifyContent: "space-between", alignItems: "center",
          marginTop: 20, paddingTop: 16, borderTop: `1px solid ${t.border.subtle}`,
        }}>
          <button
            onClick={goBack}
            disabled={step === 0}
            style={{
              ...S.btnSecondary,
              opacity: step === 0 ? 0.3 : 1,
              cursor:  step === 0 ? "not-allowed" : "pointer",
            }}
          >
            Назад
          </button>
          {step < 2 && (
            <button
              onClick={goNext}
              disabled={!canNext()}
              style={{
                ...S.btnPrimary,
                opacity: canNext() ? 1 : 0.5,
                cursor:  canNext() ? "pointer" : "not-allowed",
              }}
            >
              Далее
            </button>
          )}
        </div>
      </div>
    </div>
  );
}
