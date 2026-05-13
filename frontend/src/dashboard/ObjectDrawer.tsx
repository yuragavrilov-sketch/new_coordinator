import React from "react";
import { t } from "../theme";
import {
  Icon, ObjStatusBadge, ProgressBar,
} from "../components/ui";
import { useApi } from "../hooks/useApi";
import { fmtCompactNum, fmtMb } from "../utils/format";
import { STATUS_MAP, OBJECT_TYPES, type SchemaObject, type MigrationEvent } from "./types";
import type { ObjectDetailResp, DdlDetailResp, MigrationDetailResp } from "./api";
import { DiffSections } from "./DiffSections";

interface Props {
  schemaMigrationId: string;
  object:            SchemaObject;
  events:            MigrationEvent[];
  onClose:           () => void;
  onAction:          (o: SchemaObject, action: "pause" | "retry" | "rollback") => void;
}

export function ObjectDrawer({ schemaMigrationId, object: o, events, onClose, onAction }: Props) {
  const detail = useApi<ObjectDetailResp>(
    `/api/schema-migrations/${schemaMigrationId}/objects/${encodeURIComponent(o.id)}/detail`,
  );
  const status = o.status;
  const tone =
    status === "error" ? "error" :
    status === "warn"  ? "warn"  :
    status === "done"  ? "ok"    :
                         "info";
  const objEvents = events.filter(e => e.obj === o.name);
  const hasIssues = o.err > 0 || o.warn > 0;

  return (
    <div
      onClick={onClose}
      style={{
        position: "fixed", inset: 0,
        background: "rgba(20,20,20,0.18)",
        backdropFilter: "blur(2px)",
        zIndex: 50,
        animation: "fadeIn 140ms ease-out",
      }}
    >
      <div
        onClick={e => e.stopPropagation()}
        style={{
          position: "absolute", top: 0, right: 0,
          height: "100vh",
          width: "min(860px, 92vw)",
          background: t.bg.s1,
          borderLeft: `1px solid ${t.border.subtle}`,
          boxShadow: t.shadow.s3,
          display: "flex", flexDirection: "column",
          animation: "slideIn 200ms cubic-bezier(.22,.61,.36,1)",
        }}
      >
        {/* Head */}
        <div style={{
          display: "flex", justifyContent: "space-between", alignItems: "flex-start",
          gap: 16, padding: "20px 26px 16px",
          borderBottom: `1px solid ${t.border.subtle}`,
        }}>
          <div>
            <div style={{ display: "flex", gap: 6, alignItems: "center", marginBottom: 8, flexWrap: "wrap" }}>
              <span style={{
                fontFamily: t.font.mono,
                fontSize: "9.5px", fontWeight: 600,
                letterSpacing: "0.05em", textTransform: "uppercase",
                padding: "2px 6px", borderRadius: 3,
                background: t.bg.s3, color: t.text.secondary,
              }}>
                {OBJECT_TYPES[o.type].label}
              </span>
              <ObjStatusBadge tone={STATUS_MAP[status].tone} label={STATUS_MAP[status].label}/>
              {o.err > 0 && (
                <span style={{
                  display: "inline-flex", alignItems: "center", gap: 3,
                  fontSize: "10.5px", padding: "1px 5px", borderRadius: 3,
                  fontFamily: t.font.mono,
                  background: t.tone.errorSoft, color: t.tone.error,
                }}>
                  <Icon name="error" size={11}/> {o.err}
                </span>
              )}
              {o.warn > 0 && (
                <span style={{
                  display: "inline-flex", alignItems: "center", gap: 3,
                  fontSize: "10.5px", padding: "1px 5px", borderRadius: 3,
                  fontFamily: t.font.mono,
                  background: t.tone.warnSoft, color: t.tone.warn,
                }}>
                  <Icon name="warn" size={11}/> {o.warn}
                </span>
              )}
            </div>
            <h2 style={{
              margin: 0, fontSize: 20, fontWeight: 600,
              letterSpacing: "-0.02em",
              fontFamily: t.font.mono,
            }}>
              {o.name}
            </h2>
            <div style={{
              display: "flex", gap: 6, alignItems: "center",
              fontSize: 12, marginTop: 6,
              fontFamily: t.font.mono, color: t.text.muted,
            }}>
              <span>BILLING</span>
              <Icon name="arrow" size={14}/>
              <span>BILL19</span>
            </div>
          </div>
          <div style={{ display: "flex", gap: 4, alignItems: "center", flexShrink: 0 }}>
            {status === "running" && (
              <ActionBtn icon="pause"  label="Пауза"     onClick={() => onAction(o, "pause")}/>
            )}
            {status === "error" && (
              <ActionBtn icon="rotate" label="Повторить" primary onClick={() => onAction(o, "retry")}/>
            )}
            <ActionBtn icon="rotate" onClick={() => onAction(o, "rollback")}/>
            <ActionBtn icon="close"  onClick={onClose}/>
          </div>
        </div>

        {/* Body */}
        <div style={{
          flex: 1, overflowY: "auto",
          padding: "16px 26px 28px",
          display: "flex", flexDirection: "column", gap: 16,
        }}>
          {/* Stats strip */}
          <div style={{
            display: "grid", gridTemplateColumns: "repeat(5, minmax(0,1fr))", gap: 10,
          }}>
            <DStat label="Прогресс" value={`${o.progress.toFixed(1)}%`}>
              <ProgressBar value={o.progress} tone={tone} height={4} style={{ marginTop: 6 }}/>
            </DStat>
            {o.rows != null ? (
              <DStat
                label="Rows"
                value={fmtCompactNum(o.rowsDone)}
                sub={`из ${fmtCompactNum(o.rows)}`}
              />
            ) : (
              <DStat label="Совместимость" value={`${o.compat}%`} sub="по PL/SQL"/>
            )}
            <DStat label="Размер" value={o.sizeMb < 0.1 ? "—" : fmtMb(o.sizeMb)}/>
            <DStat
              label="Скорость"
              value={o.rowsPerSec > 0 ? `${fmtCompactNum(o.rowsPerSec)}/s` : o.mbPerSec > 0 ? `${o.mbPerSec} MB/s` : "—"}
            />
            <DStat label="ETA / Длит." value={o.eta} sub={o.dur}/>
          </div>

          {/* Issue callout */}
          {hasIssues && o.note && (
            <div style={{
              display: "grid",
              gridTemplateColumns: "auto 1fr auto",
              gap: 12, alignItems: "center",
              padding: "12px 14px",
              borderRadius: t.radius.lg,
              border: `1px solid color-mix(in oklab, ${o.err > 0 ? t.tone.error : t.tone.warn} 30%, transparent)`,
              background: o.err > 0 ? t.tone.errorSoft : t.tone.warnSoft,
              color: o.err > 0 ? t.tone.error : t.tone.warn,
            }}>
              <Icon name={o.err > 0 ? "error" : "warn"} size={14}/>
              <div>
                <div style={{
                  fontSize: 11, fontWeight: 700,
                  textTransform: "uppercase", letterSpacing: "0.06em",
                  marginBottom: 2,
                }}>
                  {o.err > 0 ? "Ошибка" : "Предупреждение"}
                </div>
                <div style={{
                  fontSize: "12.5px",
                  color: t.text.primary,
                  fontFamily: t.font.mono,
                  lineHeight: 1.4,
                }}>
                  {o.note}
                </div>
              </div>
              <div style={{ display: "flex", gap: 6 }}>
                {status === "error" && (
                  <ActionBtn icon="rotate" label="Повторить" primary onClick={() => onAction(o, "retry")}/>
                )}
                <ActionBtn icon="check" label="Подтвердить" onClick={() => onAction(o, "pause")}/>
              </div>
            </div>
          )}

          {/* Diff sections — populated from /objects/:id/detail */}
          {detail.loading && (
            <div style={{ fontSize: 12, color: t.text.muted }}>загружаем детали…</div>
          )}
          {detail.error && (
            <div style={{
              padding: "10px 12px", fontSize: 12,
              background: t.tone.errorSoft, color: t.tone.error,
              borderRadius: t.radius.sm,
            }}>
              Не удалось загрузить детали: {detail.error}
            </div>
          )}
          {detail.data && detail.data.kind === "ddl" && (
            <DiffSections detail={detail.data as DdlDetailResp}/>
          )}
          {detail.data && detail.data.kind === "migration" && (
            <MigrationDetailBlock data={detail.data as MigrationDetailResp}/>
          )}

          {/* Event log */}
          <div style={{
            background: t.bg.s1,
            border: `1px solid ${t.border.subtle}`,
            borderRadius: t.radius.lg,
            padding: 14,
          }}>
            <div style={{
              display: "flex", justifyContent: "space-between", alignItems: "baseline",
              marginBottom: 10, gap: 8,
            }}>
              <span style={{ fontSize: "12.5px", fontWeight: 600 }}>События</span>
              <span style={{ fontSize: 11, color: t.text.muted }}>{objEvents.length}</span>
            </div>
            <div style={{
              display: "flex", flexDirection: "column", gap: 1,
              background: t.bg.s2,
              borderRadius: t.radius.sm,
              padding: 6, maxHeight: 280, overflowY: "auto",
              fontFamily: t.font.mono,
            }}>
              {objEvents.length === 0 && (
                <div style={{ padding: 16, textAlign: "center", fontSize: 11, color: t.text.muted }}>
                  Нет событий по этому объекту
                </div>
              )}
              {objEvents.map((e, i) => (
                <div key={i} style={{
                  display: "grid",
                  gridTemplateColumns: "56px 44px 1fr",
                  gap: 8, padding: "4px 6px",
                  borderRadius: 3, fontSize: 11,
                  alignItems: "baseline",
                }}>
                  <span style={{ fontSize: 10, color: t.text.muted }}>{e.t}</span>
                  <span style={{
                    fontSize: "9.5px", fontWeight: 600, letterSpacing: "0.04em",
                    textTransform: "uppercase",
                    color: e.level === "error" ? t.tone.error : e.level === "warn" ? t.tone.warn : t.tone.info,
                  }}>{e.level}</span>
                  <span style={{ color: t.text.secondary, wordBreak: "break-word" }}>{e.msg}</span>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

function MigrationDetailBlock({ data }: { data: MigrationDetailResp }) {
  if (!data.found) {
    return (
      <div style={{
        background: t.bg.s1,
        border: `1px solid ${t.border.subtle}`,
        borderRadius: t.radius.lg,
        padding: 14, fontSize: 12, color: t.text.muted,
      }}>
        Миграция не найдена в БД.
      </div>
    );
  }
  const m = (data.migration || {}) as Record<string, unknown>;
  const errText      = m.error_text   as string | null | undefined;
  const failedPhase  = m.failed_phase as string | null | undefined;
  const errorCode    = m.error_code   as string | null | undefined;
  const phase        = m.phase        as string | undefined;
  const totalChunks  = m.total_chunks as number | null | undefined;
  const chunksDone   = m.chunks_done  as number | null | undefined;
  const chunksFailed = m.chunks_failed as number | null | undefined;
  const ddlDiff      = data.ddl_diff;

  return (
    <>
      {errText && (
        <div style={{
          background: t.bg.s1,
          border: `1px solid ${t.border.subtle}`,
          borderRadius: t.radius.lg,
          padding: 14,
        }}>
          <div style={{ fontSize: "12.5px", fontWeight: 600, marginBottom: 8 }}>
            Текущая ошибка
          </div>
          <div style={{ display: "flex", gap: 12, fontSize: 11, color: t.text.muted, marginBottom: 6 }}>
            {errorCode && <span>Код: <span style={{ fontFamily: t.font.mono, color: t.tone.error }}>{errorCode}</span></span>}
            {failedPhase && <span>Фаза: <span style={{ fontFamily: t.font.mono, color: t.text.primary }}>{failedPhase}</span></span>}
            {phase && <span>Текущая: <span style={{ fontFamily: t.font.mono, color: t.text.primary }}>{phase}</span></span>}
          </div>
          <pre style={{
            margin: 0, fontFamily: t.font.mono, fontSize: "11.5px",
            lineHeight: 1.5, padding: "10px 12px",
            background: t.tone.errorSoft, color: t.tone.error,
            border: `1px solid color-mix(in oklab, ${t.tone.error} 26%, transparent)`,
            borderRadius: t.radius.sm,
            whiteSpace: "pre-wrap", wordBreak: "break-word",
          }}>{errText}</pre>
        </div>
      )}

      {((totalChunks || 0) > 0 || (chunksFailed || 0) > 0) && (
        <div style={{
          background: t.bg.s1,
          border: `1px solid ${t.border.subtle}`,
          borderRadius: t.radius.lg,
          padding: 14,
        }}>
          <div style={{ fontSize: "12.5px", fontWeight: 600, marginBottom: 8 }}>
            Чанки
          </div>
          <div style={{ display: "flex", gap: 16, fontSize: 12 }}>
            <span style={{ color: t.text.muted }}>
              Всего: <span style={{ fontFamily: t.font.mono, color: t.text.primary }}>{totalChunks ?? 0}</span>
            </span>
            <span style={{ color: t.text.muted }}>
              Готово: <span style={{ fontFamily: t.font.mono, color: t.tone.ok }}>{chunksDone ?? 0}</span>
            </span>
            {(chunksFailed || 0) > 0 && (
              <span style={{ color: t.text.muted }}>
                Упало: <span style={{ fontFamily: t.font.mono, color: t.tone.error }}>{chunksFailed}</span>
              </span>
            )}
          </div>
        </div>
      )}

      {ddlDiff && ddlDiff.found && ddlDiff.match_status !== "MATCH" && (
        <DiffSections detail={ddlDiff}/>
      )}
    </>
  );
}

function DStat({ label, value, sub, children }: {
  label: string;
  value: React.ReactNode;
  sub?:  string;
  children?: React.ReactNode;
}) {
  return (
    <div style={{
      background: t.bg.s2,
      border: `1px solid ${t.border.subtle}`,
      borderRadius: t.radius.lg,
      padding: "10px 12px",
      display: "flex", flexDirection: "column", gap: 3,
    }}>
      <span style={{
        fontSize: 10, color: t.text.muted,
        textTransform: "uppercase", letterSpacing: "0.06em",
        fontWeight: 600,
      }}>{label}</span>
      <span style={{ fontSize: 16, fontWeight: 600, letterSpacing: "-0.015em", fontFamily: t.font.mono }}>
        {value}
      </span>
      {sub && <span style={{ fontSize: "10.5px", color: t.text.muted, fontFamily: t.font.mono }}>{sub}</span>}
      {children}
    </div>
  );
}

function ActionBtn({ icon, label, onClick, primary }: {
  icon:     "pause" | "rotate" | "close" | "check";
  label?:   string;
  onClick:  () => void;
  primary?: boolean;
}) {
  const isIconOnly = !label;
  return (
    <button onClick={onClick} style={{
      display: "inline-flex", alignItems: "center", gap: 6,
      padding: isIconOnly ? 6 : "5px 10px",
      borderRadius: t.radius.sm,
      fontSize: "12px",
      cursor: "pointer",
      background: primary ? t.text.primary : (isIconOnly ? "transparent" : t.bg.s1),
      color:      primary ? t.text.inverse : t.text.secondary,
      border:     `1px solid ${primary ? t.text.primary : (isIconOnly ? "transparent" : t.border.subtle)}`,
    }}>
      <Icon name={icon} size={14}/>
      {label && <span>{label}</span>}
    </button>
  );
}
