import React, { useEffect, useRef, useState } from "react";
import ReactDOM from "react-dom";
import { t } from "../../theme";
import { S } from "./styles";

interface Props {
  items:       string[];
  value:       string;
  onChange:    (v: string) => void;
  disabled?:   boolean;
  placeholder?: string;
}

export function SearchableSelect({
  items, value, onChange, disabled, placeholder,
}: Props) {
  const [open,   setOpen]   = useState(false);
  const [filter, setFilter] = useState("");
  const [pos,    setPos]    = useState<{ top: number; left: number; width: number }>({
    top: 0, left: 0, width: 0,
  });
  const triggerRef = useRef<HTMLDivElement>(null);
  const dropRef    = useRef<HTMLDivElement>(null);
  const inputRef   = useRef<HTMLInputElement>(null);

  const filtered = filter
    ? items.filter(i => i.toLowerCase().includes(filter.toLowerCase()))
    : items;

  // Close on click outside (check both trigger and dropdown)
  useEffect(() => {
    function onClickOutside(e: MouseEvent) {
      const target = e.target as Node;
      if (triggerRef.current?.contains(target)) return;
      if (dropRef.current?.contains(target))    return;
      setOpen(false);
    }
    document.addEventListener("mousedown", onClickOutside);
    return () => document.removeEventListener("mousedown", onClickOutside);
  }, []);

  // Position the dropdown and focus the search input when opened
  useEffect(() => {
    if (open && triggerRef.current) {
      const r = triggerRef.current.getBoundingClientRect();
      setPos({ top: r.bottom + 2, left: r.left, width: r.width });
      setTimeout(() => inputRef.current?.focus(), 0);
    }
  }, [open]);

  function pick(v: string) { onChange(v); setFilter(""); setOpen(false); }

  return (
    <>
      <div
        ref={triggerRef}
        onClick={() => { if (!disabled) setOpen(!open); }}
        style={{
          ...(disabled ? S.selectDis : S.select),
          display: "flex", alignItems: "center", gap: 6, minHeight: 33,
        }}
      >
        {value
          ? <span style={{ flex: 1 }}>{value}</span>
          : <span style={{ flex: 1, color: t.text.disabled }}>{placeholder}</span>
        }
        <span style={{ color: t.text.disabled, fontSize: 10 }}>{open ? "▲" : "▼"}</span>
      </div>
      {open && ReactDOM.createPortal(
        <div ref={dropRef} style={{
          position: "fixed", top: pos.top, left: pos.left, width: pos.width,
          zIndex: 9999,
          background:   t.bg.app,
          border:       `1px solid ${t.border.base}`,
          borderRadius: t.radius.md,
          maxHeight:    260,
          display:      "flex",
          flexDirection: "column",
          boxShadow:    "0 8px 24px rgba(0,0,0,.6)",
        }}>
          <input
            ref={inputRef}
            value={filter}
            onChange={e => setFilter(e.target.value)}
            placeholder="Поиск…"
            style={{
              ...S.input,
              borderRadius: 0,
              border:       "none",
              borderBottom: `1px solid ${t.border.subtle}`,
              padding:      "7px 10px",
            }}
          />
          <div style={{ overflowY: "auto", maxHeight: 220 }}>
            {filtered.length === 0 && (
              <div style={{ padding: "8px 10px", color: t.text.faint, fontSize: t.size.base }}>
                {filter ? "Ничего не найдено" : "Нет данных"}
              </div>
            )}
            {filtered.map(item => (
              <div
                key={item}
                onClick={() => pick(item)}
                style={{
                  padding:   "5px 10px",
                  fontSize:  t.size.md,
                  cursor:    "pointer",
                  color:     item === value ? t.blue.fg  : t.text.primary,
                  background: item === value ? t.bg.s3   : "transparent",
                }}
                onMouseEnter={e => (e.currentTarget.style.background = t.bg.s2)}
                onMouseLeave={e => (e.currentTarget.style.background = item === value ? t.bg.s3 : "transparent")}
              >
                {item}
              </div>
            ))}
          </div>
          <div style={{
            padding:    "3px 10px",
            fontSize:   t.size.xs,
            color:      t.text.disabled,
            borderTop:  `1px solid ${t.border.subtle}`,
            textAlign:  "right",
          }}>
            {filtered.length} / {items.length}
          </div>
        </div>,
        document.body,
      )}
    </>
  );
}
