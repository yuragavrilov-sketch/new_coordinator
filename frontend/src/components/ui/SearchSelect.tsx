import { useState, useEffect, useRef, useMemo } from "react";

interface SearchSelectProps {
  value: string;
  onChange: (v: string) => void;
  options: string[];
  placeholder?: string;
  disabled?: boolean;
  showClear?: boolean;
}

export function SearchSelect({
  value,
  onChange,
  options,
  placeholder = "Выберите...",
  disabled = false,
  showClear = true,
}: SearchSelectProps) {
  const [query, setQuery] = useState("");
  const [open, setOpen] = useState(false);
  const wrapRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (!open) return;
    const onDown = (e: MouseEvent) => {
      if (wrapRef.current && !wrapRef.current.contains(e.target as Node)) {
        setOpen(false);
        setQuery("");
      }
    };
    document.addEventListener("mousedown", onDown);
    return () => document.removeEventListener("mousedown", onDown);
  }, [open]);

  useEffect(() => {
    if (open) inputRef.current?.focus();
  }, [open]);

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    return q ? options.filter((o) => o.toLowerCase().includes(q)) : options;
  }, [options, query]);

  const handleOpen = () => {
    if (!disabled) {
      setOpen((o) => !o);
      setQuery("");
    }
  };

  const handleSelect = (opt: string) => {
    onChange(opt);
    setOpen(false);
    setQuery("");
  };

  const handleKey = (e: React.KeyboardEvent) => {
    if (e.key === "Escape") {
      setOpen(false);
      setQuery("");
    }
    if (e.key === "Enter" && filtered.length === 1) handleSelect(filtered[0]);
  };

  return (
    <div ref={wrapRef} style={{ position: "relative", minWidth: 165 }}>
      <div
        onClick={handleOpen}
        style={{
          display: "flex",
          alignItems: "center",
          gap: 6,
          background: "#1e293b",
          border: `1px solid ${open ? "#3b82f6" : "#334155"}`,
          borderRadius: 4,
          padding: "0 8px",
          height: 30,
          cursor: disabled ? "not-allowed" : "pointer",
          opacity: disabled ? 0.5 : 1,
          userSelect: "none",
        }}
      >
        <span
          style={{
            fontSize: 12,
            flex: 1,
            color: value ? "#e2e8f0" : "#475569",
            overflow: "hidden",
            textOverflow: "ellipsis",
            whiteSpace: "nowrap",
          }}
        >
          {value || placeholder}
        </span>
        <span style={{ color: "#475569", fontSize: 9, flexShrink: 0 }}>
          {open ? "▲" : "▼"}
        </span>
      </div>

      {open && (
        <div
          style={{
            position: "absolute",
            top: "calc(100% + 3px)",
            left: 0,
            right: 0,
            background: "#1e293b",
            border: "1px solid #334155",
            borderRadius: 4,
            zIndex: 200,
            boxShadow: "0 6px 20px rgba(0,0,0,0.5)",
            display: "flex",
            flexDirection: "column",
          }}
        >
          <div
            style={{
              padding: "6px 8px",
              borderBottom: "1px solid #0f1e35",
              display: "flex",
              alignItems: "center",
              gap: 6,
            }}
          >
            <span style={{ color: "#475569", fontSize: 11 }}>&#128269;</span>
            <input
              ref={inputRef}
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              onKeyDown={handleKey}
              placeholder="Поиск..."
              style={{
                background: "none",
                border: "none",
                color: "#e2e8f0",
                fontSize: 12,
                width: "100%",
                outline: "none",
              }}
            />
            {query && (
              <span
                onClick={() => setQuery("")}
                style={{
                  color: "#475569",
                  cursor: "pointer",
                  fontSize: 11,
                  flexShrink: 0,
                }}
              >
                &#10005;
              </span>
            )}
          </div>

          <div style={{ maxHeight: 200, overflowY: "auto" }}>
            {showClear && value && (
              <div
                onMouseDown={() => handleSelect("")}
                style={{
                  padding: "5px 10px",
                  fontSize: 11,
                  cursor: "pointer",
                  color: "#475569",
                  borderBottom: "1px solid #0f1e35",
                }}
                onMouseEnter={(e) =>
                  (e.currentTarget.style.background = "#0f1e35")
                }
                onMouseLeave={(e) =>
                  (e.currentTarget.style.background = "transparent")
                }
              >
                — Очистить —
              </div>
            )}
            {filtered.length === 0 ? (
              <div
                style={{ padding: "8px 10px", color: "#475569", fontSize: 12 }}
              >
                Нет совпадений
              </div>
            ) : (
              filtered.map((o) => (
                <div
                  key={o}
                  onMouseDown={() => handleSelect(o)}
                  style={{
                    padding: "6px 10px",
                    fontSize: 12,
                    cursor: "pointer",
                    background: o === value ? "#1d3a5f" : "transparent",
                    color: o === value ? "#93c5fd" : "#e2e8f0",
                  }}
                  onMouseEnter={(e) =>
                    (e.currentTarget.style.background =
                      o === value ? "#1d3a5f" : "#0f1624")
                  }
                  onMouseLeave={(e) =>
                    (e.currentTarget.style.background =
                      o === value ? "#1d3a5f" : "transparent")
                  }
                >
                  {o}
                </div>
              ))
            )}
          </div>
        </div>
      )}
    </div>
  );
}
