import { useEffect, useRef, useState } from "react";

import { ago } from "../lib/format";
import { useNotifications, useNow } from "../lib/hooks";
import { describe, type Tone } from "../lib/notify";
import { cx, Empty } from "./ui";

const dot: Record<Tone, string> = { up: "bg-up", down: "bg-down", info: "bg-accent", warn: "bg-warn" };
const SEEN_KEY = "oxchange.seen.v1";

function readSeen(): number {
  try {
    return Number(localStorage.getItem(SEEN_KEY) ?? 0) || 0;
  } catch {
    return 0;
  }
}

export function NotificationBell() {
  const all = useNotifications();
  const now = useNow(15_000);
  const [open, setOpen] = useState(false);
  const [seen, setSeen] = useState(readSeen);
  const ref = useRef<HTMLDivElement>(null);
  const newest = all.length ? all[all.length - 1]!.id : 0;
  const unread = all.filter((n) => n.id > seen).length;

  useEffect(() => {
    if (!open) return;
    const close = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    };
    const esc = (e: KeyboardEvent) => e.key === "Escape" && setOpen(false);
    document.addEventListener("mousedown", close);
    document.addEventListener("keydown", esc);
    return () => {
      document.removeEventListener("mousedown", close);
      document.removeEventListener("keydown", esc);
    };
  }, [open]);

  function toggle() {
    if (!open && newest > seen) {
      setSeen(newest);
      try {
        localStorage.setItem(SEEN_KEY, String(newest));
      } catch {
        // Not remembered across reloads; fine.
      }
    }
    setOpen(!open);
  }

  const recent = all.slice(-40).reverse();
  return (
    <div ref={ref} className="relative">
      <button
        onClick={toggle}
        aria-label={unread ? `Notifications, ${unread} new` : "Notifications"}
        aria-expanded={open}
        className="relative grid size-9 place-items-center rounded-md text-ink-2 hover:bg-hover hover:text-ink"
      >
        <svg viewBox="0 0 20 20" className="size-[18px]" fill="none" stroke="currentColor" strokeWidth="1.6">
          <path d="M5 8a5 5 0 0 1 10 0c0 4 1.5 5.5 1.5 5.5h-13S5 12 5 8ZM8.5 16.5a1.6 1.6 0 0 0 3 0" strokeLinejoin="round" />
        </svg>
        {unread > 0 && (
          <span className="num absolute top-1 right-1 grid h-4 min-w-4 place-items-center rounded-full bg-accent px-1 text-[10px] font-semibold text-white">
            {unread > 99 ? "99+" : unread}
          </span>
        )}
      </button>
      {open && (
        <div className="absolute top-11 right-0 z-40 w-[min(360px,calc(100vw-24px))] overflow-hidden rounded-lg border border-line-strong bg-raised shadow-2xl shadow-black/50">
          <div className="border-b border-line px-3.5 py-2.5 text-[13px] font-semibold">Activity</div>
          <ul className="scroll-thin max-h-[420px] overflow-y-auto">
            {recent.length === 0 && <Empty>Fills, cancels and deposits show up here.</Empty>}
            {recent.map((n) => {
              const d = describe(n);
              return (
                <li key={n.id} className="flex gap-3 border-b border-line/60 px-3.5 py-2.5 last:border-0">
                  <span className={cx("mt-1.5 size-1.5 shrink-0 rounded-full", dot[d.tone])} />
                  <div className="min-w-0 flex-1">
                    <p className="text-[13px] text-ink">{d.title}</p>
                    <p className="mt-0.5 text-xs text-ink-3">{d.detail}</p>
                  </div>
                  <span className="shrink-0 text-[11px] text-ink-3">{ago(n.created_at_ms, now)}</span>
                </li>
              );
            })}
          </ul>
        </div>
      )}
    </div>
  );
}
