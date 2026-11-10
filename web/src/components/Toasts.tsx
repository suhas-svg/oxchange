import { dismiss, useToasts, type Tone } from "../lib/notify";
import { cx } from "./ui";

const bar: Record<Tone, string> = { up: "bg-up", down: "bg-down", info: "bg-accent", warn: "bg-warn" };

export function Toasts() {
  const toasts = useToasts();
  return (
    <div aria-live="polite" className="pointer-events-none fixed right-4 bottom-4 z-50 flex w-[min(360px,calc(100vw-32px))] flex-col gap-2">
      {toasts.map((t) => (
        <div
          key={t.id}
          role="status"
          className="toast-in pointer-events-auto flex overflow-hidden rounded-lg border border-line-strong bg-raised shadow-xl shadow-black/40"
        >
          <span className={cx("w-1 shrink-0", bar[t.tone])} />
          <div className="min-w-0 flex-1 px-3 py-2.5">
            <p className="text-[13px] font-medium text-ink">{t.title}</p>
            {t.detail && <p className="mt-0.5 text-xs text-ink-3">{t.detail}</p>}
          </div>
          <button
            onClick={() => dismiss(t.id)}
            className="px-2.5 text-ink-3 hover:text-ink"
            aria-label="Dismiss"
          >
            ×
          </button>
        </div>
      ))}
    </div>
  );
}
