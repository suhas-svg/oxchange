import { useEffect, useRef, type ButtonHTMLAttributes, type ReactNode } from "react";

export function cx(...parts: (string | false | null | undefined)[]) {
  return parts.filter(Boolean).join(" ");
}

type Variant = "primary" | "buy" | "sell" | "ghost" | "outline";

export function Button({
  variant = "primary",
  size = "md",
  className,
  busy,
  children,
  ...rest
}: ButtonHTMLAttributes<HTMLButtonElement> & { variant?: Variant; size?: "sm" | "md" | "lg"; busy?: boolean }) {
  const styles: Record<Variant, string> = {
    primary: "bg-accent text-accent-ink hover:brightness-110",
    buy: "bg-up text-[#04140f] hover:brightness-110",
    sell: "bg-down text-white hover:brightness-110",
    ghost: "text-ink-2 hover:bg-hover hover:text-ink",
    outline: "border border-line-strong text-ink hover:bg-hover",
  };
  const sizes = { sm: "h-7 px-2.5 text-xs", md: "h-9 px-3.5 text-sm", lg: "h-11 px-4 text-[15px]" };
  return (
    <button
      {...rest}
      disabled={rest.disabled || busy}
      className={cx(
        "inline-flex items-center justify-center gap-2 rounded-md font-medium transition select-none",
        "disabled:cursor-not-allowed disabled:opacity-50",
        styles[variant],
        sizes[size],
        className,
      )}
    >
      {busy && <Spinner />}
      {children}
    </button>
  );
}

export function Spinner({ className }: { className?: string }) {
  return (
    <span
      aria-hidden
      className={cx("inline-block size-3.5 animate-spin rounded-full border-2 border-current border-r-transparent", className)}
    />
  );
}

export function Panel({
  title,
  actions,
  children,
  className,
  bodyClassName,
}: {
  title?: ReactNode;
  actions?: ReactNode;
  children: ReactNode;
  className?: string;
  bodyClassName?: string;
}) {
  return (
    <section className={cx("flex min-h-0 flex-col rounded-lg border border-line bg-surface", className)}>
      {(title || actions) && (
        <header className="flex h-10 shrink-0 items-center justify-between gap-2 border-b border-line px-3">
          <h2 className="text-[13px] font-semibold tracking-tight text-ink">{title}</h2>
          <div className="flex items-center gap-1">{actions}</div>
        </header>
      )}
      <div className={cx("min-h-0 flex-1", bodyClassName)}>{children}</div>
    </section>
  );
}

export function Segmented<T extends string>({
  value,
  options,
  onChange,
  className,
  size = "md",
}: {
  value: T;
  options: { value: T; label: ReactNode; activeClass?: string }[];
  onChange: (v: T) => void;
  className?: string;
  size?: "sm" | "md";
}) {
  return (
    <div role="tablist" className={cx("flex rounded-md bg-canvas p-0.5", className)}>
      {options.map((o) => {
        const active = o.value === value;
        return (
          <button
            key={o.value}
            role="tab"
            type="button"
            aria-selected={active}
            onClick={() => onChange(o.value)}
            className={cx(
              "flex-1 rounded-[5px] font-medium whitespace-nowrap transition",
              size === "sm" ? "h-6 px-2 text-xs" : "h-8 px-3 text-sm",
              active ? (o.activeClass ?? "bg-raised text-ink shadow-sm") : "text-ink-3 hover:text-ink-2",
            )}
          >
            {o.label}
          </button>
        );
      })}
    </div>
  );
}

export function Badge({ tone = "muted", children }: { tone?: "muted" | "up" | "down" | "warn" | "accent"; children: ReactNode }) {
  const tones = {
    muted: "bg-raised text-ink-2",
    up: "bg-up-soft text-up",
    down: "bg-down-soft text-down",
    warn: "bg-warn-soft text-warn",
    accent: "bg-[rgb(79_140_255/0.14)] text-accent",
  };
  return (
    <span className={cx("inline-flex h-5 items-center rounded px-1.5 text-[11px] font-medium whitespace-nowrap", tones[tone])}>
      {children}
    </span>
  );
}

export function Dot({ tone }: { tone: "up" | "warn" | "down" | "muted" }) {
  const c = { up: "bg-up", warn: "bg-warn", down: "bg-down", muted: "bg-ink-3" }[tone];
  return (
    <span className="relative inline-flex size-2">
      {tone === "up" && <span className={cx("absolute inline-flex size-full animate-ping rounded-full opacity-40", c)} />}
      <span className={cx("relative inline-flex size-2 rounded-full", c)} />
    </span>
  );
}

export function Modal({ open, onClose, title, children }: { open: boolean; onClose: () => void; title: string; children: ReactNode }) {
  const ref = useRef<HTMLDialogElement>(null);
  useEffect(() => {
    const d = ref.current;
    if (!d) return;
    if (open && !d.open) d.showModal();
    if (!open && d.open) d.close();
  }, [open]);
  return (
    <dialog
      ref={ref}
      onClose={onClose}
      onClick={(e) => {
        if (e.target === ref.current) onClose();
      }}
      className="m-auto w-[min(420px,calc(100vw-32px))] rounded-xl border border-line-strong bg-surface p-0 text-ink shadow-2xl backdrop:bg-black/60"
    >
      <div className="flex items-center justify-between border-b border-line px-5 py-3.5">
        <h2 className="text-[15px] font-semibold">{title}</h2>
        <button onClick={onClose} className="rounded p-1 text-ink-3 hover:bg-hover hover:text-ink" aria-label="Close">
          <svg viewBox="0 0 16 16" className="size-4" fill="none" stroke="currentColor" strokeWidth="1.6">
            <path d="M4 4l8 8M12 4l-8 8" />
          </svg>
        </button>
      </div>
      <div className="p-5">{children}</div>
    </dialog>
  );
}

export function Field({ label, hint, children }: { label: string; hint?: ReactNode; children: ReactNode }) {
  return (
    <label className="block">
      <span className="mb-1.5 flex items-baseline justify-between text-xs text-ink-3">
        <span className="font-medium text-ink-2">{label}</span>
        {hint}
      </span>
      {children}
    </label>
  );
}

export const inputClass =
  "h-10 w-full rounded-md border border-line-strong bg-canvas px-3 text-sm text-ink placeholder:text-ink-3 outline-none transition focus:border-accent";

export function Empty({ children }: { children: ReactNode }) {
  return <div className="flex h-full min-h-20 items-center justify-center px-4 py-6 text-center text-xs text-ink-3">{children}</div>;
}

export function Logo({ className }: { className?: string }) {
  return (
    <span className={cx("inline-flex items-center gap-2", className)}>
      <svg viewBox="0 0 32 32" className="size-6" aria-hidden>
        <rect width="32" height="32" rx="7" fill="#151b23" />
        <circle cx="16" cy="16" r="9" fill="none" stroke="#4f8cff" strokeWidth="3" />
        <path d="M11 21 21 11" stroke="#22c39a" strokeWidth="3" strokeLinecap="round" />
      </svg>
      <span className="text-[15px] font-semibold tracking-tight">Oxchange</span>
    </span>
  );
}
