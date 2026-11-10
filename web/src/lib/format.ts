// Display formatting and exact input parsing. Amounts stay integer cents
// everywhere; only display divides.

const usd = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
  minimumFractionDigits: 2,
});
const plain = new Intl.NumberFormat("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const whole = new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 });
const compact = new Intl.NumberFormat("en-US", { notation: "compact", maximumFractionDigits: 1 });

/** 123456 → "$1,234.56" */
export function money(cents: number | null | undefined): string {
  return cents == null ? "—" : usd.format(cents / 100);
}

/** 123456 → "1,234.56" (for price columns, where the $ is noise) */
export function price(cents: number | null | undefined): string {
  return cents == null ? "—" : plain.format(cents / 100);
}

export function qty(n: number | null | undefined): string {
  return n == null ? "—" : whole.format(n);
}

export function volume(n: number): string {
  return n >= 100_000 ? compact.format(n) : whole.format(n);
}

/** Change from `from` to `to` as a signed percentage, or null. */
export function change(from: number | null | undefined, to: number | null | undefined): number | null {
  if (from == null || to == null || from <= 0) return null;
  return ((to - from) / from) * 100;
}

export function percent(p: number | null): string {
  if (p == null) return "—";
  const sign = p > 0 ? "+" : "";
  return `${sign}${p.toFixed(2)}%`;
}

/**
 * Parses a dollar amount typed by a person into exact cents, or null.
 * "10" → 1000, "10.5" → 1050, "10.05" → 1005. No floating point.
 */
export function parseDollars(text: string): number | null {
  const t = text.trim().replace(/^\$/, "").replace(/,/g, "");
  const m = /^(\d{1,9})(?:\.(\d{0,2}))?$/.exec(t);
  if (!m) return null;
  const dollars = Number(m[1]);
  const frac = (m[2] ?? "").padEnd(2, "0");
  return dollars * 100 + Number(frac);
}

/** Parses a whole positive number of shares, or null. */
export function parseShares(text: string): number | null {
  const t = text.trim().replace(/,/g, "");
  if (!/^\d{1,9}$/.test(t)) return null;
  const n = Number(t);
  return n > 0 ? n : null;
}

/** Cents → the string a price input should show ("10.05"). */
export function centsToInput(cents: number): string {
  const s = String(Math.abs(cents)).padStart(3, "0");
  return `${s.slice(0, -2)}.${s.slice(-2)}`;
}

const timeFmt = new Intl.DateTimeFormat(undefined, {
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  hour12: false,
});
const dateTimeFmt = new Intl.DateTimeFormat(undefined, {
  month: "short",
  day: "numeric",
  hour: "2-digit",
  minute: "2-digit",
  hour12: false,
});

export function clock(t: number | string | Date): string {
  return timeFmt.format(new Date(t));
}

export function stamp(t: number | string | Date): string {
  return dateTimeFmt.format(new Date(t));
}

export function ago(ms: number, now = Date.now()): string {
  const s = Math.max(0, Math.round((now - ms) / 1000));
  if (s < 5) return "just now";
  if (s < 60) return `${s}s ago`;
  const m = Math.round(s / 60);
  if (m < 60) return `${m}m ago`;
  const h = Math.round(m / 60);
  if (h < 24) return `${h}h ago`;
  return stamp(ms);
}

export function titleCase(s: string): string {
  return s
    .toLowerCase()
    .split("_")
    .map((w) => (w ? w[0]!.toUpperCase() + w.slice(1) : w))
    .join(" ");
}
