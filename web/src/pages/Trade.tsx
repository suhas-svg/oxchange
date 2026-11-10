import { useEffect, useMemo, useRef, useState } from "react";
import { Navigate, useNavigate, useParams } from "react-router";

import { rememberSymbol } from "../components/AppShell";
import { BalancesPanel } from "../components/BalancesPanel";
import { openFunds } from "../components/DepositDialog";
import { OrderBook } from "../components/OrderBook";
import { OrdersPanel } from "../components/OrdersPanel";
import { OrderTicket, type Pick } from "../components/OrderTicket";
import { PriceChart } from "../components/PriceChart";
import { TradeTape } from "../components/TradeTape";
import { Badge, cx } from "../components/ui";
import { best } from "../lib/book";
import { change, percent, price, volume } from "../lib/format";
import { useBook, useSymbols, useTicker, useTickers } from "../lib/hooks";
import type { SymbolInfo } from "../lib/types";

function SymbolPicker({ current, list }: { current: SymbolInfo | undefined; list: SymbolInfo[] }) {
  const [open, setOpen] = useState(false);
  const [q, setQ] = useState("");
  const ref = useRef<HTMLDivElement>(null);
  const navigate = useNavigate();
  const ticks = useTickers(open ? list.map((s) => s.symbol) : []);

  useEffect(() => {
    if (!open) return;
    const close = (e: MouseEvent) => ref.current && !ref.current.contains(e.target as Node) && setOpen(false);
    const esc = (e: KeyboardEvent) => e.key === "Escape" && setOpen(false);
    document.addEventListener("mousedown", close);
    document.addEventListener("keydown", esc);
    return () => {
      document.removeEventListener("mousedown", close);
      document.removeEventListener("keydown", esc);
    };
  }, [open]);

  const shown = list.filter(
    (s) => !q || s.symbol.toLowerCase().includes(q.toLowerCase()) || s.name.toLowerCase().includes(q.toLowerCase()),
  );
  function go(s: string) {
    setOpen(false);
    setQ("");
    navigate(`/trade/${s}`);
  }

  return (
    <div ref={ref} className="relative">
      <button
        onClick={() => setOpen(!open)}
        aria-expanded={open}
        className="flex items-center gap-2.5 rounded-md px-2 py-1 hover:bg-hover"
      >
        <span className="text-lg font-semibold tracking-tight">{current?.symbol ?? "…"}</span>
        <span className="hidden max-w-48 truncate text-xs text-ink-3 sm:inline">{current?.name}</span>
        <svg viewBox="0 0 12 12" className="size-3 text-ink-3" fill="none" stroke="currentColor" strokeWidth="1.6">
          <path d="m3 4.5 3 3 3-3" />
        </svg>
      </button>
      {open && (
        <div className="absolute top-11 left-0 z-40 w-[min(340px,calc(100vw-24px))] overflow-hidden rounded-lg border border-line-strong bg-raised shadow-2xl shadow-black/50">
          <input
            autoFocus
            value={q}
            onChange={(e) => setQ(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && shown[0] && go(shown[0].symbol)}
            placeholder="Search stocks"
            className="h-10 w-full border-b border-line bg-transparent px-3 text-sm outline-none placeholder:text-ink-3"
          />
          <ul className="scroll-thin max-h-80 overflow-y-auto py-1">
            {shown.map((s) => {
              const last = ticks[s.symbol]?.last_price_cents ?? s.last_price_cents;
              const pct = change(s.listing_price_cents, last);
              return (
                <li key={s.symbol}>
                  <button
                    onClick={() => go(s.symbol)}
                    className={cx(
                      "flex w-full items-center gap-3 px-3 py-1.5 text-left hover:bg-hover",
                      s.symbol === current?.symbol && "bg-hover",
                    )}
                  >
                    <span className="w-12 text-[13px] font-semibold">{s.symbol}</span>
                    <span className="min-w-0 flex-1 truncate text-xs text-ink-3">{s.name}</span>
                    <span className="num text-xs text-ink-2">{price(last)}</span>
                    <span
                      className={cx(
                        "num w-14 text-right text-xs",
                        pct == null ? "text-ink-3" : pct >= 0 ? "text-up" : "text-down",
                      )}
                    >
                      {percent(pct)}
                    </span>
                  </button>
                </li>
              );
            })}
          </ul>
        </div>
      )}
    </div>
  );
}

function Stat({ label, children, className }: { label: string; children: React.ReactNode; className?: string }) {
  return (
    <div className={cx("flex flex-col", className)}>
      <span className="text-[10px] tracking-wide text-ink-3 uppercase">{label}</span>
      <span className="num text-[13px]">{children}</span>
    </div>
  );
}

export function TradePage() {
  const { symbol: raw = "" } = useParams();
  const symbol = raw.toUpperCase();
  const symbols = useSymbols();
  const list = symbols.data ?? [];
  const info = list.find((s) => s.symbol === symbol);
  const book = useBook(symbol);
  const tick = useTicker(symbol);
  const [pick, setPick] = useState<Pick | null>(null);

  useEffect(() => {
    if (info) rememberSymbol(info.symbol);
    setPick(null);
  }, [info]);

  useEffect(() => {
    const p = tick?.last_price_cents ?? info?.last_price_cents;
    document.title = info ? `${symbol}${p != null ? ` ${price(p)}` : ""} · Oxchange` : "Oxchange";
    return () => {
      document.title = "Oxchange";
    };
  }, [info, symbol, tick?.last_price_cents]);

  const { bid, ask } = useMemo(() => best(book), [book]);
  if (raw !== symbol) return <Navigate to={`/trade/${symbol}`} replace />;
  if (symbols.isSuccess && !info) return <Navigate to="/" replace />;

  const last = tick?.last_price_cents ?? info?.last_price_cents ?? null;
  const pct = change(info?.listing_price_cents, last);

  return (
    <div className="scroll-thin flex h-full flex-col overflow-y-auto xl:overflow-hidden">
      <div className="flex flex-wrap items-center gap-x-6 gap-y-2 border-b border-line px-3 py-2 sm:px-4">
        <SymbolPicker current={info} list={list} />
        <div className="flex items-baseline gap-2">
          <span
            key={tick?.at}
            className={cx(
              "num rounded px-1 text-xl font-semibold",
              tick?.move === 1 && "flash-up text-up",
              tick?.move === -1 && "flash-down text-down",
            )}
          >
            {price(last)}
          </span>
          <span className={cx("num text-[13px]", pct == null ? "text-ink-3" : pct >= 0 ? "text-up" : "text-down")}>
            {percent(pct)}
          </span>
        </div>
        <div className="flex flex-wrap gap-x-6 gap-y-1">
          <Stat label="Bid">
            <span className="text-up">{price(bid)}</span>
          </Stat>
          <Stat label="Ask">
            <span className="text-down">{price(ask)}</span>
          </Stat>
          <Stat label="Day volume">{volume(tick?.day_volume ?? info?.day_volume ?? 0)}</Stat>
          <Stat label="Listed at" className="hidden sm:flex">
            {price(info?.listing_price_cents)}
          </Stat>
        </div>
        {info && !info.trading && <Badge tone="warn">Trading halted</Badge>}
      </div>

      <div
        className={cx(
          "grid min-h-0 flex-1 gap-2 p-2",
          "grid-cols-1 md:grid-cols-2",
          "xl:grid-cols-[minmax(0,1fr)_272px_316px] xl:grid-rows-[minmax(0,1fr)_minmax(220px,36%)]",
        )}
      >
        <PriceChart symbol={symbol} className="h-[380px] md:col-span-2 xl:col-span-1 xl:h-auto" />
        <OrderBook
          book={book}
          lastCents={last}
          onPick={(p, side) => setPick({ priceCents: p, side, at: Date.now() })}
          className="h-[520px] xl:h-auto"
        />
        <OrderTicket
          symbol={symbol}
          bid={bid}
          ask={ask}
          last={last}
          pick={pick}
          className="xl:row-span-1"
        />
        <OrdersPanel symbol={symbol} className="h-[320px] md:col-span-2 xl:col-span-1 xl:h-auto" />
        <TradeTape symbol={symbol} className="h-[320px] xl:h-auto" />
        <BalancesPanel onAddFunds={() => openFunds()} className="h-[320px] xl:h-auto" />
      </div>
    </div>
  );
}
