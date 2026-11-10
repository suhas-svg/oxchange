import { useMemo, useState } from "react";
import { useNavigate } from "react-router";

import { Badge, cx, Empty, Panel, Spinner, inputClass } from "../components/ui";
import { ago, change, percent, price, volume } from "../lib/format";
import { useNow, useSymbols, useTickers, type Tick } from "../lib/hooks";
import type { SymbolInfo } from "../lib/types";

type SortKey = "symbol" | "last" | "change" | "volume";

interface Row extends SymbolInfo {
  last: number | null;
  vol: number;
  pct: number | null;
  tick?: Tick;
}

function Mover({ title, rows, tone }: { title: string; rows: Row[]; tone: "up" | "down" }) {
  const navigate = useNavigate();
  return (
    <Panel title={title}>
      {rows.length === 0 ? (
        <Empty>No trades yet today.</Empty>
      ) : (
        <ul>
          {rows.map((r) => (
            <li key={r.symbol}>
              <button
                onClick={() => navigate(`/trade/${r.symbol}`)}
                className="flex w-full items-center justify-between px-3 py-2 text-left hover:bg-hover"
              >
                <span className="text-[13px] font-semibold">{r.symbol}</span>
                <span className="flex items-center gap-3">
                  <span className="num text-[13px] text-ink-2">{price(r.last)}</span>
                  <span className={cx("num w-16 text-right text-[13px]", tone === "up" ? "text-up" : "text-down")}>
                    {percent(r.pct)}
                  </span>
                </span>
              </button>
            </li>
          ))}
        </ul>
      )}
    </Panel>
  );
}

export function MarketsPage() {
  const navigate = useNavigate();
  const symbols = useSymbols();
  const list = symbols.data ?? [];
  const ticks = useTickers(list.map((s) => s.symbol));
  const now = useNow(5_000);
  const [query, setQuery] = useState("");
  const [sort, setSort] = useState<{ key: SortKey; desc: boolean }>({ key: "symbol", desc: false });

  const rows: Row[] = useMemo(
    () =>
      list.map((s) => {
        const t = ticks[s.symbol];
        const last = t?.last_price_cents ?? s.last_price_cents;
        return { ...s, last, vol: t?.day_volume ?? s.day_volume, pct: change(s.listing_price_cents, last), tick: t };
      }),
    [list, ticks],
  );

  const shown = useMemo(() => {
    const q = query.trim().toLowerCase();
    const f = q ? rows.filter((r) => r.symbol.toLowerCase().includes(q) || r.name.toLowerCase().includes(q)) : rows;
    const val = (r: Row): number | string =>
      sort.key === "symbol" ? r.symbol : sort.key === "last" ? (r.last ?? -1) : sort.key === "change" ? (r.pct ?? -Infinity) : r.vol;
    return [...f].sort((a, b) => {
      const x = val(a);
      const y = val(b);
      const c = x < y ? -1 : x > y ? 1 : 0;
      return sort.desc ? -c : c;
    });
  }, [rows, query, sort]);

  const traded = rows.filter((r) => r.pct != null);
  const gainers = [...traded].filter((r) => r.pct! > 0).sort((a, b) => b.pct! - a.pct!).slice(0, 4);
  const losers = [...traded].filter((r) => r.pct! < 0).sort((a, b) => a.pct! - b.pct!).slice(0, 4);
  const active = [...rows].filter((r) => r.vol > 0).sort((a, b) => b.vol - a.vol).slice(0, 4);
  const totalVolume = rows.reduce((n, r) => n + r.vol, 0);

  function header(key: SortKey, label: string, align = "text-right") {
    const on = sort.key === key;
    return (
      <th className={cx("px-3 py-2 font-medium", align)}>
        <button
          className={cx("inline-flex items-center gap-1 hover:text-ink-2", on && "text-ink-2")}
          onClick={() => setSort({ key, desc: on ? !sort.desc : key !== "symbol" })}
        >
          {label}
          <span className="w-2 text-[9px]">{on ? (sort.desc ? "▼" : "▲") : ""}</span>
        </button>
      </th>
    );
  }

  return (
    <div className="scroll-thin h-full overflow-y-auto">
      <div className="mx-auto max-w-6xl space-y-4 p-4 sm:p-6">
        <div className="flex flex-wrap items-end justify-between gap-3">
          <div>
            <h1 className="text-xl font-semibold tracking-tight">Markets</h1>
            <p className="mt-1 text-sm text-ink-3">
              {list.length} simulated stocks · <span className="num">{volume(totalVolume)}</span> shares traded today. Change is
              measured from the listing price.
            </p>
          </div>
          <input
            className={cx(inputClass, "h-9 w-full sm:w-64")}
            placeholder="Search stocks"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            aria-label="Search stocks"
          />
        </div>

        <div className="grid gap-4 md:grid-cols-3">
          <Mover title="Top gainers" rows={gainers} tone="up" />
          <Mover title="Top losers" rows={losers} tone="down" />
          <Panel title="Most active">
            {active.length === 0 ? (
              <Empty>No trades yet today.</Empty>
            ) : (
              <ul>
                {active.map((r) => (
                  <li key={r.symbol}>
                    <button
                      onClick={() => navigate(`/trade/${r.symbol}`)}
                      className="flex w-full items-center justify-between px-3 py-2 text-left hover:bg-hover"
                    >
                      <span className="text-[13px] font-semibold">{r.symbol}</span>
                      <span className="num text-[13px] text-ink-2">{volume(r.vol)} sh</span>
                    </button>
                  </li>
                ))}
              </ul>
            )}
          </Panel>
        </div>

        <Panel bodyClassName="overflow-x-auto">
          {symbols.isLoading ? (
            <div className="flex h-60 items-center justify-center text-ink-3">
              <Spinner />
            </div>
          ) : symbols.isError ? (
            <Empty>Couldn't load the stock list. {String((symbols.error as Error).message)}</Empty>
          ) : (
            <table className="w-full min-w-[640px] text-[13px]">
              <thead className="border-b border-line text-xs text-ink-3">
                <tr>
                  {header("symbol", "Stock", "text-left")}
                  {header("last", "Last")}
                  {header("change", "Change")}
                  {header("volume", "Volume")}
                  <th className="px-3 py-2 text-right font-medium">Last trade</th>
                  <th className="w-24 px-3 py-2" />
                </tr>
              </thead>
              <tbody>
                {shown.map((r) => {
                  // The stream sends a ticker on subscribe too, so its arrival time only
                  // counts as a trade time once the volume has grown since the page loaded.
                  const lastAt =
                    r.tick && r.tick.day_volume > r.day_volume
                      ? r.tick.at
                      : r.last_trade_at
                        ? Date.parse(r.last_trade_at)
                        : null;
                  return (
                    <tr
                      key={r.symbol}
                      onClick={() => navigate(`/trade/${r.symbol}`)}
                      className="group cursor-pointer border-b border-line/60 last:border-0 hover:bg-hover"
                    >
                      <td className="px-3 py-2.5">
                        <div className="flex items-center gap-3">
                          <span className="grid size-8 place-items-center rounded-md bg-raised text-[10px] font-bold text-ink-2">
                            {r.symbol.slice(0, 2)}
                          </span>
                          <div>
                            <div className="flex items-center gap-2 font-semibold">
                              {r.symbol}
                              {!r.trading && <Badge tone="warn">Halted</Badge>}
                            </div>
                            <div className="text-xs text-ink-3">{r.name}</div>
                          </div>
                        </div>
                      </td>
                      <td className="px-3 py-2.5 text-right">
                        <span
                          key={r.tick?.at}
                          className={cx(
                            "num rounded px-1 py-0.5",
                            r.tick?.move === 1 && "flash-up",
                            r.tick?.move === -1 && "flash-down",
                          )}
                        >
                          {r.last == null ? <span className="text-ink-3">{price(r.listing_price_cents)}</span> : price(r.last)}
                        </span>
                      </td>
                      <td
                        className={cx(
                          "num px-3 py-2.5 text-right",
                          r.pct == null ? "text-ink-3" : r.pct > 0 ? "text-up" : r.pct < 0 ? "text-down" : "text-ink-2",
                        )}
                      >
                        {percent(r.pct)}
                      </td>
                      <td className="num px-3 py-2.5 text-right text-ink-2">{volume(r.vol)}</td>
                      <td className="px-3 py-2.5 text-right text-xs text-ink-3">{lastAt ? ago(lastAt, now) : "No trades"}</td>
                      <td className="px-3 py-2.5 text-right">
                        <span className="rounded-md border border-line-strong px-2.5 py-1 text-xs font-medium text-ink-2 opacity-0 transition group-hover:opacity-100 group-focus-within:opacity-100">
                          Trade
                        </span>
                      </td>
                    </tr>
                  );
                })}
                {shown.length === 0 && (
                  <tr>
                    <td colSpan={6}>
                      <Empty>No stock matches “{query}”.</Empty>
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          )}
        </Panel>
      </div>
    </div>
  );
}
