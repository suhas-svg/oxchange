import { clock, price, qty } from "../lib/format";
import { useTrades } from "../lib/hooks";
import { cx, Empty, Panel } from "./ui";

export function TradeTape({ symbol, className }: { symbol: string; className?: string }) {
  const trades = useTrades(symbol, 60);
  return (
    <Panel title="Trades" className={className} bodyClassName="flex flex-col">
      <div className="grid grid-cols-3 border-b border-line px-3 py-1.5 text-right text-[11px] text-ink-3">
        <span className="text-left">Price</span>
        <span>Shares</span>
        <span>Time</span>
      </div>
      <div className="scroll-thin min-h-0 flex-1 overflow-y-auto">
        {trades.length === 0 && <Empty>No trades in {symbol} yet.</Empty>}
        {trades.map((t, i) => (
          <div
            key={t.seq}
            // Only the newest row flashes, so a burst doesn't light up the whole list.
            className={cx(
              "grid h-[22px] grid-cols-3 items-center px-3 text-right text-xs",
              i === 0 && (t.side === "BUY" ? "flash-up" : "flash-down"),
            )}
          >
            <span className={cx("num text-left", t.side === "BUY" ? "text-up" : "text-down")}>{price(t.price_cents)}</span>
            <span className="num text-ink">{qty(t.quantity)}</span>
            <span className="num text-ink-3">{clock(t.time_ms)}</span>
          </div>
        ))}
      </div>
    </Panel>
  );
}
