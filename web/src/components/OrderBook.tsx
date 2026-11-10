import { useMemo } from "react";

import { best, levels, type Book, type Row } from "../lib/book";
import { price, qty } from "../lib/format";
import type { Side } from "../lib/types";
import { cx, Empty, Panel, Spinner } from "./ui";

const DEPTH = 12;

function Side_({
  rows,
  kind,
  max,
  onPick,
}: {
  rows: Row[];
  kind: "bids" | "asks";
  max: number;
  onPick: (priceCents: number, side: Side) => void;
}) {
  // Asks print best-last so the two best prices meet at the spread.
  const shown = kind === "asks" ? [...rows].reverse() : rows;
  return (
    <div className={cx("flex flex-col", kind === "asks" && "justify-end")}>
      {shown.map((r) => (
        <button
          key={r.price_cents}
          // Clicking an ask fills a buy ticket at that price, and a bid a sell.
          onClick={() => onPick(r.price_cents, kind === "asks" ? "BUY" : "SELL")}
          className="group relative grid h-[22px] grid-cols-3 items-center px-3 text-right text-xs hover:bg-hover"
          title={`${kind === "asks" ? "Buy" : "Sell"} at ${price(r.price_cents)}`}
        >
          <span
            aria-hidden
            className={cx("absolute inset-y-px right-0", kind === "asks" ? "bg-down-soft" : "bg-up-soft")}
            style={{ width: `${max ? (r.total / max) * 100 : 0}%` }}
          />
          <span className={cx("num relative text-left", kind === "asks" ? "text-down" : "text-up")}>{price(r.price_cents)}</span>
          <span className="num relative text-ink">{qty(r.quantity)}</span>
          <span className="num relative text-ink-3">{qty(r.total)}</span>
        </button>
      ))}
    </div>
  );
}

export function OrderBook({
  book,
  lastCents,
  onPick,
  className,
}: {
  book: Book | null;
  lastCents: number | null;
  onPick: (priceCents: number, side: Side) => void;
  className?: string;
}) {
  const { bids, asks, spread, max } = useMemo(() => {
    if (!book) return { bids: [], asks: [], spread: null, max: 0 };
    const b = levels(book.bids, "bids", DEPTH);
    const a = levels(book.asks, "asks", DEPTH);
    const { bid, ask } = best(book);
    return {
      bids: b,
      asks: a,
      spread: bid != null && ask != null ? ask - bid : null,
      max: Math.max(b.at(-1)?.total ?? 0, a.at(-1)?.total ?? 0),
    };
  }, [book]);

  return (
    <Panel title="Order book" className={className} bodyClassName="flex flex-col">
      <div className="grid grid-cols-3 border-b border-line px-3 py-1.5 text-right text-[11px] text-ink-3">
        <span className="text-left">Price</span>
        <span>Shares</span>
        <span>Total</span>
      </div>
      {!book ? (
        <div className="flex flex-1 items-center justify-center text-ink-3">
          <Spinner />
        </div>
      ) : bids.length === 0 && asks.length === 0 ? (
        <Empty>No resting orders. Place a limit order to start the book.</Empty>
      ) : (
        <div className="grid min-h-0 flex-1 grid-rows-[1fr_auto_1fr] overflow-hidden">
          <div className="flex min-h-0 flex-col justify-end overflow-hidden">
            {asks.length ? <Side_ rows={asks} kind="asks" max={max} onPick={onPick} /> : <Empty>No sellers</Empty>}
          </div>
          <div className="flex items-center justify-between border-y border-line bg-canvas/60 px-3 py-1.5">
            <span className="num text-[15px] font-semibold">{price(lastCents)}</span>
            <span className="text-[11px] text-ink-3">
              Spread <span className="num text-ink-2">{spread == null ? "—" : price(spread)}</span>
            </span>
          </div>
          <div className="min-h-0 overflow-hidden">
            {bids.length ? <Side_ rows={bids} kind="bids" max={max} onPick={onPick} /> : <Empty>No buyers</Empty>}
          </div>
        </div>
      )}
    </Panel>
  );
}
