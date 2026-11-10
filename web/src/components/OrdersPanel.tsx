import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { Link } from "react-router";

import { ApiError, cancelOrder } from "../lib/api";
import { ago, price, qty, stamp, titleCase } from "../lib/format";
import { useNow } from "../lib/hooks";
import { toast } from "../lib/notify";
import { useFills, useOrders } from "../lib/orders";
import { FINAL_STATUSES, type Order, type OrderStatus } from "../lib/types";
import { Badge, Button, cx, Empty, Panel, Segmented, Spinner } from "./ui";

type Tab = "open" | "history" | "fills";

const STATUS_TONE: Record<OrderStatus, "muted" | "up" | "down" | "warn" | "accent"> = {
  PENDING_RISK: "muted",
  ACCEPTED: "accent",
  OPEN: "accent",
  PARTIALLY_FILLED: "warn",
  FILLED: "up",
  CANCELED: "muted",
  REJECTED: "down",
};

function statusLabel(s: OrderStatus) {
  return s === "PENDING_RISK" ? "Checking" : titleCase(s);
}

function CancelButton({ order }: { order: Order }) {
  const qc = useQueryClient();
  const m = useMutation({
    mutationFn: () => cancelOrder(order.order_id),
    onSuccess: () => void qc.invalidateQueries({ queryKey: ["orders"] }),
    onError: (e) =>
      toast({ tone: "warn", title: "Couldn't cancel", detail: e instanceof ApiError ? e.message : "Try again." }),
  });
  // Cancels are asynchronous: the matcher confirms them with an order update.
  const requested = m.isSuccess;
  return (
    <Button variant="ghost" size="sm" busy={m.isPending} disabled={requested} onClick={() => m.mutate()}>
      {requested ? "Canceling…" : "Cancel"}
    </Button>
  );
}

function Head({ cols }: { cols: string[] }) {
  return (
    <thead className="sticky top-0 bg-surface text-[11px] text-ink-3">
      <tr className="border-b border-line">
        {cols.map((c, i) => (
          <th key={c + i} className={cx("px-3 py-1.5 font-medium whitespace-nowrap", i < 2 ? "text-left" : "text-right")}>
            {c}
          </th>
        ))}
      </tr>
    </thead>
  );
}

function SideText({ side }: { side: "BUY" | "SELL" }) {
  return <span className={side === "BUY" ? "text-up" : "text-down"}>{side === "BUY" ? "Buy" : "Sell"}</span>;
}

function OrderRows({ orders, open, now }: { orders: Order[]; open: boolean; now: number }) {
  return (
    <table className="w-full min-w-[620px] text-xs">
      <Head cols={["Stock", "Side", "Type", "Price", "Filled / Shares", "Status", open ? "" : "Updated"]} />
      <tbody>
        {orders.map((o) => (
          <tr key={o.order_id} className="border-b border-line/60 last:border-0 hover:bg-hover/50">
            <td className="px-3 py-1.5 font-semibold">
              <Link to={`/trade/${o.symbol}`} className="hover:text-accent">
                {o.symbol}
              </Link>
            </td>
            <td className="px-3 py-1.5">
              <SideText side={o.side} />
            </td>
            <td className="px-3 py-1.5 text-right text-ink-2">
              {titleCase(o.type)}
              {o.type === "LIMIT" && <span className="text-ink-3"> · {o.time_in_force}</span>}
            </td>
            <td className="num px-3 py-1.5 text-right">{o.type === "MARKET" ? <span className="text-ink-3">Market</span> : price(o.price_cents)}</td>
            <td className="num px-3 py-1.5 text-right">
              {qty(o.filled_quantity)} <span className="text-ink-3">/ {qty(o.quantity)}</span>
            </td>
            <td className="px-3 py-1.5 text-right">
              <span title={o.reject_reason ? titleCase(o.reject_reason) : undefined}>
                <Badge tone={STATUS_TONE[o.status]}>{statusLabel(o.status)}</Badge>
              </span>
            </td>
            <td className="px-3 py-1 text-right whitespace-nowrap text-ink-3">
              {open ? <CancelButton order={o} /> : <span title={stamp(o.updated_at)}>{ago(Date.parse(o.updated_at), now)}</span>}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

export function OrdersPanel({ symbol, className }: { symbol: string; className?: string }) {
  const [tab, setTab] = useState<Tab>("open");
  const [onlyThis, setOnlyThis] = useState(false);
  const orders = useOrders();
  const fills = useFills();
  const now = useNow(10_000);

  const all = (orders.data ?? []).filter((o) => !onlyThis || o.symbol === symbol);
  const open = all.filter((o) => !FINAL_STATUSES.has(o.status));
  const done = all.filter((o) => FINAL_STATUSES.has(o.status));
  const myFills = fills.filter((f) => !onlyThis || f.symbol === symbol);

  return (
    <Panel
      className={className}
      title={
        <Segmented
          size="sm"
          value={tab}
          onChange={setTab}
          options={[
            { value: "open", label: `Open${open.length ? ` ${open.length}` : ""}` },
            { value: "history", label: "History" },
            { value: "fills", label: "Fills" },
          ]}
        />
      }
      actions={
        <label className="flex cursor-pointer items-center gap-1.5 text-[11px] text-ink-3 select-none">
          <input type="checkbox" className="accent-accent" checked={onlyThis} onChange={(e) => setOnlyThis(e.target.checked)} />
          {symbol} only
        </label>
      }
      bodyClassName="scroll-thin overflow-auto"
    >
      {orders.isLoading ? (
        <div className="flex h-full min-h-24 items-center justify-center text-ink-3">
          <Spinner />
        </div>
      ) : tab === "open" ? (
        open.length ? <OrderRows orders={open} open now={now} /> : <Empty>No open orders.</Empty>
      ) : tab === "history" ? (
        done.length ? <OrderRows orders={done} open={false} now={now} /> : <Empty>Finished orders show up here.</Empty>
      ) : myFills.length ? (
        <table className="w-full min-w-[560px] text-xs">
          <Head cols={["Stock", "Side", "Price", "Shares", "Value", "Status", "Time"]} />
          <tbody>
            {myFills.map((f) => (
              <tr key={f.key} className="border-b border-line/60 last:border-0">
                <td className="px-3 py-1.5 font-semibold">{f.symbol}</td>
                <td className="px-3 py-1.5">
                  <SideText side={f.side} />
                </td>
                <td className="num px-3 py-1.5 text-right">{price(f.priceCents)}</td>
                <td className="num px-3 py-1.5 text-right">{qty(f.quantity)}</td>
                <td className="num px-3 py-1.5 text-right text-ink-2">{price(f.priceCents * f.quantity)}</td>
                <td className="px-3 py-1.5 text-right">
                  {f.settled ? (
                    <Badge tone="up">Settled</Badge>
                  ) : (
                    <span title="Traded; the money and shares are still moving between accounts">
                      <Badge tone="warn">Settling</Badge>
                    </span>
                  )}
                </td>
                <td className="num px-3 py-1.5 text-right text-ink-3" title={stamp(f.at)}>
                  {ago(f.at, now)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      ) : (
        <Empty>Your trades show up here as they happen.</Empty>
      )}
    </Panel>
  );
}
