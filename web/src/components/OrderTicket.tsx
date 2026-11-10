import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useEffect, useState, type FormEvent } from "react";

import { ApiError, newId, placeOrder, type NewOrder } from "../lib/api";
import { centsToInput, money, parseDollars, parseShares, price, qty } from "../lib/format";
import { useBalances } from "../lib/hooks";
import { toast } from "../lib/notify";
import type { OrderType, Side, TimeInForce } from "../lib/types";
import { Button, cx, Field, Panel, Segmented, inputClass } from "./ui";

/** Market orders trade at most this far from the last price (the matcher's collar). */
const COLLAR = 0.05;

export interface Pick {
  priceCents: number;
  side: Side;
  /** Changes on every click, so picking the same price twice still applies. */
  at: number;
}

const REASONS: Record<string, string> = {
  INSUFFICIENT_FUNDS: "Not enough available funds for this order.",
  RISK_LIMIT_EXCEEDED: "This order is over the per-order risk limit.",
  SYMBOL_NOT_TRADING: "Trading in this stock is halted.",
};

export function OrderTicket({
  symbol,
  bid,
  ask,
  last,
  pick,
  className,
}: {
  symbol: string;
  bid: number | null;
  ask: number | null;
  last: number | null;
  pick: Pick | null;
  className?: string;
}) {
  const qc = useQueryClient();
  const balances = useBalances();
  const [side, setSide] = useState<Side>("BUY");
  const [type, setType] = useState<OrderType>("LIMIT");
  const [tif, setTif] = useState<TimeInForce>("GTC");
  const [priceText, setPriceText] = useState("");
  const [qtyText, setQtyText] = useState("");
  const [error, setError] = useState<string | null>(null);

  // A new stock starts with an empty ticket.
  useEffect(() => {
    setPriceText("");
    setQtyText("");
    setError(null);
  }, [symbol]);

  // A click in the order book fills in the price and side.
  useEffect(() => {
    if (!pick) return;
    setSide(pick.side);
    setType("LIMIT");
    setPriceText(centsToInput(pick.priceCents));
    setError(null);
  }, [pick]);

  const cash = balances.data?.find((b) => b.asset === "USD")?.available ?? 0;
  const shares = balances.data?.find((b) => b.asset === symbol)?.available ?? 0;

  const priceCents = type === "LIMIT" ? parseDollars(priceText) : null;
  const quantity = parseShares(qtyText);
  // What a buy reserves: the limit price, or the collar's ceiling for a market buy.
  const worst = type === "LIMIT" ? priceCents : last != null ? Math.ceil(last * (1 + (side === "BUY" ? COLLAR : -COLLAR))) : null;
  const total = worst != null && quantity != null ? worst * quantity : null;
  const short = side === "BUY" ? total != null && total > cash : quantity != null && quantity > shares;

  const m = useMutation({
    mutationFn: async (o: NewOrder) => {
      try {
        return await placeOrder(o);
      } catch (e) {
        // Safe to resend: the same client_order_id returns the same order.
        if (e instanceof ApiError && e.temporary) return placeOrder(o);
        throw e;
      }
    },
    onSuccess: (o) => {
      void qc.invalidateQueries({ queryKey: ["orders"] });
      void qc.invalidateQueries({ queryKey: ["balances"] });
      setQtyText("");
      if (o.status === "REJECTED") {
        toast({ tone: "down", title: `Order rejected`, detail: REASONS[o.reject_reason ?? ""] ?? o.reject_reason ?? undefined });
      } else {
        const what = `${o.side === "BUY" ? "Buy" : "Sell"} ${qty(o.quantity)} ${o.symbol}`;
        toast({
          tone: "info",
          title: `Order sent: ${what}`,
          detail: o.type === "LIMIT" ? `Limit ${price(o.price_cents)} · ${o.time_in_force}` : "Market order",
        });
      }
    },
    onError: (e) => {
      if (e instanceof ApiError) setError(REASONS[e.code] ?? e.message);
      else setError("Something went wrong. Try again.");
    },
  });

  function submit(e: FormEvent) {
    e.preventDefault();
    setError(null);
    if (type === "LIMIT" && (priceCents == null || priceCents <= 0)) return setError("Enter a limit price, like 10.25.");
    if (quantity == null) return setError("Enter a whole number of shares.");
    if (type === "MARKET" && last == null) return setError("No trades yet, so a market order has no price to work from. Use a limit order.");
    m.mutate({
      client_order_id: newId("web"),
      symbol,
      side,
      type,
      quantity,
      ...(type === "LIMIT" ? { price_cents: priceCents!, time_in_force: tif } : {}),
    });
  }

  function fraction(f: number) {
    if (side === "SELL") {
      const n = Math.floor(shares * f);
      setQtyText(n > 0 ? String(n) : "");
      return;
    }
    if (worst == null || worst <= 0) return setError("Set a price first.");
    const n = Math.floor((cash * f) / worst);
    setQtyText(n > 0 ? String(n) : "");
  }

  const quick = [
    { label: "Bid", v: bid },
    { label: "Last", v: last },
    { label: "Ask", v: ask },
  ];

  return (
    <Panel title={`Place order · ${symbol}`} className={className} bodyClassName="scroll-thin overflow-y-auto">
      <form onSubmit={submit} className="space-y-3.5 p-3">
        <Segmented
          value={side}
          onChange={setSide}
          options={[
            { value: "BUY", label: "Buy", activeClass: "bg-up text-[#04140f]" },
            { value: "SELL", label: "Sell", activeClass: "bg-down text-white" },
          ]}
        />
        <div className="flex gap-2">
          <Segmented
            size="sm"
            className="flex-1"
            value={type}
            onChange={setType}
            options={[
              { value: "LIMIT", label: "Limit" },
              { value: "MARKET", label: "Market" },
            ]}
          />
          {type === "LIMIT" && (
            <Segmented
              size="sm"
              value={tif}
              onChange={setTif}
              options={[
                { value: "GTC", label: <span title="Good till canceled: rests in the book">GTC</span> },
                { value: "IOC", label: <span title="Immediate or cancel: whatever can't fill now is canceled">IOC</span> },
              ]}
            />
          )}
        </div>

        {type === "LIMIT" ? (
          <Field
            label="Limit price"
            hint={
              <span className="flex gap-1">
                {quick.map((q) => (
                  <button
                    key={q.label}
                    type="button"
                    disabled={q.v == null}
                    onClick={() => q.v != null && setPriceText(centsToInput(q.v))}
                    className="rounded px-1 text-[11px] text-ink-3 hover:bg-hover hover:text-ink disabled:opacity-40"
                  >
                    {q.label}
                  </button>
                ))}
              </span>
            }
          >
            <div className="relative">
              <input
                className={cx(inputClass, "num pr-12")}
                inputMode="decimal"
                placeholder="0.00"
                value={priceText}
                onChange={(e) => setPriceText(e.target.value)}
              />
              <span className="pointer-events-none absolute top-1/2 right-3 -translate-y-1/2 text-xs text-ink-3">USD</span>
            </div>
          </Field>
        ) : (
          <p className="rounded-md bg-canvas px-3 py-2 text-xs leading-relaxed text-ink-3">
            Fills against the book now, up to 5% from the last price
            {last != null && <span className="num text-ink-2"> ({price(last)})</span>}. Whatever can't fill within that is
            canceled.
          </p>
        )}

        <Field label="Shares">
          <input
            className={cx(inputClass, "num")}
            inputMode="numeric"
            placeholder="0"
            value={qtyText}
            onChange={(e) => setQtyText(e.target.value)}
          />
        </Field>
        <div className="grid grid-cols-4 gap-1.5">
          {[0.25, 0.5, 0.75, 1].map((f) => (
            <button
              key={f}
              type="button"
              onClick={() => fraction(f)}
              className="h-6 rounded border border-line text-[11px] text-ink-3 hover:border-line-strong hover:text-ink"
            >
              {f === 1 ? "Max" : `${f * 100}%`}
            </button>
          ))}
        </div>

        <dl className="space-y-1.5 border-t border-line pt-3 text-xs">
          <div className="flex justify-between">
            <dt className="text-ink-3">{side === "BUY" ? "Cash available" : `${symbol} available`}</dt>
            <dd className="num text-ink-2">{side === "BUY" ? money(cash) : `${qty(shares)} sh`}</dd>
          </div>
          <div className="flex justify-between">
            <dt className="text-ink-3">{type === "MARKET" ? (side === "BUY" ? "Up to" : "At least") : "Total"}</dt>
            <dd className={cx("num", short ? "text-down" : "text-ink")}>{money(total)}</dd>
          </div>
        </dl>

        {error && (
          <p role="alert" className="text-xs text-down">
            {error}
          </p>
        )}
        {!error && short && (
          <p className="text-xs text-warn">{side === "BUY" ? "More than your available cash." : "More shares than you have."}</p>
        )}

        <Button type="submit" variant={side === "BUY" ? "buy" : "sell"} size="lg" className="w-full" busy={m.isPending}>
          {side === "BUY" ? "Buy" : "Sell"} {symbol}
        </Button>
      </form>
    </Panel>
  );
}
