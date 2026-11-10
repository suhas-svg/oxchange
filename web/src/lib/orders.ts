import { useQuery } from "@tanstack/react-query";
import { useMemo } from "react";

import { listOrders } from "./api";
import { useNotifications, useSession } from "./hooks";
import type { Order, Side } from "./types";

/** Orders Risk has not finished with yet; their status may still change without a notification. */
const IN_FLIGHT = new Set(["PENDING_RISK", "ACCEPTED"]);

export function useOrders() {
  const session = useSession();
  return useQuery({
    queryKey: ["orders"],
    queryFn: () => listOrders({ limit: 100 }).then((r) => r.orders),
    enabled: !!session,
    staleTime: 5_000,
    refetchInterval: (q) => ((q.state.data as Order[] | undefined)?.some((o) => IN_FLIGHT.has(o.status)) ? 1_500 : false),
  });
}

export interface Fill {
  key: string;
  tradeId: string;
  orderId: string;
  symbol: string;
  side: Side;
  priceCents: number;
  quantity: number;
  at: number;
  settled: boolean;
}

/** The user's fills from the notification stream, newest first, with whether each has settled. */
export function useFills(): Fill[] {
  const all = useNotifications();
  return useMemo(() => {
    const settled = new Set<string>();
    for (const n of all) if (n.type === "settlement") settled.add(`${n.data.trade_id}:${n.data.order_id}`);
    const out: Fill[] = [];
    for (const n of all) {
      if (n.type !== "fill") continue;
      const key = `${n.data.trade_id}:${n.data.order_id}`;
      out.push({
        key,
        tradeId: n.data.trade_id,
        orderId: n.data.order_id,
        symbol: n.data.symbol,
        side: n.data.side,
        priceCents: n.data.price_cents,
        quantity: n.data.quantity,
        at: n.data.executed_at_ms,
        settled: settled.has(key),
      });
    }
    return out.reverse();
  }, [all]);
}
