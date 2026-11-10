// Toasts, and plain-language text for each kind of notification.

import { useSyncExternalStore } from "react";

import { money, price, qty } from "./format";
import type { Notification } from "./types";

export type Tone = "up" | "down" | "info" | "warn";

export interface Toast {
  id: number;
  tone: Tone;
  title: string;
  detail?: string;
}

let toasts: Toast[] = [];
let nextId = 1;
const listeners = new Set<() => void>();

function emit() {
  for (const l of listeners) l();
}

export function toast(t: Omit<Toast, "id">, ms = 5000) {
  const id = nextId++;
  toasts = [...toasts, { ...t, id }].slice(-5);
  emit();
  setTimeout(() => dismiss(id), ms);
}

export function dismiss(id: number) {
  toasts = toasts.filter((t) => t.id !== id);
  emit();
}

export function useToasts(): Toast[] {
  return useSyncExternalStore(
    (l) => {
      listeners.add(l);
      return () => listeners.delete(l);
    },
    () => toasts,
  );
}

const CANCEL_REASONS: Record<string, string> = {
  USER: "you canceled it",
  IOC_REMAINDER: "the unfilled part of an immediate order",
  SELF_TRADE: "it would have traded with your own order",
  INVALID: "the order was invalid",
};

const REJECT_REASONS: Record<string, string> = {
  NOT_FOUND: "the order had already finished",
  NOT_OWNER: "the order is not yours",
};

function amount(asset: string, n: number) {
  return asset === "USD" || asset === "" ? money(n) : `${qty(n)} ${asset}`;
}

/** A headline and a detail line for a notification. */
export function describe(n: Notification): { tone: Tone; title: string; detail: string } {
  switch (n.type) {
    case "fill": {
      const d = n.data;
      const verb = d.side === "BUY" ? "Bought" : "Sold";
      return {
        tone: d.side === "BUY" ? "up" : "down",
        title: `${verb} ${qty(d.quantity)} ${d.symbol} at ${price(d.price_cents)}`,
        detail: `Settling · ${money(d.price_cents * d.quantity)} total`,
      };
    }
    case "settlement": {
      const d = n.data;
      const verb = d.side === "BUY" ? "bought" : "sold";
      return {
        tone: "info",
        title: `Settled: ${verb} ${qty(d.quantity)} ${d.symbol}`,
        detail: `${money(d.price_cents * d.quantity)} at ${price(d.price_cents)}`,
      };
    }
    case "order_update": {
      const d = n.data;
      if (d.update === "CANCEL_REJECTED") {
        return {
          tone: "warn",
          title: `Cancel refused on ${d.symbol}`,
          detail: REJECT_REASONS[d.reason] ?? d.reason.toLowerCase(),
        };
      }
      return {
        tone: "warn",
        title: `Canceled ${qty(d.canceled_quantity ?? 0)} ${d.symbol}`,
        detail: `Because ${CANCEL_REASONS[d.reason] ?? d.reason.toLowerCase()}. The reserved funds are released.`,
      };
    }
    case "account_update": {
      const d = n.data;
      const verb = d.change === "DEPOSIT" ? "Deposited" : "Withdrew";
      return {
        tone: "info",
        title: `${verb} ${amount(d.asset, d.amount)}`,
        detail: `Available now: ${amount(d.asset, d.available)}`,
      };
    }
  }
}
