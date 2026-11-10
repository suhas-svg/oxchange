// A stock's order book rebuilt from the public stream: a snapshot, then
// deltas. Each delta names the seq of the book message before it
// (`prev_seq`); if that is not the seq we last applied, a message was lost
// and the book must be reloaded (spec: "If a client sees a gap in seq, it
// must resubscribe to get a fresh snapshot").

import type { Level, PublicMessage } from "./types";

export interface Book {
  seq: number;
  /** price → quantity */
  bids: Map<number, number>;
  asks: Map<number, number>;
}

export type BookMessage = Extract<PublicMessage, { type: "book_snapshot" | "book_delta" }>;

export type Applied = { book: Book } | { gap: true };

function toMap(levels: Level[]): Map<number, number> {
  const m = new Map<number, number>();
  for (const l of levels) if (l.quantity > 0) m.set(l.price_cents, l.quantity);
  return m;
}

/** Applies one message. A delta without a book, or out of order, is a gap. */
export function apply(book: Book | null, m: BookMessage): Applied {
  if (m.type === "book_snapshot") {
    return { book: { seq: m.seq, bids: toMap(m.bids), asks: toMap(m.asks) } };
  }
  if (!book) return { gap: true };
  // An old delta (already covered by the snapshot) changes nothing.
  if (m.seq <= book.seq) return { book };
  if (m.prev_seq !== book.seq) return { gap: true };
  const side = new Map(m.side === "BUY" ? book.bids : book.asks);
  if (m.quantity > 0) side.set(m.price_cents, m.quantity);
  else side.delete(m.price_cents);
  return {
    book: m.side === "BUY" ? { seq: m.seq, bids: side, asks: book.asks } : { seq: m.seq, bids: book.bids, asks: side },
  };
}

export interface Row {
  price_cents: number;
  quantity: number;
  /** Running total from the best price outward. */
  total: number;
}

/** The best `depth` levels of one side, best first, with running totals. */
export function levels(side: Map<number, number>, kind: "bids" | "asks", depth: number): Row[] {
  const prices = [...side.keys()].sort((a, b) => (kind === "bids" ? b - a : a - b)).slice(0, depth);
  let total = 0;
  return prices.map((p) => {
    const q = side.get(p) ?? 0;
    total += q;
    return { price_cents: p, quantity: q, total };
  });
}

export function best(book: Book | null): { bid: number | null; ask: number | null } {
  if (!book) return { bid: null, ask: null };
  let bid: number | null = null;
  let ask: number | null = null;
  for (const p of book.bids.keys()) if (bid === null || p > bid) bid = p;
  for (const p of book.asks.keys()) if (ask === null || p < ask) ask = p;
  return { bid, ask };
}
