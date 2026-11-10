import { describe, expect, it } from "vitest";

import { apply, best, levels, type Book } from "./book";
import { centsToInput, parseDollars, parseShares } from "./format";

const snapshot = {
  type: "book_snapshot" as const,
  symbol: "ACME",
  seq: 10,
  bids: [
    { price_cents: 1000, quantity: 5 },
    { price_cents: 990, quantity: 7 },
  ],
  asks: [{ price_cents: 1010, quantity: 3 }],
};

const delta = (seq: number, prev: number, side: "BUY" | "SELL", price: number, quantity: number) => ({
  type: "book_delta" as const,
  symbol: "ACME",
  seq,
  prev_seq: prev,
  side,
  price_cents: price,
  quantity,
});

function bookOf(r: ReturnType<typeof apply>): Book {
  if (!("book" in r)) throw new Error("expected a book");
  return r.book;
}

describe("order book", () => {
  it("applies deltas in order and removes emptied levels", () => {
    let b = bookOf(apply(null, snapshot));
    // Seqs 11 and 12 were trades; the next book message names 10.
    b = bookOf(apply(b, delta(13, 10, "BUY", 1000, 0)));
    b = bookOf(apply(b, delta(14, 13, "SELL", 1005, 4)));
    expect(levels(b.bids, "bids", 10)).toEqual([{ price_cents: 990, quantity: 7, total: 7 }]);
    expect(levels(b.asks, "asks", 10)).toEqual([
      { price_cents: 1005, quantity: 4, total: 4 },
      { price_cents: 1010, quantity: 3, total: 7 },
    ]);
    expect(best(b)).toEqual({ bid: 990, ask: 1005 });
  });

  it("reports a gap when a book message was missed", () => {
    const b = bookOf(apply(null, snapshot));
    expect(apply(b, delta(15, 13, "BUY", 1000, 1))).toEqual({ gap: true });
    expect(apply(null, delta(15, 13, "BUY", 1000, 1))).toEqual({ gap: true });
  });

  it("ignores deltas the snapshot already covers", () => {
    const b = bookOf(apply(null, snapshot));
    expect(bookOf(apply(b, delta(9, 8, "BUY", 1000, 99)))).toBe(b);
  });
});

describe("input parsing", () => {
  it("parses dollars into exact cents", () => {
    expect(parseDollars("10")).toBe(1000);
    expect(parseDollars("10.5")).toBe(1050);
    expect(parseDollars("$1,234.05")).toBe(123405);
    expect(parseDollars("0.07")).toBe(7);
    expect(parseDollars("10.055")).toBeNull();
    expect(parseDollars("-1")).toBeNull();
    expect(parseDollars("abc")).toBeNull();
    expect(centsToInput(1005)).toBe("10.05");
    expect(centsToInput(7)).toBe("0.07");
  });

  it("accepts only whole positive share counts", () => {
    expect(parseShares("120")).toBe(120);
    expect(parseShares("0")).toBeNull();
    expect(parseShares("1.5")).toBeNull();
  });
});
