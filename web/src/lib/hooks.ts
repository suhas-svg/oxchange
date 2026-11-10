import { useQuery } from "@tanstack/react-query";
import { useEffect, useMemo, useRef, useState, useSyncExternalStore } from "react";

import * as api from "./api";
import { apply, type Book, type BookMessage } from "./book";
import { privateStream } from "./privateStream";
import { publicStream, type Status } from "./publicStream";
import { getSession, subscribeSession } from "./session";
import type { Notification, PublicMessage, Trade } from "./types";

export function useSession() {
  return useSyncExternalStore(subscribeSession, getSession, () => null);
}

export function useMarketStatus(): Status {
  return useSyncExternalStore(
    (cb) => publicStream.onStatus(cb),
    () => publicStream.getStatus(),
  );
}

/** The live book for one stock; null until the first snapshot. */
export function useBook(symbol: string): Book | null {
  const [book, setBook] = useState<Book | null>(null);
  useEffect(() => {
    let current: Book | null = null;
    let waiting = false;
    setBook(null);
    const off = publicStream.subscribe("book", symbol, (m) => {
      if (m.type !== "book_snapshot" && m.type !== "book_delta") return;
      if (waiting && m.type !== "book_snapshot") return;
      const r = apply(current, m as BookMessage);
      if ("gap" in r) {
        // Missed a message: drop the book and ask for a fresh snapshot.
        waiting = true;
        publicStream.resync(symbol);
        return;
      }
      waiting = false;
      current = r.book;
      setBook(r.book);
    });
    return off;
  }, [symbol]);
  return book;
}

/** Recent trades, newest first: REST history plus the live stream. */
export function useTrades(symbol: string, keep = 60): Trade[] {
  const [live, setLive] = useState<Trade[]>([]);
  const history = useQuery({
    queryKey: ["trades", symbol],
    queryFn: () => api.recentTrades(symbol, keep),
    staleTime: Infinity,
  });
  // Trades during a disconnect never reach the stream: reload the history
  // each time the market stream comes back.
  const status = useMarketStatus();
  const wasDown = useRef(false);
  const { refetch } = history;
  useEffect(() => {
    if (status !== "live") wasDown.current = true;
    else if (wasDown.current) {
      wasDown.current = false;
      void refetch();
    }
  }, [status, refetch]);
  useEffect(() => {
    setLive([]);
    return publicStream.subscribe("trades", symbol, (m) => {
      if (m.type !== "trade") return;
      const t: Trade = {
        seq: m.seq,
        price_cents: m.price_cents,
        quantity: m.quantity,
        side: m.side,
        time_ms: Math.floor(m.time_ns / 1_000_000),
      };
      setLive((prev) => [t, ...prev].slice(0, keep));
    });
  }, [symbol, keep]);
  return useMemo(() => {
    const seen = new Set<number>();
    const out: Trade[] = [];
    for (const t of [...live, ...(history.data ?? [])]) {
      if (seen.has(t.seq)) continue;
      seen.add(t.seq);
      out.push(t);
    }
    return out.sort((a, b) => b.seq - a.seq).slice(0, keep);
  }, [live, history.data, keep]);
}

export interface Tick {
  last_price_cents: number | null;
  day_volume: number;
  /** -1, 0 or 1 versus the previous price, for a brief flash. */
  move: number;
  at: number;
}

/** Live tickers for several stocks (one subscription each). */
export function useTickers(symbols: string[]): Record<string, Tick> {
  const [ticks, setTicks] = useState<Record<string, Tick>>({});
  const joined = symbols.join(",");
  useEffect(() => {
    const list = joined ? joined.split(",") : [];
    const offs = list.map((s) =>
      publicStream.subscribe("ticker", s, (m: PublicMessage) => {
        if (m.type !== "ticker") return;
        setTicks((prev) => {
          const old = prev[s];
          const move =
            old?.last_price_cents != null && m.last_price_cents != null
              ? Math.sign(m.last_price_cents - old.last_price_cents)
              : 0;
          return { ...prev, [s]: { last_price_cents: m.last_price_cents, day_volume: m.day_volume, move, at: Date.now() } };
        });
      }),
    );
    return () => offs.forEach((off) => off());
  }, [joined]);
  return ticks;
}

export function useTicker(symbol: string): Tick | undefined {
  return useTickers([symbol])[symbol];
}

export function useNotifications(): Notification[] {
  return useSyncExternalStore(privateStream.subscribe, privateStream.getAll);
}

export function usePrivateStatus() {
  return useSyncExternalStore(privateStream.subscribe, privateStream.getStatus);
}

export function useBalances() {
  const session = useSession();
  return useQuery({
    queryKey: ["balances"],
    queryFn: api.getBalances,
    enabled: !!session,
    staleTime: 10_000,
  });
}

export function useSymbols() {
  return useQuery({ queryKey: ["symbols"], queryFn: api.listSymbols, staleTime: 60_000 });
}

/** Re-renders every `ms`, for "3s ago" labels. */
export function useNow(ms = 1000): number {
  const [now, setNow] = useState(Date.now());
  useEffect(() => {
    const t = setInterval(() => setNow(Date.now()), ms);
    return () => clearInterval(t);
  }, [ms]);
  return now;
}

/** The previous value of `v`. */
export function usePrevious<T>(v: T): T | undefined {
  const r = useRef<T | undefined>(undefined);
  useEffect(() => {
    r.current = v;
  }, [v]);
  return r.current;
}
