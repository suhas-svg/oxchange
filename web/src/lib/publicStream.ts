// One shared connection to /ws/public for the whole app.
//
// Components subscribe to (channel, symbol) pairs; the stream counts how
// many want each pair and tells the server only about the first and last.
// It reconnects with backoff and resubscribes everything, and each book
// subscriber gets a fresh snapshot then. The Gateway allows 50
// subscriptions per connection, so callers keep their sets small.

import type { Channel, PublicMessage } from "./types";

export type Status = "connecting" | "live" | "reconnecting";
type Listener = (m: PublicMessage) => void;

function key(channel: Channel, symbol: string) {
  return `${channel}:${symbol}`;
}

function wsUrl(path: string) {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  return `${proto}://${location.host}${path}`;
}

class PublicStream {
  private ws: WebSocket | null = null;
  private subs = new Map<string, Set<Listener>>();
  private statusListeners = new Set<(s: Status) => void>();
  private status: Status = "connecting";
  private retry = 0;
  private timer: ReturnType<typeof setTimeout> | null = null;

  private setStatus(s: Status) {
    this.status = s;
    for (const l of this.statusListeners) l(s);
  }

  getStatus() {
    return this.status;
  }

  onStatus(l: (s: Status) => void): () => void {
    this.statusListeners.add(l);
    return () => this.statusListeners.delete(l);
  }

  private send(op: "subscribe" | "unsubscribe", k: string) {
    if (this.ws?.readyState !== WebSocket.OPEN) return;
    const [channel, symbol] = k.split(":");
    this.ws.send(JSON.stringify({ op, channel, symbol }));
  }

  private connect() {
    if (this.ws || this.subs.size === 0) return;
    const ws = new WebSocket(wsUrl("/ws/public"));
    this.ws = ws;
    ws.onopen = () => {
      this.retry = 0;
      this.setStatus("live");
      for (const k of this.subs.keys()) this.send("subscribe", k);
    };
    ws.onmessage = (e) => {
      let m: PublicMessage;
      try {
        m = JSON.parse(String(e.data)) as PublicMessage;
      } catch {
        return;
      }
      if (m.type === "error") {
        console.warn("market stream:", m.message);
        return;
      }
      if (m.type === "resync_required") return; // the server closes next
      const channel: Channel = m.type === "trade" ? "trades" : m.type === "ticker" ? "ticker" : "book";
      const ls = this.subs.get(key(channel, m.symbol));
      if (ls) for (const l of ls) l(m);
    };
    ws.onclose = () => {
      // A socket closed on purpose has already been replaced or dropped.
      if (this.ws !== ws) return;
      this.ws = null;
      if (this.subs.size === 0) return;
      this.setStatus("reconnecting");
      const delay = Math.min(10_000, 500 * 2 ** this.retry) * (0.75 + Math.random() * 0.5);
      this.retry += 1;
      this.timer = setTimeout(() => {
        this.timer = null;
        this.connect();
      }, delay);
    };
  }

  /** Starts listening; the returned function stops. */
  subscribe(channel: Channel, symbol: string, l: Listener): () => void {
    const k = key(channel, symbol);
    let set = this.subs.get(k);
    if (!set) {
      set = new Set();
      this.subs.set(k, set);
      this.send("subscribe", k);
    }
    set.add(l);
    this.connect();
    return () => {
      const s = this.subs.get(k);
      if (!s) return;
      s.delete(l);
      if (s.size === 0) {
        this.subs.delete(k);
        this.send("unsubscribe", k);
      }
      if (this.subs.size === 0) {
        if (this.timer) clearTimeout(this.timer);
        this.timer = null;
        this.ws?.close();
        this.ws = null;
      }
    };
  }

  /** Asks again for a fresh book snapshot after a gap. */
  resync(symbol: string) {
    const k = key("book", symbol);
    if (!this.subs.has(k)) return;
    this.send("unsubscribe", k);
    this.send("subscribe", k);
  }
}

export const publicStream = new PublicStream();
