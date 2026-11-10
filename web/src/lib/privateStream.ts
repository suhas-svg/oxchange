// The logged-in user's messages: fills, settlements, cancels and deposits.
//
// On connect:
// 1. Get a one-time ticket (POST /v1/ws-ticket) and open /ws/private.
// 2. The server sends `ready` with the newest stored ID; live messages
//    follow from there.
// 3. Fetch everything after the last ID this tab has, up to `ready`'s ID,
//    over REST. Messages are kept by ID, so one that arrives both ways is
//    kept once.
//
// So a reconnect never loses a message (spec: "Live delivery can be missed
// during a reconnect, so clients call GET /v1/notifications?after={id}").

import { latestNotifications, notificationsAfter, wsTicket } from "./api";
import type { Notification } from "./types";

export type PrivateStatus = "idle" | "connecting" | "live" | "reconnecting";

/** Keep at most this many messages in memory. */
const KEEP = 500;

type Listener = () => void;
type MessageListener = (n: Notification, live: boolean) => void;

class PrivateStream {
  private ws: WebSocket | null = null;
  private byId = new Map<number, Notification>();
  private sorted: Notification[] = [];
  private lastId = 0;
  private listeners = new Set<Listener>();
  private messageListeners = new Set<MessageListener>();
  private status: PrivateStatus = "idle";
  private retry = 0;
  private timer: ReturnType<typeof setTimeout> | null = null;
  private running = false;
  /** Bumped on stop, so callbacks from an old session do nothing. */
  private generation = 0;

  getStatus = () => this.status;
  getAll = () => this.sorted;

  subscribe = (l: Listener) => {
    this.listeners.add(l);
    return () => this.listeners.delete(l);
  };

  /** Called once per new message; `live` is false for catch-up. */
  onMessage(l: MessageListener): () => void {
    this.messageListeners.add(l);
    return () => this.messageListeners.delete(l);
  }

  private changed() {
    for (const l of this.listeners) l();
  }

  private setStatus(s: PrivateStatus) {
    this.status = s;
    this.changed();
  }

  private add(n: Notification, live: boolean): boolean {
    if (this.byId.has(n.id)) return false;
    this.byId.set(n.id, n);
    this.lastId = Math.max(this.lastId, n.id);
    for (const l of this.messageListeners) l(n, live);
    return true;
  }

  private publish() {
    this.sorted = [...this.byId.values()].sort((a, b) => a.id - b.id).slice(-KEEP);
    if (this.byId.size > KEEP * 2) {
      this.byId = new Map(this.sorted.map((n) => [n.id, n]));
    }
    this.changed();
  }

  async start() {
    if (this.running) return;
    this.running = true;
    const gen = ++this.generation;
    this.setStatus("connecting");
    try {
      // History first, so the page has something before the stream opens.
      const recent = await latestNotifications(100);
      if (gen !== this.generation) return;
      for (const n of recent) this.add(n, false);
      this.publish();
    } catch {
      // The stream's catch-up fills in once it connects.
    }
    void this.connect(gen);
  }

  stop() {
    this.running = false;
    this.generation += 1;
    if (this.timer) clearTimeout(this.timer);
    this.timer = null;
    const ws = this.ws;
    this.ws = null;
    ws?.close();
    this.byId.clear();
    this.sorted = [];
    this.lastId = 0;
    this.setStatus("idle");
  }

  private async connect(gen: number) {
    if (gen !== this.generation) return;
    let ticket: string;
    try {
      ticket = await wsTicket();
    } catch {
      this.scheduleReconnect(gen);
      return;
    }
    if (gen !== this.generation) return;
    const proto = location.protocol === "https:" ? "wss" : "ws";
    const ws = new WebSocket(`${proto}://${location.host}/ws/private?ticket=${encodeURIComponent(ticket)}`);
    this.ws = ws;
    ws.onmessage = (e) => {
      if (gen !== this.generation) return;
      let m: { type: string; id?: number; last_id?: number };
      try {
        m = JSON.parse(String(e.data));
      } catch {
        return;
      }
      if (m.type === "ready") {
        this.retry = 0;
        this.setStatus("live");
        void this.catchUp(gen, m.last_id ?? 0);
        return;
      }
      if (typeof m.id === "number" && this.add(m as unknown as Notification, true)) this.publish();
    };
    ws.onclose = () => {
      if (this.ws !== ws) return;
      this.ws = null;
      this.scheduleReconnect(gen);
    };
  }

  /** Everything stored after this tab's last ID, up to where live began. */
  private async catchUp(gen: number, upTo: number) {
    let after = this.lastId;
    let added = false;
    try {
      while (after < upTo && gen === this.generation) {
        const page = await notificationsAfter(after, 100);
        for (const n of page.notifications) added = this.add(n, false) || added;
        if (page.next_after == null) break;
        after = page.next_after;
      }
    } catch {
      // Try again on the next reconnect.
    }
    if (added) this.publish();
  }

  private scheduleReconnect(gen: number) {
    if (gen !== this.generation || !this.running) return;
    this.setStatus("reconnecting");
    const delay = Math.min(10_000, 500 * 2 ** this.retry) * (0.75 + Math.random() * 0.5);
    this.retry += 1;
    this.timer = setTimeout(() => {
      this.timer = null;
      void this.connect(gen);
    }, delay);
  }
}

export const privateStream = new PrivateStream();
