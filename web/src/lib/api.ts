// The Gateway's REST API. Every call goes to the same origin; the web
// server (or the Vite dev server) forwards /v1 to the Gateway.

import { accessToken, getSession, sessionFrom, setSession } from "./session";
import type { Balance, Candle, Notification, Order, OrderType, Side, SymbolInfo, TimeInForce, Tokens, Trade } from "./types";

export class ApiError extends Error {
  constructor(
    public status: number,
    public code: string,
    message: string,
    public traceId?: string,
    public retryAfter?: number,
  ) {
    super(message);
  }

  /** Worth retrying with the same request (the spec's SERVICE_UNAVAILABLE). */
  get temporary(): boolean {
    return this.status === 503 || this.status === 0;
  }
}

interface Options {
  body?: unknown;
  auth?: boolean;
  headers?: Record<string, string>;
}

async function send(method: string, path: string, opts: Options, token: string | null): Promise<Response> {
  const headers: Record<string, string> = { Accept: "application/json", ...opts.headers };
  if (opts.body !== undefined) headers["Content-Type"] = "application/json";
  if (token) headers.Authorization = `Bearer ${token}`;
  try {
    return await fetch(path, {
      method,
      headers,
      body: opts.body === undefined ? undefined : JSON.stringify(opts.body),
    });
  } catch {
    throw new ApiError(0, "NETWORK", "Can't reach the exchange. Check your connection.");
  }
}

async function request<T>(method: string, path: string, opts: Options = {}): Promise<T> {
  const auth = opts.auth ?? true;
  let token: string | null = null;
  if (auth) {
    token = await accessToken();
    if (!token) throw new ApiError(401, "UNAUTHENTICATED", "Please log in.");
  }
  let r = await send(method, path, opts, token);
  if (r.status === 401 && auth) {
    // Expired or revoked between the check and the call: refresh once.
    token = await accessToken(true);
    if (!token) throw new ApiError(401, "UNAUTHENTICATED", "Your session ended. Please log in again.");
    r = await send(method, path, opts, token);
  }
  if (r.status === 204) return undefined as T;
  const text = await r.text();
  let body: unknown = null;
  try {
    body = text ? JSON.parse(text) : null;
  } catch {
    body = null;
  }
  if (!r.ok) {
    const e = (body as { error?: { code?: string; message?: string; trace_id?: string } } | null)?.error;
    const retry = Number(r.headers.get("Retry-After") ?? "");
    throw new ApiError(
      r.status,
      e?.code ?? `HTTP_${r.status}`,
      e?.message ?? `Request failed (${r.status})`,
      e?.trace_id ?? r.headers.get("X-Trace-Id") ?? undefined,
      Number.isFinite(retry) && retry > 0 ? retry : undefined,
    );
  }
  return body as T;
}

// --- Login -------------------------------------------------------------------

export async function register(email: string, password: string): Promise<void> {
  await request<{ user_id: string }>("POST", "/v1/auth/register", { body: { email, password }, auth: false });
}

export async function login(email: string, password: string): Promise<void> {
  const t = await request<Tokens>("POST", "/v1/auth/login", { body: { email, password }, auth: false });
  setSession(sessionFrom(t, t.user_id ?? "", email));
}

export async function logout(): Promise<void> {
  const s = getSession();
  try {
    if (s) await request<void>("POST", "/v1/auth/logout", { body: { refresh_token: s.refreshToken } });
  } finally {
    setSession(null);
  }
}

// --- Account -----------------------------------------------------------------

export const getBalances = () => request<{ balances: Balance[] }>("GET", "/v1/account/balances").then((r) => r.balances);

/** `asset` is "USD" (amount in cents) or a stock symbol (whole shares). */
export function deposit(asset: string, amount: number, idempotencyKey: string) {
  return request<{ transaction_id: string; available: number }>("POST", "/v1/account/deposits", {
    body: { amount_cents: amount, asset },
    headers: { "Idempotency-Key": idempotencyKey },
  });
}

export function withdraw(asset: string, amount: number, idempotencyKey: string) {
  return request<{ transaction_id: string; available: number }>("POST", "/v1/account/withdrawals", {
    body: { amount_cents: amount, asset },
    headers: { "Idempotency-Key": idempotencyKey },
  });
}

// --- Orders ------------------------------------------------------------------

export interface NewOrder {
  client_order_id: string;
  symbol: string;
  side: Side;
  type: OrderType;
  time_in_force?: TimeInForce;
  price_cents?: number;
  quantity: number;
}

export const placeOrder = (o: NewOrder) => request<Order>("POST", "/v1/orders", { body: o });

export const cancelOrder = (orderId: string) => request<Order>("DELETE", `/v1/orders/${encodeURIComponent(orderId)}`);

export function listOrders(q: { symbol?: string; openOnly?: boolean; limit?: number }) {
  const p = new URLSearchParams();
  if (q.symbol) p.set("symbol", q.symbol);
  if (q.openOnly) p.set("open_only", "true");
  p.set("limit", String(q.limit ?? 50));
  return request<{ orders: Order[]; next_cursor: string | null }>("GET", `/v1/orders?${p}`);
}

// --- Market (no login) -------------------------------------------------------

export const listSymbols = () =>
  request<{ symbols: SymbolInfo[] }>("GET", "/v1/symbols", { auth: false }).then((r) => r.symbols);

export async function recentTrades(symbol: string, limit = 60): Promise<Trade[]> {
  const r = await request<{
    trades: { seq: number; price_cents: number; quantity: number; aggressor_side: "BUY" | "SELL"; executed_at_ms: number }[];
  }>("GET", `/v1/symbols/${encodeURIComponent(symbol)}/trades?limit=${limit}`, { auth: false });
  return r.trades.map((t) => ({
    seq: t.seq,
    price_cents: t.price_cents,
    quantity: t.quantity,
    side: t.aggressor_side,
    time_ms: t.executed_at_ms,
  }));
}

/** Newest first, as the API returns them. Pages of 100 (the API's maximum) until `limit`. */
export async function candles(symbol: string, limit = 300): Promise<Candle[]> {
  const out: Candle[] = [];
  let before: number | null = null;
  while (out.length < limit) {
    const n = Math.min(100, limit - out.length);
    const q = `limit=${n}${before != null ? `&before_ms=${before}` : ""}`;
    const r: { candles: Candle[]; next_before_ms: number | null } = await request(
      "GET",
      `/v1/symbols/${encodeURIComponent(symbol)}/candles?${q}`,
      { auth: false },
    );
    out.push(...r.candles);
    if (r.next_before_ms == null) break;
    before = r.next_before_ms;
  }
  return out;
}

// --- Notifications -----------------------------------------------------------

export function notificationsAfter(after: number, limit = 100) {
  return request<{ notifications: Notification[]; next_after: number | null }>(
    "GET",
    `/v1/notifications?after=${after}&limit=${limit}`,
  );
}

export const latestNotifications = (limit = 100) =>
  request<{ notifications: Notification[] }>("GET", `/v1/notifications?limit=${limit}`).then((r) => r.notifications);

export const wsTicket = () => request<{ ticket: string; expires_in_s: number }>("POST", "/v1/ws-ticket").then((r) => r.ticket);

export function newId(prefix: string): string {
  return `${prefix}-${crypto.randomUUID()}`;
}
