// The Gateway's JSON, as the browser sees it. Money is integer cents; stock
// amounts are whole shares.

export type Side = "BUY" | "SELL";
export type OrderType = "LIMIT" | "MARKET";
export type TimeInForce = "GTC" | "IOC";
export type OrderStatus =
  | "PENDING_RISK"
  | "ACCEPTED"
  | "OPEN"
  | "PARTIALLY_FILLED"
  | "FILLED"
  | "CANCELED"
  | "REJECTED";

export const FINAL_STATUSES: ReadonlySet<OrderStatus> = new Set(["FILLED", "CANCELED", "REJECTED"]);

export interface Tokens {
  access_token: string;
  access_expires_at: string;
  refresh_token: string;
  refresh_expires_at: string;
  user_id?: string;
}

export interface Balance {
  asset: string;
  available: number;
  reserved: number;
}

export interface Order {
  order_id: string;
  client_order_id: string;
  symbol: string;
  side: Side;
  type: OrderType;
  time_in_force: TimeInForce;
  price_cents: number;
  quantity: number;
  filled_quantity: number;
  canceled_quantity: number;
  status: OrderStatus;
  reject_reason: string | null;
  created_at: string;
  updated_at: string;
}

export interface SymbolInfo {
  symbol: string;
  name: string;
  trading: boolean;
  listing_price_cents: number;
  last_price_cents: number | null;
  day_volume: number;
  last_trade_at: string | null;
}

export interface Trade {
  seq: number;
  price_cents: number;
  quantity: number;
  /** The side of the incoming order. */
  side: Side;
  time_ms: number;
}

export interface Candle {
  bucket_start_ms: number;
  open_cents: number;
  high_cents: number;
  low_cents: number;
  close_cents: number;
  volume: number;
}

export interface Level {
  price_cents: number;
  quantity: number;
}

// --- Public stream (/ws/public) ---------------------------------------------

export type PublicMessage =
  | { type: "book_snapshot"; symbol: string; seq: number; bids: Level[]; asks: Level[] }
  | {
      type: "book_delta";
      symbol: string;
      seq: number;
      prev_seq: number;
      side: Side;
      price_cents: number;
      quantity: number;
    }
  | { type: "trade"; symbol: string; seq: number; price_cents: number; quantity: number; side: Side; time_ns: number }
  | { type: "ticker"; symbol: string; seq: number; last_price_cents: number | null; day_volume: number }
  | { type: "resync_required"; symbol: string }
  | { type: "error"; message: string };

export type Channel = "book" | "trades" | "ticker";

// --- Private stream (/ws/private) and GET /v1/notifications ----------------

export interface FillData {
  symbol: string;
  trade_id: string;
  order_id: string;
  side: Side;
  price_cents: number;
  quantity: number;
  executed_at_ms: number;
}

export interface SettlementData {
  symbol: string;
  trade_id: string;
  order_id: string;
  side: Side;
  price_cents: number;
  quantity: number;
  settled_at_ms: number;
}

export interface OrderUpdateData {
  symbol: string;
  order_id: string;
  update: "CANCELED" | "CANCEL_REJECTED";
  canceled_quantity?: number;
  reason: string;
  at_ms: number;
}

export interface AccountUpdateData {
  change: "DEPOSIT" | "WITHDRAWAL";
  asset: string;
  amount: number;
  available: number;
  at_ms: number;
}

export type Notification =
  | { type: "fill"; id: number; created_at_ms: number; data: FillData }
  | { type: "settlement"; id: number; created_at_ms: number; data: SettlementData }
  | { type: "order_update"; id: number; created_at_ms: number; data: OrderUpdateData }
  | { type: "account_update"; id: number; created_at_ms: number; data: AccountUpdateData };
