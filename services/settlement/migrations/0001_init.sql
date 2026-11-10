-- Settlement database (spec: "Data storage → Settlement").

CREATE TABLE trades (
  trade_id      TEXT PRIMARY KEY,
  symbol        TEXT NOT NULL,
  seq           BIGINT NOT NULL,
  price_cents   BIGINT NOT NULL CHECK (price_cents > 0),
  quantity      BIGINT NOT NULL CHECK (quantity > 0),
  buy_order_id  UUID NOT NULL,
  sell_order_id UUID NOT NULL,
  buyer_id      UUID NOT NULL,
  seller_id     UUID NOT NULL,
  status        TEXT NOT NULL CHECK (status IN ('PENDING', 'SETTLED')),
  executed_at   TIMESTAMPTZ NOT NULL,
  settled_at    TIMESTAMPTZ,
  CHECK ((status = 'SETTLED') = (settled_at IS NOT NULL))
);
-- The settlement_pending gauge, and the auditor's per-order fill totals.
CREATE INDEX trades_pending ON trades (executed_at) WHERE status = 'PENDING';
CREATE INDEX trades_by_buy_order ON trades (buy_order_id);
CREATE INDEX trades_by_sell_order ON trades (sell_order_id);

-- settlement.events (see the outbox crate).
CREATE TABLE outbox (
  id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  topic        TEXT NOT NULL,
  message_key  TEXT NOT NULL,
  payload      BYTEA NOT NULL,
  headers      JSONB NOT NULL DEFAULT '{}',
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at TIMESTAMPTZ
);
CREATE INDEX outbox_unpublished ON outbox (id) WHERE published_at IS NULL;

-- Last matching.events seq applied per stock (see the consumer crate).
CREATE TABLE consumer_progress (
  symbol   TEXT PRIMARY KEY,
  last_seq BIGINT NOT NULL
);
