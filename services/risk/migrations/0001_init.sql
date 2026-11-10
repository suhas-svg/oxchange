-- Risk database (spec: "Data storage → Risk database").
--
-- Differences from the spec's sketch:
-- * `symbols` lists the tradable stocks, their listing prices, and the last
--   trade price. The spec keeps the last price in Redis (ticker:{symbol},
--   written by Market Data, which arrives in Phase 3). Until then Risk
--   records it from the trades it already reads, in the same transaction.

CREATE TABLE symbols (
  symbol              TEXT PRIMARY KEY,
  name                TEXT NOT NULL,
  listing_price_cents BIGINT NOT NULL CHECK (listing_price_cents > 0),
  last_price_cents    BIGINT CHECK (last_price_cents > 0),
  trading             BOOLEAN NOT NULL DEFAULT true
);

-- Simulated stocks; none is a real company.
INSERT INTO symbols (symbol, name, listing_price_cents) VALUES
  ('ACME', 'Acme Simulated Holdings', 1000),
  ('BOLT', 'Bolt Simulated Motors', 2500),
  ('CRUX', 'Crux Simulated Labs', 4200),
  ('DYNA', 'Dyna Simulated Power', 1875),
  ('ECHO', 'Echo Simulated Media', 950),
  ('FLUX', 'Flux Simulated Energy', 3310),
  ('GRID', 'Grid Simulated Utilities', 6400),
  ('HALO', 'Halo Simulated Health', 12050),
  ('IONX', 'Ionx Simulated Materials', 780),
  ('JADE', 'Jade Simulated Retail', 2290),
  ('KILO', 'Kilo Simulated Foods', 5115),
  ('LUMA', 'Luma Simulated Optics', 8820),
  ('MESA', 'Mesa Simulated Mining', 1430),
  ('NOVA', 'Nova Simulated Aerospace', 23500),
  ('OPAL', 'Opal Simulated Finance', 4675),
  ('PIKE', 'Pike Simulated Logistics', 3050),
  ('QUAD', 'Quad Simulated Robotics', 15990),
  ('RUBY', 'Ruby Simulated Software', 7340),
  ('SOLR', 'Solr Simulated Solar', 2105),
  ('TIDE', 'Tide Simulated Shipping', 1260);

CREATE TABLE orders (
  order_id          UUID PRIMARY KEY,
  user_id           UUID NOT NULL,
  client_order_id   TEXT NOT NULL,
  symbol            TEXT NOT NULL REFERENCES symbols(symbol),
  side              TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
  order_type        TEXT NOT NULL CHECK (order_type IN ('LIMIT', 'MARKET')),
  time_in_force     TEXT NOT NULL CHECK (time_in_force IN ('GTC', 'IOC')),
  limit_price_cents BIGINT NOT NULL,             -- protected price for market orders
  quantity          BIGINT NOT NULL CHECK (quantity > 0),
  filled_quantity   BIGINT NOT NULL DEFAULT 0 CHECK (filled_quantity >= 0),
  canceled_quantity BIGINT NOT NULL DEFAULT 0 CHECK (canceled_quantity >= 0),
  status            TEXT NOT NULL CHECK (status IN
                      ('PENDING_RISK', 'ACCEPTED', 'OPEN', 'PARTIALLY_FILLED',
                       'FILLED', 'CANCELED', 'REJECTED')),
  reject_reason     TEXT,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (user_id, client_order_id),
  CHECK (filled_quantity + canceled_quantity <= quantity)
);
CREATE INDEX open_orders_by_user ON orders (user_id)
  WHERE status IN ('ACCEPTED', 'OPEN', 'PARTIALLY_FILLED');
CREATE INDEX orders_by_user ON orders (user_id, order_id DESC);
CREATE INDEX pending_orders ON orders (created_at) WHERE status = 'PENDING_RISK';

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

CREATE TABLE consumer_progress (
  symbol   TEXT PRIMARY KEY,
  last_seq BIGINT NOT NULL
);
