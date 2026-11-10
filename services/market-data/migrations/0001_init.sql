-- Market Data (spec: "Data storage → Settlement, Market Data, and
-- Notifications databases"). Public data only: everything here can be
-- rebuilt from matching.events.

CREATE TABLE candles_1m (
  symbol       TEXT NOT NULL,
  bucket_start TIMESTAMPTZ NOT NULL,
  open_cents   BIGINT NOT NULL CHECK (open_cents > 0),
  high_cents   BIGINT NOT NULL,
  low_cents    BIGINT NOT NULL,
  close_cents  BIGINT NOT NULL,
  volume       BIGINT NOT NULL CHECK (volume > 0),
  PRIMARY KEY (symbol, bucket_start),
  CHECK (low_cents <= open_cents AND open_cents <= high_cents),
  CHECK (low_cents <= close_cents AND close_cents <= high_cents)
);

CREATE TABLE market_trades (
  symbol         TEXT NOT NULL,
  seq            BIGINT NOT NULL,
  price_cents    BIGINT NOT NULL CHECK (price_cents > 0),
  quantity       BIGINT NOT NULL CHECK (quantity > 0),
  aggressor_side TEXT NOT NULL CHECK (aggressor_side IN ('BUY', 'SELL')),
  executed_at    TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (symbol, seq)
);

-- Last matching.events seq recorded per stock (see the consumer crate).
CREATE TABLE consumer_progress (
  symbol   TEXT PRIMARY KEY,
  last_seq BIGINT NOT NULL
);
