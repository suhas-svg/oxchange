-- Accounts database (spec: "Data storage → Accounts database").
--
-- The ledger is double-entry: every transaction's entries add up to zero per
-- asset, so money can move but cannot appear or vanish. Deposits and
-- withdrawals balance against the external account for that asset.
--
-- Differences from the spec's sketch, all additive:
-- * ledger_transactions keeps a hash of the request and the encoded first
--   response, so a retry returns the first result and a reused key with
--   different data is refused (IDEMPOTENCY_CONFLICT).
-- * A voided reservation is written by a Release that does not know the
--   order's user or asset, so those columns are NULL only for voided rows.
-- * The outbox table (shared pattern) carries accounts.events.

CREATE TABLE users (
  id            UUID PRIMARY KEY,
  email         TEXT NOT NULL UNIQUE,
  password_hash TEXT NOT NULL,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE refresh_tokens (
  id         UUID PRIMARY KEY,
  user_id    UUID NOT NULL REFERENCES users(id),
  token_hash BYTEA NOT NULL UNIQUE,
  expires_at TIMESTAMPTZ NOT NULL,
  revoked_at TIMESTAMPTZ
);
CREATE INDEX refresh_tokens_by_user ON refresh_tokens (user_id) WHERE revoked_at IS NULL;

CREATE TABLE ledger_accounts (
  id       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  owner_id UUID REFERENCES users(id),            -- NULL for the external account
  asset    TEXT NOT NULL,                        -- 'USD' or a stock symbol
  kind     TEXT NOT NULL CHECK (kind IN ('available', 'reserved', 'external')),
  balance  BIGINT NOT NULL DEFAULT 0,
  UNIQUE NULLS NOT DISTINCT (owner_id, asset, kind),
  CHECK ((kind = 'external') = (owner_id IS NULL)),
  -- PostgreSQL itself refuses a negative user balance, even if the code has a bug.
  CHECK (kind = 'external' OR balance >= 0)
);

CREATE TABLE ledger_transactions (
  id              UUID PRIMARY KEY,
  idempotency_key TEXT NOT NULL UNIQUE,          -- for example settle:ACME-1042
  kind            TEXT NOT NULL,
  request_hash    BYTEA NOT NULL,
  response        BYTEA,                         -- set before commit
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE ledger_entries (
  id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  transaction_id UUID NOT NULL REFERENCES ledger_transactions(id),
  account_id     BIGINT NOT NULL REFERENCES ledger_accounts(id),
  amount         BIGINT NOT NULL CHECK (amount <> 0)  -- signed
);
CREATE INDEX ON ledger_entries (account_id, id);
CREATE INDEX ON ledger_entries (transaction_id);

CREATE TABLE reservations (
  order_id          UUID PRIMARY KEY,
  user_id           UUID REFERENCES users(id),
  asset             TEXT,
  limit_price_cents BIGINT,                      -- NULL for sell orders
  original_amount   BIGINT NOT NULL,
  remaining_amount  BIGINT NOT NULL CHECK (remaining_amount >= 0),
  status            TEXT NOT NULL CHECK (status IN ('active', 'closed', 'voided')),
  CHECK (status = 'voided' OR (user_id IS NOT NULL AND asset IS NOT NULL)),
  CHECK (remaining_amount <= original_amount)
);
CREATE INDEX reservations_active ON reservations (user_id, asset) WHERE status = 'active';

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
