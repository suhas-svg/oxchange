-- Notifications database (spec: "Data storage → Settlement, Market Data,
-- and Notifications databases").

-- One row per user per event. The unique pair makes a redelivered event a
-- no-op, so a user never gets the same message twice.
CREATE TABLE notifications (
  id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  user_id    UUID NOT NULL,
  event_id   TEXT NOT NULL,
  kind       TEXT NOT NULL CHECK (kind IN ('order_update', 'fill', 'settlement', 'account_update')),
  body       JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  read_at    TIMESTAMPTZ,
  UNIQUE (event_id, user_id)
);

-- "Everything for this user after ID n", for the live stream and for
-- GET /v1/notifications?after=.
CREATE INDEX notifications_user_id ON notifications (user_id, id);

-- One-time tickets for /ws/private. Only the SHA-256 hash is stored, so a
-- database read cannot be used to open someone's stream.
CREATE TABLE ws_tickets (
  ticket_hash BYTEA PRIMARY KEY,
  user_id     UUID NOT NULL,
  expires_at  TIMESTAMPTZ NOT NULL
);
