-- Market-order reference prices now come from Market Data's Redis ticker
-- (ticker:{symbol}), as the spec describes, so Risk no longer records the
-- last trade price itself.
ALTER TABLE symbols DROP COLUMN last_price_cents;
