#!/usr/bin/env python3
"""One trade through the public API, start to finish.

    python scripts/smoke.py [base_url]        (default http://localhost:3000)

Registers a seller and a buyer, gives them simulated shares and cash, crosses
one order, and waits for the trade to settle into both accounts. Exits 0
when it did. It checks that a deployment works end to end; it is not a load
test. It trades a stock whose order book is empty, so earlier orders cannot
match its own, and cancels its orders again if it fails.

Standard library only, so it runs anywhere Python 3 does (CI included).
"""

import json
import secrets
import sys
import time
import urllib.error
import urllib.request

BASE = (sys.argv[1] if len(sys.argv) > 1 else "http://localhost:3000").rstrip("/")
PREFERRED = "ACME"
SYMBOL = PREFERRED  # replaced by the stock chosen in main()
QUANTITY = 10
DEADLINE_S = 90


def call(method, path, body=None, token=None, headers=None):
    """Returns (status, parsed JSON or None). Waits out rate limits and brief outages."""
    data = None if body is None else json.dumps(body).encode()
    for _ in range(40):
        req = urllib.request.Request(BASE + path, data=data, method=method)
        req.add_header("Accept", "application/json")
        if data is not None:
            req.add_header("Content-Type", "application/json")
        if token:
            req.add_header("Authorization", f"Bearer {token}")
        for k, v in (headers or {}).items():
            req.add_header(k, v)
        try:
            with urllib.request.urlopen(req, timeout=15) as r:
                text = r.read().decode()
                return r.status, (json.loads(text) if text else None)
        except urllib.error.HTTPError as e:
            text = e.read().decode()
            if e.code == 429:
                time.sleep(min(30, int(e.headers.get("Retry-After") or 2)))
                continue
            if e.code in (502, 503, 504):
                time.sleep(2)
                continue
            try:
                return e.code, json.loads(text)
            except ValueError:
                return e.code, {"raw": text[:200]}
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            time.sleep(2)
    fail(f"{method} {path}: no answer after many tries")


# (token, order_id) of the orders placed, cancelled if the test fails so
# they do not rest on the book.
PLACED = []


def fail(message):
    print(f"SMOKE FAILED: {message}")
    for token, order_id in PLACED:
        try:
            call("DELETE", f"/v1/orders/{order_id}", token=token)
        except SystemExit:
            pass
    sys.exit(1)


def expect(status, body, wanted, what):
    if status not in wanted:
        fail(f"{what}: HTTP {status} {body}")
    return body


def new_user(label):
    email = f"smoke-{label}-{secrets.token_hex(6)}@example.test"
    password = secrets.token_urlsafe(18)
    expect(*call("POST", "/v1/auth/register", {"email": email, "password": password}), (201,), f"register {label}")
    tokens = expect(*call("POST", "/v1/auth/login", {"email": email, "password": password}), (200,), f"login {label}")
    return tokens["access_token"]


def deposit(token, asset, amount):
    expect(
        *call(
            "POST",
            "/v1/account/deposits",
            {"asset": asset, "amount_cents": amount},
            token,
            {"Idempotency-Key": f"smoke-{secrets.token_hex(8)}"},
        ),
        (200, 201),
        f"deposit {asset}",
    )


def place(token, side, price_cents):
    order = expect(
        *call(
            "POST",
            "/v1/orders",
            {
                "client_order_id": f"smoke-{secrets.token_hex(8)}",
                "symbol": SYMBOL,
                "side": side,
                "type": "LIMIT",
                "time_in_force": "GTC",
                "price_cents": price_cents,
                "quantity": QUANTITY,
            },
            token,
        ),
        (200, 202),
        f"place {side}",
    )
    PLACED.append((token, order["order_id"]))
    return order["order_id"]


def available(token, asset):
    body = expect(*call("GET", "/v1/account/balances", token=token), (200,), "balances")
    return next((b["available"] for b in body["balances"] if b["asset"] == asset), 0)


def main():
    symbols = expect(*call("GET", "/v1/symbols"), (200,), "list symbols")["symbols"]
    global SYMBOL
    symbols.sort(key=lambda s: s["symbol"] != PREFERRED)

    def book_empty(symbol):
        book = expect(*call("GET", f"/v1/symbols/{symbol}/book"), (200,), f"{symbol} book")
        return not book["bids"] and not book["asks"]

    stock = next((s for s in symbols if book_empty(s["symbol"])), None)
    if stock is None:
        fail("every listed stock has resting orders; nothing quiet to trade")
    SYMBOL = stock["symbol"]
    price = stock["last_price_cents"] or stock["listing_price_cents"]
    print(f"{len(symbols)} stocks listed; trading {QUANTITY} {SYMBOL} at {price} cents")

    seller, buyer = new_user("seller"), new_user("buyer")
    deposit(seller, SYMBOL, QUANTITY)
    deposit(buyer, "USD", price * QUANTITY)
    sell_id = place(seller, "SELL", price)
    buy_id = place(buyer, "BUY", price)
    print("orders placed; waiting for the trade to settle")

    started = time.time()
    state = ""
    while time.time() - started < DEADLINE_S:
        sell = expect(*call("GET", f"/v1/orders/{sell_id}", token=seller), (200,), "get sell order")
        buy = expect(*call("GET", f"/v1/orders/{buy_id}", token=buyer), (200,), "get buy order")
        shares, cash = available(buyer, SYMBOL), available(seller, "USD")
        state = f"sell={sell['status']} buy={buy['status']} buyer_shares={shares} seller_cash={cash}"
        if (
            sell["status"] == "FILLED"
            and buy["status"] == "FILLED"
            and shares == QUANTITY
            and cash == price * QUANTITY
        ):
            break
        if "REJECTED" in (sell["status"], buy["status"]):
            fail(f"an order was rejected: {state}")
        time.sleep(1)
    else:
        fail(f"the trade did not settle within {DEADLINE_S} s: {state}")
    print(f"settled in {time.time() - started:.1f} s: {state}")

    # These two are written by consumers of the event stream, a moment
    # after the balances change.
    problem = ""
    for _ in range(30):
        notes = expect(*call("GET", "/v1/notifications?limit=50", token=buyer), (200,), "notifications")
        kinds = {n["type"] for n in notes["notifications"]}
        trades = expect(*call("GET", f"/v1/symbols/{SYMBOL}/trades?limit=20"), (200,), "public trades")["trades"]
        if not {"fill", "settlement"} <= kinds:
            problem = f"the buyer's notifications are missing a fill or settlement: {sorted(kinds)}"
        elif not any(t["price_cents"] == price and t["quantity"] == QUANTITY for t in trades):
            problem = "the trade is missing from the public trade list"
        else:
            problem = ""
            break
        time.sleep(1)
    if problem:
        fail(problem)

    print("SMOKE PASSED")


if __name__ == "__main__":
    main()
