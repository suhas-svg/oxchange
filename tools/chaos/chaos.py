#!/usr/bin/env python3
"""The spec's 11 failure tests, run against a live Compose stack.

    python tools/chaos/chaos.py                 # all 11, full length
    python tools/chaos/chaos.py --quick         # shorter versions of each
    python tools/chaos/chaos.py --only redis_wipe,broker_loss
    python tools/chaos/chaos.py --list

Each scenario runs under steady trading from tools/loadgen. The failure is
injected while orders flow; then traffic stops and the stack is given time to
finish (the Risk sweeper needs about 35 s). The scenario passes only if:

- the auditor passes all 11 rules (`auditor --traffic-stopped`, with the
  Kafka scan for rule 9),
- nothing is left half-done: no order stuck before the matcher, no
  unpublished outbox rows, no unsettled trades,
- and the scenario's own "must also show" check holds (spec, "Failure
  tests").

The stack is the normal docker-compose.yml plus tools/chaos/compose.chaos.yml
(Toxiproxy, three Redpanda brokers, crash points, frequent snapshots, and
PostgreSQL without synchronous commit unless CHAOS_SYNC_COMMIT=on). It
runs as its own Compose project, `oxchaos` by default, so it never touches
the development stack's data. A report is written to target/chaos/.

Standard library only. Needs Docker with about 6 GB of memory.
"""

import argparse
import datetime as dt
import json
import os
import re
import secrets
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "target" / "chaos"
GATEWAY = "http://127.0.0.1:8000"
TOXIPROXY = "http://127.0.0.1:8474"
MATCHERS = ["matching-0", "matching-1", "matching-2", "matching-3"]
APP_SERVICES = [
    "accounts", "risk", "settlement", "market-data", "notifications", "gateway", *MATCHERS,
]
RECOVERY_LIMIT_MS = 10_000
# Risk's sweeper handles orders older than 30 s; give it one more pass.
SETTLE_S = 45

ARGS = None


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def compose(*args, env=None, check=True, capture=True, timeout=600):
    cmd = [
        "docker", "compose", "-p", ARGS.project,
        "-f", str(ROOT / "docker-compose.yml"),
        "-f", str(ROOT / "tools" / "chaos" / "compose.chaos.yml"),
        *args,
    ]
    full_env = {**os.environ, **(env or {})}
    r = subprocess.run(
        cmd, cwd=ROOT, env=full_env, text=True, timeout=timeout,
        capture_output=capture, encoding="utf-8", errors="replace",
    )
    if check and r.returncode != 0:
        raise RuntimeError(f"{' '.join(args)} failed ({r.returncode}): {(r.stderr or '')[-800:]}")
    return r


def now_iso():
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def logs_since(service, since):
    r = compose("logs", "--no-log-prefix", "--since", since, service, check=False)
    return (r.stdout or "") + (r.stderr or "")


def json_lines(text):
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("{"):
            try:
                yield json.loads(line)
            except ValueError:
                pass


def psql(db, sql):
    r = compose("exec", "-T", "postgres", "psql", "-U", "postgres", "-d", db, "-tAc", sql)
    return r.stdout.strip()


def http(method, path, body=None, token=None, headers=None, base=GATEWAY, timeout=10):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(base + path, data=data, method=method)
    req.add_header("Accept", "application/json")
    if data is not None:
        req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            text = r.read().decode()
            return r.status, (json.loads(text) if text else None)
    except urllib.error.HTTPError as e:
        text = e.read().decode()
        try:
            return e.code, json.loads(text)
        except ValueError:
            return e.code, {"raw": text[:200]}
    except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as e:
        return 0, {"error": str(e)}


def wait_until(what, fn, timeout_s, every_s=1.0):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        v = fn()
        if v:
            return v
        time.sleep(every_s)
    raise RuntimeError(f"timed out after {timeout_s} s waiting for {what}")


# ------------------------------------------------------------------ load


class Load:
    """tools/loadgen in the background for `seconds`."""

    def __init__(self, name, seconds):
        self.name = name
        self.seconds = seconds
        self.started = threading.Event()
        self.output = []
        self.report = None
        out = f"/out/chaos-{name}.json"
        self.proc = subprocess.Popen(
            [
                "docker", "compose", "-p", ARGS.project,
                "-f", str(ROOT / "docker-compose.yml"),
                "-f", str(ROOT / "tools" / "chaos" / "compose.chaos.yml"),
                "run", "--rm", "-T", "--no-deps", "loadgen",
                "--rate", str(ARGS.rate), "--duration", str(seconds),
                "--users", str(ARGS.users), "--watchers", "10", "--public-clients", "2",
                "--seed", str(ARGS.seed), "--label", name, "--out", out,
            ],
            cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, encoding="utf-8", errors="replace",
        )
        self.reader = threading.Thread(target=self._read, daemon=True)
        self.reader.start()
        if not self.started.wait(timeout=300):
            self.proc.kill()
            raise RuntimeError("loadgen did not start sending: " + "".join(self.output[-20:]))
        self.t0 = time.monotonic()
        log(f"load running: {ARGS.rate} orders/s for {seconds} s")

    def _read(self):
        for line in self.proc.stdout:
            self.output.append(line)
            if line.startswith("sending "):
                self.started.set()
            if ARGS.verbose:
                sys.stdout.write("    loadgen | " + line)

    def at(self, seconds):
        """Sleeps until `seconds` into the load."""
        delay = self.t0 + seconds - time.monotonic()
        if delay > 0:
            time.sleep(delay)

    def finish(self):
        self.proc.wait(timeout=self.seconds + 600)
        self.reader.join(timeout=10)
        path = ROOT / "target" / "loadgen" / f"chaos-{self.name}.json"
        if path.exists():
            self.report = json.loads(path.read_text())
        if self.proc.returncode != 0:
            raise RuntimeError("loadgen failed: " + "".join(self.output[-20:]))
        r = self.report or {}
        log(
            f"load done: {r.get('ok')} of {r.get('finished')} ok "
            f"({r.get('ok_percent', 0):.1f}%), rejects {r.get('reject_codes')}"
        )
        return r


# ------------------------------------------------------------- checks


def audit():
    """Runs the auditor after traffic stopped; retries while work is still finishing."""
    last = ""
    for attempt in range(4):
        r = compose("run", "--rm", "--no-deps", "-T", "auditor", "--traffic-stopped", check=False)
        text = (r.stdout or "") + (r.stderr or "")
        last = text
        if r.returncode == 0:
            summary = [l.get("fields", {}) for l in json_lines(text)
                       if l.get("fields", {}).get("message") == "PASS"]
            return True, summary[-1] if summary else {"message": "PASS"}
        log(f"auditor did not pass (attempt {attempt + 1}); waiting 20 s")
        time.sleep(20)
    failures = [l.get("fields") for l in json_lines(last) if l.get("level") in ("ERROR", "WARN")]
    return False, {"failures": failures[-15:]}


def leftovers():
    """Work that must be finished once traffic has stopped."""
    stuck = psql(
        "risk",
        "SELECT count(*) FROM orders WHERE status IN ('PENDING_RISK','ACCEPTED') "
        "AND updated_at < now() - interval '60 seconds'",
    )
    found = {
        "orders_stuck_before_matcher": int(stuck or 0),
        "risk_outbox_unpublished": int(psql("risk", "SELECT count(*) FROM outbox WHERE published_at IS NULL") or 0),
        "accounts_outbox_unpublished": int(psql("accounts", "SELECT count(*) FROM outbox WHERE published_at IS NULL") or 0),
        "settlement_outbox_unpublished": int(psql("settlement", "SELECT count(*) FROM outbox WHERE published_at IS NULL") or 0),
        "trades_pending": int(psql("settlement", "SELECT count(*) FROM trades WHERE status = 'PENDING'") or 0),
    }
    return all(v == 0 for v in found.values()), found


def settle_and_check(load):
    report = load.finish()
    log(f"traffic stopped; letting the stack finish ({SETTLE_S} s)")
    time.sleep(SETTLE_S)
    clean, found = False, {}
    for _ in range(6):
        clean, found = leftovers()
        if clean:
            break
        time.sleep(10)
    audited, audit_summary = audit()
    return report, {
        "auditor_passed": audited,
        "auditor": audit_summary,
        "nothing_left_over": clean,
        "leftovers": found,
    }


# -------------------------------------------------------------- helpers


def kill(*services):
    """SIGKILL, then wait until Docker reports every container exited (a
    `start` sent sooner finds the container still running and does nothing)."""
    compose("kill", "-s", "SIGKILL", *services)

    def stopped():
        r = compose("ps", "-a", "--format", "json", *services, check=False)
        rows = [json.loads(l) for l in (r.stdout or "").splitlines() if l.strip().startswith("{")]
        return rows and all(row.get("State") in ("exited", "dead") for row in rows)
    wait_until(f"{', '.join(services)} to stop", stopped, 30, every_s=0.2)


def start(*services):
    """Start stopped containers directly. `compose start` first waits for
    every dependency to be healthy, which adds tens of seconds to each
    restart and fails outright while a broker is down."""
    r = compose("ps", "-aq", *services)
    ids = r.stdout.split()
    if not ids:
        raise RuntimeError(f"no containers for {', '.join(services)}")
    subprocess.run(["docker", "start", *ids], check=True, capture_output=True, text=True)


def restore():
    """Everything back up after a scenario, so one failure does not leave
    services down for the next."""
    r = compose("up", "-d", "--wait", check=False, timeout=600)
    if r.returncode != 0:
        log(f"  could not restore the stack: {(r.stderr or '')[-300:]}")


def matcher_ready(service, since, timeout_s=60):
    """The matcher's ready line after `since`: its recovery_ms."""
    def ready():
        for l in json_lines(logs_since(service, since)):
            f = l.get("fields", {})
            if "matcher is ready" in str(f.get("message", "")):
                return {"recovery_ms": f.get("recovery_ms")}
        return None
    return wait_until(f"{service} ready", ready, timeout_s, every_s=0.5)


def healthy(services, timeout_s=180):
    def ok():
        r = compose("ps", "--format", "json", *services, check=False)
        rows = [json.loads(l) for l in (r.stdout or "").splitlines() if l.strip().startswith("{")]
        if len(rows) < len(services):
            return False
        for row in rows:
            state, health = row.get("State"), row.get("Health", "")
            if state != "running" or health not in ("", "healthy"):
                return False
        return True
    return wait_until("services healthy", ok, timeout_s, every_s=2)


def toxic(add=None, remove=None):
    if remove:
        http("DELETE", f"/proxies/accounts/toxics/{remove}", base=TOXIPROXY)
    if add:
        s, body = http("POST", "/proxies/accounts/toxics", add, base=TOXIPROXY)
        if s not in (200, 201):
            raise RuntimeError(f"toxiproxy refused {add}: {s} {body}")


class Probe:
    """One user of its own for direct checks (market orders)."""

    def __init__(self):
        email = f"probe-{secrets.token_hex(6)}@chaos.test"
        password = secrets.token_urlsafe(18)
        s, _ = http("POST", "/v1/auth/register", {"email": email, "password": password})
        if s != 201:
            raise RuntimeError(f"probe register: {s}")
        s, body = http("POST", "/v1/auth/login", {"email": email, "password": password})
        if s != 200:
            raise RuntimeError(f"probe login: {s} {body}")
        self.token = body["access_token"]
        s, body = http(
            "POST", "/v1/account/deposits", {"amount_cents": 1_000_000_000},
            token=self.token, headers={"Idempotency-Key": secrets.token_hex(8)},
        )
        if s != 201:
            raise RuntimeError(f"probe deposit: {s} {body}")

    def market_buy(self, symbol):
        s, body = http(
            "POST", "/v1/orders",
            {"client_order_id": secrets.token_hex(8), "symbol": symbol, "side": "BUY",
             "type": "MARKET", "quantity": 1},
            token=self.token,
        )
        err = body.get("error") if isinstance(body, dict) else None
        code = err.get("code") if isinstance(err, dict) else err
        return s, code


# ------------------------------------------------------------ scenarios
# Each returns (passed, details) for its own "must also show" check.


def engine_crash():
    """Kill a matcher every 30 s for 10 minutes; each recovery under 10 s."""
    total = 120 if ARGS.quick else 600
    load = Load("engine_crash", total + 20)
    recoveries = []
    for i, at in enumerate(range(15, total, 30)):
        load.at(at)
        m = MATCHERS[i % len(MATCHERS)]
        kill(m)
        since = now_iso()
        t = time.monotonic()
        start(m)
        r = matcher_ready(m, since)
        r.update(matcher=m, wall_ms=int((time.monotonic() - t) * 1000))
        recoveries.append(r)
        log(f"  {m}: ready, recovery {r['recovery_ms']} ms (start to ready line {r['wall_ms']} ms)")
    worst = max((r["recovery_ms"] or 0) for r in recoveries)
    return load, worst < RECOVERY_LIMIT_MS, {
        "kills": len(recoveries), "worst_recovery_ms": worst, "recoveries": recoveries,
    }


def replay_crash():
    """Kill a matcher while it is recovering; the second recovery succeeds."""
    load = Load("replay_crash", 90)
    m = "matching-2"
    point = "matching::during_replay"
    load.at(10)
    kill(m)
    log(f"  {m} down for 25 s so commands pile up for its replay")
    load.at(35)
    # A replay of a few hundred commands takes milliseconds, too quick for a
    # timed kill, so a crash point ends the process on its first replayed
    # command: after the snapshot is loaded, before the matcher is ready.
    since = now_iso()
    compose("up", "-d", "--no-deps", m, env={"MATCHING_FAILPOINTS": f"{point}=1*return(exit)"})
    fired = wait_until(f"{point} to fire",
                       lambda: f"crash point {point} fired" in logs_since(m, since), 60)
    first = logs_since(m, since)
    killed_mid_recovery = bool(fired) and "starting matching engine" in first         and "matcher is ready" not in first
    since = now_iso()
    compose("up", "-d", "--no-deps", m, env={"MATCHING_FAILPOINTS": ""})
    second = matcher_ready(m, since)
    log(f"  {m}: crashed mid-replay={killed_mid_recovery}; second recovery {second['recovery_ms']} ms")
    return load, killed_mid_recovery and second["recovery_ms"] is not None, {
        "crashed_mid_replay": killed_mid_recovery, "second_recovery_ms": second["recovery_ms"],
    }


def corrupt_snapshot():
    """Damage the newest snapshot, restart; it falls back to the previous one."""
    load = Load("corrupt_snapshot", 100)
    m = "matching-1"
    load.at(60)  # several snapshots by now (SNAPSHOT_INTERVAL=500 in the overlay)
    kill(m)
    script = (
        # The root filesystem is read-only: no temporary files.
        "set -e; cd /var/lib/matching; n=$(ls -1 *.snap | wc -l); "
        "f=$(ls -1 *.snap | sort | tail -n 1); echo \"$n $f\"; "
        "printf 'CORRUPTCORRUPT' | dd of=\"$f\" bs=1 seek=40 conv=notrunc 2>/dev/null"
    )
    r = compose("run", "--rm", "--no-deps", "-T", "--entrypoint", "sh", m, "-c", script)
    count, newest = (r.stdout.strip().splitlines() or ["0 ?"])[-1].split(" ", 1)
    log(f"  {m}: {count} snapshots; damaged the newest, {newest}")
    since = now_iso()
    start(m)
    ready = matcher_ready(m, since)
    lines = list(json_lines(logs_since(m, since)))
    skipped = [l["fields"] for l in lines if l.get("fields", {}).get("message") == "skipped unusable snapshot"]
    used = [l["fields"].get("snapshot") for l in lines
            if l.get("fields", {}).get("message") == "engine state recovered"]
    fell_back = bool(skipped) and bool(used) and used[-1] is not None and newest not in str(used[-1])
    log(f"  {m}: skipped {len(skipped)}, recovered from {used[-1] if used else None}")
    return load, int(count) >= 2 and fell_back, {
        "snapshots": int(count), "damaged": newest, "skipped": skipped,
        "recovered_from": used[-1] if used else None, "recovery_ms": ready["recovery_ms"],
    }


def crash_point(service, var, point, label, seconds=90):
    load = Load(label, seconds)
    load.at(15)
    since = now_iso()
    log(f"  turning on {point} in {service}")
    compose("up", "-d", "--no-deps", service, env={var: f"{point}=1*return(exit)"})
    fired = wait_until(
        f"{point} to fire",
        lambda: f"crash point {point} fired" in logs_since(service, since), 60,
    )
    log(f"  {point} fired; {service} crashed at that exact moment")
    compose("up", "-d", "--no-deps", service, env={var: ""})
    healthy([service])
    return load, since, bool(fired)


def risk_crash():
    """Crash after Reserve, before the status save; the sweeper releases the funds."""
    load, since, fired = crash_point("risk", "RISK_FAILPOINTS", "risk::after_reserve", "risk_crash")
    report, checks = settle_and_check(load)
    swept = [l["fields"] for l in json_lines(logs_since("risk", since))
             if l.get("fields", {}).get("message") == "sweeper rejected stale orders"]
    rejected = sum(int(f.get("rejected", 0)) for f in swept)
    log(f"  sweeper rejected {rejected} stale orders")
    return None, fired and rejected >= 1, {"sweeper_rejected": rejected}, (report, checks)


def settlement_crash():
    """Crash after SettleTrade, before SETTLED; the balance changes only once."""
    load, since, fired = crash_point(
        "settlement", "SETTLEMENT_FAILPOINTS", "settlement::after_settle_trade", "settlement_crash",
    )
    report, checks = settle_and_check(load)
    # Rule 7 of the auditor checks exactly one ledger transaction per trade.
    return None, fired, {"note": "auditor rule 7: one settled record and one ledger transaction per trade"}, (report, checks)


def lost_reply():
    """Drop Accounts' replies to Risk; the reservation is voided, the order rejected."""
    load = Load("lost_reply", 80)
    since = now_iso()
    load.at(20)
    log("  dropping every reply from Accounts to Risk for 8 s")
    toxic(add={"name": "drop_replies", "type": "timeout", "stream": "downstream",
               "toxicity": 1.0, "attributes": {"timeout": 2000}})
    time.sleep(8)
    toxic(remove="drop_replies")
    report, checks = settle_and_check(load)
    lines = list(json_lines(logs_since("risk", since)))
    voiding = sum(1 for l in lines if "reserve outcome unknown" in str(l.get("fields", {}).get("message")))
    rejected = int(psql("risk", "SELECT count(*) FROM orders WHERE status = 'REJECTED' AND reject_reason <> ''") or 0)
    log(f"  Risk voided {voiding} reservations with unknown outcome")
    return None, voiding >= 1, {"reserve_outcome_unknown": voiding, "rejected_orders_total": rejected}, (report, checks)


def slow_accounts():
    """2 s of network delay to Accounts; orders rejected cleanly, no stuck funds."""
    load = Load("slow_accounts", 80)
    load.at(20)
    log("  adding 2 s of delay between Risk and Accounts for 20 s")
    toxic(add={"name": "slow", "type": "latency", "stream": "downstream",
               "toxicity": 1.0, "attributes": {"latency": 2000, "jitter": 0}})
    time.sleep(20)
    toxic(remove="slow")
    report, checks = settle_and_check(load)
    timeouts = sum(v.get("outcomes", {}).get("timeout", 0) for v in report.get("requests", {}).values())
    errors = sum(v.get("outcomes", {}).get("error", 0) for v in report.get("requests", {}).values())
    clean = timeouts == 0 and errors == 0
    log(f"  client timeouts {timeouts}, errors {errors}; rejects {report.get('reject_codes')}")
    return None, clean, {"client_timeouts": timeouts, "client_errors": errors,
                         "reject_codes": report.get("reject_codes")}, (report, checks)


def duplicates():
    """Republish 1% of Kafka messages; no duplicate effects."""
    load = Load("duplicates", 90)
    load.at(5)
    r = compose("run", "--rm", "--no-deps", "-T", "e2e", "e2e", "republish", "70", "1", str(ARGS.seed),
                timeout=300)
    line = (r.stdout.strip().splitlines() or [""])[-1]
    log(f"  {line}")
    m = re.search(r"republished (\d+)", line)
    copied = int(m.group(1)) if m else 0
    return load, copied > 0, {"republished": copied, "summary": line}


def broker_loss():
    """Kill one of three brokers; no acknowledged message is lost."""
    load = Load("broker_loss", 100)
    load.at(20)
    log("  SIGKILL redpanda-2 (one of three brokers) for 40 s")
    kill("redpanda-2")
    load.at(60)
    start("redpanda-2")
    report, checks = settle_and_check(load)
    r = compose("run", "--rm", "--no-deps", "-T", "e2e", "e2e", "verify", "300", check=False, timeout=900)
    verified = r.returncode == 0
    tail = ((r.stdout or "") + (r.stderr or "")).strip().splitlines()[-6:]
    log(f"  e2e verify: {'pass' if verified else 'FAIL'}")
    return None, verified and checks["nothing_left_over"], {
        "matching_events_verified": verified, "verify_output": tail,
    }, (report, checks)


def redis_wipe():
    """Delete and restart Redis; reduced mode, then full recovery."""
    load = Load("redis_wipe", 110)
    probe = Probe()
    load.at(20)
    symbol = "ACME"
    before = probe.market_buy(symbol)
    log(f"  market order before: {before}")
    compose("exec", "-T", "redis", "redis-cli", "FLUSHALL", check=False)
    compose("rm", "-sf", "redis")
    time.sleep(3)
    during = probe.market_buy(symbol)
    log(f"  Redis gone; market order: {during}")
    time.sleep(10)
    compose("up", "-d", "--no-deps", "redis")
    healthy(["redis"])
    t = time.monotonic()
    after = wait_until("market orders accepted again", lambda: (lambda r: r if r[0] == 202 else None)(probe.market_buy(symbol)), 90, every_s=2)
    recovered_s = round(time.monotonic() - t, 1)
    log(f"  market orders accepted again {recovered_s} s after Redis returned")
    reduced = during[0] == 503
    return load, reduced and after[0] == 202, {
        "before": before, "while_down": during, "after": after, "recovered_after_s": recovered_s,
    }


def everything_restarts():
    """Restart every service at once; full catch-up without manual repair."""
    load = Load("everything_restarts", 110)
    load.at(25)
    log(f"  SIGKILL {len(APP_SERVICES)} services at once, then start them all")
    kill(*APP_SERVICES)
    t = time.monotonic()
    start(*APP_SERVICES)
    healthy(APP_SERVICES, timeout_s=240)
    back_s = round(time.monotonic() - t, 1)
    log(f"  all services back in {back_s} s")
    return load, True, {"back_after_s": back_s}


SCENARIOS = [
    ("engine_crash", "Engine crash", engine_crash),
    ("replay_crash", "Crash during replay", replay_crash),
    ("corrupt_snapshot", "Corrupted snapshot", corrupt_snapshot),
    ("risk_crash", "Exact-moment Risk crash", risk_crash),
    ("settlement_crash", "Exact-moment settlement crash", settlement_crash),
    ("lost_reply", "Lost reservation reply", lost_reply),
    ("slow_accounts", "Slow Accounts", slow_accounts),
    ("duplicates", "Duplicate events", duplicates),
    ("broker_loss", "Broker loss", broker_loss),
    ("redis_wipe", "Redis wipe", redis_wipe),
    ("everything_restarts", "Everything restarts", everything_restarts),
]


def run_scenario(key, title, fn):
    log(f"=== {title}")
    t = time.monotonic()
    try:
        out = fn()
        if len(out) == 4:
            _, own_ok, details, (report, checks) = out
        else:
            load, own_ok, details = out
            report, checks = settle_and_check(load)
        passed = own_ok and checks["auditor_passed"] and checks["nothing_left_over"]
        error = None
    except Exception as e:  # noqa: BLE001 - a scenario that blows up is a failure, not a crash
        passed, own_ok, details, report, checks, error = False, False, {}, {}, {}, str(e)
        log(f"  ERROR: {e}")
    finally:
        restore()
    result = {
        "scenario": key, "title": title, "passed": passed, "own_check": own_ok,
        "details": details, "checks": checks, "error": error,
        "seconds": round(time.monotonic() - t),
        "load": {k: report.get(k) for k in ("scheduled", "ok", "ok_percent", "reject_codes",
                                            "order_accepted_latency")} if report else None,
    }
    log(f"=== {title}: {'PASS' if passed else 'FAIL'} "
        f"(auditor {'ok' if checks.get('auditor_passed') else 'FAILED'}, "
        f"leftovers {checks.get('leftovers')}, own check {own_ok})")
    return result


def markdown(results, started):
    rows = []
    for r in results:
        d = r["details"]
        show = {
            "engine_crash": lambda: f"{d.get('kills')} kills, worst recovery {d.get('worst_recovery_ms')} ms",
            "replay_crash": lambda: f"crashed mid-replay: {d.get('crashed_mid_replay')}; second recovery {d.get('second_recovery_ms')} ms",
            "corrupt_snapshot": lambda: f"skipped the damaged snapshot, recovered from `{Path(str(d.get('recovered_from'))).name}`",
            "risk_crash": lambda: f"sweeper rejected {d.get('sweeper_rejected')} stale orders",
            "settlement_crash": lambda: "crashed after SettleTrade; one ledger transaction per trade",
            "lost_reply": lambda: f"{d.get('reserve_outcome_unknown')} reservations voided",
            "slow_accounts": lambda: f"{d.get('client_timeouts')} client timeouts; rejects {d.get('reject_codes')}",
            "duplicates": lambda: f"{d.get('republished')} messages republished",
            "broker_loss": lambda: f"matching.events verified: {d.get('matching_events_verified')}",
            "redis_wipe": lambda: f"market order while down: {d.get('while_down')}; back after {d.get('recovered_after_s')} s",
            "everything_restarts": lambda: f"all services back in {d.get('back_after_s')} s",
        }.get(r["scenario"], lambda: "")
        try:
            shown = show()
        except Exception:  # noqa: BLE001
            shown = ""
        if r["error"]:
            shown = f"error: {r['error'][:120]}"
        load = r.get("load") or {}
        rows.append(
            f"| {r['title']} | {'PASS' if r['passed'] else 'FAIL'} | "
            f"{'pass' if r['checks'].get('auditor_passed') else 'fail'} | "
            f"{load.get('ok_percent', 0) or 0:.1f}% of {load.get('scheduled', 0)} | {shown} |"
        )
    return "\n".join([
        f"# Failure tests, {started}",
        "",
        f"Rate {ARGS.rate} orders/s, {ARGS.users} users, seed {ARGS.seed}"
        f"{', quick mode' if ARGS.quick else ''}. PostgreSQL synchronous_commit="
        f"{os.environ.get('CHAOS_SYNC_COMMIT', 'off')} (see tools/chaos/compose.chaos.yml).",
        "",
        "| Scenario | Result | Auditor | Orders ok | Must also show |",
        "|---|---|---|---|---|",
        *rows,
        "",
    ])


def main():
    global ARGS
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--project", default="oxchaos", help="Compose project name (default oxchaos)")
    p.add_argument("--rate", type=int, default=25, help="orders per second during each test")
    p.add_argument("--users", type=int, default=100)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--quick", action="store_true", help="2-minute engine crash test instead of 10")
    p.add_argument("--only", help="comma-separated scenario names")
    p.add_argument("--keep", action="store_true",
                   help="keep the project's volumes (only valid if PostgreSQL has not "
                        "stopped since the last run: its data lives in memory)")
    p.add_argument("--down", action="store_true", help="remove the project when done")
    p.add_argument("--list", action="store_true")
    p.add_argument("--verbose", action="store_true", help="show loadgen output")
    ARGS = p.parse_args()

    if ARGS.list:
        for key, title, fn in SCENARIOS:
            print(f"{key:22} {title}: {fn.__doc__}")
        return 0
    chosen = SCENARIOS
    if ARGS.only:
        names = {n.strip() for n in ARGS.only.split(",")}
        unknown = names - {k for k, _, _ in SCENARIOS}
        if unknown:
            print(f"unknown scenarios: {', '.join(sorted(unknown))}", file=sys.stderr)
            return 2
        chosen = [s for s in SCENARIOS if s[0] in names]

    OUT.mkdir(parents=True, exist_ok=True)
    (ROOT / "target" / "loadgen").mkdir(parents=True, exist_ok=True)
    started = time.strftime("%Y-%m-%d %H:%M")
    # PostgreSQL keeps its data in memory in this overlay, while Kafka and the
    # matchers' snapshots are on volumes. Once PostgreSQL has stopped, the two
    # disagree (consumers see sequence numbers jump and rightly halt), so a
    # run starts from nothing unless told otherwise.
    if not ARGS.keep:
        log(f"removing project {ARGS.project} and its volumes")
        compose("down", "-v", "--remove-orphans", check=False)
    log(f"starting the stack (project {ARGS.project})")
    for attempt in range(3):
        r = compose("up", "-d", "--wait", check=False, timeout=900)
        if r.returncode == 0:
            break
        # A fresh Postgres on a slow disk can miss its first health deadline.
        log(f"stack not healthy yet (attempt {attempt + 1}): {(r.stderr or '')[-300:]}")
    else:
        raise SystemExit("the stack did not become healthy")

    # Fresh connections and caches everywhere, so the first scenario does not
    # measure a cold start.
    for attempt in range(5):
        log("warming up (20 s of traffic)")
        warm = Load("warmup", 20).finish()
        if (warm.get("ok_percent") or 0) >= 95:
            break
        time.sleep(15)
    else:
        raise SystemExit("the stack never served steady traffic; not running the tests")

    results = []
    for key, title, fn in chosen:
        results.append(run_scenario(key, title, fn))
        (OUT / "results.json").write_text(json.dumps(results, indent=2))
    md = markdown(results, started)
    (OUT / "results.md").write_text(md, encoding="utf-8")
    print("\n" + md)
    log(f"report: {OUT / 'results.md'}")
    if ARGS.down:
        compose("down", "-v", "--remove-orphans", check=False)
    return 0 if all(r["passed"] for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
