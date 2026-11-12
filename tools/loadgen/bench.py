#!/usr/bin/env python3
"""Run the load benchmarks and write a Markdown summary.

    python tools/loadgen/bench.py sweep --rates 50,100,200,400,800 --duration 60
    python tools/loadgen/bench.py sustained --rate 200 --duration 1800
    python tools/loadgen/bench.py memory --rate 200 --duration 3600

Every run goes through `docker compose run loadgen` (inside the Compose
network, like a real client of the Gateway) and keeps its JSON report in
target/bench/. Pass the same -f/-p as the stack with --compose, e.g.
--compose "-p oxchaos -f docker-compose.yml -f tools/chaos/compose.chaos.yml".

Dependencies: Python 3.9+ standard library and Docker.
"""

import argparse
import json
import shlex
import subprocess
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "target" / "bench"


def compose_cmd(args):
    return ["docker", "compose", *shlex.split(args.compose)]


def loadgen(args, label, rate, duration, extra=()):
    """One loadgen run; returns its report."""
    out = f"/out/bench-{label}.json"
    cmd = [*compose_cmd(args), "run", "--rm", "-T", "--no-deps", "loadgen",
           "--rate", str(rate), "--duration", str(duration), "--users", str(args.users),
           "--watchers", str(args.watchers), "--public-clients", str(args.public_clients),
           "--seed", str(args.seed), "--label", label, "--out", out, *extra]
    print(f"[{time.strftime('%H:%M:%S')}] {label}: {rate}/s for {duration} s", flush=True)
    r = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True,
                       encoding="utf-8", errors="replace", timeout=duration + 900)
    if r.returncode != 0:
        raise SystemExit(f"loadgen failed ({r.returncode}): {r.stderr[-800:]}")
    report = json.loads((ROOT / "target" / "loadgen" / f"bench-{label}.json").read_text())
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / f"{label}.json").write_text(json.dumps(report, indent=2))
    return report


def p99(section):
    lat = (section or {}).get("latency", section) or {}
    return lat.get("p99_ms")


def row(r):
    acc = r.get("order_accepted_latency") or {}
    fill = (r.get("order_to_fill_notification") or {}).get("latency") or {}
    pub = (r.get("trade_to_public_message") or {}).get("latency") or {}
    prom = r.get("prometheus_p99_ms") or {}

    def f(v):
        return "–" if v is None else f"{v:,.0f}" if v >= 100 else f"{v:.1f}"
    return (f"| {r['config']['rate_per_s']} | {r['achieved_ok_per_s']:.0f} | {r['ok_percent']:.2f}% "
            f"| {f(acc.get('p50_ms'))} | {f(acc.get('p99_ms'))} | {f(fill.get('p99_ms'))} "
            f"| {f(prom.get('trade_to_settled'))} | {f(pub.get('p99_ms'))} "
            f"| {f(r.get('max_send_lateness_ms'))} |")


HEADER = ("| Target rate/s | Accepted/s | OK | Accepted p50 ms | Accepted p99 ms | "
          "Fill notification p99 ms | Trade to settled p99 ms | Public trade p99 ms | "
          "Generator lateness max ms |\n|---|---|---|---|---|---|---|---|---|")


def sweep(args):
    rows = []
    for rate in [int(x) for x in args.rates.split(",")]:
        r = loadgen(args, f"{args.label}-sweep-{rate}", rate, args.duration)
        rows.append(row(r))
        print(rows[-1], flush=True)
        if r["ok_percent"] < args.stop_below:
            print(f"ok {r['ok_percent']:.1f}% is under {args.stop_below}%: stopping the sweep")
            break
        time.sleep(args.pause)
    return "\n".join([HEADER, *rows])


def sample_memory(args, stop, samples):
    while not stop.is_set():
        r = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{.Name}} {{.MemUsage}}"],
                           capture_output=True, text=True)
        t = time.time()
        for line in r.stdout.splitlines():
            try:
                name, usage = line.split(" ", 1)
                used = usage.split("/")[0].strip()
                mib = float(used[:-3]) * {"KiB": 1 / 1024, "MiB": 1, "GiB": 1024}[used[-3:]]
            except (ValueError, KeyError):
                continue
            samples.setdefault(name, []).append((t, mib))
        stop.wait(args.sample_every)


def slope_mib_per_h(points):
    """Least-squares slope over the second half (after warm-up)."""
    pts = points[len(points) // 2:]
    if len(pts) < 3:
        return None
    n = len(pts)
    mx = sum(t for t, _ in pts) / n
    my = sum(m for _, m in pts) / n
    var = sum((t - mx) ** 2 for t, _ in pts)
    return None if var == 0 else sum((t - mx) * (m - my) for t, m in pts) / var * 3600


def long_run(args, kind):
    samples, stop = {}, threading.Event()
    th = threading.Thread(target=sample_memory, args=(args, stop, samples), daemon=True)
    th.start()
    try:
        r = loadgen(args, f"{args.label}-{kind}-{args.rate}", args.rate, args.duration)
    finally:
        stop.set()
        th.join()
    (OUT / f"{args.label}-{kind}-memory.json").write_text(json.dumps(samples))
    mem = ["| Container | Start MiB | End MiB | Trend, second half (MiB/h) |", "|---|---|---|---|"]
    for name, pts in sorted(samples.items()):
        if not pts:
            continue
        s = slope_mib_per_h(pts)
        mem.append(f"| {name} | {pts[0][1]:.0f} | {pts[-1][1]:.0f} | {'–' if s is None else f'{s:+.1f}'} |")
    t = r["timeline_ok_notok_per_s"]
    worst = min((ok for ok, _ in t[5:]), default=0)
    return "\n".join([HEADER, row(r), "",
                      f"Lowest accepted orders in any one second (after the first 5 s): {worst}.", "",
                      *mem])


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("kind", choices=["sweep", "sustained", "memory"])
    p.add_argument("--compose", default="", help="extra docker compose arguments (-p, -f)")
    p.add_argument("--rates", default="50,100,200,400,800")
    p.add_argument("--rate", type=int, default=200)
    p.add_argument("--duration", type=int, default=60)
    p.add_argument("--users", type=int, default=500)
    p.add_argument("--watchers", type=int, default=50)
    p.add_argument("--public-clients", type=int, default=20)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--label", default="run")
    p.add_argument("--stop-below", type=float, default=95.0, help="sweep: stop once ok%% drops under this")
    p.add_argument("--pause", type=int, default=20, help="sweep: seconds between steps")
    p.add_argument("--sample-every", type=int, default=30, help="memory sample period (s)")
    args = p.parse_args()
    md = sweep(args) if args.kind == "sweep" else long_run(args, args.kind)
    OUT.mkdir(parents=True, exist_ok=True)
    path = OUT / f"{args.label}-{args.kind}.md"
    path.write_text(md + "\n", encoding="utf-8")
    print("\n" + md + f"\n\nwritten to {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
