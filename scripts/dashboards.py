#!/usr/bin/env python3
"""Writes the four Grafana dashboards from the spec ("Observability →
Grafana dashboards") as JSON, for Grafana to load at startup.

    python scripts/dashboards.py

Edit the panels here, not the JSON: the files are regenerated whole.
Output: deploy/helm/oxchange/files/observability/dashboards/*.json
"""

import json
import pathlib

OUT = pathlib.Path(__file__).resolve().parent.parent / "deploy/helm/oxchange/files/observability/dashboards"
PROM = {"type": "prometheus", "uid": "prometheus"}


def q(expr, legend=""):
    return {"expr": expr, "legendFormat": legend, "datasource": PROM}


def series(title, targets, unit="short", w=12, h=8, desc="", stack=False):
    return {
        "type": "timeseries",
        "title": title,
        "description": desc,
        "datasource": PROM,
        "targets": targets,
        "gridPos": {"w": w, "h": h},
        "fieldConfig": {
            "defaults": {
                "unit": unit,
                "custom": {
                    "lineWidth": 1,
                    "fillOpacity": 10,
                    "showPoints": "never",
                    "stacking": {"mode": "normal" if stack else "none"},
                },
            },
            "overrides": [],
        },
        "options": {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}},
    }


def stat(title, expr, legend="", unit="short", w=6, h=4, desc="", bad_above=None, good=None):
    steps = [{"color": "green", "value": None}]
    if bad_above is not None:
        steps.append({"color": "red", "value": bad_above})
    if good is not None:
        # 1 = good (green), anything less = red.
        steps = [{"color": "red", "value": None}, {"color": "green", "value": good}]
    return {
        "type": "stat",
        "title": title,
        "description": desc,
        "datasource": PROM,
        "targets": [q(expr, legend)],
        "gridPos": {"w": w, "h": h},
        "fieldConfig": {"defaults": {"unit": unit, "thresholds": {"mode": "absolute", "steps": steps}}, "overrides": []},
        "options": {"colorMode": "background", "graphMode": "none", "reduceOptions": {"calcs": ["lastNotNull"]}},
    }


def quantiles(metric, by="", qs=(0.5, 0.99), window="5m", extra=""):
    group = f"le{', ' + by if by else ''}"
    out = []
    for p in qs:
        label = f"p{int(p * 100) if p * 100 == int(p * 100) else p * 100}"
        legend = f"{label} {{{{{by}}}}}" if by else label
        out.append(q(f"histogram_quantile({p}, sum by ({group}) (rate({metric}_bucket{extra}[{window}])))", legend))
    return out


def dashboard(uid, title, description, panels):
    # Lay panels out left to right, wrapping at 24 columns.
    x = y = row_h = 0
    for i, p in enumerate(panels, 1):
        w, h = p["gridPos"]["w"], p["gridPos"]["h"]
        if x + w > 24:
            x, y, row_h = 0, y + row_h, 0
        p["gridPos"].update({"x": x, "y": y})
        p["id"] = i
        x += w
        row_h = max(row_h, h)
    return {
        "uid": uid,
        "title": title,
        "description": description,
        "tags": ["oxchange"],
        "timezone": "browser",
        "refresh": "10s",
        "time": {"from": "now-30m", "to": "now"},
        "schemaVersion": 39,
        "editable": True,
        "panels": panels,
    }


SERVER_FAULTS = 'code=~"UNKNOWN|DEADLINE_EXCEEDED|UNIMPLEMENTED|INTERNAL|UNAVAILABLE|DATA_LOSS"'

overview = dashboard(
    "oxchange-overview",
    "Oxchange: system overview",
    "Order rate, error rate, end-to-end latency and service health.",
    [
        stat("Firing alerts", 'count(ALERTS{alertstate="firing"}) or vector(0)', bad_above=1),
        stat("Services up", 'sum(up)', desc="Scrape targets answering"),
        stat("Services down", 'count(up == 0) or vector(0)', bad_above=1),
        stat("Orders/s", 'sum(rate(orders_received_total[1m]))', unit="reqps"),
        series("Orders received by result", [q("sum by (result) (rate(orders_received_total[1m]))", "{{result}}")],
               unit="reqps", stack=True, desc="Risk's answer to PlaceOrder"),
        series("End-to-end order latency", quantiles("order_end_to_end_seconds", qs=(0.5, 0.99, 0.999)),
               unit="s", desc="Order saved by Risk to the matcher's answer reaching Risk. Alert: p99 over 100 ms for 5 minutes."),
        series("Public API latency by route (p99)", quantiles("http_request_duration_seconds", by="route", qs=(0.99,),
               extra='{service="gateway"}'), unit="s", desc="Gateway, request to response"),
        series("Error rate", [
            q('sum(rate(http_requests_total{service="gateway", status=~"5.."}[1m])) / sum(rate(http_requests_total{service="gateway"}[1m]))', "public API 5xx"),
            q(f'sum by (service) (rate(grpc_requests_total{{{SERVER_FAULTS}}}[1m])) / sum by (service) (rate(grpc_requests_total[1m]))', "gRPC faults {{service}}"),
        ], unit="percentunit", desc="Server faults only; refused orders are not errors"),
        series("Requests by service", [
            q("sum by (service) (rate(grpc_requests_total[1m]))", "gRPC {{service}}"),
            q("sum by (service) (rate(http_requests_total[1m]))", "HTTP {{service}}"),
        ], unit="reqps"),
        series("WebSocket clients", [
            q("sum by (service) (websocket_clients)", "{{service}}"),
            q("sum by (service) (rate(websocket_dropped_slow_total[5m]))", "dropped slow/s {{service}}"),
        ]),
        series("Memory per service", [q("sum by (service) (process_resident_memory_bytes)", "{{service}}")],
               unit="bytes", desc="Target: no growth trend over an hour of steady load"),
        series("CPU per service", [q("sum by (service) (rate(process_cpu_seconds_total[1m]))", "{{service}}")],
               unit="percentunit"),
    ],
)

matching = dashboard(
    "oxchange-matching",
    "Oxchange: matching engine",
    "Throughput, per-command latency, input lag and recovery time.",
    [
        stat("Matchers ready", "sum(matcher_ready)", desc="Partitions with a live engine past replay"),
        stat("Commands/s", "sum(rate(matcher_commands_total[1m]))", unit="ops"),
        stat("Input lag", "sum(matcher_input_lag)", desc="Commands waiting, all partitions", bad_above=10000),
        stat("Last recovery", "max(matcher_recovery_seconds_sum)", unit="s",
             desc="Restart to ready, including replay (target: at most 10 s for 100,000 commands)", bad_above=10),
        series("Throughput by partition", [q("sum by (instance) (rate(matcher_commands_total[1m]))", "{{instance}}")], unit="ops"),
        series("Commands by type", [q("sum by (type) (rate(matcher_commands_total[1m]))", "{{type}}")], unit="ops", stack=True),
        series("Per-command processing time", quantiles("matcher_command_seconds", qs=(0.5, 0.99, 0.999), window="1m"),
               unit="s", desc="Time in the engine for one command (target p99 at most 10 µs)"),
        series("Input lag by partition", [q("matcher_input_lag", "{{instance}}")],
               desc="Commands in orders.commands after the one being processed"),
        series("Recovery time by partition", [q("matcher_recovery_seconds_sum", "{{instance}}")], unit="s"),
        series("Duplicates ignored", [q("sum by (instance) (rate(matcher_duplicates_total[5m]))", "{{instance}}")], unit="ops"),
    ],
)

money = dashboard(
    "oxchange-money-safety",
    "Oxchange: money safety",
    "Settlement delay, pending trades, sequence gaps and ledger imbalance.",
    [
        stat("Ledger imbalance", "max(ledger_imbalance_total) or vector(0)", bad_above=1,
             desc="Broken auditor rules in its last run; must stay 0"),
        stat("Sequence gaps", "sum(consumer_seq_gaps_total) or vector(0)", bad_above=1,
             desc="Missing matching.events; must stay 0"),
        stat("Halted partitions", "sum(consumer_halted) or vector(0)", bad_above=1),
        stat("Since last audit", "time() - max(auditor_last_run_timestamp_seconds)", unit="s",
             desc="The CronJob runs every 5 minutes"),
        series("Trade to final balance", quantiles("settlement_seconds", qs=(0.5, 0.99)), unit="s",
               desc="Trade executed to SETTLED (target p99 at most 250 ms)"),
        series("Pending trades", [
            q("sum(settlement_pending)", "pending"),
            q("max(settlement_oldest_pending_seconds)", "oldest (s)"),
        ], desc="Alert: a trade waiting over 60 s"),
        series("Trades settled", [q("sum(rate(settlement_seconds_count[1m]))", "settled/s")], unit="ops"),
        series("Auditor result", [
            q('ledger_imbalance_total', "broken rules {{mode}}"),
            q('auditor_rules_skipped', "skipped {{mode}}"),
        ]),
        series("Consumer retries", [q("sum by (service, topic) (rate(consumer_retries_total[5m]))", "{{service}} {{topic}}")], unit="ops"),
    ],
)

kafka_db = dashboard(
    "oxchange-kafka-db",
    "Oxchange: Kafka and database",
    "Consumer lag, outbox depth, connection use and slow queries.",
    [
        series("Consumer lag", [q("max by (service, topic, partition) (consumer_lag)", "{{service}} {{topic}}/{{partition}}")],
               desc="Alert: more than 50,000 for 2 minutes"),
        series("Outbox waiting", [q("max by (service) (outbox_unpublished)", "{{service}}")],
               desc="Alert: more than 10,000 for 1 minute"),
        series("Outbox to Kafka (p99)", quantiles("outbox_publish_seconds", by="topic", qs=(0.99,)), unit="s",
               desc="Saved in the outbox to confirmed by Kafka"),
        series("Pool connections in use", [
            q('sum by (service) (db_pool_connections{state="in_use"})', "in use {{service}}"),
            q("max by (service) (db_pool_max_connections)", "max {{service}}"),
        ]),
        series("Slow queries (over 1 s)", [q("sum by (service) (rate(db_slow_queries_total[5m]))", "{{service}}")], unit="ops"),
        series("PostgreSQL connections by database", [q("sum by (datname) (pg_stat_activity_count)", "{{datname}}")]),
        series("Longest open transaction", [q("max by (datname) (pg_stat_activity_max_tx_duration)", "{{datname}}")], unit="s"),
        series("gRPC latency by method (p99)", quantiles("grpc_request_duration_seconds", by="method", qs=(0.99,)), unit="s"),
    ],
)

OUT.mkdir(parents=True, exist_ok=True)
for name, d in [("overview", overview), ("matching", matching), ("money-safety", money), ("kafka-db", kafka_db)]:
    (OUT / f"{name}.json").write_text(json.dumps(d, indent=1) + "\n", encoding="utf-8", newline="\n")
    print(f"wrote {name}.json ({len(d['panels'])} panels)")
