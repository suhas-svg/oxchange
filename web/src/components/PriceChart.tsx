// One-minute candles: history over REST, then the current candle built from
// the live trade stream. lightweight-charts wants times in UTC seconds; the
// axis labels are formatted in the viewer's local time.

import {
  CandlestickSeries,
  ColorType,
  createChart,
  CrosshairMode,
  HistogramSeries,
  type CandlestickData,
  type IChartApi,
  type ISeriesApi,
  type Time,
  type UTCTimestamp,
} from "lightweight-charts";
import { useEffect, useRef, useState } from "react";

import { candles as loadCandles } from "../lib/api";
import { price, volume as fmtVolume } from "../lib/format";
import { useMarketStatus } from "../lib/hooks";
import { publicStream } from "../lib/publicStream";
import { cx, Panel, Spinner } from "./ui";

const UP = "#22c39a";
const DOWN = "#f0596b";

interface Bar {
  time: UTCTimestamp;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

const hm = new Intl.DateTimeFormat(undefined, { hour: "2-digit", minute: "2-digit", hour12: false });
const full = new Intl.DateTimeFormat(undefined, { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit", hour12: false });

function volumeBar(b: Bar) {
  return { time: b.time, value: b.volume, color: b.close >= b.open ? "rgba(34,195,154,0.35)" : "rgba(240,89,107,0.35)" };
}

export function PriceChart({ symbol, className }: { symbol: string; className?: string }) {
  const el = useRef<HTMLDivElement>(null);
  const chart = useRef<IChartApi | null>(null);
  const series = useRef<{ c: ISeriesApi<"Candlestick">; v: ISeriesApi<"Histogram"> } | null>(null);
  const [state, setState] = useState<"loading" | "ready" | "empty" | "error">("loading");
  const [legend, setLegend] = useState<Bar | null>(null);
  const status = useMarketStatus();

  // The chart object lives as long as the panel.
  useEffect(() => {
    if (!el.current) return;
    const c = createChart(el.current, {
      autoSize: true,
      layout: {
        background: { type: ColorType.Solid, color: "transparent" },
        textColor: "#6b7685",
        fontFamily: "JetBrains Mono Variable, ui-monospace, monospace",
        fontSize: 11,
        attributionLogo: false,
      },
      grid: { vertLines: { color: "#161d26" }, horzLines: { color: "#161d26" } },
      rightPriceScale: { borderColor: "#222b36" },
      timeScale: {
        borderColor: "#222b36",
        timeVisible: true,
        secondsVisible: false,
        rightOffset: 4,
        tickMarkFormatter: (t: Time) => hm.format(new Date((t as number) * 1000)),
      },
      crosshair: { mode: CrosshairMode.Normal },
      localization: {
        timeFormatter: (t: Time) => full.format(new Date((t as number) * 1000)),
        priceFormatter: (p: number) => price(Math.round(p * 100)),
      },
    });
    const cs = c.addSeries(CandlestickSeries, {
      upColor: UP,
      downColor: DOWN,
      wickUpColor: UP,
      wickDownColor: DOWN,
      borderVisible: false,
      priceFormat: { type: "price", precision: 2, minMove: 0.01 },
    });
    const vs = c.addSeries(HistogramSeries, { priceFormat: { type: "volume" }, priceScaleId: "", lastValueVisible: false, priceLineVisible: false });
    vs.priceScale().applyOptions({ scaleMargins: { top: 0.8, bottom: 0 } });
    chart.current = c;
    series.current = { c: cs, v: vs };
    return () => {
      c.remove();
      chart.current = null;
      series.current = null;
    };
  }, []);

  // History plus live trades for the current stock. Reloads after the
  // market stream reconnects, since trades during the gap were missed.
  useEffect(() => {
    if (status !== "live") return;
    const s = series.current;
    if (!s) return;
    let cancelled = false;
    let last: Bar | null = null;
    let loaded = false;
    const early: { ms: number; p: number; q: number }[] = [];
    setState("loading");
    setLegend(null);

    const addTrade = (ms: number, p: number, q: number) => {
      const t = (Math.floor(ms / 60_000) * 60) as UTCTimestamp;
      const px = p / 100;
      if (last && t < last.time) return; // Older than the newest candle: already counted.
      if (last && t === last.time) {
        last = { ...last, high: Math.max(last.high, px), low: Math.min(last.low, px), close: px, volume: last.volume + q };
      } else {
        last = { time: t, open: px, high: px, low: px, close: px, volume: q };
      }
      s.c.update(last as CandlestickData<Time>);
      s.v.update(volumeBar(last));
      setLegend(last);
      setState("ready");
    };

    const off = publicStream.subscribe("trades", symbol, (m) => {
      if (m.type !== "trade") return;
      const ms = Math.floor(m.time_ns / 1_000_000);
      if (!loaded) early.push({ ms, p: m.price_cents, q: m.quantity });
      else addTrade(ms, m.price_cents, m.quantity);
    });

    loadCandles(symbol, 300)
      .then((rows) => {
        if (cancelled) return;
        const bars: Bar[] = rows
          .map((r) => ({
            time: Math.floor(r.bucket_start_ms / 1000) as UTCTimestamp,
            open: r.open_cents / 100,
            high: r.high_cents / 100,
            low: r.low_cents / 100,
            close: r.close_cents / 100,
            volume: r.volume,
          }))
          .sort((a, b) => a.time - b.time);
        s.c.setData(bars as CandlestickData<Time>[]);
        s.v.setData(bars.map(volumeBar));
        last = bars.at(-1) ?? null;
        loaded = true;
        // Trades that arrived while loading may already be in the newest
        // candle; only ones from a later minute are certainly new.
        for (const e of early) {
          const t = Math.floor(e.ms / 60_000) * 60;
          if (!last || t > last.time) addTrade(e.ms, e.p, e.q);
        }
        setLegend(last);
        setState(last ? "ready" : "empty");
        chart.current?.timeScale().scrollToRealTime();
      })
      .catch(() => !cancelled && setState("error"));

    const onMove = (p: { time?: Time; seriesData: Map<unknown, unknown> }) => {
      const d = p.seriesData.get(s.c) as CandlestickData<Time> | undefined;
      const v = p.seriesData.get(s.v) as { value: number } | undefined;
      if (d && p.time) setLegend({ ...(d as Bar), volume: v?.value ?? 0 });
      else setLegend(last);
    };
    chart.current?.subscribeCrosshairMove(onMove);

    return () => {
      cancelled = true;
      off();
      chart.current?.unsubscribeCrosshairMove(onMove);
    };
  }, [symbol, status]);

  const up = legend ? legend.close >= legend.open : true;
  return (
    <Panel
      className={className}
      title={
        <span className="flex items-center gap-3">
          Price
          <span className="text-[11px] font-normal text-ink-3">1 minute</span>
        </span>
      }
      actions={
        legend && (
          <span className={cx("num hidden gap-2.5 text-[11px] sm:flex", up ? "text-up" : "text-down")}>
            <span><span className="text-ink-3">O</span> {legend.open.toFixed(2)}</span>
            <span><span className="text-ink-3">H</span> {legend.high.toFixed(2)}</span>
            <span><span className="text-ink-3">L</span> {legend.low.toFixed(2)}</span>
            <span><span className="text-ink-3">C</span> {legend.close.toFixed(2)}</span>
            <span><span className="text-ink-3">V</span> {fmtVolume(legend.volume)}</span>
          </span>
        )
      }
      bodyClassName="relative"
    >
      <div ref={el} className="absolute inset-0" />
      {state !== "ready" && (
        <div className="pointer-events-none absolute inset-0 flex items-center justify-center text-xs text-ink-3">
          {state === "loading" && <Spinner />}
          {state === "empty" && "No trades yet. The chart starts with the first one."}
          {state === "error" && "Couldn't load price history."}
        </div>
      )}
    </Panel>
  );
}
