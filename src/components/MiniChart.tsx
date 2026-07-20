import { useEffect, useRef, useState } from "react";
import {
  CandlestickSeries,
  createChart,
  LineSeries,
  type IChartApi,
  type ISeriesApi,
  type UTCTimestamp,
} from "lightweight-charts";
import type { Candle, Quote } from "../types";
import { aggregate, applyTick, lastMovingAverage, MA_PERIODS, movingAverage } from "../lib/candles";
import { chartColors } from "../lib/theme";
import { getCandles } from "../lib/tauri";
import { useAccountStore } from "../stores/accountStore";
import { useMarketStore } from "../stores/marketStore";
import { useSettingsStore } from "../stores/settingsStore";
import { useUiStore } from "../stores/uiStore";

const INTERVALS = [10, 15];
const VISIBLE_BARS = 60;
const CACHE_TTL_MS = 60_000;

function toCandleData(b: Candle) {
  return { time: b.time as UTCTimestamp, open: b.open, high: b.high, low: b.low, close: b.close };
}

export function MiniChart() {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const candleSeriesRef = useRef<ISeriesApi<"Candlestick"> | null>(null);
  const maSeriesRef = useRef<Map<number, ISeriesApi<"Line">>>(new Map());
  const barsRef = useRef<Candle[]>([]);
  // 종목별 1분봉 캐시: 주기 전환은 재요청 없이 재집계, 종목 복귀는 TTL 이내면 재사용.
  // 활성 종목은 실시간 틱이 캐시를 갱신하므로 항상 신선하다. (KIS 유량 절약)
  const oneMinCacheRef = useRef<Map<string, { bars: Candle[]; fetchedAt: number }>>(new Map());

  const chartCode = useUiStore((s) => s.chartCode);
  const collapsed = useUiStore((s) => s.chartCollapsed);
  const interval = useSettingsStore((s) => s.settings?.chartInterval ?? 10);
  const setChartInterval = useSettingsStore((s) => s.setChartInterval);
  const theme = useSettingsStore((s) => s.settings?.theme ?? "default");
  const palette = chartColors(theme);
  // 캐시가 전혀 없는 종목을 로드하는 동안 표시 (첫 백필은 수 초 걸릴 수 있음)
  const [loading, setLoading] = useState(false);
  // WS 재연결 시 +1 → 로드 effect 재실행 (끊긴 동안의 틱 공백을 REST 재조회로 메움)
  const [reloadKey, setReloadKey] = useState(0);

  // 순단 복구 감지: 끊긴 동안 놓친 틱은 돌아오지 않으므로 캐시를 버리고 다시 불러온다
  useEffect(() => {
    let prev = useAccountStore.getState().connected;
    return useAccountStore.subscribe((state) => {
      if (state.connected && !prev) {
        oneMinCacheRef.current = new Map();
        setReloadKey((k) => k + 1);
      }
      prev = state.connected;
    });
  }, []);

  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    // 생성 effect는 마운트 1회만 실행되므로 팔레트는 저장소에서 직접 읽는다
    // (테마 변경은 아래 테마 effect가 applyOptions로 반영)
    const colors = chartColors(useSettingsStore.getState().settings?.theme ?? "default");
    const chart = createChart(el, {
      autoSize: true,
      layout: {
        background: { color: "transparent" },
        textColor: colors.text,
        fontSize: 10,
        attributionLogo: false,
      },
      grid: {
        vertLines: { color: colors.grid },
        horzLines: { color: colors.grid },
      },
      rightPriceScale: { borderColor: colors.border },
      timeScale: {
        borderColor: colors.border,
        timeVisible: true,
        secondsVisible: false,
      },
      crosshair: { horzLine: { visible: true }, vertLine: { visible: true } },
    });
    const candle = chart.addSeries(CandlestickSeries, {
      upColor: colors.up,
      borderUpColor: colors.up,
      wickUpColor: colors.up,
      downColor: colors.down,
      borderDownColor: colors.down,
      wickDownColor: colors.down,
      priceFormat: { type: "price", precision: 0, minMove: 1 },
    });
    for (const p of MA_PERIODS) {
      maSeriesRef.current.set(
        p,
        chart.addSeries(LineSeries, {
          color: colors.ma[p],
          lineWidth: 1,
          priceLineVisible: false,
          lastValueVisible: false,
          crosshairMarkerVisible: false,
        }),
      );
    }
    chartRef.current = chart;
    candleSeriesRef.current = candle;
    return () => {
      chart.remove();
      chartRef.current = null;
      candleSeriesRef.current = null;
      maSeriesRef.current = new Map();
    };
  }, []);

  // 테마 변경 시 차트 캔버스 색을 회색조/기본색으로 전환 (차트 재생성·데이터 재요청 없음)
  useEffect(() => {
    const chart = chartRef.current;
    const candle = candleSeriesRef.current;
    if (!chart || !candle) return;
    const colors = chartColors(theme);
    chart.applyOptions({
      layout: { textColor: colors.text },
      grid: {
        vertLines: { color: colors.grid },
        horzLines: { color: colors.grid },
      },
      rightPriceScale: { borderColor: colors.border },
      timeScale: { borderColor: colors.border },
    });
    candle.applyOptions({
      upColor: colors.up,
      borderUpColor: colors.up,
      wickUpColor: colors.up,
      downColor: colors.down,
      borderDownColor: colors.down,
      wickDownColor: colors.down,
    });
    for (const p of MA_PERIODS) {
      maSeriesRef.current.get(p)?.applyOptions({ color: colors.ma[p] });
    }
  }, [theme]);

  // 종목/주기 변경 시: 캐시가 있으면 즉시 렌더(stale-while-revalidate),
  // TTL이 지난 경우에만 백그라운드 재조회 후 갱신. 실패해도 기존 화면 유지.
  useEffect(() => {
    if (!chartCode) return;
    let cancelled = false;

    const render = (oneMin: Candle[]) => {
      if (cancelled || !candleSeriesRef.current) return;
      const bars = aggregate(oneMin, interval);
      barsRef.current = bars;
      candleSeriesRef.current.setData(bars.map(toCandleData));
      for (const p of MA_PERIODS) {
        maSeriesRef.current
          .get(p)
          ?.setData(movingAverage(bars, p).map((m) => ({ time: m.time as UTCTimestamp, value: m.value })));
      }
      chartRef.current
        ?.timeScale()
        .setVisibleLogicalRange({ from: bars.length - VISIBLE_BARS, to: bars.length + 2 });
    };

    const cached = oneMinCacheRef.current.get(chartCode);
    if (cached) render(cached.bars);

    const isFresh = cached !== undefined && Date.now() - cached.fetchedAt < CACHE_TTL_MS;
    if (!isFresh) {
      if (!cached) setLoading(true);
      void (async () => {
        try {
          const oneMin = await getCandles(chartCode);
          if (cancelled) return;
          oneMinCacheRef.current.set(chartCode, { bars: oneMin, fetchedAt: Date.now() });
          render(oneMin);
        } catch (err) {
          // 이미 그려둔 캐시가 있으면 조용히 유지, 아예 없을 때만 사용자에게 알림
          if (!cached && !cancelled) {
            useUiStore.getState().pushToast("error", `차트 로드 실패: ${String(err)}`);
          }
        } finally {
          if (!cancelled) setLoading(false);
        }
      })();
    } else {
      setLoading(false);
    }
    return () => {
      cancelled = true;
    };
  }, [chartCode, interval, reloadKey]);

  // 실시간 체결 틱 → 마지막 봉/이동평균 갱신
  useEffect(() => {
    if (!chartCode) return;
    let prev: Quote | undefined;
    return useMarketStore.subscribe((state) => {
      const q = state.quotes[chartCode];
      if (!q || q === prev) return;
      prev = q;
      if (q.volume <= 0 || !candleSeriesRef.current) return;
      const tick = { price: q.price, volume: q.volume, ts: q.ts };
      // 1분봉 캐시도 함께 갱신해 주기 전환 시 재요청 없이 최신 상태를 유지한다
      const cached = oneMinCacheRef.current.get(chartCode);
      if (cached) {
        oneMinCacheRef.current.set(chartCode, {
          bars: applyTick(cached.bars, tick, 1),
          fetchedAt: Date.now(),
        });
      }
      const bars = (barsRef.current = applyTick(barsRef.current, tick, interval));
      const last = bars[bars.length - 1];
      if (!last) return;
      candleSeriesRef.current.update(toCandleData(last));
      for (const p of MA_PERIODS) {
        const point = lastMovingAverage(bars, p);
        if (point) {
          maSeriesRef.current.get(p)?.update({ time: point.time as UTCTimestamp, value: point.value });
        }
      }
    });
  }, [chartCode, interval]);

  return (
    <div className={collapsed ? "chart-wrap hidden" : "chart-wrap"}>
      <div className="chart-header">
        <div className="ma-legend">
          {MA_PERIODS.map((p) => (
            <span key={p} style={{ color: palette.ma[p] }}>
              {p}
            </span>
          ))}
        </div>
        <div className="interval-toggle">
          {INTERVALS.map((min) => (
            <button
              key={min}
              className={min === interval ? "active" : ""}
              onClick={() => setChartInterval(min)}
            >
              {min}분
            </button>
          ))}
        </div>
      </div>
      <div className="chart-body">
        <div className="chart-container" ref={containerRef} />
        {loading && <div className="chart-loading">차트 불러오는 중…</div>}
      </div>
    </div>
  );
}
