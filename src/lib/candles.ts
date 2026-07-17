import type { Candle } from "../types";

/** 1분봉 배열을 intervalMin(10/15) 분봉으로 집계한다. 입력은 time 오름차순이어야 한다. */
export function aggregate(oneMin: readonly Candle[], intervalMin: number): Candle[] {
  const bucketSec = intervalMin * 60;
  const out: Candle[] = [];
  for (const c of oneMin) {
    const bucket = Math.floor(c.time / bucketSec) * bucketSec;
    const last = out[out.length - 1];
    if (last && last.time === bucket) {
      out[out.length - 1] = {
        time: bucket,
        open: last.open,
        high: Math.max(last.high, c.high),
        low: Math.min(last.low, c.low),
        close: c.close,
        volume: last.volume + c.volume,
      };
    } else {
      out.push({ ...c, time: bucket });
    }
  }
  return out;
}

/**
 * 실시간 체결 틱을 집계봉 배열에 반영한 새 배열을 반환한다.
 * 같은 버킷이면 마지막 봉을 갱신, 새 버킷이면 봉을 추가한다.
 */
export function applyTick(
  bars: readonly Candle[],
  tick: { price: number; volume: number; ts: number },
  intervalMin: number,
): Candle[] {
  const bucketSec = intervalMin * 60;
  const bucket = Math.floor(tick.ts / bucketSec) * bucketSec;
  const last = bars[bars.length - 1];
  if (last && last.time === bucket) {
    const updated: Candle = {
      time: bucket,
      open: last.open,
      high: Math.max(last.high, tick.price),
      low: Math.min(last.low, tick.price),
      close: tick.price,
      volume: last.volume + tick.volume,
    };
    return [...bars.slice(0, -1), updated];
  }
  if (last && bucket < last.time) return [...bars]; // 늦게 도착한 과거 틱은 무시
  return [
    ...bars,
    { time: bucket, open: tick.price, high: tick.price, low: tick.price, close: tick.price, volume: tick.volume },
  ];
}

export interface MaPoint {
  time: number;
  value: number;
}

/** 단순이동평균. 데이터가 period 미만인 구간은 생략한다. */
export function movingAverage(bars: readonly Candle[], period: number): MaPoint[] {
  if (bars.length < period) return [];
  const out: MaPoint[] = [];
  let sum = 0;
  for (let i = 0; i < bars.length; i++) {
    sum += bars[i].close;
    if (i >= period) sum -= bars[i - period].close;
    if (i >= period - 1) out.push({ time: bars[i].time, value: sum / period });
  }
  return out;
}

/** 마지막 봉 기준 이동평균 1점만 계산 (실시간 갱신용). 데이터 부족 시 null. */
export function lastMovingAverage(bars: readonly Candle[], period: number): MaPoint | null {
  if (bars.length < period) return null;
  let sum = 0;
  for (let i = bars.length - period; i < bars.length; i++) sum += bars[i].close;
  return { time: bars[bars.length - 1].time, value: sum / period };
}

export const MA_PERIODS = [5, 20, 60, 120] as const;
