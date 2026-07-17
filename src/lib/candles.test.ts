import { describe, expect, it } from "vitest";
import type { Candle } from "../types";
import { aggregate, applyTick, lastMovingAverage, movingAverage } from "./candles";

function bar(time: number, close: number, volume = 10): Candle {
  return { time, open: close - 2, high: close + 3, low: close - 5, close, volume };
}

const MIN = 60;

describe("aggregate", () => {
  it("1분봉을 10분봉으로 묶는다", () => {
    const t0 = 1_000_000_800; // 10분 경계
    const oneMin = [
      bar(t0, 100),
      bar(t0 + MIN, 105),
      bar(t0 + 9 * MIN, 110),
      bar(t0 + 10 * MIN, 120), // 다음 버킷
    ];
    const out = aggregate(oneMin, 10);
    expect(out).toHaveLength(2);
    expect(out[0].open).toBe(98); // 첫 봉의 open
    expect(out[0].close).toBe(110); // 버킷 마지막 봉의 close
    expect(out[0].high).toBe(113);
    expect(out[0].low).toBe(95);
    expect(out[0].volume).toBe(30);
    expect(out[1].time).toBe(t0 + 10 * MIN);
  });

  it("빈 입력은 빈 배열", () => {
    expect(aggregate([], 10)).toEqual([]);
  });
});

describe("applyTick", () => {
  const t0 = 1_000_000_800;
  const bars = aggregate([bar(t0, 100)], 10);

  it("같은 버킷이면 마지막 봉을 갱신하고 원본은 불변", () => {
    const next = applyTick(bars, { price: 130, volume: 5, ts: t0 + 5 * MIN }, 10);
    expect(next).toHaveLength(1);
    expect(next[0].close).toBe(130);
    expect(next[0].high).toBe(130);
    expect(next[0].volume).toBe(15);
    expect(bars[0].close).toBe(100); // 불변성
  });

  it("새 버킷이면 봉을 추가한다", () => {
    const next = applyTick(bars, { price: 90, volume: 5, ts: t0 + 10 * MIN }, 10);
    expect(next).toHaveLength(2);
    expect(next[1].open).toBe(90);
    expect(next[1].time).toBe(t0 + 10 * MIN);
  });

  it("과거 버킷 틱은 무시한다", () => {
    const next = applyTick(bars, { price: 90, volume: 5, ts: t0 - 10 * MIN }, 10);
    expect(next).toHaveLength(1);
    expect(next[0].close).toBe(100);
  });
});

describe("movingAverage", () => {
  const bars = [bar(0, 10), bar(MIN, 20), bar(2 * MIN, 30), bar(3 * MIN, 40)];

  it("period 미만 구간은 생략한다", () => {
    const ma = movingAverage(bars, 3);
    expect(ma).toHaveLength(2);
    expect(ma[0]).toEqual({ time: 2 * MIN, value: 20 });
    expect(ma[1]).toEqual({ time: 3 * MIN, value: 30 });
  });

  it("데이터 부족이면 빈 배열", () => {
    expect(movingAverage(bars, 5)).toEqual([]);
  });

  it("lastMovingAverage는 마지막 점과 일치한다", () => {
    const full = movingAverage(bars, 3);
    expect(lastMovingAverage(bars, 3)).toEqual(full[full.length - 1]);
    expect(lastMovingAverage(bars, 5)).toBeNull();
  });
});
