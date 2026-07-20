import { describe, expect, it } from "vitest";
import { livePnlRate } from "./pnl";

describe("livePnlRate", () => {
  it("상승 시 양수 수익률을 계산한다", () => {
    expect(livePnlRate(10_000, 10_300)).toBeCloseTo(3);
  });

  it("하락 시 음수 수익률을 계산한다", () => {
    expect(livePnlRate(10_000, 9_500)).toBeCloseTo(-5);
  });

  it("보합이면 0%다", () => {
    expect(livePnlRate(10_000, 10_000)).toBe(0);
  });

  it("평균단가가 0 이하이면 null (서버값 폴백)", () => {
    expect(livePnlRate(0, 10_000)).toBeNull();
    expect(livePnlRate(-1, 10_000)).toBeNull();
  });

  it("현재가가 없거나 0 이하이면 null (서버값 폴백)", () => {
    expect(livePnlRate(10_000, undefined)).toBeNull();
    expect(livePnlRate(10_000, 0)).toBeNull();
    expect(livePnlRate(10_000, -1)).toBeNull();
  });
});
