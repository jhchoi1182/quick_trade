import { describe, expect, it } from "vitest";
import { formatCompactKrw, formatPrice, formatRate, rateClass } from "./format";

describe("formatPrice", () => {
  it("천 단위 구분", () => {
    expect(formatPrice(12805)).toBe("12,805");
  });
});

describe("formatCompactKrw", () => {
  it("만/억 축약", () => {
    expect(formatCompactKrw(3_204_500)).toBe("320만");
    expect(formatCompactKrw(250_000_000)).toBe("2.50억");
    expect(formatCompactKrw(9_999)).toBe("9,999");
  });
});

describe("formatRate", () => {
  it("부호와 소수 2자리", () => {
    expect(formatRate(1.234)).toBe("+1.23%");
    expect(formatRate(-0.5)).toBe("-0.50%");
    expect(formatRate(0)).toBe("0.00%");
  });
});

describe("rateClass", () => {
  it("상승 빨강/하락 파랑 클래스", () => {
    expect(rateClass(1)).toBe("up");
    expect(rateClass(-1)).toBe("down");
    expect(rateClass(0)).toBe("flat");
  });
});
