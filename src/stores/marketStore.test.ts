import { beforeEach, describe, expect, it } from "vitest";
import { useMarketStore } from "./marketStore";
import type { Quote } from "../types";

const quote: Quote = {
  code: "0193T0",
  price: 10_000,
  changeRate: 0,
  ask1: 10_005,
  bid1: 9_995,
  ask1Qty: 100,
  bid1Qty: 100,
  volume: 1,
  tradeTs: 1,
  bookTs: 1,
};

describe("marketStore 런타임 캐시", () => {
  beforeEach(() => {
    useMarketStore.setState({ quotes: {}, cacheGeneration: 0 });
  });

  it("재동기화 시 시세를 비우고 차트 캐시 세대를 올린다", () => {
    useMarketStore.getState().applyQuote(quote);
    useMarketStore.getState().resetRuntimeCache();

    const state = useMarketStore.getState();
    expect(state.quotes).toEqual({});
    expect(state.cacheGeneration).toBe(1);
  });
});
