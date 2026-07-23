import { create } from "zustand";
import type { Quote } from "../types";

interface MarketState {
  quotes: Record<string, Quote>;
  /** 런타임 초기화 때 차트 내부 1분봉 캐시까지 폐기시키는 단조 세대값 */
  cacheGeneration: number;
  applyQuote: (q: Quote) => void;
  resetRuntimeCache: () => void;
}

export const useMarketStore = create<MarketState>((set) => ({
  quotes: {},
  cacheGeneration: 0,
  applyQuote: (q) =>
    set((s) => ({ quotes: { ...s.quotes, [q.code]: q } })),
  resetRuntimeCache: () =>
    set((s) => ({ quotes: {}, cacheGeneration: s.cacheGeneration + 1 })),
}));

export function selectQuote(code: string) {
  return (s: MarketState): Quote | undefined => s.quotes[code];
}
