import { create } from "zustand";
import type { Quote } from "../types";

interface MarketState {
  quotes: Record<string, Quote>;
  applyQuote: (q: Quote) => void;
}

export const useMarketStore = create<MarketState>((set) => ({
  quotes: {},
  applyQuote: (q) =>
    set((s) => ({ quotes: { ...s.quotes, [q.code]: q } })),
}));

export function selectQuote(code: string) {
  return (s: MarketState): Quote | undefined => s.quotes[code];
}
