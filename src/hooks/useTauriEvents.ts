import { useEffect } from "react";
import { listen, type UnlistenFn } from "@tauri-apps/api/event";
import type { AccountSnapshot, ConnEvent, FillEvent, Quote } from "../types";
import { useMarketStore } from "../stores/marketStore";
import { useAccountStore } from "../stores/accountStore";
import { useUiStore } from "../stores/uiStore";
import { formatPrice } from "../lib/format";

/** Rust 엔진이 emit하는 이벤트를 스토어에 연결한다. App에서 1회만 사용. */
export function useTauriEvents(): void {
  useEffect(() => {
    const unlisteners: Promise<UnlistenFn>[] = [
      listen<Quote>("quote", (e) => {
        useMarketStore.getState().applyQuote(e.payload);
      }),
      listen<AccountSnapshot>("account", (e) => {
        useAccountStore.getState().applySnapshot(e.payload);
      }),
      listen<FillEvent>("fill", (e) => {
        const { side, qty, price } = e.payload;
        const label = side === "buy" ? "매수 체결" : "매도 체결";
        useUiStore.getState().pushToast("success", `${label} ${qty}주 @ ${formatPrice(price)}`);
      }),
      listen<ConnEvent>("conn", (e) => {
        useAccountStore.getState().setConnected(e.payload.connected);
      }),
      listen<string>("engine-error", (e) => {
        useUiStore.getState().pushToast("error", e.payload);
      }),
    ];
    return () => {
      for (const p of unlisteners) void p.then((un) => un());
    };
  }, []);
}
