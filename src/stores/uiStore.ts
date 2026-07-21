import { create } from "zustand";
import { persist } from "zustand/middleware";

export interface ToastItem {
  id: number;
  kind: "success" | "error" | "info";
  message: string;
}

interface UiState {
  chartCode: string;
  tradeCode: string;
  settingsOpen: boolean;
  chartCollapsed: boolean;
  /** 접기 전 창 높이(logical px) — 펼칠 때 복원 */
  expandedHeight: number;
  alwaysOnTop: boolean;
  /** 예약 매도 마지막 사용 목표% (기본 0.3) — localStorage 영속 */
  reservedPct: number;
  toasts: ToastItem[];
  setChartCode: (code: string) => void;
  setTradeCode: (code: string) => void;
  setSettingsOpen: (open: boolean) => void;
  setChartCollapsed: (collapsed: boolean) => void;
  setExpandedHeight: (h: number) => void;
  setAlwaysOnTop: (on: boolean) => void;
  setReservedPct: (pct: number) => void;
  pushToast: (kind: ToastItem["kind"], message: string) => void;
  removeToast: (id: number) => void;
}

let toastSeq = 0;

export const useUiStore = create<UiState>()(
  persist(
    (set) => ({
      chartCode: "",
      tradeCode: "",
      settingsOpen: false,
      chartCollapsed: false,
      expandedHeight: 460,
      alwaysOnTop: false,
      reservedPct: 0.3,
      toasts: [],
      setChartCode: (code) => set({ chartCode: code }),
      setTradeCode: (code) => set({ tradeCode: code }),
      setSettingsOpen: (open) => set({ settingsOpen: open }),
      setChartCollapsed: (collapsed) => set({ chartCollapsed: collapsed }),
      setExpandedHeight: (h) => set({ expandedHeight: h }),
      setAlwaysOnTop: (on) => set({ alwaysOnTop: on }),
      setReservedPct: (pct) => set({ reservedPct: pct }),
      pushToast: (kind, message) => {
        const id = ++toastSeq;
        set((s) => ({ toasts: [...s.toasts, { id, kind, message }] }));
        setTimeout(() => {
          useUiStore.getState().removeToast(id);
        }, 2500);
      },
      removeToast: (id) => set((s) => ({ toasts: s.toasts.filter((t) => t.id !== id) })),
    }),
    {
      name: "quick-trade-ui",
      partialize: (s) => ({
        chartCode: s.chartCode,
        tradeCode: s.tradeCode,
        chartCollapsed: s.chartCollapsed,
        expandedHeight: s.expandedHeight,
        alwaysOnTop: s.alwaysOnTop,
        reservedPct: s.reservedPct,
      }),
    },
  ),
);
