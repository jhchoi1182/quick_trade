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
  historyOpen: boolean;
  /** 장부 변경 이벤트를 기록 모달의 지연 재조회 신호로 변환한다. */
  historyRevision: number;
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
  setHistoryOpen: (open: boolean) => void;
  bumpHistoryRevision: () => void;
  setChartCollapsed: (collapsed: boolean) => void;
  setExpandedHeight: (h: number) => void;
  setAlwaysOnTop: (on: boolean) => void;
  setReservedPct: (pct: number) => void;
  pushToast: (kind: ToastItem["kind"], message: string) => void;
  removeToast: (id: number) => void;
}

let toastSeq = 0;

const PERSIST_KEY = "easy-scalping-ui";
const LEGACY_PERSIST_KEY = "quick-trade-ui";

// 구 quick-trade-ui 영속 데이터를 새 키로 1회 이관한다(차트/매매 종목·창 설정 보존).
if (typeof localStorage !== "undefined") {
  const legacy = localStorage.getItem(LEGACY_PERSIST_KEY);
  if (legacy !== null && localStorage.getItem(PERSIST_KEY) === null) {
    localStorage.setItem(PERSIST_KEY, legacy);
    localStorage.removeItem(LEGACY_PERSIST_KEY);
  }
}

export const useUiStore = create<UiState>()(
  persist(
    (set) => ({
      chartCode: "",
      tradeCode: "",
      settingsOpen: false,
      historyOpen: false,
      historyRevision: 0,
      chartCollapsed: false,
      expandedHeight: 460,
      alwaysOnTop: false,
      reservedPct: 0.3,
      toasts: [],
      setChartCode: (code) => set({ chartCode: code }),
      setTradeCode: (code) => set({ tradeCode: code }),
      setSettingsOpen: (open) => set({ settingsOpen: open }),
      setHistoryOpen: (open) => set({ historyOpen: open }),
      bumpHistoryRevision: () => set((s) => ({ historyRevision: s.historyRevision + 1 })),
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
      name: PERSIST_KEY,
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
