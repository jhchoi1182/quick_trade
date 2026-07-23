import { lazy, Suspense, useEffect, useRef } from "react";
import { applyAlwaysOnTop, applyChartCollapsed, TitleBar } from "./components/TitleBar";
import { MiniChart } from "./components/MiniChart";
import { ModeBar } from "./components/ModeBar";
import { AutomationPanel } from "./components/AutomationPanel";
import { TradeBar } from "./components/TradeBar";
import { TradeButtons } from "./components/TradeButtons";
import { ReservedSell } from "./components/ReservedSell";
import { StatusStrip } from "./components/StatusStrip";
import { SettingsModal } from "./components/SettingsModal";
import { ToastOverlay } from "./components/Toast";
import { useTauriEvents } from "./hooks/useTauriEvents";
import { useSettingsStore } from "./stores/settingsStore";
import { useUiStore } from "./stores/uiStore";
import { getAccount, getReservations } from "./lib/tauri";
import { useAccountStore } from "./stores/accountStore";
import { useReservationStore } from "./stores/reservationStore";
import { useAutomationStore } from "./stores/automationStore";

const HistoryModal = lazy(() => import("./components/HistoryModal"));

export default function App() {
  useTauriEvents();
  const settingsReady = useSettingsStore((s) => s.settings !== null);
  const chartSymbols = useSettingsStore((s) => s.settings?.chartSymbols);
  const tradeSymbols = useSettingsStore((s) => s.settings?.tradeSymbols);
  const theme = useSettingsStore((s) => s.settings?.theme ?? "default");
  const opacity = useSettingsStore((s) => s.settings?.opacity ?? 1);
  const load = useSettingsStore((s) => s.load);
  const hydrateAutomation = useAutomationStore((s) => s.hydrate);
  const controlMode = useAutomationStore((s) => s.snapshot?.mode ?? "manual");
  const historyOpen = useUiStore((s) => s.historyOpen);
  const bootstrapped = useRef(false);
  const runtimeHydrated = useRef(false);

  useEffect(() => {
    if (bootstrapped.current) return;
    bootstrapped.current = true;
    void load();
    // 지난 세션의 창 상태 복원 (차트 접힘 → 창 높이, 맨위로 고정)
    const ui = useUiStore.getState();
    if (ui.chartCollapsed) {
      void applyChartCollapsed(true).catch(() => {});
    }
    if (ui.alwaysOnTop) {
      void applyAlwaysOnTop(true).catch(() => {});
    }
  }, [load]);

  // 설정 로드 후 셀렉터 기본값 보정 (저장된 선택이 리스트에 없으면 첫 항목으로)
  useEffect(() => {
    if (!chartSymbols || !tradeSymbols) return;
    const ui = useUiStore.getState();
    if (!chartSymbols.some((s) => s.code === ui.chartCode)) {
      ui.setChartCode(chartSymbols[0]?.code ?? "");
    }
    if (!tradeSymbols.some((s) => s.code === ui.tradeCode)) {
      ui.setTradeCode(tradeSymbols[0]?.code ?? "");
    }
  }, [chartSymbols, tradeSymbols]);

  // 설정의 투명도 같은 UI 값이 바뀔 때 계좌/예약을 재조회하지 않도록 런타임 초기화는 1회만 한다.
  useEffect(() => {
    if (!settingsReady || runtimeHydrated.current) return;
    runtimeHydrated.current = true;
    void Promise.allSettled([
      getAccount().then((snap) => useAccountStore.getState().applySnapshot(snap)),
      getReservations().then((list) => useReservationStore.getState().hydrate(list)),
      hydrateAutomation(),
    ]);
  }, [hydrateAutomation, settingsReady]);

  if (!settingsReady) {
    return <div className="app loading">불러오는 중…</div>;
  }

  return (
    <div className="app" data-theme={theme} style={{ opacity }}>
      <TitleBar />
      <ModeBar />
      <MiniChart />
      {controlMode === "auto" ? (
        <AutomationPanel />
      ) : (
        <>
          <TradeBar />
          <TradeButtons />
          <ReservedSell />
          {controlMode === "shadow" ? <AutomationPanel /> : null}
        </>
      )}
      <StatusStrip />
      <SettingsModal />
      {historyOpen ? (
        <Suspense fallback={<div className="modal-backdrop"><div className="history-loading">기록 불러오는 중…</div></div>}>
          <HistoryModal />
        </Suspense>
      ) : null}
      <ToastOverlay />
    </div>
  );
}
