import { useEffect } from "react";
import { applyAlwaysOnTop, applyChartCollapsed, TitleBar } from "./components/TitleBar";
import { MiniChart } from "./components/MiniChart";
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

export default function App() {
  useTauriEvents();
  const settings = useSettingsStore((s) => s.settings);
  const load = useSettingsStore((s) => s.load);

  useEffect(() => {
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
    if (!settings) return;
    const ui = useUiStore.getState();
    if (!settings.chartSymbols.some((s) => s.code === ui.chartCode)) {
      ui.setChartCode(settings.chartSymbols[0]?.code ?? "");
    }
    if (!settings.tradeSymbols.some((s) => s.code === ui.tradeCode)) {
      ui.setTradeCode(settings.tradeSymbols[0]?.code ?? "");
    }
    void getAccount()
      .then((snap) => useAccountStore.getState().applySnapshot(snap))
      .catch(() => {});
    void getReservations()
      .then((list) => useReservationStore.getState().hydrate(list))
      .catch(() => {});
  }, [settings]);

  if (!settings) {
    return <div className="app loading">불러오는 중…</div>;
  }

  return (
    <div className="app" data-theme={settings.theme} style={{ opacity: settings.opacity }}>
      <TitleBar />
      <MiniChart />
      <TradeBar />
      <TradeButtons />
      <ReservedSell />
      <StatusStrip />
      <SettingsModal />
      <ToastOverlay />
    </div>
  );
}
