import { create } from "zustand";
import type { Settings } from "../types";
import { getSettings, saveSettings } from "../lib/tauri";

interface SettingsState {
  settings: Settings | null;
  load: () => Promise<void>;
  save: (next: Settings) => Promise<void>;
  setOpacity: (v: number) => void;
  setChartInterval: (min: number) => void;
}

// 슬라이더 드래그 등으로 조용한 저장이 연발되지 않게 디바운스.
// 예약 시점의 객체를 캡처하지 않고 발사 시점의 최신 스토어 상태를 저장한다 —
// 그 사이 모달 저장(엔진 재시작 포함)이 있었을 때 옛 설정으로 되덮는 경합 방지.
let silentSaveTimer: ReturnType<typeof setTimeout> | undefined;

function cancelSilentSave(): void {
  if (silentSaveTimer) {
    clearTimeout(silentSaveTimer);
    silentSaveTimer = undefined;
  }
}

function scheduleSilentSave(): void {
  cancelSilentSave();
  silentSaveTimer = setTimeout(() => {
    silentSaveTimer = undefined;
    const latest = useSettingsStore.getState().settings;
    if (latest) void saveSettings(latest).catch(() => {});
  }, 400);
}

export const useSettingsStore = create<SettingsState>((set, get) => ({
  settings: null,
  load: async () => {
    const settings = await getSettings();
    set({ settings });
  },
  save: async (next) => {
    cancelSilentSave(); // 예약된 조용한 저장이 이 저장을 되덮지 않게 취소
    await saveSettings(next);
    set({ settings: next });
  },
  // 투명도/차트주기는 즉시 반영하고, 저장은 디바운스해 백그라운드로 시도한다.
  setOpacity: (v) => {
    const cur = get().settings;
    if (!cur) return;
    set({ settings: { ...cur, opacity: v } });
    scheduleSilentSave();
  },
  setChartInterval: (min) => {
    const cur = get().settings;
    if (!cur) return;
    set({ settings: { ...cur, chartInterval: min } });
    scheduleSilentSave();
  },
}));
