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

// 슬라이더 드래그 등으로 조용한 저장이 연발되지 않게 디바운스
let silentSaveTimer: ReturnType<typeof setTimeout> | undefined;
function debouncedSave(settings: Settings): void {
  if (silentSaveTimer) clearTimeout(silentSaveTimer);
  silentSaveTimer = setTimeout(() => {
    void saveSettings(settings).catch(() => {});
  }, 400);
}

export const useSettingsStore = create<SettingsState>((set, get) => ({
  settings: null,
  load: async () => {
    const settings = await getSettings();
    set({ settings });
  },
  save: async (next) => {
    await saveSettings(next);
    set({ settings: next });
  },
  // 투명도/차트주기는 즉시 반영하고, 저장은 디바운스해 백그라운드로 시도한다.
  setOpacity: (v) => {
    const cur = get().settings;
    if (!cur) return;
    const next = { ...cur, opacity: v };
    set({ settings: next });
    debouncedSave(next);
  },
  setChartInterval: (min) => {
    const cur = get().settings;
    if (!cur) return;
    const next = { ...cur, chartInterval: min };
    set({ settings: next });
    debouncedSave(next);
  },
}));
