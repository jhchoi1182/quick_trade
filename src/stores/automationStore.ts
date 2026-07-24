import { create } from "zustand";
import { getAutomationStatus, setControlMode } from "../lib/tauri";
import type { AutomationSnapshot, ControlMode } from "../types";

interface AutomationState {
  snapshot: AutomationSnapshot | null;
  loading: boolean;
  changingTo: ControlMode | null;
  hydrate: () => Promise<void>;
  startPolling: () => void;
  stopPolling: () => void;
  applySnapshot: (snapshot: AutomationSnapshot) => void;
  changeMode: (mode: ControlMode) => Promise<AutomationSnapshot>;
}

const AUTOMATION_POLL_INTERVAL_MS = 5_000;
let automationPollTimer: ReturnType<typeof setInterval> | undefined;

function keepNewest(current: AutomationSnapshot | null, next: AutomationSnapshot): AutomationSnapshot {
  if (!current) return next;
  if (next.runtimeGeneration !== current.runtimeGeneration) {
    return next.runtimeGeneration < current.runtimeGeneration ? current : next;
  }

  const currentRevision = current.revision ?? 0;
  const nextRevision = next.revision ?? 0;
  return nextRevision < currentRevision ? current : next;
}

export const useAutomationStore = create<AutomationState>((set, get) => ({
  snapshot: null,
  loading: false,
  changingTo: null,
  hydrate: async () => {
    if (get().loading) return;
    set({ loading: true });
    try {
      const snapshot = await getAutomationStatus();
      set((state) => ({ snapshot: keepNewest(state.snapshot, snapshot) }));
    } finally {
      set({ loading: false });
    }
  },
  startPolling: () => {
    if (automationPollTimer !== undefined) return;
    automationPollTimer = setInterval(() => {
      void get().hydrate().catch(() => {
        // 이벤트가 기본 경로다. 일시 조회 실패는 마지막 snapshot을 유지하고
        // 다음 폴링에서 다시 동기화한다.
      });
    }, AUTOMATION_POLL_INTERVAL_MS);
  },
  stopPolling: () => {
    if (automationPollTimer === undefined) return;
    clearInterval(automationPollTimer);
    automationPollTimer = undefined;
  },
  applySnapshot: (snapshot) => {
    set((state) => ({ snapshot: keepNewest(state.snapshot, snapshot) }));
  },
  changeMode: async (mode) => {
    if (get().changingTo !== null) {
      throw new Error("다른 모드 전환이 진행 중입니다");
    }
    set({ changingTo: mode });
    try {
      const snapshot = await setControlMode(mode);
      let accepted = false;
      set((state) => {
        const newest = keepNewest(state.snapshot, snapshot);
        accepted = newest === snapshot;
        return { snapshot: newest };
      });
      if (!accepted) {
        const current = await getAutomationStatus();
        set((state) => ({ snapshot: keepNewest(state.snapshot, current) }));
      }
      return get().snapshot ?? snapshot;
    } catch (error) {
      try {
        const current = await getAutomationStatus();
        set((state) => ({ snapshot: keepNewest(state.snapshot, current) }));
      } catch {
        // 원래 전환 오류를 유지한다. 다음 이벤트·폴링에서도 상태를 복구한다.
      }
      throw error;
    } finally {
      set((state) => (state.changingTo === mode ? { changingTo: null } : {}));
    }
  },
}));
