import { create } from "zustand";
import { getAutomationStatus, setControlMode } from "../lib/tauri";
import type { AutomationSnapshot, ControlMode } from "../types";

interface AutomationState {
  snapshot: AutomationSnapshot | null;
  loading: boolean;
  changing: boolean;
  hydrate: () => Promise<void>;
  applySnapshot: (snapshot: AutomationSnapshot) => void;
  changeMode: (mode: ControlMode) => Promise<void>;
}

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
  changing: false,
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
  applySnapshot: (snapshot) => {
    set((state) => ({ snapshot: keepNewest(state.snapshot, snapshot) }));
  },
  changeMode: async (mode) => {
    if (get().changing || get().snapshot?.mode === mode) return;
    set({ changing: true });
    try {
      const snapshot = await setControlMode(mode);
      set((state) => ({ snapshot: keepNewest(state.snapshot, snapshot) }));
    } finally {
      set({ changing: false });
    }
  },
}));
