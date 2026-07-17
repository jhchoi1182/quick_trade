import { create } from "zustand";
import type { AccountSnapshot, Position } from "../types";

interface AccountState {
  cash: number;
  positions: Record<string, Position>;
  connected: boolean;
  applySnapshot: (snap: AccountSnapshot) => void;
  setConnected: (v: boolean) => void;
}

export const useAccountStore = create<AccountState>((set) => ({
  cash: 0,
  positions: {},
  connected: false,
  applySnapshot: (snap) =>
    set(() => ({
      cash: snap.cash,
      positions: Object.fromEntries(snap.positions.map((p) => [p.code, p])),
    })),
  setConnected: (v) => set({ connected: v }),
}));
