import { create } from "zustand";
import type { Reservation } from "../types";

/**
 * 걸려 있는 예약 매도(런타임 상태). 백엔드 emit("reservation")이 유일한 writer이고
 * 컴포넌트는 읽기만 한다. 영속하지 않는다 (엔진 메모리가 소스, 재시작 시 유실).
 */
interface ReservationState {
  reservations: Record<string, Reservation>;
  applyReservation: (r: Reservation) => void;
  clearReservation: (code: string) => void;
  hydrate: (list: Reservation[]) => void;
}

export const useReservationStore = create<ReservationState>((set) => ({
  reservations: {},
  applyReservation: (r) =>
    set((s) => ({ reservations: { ...s.reservations, [r.code]: r } })),
  clearReservation: (code) =>
    set((s) => {
      if (!s.reservations[code]) return s;
      const next = { ...s.reservations };
      delete next[code];
      return { reservations: next };
    }),
  hydrate: (list) =>
    set(() => ({ reservations: Object.fromEntries(list.map((r) => [r.code, r])) })),
}));
