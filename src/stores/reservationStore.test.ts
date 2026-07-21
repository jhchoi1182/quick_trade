import { beforeEach, describe, expect, it } from "vitest";
import { useReservationStore } from "./reservationStore";
import type { Reservation } from "../types";

const resv = (code: string, qty = 100): Reservation => ({
  code,
  targetPct: 0.3,
  targetPrice: 12_340,
  qty,
  status: "waiting",
});

describe("reservationStore", () => {
  beforeEach(() => {
    useReservationStore.setState({ reservations: {} });
  });

  it("applyReservation은 새 객체를 만든다 (불변)", () => {
    const before = useReservationStore.getState().reservations;
    useReservationStore.getState().applyReservation(resv("0193T0"));
    const after = useReservationStore.getState().reservations;
    expect(after).not.toBe(before);
    expect(after["0193T0"].qty).toBe(100);
  });

  it("같은 코드 재적용은 값을 덮어쓴다", () => {
    const s = useReservationStore.getState();
    s.applyReservation(resv("0193T0", 100));
    s.applyReservation(resv("0193T0", 40));
    expect(useReservationStore.getState().reservations["0193T0"].qty).toBe(40);
  });

  it("clearReservation은 해당 코드만 제거하고 새 객체를 만든다", () => {
    const s = useReservationStore.getState();
    s.applyReservation(resv("0193T0"));
    s.applyReservation(resv("0197X0"));
    const before = useReservationStore.getState().reservations;
    s.clearReservation("0193T0");
    const after = useReservationStore.getState().reservations;
    expect(after).not.toBe(before);
    expect(after["0193T0"]).toBeUndefined();
    expect(after["0197X0"]).toBeDefined();
  });

  it("없는 코드 clear는 상태 객체를 바꾸지 않는다", () => {
    const before = useReservationStore.getState().reservations;
    useReservationStore.getState().clearReservation("nope");
    expect(useReservationStore.getState().reservations).toBe(before);
  });

  it("hydrate는 목록을 코드 맵으로 교체한다", () => {
    useReservationStore.getState().hydrate([resv("0193T0"), resv("0193W0")]);
    const map = useReservationStore.getState().reservations;
    expect(Object.keys(map).sort()).toEqual(["0193T0", "0193W0"]);
  });
});
