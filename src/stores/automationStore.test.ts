import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { AutomationSnapshot } from "../types";

const { getAutomationStatusMock, setControlModeMock } = vi.hoisted(() => ({
  getAutomationStatusMock: vi.fn(),
  setControlModeMock: vi.fn(),
}));

vi.mock("../lib/tauri", () => ({
  getAutomationStatus: getAutomationStatusMock,
  setControlMode: setControlModeMock,
}));

import { useAutomationStore } from "./automationStore";

function snapshot(runtimeId: string, runtimeGeneration: number, revision: number): AutomationSnapshot {
  return {
    runtimeId,
    runtimeGeneration,
    revision,
    mode: "auto",
    phase: "idle",
    nextDecisionAt: null,
    scenarios: [],
    marketDayStatus: "open",
  };
}

describe("automationStore 스냅샷 순서", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    useAutomationStore.getState().stopPolling();
    getAutomationStatusMock.mockReset();
    setControlModeMock.mockReset();
    useAutomationStore.setState({ snapshot: null, loading: false, changing: false });
  });

  afterEach(() => {
    useAutomationStore.getState().stopPolling();
    vi.useRealTimers();
  });

  it("새 엔진 B 뒤에 늦게 도착한 이전 엔진 A를 무시한다", () => {
    const store = useAutomationStore.getState();
    store.applySnapshot(snapshot("engine-a", 10, 50));
    store.applySnapshot(snapshot("engine-b", 11, 1));
    store.applySnapshot(snapshot("engine-a", 10, 999));

    expect(useAutomationStore.getState().snapshot).toMatchObject({
      runtimeId: "engine-b",
      runtimeGeneration: 11,
      revision: 1,
    });
  });

  it("같은 엔진 세대에서는 낮은 revision을 무시한다", () => {
    const store = useAutomationStore.getState();
    store.applySnapshot(snapshot("engine-a", 10, 7));
    store.applySnapshot(snapshot("engine-a", 10, 6));

    expect(useAutomationStore.getState().snapshot?.revision).toBe(7);
  });

  it("5초마다 조회하며 중복 시작과 cleanup 뒤 추가 조회를 막는다", async () => {
    getAutomationStatusMock.mockResolvedValue(snapshot("engine-a", 1, 1));
    const store = useAutomationStore.getState();
    store.startPolling();
    store.startPolling();

    await vi.advanceTimersByTimeAsync(4_999);
    expect(getAutomationStatusMock).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1);
    expect(getAutomationStatusMock).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(5_000);
    expect(getAutomationStatusMock).toHaveBeenCalledTimes(2);

    store.stopPolling();
    await vi.advanceTimersByTimeAsync(10_000);
    expect(getAutomationStatusMock).toHaveBeenCalledTimes(2);
  });

  it("조회가 진행 중이면 다음 폴링 요청을 건너뛴다", async () => {
    let resolveRequest: ((value: AutomationSnapshot) => void) | undefined;
    getAutomationStatusMock.mockImplementation(
      () =>
        new Promise<AutomationSnapshot>((resolve) => {
          resolveRequest = resolve;
        }),
    );
    useAutomationStore.getState().startPolling();

    await vi.advanceTimersByTimeAsync(5_000);
    expect(getAutomationStatusMock).toHaveBeenCalledTimes(1);
    expect(useAutomationStore.getState().loading).toBe(true);
    await vi.advanceTimersByTimeAsync(5_000);
    expect(getAutomationStatusMock).toHaveBeenCalledTimes(1);

    resolveRequest?.(snapshot("engine-a", 1, 1));
    await Promise.resolve();
    await Promise.resolve();
    expect(useAutomationStore.getState().loading).toBe(false);
  });

  it("일시 조회 실패 뒤 다음 주기에 복구한다", async () => {
    getAutomationStatusMock
      .mockRejectedValueOnce(new Error("temporary"))
      .mockResolvedValueOnce(snapshot("engine-a", 1, 2));
    useAutomationStore.getState().startPolling();

    await vi.advanceTimersByTimeAsync(5_000);
    expect(useAutomationStore.getState().snapshot).toBeNull();
    expect(useAutomationStore.getState().loading).toBe(false);

    await vi.advanceTimersByTimeAsync(5_000);
    expect(useAutomationStore.getState().snapshot).toMatchObject({
      runtimeId: "engine-a",
      revision: 2,
    });
  });
});
