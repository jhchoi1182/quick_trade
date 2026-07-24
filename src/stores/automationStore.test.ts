import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { AutomationSnapshot, ControlMode } from "../types";

const { getAutomationStatusMock, setControlModeMock } = vi.hoisted(() => ({
  getAutomationStatusMock: vi.fn(),
  setControlModeMock: vi.fn(),
}));

vi.mock("../lib/tauri", () => ({
  getAutomationStatus: getAutomationStatusMock,
  setControlMode: setControlModeMock,
}));

import { useAutomationStore } from "./automationStore";

function snapshot(
  runtimeId: string,
  runtimeGeneration: number,
  revision: number,
  mode: ControlMode = "auto",
): AutomationSnapshot {
  return {
    runtimeId,
    runtimeGeneration,
    revision,
    mode,
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
    useAutomationStore.setState({ snapshot: null, loading: false, changingTo: null });
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

  it("백엔드 응답 전에는 기존 모드를 유지하고 대상 모드만 전환 중으로 표시한다", async () => {
    let resolveRequest: ((value: AutomationSnapshot) => void) | undefined;
    setControlModeMock.mockImplementation(
      () =>
        new Promise<AutomationSnapshot>((resolve) => {
          resolveRequest = resolve;
        }),
    );
    useAutomationStore.setState({
      snapshot: snapshot("engine-a", 1, 1, "manual"),
    });

    const changing = useAutomationStore.getState().changeMode("auto");

    expect(useAutomationStore.getState().snapshot?.mode).toBe("manual");
    expect(useAutomationStore.getState().changingTo).toBe("auto");
    resolveRequest?.(snapshot("engine-a", 1, 2, "auto"));
    await changing;

    expect(useAutomationStore.getState().snapshot?.mode).toBe("auto");
    expect(useAutomationStore.getState().changingTo).toBeNull();
  });

  it("화면상 이미 선택된 모드도 백엔드에 보내 실제 상태를 재확인한다", async () => {
    useAutomationStore.setState({
      snapshot: snapshot("engine-a", 1, 1, "auto"),
    });
    setControlModeMock.mockResolvedValue(snapshot("engine-a", 1, 2, "auto"));

    await useAutomationStore.getState().changeMode("auto");

    expect(setControlModeMock).toHaveBeenCalledWith("auto");
    expect(useAutomationStore.getState().snapshot?.revision).toBe(2);
  });

  it("전환 실패 후 현재 백엔드 상태를 강제 조회해 화면을 복구한다", async () => {
    useAutomationStore.setState({
      snapshot: snapshot("engine-a", 1, 1, "manual"),
    });
    setControlModeMock.mockRejectedValue(new Error("전환 실패"));
    getAutomationStatusMock.mockResolvedValue(snapshot("engine-b", 2, 1, "auto"));

    await expect(useAutomationStore.getState().changeMode("auto")).rejects.toThrow("전환 실패");

    expect(getAutomationStatusMock).toHaveBeenCalledTimes(1);
    expect(useAutomationStore.getState().snapshot?.mode).toBe("auto");
    expect(useAutomationStore.getState().changingTo).toBeNull();
  });

  it("이전 엔진의 명령 응답이면 현재 엔진 상태를 다시 조회한다", async () => {
    useAutomationStore.setState({
      snapshot: snapshot("engine-b", 11, 1, "manual"),
    });
    setControlModeMock.mockResolvedValue(snapshot("engine-a", 10, 999, "auto"));
    getAutomationStatusMock.mockResolvedValue(snapshot("engine-b", 11, 2, "auto"));

    const confirmed = await useAutomationStore.getState().changeMode("auto");

    expect(getAutomationStatusMock).toHaveBeenCalledTimes(1);
    expect(confirmed).toMatchObject({ runtimeId: "engine-b", mode: "auto", revision: 2 });
  });
});
