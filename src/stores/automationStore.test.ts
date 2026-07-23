import { beforeEach, describe, expect, it } from "vitest";
import { useAutomationStore } from "./automationStore";
import type { AutomationSnapshot } from "../types";

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
    useAutomationStore.setState({ snapshot: null, loading: false, changing: false });
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
});
