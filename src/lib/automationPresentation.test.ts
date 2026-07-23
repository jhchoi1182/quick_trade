import { describe, expect, it } from "vitest";
import {
  formatDecisionStatus,
  formatEmptyDecisionMessage,
  formatLiveScenarioProgress,
  formatMarketRegime,
  formatSetupLabel,
} from "./automationPresentation";

describe("전문 스캘퍼 판단 표시", () => {
  it("상품 방향과 셋업 조합을 네 진입 패턴으로 표시한다", () => {
    expect(formatSetupLabel("LEVERAGE", "CONTINUATION")).toBe("상승 돌파");
    expect(formatSetupLabel("INVERSE", "CONTINUATION")).toBe("하락 이탈");
    expect(formatSetupLabel("LEVERAGE", "REVERSAL")).toBe("지지 반등");
    expect(formatSetupLabel("INVERSE", "REVERSAL")).toBe("저항 반락");
  });

  it("장부의 소문자 직렬화 값도 같은 표시 계약을 쓴다", () => {
    expect(formatSetupLabel("leverage", "reversal")).toBe("지지 반등");
    expect(formatMarketRegime("transition")).toBe("국면 전환");
  });

  it("반전 기준선 관찰 전후와 종결 상태를 구분한다", () => {
    const base = {
      product: "INVERSE" as const,
      setupType: "REVERSAL" as const,
      triggerPrice: 184_900,
      targetReturnPct: 0.3,
      status: "armed" as const,
      terminalReason: null,
    };
    expect(formatLiveScenarioProgress(base)).toBe("기준선 시험 대기");
    expect(formatLiveScenarioProgress({ ...base, referenceObservedAt: 1_000 }))
      .toBe("기준선 확인 · 반전 확인 대기");
    expect(formatLiveScenarioProgress({ ...base, status: "missed" }))
      .toContain("추격 안 함");
    expect(formatLiveScenarioProgress({ ...base, status: "invalidated" }))
      .toBe("무효화가 침범");
  });

  it("SKIP과 무효화 결정 상태를 사용자 문구로 표시한다", () => {
    expect(formatDecisionStatus("skipped")).toBe("SKIP");
    expect(formatDecisionStatus("invalidated")).toBe("무효화");
  });

  it("빈 시나리오를 판단 상태별로 구분해 실시간과 이력에 공통 표시한다", () => {
    expect(formatEmptyDecisionMessage("skipped")).toBe("SKIP · 신규 진입 시나리오 없음");
    expect(formatEmptyDecisionMessage("invalid")).toBe(
      "INVALID · 모든 시나리오 의미 검증 실패",
    );
    expect(formatEmptyDecisionMessage("discarded")).toContain("판단 폐기");
    expect(formatEmptyDecisionMessage("error")).toContain("판단 오류");
    expect(formatEmptyDecisionMessage("missed")).toContain("추격 진입 안 함");
    expect(formatEmptyDecisionMessage("invalidated")).toContain("진입 무효");
  });

  it("구 런타임의 상태 누락은 판단 요약이 있을 때만 기존 SKIP으로 복원한다", () => {
    expect(formatEmptyDecisionMessage(undefined, true)).toContain("SKIP");
    expect(formatEmptyDecisionMessage(undefined, false)).toBe("무장된 진입 조건이 없습니다");
  });
});
