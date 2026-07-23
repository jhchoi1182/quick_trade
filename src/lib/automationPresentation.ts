import type {
  AutomationDecisionStatus,
  AutomationScenario,
  DecisionMarketRegime,
  DecisionScenarioProduct,
  DecisionSetupType,
  MarketRegime,
  ScenarioProduct,
  SetupType,
} from "../types";

type AnyProduct = ScenarioProduct | DecisionScenarioProduct;
type AnySetupType = SetupType | DecisionSetupType | null | undefined;
type AnyMarketRegime = MarketRegime | DecisionMarketRegime | null | undefined;

/** 시나리오가 없는 판단을 SKIP·검증 실패·폐기 등 실제 종결 상태대로 표시한다. */
export function formatEmptyDecisionMessage(
  status: AutomationDecisionStatus | null | undefined,
  hasDecisionSummary = false,
): string {
  switch (status) {
    case "skipped":
      return "SKIP · 신규 진입 시나리오 없음";
    case "invalid":
      return "INVALID · 모든 시나리오 의미 검증 실패";
    case "discarded":
      return "판단 폐기 · 만료 또는 실행 상태 변경";
    case "error":
      return "판단 오류 · 다음 슬롯에서 재시도";
    case "missed":
      return "추격 진입 안 함 · 확인가 선행 통과";
    case "invalidated":
      return "진입 무효 · 무효화가 침범";
    case "expired":
      return "판단 만료 · 진입 조건 미확정";
    case "replaced":
      return "판단 교체 · 새 판단 대기";
    case "triggered":
      return "진입 판단이 확정되었습니다";
    case "armed":
      return "무장된 진입 조건이 없습니다";
    default:
      // decisionStatus가 없던 구 런타임은 판단 요약 존재 여부로 기존 SKIP을 복원한다.
      return hasDecisionSummary
        ? "SKIP · 신규 진입 시나리오 없음"
        : "무장된 진입 조건이 없습니다";
  }
}

/** 상품 방향과 셋업을 결합해 사용자가 실제로 감시하는 패턴을 표시한다. */
export function formatSetupLabel(product: AnyProduct, setupType: AnySetupType): string {
  const leverage = product.toUpperCase() === "LEVERAGE";
  const reversal = setupType?.toUpperCase() === "REVERSAL";
  if (leverage) return reversal ? "지지 반등" : "상승 돌파";
  return reversal ? "저항 반락" : "하락 이탈";
}

export function formatMarketRegime(regime: AnyMarketRegime): string {
  switch (regime?.toUpperCase()) {
    case "UPTREND":
      return "상승 추세";
    case "DOWNTREND":
      return "하락 추세";
    case "RANGE":
      return "박스권";
    case "TRANSITION":
      return "국면 전환";
    default:
      return "불명확";
  }
}

export function formatLiveScenarioProgress(scenario: AutomationScenario): string {
  if (scenario.status === "confirming") {
    const seconds = Math.min(3, Math.max(0, (scenario.confirmingElapsedMs ?? 0) / 1000));
    return `확인가 확인 ${seconds.toFixed(1)}초 · ${Math.min(3, scenario.confirmingTicks ?? 0)}/3틱`;
  }
  switch (scenario.status) {
    case "armed":
      if (scenario.setupType === "REVERSAL") {
        return scenario.referenceObservedAt == null
          ? "기준선 시험 대기"
          : "기준선 확인 · 반전 확인 대기";
      }
      return "확인가 대기";
    case "triggered":
      return "진입 확정";
    case "cancelledByOco":
      return "OCO 취소";
    case "expired":
      return "만료";
    case "replaced":
      return "새 판단으로 교체";
    case "missed":
      return "확인가 선행 통과 · 추격 안 함";
    case "invalidated":
      return "무효화가 침범";
    case "invalid":
      return "의미 검증 실패";
  }
}

export function formatLedgerScenarioStatus(status: string): string {
  switch (status) {
    case "armed":
      return "대기";
    case "confirming":
      return "확인 중";
    case "triggered":
      return "진입";
    case "expired":
      return "만료";
    case "replaced":
      return "교체";
    case "cancelled_by_oco":
      return "OCO 취소";
    case "missed":
      return "추격 안 함";
    case "invalidated":
      return "무효화";
    case "invalid":
      return "검증 실패";
    default:
      return status;
  }
}

export function formatDecisionStatus(status: string): string {
  switch (status) {
    case "armed":
      return "감시";
    case "skipped":
      return "SKIP";
    case "triggered":
      return "진입";
    case "expired":
      return "만료";
    case "replaced":
      return "교체";
    case "missed":
      return "추격 안 함";
    case "invalidated":
      return "무효화";
    case "invalid":
      return "검증 실패";
    case "error":
      return "오류";
    case "discarded":
      return "폐기";
    default:
      return status;
  }
}
