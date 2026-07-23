import { useEffect, useState } from "react";
import type { AutomationPhase, AutomationScenario, ScenarioProduct } from "../types";
import { formatCompactKrw, formatPrice, formatRate, rateClass } from "../lib/format";
import { useAutomationStore } from "../stores/automationStore";

const EMPTY_SCENARIOS: AutomationScenario[] = [];
const KST_OFFSET_SECONDS = 9 * 60 * 60;

const PHASE_LABEL: Record<AutomationPhase, string> = {
  reconciling: "주문·잔고 확인",
  idle: "다음 판단 대기",
  analyzing: "LLM 분석 중",
  armedOco: "양방향 조건 감시",
  entryPending: "진입 주문 확인",
  holding: "포지션 관리",
  exitPending: "청산 주문 확인",
  handoff: "수동 인계 중",
  suspended: "자동 진입 중단",
};

const PRODUCT_LABEL: Record<ScenarioProduct, string> = {
  LEVERAGE: "레버리지",
  INVERSE: "곱버스",
};

function formatClock(fakeKstEpoch: number | null | undefined): string {
  if (!fakeKstEpoch) return "-";
  const date = new Date(fakeKstEpoch * 1000);
  const hour = String(date.getUTCHours()).padStart(2, "0");
  const minute = String(date.getUTCMinutes()).padStart(2, "0");
  return `${hour}:${minute}`;
}

function scenarioProgress(scenario: AutomationScenario): string {
  if (scenario.status === "confirming") {
    const seconds = Math.min(3, Math.max(0, (scenario.confirmingElapsedMs ?? 0) / 1000));
    return `${scenario.product === "LEVERAGE" ? "상단" : "하단"} 확인 ${seconds.toFixed(1)}초 · ${Math.min(3, scenario.confirmingTicks ?? 0)}/3틱`;
  }
  switch (scenario.status) {
    case "armed":
      return "조건 대기";
    case "triggered":
      return "진입 확정";
    case "cancelledByOco":
      return "OCO 취소";
    case "expired":
      return "만료";
    case "replaced":
      return "새 판단으로 교체";
    case "invalid":
      return "무효";
  }
}

function useDeadlineSeconds(deadline: number | undefined): number | undefined {
  const calculate = () => deadline === undefined
    ? undefined
    : Math.max(0, Math.ceil(deadline - (Date.now() / 1000 + KST_OFFSET_SECONDS)));
  const [remaining, setRemaining] = useState<number | undefined>(calculate);

  useEffect(() => {
    setRemaining(calculate());
    if (deadline === undefined) return;
    const timer = window.setInterval(() => setRemaining(calculate()), 1000);
    return () => window.clearInterval(timer);
  }, [deadline]);

  return remaining;
}

function ScenarioRow({ scenario }: { scenario: AutomationScenario }) {
  const isUp = scenario.product === "LEVERAGE";
  return (
    <div className={`automation-scenario ${isUp ? "up-scenario" : "down-scenario"}`}>
      <span className="scenario-arrow" aria-hidden="true">
        {isUp ? "↑" : "↓"}
      </span>
      <div className="scenario-main">
        <div className="scenario-condition">
          <b>{formatPrice(scenario.triggerPrice)}</b> 확인 시 {PRODUCT_LABEL[scenario.product]}
          <span className="scenario-target">목표 +{scenario.targetReturnPct.toFixed(1)}%</span>
        </div>
        <div className={`scenario-progress status-${scenario.status}`}>{scenarioProgress(scenario)}</div>
      </div>
    </div>
  );
}

export function AutomationPanel() {
  const mode = useAutomationStore((s) => s.snapshot?.mode ?? "manual");
  const phase = useAutomationStore((s) => s.snapshot?.phase ?? "reconciling");
  const nextDecisionAt = useAutomationStore((s) => s.snapshot?.nextDecisionAt ?? null);
  const scenarios = useAutomationStore((s) => s.snapshot?.scenarios ?? EMPTY_SCENARIOS);
  const position = useAutomationStore((s) => s.snapshot?.position ?? null);
  const shadowCash = useAutomationStore((s) => s.snapshot?.shadowCash ?? null);
  const error = useAutomationStore((s) => s.snapshot?.error ?? null);
  const marketDayStatus = useAutomationStore((s) => s.snapshot?.marketDayStatus ?? "unknown");
  const marketDayMessage = useAutomationStore((s) => s.snapshot?.marketDayMessage ?? null);
  const remainingSeconds = useDeadlineSeconds(position?.exitDeadline);
  const phaseLabel = marketDayStatus === "closed"
    ? "휴장일 · 자동 일시정지"
    : marketDayStatus === "unknown"
      ? "개장일 확인 대기 · 신규 진입 중지"
      : PHASE_LABEL[phase];

  return (
    <section className={`automation-panel ${mode === "shadow" ? "shadow" : "auto"}`} aria-live="polite">
      <div className="automation-head">
        <span className="automation-kind">{mode === "shadow" ? "SHADOW" : "AUTO"}</span>
        <span className="automation-phase">{phaseLabel}</span>
        <span className="automation-next">다음 {formatClock(nextDecisionAt)}</span>
      </div>

      {position ? (
        <div className="automation-position">
          <span>{PRODUCT_LABEL[position.product]}</span>
          <span>{position.qty.toLocaleString("ko-KR")}주</span>
          <span className={rateClass(position.pnlRate)}>{formatRate(position.pnlRate)}</span>
          <span>목표 +{position.targetReturnPct.toFixed(1)}%</span>
          {position.targetReturnPct > 0.3 ? (
            <span className="position-profit-guard">
              {position.profitGuardArmed ? "+0.3% 보호 무장" : "수익 보호 대기"}
            </span>
          ) : null}
          {remainingSeconds !== undefined ? (
            <span className="position-timer">{remainingSeconds}초</span>
          ) : null}
        </div>
      ) : null}

      {scenarios.length > 0 ? (
        <div className="automation-scenarios">
          {scenarios.slice(0, 2).map((scenario, index) => (
            <ScenarioRow key={scenario.id ?? `${scenario.product}-${index}`} scenario={scenario} />
          ))}
        </div>
      ) : !position ? (
        <div className="automation-empty">
          {phase === "analyzing" ? "현재 시세와 10·15분봉을 분석하고 있습니다" : "무장된 진입 조건이 없습니다"}
        </div>
      ) : null}

      {mode === "shadow" && shadowCash !== null ? (
        <div className="shadow-cash">가상 주문가능금액 {formatCompactKrw(shadowCash)}</div>
      ) : null}
      {error ? <div className="automation-error">{error}</div> : null}
      {marketDayStatus !== "open" && marketDayMessage ? (
        <div className="automation-market-message">{marketDayMessage}</div>
      ) : null}
    </section>
  );
}
