export interface SymbolConfig {
  code: string;
  label: string;
  /** 호가단위 계산용: ETF/ETN은 5원 고정, 일반 주식은 가격대별 단위 */
  etf: boolean;
}

export type ControlMode = "manual" | "auto" | "shadow";
export type MarketDayStatus = "open" | "closed" | "unknown";

export interface AutoSymbols {
  underlying: string;
  leverage: string;
  inverse: string;
}

/** default: 기본(빨강/파랑), mono: 업무 중 위장용 무채색 */
export type ThemeName = "default" | "mono";

export interface Settings {
  configVersion: number;
  appKey: string;
  appSecret: string;
  cano: string;
  acntPrdtCd: string;
  htsId: string;
  realTradingConfirmed: boolean;
  openaiApiKey: string;
  autoSymbols: AutoSymbols;
  theme: ThemeName;
  /** 주문 거래소: KRX 고정 또는 SOR(KRX/NXT 스마트 라우팅) */
  exchange: "KRX" | "SOR";
  tradeSymbols: SymbolConfig[];
  chartSymbols: SymbolConfig[];
  chartInterval: number;
  opacity: number;
}

export interface Candle {
  /** epoch seconds (KST 기준 그대로 초 단위 변환) */
  time: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

export interface Quote {
  code: string;
  price: number;
  changeRate: number;
  ask1: number;
  bid1: number;
  ask1Qty: number;
  bid1Qty: number;
  volume: number;
  /** 기존 호환용 시세 시각. 새 엔진에서는 tradeTs를 우선한다. */
  ts?: number;
  tradeTs: number;
  bookTs: number;
}

export interface Position {
  code: string;
  qty: number;
  avgPrice: number;
  evalPnl: number;
  pnlRate: number;
}

export interface AccountSnapshot {
  cash: number;
  positions: Position[];
}

export interface OrderResult {
  ok: boolean;
  side: "buy" | "sell";
  code: string;
  qty: number;
  price: number;
  message: string;
}

export interface FillEvent {
  code: string;
  side: "buy" | "sell";
  qty: number;
  price: number;
}

export interface ConnEvent {
  connected: boolean;
}

export type ReservationStatus = "waiting" | "filled" | "cancelled";

/** 예약 매도 상태 (백엔드 emit("reservation") / get_reservations 페이로드) */
export interface Reservation {
  code: string;
  /** 목표 수익률(%) — 평단 기준 */
  targetPct: number;
  /** 계산된 목표 지정가(호가단위 올림) */
  targetPrice: number;
  qty: number;
  status: ReservationStatus;
  /** 취소 사유 등 사용자 안내 (없을 수 있음) */
  reason?: string;
}

export type AutomationPhase =
  | "reconciling"
  | "idle"
  | "analyzing"
  | "armedOco"
  | "entryPending"
  | "holding"
  | "exitPending"
  | "handoff"
  | "suspended";

export type ScenarioProduct = "LEVERAGE" | "INVERSE";

export type ScenarioStatus =
  | "armed"
  | "confirming"
  | "triggered"
  | "expired"
  | "replaced"
  | "cancelledByOco"
  | "invalid";

export interface AutomationScenario {
  id?: string | number;
  code?: string;
  product: ScenarioProduct;
  triggerPrice: number;
  targetReturnPct: number;
  status: ScenarioStatus;
  confirmingElapsedMs?: number;
  confirmingTicks?: number;
}

export interface AutomationPosition {
  code: string;
  product: ScenarioProduct;
  qty: number;
  avgPrice: number;
  pnlRate: number;
  targetReturnPct: number;
  targetPrice?: number;
  exitDeadline?: number;
  shadow?: boolean;
  profitGuardArmed: boolean;
}

/** 백엔드가 emit("automation-state") 및 조회 명령으로 전달하는 전체 스냅샷 */
export interface AutomationSnapshot {
  runtimeId: string;
  /** 같은 프로세스에서 재생성된 엔진의 단조 세대값 */
  runtimeGeneration: number;
  mode: ControlMode;
  phase: AutomationPhase;
  revision?: number;
  nextDecisionAt: number | null;
  decisionId?: number | null;
  groupId?: string | number | null;
  scenarios: AutomationScenario[];
  position?: AutomationPosition | null;
  shadowCash?: number | null;
  error?: string | null;
  marketDayStatus: MarketDayStatus;
  marketDayMessage?: string | null;
}

export type TradeRecordKind = "real" | "shadow";
export type TradeOrigin = "manual" | "auto" | "auto_handoff" | "shadow" | "external";

export interface TradeRecord {
  id: number;
  tradeId: string;
  sessionId?: string | null;
  executionKind: TradeRecordKind;
  origin: TradeOrigin;
  code: string;
  entryQty: number;
  exitQty: number;
  entryAvgPrice: number;
  exitAvgPrice?: number | null;
  pnlRate?: number | null;
  enteredAt: number;
  exitedAt?: number | null;
  status: "open" | "closed" | "handed_off" | "interrupted";
  exitReason?: string | null;
  updatedAt: number;
}

export type DecisionScenarioProduct = "leverage" | "inverse";

export interface DecisionScenarioRecord {
  id: number;
  decisionId: string;
  product: DecisionScenarioProduct;
  triggerPrice: number;
  targetReturnPct: number;
  status: "armed" | "confirming" | "triggered" | "expired" | "replaced" | "cancelled_by_oco" | "invalid";
  confirmationStartedAt?: number | null;
  confirmationTickCount: number;
  updatedAt: number;
}

export interface LlmDecisionRecord {
  id: number;
  decisionId: string;
  sessionId?: string | null;
  controlMode: ControlMode;
  revision: number;
  asOfTs: number;
  expiresAt: number;
  underlyingPrice: number;
  status: "armed" | "skipped" | "triggered" | "expired" | "replaced" | "invalid" | "error" | "discarded";
  model: string;
  promptVersion: string;
  inputTokens: number;
  cachedInputTokens: number;
  cacheWriteTokens: number;
  outputTokens: number;
  reasoningTokens: number;
  latencyMs: number;
  inputHash?: string | null;
  chartHash?: string | null;
  error?: string | null;
  createdAt: number;
  scenarios: DecisionScenarioRecord[];
}

export type HistoryCursor = string | number | null;

export interface CursorPage<T> {
  items: T[];
  nextCursor: HistoryCursor;
}
