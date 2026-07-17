export interface SymbolConfig {
  code: string;
  label: string;
  /** 호가단위 계산용: ETF/ETN은 5원 고정, 일반 주식은 가격대별 단위 */
  etf: boolean;
}

export type TradeMode = "real" | "paper" | "demo";

/** default: 기본(빨강/파랑), mono: 업무 중 위장용 무채색 */
export type ThemeName = "default" | "mono";

export interface Settings {
  appKey: string;
  appSecret: string;
  cano: string;
  acntPrdtCd: string;
  htsId: string;
  mode: TradeMode;
  theme: ThemeName;
  /** 주문 거래소: KRX 고정 또는 SOR(KRX/NXT 스마트 라우팅). 모의투자는 KRX 강제 */
  exchange: "KRX" | "SOR";
  tradeSymbols: SymbolConfig[];
  chartSymbols: SymbolConfig[];
  bufferTicks: number;
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
  volume: number;
  ts: number;
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
