use serde::{Deserialize, Serialize};

fn default_true() -> bool {
    true
}

fn default_theme() -> String {
    "default".into()
}

fn default_exchange() -> String {
    "KRX".into()
}

fn default_account_product_code() -> String {
    "01".into()
}

fn default_config_version() -> u32 {
    2
}

fn default_chart_interval() -> u32 {
    10
}

fn default_opacity() -> f64 {
    1.0
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymbolConfig {
    pub code: String,
    pub label: String,
    /// ETF/ETN이면 호가단위 5원 고정, 아니면 KRX 가격대별 호가단위
    #[serde(default = "default_true")]
    pub etf: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ControlMode {
    #[default]
    Manual,
    Auto,
    Shadow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MarketDayStatus {
    Open,
    Closed,
    #[default]
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ProductKind {
    Leverage,
    Inverse,
}

impl ProductKind {
    pub fn code<'a>(&self, symbols: &'a AutoSymbols) -> &'a str {
        match self {
            Self::Leverage => &symbols.leverage,
            Self::Inverse => &symbols.inverse,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AutoSymbols {
    pub underlying: String,
    pub leverage: String,
    pub inverse: String,
}

impl Default for AutoSymbols {
    fn default() -> Self {
        Self {
            underlying: "000660".into(),
            leverage: "0193T0".into(),
            inverse: "0197X0".into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Settings {
    #[serde(default = "default_config_version")]
    pub config_version: u32,
    #[serde(default)]
    pub app_key: String,
    #[serde(default)]
    pub app_secret: String,
    #[serde(default)]
    pub cano: String,
    #[serde(default = "default_account_product_code")]
    pub acnt_prdt_cd: String,
    #[serde(default)]
    pub hts_id: String,
    /// 구 demo/paper 설정이 조용히 실전에 연결되지 않도록 하는 1회 확인값.
    #[serde(default)]
    pub real_trading_confirmed: bool,
    /// OpenAI 키도 기존 KIS 키와 같은 편의 우선 정책으로 설정 파일에 저장한다.
    #[serde(default)]
    pub openai_api_key: String,
    #[serde(default)]
    pub auto_symbols: AutoSymbols,
    /// "default"(기본 색상) | "mono"(무채색 위장 테마) — UI에서만 해석
    #[serde(default = "default_theme")]
    pub theme: String,
    /// 주문 거래소: "KRX" | "SOR"(스마트 라우팅)
    #[serde(default = "default_exchange")]
    pub exchange: String,
    #[serde(default)]
    pub trade_symbols: Vec<SymbolConfig>,
    #[serde(default)]
    pub chart_symbols: Vec<SymbolConfig>,
    #[serde(default = "default_chart_interval")]
    pub chart_interval: u32,
    #[serde(default = "default_opacity")]
    pub opacity: f64,
}

impl Default for Settings {
    fn default() -> Self {
        let sym = |code: &str, label: &str, etf: bool| SymbolConfig {
            code: code.into(),
            label: label.into(),
            etf,
        };
        Self {
            config_version: default_config_version(),
            app_key: String::new(),
            app_secret: String::new(),
            cano: String::new(),
            acnt_prdt_cd: "01".into(),
            hts_id: String::new(),
            real_trading_confirmed: false,
            openai_api_key: String::new(),
            auto_symbols: AutoSymbols::default(),
            theme: default_theme(),
            exchange: default_exchange(),
            trade_symbols: vec![
                sym("0193T0", "하이닉스 레버리지", true),
                sym("0197X0", "하이닉스 곱버스", true),
                sym("0193W0", "삼전 레버리지", true),
                sym("0193L0", "삼전 곱버스", true),
            ],
            chart_symbols: vec![
                sym("000660", "SK하이닉스", false),
                sym("005930", "삼성전자", false),
            ],
            chart_interval: 10,
            opacity: 1.0,
        }
    }
}

impl Settings {
    pub fn all_codes(&self) -> Vec<String> {
        let mut codes: Vec<String> = self
            .chart_symbols
            .iter()
            .chain(self.trade_symbols.iter())
            .map(|s| s.code.clone())
            .collect();
        codes.extend([
            self.auto_symbols.underlying.clone(),
            self.auto_symbols.leverage.clone(),
            self.auto_symbols.inverse.clone(),
        ]);
        codes.sort();
        codes.dedup();
        codes
    }

    pub fn is_etf(&self, code: &str) -> bool {
        if code == self.auto_symbols.leverage || code == self.auto_symbols.inverse {
            return true;
        }
        if code == self.auto_symbols.underlying {
            return false;
        }
        self.trade_symbols
            .iter()
            .chain(self.chart_symbols.iter())
            .find(|s| s.code == code)
            .map(|s| s.etf)
            .unwrap_or(true)
    }
}

/// time은 KST 벽시계 시각을 UTC epoch처럼 취급한 "가짜 epoch" 초.
/// 차트 라이브러리가 UTC로 렌더링하므로 이렇게 해야 한국 시간이 그대로 표시된다.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Candle {
    pub time: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

/// tradeTs/bookTs도 Candle.time과 같은 가짜 epoch 초 단위.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Quote {
    pub code: String,
    pub price: f64,
    pub change_rate: f64,
    pub ask1: f64,
    pub bid1: f64,
    pub ask1_qty: u64,
    pub bid1_qty: u64,
    /// 이 틱의 체결량. 호가만 갱신된 경우 0 (차트는 volume 0 틱을 무시)
    pub volume: f64,
    /// 거래소 누적거래량 기반 체결 식별 순번. 동일 프레임 재전송은 같은 값이다.
    #[serde(skip_serializing)]
    pub trade_sequence: u64,
    /// WebSocket 프레임 파싱 직후의 프로세스 단조시각(마이크로초).
    #[serde(skip_serializing)]
    pub received_at_micros: u64,
    pub trade_ts: i64,
    pub book_ts: i64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Position {
    pub code: String,
    pub qty: u64,
    pub avg_price: f64,
    pub eval_pnl: f64,
    pub pnl_rate: f64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountSnapshot {
    pub cash: u64,
    pub positions: Vec<Position>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderResult {
    pub ok: bool,
    pub side: Side,
    pub code: String,
    pub qty: u64,
    pub price: u64,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub local_order_id: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FillEvent {
    pub fill_id: String,
    pub order_no: String,
    pub original_order_no: String,
    pub org_no: String,
    pub code: String,
    pub side: Side,
    pub qty: u64,
    pub price: f64,
    pub filled_at: i64,
    pub status: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ScenarioStatus {
    Armed,
    Confirming,
    Triggered,
    Expired,
    Replaced,
    CancelledByOco,
    Invalid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum AutomationPhase {
    Reconciling,
    Idle,
    Analyzing,
    ArmedOco,
    EntryPending,
    Holding,
    ExitPending,
    Handoff,
    Suspended,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelScenario {
    pub product: ProductKind,
    pub trigger_price: u64,
    pub target_return_pct: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelDecision {
    pub scenarios: Vec<ModelScenario>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AutomationScenarioInfo {
    pub id: i64,
    pub product: ProductKind,
    pub code: String,
    pub trigger_price: u64,
    pub target_return_pct: f64,
    pub status: ScenarioStatus,
    pub confirming_ticks: u32,
    pub confirming_elapsed_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AutomationPositionInfo {
    pub product: ProductKind,
    pub code: String,
    pub qty: u64,
    pub avg_price: f64,
    pub pnl_rate: f64,
    pub target_return_pct: f64,
    pub target_price: u64,
    pub exit_deadline: i64,
    pub shadow: bool,
    pub profit_guard_armed: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AutomationSnapshot {
    pub runtime_id: String,
    pub runtime_generation: u64,
    pub mode: ControlMode,
    pub phase: AutomationPhase,
    pub revision: u64,
    pub next_decision_at: Option<i64>,
    pub decision_id: Option<i64>,
    pub group_id: Option<i64>,
    pub scenarios: Vec<AutomationScenarioInfo>,
    pub position: Option<AutomationPositionInfo>,
    pub shadow_cash: Option<u64>,
    pub error: Option<String>,
    pub market_day_status: MarketDayStatus,
    pub market_day_message: Option<String>,
}

/// 예약 매도 상태 — 프론트로 emit("reservation") + get_reservations 반환에 사용.
/// (원주문번호 등 내부 식별자는 엔진 메모리에만 두고 프론트로 보내지 않는다)
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReservationInfo {
    pub code: String,
    /// 목표 수익률(%) — 평단 기준
    pub target_pct: f64,
    /// 계산된 목표 지정가(호가단위 올림)
    pub target_price: u64,
    /// 걸어둔(남은) 수량
    pub qty: u64,
    /// "waiting" | "filled" | "cancelled"
    pub status: String,
    /// 취소 사유 등 사용자 안내 (없으면 생략)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// 실시간 KIS 피드(테스트에서는 Broker 더블)가 엔진으로 보내는 이벤트
#[derive(Debug, Clone)]
pub enum FeedEvent {
    Quote(Quote),
    /// 호가만 갱신 (체결 없음)
    Book {
        code: String,
        ask1: f64,
        bid1: f64,
        ask1_qty: u64,
        bid1_qty: u64,
        ts: i64,
    },
    Fill(FillEvent),
    Conn(bool),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settings_deserializes_old_config_with_defaults() {
        // theme/exchange 필드가 없던 예전 config.json도 기본값으로 읽혀야 하고,
        // 폐기된 bufferTicks 키는 무시되어야 한다
        let old = r#"{
            "appKey":"","appSecret":"","cano":"","acntPrdtCd":"01","htsId":"",
            "mode":"demo",
            "tradeSymbols":[{"code":"0193T0","label":"L","etf":true}],
            "chartSymbols":[{"code":"000660","label":"H","etf":false}],
            "bufferTicks":2,"chartInterval":10,"opacity":1.0
        }"#;
        let s: Settings = serde_json::from_str(old).unwrap();
        assert_eq!(s.theme, "default");
        assert_eq!(s.exchange, "KRX");
        assert_eq!(s.auto_symbols.underlying, "000660");
        assert!(!s.real_trading_confirmed);
    }
}
