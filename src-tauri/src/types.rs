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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymbolConfig {
    pub code: String,
    pub label: String,
    /// ETF/ETN이면 호가단위 5원 고정, 아니면 KRX 가격대별 호가단위
    #[serde(default = "default_true")]
    pub etf: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TradeMode {
    Real,
    Paper,
    Demo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Settings {
    pub app_key: String,
    pub app_secret: String,
    pub cano: String,
    pub acnt_prdt_cd: String,
    pub hts_id: String,
    pub mode: TradeMode,
    /// "default"(기본 색상) | "mono"(무채색 위장 테마) — UI에서만 해석
    #[serde(default = "default_theme")]
    pub theme: String,
    /// 주문 거래소: "KRX" | "SOR"(스마트 라우팅). 모의투자는 KRX 강제
    #[serde(default = "default_exchange")]
    pub exchange: String,
    pub trade_symbols: Vec<SymbolConfig>,
    pub chart_symbols: Vec<SymbolConfig>,
    pub chart_interval: u32,
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
            app_key: String::new(),
            app_secret: String::new(),
            cano: String::new(),
            acnt_prdt_cd: "01".into(),
            hts_id: String::new(),
            mode: TradeMode::Demo,
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
        codes.sort();
        codes.dedup();
        codes
    }

    pub fn is_etf(&self, code: &str) -> bool {
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

/// ts도 Candle.time과 같은 가짜 epoch 초 단위
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Quote {
    pub code: String,
    pub price: f64,
    pub change_rate: f64,
    pub ask1: f64,
    pub bid1: f64,
    /// 이 틱의 체결량. 호가만 갱신된 경우 0 (차트는 volume 0 틱을 무시)
    pub volume: f64,
    pub ts: i64,
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
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FillEvent {
    pub code: String,
    pub side: Side,
    pub qty: u64,
    pub price: f64,
}

/// 실시간 피드(웹소켓/모의 피드)가 엔진으로 보내는 이벤트
#[derive(Debug, Clone)]
pub enum FeedEvent {
    Quote(Quote),
    /// 호가만 갱신 (체결 없음)
    Book {
        code: String,
        ask1: f64,
        bid1: f64,
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
    }
}
