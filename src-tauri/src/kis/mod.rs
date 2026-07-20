pub mod account;
pub mod auth;
pub mod crypto;
pub mod order;
pub mod quote;
pub mod rest;
pub mod ws;

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::broker::{Broker, OrderAck};
use crate::error::{AppError, AppResult};
use crate::types::{AccountSnapshot, Candle, FeedEvent, Quote, Settings, Side, TradeMode};

pub struct KisBroker {
    rest: Arc<rest::KisRest>,
    settings: Settings,
}

impl KisBroker {
    pub fn new(settings: &Settings) -> AppResult<Self> {
        if settings.cano.is_empty() {
            return Err(AppError::Config("계좌번호가 설정되지 않았습니다".into()));
        }
        Ok(Self {
            rest: Arc::new(rest::KisRest::new(settings)?),
            settings: settings.clone(),
        })
    }

    fn cano(&self) -> &str {
        &self.settings.cano
    }

    fn prdt(&self) -> &str {
        &self.settings.acnt_prdt_cd
    }

    /// 주문 거래소: 모의투자는 SOR 미지원이므로 KRX 강제
    fn exchange(&self) -> &str {
        if matches!(self.settings.mode, TradeMode::Paper) {
            "KRX"
        } else {
            &self.settings.exchange
        }
    }
}

#[async_trait::async_trait]
impl Broker for KisBroker {
    async fn candles_1m(&self, code: &str) -> AppResult<Vec<Candle>> {
        quote::candles_1m(&self.rest, code).await
    }

    async fn account(&self) -> AppResult<AccountSnapshot> {
        let positions = account::positions(&self.rest, self.cano(), self.prdt()).await?;
        // 매수가능조회는 PDNO가 필수라 매매 종목 중 하나를 넣는다 (ord_psbl_cash는 종목 무관)
        let any_code = self
            .settings
            .trade_symbols
            .first()
            .map(|s| s.code.as_str())
            .unwrap_or("005930");
        let cash = account::orderable_cash(&self.rest, self.cano(), self.prdt(), any_code).await?;
        Ok(AccountSnapshot { cash, positions })
    }

    async fn snapshot(&self, code: &str) -> AppResult<Quote> {
        quote::snapshot(&self.rest, code).await
    }

    async fn max_buy_qty(&self, code: &str, limit_price: u64) -> AppResult<u64> {
        account::max_buy_qty(&self.rest, self.cano(), self.prdt(), code, limit_price).await
    }

    async fn place_buy(&self, code: &str, qty: u64, limit_price: u64, ioc: bool) -> AppResult<OrderAck> {
        let dvsn = if ioc { order::ORD_DVSN_IOC_LIMIT } else { order::ORD_DVSN_LIMIT };
        order::order_cash(
            &self.rest, self.cano(), self.prdt(), Side::Buy, code, qty, dvsn, limit_price, self.exchange(),
        )
        .await
    }

    async fn place_sell_market(&self, code: &str, qty: u64) -> AppResult<OrderAck> {
        order::order_cash(
            &self.rest, self.cano(), self.prdt(), Side::Sell, code, qty, order::ORD_DVSN_MARKET, 0, self.exchange(),
        )
        .await
    }

    async fn start_feed(
        &self,
        codes: Vec<String>,
        tx: mpsc::Sender<FeedEvent>,
    ) -> AppResult<Vec<JoinHandle<()>>> {
        let approval_key = self.rest.token.ws_approval_key().await?;
        let trade_codes: Vec<String> =
            self.settings.trade_symbols.iter().map(|s| s.code.clone()).collect();
        let notice_tr = match self.settings.mode {
            TradeMode::Paper => "H0STCNI9",
            _ => "H0STCNI0",
        };
        let notice = if self.settings.hts_id.is_empty() {
            tracing::warn!("HTS ID 미설정 — 실시간 체결통보 없이 동작 (잔고는 주기 갱신)");
            None
        } else {
            Some((notice_tr.to_string(), self.settings.hts_id.clone()))
        };
        let cfg = ws::WsConfig {
            url: self.rest.ws_url().to_string(),
            approval_key,
            subs: build_subs(self.settings.mode, &codes, &trade_codes),
            notice,
        };
        Ok(vec![ws::spawn_ws(cfg, tx)])
    }
}

/// 종목별 실시간 구독 목록 (tr_id, tr_key).
/// 매매 종목은 실전에서도 KRX 단독 TR(H0ST~)로 구독한다 — 신형 코드 ETF/ETN은
/// KRX+NXT 통합 TR(H0UN~)이 시세를 내려주지 않아 수익률 틱 갱신이 끊긴다.
/// 차트 전용 종목은 통합 TR로 NXT 프리/애프터 시세까지 유지한다.
/// 같은 종목을 두 TR로 겹쳐 구독하면 차트 거래량이 이중 집계되므로 코드당 체결가 구독은 1회.
fn build_subs(mode: TradeMode, all_codes: &[String], trade_codes: &[String]) -> Vec<(String, String)> {
    let mut subs: Vec<(String, String)> = Vec::new();
    for code in all_codes {
        let krx_only = matches!(mode, TradeMode::Paper) || trade_codes.contains(code);
        let tr_price = if krx_only { "H0STCNT0" } else { "H0UNCNT0" };
        subs.push((tr_price.to_string(), code.clone()));
    }
    for code in trade_codes {
        subs.push(("H0STASP0".to_string(), code.clone()));
    }
    subs
}

#[cfg(test)]
mod tests {
    use super::*;

    fn codes(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn build_subs_real_trade_codes_use_krx_tr() {
        let all = codes(&["000660", "005930", "0193T0"]);
        let trade = codes(&["0193T0"]);
        let subs = build_subs(TradeMode::Real, &all, &trade);

        // 매매 종목: KRX 체결가 + KRX 호가, 통합 TR로는 구독하지 않는다
        assert!(subs.contains(&("H0STCNT0".into(), "0193T0".into())));
        assert!(subs.contains(&("H0STASP0".into(), "0193T0".into())));
        assert!(!subs.iter().any(|(tr, c)| tr.starts_with("H0UN") && c == "0193T0"));
        // 차트 전용 종목: 통합 체결가 유지, 호가 없음
        assert!(subs.contains(&("H0UNCNT0".into(), "000660".into())));
        assert!(!subs.iter().any(|(tr, c)| tr == "H0STASP0" && c == "000660"));
        // 코드당 체결가 구독은 1회 (거래량 이중 집계 방지)
        let price_subs = subs.iter().filter(|(tr, c)| tr.ends_with("CNT0") && c == "0193T0").count();
        assert_eq!(price_subs, 1);
    }

    #[test]
    fn build_subs_paper_is_all_krx() {
        let all = codes(&["005930", "0193T0"]);
        let trade = codes(&["0193T0"]);
        let subs = build_subs(TradeMode::Paper, &all, &trade);
        assert!(subs.iter().all(|(tr, _)| tr.starts_with("H0ST")));
        assert!(subs.contains(&("H0STCNT0".into(), "005930".into())));
    }

    #[test]
    fn build_subs_trade_code_overlapping_chart_stays_krx() {
        // 매매 종목을 차트에도 넣은 경우: KRX 체결가 1회 + 호가 1회만
        let all = codes(&["0193T0"]);
        let trade = codes(&["0193T0"]);
        let subs = build_subs(TradeMode::Real, &all, &trade);
        assert_eq!(subs, vec![
            ("H0STCNT0".to_string(), "0193T0".to_string()),
            ("H0STASP0".to_string(), "0193T0".to_string()),
        ]);
    }
}
