pub mod account;
pub mod auth;
pub mod calendar;
pub mod crypto;
pub mod inquiry;
pub mod order;
pub mod quote;
pub mod rest;
pub mod ws;

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::broker::{
    Broker, BrokerFill, BrokerMarketDay, BrokerOpenOrder, BrokerOrderStatus, OrderAck,
};
use crate::error::{AppError, AppResult};
use crate::types::{AccountSnapshot, Candle, FeedEvent, Quote, Settings, Side};

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

    fn exchange(&self) -> &str {
        &self.settings.exchange
    }
}

#[async_trait::async_trait]
impl Broker for KisBroker {
    async fn market_days(&self, basis_date: &str) -> AppResult<Vec<BrokerMarketDay>> {
        calendar::market_days(&self.rest, basis_date).await
    }

    async fn candles_1m(&self, code: &str) -> AppResult<Vec<Candle>> {
        quote::candles_1m(&self.rest, code).await
    }

    async fn account(&self) -> AppResult<AccountSnapshot> {
        let positions = account::positions(&self.rest, self.cano(), self.prdt()).await?;
        // 계좌 스냅샷용 대표 현금값이다. 실제 Auto/수동 재주문 수량은 주문할
        // 종목을 넣은 orderable_cash/max_buy_qty로 다시 확인한다.
        let any_code = self
            .settings
            .trade_symbols
            .first()
            .map(|s| s.code.as_str())
            .unwrap_or("005930");
        let cash = account::orderable_cash(&self.rest, self.cano(), self.prdt(), any_code).await?;
        Ok(AccountSnapshot { cash, positions })
    }

    async fn orderable_cash(&self, code: &str) -> AppResult<u64> {
        account::orderable_cash(&self.rest, self.cano(), self.prdt(), code).await
    }

    async fn snapshot(&self, code: &str) -> AppResult<Quote> {
        quote::snapshot(&self.rest, code).await
    }

    async fn max_buy_qty(&self, code: &str, limit_price: u64) -> AppResult<u64> {
        account::max_buy_qty(&self.rest, self.cano(), self.prdt(), code, limit_price).await
    }

    async fn place_buy(
        &self,
        code: &str,
        qty: u64,
        limit_price: u64,
        ioc: bool,
    ) -> AppResult<OrderAck> {
        let dvsn = if ioc {
            order::ORD_DVSN_IOC_LIMIT
        } else {
            order::ORD_DVSN_LIMIT
        };
        order::order_cash(
            &self.rest,
            self.cano(),
            self.prdt(),
            Side::Buy,
            code,
            qty,
            dvsn,
            limit_price,
            self.exchange(),
        )
        .await
    }

    async fn place_sell_market(&self, code: &str, qty: u64) -> AppResult<OrderAck> {
        order::order_cash(
            &self.rest,
            self.cano(),
            self.prdt(),
            Side::Sell,
            code,
            qty,
            order::ORD_DVSN_MARKET,
            0,
            self.exchange(),
        )
        .await
    }

    async fn place_sell_limit(
        &self,
        code: &str,
        qty: u64,
        limit_price: u64,
    ) -> AppResult<OrderAck> {
        order::order_cash(
            &self.rest,
            self.cano(),
            self.prdt(),
            Side::Sell,
            code,
            qty,
            order::ORD_DVSN_LIMIT,
            limit_price,
            self.exchange(),
        )
        .await
    }

    async fn cancel_order(&self, _code: &str, order_no: &str, org_no: &str) -> AppResult<OrderAck> {
        order::cancel_order(
            &self.rest,
            self.cano(),
            self.prdt(),
            order_no,
            org_no,
            self.exchange(),
        )
        .await
    }

    async fn open_orders(&self) -> AppResult<Vec<BrokerOpenOrder>> {
        inquiry::open_orders(&self.rest, self.cano(), self.prdt()).await
    }

    async fn today_fills(&self) -> AppResult<Vec<BrokerFill>> {
        inquiry::today_fills(&self.rest, self.cano(), self.prdt()).await
    }

    async fn order_status(
        &self,
        trading_date: &str,
        order_no: &str,
    ) -> AppResult<Option<BrokerOrderStatus>> {
        inquiry::order_status(&self.rest, self.cano(), self.prdt(), trading_date, order_no).await
    }

    async fn start_feed(
        &self,
        codes: Vec<String>,
        tx: mpsc::Sender<FeedEvent>,
    ) -> AppResult<Vec<JoinHandle<()>>> {
        let mut trade_codes: Vec<String> = self
            .settings
            .trade_symbols
            .iter()
            .map(|s| s.code.clone())
            .collect();
        trade_codes.extend([
            self.settings.auto_symbols.underlying.clone(),
            self.settings.auto_symbols.leverage.clone(),
            self.settings.auto_symbols.inverse.clone(),
        ]);
        trade_codes.sort();
        trade_codes.dedup();
        let notice = if self.settings.hts_id.is_empty() {
            tracing::warn!("HTS ID 미설정 — 실시간 체결통보 없이 동작 (잔고는 주기 갱신)");
            None
        } else {
            Some(("H0STCNI0".to_string(), self.settings.hts_id.clone()))
        };
        let cfg = ws::WsConfig {
            url: self.rest.ws_url().to_string(),
            approval: self.rest.token.approval_issuer(),
            subs: build_subs(&codes, &trade_codes),
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
fn build_subs(all_codes: &[String], trade_codes: &[String]) -> Vec<(String, String)> {
    let mut subs: Vec<(String, String)> = Vec::new();
    for code in all_codes {
        let krx_only = trade_codes.contains(code);
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
        let subs = build_subs(&all, &trade);

        // 매매 종목: KRX 체결가 + KRX 호가, 통합 TR로는 구독하지 않는다
        assert!(subs.contains(&("H0STCNT0".into(), "0193T0".into())));
        assert!(subs.contains(&("H0STASP0".into(), "0193T0".into())));
        assert!(!subs
            .iter()
            .any(|(tr, c)| tr.starts_with("H0UN") && c == "0193T0"));
        // 차트 전용 종목: 통합 체결가 유지, 호가 없음
        assert!(subs.contains(&("H0UNCNT0".into(), "000660".into())));
        assert!(!subs.iter().any(|(tr, c)| tr == "H0STASP0" && c == "000660"));
        // 코드당 체결가 구독은 1회 (거래량 이중 집계 방지)
        let price_subs = subs
            .iter()
            .filter(|(tr, c)| tr.ends_with("CNT0") && c == "0193T0")
            .count();
        assert_eq!(price_subs, 1);
    }

    #[test]
    fn build_subs_trade_code_overlapping_chart_stays_krx() {
        // 매매 종목을 차트에도 넣은 경우: KRX 체결가 1회 + 호가 1회만
        let all = codes(&["0193T0"]);
        let trade = codes(&["0193T0"]);
        let subs = build_subs(&all, &trade);
        assert_eq!(
            subs,
            vec![
                ("H0STCNT0".to_string(), "0193T0".to_string()),
                ("H0STASP0".to_string(), "0193T0".to_string()),
            ]
        );
    }
}
