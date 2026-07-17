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
        // 실전은 KRX+NXT 통합 시세(H0UN~), 모의는 KRX(H0ST~)만 지원
        let (tr_price, tr_book, notice_tr) = match self.settings.mode {
            TradeMode::Paper => ("H0STCNT0", "H0STASP0", "H0STCNI9"),
            _ => ("H0UNCNT0", "H0UNASP0", "H0STCNI0"),
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
            tr_price: tr_price.to_string(),
            tr_book: tr_book.to_string(),
            price_codes: codes,
            book_codes: self.settings.trade_symbols.iter().map(|s| s.code.clone()).collect(),
            notice,
        };
        Ok(vec![ws::spawn_ws(cfg, tx)])
    }
}
