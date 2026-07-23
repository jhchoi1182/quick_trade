use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::error::AppResult;
use crate::types::{AccountSnapshot, Candle, FeedEvent, Quote, Side};

/// 주문 접수 응답 (체결은 피드의 Fill 이벤트로 별도 통지)
#[derive(Debug, Clone)]
pub struct OrderAck {
    pub order_no: String,
    /// 한국거래소전송주문조직번호(KRX_FWDG_ORD_ORGNO) — 정정취소 시 원주문 식별에 필요
    pub org_no: String,
    pub message: String,
}

#[derive(Debug, Clone)]
pub struct BrokerOpenOrder {
    pub order_no: String,
    pub original_order_no: String,
    pub org_no: String,
    pub code: String,
    pub side: Side,
    pub ordered_qty: u64,
    pub filled_qty: u64,
    pub cancelable_qty: u64,
    pub price: f64,
    pub ordered_at: i64,
}

#[derive(Debug, Clone)]
pub struct BrokerFill {
    pub fill_id: String,
    pub order_no: String,
    pub original_order_no: String,
    pub org_no: String,
    pub code: String,
    pub side: Side,
    pub qty: u64,
    pub price: f64,
    pub filled_at: i64,
}

/// KIS 일별 주문체결 조회에서 주문번호 하나를 정확히 조회한 누적 상태.
///
/// `open_orders`에서 주문이 사라졌다는 사실만으로는 체결·취소·거부와 조회 지연을
/// 구분할 수 없다. 자동 청산 재주문은 이 상태가 [`Self::is_terminal`]을 만족할
/// 때만 허용해야 한다.
#[derive(Debug, Clone, PartialEq)]
pub struct BrokerOrderStatus {
    pub trading_date: String,
    pub order_no: String,
    pub original_order_no: String,
    pub org_no: String,
    pub code: String,
    pub side: Side,
    pub ordered_qty: u64,
    pub filled_qty: u64,
    pub cancelled_qty: u64,
    pub remaining_qty: u64,
    pub rejected_qty: u64,
    pub avg_fill_price: f64,
    pub ordered_at: i64,
}

impl BrokerOrderStatus {
    /// 더 이상 이 주문에서 경제 체결이 생기지 않는다고 확정할 수 있는 상태인지 판정한다.
    ///
    /// KIS 누적값이 반영 도중이면 잔여수량이 0이더라도 합계가 주문수량보다 작을 수
    /// 있으므로 진행 중으로 취급한다. 체결 평균가가 아직 반영되지 않은 경우도 마찬가지다.
    pub fn is_terminal(&self) -> bool {
        if self.ordered_qty == 0
            || self.filled_qty > self.ordered_qty
            || self.cancelled_qty > self.ordered_qty
            || self.remaining_qty > self.ordered_qty
            || self.rejected_qty > self.ordered_qty
            || (self.filled_qty > 0
                && (!self.avg_fill_price.is_finite() || self.avg_fill_price <= 0.0))
        {
            return false;
        }

        let Some(terminal_qty) = self
            .filled_qty
            .checked_add(self.cancelled_qty)
            .and_then(|qty| qty.checked_add(self.rejected_qty))
        else {
            return false;
        };

        // 합계가 주문수량을 넘는 상태도 조회 반영 중 모순이다. `>=`로 종결하면
        // 다른 주문의 수량이 섞인 응답을 확정 상태로 오인할 수 있다.
        self.remaining_qty == 0 && terminal_qty == self.ordered_qty
    }
}

/// 섀도 실행기가 실계좌에서 한 번 복사할 수 있는 최소 읽기 권한.
///
/// 주문 메서드가 없는 별도 trait object를 섀도 executor에 주입해, 가상 체결
/// 코드에서는 KIS 주문 POST를 컴파일 단계에서 호출할 수 없게 한다.
#[async_trait::async_trait]
pub trait ShadowCashSource: Send + Sync {
    async fn orderable_cash(&self, code: &str) -> AppResult<u64>;
}

/// 전체 브로커를 읽기 전용 섀도 capability로 축소하는 어댑터.
#[derive(Clone)]
pub struct BrokerShadowCashSource {
    broker: Arc<dyn Broker>,
}

impl BrokerShadowCashSource {
    pub fn new(broker: Arc<dyn Broker>) -> Self {
        Self { broker }
    }
}

#[async_trait::async_trait]
impl ShadowCashSource for BrokerShadowCashSource {
    async fn orderable_cash(&self, code: &str) -> AppResult<u64> {
        self.broker.orderable_cash(code).await
    }
}

/// 실전 브로커 인터페이스. 테스트에서는 cfg(test) 더블로 구현한다.
#[async_trait::async_trait]
pub trait Broker: Send + Sync {
    /// 차트용 1분봉 (MA120 계산이 가능하도록 과거 영업일 포함 약 5일치)
    async fn candles_1m(&self, code: &str) -> AppResult<Vec<Candle>>;

    /// 예수금 + 보유 종목
    async fn account(&self) -> AppResult<AccountSnapshot>;

    /// 특정 종목을 기준으로 한 미수없는매수금액.
    ///
    /// KIS 매수가능조회는 상품번호가 필수이고 종목별 주문 조건이 반영될 수 있으므로,
    /// 자동 진입 수량은 실제로 주문할 종목을 넣어 다시 조회한다. 테스트 더블은 기존
    /// 계좌 스냅샷의 현금을 기본값으로 사용할 수 있다.
    async fn orderable_cash(&self, _code: &str) -> AppResult<u64> {
        Ok(self.account().await?.cash)
    }

    /// 실시간 캐시가 낡았을 때의 REST 폴백 스냅샷
    async fn snapshot(&self, code: &str) -> AppResult<Quote>;

    /// 지정가 limit_price 기준 미수 없이 매수 가능한 최대 수량 (매수 거부 시 재주문 수량 산정용)
    async fn max_buy_qty(&self, code: &str, limit_price: u64) -> AppResult<u64>;

    /// 지정가 매수 (ioc=true면 IOC지정가: 미체결 잔량 즉시 자동취소)
    async fn place_buy(
        &self,
        code: &str,
        qty: u64,
        limit_price: u64,
        ioc: bool,
    ) -> AppResult<OrderAck>;

    /// 시장가 전량 매도
    async fn place_sell_market(&self, code: &str, qty: u64) -> AppResult<OrderAck>;

    /// 지정가 매도 (예약 매도용) — 목표 호가에 걸어두고 시세 도달 시 체결
    async fn place_sell_limit(&self, code: &str, qty: u64, limit_price: u64)
        -> AppResult<OrderAck>;

    /// 미체결 주문 취소 (예약 매도 취소용). order_no=원주문번호, org_no=주문조직번호
    async fn cancel_order(&self, code: &str, order_no: &str, org_no: &str) -> AppResult<OrderAck>;

    /// 재시작 조정용 정정취소 가능 주문. 테스트 더블은 빈 목록을 기본으로 쓸 수 있다.
    async fn open_orders(&self) -> AppResult<Vec<BrokerOpenOrder>> {
        Ok(Vec::new())
    }

    /// 재시작 조정용 당일 체결 목록.
    async fn today_fills(&self) -> AppResult<Vec<BrokerFill>> {
        Ok(Vec::new())
    }

    /// 주문일자와 KIS 주문번호로 일별 주문 누적 상태를 정확히 조회한다.
    ///
    /// 기본 구현의 `None`은 테스트 더블의 호환을 위한 값이며 "주문 부재 확정"이
    /// 아니다. 호출자는 `None`을 불명확/진행 중으로 취급하고 같은 주문을 재POST하면
    /// 안 된다. 제품 런타임의 `KisBroker`는 실제 조회를 구현한다.
    async fn order_status(
        &self,
        _trading_date: &str,
        _order_no: &str,
    ) -> AppResult<Option<BrokerOrderStatus>> {
        Ok(None)
    }

    /// 실시간 피드 시작. 생성된 백그라운드 태스크 핸들을 반환한다 (엔진 재시작 시 abort용)
    async fn start_feed(
        &self,
        codes: Vec<String>,
        tx: mpsc::Sender<FeedEvent>,
    ) -> AppResult<Vec<JoinHandle<()>>>;
}
