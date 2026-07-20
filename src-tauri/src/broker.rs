use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::error::AppResult;
use crate::types::{AccountSnapshot, Candle, FeedEvent, Quote};

/// 주문 접수 응답 (체결은 피드의 Fill 이벤트로 별도 통지)
#[derive(Debug, Clone)]
pub struct OrderAck {
    pub order_no: String,
    pub message: String,
}

/// 실전(KIS)/데모(Mock) 공용 브로커 인터페이스
#[async_trait::async_trait]
pub trait Broker: Send + Sync {
    /// 차트용 1분봉 (MA120 계산이 가능하도록 과거 영업일 포함 약 5일치)
    async fn candles_1m(&self, code: &str) -> AppResult<Vec<Candle>>;

    /// 예수금 + 보유 종목
    async fn account(&self) -> AppResult<AccountSnapshot>;

    /// 실시간 캐시가 낡았을 때의 REST 폴백 스냅샷
    async fn snapshot(&self, code: &str) -> AppResult<Quote>;

    /// 지정가 limit_price 기준 미수 없이 매수 가능한 최대 수량 (매수 거부 시 재주문 수량 산정용)
    async fn max_buy_qty(&self, code: &str, limit_price: u64) -> AppResult<u64>;

    /// 지정가 매수 (ioc=true면 IOC지정가: 미체결 잔량 즉시 자동취소)
    async fn place_buy(&self, code: &str, qty: u64, limit_price: u64, ioc: bool) -> AppResult<OrderAck>;

    /// 시장가 전량 매도
    async fn place_sell_market(&self, code: &str, qty: u64) -> AppResult<OrderAck>;

    /// 실시간 피드 시작. 생성된 백그라운드 태스크 핸들을 반환한다 (엔진 재시작 시 abort용)
    async fn start_feed(
        &self,
        codes: Vec<String>,
        tx: mpsc::Sender<FeedEvent>,
    ) -> AppResult<Vec<JoinHandle<()>>>;
}
