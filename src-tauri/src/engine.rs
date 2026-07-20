use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};

use tauri::{AppHandle, Emitter};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::broker::Broker;
use crate::candle_cache::CandleCache;
use crate::error::AppResult;
use crate::kis::KisBroker;
use crate::mock::MockBroker;
use crate::types::{
    AccountSnapshot, Candle, FeedEvent, OrderResult, Quote, Settings, Side, TradeMode,
};
use crate::util::{buy_limit_price, max_buy_qty, now_kst_fake_epoch};

/// 캐시가 이 초수보다 낡으면 주문 전에 REST 스냅샷으로 폴백
const QUOTE_FRESH_SECS: i64 = 10;
/// 스냅샷 실패 시 이 초수 이내의 캐시는 최후 수단으로 허용
const QUOTE_STALE_LIMIT_SECS: i64 = 60;
const ACCOUNT_REFRESH_SECS: u64 = 30;

/// 프론트로 이벤트를 내보내는 콜백. Engine이 tauri 타입을 직접 들지 않게 분리한다 —
/// tauri 심볼을 링크하면 매니페스트 없는 단위 테스트 exe가 comctl32 v6 로드 실패로 죽는다.
type EmitFn = Box<dyn Fn(&str, serde_json::Value) + Send + Sync>;

pub struct Engine {
    /// UI 전용 필드(테마·차트 주기 등)는 엔진 재시작 없이 갱신되므로 RwLock으로 보관
    settings: RwLock<Settings>,
    broker: Arc<dyn Broker>,
    quotes: RwLock<HashMap<String, Quote>>,
    account: RwLock<AccountSnapshot>,
    candle_cache: CandleCache,
    last_error: Mutex<String>,
    /// 지연 잔고 갱신이 이미 예약되어 있으면 true — 연속 체결통보를 1회 갱신으로 합류
    refresh_pending: AtomicBool,
    /// 잔고를 바꾼 사건(주문 접수/체결통보)마다 +1 — 캐시 최신성 판정용
    account_gen: AtomicU64,
    /// 마지막 성공 갱신이 반영한 account_gen 값. account_gen보다 작으면 캐시가 낡은 것
    account_refreshed_gen: AtomicU64,
    /// 테스트에서는 None (이벤트 emit 생략)
    emit_fn: Option<EmitFn>,
    /// 지연 잔고 갱신 태스크에서 자기 자신을 참조하기 위한 약한 포인터
    weak: Weak<Engine>,
}

/// 엔진 + 백그라운드 태스크 핸들. drop 시 태스크 전부 중단.
pub struct EngineHandle {
    pub engine: Arc<Engine>,
    tasks: Vec<JoinHandle<()>>,
}

impl Drop for EngineHandle {
    fn drop(&mut self) {
        for t in &self.tasks {
            t.abort();
        }
    }
}

pub async fn start(app: AppHandle, settings: Settings) -> AppResult<EngineHandle> {
    let broker: Arc<dyn Broker> = match settings.mode {
        TradeMode::Demo => Arc::new(MockBroker::new(&settings)),
        _ => Arc::new(KisBroker::new(&settings)?),
    };

    let engine = Arc::new_cyclic(|weak| Engine {
        settings: RwLock::new(settings.clone()),
        broker: Arc::clone(&broker),
        quotes: RwLock::new(HashMap::new()),
        account: RwLock::new(AccountSnapshot { cash: 0, positions: Vec::new() }),
        candle_cache: CandleCache::new(),
        last_error: Mutex::new(String::new()),
        refresh_pending: AtomicBool::new(false),
        account_gen: AtomicU64::new(0),
        account_refreshed_gen: AtomicU64::new(0),
        emit_fn: Some(Box::new(move |event, payload| {
            let _ = app.emit(event, payload);
        })),
        weak: weak.clone(),
    });

    let (tx, rx) = mpsc::channel::<FeedEvent>(512);
    let mut tasks = broker.start_feed(settings.all_codes(), tx).await?;

    tasks.push(tokio::spawn(consume_feed(Arc::clone(&engine), rx)));
    tasks.push(tokio::spawn(periodic_refresh(Arc::clone(&engine))));

    // 차트 종목을 순차로 미리 백필해 셀렉터 전환이 즉시 되게 한다.
    // 프론트가 같은 종목을 요청하면 단일 비행으로 합쳐지므로 중복 호출은 없다.
    let warm_engine = Arc::clone(&engine);
    let warm_codes: Vec<String> = settings.chart_symbols.iter().map(|s| s.code.clone()).collect();
    let seed_codes: Vec<String> = settings.trade_symbols.iter().map(|s| s.code.clone()).collect();
    tasks.push(tokio::spawn(async move {
        // 매매 종목 시세 1회 시드: 첫 틱 도착 전에도 수익률 기준가가 있고,
        // 기준 Quote 부재로 Book 이벤트가 버려지는 공백도 없앤다 (폴링 아님)
        for code in &seed_codes {
            warm_engine.fresh_quote(code).await;
        }
        for code in warm_codes {
            if let Err(e) = warm_engine.candles(&code).await {
                tracing::warn!("차트 워밍업 실패({code}): {e}");
            }
        }
    }));

    Ok(EngineHandle { engine, tasks })
}

async fn consume_feed(engine: Arc<Engine>, mut rx: mpsc::Receiver<FeedEvent>) {
    // 실가동 검증용: 종목별 첫 틱 로그로 구독이 실제 시세를 내려주는지 즉시 판별
    let mut first_tick_seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    while let Some(ev) = rx.recv().await {
        match ev {
            FeedEvent::Quote(q) => {
                if first_tick_seen.insert(q.code.clone()) {
                    tracing::info!("실시간 체결가 첫 수신: {}", q.code);
                }
                engine.quotes.write().unwrap().insert(q.code.clone(), q.clone());
                engine.emit("quote", &q);
            }
            FeedEvent::Book { code, ask1, bid1, ts } => {
                let merged = {
                    let mut map = engine.quotes.write().unwrap();
                    map.get_mut(&code).map(|q| {
                        q.ask1 = ask1;
                        q.bid1 = bid1;
                        q.ts = ts;
                        q.volume = 0.0;
                        q.clone()
                    })
                };
                if let Some(q) = merged {
                    engine.emit("quote", &q);
                }
            }
            FeedEvent::Fill(fill) => {
                engine.emit("fill", &fill);
                engine.mark_account_stale();
                // 부분체결이 연발해도 잔고 조회는 1회로 합류 → 리미터 큐를 채우지 않는다
                engine.schedule_account_refresh();
            }
            FeedEvent::Conn(connected) => {
                engine.emit("conn", &serde_json::json!({ "connected": connected }));
            }
        }
    }
}

async fn periodic_refresh(engine: Arc<Engine>) {
    let mut timer = tokio::time::interval(std::time::Duration::from_secs(ACCOUNT_REFRESH_SECS));
    loop {
        timer.tick().await;
        engine.refresh_account().await;
    }
}

impl Engine {
    /// 브로커 재시작이 필요 없는 설정 변경(테마·차트 주기 등)을 반영
    pub fn update_settings(&self, new: Settings) {
        *self.settings.write().unwrap() = new;
    }

    /// 프론트로 이벤트 전송 (테스트 등 콜백이 없으면 생략)
    fn emit<S: serde::Serialize>(&self, event: &str, payload: &S) {
        let Some(f) = &self.emit_fn else { return };
        match serde_json::to_value(payload) {
            Ok(v) => f(event, v),
            Err(e) => tracing::warn!("이벤트 직렬화 실패({event}): {e}"),
        }
    }

    pub async fn refresh_account(&self) {
        // 조회 도중 새 체결이 오면 gen이 더 커져 다음 주문이 다시 갱신하게 된다
        let gen = self.account_gen.load(Ordering::SeqCst);
        match self.broker.account().await {
            Ok(snap) => {
                *self.account.write().unwrap() = snap.clone();
                self.account_refreshed_gen.fetch_max(gen, Ordering::SeqCst);
                self.last_error.lock().unwrap().clear();
                self.emit("account", &snap);
            }
            Err(e) => {
                let msg = e.to_string();
                let mut last = self.last_error.lock().unwrap();
                if *last != msg {
                    *last = msg.clone();
                    self.emit("engine-error", &format!("계좌 조회 실패: {msg}"));
                }
            }
        }
    }

    pub fn account_snapshot(&self) -> AccountSnapshot {
        self.account.read().unwrap().clone()
    }

    /// 1분봉 조회 — 캐시(TTL 60s) + 단일 비행으로 중복 백필을 막는다
    pub async fn candles(&self, code: &str) -> AppResult<Vec<Candle>> {
        let started = std::time::Instant::now();
        let broker = Arc::clone(&self.broker);
        let bars = self
            .candle_cache
            .get_or_fetch(code, || async move { broker.candles_1m(code).await })
            .await?;
        let elapsed = started.elapsed();
        if elapsed.as_millis() > 300 {
            tracing::info!("분봉 로드 {code}: {}건, {:.1}초", bars.len(), elapsed.as_secs_f32());
        }
        Ok((*bars).clone())
    }

    /// 주문에 쓸 신선한 시세. 캐시가 낡았으면 REST 폴백, 그래도 없으면 None.
    async fn fresh_quote(&self, code: &str) -> Option<Quote> {
        let cached = self.quotes.read().unwrap().get(code).cloned();
        let now = now_kst_fake_epoch();
        if let Some(q) = &cached {
            if now - q.ts <= QUOTE_FRESH_SECS {
                return cached;
            }
        }
        match self.broker.snapshot(code).await {
            Ok(q) => {
                self.quotes.write().unwrap().insert(code.to_string(), q.clone());
                // 백엔드 캐시만 채우면 화면 수익률은 낡은 값에 머문다 — 프론트에도 반영
                self.emit("quote", &q);
                Some(q)
            }
            Err(e) => {
                tracing::warn!("스냅샷 폴백 실패({code}): {e}");
                cached.filter(|q| now - q.ts <= QUOTE_STALE_LIMIT_SECS)
            }
        }
    }

    /// 원클릭 즉시 매수: 매도1호가 +3% IOC지정가, 매도1호가 기준 주문가능현금의 95%
    pub async fn buy_max(&self, code: &str) -> OrderResult {
        let fail = |message: String| OrderResult {
            ok: false,
            side: Side::Buy,
            code: code.to_string(),
            qty: 0,
            price: 0,
            message,
        };

        let Some(q) = self.fresh_quote(code).await else {
            return fail("시세 없음 — 연결 상태를 확인하세요".into());
        };
        // 지금 살 수 있는 매물을 기준으로 삼고, 호가가 없을 때만 현재가로 폴백한다.
        // 신선한 시세는 웹소켓 캐시에서 읽으므로 이 계산은 REST 호출을 추가하지 않는다.
        let base = if q.ask1 > 0.0 { q.ask1 } else { q.price };
        if base <= 0.0 {
            return fail("시세 정보 없음".into());
        }

        let (etf, mode) = {
            let s = self.settings.read().unwrap();
            (s.is_etf(code), s.mode)
        };
        let limit = buy_limit_price(base as u64, etf);
        // 매도 직후 재매수 등 직전 주문이 아직 캐시에 반영 전이면 동기 갱신 (스캘핑 연속 매매 대응)
        self.sync_account_if_stale().await;
        let cash = self.account.read().unwrap().cash;
        // 수량은 매도1호가 기준 95%로 잡는다. +3%는 체결 허용 상한일 뿐 실제 체결가는
        // 현재 매도호가부터 적용되므로 시드가 약 95% 투입된다.
        let mut qty = max_buy_qty(cash, base as u64);
        if qty == 0 {
            // 앱 시작 직후 첫 갱신 전 등 캐시가 비어 있을 수 있다 — 동기 갱신 후 재계산
            self.refresh_account().await;
            let cash = self.account.read().unwrap().cash;
            qty = max_buy_qty(cash, base as u64);
            if qty == 0 {
                return fail(format!("주문가능금액 부족 (예수금 {cash}원)"));
            }
        }

        // 모의투자는 IOC 미지원이라 일반 지정가로 대체
        let ioc = !matches!(mode, TradeMode::Paper);
        match self.broker.place_buy(code, qty, limit, ioc).await {
            Ok(ack) => {
                self.mark_account_stale();
                self.schedule_account_refresh();
                OrderResult {
                    ok: true,
                    side: Side::Buy,
                    code: code.to_string(),
                    qty,
                    price: limit,
                    message: ack.message,
                }
            }
            Err(e) => {
                // 캐시 예수금이 실제 주문가능금액보다 부풀려졌을 수 있다(미정산 매도대금 등).
                // KIS가 계산한 매수가능수량으로 1회만 재주문 — 첫 주문은 확정 거부라 이중 주문 위험 없음.
                let retried = self.retry_buy_with_psbl(code, qty, limit, ioc).await;
                self.schedule_account_refresh();
                retried.unwrap_or_else(|| fail(e.to_string()))
            }
        }
    }

    /// 매수 거부 시 KIS 매수가능수량(미수없는매수수량)으로 1회 재주문.
    /// 수량이 줄지 않으면 금액 부족이 원인이 아니므로 None을 돌려 원래 에러를 노출한다.
    async fn retry_buy_with_psbl(
        &self,
        code: &str,
        rejected_qty: u64,
        limit: u64,
        ioc: bool,
    ) -> Option<OrderResult> {
        let fail = |message: String| OrderResult {
            ok: false,
            side: Side::Buy,
            code: code.to_string(),
            qty: 0,
            price: 0,
            message,
        };

        let qty = match self.broker.max_buy_qty(code, limit).await {
            Ok(q) => q,
            Err(e) => {
                tracing::warn!("매수가능수량 조회 실패({code}): {e}");
                return None;
            }
        };
        if qty >= rejected_qty {
            return None;
        }
        if qty == 0 {
            return Some(fail("주문가능금액 부족".into()));
        }

        tracing::info!("매수 거부 → KIS 매수가능수량 {qty}주로 재주문 ({code}, 거부 수량 {rejected_qty}주)");
        match self.broker.place_buy(code, qty, limit, ioc).await {
            Ok(ack) => {
                self.mark_account_stale();
                Some(OrderResult {
                    ok: true,
                    side: Side::Buy,
                    code: code.to_string(),
                    qty,
                    price: limit,
                    message: ack.message,
                })
            }
            Err(e) => Some(fail(e.to_string())),
        }
    }

    /// 원클릭 전량 매도: 보유 수량 전부 시장가
    pub async fn sell_all(&self, code: &str) -> OrderResult {
        let fail = |message: String| OrderResult {
            ok: false,
            side: Side::Sell,
            code: code.to_string(),
            qty: 0,
            price: 0,
            message,
        };

        // 매수 체결 직후 매도 등 직전 주문이 아직 캐시에 반영 전이면 동기 갱신 (스캘핑 연속 매매 대응)
        self.sync_account_if_stale().await;
        let mut qty = self.cached_position_qty(code);
        if qty == 0 {
            // 앱 밖(HTS 등)에서 산 종목처럼 엔진이 모르는 변동일 수 있다 — 동기 갱신 후 재확인
            self.refresh_account().await;
            qty = self.cached_position_qty(code);
        }
        if qty == 0 {
            return fail("보유 수량 없음".into());
        }

        match self.broker.place_sell_market(code, qty).await {
            Ok(ack) => {
                self.mark_account_stale();
                self.schedule_account_refresh();
                OrderResult {
                    ok: true,
                    side: Side::Sell,
                    code: code.to_string(),
                    qty,
                    price: 0,
                    message: ack.message,
                }
            }
            Err(e) => {
                self.schedule_account_refresh();
                fail(e.to_string())
            }
        }
    }

    /// 잔고를 바꾼 사건(주문 접수/체결통보) 발생 표시 — 다음 주문이 캐시를 갱신하게 한다
    fn mark_account_stale(&self) {
        self.account_gen.fetch_add(1, Ordering::SeqCst);
    }

    /// 마지막 사건 이후 성공한 잔고 갱신이 없으면 동기로 1회 갱신.
    /// 최신이면 REST 호출 없이 즉시 반환하므로 정상 주문 경로의 속도는 그대로다.
    async fn sync_account_if_stale(&self) {
        let stale = self.account_refreshed_gen.load(Ordering::SeqCst)
            < self.account_gen.load(Ordering::SeqCst);
        if stale {
            self.refresh_account().await;
        }
    }

    fn cached_position_qty(&self, code: &str) -> u64 {
        self.account
            .read()
            .unwrap()
            .positions
            .iter()
            .find(|p| p.code == code)
            .map(|p| p.qty)
            .unwrap_or(0)
    }

    /// 주문·체결 직후 잠시 뒤 잔고 갱신 (체결통보와 별개의 안전망).
    /// 이미 예약된 갱신이 있으면 합류해 REST 호출(1회당 GET 2건)이 리미터 큐에 쌓이지 않게 한다.
    fn schedule_account_refresh(&self) {
        if self.refresh_pending.swap(true, Ordering::SeqCst) {
            return;
        }
        let Some(engine) = self.weak.upgrade() else {
            self.refresh_pending.store(false, Ordering::SeqCst);
            return;
        };
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(700)).await;
            // 대기 중 도착한 체결통보가 새 예약을 걸 수 있도록 조회 전에 해제
            engine.refresh_pending.store(false, Ordering::SeqCst);
            engine.refresh_account().await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::OrderAck;
    use crate::error::{AppError, AppResult};
    use crate::mock::MockBroker;
    use std::sync::atomic::AtomicUsize;

    fn test_engine(broker: Arc<dyn Broker>, settings: Settings) -> Arc<Engine> {
        test_engine_with_emit(broker, settings, None)
    }

    fn test_engine_with_emit(
        broker: Arc<dyn Broker>,
        settings: Settings,
        emit_fn: Option<EmitFn>,
    ) -> Arc<Engine> {
        Arc::new_cyclic(|weak| Engine {
            settings: RwLock::new(settings),
            broker,
            quotes: RwLock::new(HashMap::new()),
            account: RwLock::new(AccountSnapshot { cash: 0, positions: Vec::new() }),
            candle_cache: CandleCache::new(),
            last_error: Mutex::new(String::new()),
            refresh_pending: AtomicBool::new(false),
            account_gen: AtomicU64::new(0),
            account_refreshed_gen: AtomicU64::new(0),
            emit_fn,
            weak: weak.clone(),
        })
    }

    /// 신선한 웹소켓 시세를 캐시에 심는다 (버튼 클릭 경로 테스트용)
    fn seed_quote(engine: &Engine, ask1: f64) {
        engine.quotes.write().unwrap().insert(
            "0193T0".into(),
            Quote {
                code: "0193T0".into(),
                price: 10_500.0,
                change_rate: 0.0,
                ask1,
                bid1: 9_995.0,
                volume: 0.0,
                ts: now_kst_fake_epoch(),
            },
        );
    }

    /// 스캘핑 핵심 시나리오: 매수 체결 직후(지연 잔고 갱신 도착 전) 즉시 매도가 성공해야 한다
    #[tokio::test]
    async fn sell_right_after_buy_succeeds() {
        let settings = Settings::default();
        let engine = test_engine(Arc::new(MockBroker::new(&settings)), settings);
        engine.refresh_account().await; // 초기 예수금 캐시 적재

        let buy = engine.buy_max("0193T0").await;
        assert!(buy.ok, "매수 실패: {}", buy.message);
        assert!(buy.qty > 0);

        // 캐시에는 아직 포지션이 없다 — 지연 갱신(700ms)을 기다리지 않고 즉시 매도
        let sell = engine.sell_all("0193T0").await;
        assert!(sell.ok, "매도 실패: {}", sell.message);
        assert_eq!(sell.qty, buy.qty, "전량 매도여야 한다");
    }

    /// 매도 직후 재매수: 낡은 캐시의 잔돈이 아니라 매도 대금이 반영된 예수금으로 최대 수량이 잡혀야 한다
    #[tokio::test]
    async fn rebuy_right_after_sell_uses_fresh_cash() {
        let settings = Settings::default();
        let engine = test_engine(Arc::new(MockBroker::new(&settings)), settings);
        engine.refresh_account().await;

        let buy1 = engine.buy_max("0193T0").await;
        assert!(buy1.ok, "{}", buy1.message);
        let sell = engine.sell_all("0193T0").await;
        assert!(sell.ok, "{}", sell.message);

        let buy2 = engine.buy_max("0193T0").await;
        assert!(buy2.ok, "재매수 실패: {}", buy2.message);
        // 낡은 캐시(매수 후 잔돈)로 계산하면 한두 주만 사게 된다
        assert!(
            buy2.qty > buy1.qty / 2,
            "재매수 수량이 너무 적음 (낡은 예수금 캐시 사용 의심): {} vs 첫 매수 {}",
            buy2.qty,
            buy1.qty
        );
    }

    /// 신선한 웹소켓 시세와 계좌 캐시가 있으면 버튼 클릭 경로는 주문 외 REST 조회를 하지 않는다.
    /// 현재가와 관계없이 지금 체결 가능한 매도1호가를 기준으로 상한과 95% 수량을 계산한다.
    #[tokio::test]
    async fn cached_buy_places_order_without_extra_queries() {
        let broker = Arc::new(FastBuyBroker {
            account_calls: AtomicUsize::new(0),
            snapshot_calls: AtomicUsize::new(0),
            order: Mutex::new(None),
        });
        let settings = Settings::default();
        let engine = test_engine(Arc::clone(&broker) as Arc<dyn Broker>, settings);
        engine.refresh_account().await;
        seed_quote(&engine, 10_100.0);

        let result = engine.buy_max("0193T0").await;

        assert!(result.ok, "{}", result.message);
        assert_eq!(result.price, 10_400); // 10,100원 +3%를 ETF 호가단위로 내림
        assert_eq!(result.qty, 94); // 1,000,000원 × 95% / 매도1호가 10,100원
        assert_eq!(broker.account_calls.load(Ordering::SeqCst), 1);
        assert_eq!(broker.snapshot_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            broker.order.lock().unwrap().as_ref(),
            Some(&("0193T0".into(), 94, 10_400, true))
        );
    }

    struct FastBuyBroker {
        account_calls: AtomicUsize,
        snapshot_calls: AtomicUsize,
        order: Mutex<Option<(String, u64, u64, bool)>>,
    }

    #[async_trait::async_trait]
    impl Broker for FastBuyBroker {
        async fn candles_1m(&self, _code: &str) -> AppResult<Vec<Candle>> {
            Ok(Vec::new())
        }
        async fn account(&self) -> AppResult<AccountSnapshot> {
            self.account_calls.fetch_add(1, Ordering::SeqCst);
            Ok(AccountSnapshot {
                cash: 1_000_000,
                positions: Vec::new(),
            })
        }
        async fn snapshot(&self, _code: &str) -> AppResult<Quote> {
            self.snapshot_calls.fetch_add(1, Ordering::SeqCst);
            Err(AppError::Config("신선한 캐시가 있어 호출되면 안 됨".into()))
        }
        async fn max_buy_qty(&self, _code: &str, _limit_price: u64) -> AppResult<u64> {
            Err(AppError::Config("빠른 경로에서 호출되면 안 됨".into()))
        }
        async fn place_buy(
            &self,
            code: &str,
            qty: u64,
            price: u64,
            ioc: bool,
        ) -> AppResult<OrderAck> {
            *self.order.lock().unwrap() = Some((code.into(), qty, price, ioc));
            Ok(OrderAck {
                order_no: "TEST-BUY".into(),
                message: "주문 접수".into(),
            })
        }
        async fn place_sell_market(&self, _code: &str, _qty: u64) -> AppResult<OrderAck> {
            Err(AppError::Order("테스트에서 미사용".into()))
        }
        async fn start_feed(
            &self,
            _codes: Vec<String>,
            _tx: mpsc::Sender<FeedEvent>,
        ) -> AppResult<Vec<JoinHandle<()>>> {
            Ok(Vec::new())
        }
    }

    /// 잔고 조회 횟수만 세는 브로커 (합류 검증용)
    struct CountingBroker {
        account_calls: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl Broker for CountingBroker {
        async fn candles_1m(&self, _code: &str) -> AppResult<Vec<Candle>> {
            Ok(Vec::new())
        }
        async fn account(&self) -> AppResult<AccountSnapshot> {
            self.account_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(AccountSnapshot { cash: 0, positions: Vec::new() })
        }
        async fn snapshot(&self, _code: &str) -> AppResult<Quote> {
            Err(AppError::Config("테스트에서 미사용".into()))
        }
        async fn max_buy_qty(&self, _code: &str, _limit_price: u64) -> AppResult<u64> {
            Err(AppError::Order("테스트에서 미사용".into()))
        }
        async fn place_buy(&self, _c: &str, _q: u64, _p: u64, _i: bool) -> AppResult<OrderAck> {
            Err(AppError::Order("테스트에서 미사용".into()))
        }
        async fn place_sell_market(&self, _c: &str, _q: u64) -> AppResult<OrderAck> {
            Err(AppError::Order("테스트에서 미사용".into()))
        }
        async fn start_feed(
            &self,
            _codes: Vec<String>,
            _tx: mpsc::Sender<FeedEvent>,
        ) -> AppResult<Vec<JoinHandle<()>>> {
            Ok(Vec::new())
        }
    }

    /// 부분체결 연발 등으로 갱신 예약이 몰려도 실제 잔고 조회는 1회로 합류돼야 한다
    #[tokio::test]
    async fn burst_refresh_requests_coalesce_into_one() {
        let broker = Arc::new(CountingBroker { account_calls: AtomicUsize::new(0) });
        let engine = test_engine(Arc::clone(&broker) as Arc<dyn Broker>, Settings::default());

        for _ in 0..5 {
            engine.schedule_account_refresh();
        }
        tokio::time::sleep(std::time::Duration::from_millis(1_000)).await;
        assert_eq!(
            broker.account_calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "연속 예약은 1회 조회로 합류돼야 한다"
        );
    }

    /// 첫 매수는 거부하고 두 번째부터 접수하는 브로커 (거부 시 재주문 검증용)
    struct RejectFirstBuyBroker {
        psbl_qty: u64,
        buy_calls: AtomicUsize,
        last_order: Mutex<Option<(u64, u64)>>, // (qty, price)
    }

    impl RejectFirstBuyBroker {
        fn new(psbl_qty: u64) -> Arc<Self> {
            Arc::new(Self {
                psbl_qty,
                buy_calls: AtomicUsize::new(0),
                last_order: Mutex::new(None),
            })
        }
    }

    const REJECT_MSG: &str = "주문가능금액을 초과했습니다";

    #[async_trait::async_trait]
    impl Broker for RejectFirstBuyBroker {
        async fn candles_1m(&self, _code: &str) -> AppResult<Vec<Candle>> {
            Ok(Vec::new())
        }
        async fn account(&self) -> AppResult<AccountSnapshot> {
            Ok(AccountSnapshot { cash: 1_000_000, positions: Vec::new() })
        }
        async fn snapshot(&self, _code: &str) -> AppResult<Quote> {
            Err(AppError::Config("테스트에서 미사용".into()))
        }
        async fn max_buy_qty(&self, _code: &str, _limit_price: u64) -> AppResult<u64> {
            Ok(self.psbl_qty)
        }
        async fn place_buy(&self, _c: &str, qty: u64, price: u64, _i: bool) -> AppResult<OrderAck> {
            if self.buy_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(AppError::Order(REJECT_MSG.into()));
            }
            *self.last_order.lock().unwrap() = Some((qty, price));
            Ok(OrderAck { order_no: "RETRY-BUY".into(), message: "주문 접수".into() })
        }
        async fn place_sell_market(&self, _c: &str, _q: u64) -> AppResult<OrderAck> {
            Err(AppError::Order("테스트에서 미사용".into()))
        }
        async fn start_feed(
            &self,
            _codes: Vec<String>,
            _tx: mpsc::Sender<FeedEvent>,
        ) -> AppResult<Vec<JoinHandle<()>>> {
            Ok(Vec::new())
        }
    }

    /// 캐시 예수금이 부풀려져 거부돼도 KIS 매수가능수량으로 1회 재주문해 체결시킨다
    #[tokio::test]
    async fn rejected_buy_retries_with_kis_psbl_qty() {
        let broker = RejectFirstBuyBroker::new(90); // 캐시 기준 94주 < 실제 허용 90주
        let engine = test_engine(Arc::clone(&broker) as Arc<dyn Broker>, Settings::default());
        engine.refresh_account().await;
        seed_quote(&engine, 10_100.0);

        let result = engine.buy_max("0193T0").await;

        assert!(result.ok, "{}", result.message);
        assert_eq!(result.qty, 90, "KIS가 계산한 수량으로 재주문해야 한다");
        assert_eq!(result.price, 10_400);
        assert_eq!(broker.buy_calls.load(Ordering::SeqCst), 2);
        assert_eq!(*broker.last_order.lock().unwrap(), Some((90, 10_400)));
    }

    /// 매수가능수량이 0이면 재주문 없이 명확한 실패를 돌려준다
    #[tokio::test]
    async fn rejected_buy_with_zero_psbl_fails_without_retry() {
        let broker = RejectFirstBuyBroker::new(0);
        let engine = test_engine(Arc::clone(&broker) as Arc<dyn Broker>, Settings::default());
        engine.refresh_account().await;
        seed_quote(&engine, 10_100.0);

        let result = engine.buy_max("0193T0").await;

        assert!(!result.ok);
        assert!(result.message.contains("주문가능금액 부족"), "{}", result.message);
        assert_eq!(broker.buy_calls.load(Ordering::SeqCst), 1, "재주문하면 안 된다");
    }

    /// 금액 부족이 원인이 아니면(수량이 줄지 않으면) 원래 에러를 그대로 노출한다
    #[tokio::test]
    async fn rejected_buy_for_other_reason_keeps_original_error() {
        let broker = RejectFirstBuyBroker::new(1_000); // 허용 수량이 주문 수량보다 크다
        let engine = test_engine(Arc::clone(&broker) as Arc<dyn Broker>, Settings::default());
        engine.refresh_account().await;
        seed_quote(&engine, 10_100.0);

        let result = engine.buy_max("0193T0").await;

        assert!(!result.ok);
        assert!(result.message.contains(REJECT_MSG), "{}", result.message);
        assert_eq!(broker.buy_calls.load(Ordering::SeqCst), 1, "재주문하면 안 된다");
    }

    /// REST 스냅샷 폴백으로 받은 시세도 프론트로 emit돼야 화면 수익률이 갱신된다
    #[tokio::test]
    async fn snapshot_fallback_emits_quote_event() {
        let settings = Settings::default();
        let broker = Arc::new(MockBroker::new(&settings));
        let events: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&events);
        let emit: EmitFn = Box::new(move |event, _payload| {
            sink.lock().unwrap().push(event.to_string());
        });
        let engine = test_engine_with_emit(broker, settings, Some(emit));

        let q = engine.fresh_quote("0193T0").await;

        assert!(q.is_some());
        assert!(
            events.lock().unwrap().iter().any(|e| e == "quote"),
            "스냅샷 폴백 시 quote 이벤트가 emit돼야 한다"
        );
    }
}
