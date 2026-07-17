use std::collections::HashMap;
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

pub struct Engine {
    /// UI 전용 필드(버퍼틱 등)는 엔진 재시작 없이 갱신되므로 RwLock으로 보관
    settings: RwLock<Settings>,
    broker: Arc<dyn Broker>,
    quotes: RwLock<HashMap<String, Quote>>,
    account: RwLock<AccountSnapshot>,
    candle_cache: CandleCache,
    last_error: Mutex<String>,
    app: AppHandle,
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
        app,
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
    tasks.push(tokio::spawn(async move {
        for code in warm_codes {
            if let Err(e) = warm_engine.candles(&code).await {
                tracing::warn!("차트 워밍업 실패({code}): {e}");
            }
        }
    }));

    Ok(EngineHandle { engine, tasks })
}

async fn consume_feed(engine: Arc<Engine>, mut rx: mpsc::Receiver<FeedEvent>) {
    while let Some(ev) = rx.recv().await {
        match ev {
            FeedEvent::Quote(q) => {
                engine.quotes.write().unwrap().insert(q.code.clone(), q.clone());
                let _ = engine.app.emit("quote", &q);
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
                    let _ = engine.app.emit("quote", &q);
                }
            }
            FeedEvent::Fill(fill) => {
                let _ = engine.app.emit("fill", &fill);
                let eng = Arc::clone(&engine);
                tokio::spawn(async move { eng.refresh_account().await });
            }
            FeedEvent::Conn(connected) => {
                let _ = engine.app.emit("conn", serde_json::json!({ "connected": connected }));
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
    /// 브로커 재시작이 필요 없는 설정 변경(버퍼틱 등)을 반영
    pub fn update_settings(&self, new: Settings) {
        *self.settings.write().unwrap() = new;
    }

    pub async fn refresh_account(&self) {
        match self.broker.account().await {
            Ok(snap) => {
                *self.account.write().unwrap() = snap.clone();
                self.last_error.lock().unwrap().clear();
                let _ = self.app.emit("account", &snap);
            }
            Err(e) => {
                let msg = e.to_string();
                let mut last = self.last_error.lock().unwrap();
                if *last != msg {
                    *last = msg.clone();
                    let _ = self.app.emit("engine-error", format!("계좌 조회 실패: {msg}"));
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
                Some(q)
            }
            Err(e) => {
                tracing::warn!("스냅샷 폴백 실패({code}): {e}");
                cached.filter(|q| now - q.ts <= QUOTE_STALE_LIMIT_SECS)
            }
        }
    }

    /// 원클릭 최대 수량 매수: 매도1호가+버퍼틱 IOC지정가
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
        let ask1 = if q.ask1 > 0.0 { q.ask1 } else { q.price };
        if ask1 <= 0.0 {
            return fail("호가 정보 없음".into());
        }

        let (etf, buffer_ticks, mode) = {
            let s = self.settings.read().unwrap();
            (s.is_etf(code), s.buffer_ticks, s.mode)
        };
        let limit = buy_limit_price(ask1 as u64, buffer_ticks, etf);
        let cash = self.account.read().unwrap().cash;
        let qty = max_buy_qty(cash, limit);
        if qty == 0 {
            return fail(format!("주문가능금액 부족 (예수금 {cash}원)"));
        }

        // 모의투자는 IOC 미지원이라 일반 지정가로 대체
        let ioc = !matches!(mode, TradeMode::Paper);
        match self.broker.place_buy(code, qty, limit, ioc).await {
            Ok(ack) => {
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
                self.schedule_account_refresh();
                fail(e.to_string())
            }
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

        let qty = self
            .account
            .read()
            .unwrap()
            .positions
            .iter()
            .find(|p| p.code == code)
            .map(|p| p.qty)
            .unwrap_or(0);
        if qty == 0 {
            return fail("보유 수량 없음".into());
        }

        match self.broker.place_sell_market(code, qty).await {
            Ok(ack) => {
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

    /// 주문 직후 체결 반영을 위해 잠시 뒤 잔고 갱신 (체결통보와 별개의 안전망)
    fn schedule_account_refresh(&self) {
        let Some(engine) = self.weak.upgrade() else { return };
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(700)).await;
            engine.refresh_account().await;
        });
    }
}
