use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use chrono::{Datelike, Duration as ChronoDuration, NaiveDate, NaiveDateTime, NaiveTime, Weekday};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::broker::{Broker, OrderAck};
use crate::error::{AppError, AppResult};
use crate::types::{AccountSnapshot, Candle, FeedEvent, FillEvent, Position, Quote, Settings, Side};
use crate::util::{naive_to_fake_epoch, now_kst, tick_size};

const INITIAL_CASH: u64 = 10_000_000;
const SESSION_OPEN: (u32, u32) = (9, 0);
const SESSION_CLOSE: (u32, u32) = (15, 30);
const HISTORY_DAYS: usize = 6;

fn base_price(code: &str) -> u64 {
    match code {
        "000660" => 194_500,
        "005930" => 86_400,
        "0193T0" => 12_800,
        "0197X0" => 8_300,
        "0193W0" => 15_200,
        "0193L0" => 9_100,
        _ => {
            let h: u32 = code.bytes().map(u32::from).sum();
            5_000 + u64::from(h % 40) * 500
        }
    }
}

struct SymState {
    price: f64,
    day_open: f64,
    etf: bool,
}

struct MockMarket {
    syms: HashMap<String, SymState>,
    cash: u64,
    positions: HashMap<String, (u64, f64)>, // code -> (qty, avg)
}

impl MockMarket {
    fn quote(&self, code: &str, volume: f64, ts: i64) -> Option<Quote> {
        let s = self.syms.get(code)?;
        let tick = tick_size(s.price as u64, s.etf) as f64;
        Some(Quote {
            code: code.to_string(),
            price: s.price,
            change_rate: (s.price / s.day_open - 1.0) * 100.0,
            ask1: s.price + tick,
            bid1: (s.price - tick).max(tick),
            volume,
            ts,
        })
    }
}

pub struct MockBroker {
    market: Arc<Mutex<MockMarket>>,
    tx: Mutex<Option<mpsc::Sender<FeedEvent>>>,
}

impl MockBroker {
    pub fn new(settings: &Settings) -> Self {
        let mut syms = HashMap::new();
        for s in settings.chart_symbols.iter().chain(settings.trade_symbols.iter()) {
            let p = base_price(&s.code) as f64;
            syms.insert(
                s.code.clone(),
                SymState { price: p, day_open: p, etf: s.etf },
            );
        }
        Self {
            market: Arc::new(Mutex::new(MockMarket {
                syms,
                cash: INITIAL_CASH,
                positions: HashMap::new(),
            })),
            tx: Mutex::new(None),
        }
    }

    async fn send_fill(&self, fill: FillEvent) {
        let tx = self.tx.lock().unwrap().clone();
        if let Some(tx) = tx {
            let _ = tx.send(FeedEvent::Fill(fill)).await;
        }
    }
}

/// 최근 영업일들의 장중 1분 단위 시각 목록 (오래된 것부터)
fn session_minutes(days: usize) -> Vec<NaiveDateTime> {
    let now = now_kst();
    let open = NaiveTime::from_hms_opt(SESSION_OPEN.0, SESSION_OPEN.1, 0).unwrap();
    let close = NaiveTime::from_hms_opt(SESSION_CLOSE.0, SESSION_CLOSE.1, 0).unwrap();

    let mut dates: Vec<NaiveDate> = Vec::new();
    let mut d = now.date();
    while dates.len() < days {
        let is_weekend = matches!(d.weekday(), Weekday::Sat | Weekday::Sun);
        let today_before_open = d == now.date() && now.time() < open;
        if !is_weekend && !today_before_open {
            dates.push(d);
        }
        d -= ChronoDuration::days(1);
    }
    dates.reverse();

    let mut out = Vec::new();
    for date in dates {
        let mut t = open;
        while t < close {
            let dt = date.and_time(t);
            if date == now.date() && dt > now {
                break;
            }
            out.push(dt);
            t += ChronoDuration::minutes(1);
        }
    }
    out
}

#[async_trait::async_trait]
impl Broker for MockBroker {
    async fn candles_1m(&self, code: &str) -> AppResult<Vec<Candle>> {
        let (end_price, etf) = {
            let m = self.market.lock().unwrap();
            let s = m
                .syms
                .get(code)
                .ok_or_else(|| AppError::Config(format!("데모에 없는 종목: {code}")))?;
            (s.price, s.etf)
        };

        let minutes = session_minutes(HISTORY_DAYS);
        let n = minutes.len();
        let seed: u64 = code.bytes().map(u64::from).fold(7, |a, b| a.wrapping_mul(31).wrapping_add(b));
        let mut rng = StdRng::seed_from_u64(seed);

        // 현재가에서 뒤로 걸어가며 종가 시퀀스를 만든 뒤 앞으로 캔들 구성
        let mut closes = vec![0.0f64; n];
        let mut p = end_price;
        for i in (0..n).rev() {
            closes[i] = p;
            let drift: f64 = rng.gen_range(-0.0012..0.0012);
            p = (p * (1.0 - drift)).max(1.0);
        }

        let mut out = Vec::with_capacity(n);
        let mut prev_close = closes.first().copied().unwrap_or(end_price);
        for (i, dt) in minutes.iter().enumerate() {
            let close = closes[i];
            let open = prev_close;
            let tick = tick_size(close as u64, etf) as f64;
            let wiggle = tick * rng.gen_range(0.0..3.0);
            let high = open.max(close) + wiggle;
            let low = (open.min(close) - wiggle).max(tick);
            let round = |v: f64| (v / tick).round() * tick;
            out.push(Candle {
                time: naive_to_fake_epoch(*dt),
                open: round(open),
                high: round(high),
                low: round(low),
                close: round(close),
                volume: rng.gen_range(500.0..30_000.0),
            });
            prev_close = close;
        }
        Ok(out)
    }

    async fn account(&self) -> AppResult<AccountSnapshot> {
        let m = self.market.lock().unwrap();
        let positions = m
            .positions
            .iter()
            .filter(|(_, (qty, _))| *qty > 0)
            .map(|(code, (qty, avg))| {
                let cur = m.syms.get(code).map(|s| s.price).unwrap_or(*avg);
                let pnl = (cur - avg) * (*qty as f64);
                Position {
                    code: code.clone(),
                    qty: *qty,
                    avg_price: *avg,
                    eval_pnl: pnl,
                    pnl_rate: if *avg > 0.0 { (cur / avg - 1.0) * 100.0 } else { 0.0 },
                }
            })
            .collect();
        Ok(AccountSnapshot { cash: m.cash, positions })
    }

    async fn snapshot(&self, code: &str) -> AppResult<Quote> {
        let m = self.market.lock().unwrap();
        m.quote(code, 0.0, crate::util::now_kst_fake_epoch())
            .ok_or_else(|| AppError::Config(format!("데모에 없는 종목: {code}")))
    }

    async fn place_buy(&self, code: &str, qty: u64, limit_price: u64, _ioc: bool) -> AppResult<OrderAck> {
        let fill = {
            let mut m = self.market.lock().unwrap();
            let s = m
                .syms
                .get(code)
                .ok_or_else(|| AppError::Order(format!("데모에 없는 종목: {code}")))?;
            let tick = tick_size(s.price as u64, s.etf) as f64;
            let ask1 = s.price + tick;
            if (limit_price as f64) < ask1 {
                return Ok(OrderAck {
                    order_no: "DEMO-IOC".into(),
                    message: "IOC 미체결 — 잔량 자동취소".into(),
                });
            }
            let fill_price = ask1;
            let cost = (fill_price * qty as f64) as u64;
            if cost > m.cash {
                return Err(AppError::Order("주문가능금액 부족 (데모)".into()));
            }
            m.cash -= cost;
            let entry = m.positions.entry(code.to_string()).or_insert((0, 0.0));
            let total_cost = entry.1 * entry.0 as f64 + fill_price * qty as f64;
            entry.0 += qty;
            entry.1 = total_cost / entry.0 as f64;
            FillEvent { code: code.to_string(), side: Side::Buy, qty, price: fill_price }
        };
        self.send_fill(fill).await;
        Ok(OrderAck { order_no: "DEMO-BUY".into(), message: "데모 체결".into() })
    }

    async fn place_sell_market(&self, code: &str, qty: u64) -> AppResult<OrderAck> {
        let fill = {
            let mut m = self.market.lock().unwrap();
            let s = m
                .syms
                .get(code)
                .ok_or_else(|| AppError::Order(format!("데모에 없는 종목: {code}")))?;
            let tick = tick_size(s.price as u64, s.etf) as f64;
            let fill_price = (s.price - tick).max(tick);
            let held = m.positions.get(code).map(|(q, _)| *q).unwrap_or(0);
            if held < qty || qty == 0 {
                return Err(AppError::Order("보유 수량 부족 (데모)".into()));
            }
            m.cash += (fill_price * qty as f64) as u64;
            let entry = m.positions.get_mut(code).unwrap();
            entry.0 -= qty;
            if entry.0 == 0 {
                m.positions.remove(code);
            }
            FillEvent { code: code.to_string(), side: Side::Sell, qty, price: fill_price }
        };
        self.send_fill(fill).await;
        Ok(OrderAck { order_no: "DEMO-SELL".into(), message: "데모 체결".into() })
    }

    async fn start_feed(
        &self,
        codes: Vec<String>,
        tx: mpsc::Sender<FeedEvent>,
    ) -> AppResult<Vec<JoinHandle<()>>> {
        *self.tx.lock().unwrap() = Some(tx.clone());
        let market = Arc::clone(&self.market);

        let handle = tokio::spawn(async move {
            let _ = tx.send(FeedEvent::Conn(true)).await;
            let mut rng = StdRng::from_entropy();
            let mut timer = tokio::time::interval(std::time::Duration::from_millis(400));
            loop {
                timer.tick().await;
                let ts = crate::util::now_kst_fake_epoch();
                let quotes: Vec<Quote> = {
                    let mut m = market.lock().unwrap();
                    for code in &codes {
                        if let Some(s) = m.syms.get_mut(code) {
                            let tick = tick_size(s.price as u64, s.etf) as f64;
                            let step: i32 = rng.gen_range(-2..=2);
                            // 시가에서 멀어질수록 되돌리는 힘을 줘서 데모 시세가 발산하지 않게 한다
                            let bias = ((s.day_open - s.price) / (s.day_open * 0.02)).clamp(-1.0, 1.0);
                            let raw = s.price + tick * (step as f64 + bias);
                            // 호가단위에 맞춰 반올림 (소수점 가격 방지)
                            s.price = ((raw / tick).round() * tick).max(tick);
                        }
                    }
                    codes
                        .iter()
                        .filter_map(|code| m.quote(code, rng.gen_range(1.0..150.0), ts))
                        .collect()
                };
                for q in quotes {
                    if tx.send(FeedEvent::Quote(q)).await.is_err() {
                        return; // 엔진 종료됨
                    }
                }
            }
        });
        Ok(vec![handle])
    }
}
