use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use chrono::{Datelike, Duration as ChronoDuration, NaiveDate, NaiveDateTime, NaiveTime, Weekday};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::broker::{Broker, OrderAck};
use crate::error::{AppError, AppResult};
use crate::types::{
    AccountSnapshot, Candle, FeedEvent, FillEvent, Position, Quote, Settings, Side,
};
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

/// 데모용 미체결 지정가 매도 (예약 매도) — 시세가 limit_price에 도달하면 체결
struct RestingSell {
    qty: u64,
    limit_price: u64,
    order_no: String,
}

struct MockMarket {
    syms: HashMap<String, SymState>,
    cash: u64,
    positions: HashMap<String, (u64, f64)>, // code -> (qty, avg)
    /// code -> 걸어둔 지정가 매도. 코드당 1건(예약 교체 시 덮어씀)
    resting_sells: HashMap<String, RestingSell>,
}

/// 미체결 지정가 매도 중 현재가가 목표가에 도달한 것을 체결 처리하고 FillEvent를 반환한다.
/// (실전에서는 거래소가 하는 일을 데모에서 흉내 낸다)
fn check_resting_fills(m: &mut MockMarket) -> Vec<FillEvent> {
    let triggered: Vec<(String, u64, u64)> = m
        .resting_sells
        .iter()
        .filter_map(|(code, r)| {
            let price = m.syms.get(code)?.price;
            (price >= r.limit_price as f64).then(|| (code.clone(), r.qty, r.limit_price))
        })
        .collect();

    let mut fills = Vec::new();
    for (code, qty, limit_price) in triggered {
        m.resting_sells.remove(&code);
        let held = m.positions.get(&code).map(|(q, _)| *q).unwrap_or(0);
        let sell_qty = qty.min(held);
        if sell_qty == 0 {
            continue;
        }
        m.cash += limit_price * sell_qty;
        if let Some(entry) = m.positions.get_mut(&code) {
            entry.0 -= sell_qty;
            if entry.0 == 0 {
                m.positions.remove(&code);
            }
        }
        // 목표 호가 그대로 체결 (슬리피지 없음)
        fills.push(FillEvent {
            fill_id: format!("mock:{code}:{sell_qty}:{limit_price}"),
            order_no: String::new(),
            original_order_no: String::new(),
            org_no: String::new(),
            code,
            side: Side::Sell,
            qty: sell_qty,
            price: limit_price as f64,
            filled_at: crate::util::now_kst_fake_epoch(),
            status: "filled".into(),
        });
    }
    fills
}

impl MockMarket {
    fn quote(&self, code: &str, volume: f64, ts: i64) -> Option<Quote> {
        let s = self.syms.get(code)?;
        let tick = tick_size(s.price as u64, s.etf) as f64;
        let received_at_micros = crate::util::monotonic_now()
            .as_micros()
            .try_into()
            .unwrap_or(u64::MAX);
        Some(Quote {
            code: code.to_string(),
            price: s.price,
            change_rate: (s.price / s.day_open - 1.0) * 100.0,
            ask1: s.price + tick,
            bid1: (s.price - tick).max(tick),
            ask1_qty: 100_000,
            bid1_qty: 100_000,
            volume,
            trade_sequence: received_at_micros,
            received_at_micros,
            trade_ts: ts,
            book_ts: ts,
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
        for s in settings
            .chart_symbols
            .iter()
            .chain(settings.trade_symbols.iter())
        {
            let p = base_price(&s.code) as f64;
            syms.insert(
                s.code.clone(),
                SymState {
                    price: p,
                    day_open: p,
                    etf: s.etf,
                },
            );
        }
        Self {
            market: Arc::new(Mutex::new(MockMarket {
                syms,
                cash: INITIAL_CASH,
                positions: HashMap::new(),
                resting_sells: HashMap::new(),
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
        let seed: u64 = code
            .bytes()
            .map(u64::from)
            .fold(7, |a, b| a.wrapping_mul(31).wrapping_add(b));
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
                    pnl_rate: if *avg > 0.0 {
                        (cur / avg - 1.0) * 100.0
                    } else {
                        0.0
                    },
                }
            })
            .collect();
        Ok(AccountSnapshot {
            cash: m.cash,
            positions,
        })
    }

    async fn snapshot(&self, code: &str) -> AppResult<Quote> {
        let m = self.market.lock().unwrap();
        m.quote(code, 0.0, crate::util::now_kst_fake_epoch())
            .ok_or_else(|| AppError::Config(format!("데모에 없는 종목: {code}")))
    }

    async fn max_buy_qty(&self, _code: &str, limit_price: u64) -> AppResult<u64> {
        if limit_price == 0 {
            return Ok(0);
        }
        let m = self.market.lock().unwrap();
        Ok(m.cash / limit_price)
    }

    async fn place_buy(
        &self,
        code: &str,
        qty: u64,
        limit_price: u64,
        _ioc: bool,
    ) -> AppResult<OrderAck> {
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
                    org_no: "DEMO-ORG".into(),
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
            FillEvent {
                code: code.to_string(),
                side: Side::Buy,
                qty,
                price: fill_price,
                fill_id: format!("DEMO-BUY-{code}"),
                order_no: "DEMO-BUY".into(),
                original_order_no: String::new(),
                org_no: "DEMO-ORG".into(),
                filled_at: crate::util::now_kst_fake_epoch(),
                status: "filled".into(),
            }
        };
        self.send_fill(fill).await;
        Ok(OrderAck {
            order_no: "DEMO-BUY".into(),
            org_no: "DEMO-ORG".into(),
            message: "데모 체결".into(),
        })
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
            FillEvent {
                code: code.to_string(),
                side: Side::Sell,
                qty,
                price: fill_price,
                fill_id: format!("DEMO-SELL-{code}"),
                order_no: "DEMO-SELL".into(),
                original_order_no: String::new(),
                org_no: "DEMO-ORG".into(),
                filled_at: crate::util::now_kst_fake_epoch(),
                status: "filled".into(),
            }
        };
        self.send_fill(fill).await;
        Ok(OrderAck {
            order_no: "DEMO-SELL".into(),
            org_no: "DEMO-ORG".into(),
            message: "데모 체결".into(),
        })
    }

    async fn place_sell_limit(
        &self,
        code: &str,
        qty: u64,
        limit_price: u64,
    ) -> AppResult<OrderAck> {
        let mut m = self.market.lock().unwrap();
        let held = m.positions.get(code).map(|(q, _)| *q).unwrap_or(0);
        if held < qty || qty == 0 {
            return Err(AppError::Order("보유 수량 부족 (데모)".into()));
        }
        // 즉시 체결하지 않고 호가창에 걸어둔다 — 피드 루프가 시세 도달 시 체결한다
        let order_no = format!("DEMO-RESV-{code}");
        m.resting_sells.insert(
            code.to_string(),
            RestingSell {
                qty,
                limit_price,
                order_no: order_no.clone(),
            },
        );
        Ok(OrderAck {
            order_no,
            org_no: "DEMO-ORG".into(),
            message: "데모 예약 접수".into(),
        })
    }

    async fn cancel_order(
        &self,
        code: &str,
        _order_no: &str,
        _org_no: &str,
    ) -> AppResult<OrderAck> {
        let mut m = self.market.lock().unwrap();
        match m.resting_sells.remove(code) {
            Some(r) => Ok(OrderAck {
                order_no: r.order_no,
                org_no: "DEMO-ORG".into(),
                message: "데모 예약 취소".into(),
            }),
            None => Err(AppError::Order("취소할 예약이 없습니다 (데모)".into())),
        }
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
                let (quotes, fills): (Vec<Quote>, Vec<FillEvent>) = {
                    let mut m = market.lock().unwrap();
                    for code in &codes {
                        if let Some(s) = m.syms.get_mut(code) {
                            let tick = tick_size(s.price as u64, s.etf) as f64;
                            let step: i32 = rng.gen_range(-2..=2);
                            // 시가에서 멀어질수록 되돌리는 힘을 줘서 데모 시세가 발산하지 않게 한다
                            let bias =
                                ((s.day_open - s.price) / (s.day_open * 0.02)).clamp(-1.0, 1.0);
                            let raw = s.price + tick * (step as f64 + bias);
                            // 호가단위에 맞춰 반올림 (소수점 가격 방지)
                            s.price = ((raw / tick).round() * tick).max(tick);
                        }
                    }
                    // 예약 매도 중 목표가 도달분을 체결 처리
                    let fills = check_resting_fills(&mut m);
                    let quotes = codes
                        .iter()
                        .filter_map(|code| m.quote(code, rng.gen_range(1.0..150.0), ts))
                        .collect();
                    (quotes, fills)
                };
                for q in quotes {
                    if tx.send(FeedEvent::Quote(q)).await.is_err() {
                        return; // 엔진 종료됨
                    }
                }
                for f in fills {
                    if tx.send(FeedEvent::Fill(f)).await.is_err() {
                        return; // 엔진 종료됨
                    }
                }
            }
        });
        Ok(vec![handle])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn market_with_holding(code: &str, price: f64, qty: u64, avg: f64) -> MockMarket {
        let mut syms = HashMap::new();
        syms.insert(
            code.to_string(),
            SymState {
                price,
                day_open: price,
                etf: true,
            },
        );
        let mut positions = HashMap::new();
        positions.insert(code.to_string(), (qty, avg));
        MockMarket {
            syms,
            cash: 0,
            positions,
            resting_sells: HashMap::new(),
        }
    }

    #[test]
    fn resting_sell_fills_only_when_price_reaches_target() {
        let mut m = market_with_holding("0193T0", 10_000.0, 100, 9_800.0);
        m.resting_sells.insert(
            "0193T0".into(),
            RestingSell {
                qty: 100,
                limit_price: 10_050,
                order_no: "R".into(),
            },
        );
        // 목표가(10,050) 미달 → 체결 없음, 예약 유지
        assert!(check_resting_fills(&mut m).is_empty());
        assert_eq!(m.resting_sells.len(), 1);

        // 시세가 목표가 도달 → 전량 체결
        m.syms.get_mut("0193T0").unwrap().price = 10_050.0;
        let fills = check_resting_fills(&mut m);
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].qty, 100);
        assert_eq!(fills[0].price, 10_050.0); // 슬리피지 없이 목표가 체결
        assert_eq!(fills[0].side, Side::Sell);
        // 예약 제거 + 보유 소진 + 대금 반영
        assert!(m.resting_sells.is_empty());
        assert!(!m.positions.contains_key("0193T0"));
        assert_eq!(m.cash, 10_050 * 100);
    }

    #[tokio::test]
    async fn place_and_cancel_reserved_sell() {
        let settings = Settings::default();
        let broker = MockBroker::new(&settings);
        // 보유 없으면 예약 거부
        assert!(broker.place_sell_limit("0193T0", 10, 13_000).await.is_err());

        // 보유를 채운 뒤 예약 → 접수(즉시 체결 아님)
        broker
            .market
            .lock()
            .unwrap()
            .positions
            .insert("0193T0".into(), (10, 12_800.0));
        let ack = broker.place_sell_limit("0193T0", 10, 13_000).await.unwrap();
        assert!(ack.order_no.contains("RESV"));
        assert_eq!(broker.market.lock().unwrap().resting_sells.len(), 1);

        // 취소 → 예약 제거
        broker
            .cancel_order("0193T0", &ack.order_no, &ack.org_no)
            .await
            .unwrap();
        assert!(broker.market.lock().unwrap().resting_sells.is_empty());
        // 취소할 예약이 없으면 에러
        assert!(broker.cancel_order("0193T0", "x", "y").await.is_err());
    }
}
