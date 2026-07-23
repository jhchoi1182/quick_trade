use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use tokio::sync::Mutex;

use crate::error::AppResult;
use crate::types::{Candle, Quote};

/// 15분봉 120개보다 넉넉한 1분봉 이력(40시간)을 유지한다.
pub const MAX_ONE_MINUTE_BARS: usize = 2_400;

#[derive(Default)]
struct HistoryEntry {
    bars: Arc<Vec<Candle>>,
    /// 시작 전 또는 실제 연결 공백 뒤에는 다음 조회가 백필을 수행해야 한다.
    needs_backfill: bool,
    /// 백필 도중 들어온 실시간 체결을 응답으로 덮어쓰지 않기 위한 임시 봉.
    pending: Vec<Candle>,
    /// 백필 도중 또 다른 연결 공백이 생겼는지 판별한다.
    generation: u64,
    /// 같은 WebSocket 체결 프레임 재전송으로 거래량을 중복 합산하지 않는다.
    last_trade_sequence: Option<u64>,
    /// 거래일이 바뀌면 KIS 누적거래량 순번이 다시 작아질 수 있다.
    last_trade_day: Option<i64>,
}

/// 종목별 1분봉을 최초 한 번 백필한 뒤 실시간 체결로만 갱신한다.
///
/// 60초 TTL을 두지 않는다. 정상 연결 중 반복되는 차트/LLM 조회는 언제나
/// 메모리 스냅샷을 반환하며, 실제 연결 공백이 확인되면 다음 조회에서 다시
/// 백필한다. 종목별 잠금으로 동시 조회도 실제 브로커 호출 한 번으로 합친다.
pub struct MarketHistory {
    entries: Mutex<HashMap<String, HistoryEntry>>,
    flights: Mutex<HashMap<String, Arc<Mutex<()>>>>,
}

impl Default for MarketHistory {
    fn default() -> Self {
        Self::new()
    }
}

impl MarketHistory {
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            flights: Mutex::new(HashMap::new()),
        }
    }

    /// 정상 초기화된 이력이 있으면 브로커를 호출하지 않고 그대로 반환한다.
    pub async fn get_or_fetch<F, Fut>(&self, code: &str, fetch: F) -> AppResult<Arc<Vec<Candle>>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = AppResult<Vec<Candle>>>,
    {
        if let Some(bars) = self.healthy(code).await {
            return Ok(bars);
        }

        let flight = {
            let mut flights = self.flights.lock().await;
            Arc::clone(flights.entry(code.to_string()).or_default())
        };
        let _flight_guard = flight.lock().await;

        // 같은 비행을 기다리는 동안 앞선 요청이 정상 이력을 채웠을 수 있다.
        if let Some(bars) = self.healthy(code).await {
            return Ok(bars);
        }

        let generation = {
            let mut entries = self.entries.lock().await;
            let entry = entries
                .entry(code.to_string())
                .or_insert_with(|| HistoryEntry {
                    needs_backfill: true,
                    ..HistoryEntry::default()
                });
            entry.generation
        };

        let mut fetched = normalize_bars(fetch().await?);

        let mut entries = self.entries.lock().await;
        let entry = entries.entry(code.to_string()).or_default();
        for pending in entry.pending.drain(..) {
            merge_pending_bar(&mut fetched, pending);
        }
        trim_history(&mut fetched);

        // 요청 도중 새 공백 표시가 들어왔으면 이번 결과는 화면에 보존하되,
        // 정상으로 간주하지 않아 다음 조회가 한 번 더 복구 백필하게 한다.
        let gap_during_fetch = entry.generation != generation;
        entry.bars = Arc::new(fetched);
        entry.needs_backfill = gap_during_fetch;
        Ok(Arc::clone(&entry.bars))
    }

    /// 정상 상태의 현재 불변 스냅샷. 초기화 전이나 공백 표시 뒤에는 `None`이다.
    ///
    /// 호출자는 이 메서드가 브로커를 호출하지 않는다는 점을 이용해 피드 직렬화
    /// 구간에서도 봉·호가·체결 커서를 같은 논리 시점에 복사할 수 있다.
    pub(crate) async fn healthy_snapshot(&self, code: &str) -> Option<Arc<Vec<Candle>>> {
        self.healthy(code).await
    }

    #[cfg(test)]
    pub async fn cached(&self, code: &str) -> Option<Arc<Vec<Candle>>> {
        self.healthy_snapshot(code).await
    }

    /// 실시간 체결 한 건을 해당 종목의 1분봉에 반영한다.
    /// 호가 이벤트(volume=0), 잘못된 가격/시각은 무시한다.
    pub async fn apply_trade(&self, quote: &Quote) -> bool {
        if quote.volume <= 0.0
            || quote.price <= 0.0
            || quote.trade_ts <= 0
            || quote.trade_sequence == 0
        {
            return false;
        }

        let tick = Candle {
            time: minute_bucket(quote.trade_ts),
            open: quote.price,
            high: quote.price,
            low: quote.price,
            close: quote.price,
            volume: quote.volume,
        };

        let mut entries = self.entries.lock().await;
        let entry = entries
            .entry(quote.code.clone())
            .or_insert_with(|| HistoryEntry {
                needs_backfill: true,
                ..HistoryEntry::default()
            });
        let trade_day = quote.trade_ts.div_euclid(86_400);
        match entry.last_trade_day {
            Some(last_day) if trade_day < last_day => return false,
            Some(last_day)
                if trade_day == last_day
                    && entry
                        .last_trade_sequence
                        .is_some_and(|sequence| quote.trade_sequence <= sequence) =>
            {
                return false;
            }
            Some(last_day) if trade_day > last_day => {
                entry.last_trade_sequence = None;
            }
            _ => {}
        }
        entry.last_trade_day = Some(trade_day);
        entry.last_trade_sequence = Some(quote.trade_sequence);

        if entry.needs_backfill {
            apply_live_bar(&mut entry.pending, tick);
        } else {
            let bars = Arc::make_mut(&mut entry.bars);
            apply_live_bar(bars, tick);
            trim_history(bars);
        }
        true
    }

    /// 실제 WebSocket 연결 공백이 확인된 종목만 다음 조회에서 재백필한다.
    #[cfg(test)]
    pub async fn mark_gap(&self, code: &str) {
        let mut entries = self.entries.lock().await;
        let entry = entries.entry(code.to_string()).or_default();
        entry.needs_backfill = true;
        entry.pending.clear();
        entry.last_trade_sequence = None;
        entry.last_trade_day = None;
        entry.generation = entry.generation.wrapping_add(1);
    }

    /// 전체 구독이 끊긴 경우 모든 종목을 다음 조회 시 재백필 대상으로 만든다.
    pub async fn mark_all_gapped(&self) {
        let mut entries = self.entries.lock().await;
        for entry in entries.values_mut() {
            entry.needs_backfill = true;
            entry.pending.clear();
            entry.last_trade_sequence = None;
            entry.last_trade_day = None;
            entry.generation = entry.generation.wrapping_add(1);
        }
    }

    async fn healthy(&self, code: &str) -> Option<Arc<Vec<Candle>>> {
        self.entries
            .lock()
            .await
            .get(code)
            .filter(|entry| !entry.needs_backfill)
            .map(|entry| Arc::clone(&entry.bars))
    }
}

fn minute_bucket(ts: i64) -> i64 {
    ts.div_euclid(60) * 60
}

fn normalize_bars(mut bars: Vec<Candle>) -> Vec<Candle> {
    bars.retain(|bar| {
        bar.time > 0
            && bar.open.is_finite()
            && bar.high.is_finite()
            && bar.low.is_finite()
            && bar.close.is_finite()
            && bar.volume.is_finite()
    });
    for bar in &mut bars {
        bar.time = minute_bucket(bar.time);
    }
    bars.sort_by_key(|bar| bar.time);

    let mut normalized: Vec<Candle> = Vec::with_capacity(bars.len());
    for bar in bars {
        if let Some(last) = normalized.last_mut().filter(|last| last.time == bar.time) {
            last.high = last.high.max(bar.high);
            last.low = last.low.min(bar.low);
            last.close = bar.close;
            last.volume += bar.volume.max(0.0);
        } else {
            normalized.push(bar);
        }
    }
    trim_history(&mut normalized);
    normalized
}

/// 정상 연결 중 체결은 시간 오름차순이어야 한다. 늦게 도착한 과거 체결은
/// 이미 확정된 분봉을 왜곡하지 않도록 무시한다.
fn apply_live_bar(bars: &mut Vec<Candle>, tick: Candle) {
    match bars.last_mut() {
        Some(last) if last.time == tick.time => {
            last.high = last.high.max(tick.high);
            last.low = last.low.min(tick.low);
            last.close = tick.close;
            last.volume += tick.volume;
        }
        Some(last) if last.time > tick.time => {}
        _ => bars.push(tick),
    }
}

/// 백필 응답과 동시 체결이 같은 분에 겹칠 수 있어 거래량은 합산하지 않고
/// 더 큰 값을 택한다. 고가/저가/종가는 실시간 체결을 우선한다.
fn merge_pending_bar(bars: &mut Vec<Candle>, pending: Candle) {
    match bars.binary_search_by_key(&pending.time, |bar| bar.time) {
        Ok(index) => {
            let bar = &mut bars[index];
            bar.high = bar.high.max(pending.high);
            bar.low = bar.low.min(pending.low);
            bar.close = pending.close;
            bar.volume = bar.volume.max(pending.volume);
        }
        Err(index) => bars.insert(index, pending),
    }
}

fn trim_history(bars: &mut Vec<Candle>) {
    if bars.len() > MAX_ONE_MINUTE_BARS {
        bars.drain(..bars.len() - MAX_ONE_MINUTE_BARS);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::*;

    fn candle(time: i64, price: f64, volume: f64) -> Candle {
        Candle {
            time,
            open: price,
            high: price,
            low: price,
            close: price,
            volume,
        }
    }

    fn quote(ts: i64, price: f64, volume: f64) -> Quote {
        Quote {
            code: "000660".into(),
            price,
            change_rate: 0.0,
            ask1: price + 100.0,
            bid1: price - 100.0,
            ask1_qty: 10,
            bid1_qty: 11,
            volume,
            trade_sequence: ts.max(0) as u64,
            received_at_micros: ts.max(0) as u64,
            trade_ts: ts,
            book_ts: ts,
        }
    }

    #[tokio::test]
    async fn concurrent_initialization_is_single_flight() {
        let history = MarketHistory::new();
        let calls = AtomicUsize::new(0);
        let first = || async {
            calls.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(25)).await;
            Ok(vec![candle(120, 100.0, 1.0)])
        };
        let second = || async {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(vec![candle(120, 200.0, 1.0)])
        };

        let (a, b) = tokio::join!(
            history.get_or_fetch("000660", first),
            history.get_or_fetch("000660", second),
        );

        assert_eq!(a.unwrap()[0].close, 100.0);
        assert_eq!(b.unwrap()[0].close, 100.0);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn healthy_reads_never_repeat_backfill() {
        let history = MarketHistory::new();
        let calls = AtomicUsize::new(0);
        let fetch = || {
            calls.fetch_add(1, Ordering::SeqCst);
            async { Ok(vec![candle(120, 100.0, 1.0)]) }
        };

        history.get_or_fetch("000660", fetch).await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        history.get_or_fetch("000660", fetch).await.unwrap();
        history.get_or_fetch("000660", fetch).await.unwrap();

        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn explicit_gap_causes_one_new_backfill() {
        let history = MarketHistory::new();
        let calls = AtomicUsize::new(0);
        let fetch = || {
            let call = calls.fetch_add(1, Ordering::SeqCst);
            async move { Ok(vec![candle(120, 100.0 + call as f64, 1.0)]) }
        };

        history.get_or_fetch("000660", fetch).await.unwrap();
        history.mark_gap("000660").await;
        let restored = history.get_or_fetch("000660", fetch).await.unwrap();
        history.get_or_fetch("000660", fetch).await.unwrap();

        assert_eq!(restored[0].close, 101.0);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn trade_ticks_update_current_and_next_minute_candles() {
        let history = MarketHistory::new();
        history
            .get_or_fetch("000660", || async {
                Ok(vec![Candle {
                    time: 120,
                    open: 100.0,
                    high: 105.0,
                    low: 95.0,
                    close: 101.0,
                    volume: 10.0,
                }])
            })
            .await
            .unwrap();

        assert!(history.apply_trade(&quote(125, 110.0, 3.0)).await);
        assert!(history.apply_trade(&quote(181, 107.0, 2.0)).await);
        assert!(!history.apply_trade(&quote(182, 108.0, 0.0)).await);

        let bars = history.cached("000660").await.unwrap();
        assert_eq!(bars.len(), 2);
        assert_eq!(bars[0].open, 100.0);
        assert_eq!(bars[0].high, 110.0);
        assert_eq!(bars[0].low, 95.0);
        assert_eq!(bars[0].close, 110.0);
        assert_eq!(bars[0].volume, 13.0);
        assert_eq!(bars[1].time, 180);
        assert_eq!(bars[1].close, 107.0);
        assert_eq!(bars[1].volume, 2.0);
    }

    #[tokio::test]
    async fn healthy_snapshot은_이후_실시간_체결에도_불변이다() {
        let history = MarketHistory::new();
        history
            .get_or_fetch("000660", || async { Ok(vec![candle(120, 100.0, 10.0)]) })
            .await
            .unwrap();
        let captured = history.healthy_snapshot("000660").await.unwrap();

        assert!(history.apply_trade(&quote(125, 110.0, 3.0)).await);

        assert_eq!(captured[0].close, 100.0);
        assert_eq!(captured[0].volume, 10.0);
        let latest = history.healthy_snapshot("000660").await.unwrap();
        assert_eq!(latest[0].close, 110.0);
        assert_eq!(latest[0].volume, 13.0);
    }

    #[tokio::test]
    async fn duplicate_or_out_of_order_trade_sequence_does_not_double_volume() {
        let history = MarketHistory::new();
        history
            .get_or_fetch("000660", || async { Ok(vec![candle(120, 100.0, 10.0)]) })
            .await
            .unwrap();

        let latest = quote(125, 110.0, 3.0);
        assert!(history.apply_trade(&latest).await);
        assert!(!history.apply_trade(&latest).await);
        assert!(!history.apply_trade(&quote(124, 109.0, 2.0)).await);

        let bars = history.cached("000660").await.unwrap();
        assert_eq!(bars[0].close, 110.0);
        assert_eq!(bars[0].volume, 13.0);
    }

    #[tokio::test]
    async fn reconnect_gap_resets_trade_sequence_space() {
        let history = MarketHistory::new();
        history
            .get_or_fetch("000660", || async { Ok(vec![candle(120, 100.0, 10.0)]) })
            .await
            .unwrap();
        assert!(history.apply_trade(&quote(125, 110.0, 3.0)).await);

        history.mark_gap("000660").await;
        assert!(history.apply_trade(&quote(126, 111.0, 2.0)).await);
    }

    #[tokio::test]
    async fn next_trading_day_accepts_reset_cumulative_sequence() {
        let history = MarketHistory::new();
        history
            .get_or_fetch("000660", || async {
                Ok(vec![candle(86_400 + 120, 100.0, 10.0)])
            })
            .await
            .unwrap();

        let mut first_day = quote(86_400 + 125, 110.0, 3.0);
        first_day.trade_sequence = 100_000;
        assert!(history.apply_trade(&first_day).await);

        let mut next_day = quote(2 * 86_400 + 125, 111.0, 2.0);
        next_day.trade_sequence = 10;
        assert!(history.apply_trade(&next_day).await);
    }
}
