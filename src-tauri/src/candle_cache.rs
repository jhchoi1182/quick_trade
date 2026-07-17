use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;

use crate::error::AppResult;
use crate::types::Candle;

const DEFAULT_TTL: Duration = Duration::from_secs(60);

struct Entry {
    fetched_at: Instant,
    bars: Arc<Vec<Candle>>,
}

/// 종목별 1분봉 캐시 + 단일 비행(single-flight).
/// 같은 종목을 동시에 요청해도(React StrictMode 이중 effect, 빠른 셀렉터 토글)
/// 실제 KIS 백필은 1회만 나가고 나머지는 결과를 공유한다 — 유량 절약의 핵심.
pub struct CandleCache {
    entries: Mutex<HashMap<String, Entry>>,
    flights: Mutex<HashMap<String, Arc<Mutex<()>>>>,
    ttl: Duration,
}

impl CandleCache {
    pub fn new() -> Self {
        Self::with_ttl(DEFAULT_TTL)
    }

    pub fn with_ttl(ttl: Duration) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            flights: Mutex::new(HashMap::new()),
            ttl,
        }
    }

    pub async fn get_or_fetch<F, Fut>(&self, code: &str, fetch: F) -> AppResult<Arc<Vec<Candle>>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = AppResult<Vec<Candle>>>,
    {
        if let Some(bars) = self.fresh(code).await {
            return Ok(bars);
        }
        let flight = {
            let mut flights = self.flights.lock().await;
            Arc::clone(flights.entry(code.to_string()).or_default())
        };
        let _guard = flight.lock().await;
        // 비행 대기 중 다른 요청이 채웠을 수 있으므로 재확인
        if let Some(bars) = self.fresh(code).await {
            return Ok(bars);
        }
        let bars = Arc::new(fetch().await?);
        self.entries.lock().await.insert(
            code.to_string(),
            Entry { fetched_at: Instant::now(), bars: Arc::clone(&bars) },
        );
        Ok(bars)
    }

    async fn fresh(&self, code: &str) -> Option<Arc<Vec<Candle>>> {
        let entries = self.entries.lock().await;
        entries
            .get(code)
            .filter(|e| e.fetched_at.elapsed() < self.ttl)
            .map(|e| Arc::clone(&e.bars))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn bar() -> Candle {
        Candle { time: 0, open: 1.0, high: 1.0, low: 1.0, close: 1.0, volume: 1.0 }
    }

    #[tokio::test]
    async fn concurrent_requests_fetch_once() {
        let cache = CandleCache::new();
        let count = AtomicUsize::new(0);

        let fetch = || async {
            count.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(30)).await;
            Ok(vec![bar()])
        };
        let fetch2 = || async {
            count.fetch_add(1, Ordering::SeqCst);
            Ok(vec![bar()])
        };

        let (a, b) = tokio::join!(
            cache.get_or_fetch("0193T0", fetch),
            cache.get_or_fetch("0193T0", fetch2),
        );
        assert_eq!(a.unwrap().len(), 1);
        assert_eq!(b.unwrap().len(), 1);
        assert_eq!(count.load(Ordering::SeqCst), 1, "동시 요청은 fetch 1회만");
    }

    #[tokio::test]
    async fn expired_cache_refetches() {
        let cache = CandleCache::with_ttl(Duration::from_millis(10));
        let count = AtomicUsize::new(0);
        let make = || {
            count.fetch_add(1, Ordering::SeqCst);
            async { Ok(vec![bar()]) }
        };
        cache.get_or_fetch("X", make).await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        cache.get_or_fetch("X", make).await.unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn fetch_error_is_not_cached() {
        let cache = CandleCache::new();
        let err = cache
            .get_or_fetch("Y", || async {
                Err(crate::error::AppError::Kis("일시 오류".into()))
            })
            .await;
        assert!(err.is_err());
        // 실패는 캐시되지 않아 다음 요청이 다시 시도한다
        let ok = cache.get_or_fetch("Y", || async { Ok(vec![bar()]) }).await;
        assert!(ok.is_ok());
    }
}
