use std::time::{Duration, Instant};

use serde_json::Value;
use tokio::sync::Mutex;

use crate::config;
use crate::error::{AppError, AppResult};
use crate::kis::auth::TokenManager;
use crate::types::{Settings, TradeMode};

pub const REAL_BASE: &str = "https://openapi.koreainvestment.com:9443";
pub const PAPER_BASE: &str = "https://openapivts.koreainvestment.com:29443";
pub const REAL_WS: &str = "ws://ops.koreainvestment.com:21000";
pub const PAPER_WS: &str = "ws://ops.koreainvestment.com:31000";

/// 균일 간격 리미터. KIS 게이트웨이는 고정 초 단위로 건수를 집계하므로
/// 슬라이딩 윈도우 방식은 초 경계에서 버스트가 생겨 EGW00201(초당 거래건수 초과)을 유발한다.
/// 호출 사이 최소 간격을 강제해 버스트를 원천 차단한다.
struct RateLimiter {
    /// 다음 호출이 허용되는 시각. 대기자는 자기 슬롯을 선점한 뒤 그 시각까지 잔다.
    next_slot: Mutex<Instant>,
    min_gap: Duration,
}

impl RateLimiter {
    fn new(min_gap: Duration) -> Self {
        Self { next_slot: Mutex::new(Instant::now()), min_gap }
    }

    async fn acquire(&self) {
        let due = {
            let mut slot = self.next_slot.lock().await;
            let due = (*slot).max(Instant::now());
            *slot = due + self.min_gap;
            due
        };
        tokio::time::sleep_until(due.into()).await;
    }
}

/// KIS 유량 초과 응답 판정 (HTTP 500 + msg_cd EGW00201)
pub fn is_rate_limit_error(msg: &str) -> bool {
    msg.contains("EGW00201") || msg.contains("초당 거래건수")
}

pub struct KisRest {
    http: reqwest::Client,
    pub base: String,
    pub mode: TradeMode,
    app_key: String,
    app_secret: String,
    pub token: TokenManager,
    limiter: RateLimiter,
}

impl KisRest {
    pub fn new(settings: &Settings) -> AppResult<Self> {
        if settings.app_key.is_empty() || settings.app_secret.is_empty() {
            return Err(AppError::Config("APP KEY/SECRET이 설정되지 않았습니다".into()));
        }
        // 실전 한도 20건/s, 모의 2건/s — 안전 마진을 둔 균일 간격
        let (base, gap_ms) = match settings.mode {
            TradeMode::Real => (REAL_BASE, 90),
            TradeMode::Paper => (PAPER_BASE, 550),
            TradeMode::Demo => return Err(AppError::Config("데모 모드는 KIS 연결을 사용하지 않습니다".into())),
        };
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(7))
            .build()?;
        let token = TokenManager::new(
            http.clone(),
            base.to_string(),
            settings.app_key.clone(),
            settings.app_secret.clone(),
            config::token_path(),
        );
        Ok(Self {
            http,
            base: base.to_string(),
            mode: settings.mode,
            app_key: settings.app_key.clone(),
            app_secret: settings.app_secret.clone(),
            token,
            limiter: RateLimiter::new(Duration::from_millis(gap_ms)),
        })
    }

    pub fn ws_url(&self) -> &'static str {
        match self.mode {
            TradeMode::Paper => PAPER_WS,
            _ => REAL_WS,
        }
    }

    /// 조회 요청. 유량 초과(EGW00201) 시 자동 재시도한다 (읽기 전용이라 안전).
    pub async fn get(&self, path: &str, tr_id: &str, params: &[(&str, String)]) -> AppResult<Value> {
        let mut attempt = 0u32;
        loop {
            self.limiter.acquire().await;
            let bearer = self.token.bearer().await?;
            let resp = self
                .http
                .get(format!("{}{}", self.base, path))
                .query(params)
                .header("authorization", format!("Bearer {bearer}"))
                .header("appkey", &self.app_key)
                .header("appsecret", &self.app_secret)
                .header("tr_id", tr_id)
                .header("custtype", "P")
                .send()
                .await?;
            let result = Self::into_json(resp).await;
            match &result {
                Err(AppError::Kis(msg)) if is_rate_limit_error(msg) && attempt < 4 => {
                    // 지수 백오프: 0.5s → 1s → 2s → 4s (읽기 전용이라 재시도 안전)
                    let delay = Duration::from_millis(500u64 << attempt);
                    attempt += 1;
                    tracing::warn!("KIS 유량 초과({tr_id}) — {attempt}번째 재시도 ({delay:?} 대기)");
                    tokio::time::sleep(delay).await;
                }
                _ => return result,
            }
        }
    }

    pub async fn post(&self, path: &str, tr_id: &str, body: &Value) -> AppResult<Value> {
        self.limiter.acquire().await;
        let bearer = self.token.bearer().await?;
        let resp = self
            .http
            .post(format!("{}{}", self.base, path))
            .json(body)
            .header("authorization", format!("Bearer {bearer}"))
            .header("appkey", &self.app_key)
            .header("appsecret", &self.app_secret)
            .header("tr_id", tr_id)
            .header("custtype", "P")
            .send()
            .await?;
        Self::into_json(resp).await
    }

    async fn into_json(resp: reqwest::Response) -> AppResult<Value> {
        let status = resp.status();
        let text = resp.text().await?;
        let v: Value = serde_json::from_str(&text)
            .map_err(|_| AppError::Kis(format!("HTTP {status}: {text}")))?;
        if !status.is_success() {
            return Err(AppError::Kis(format!(
                "HTTP {status}: {}",
                v["msg1"].as_str().unwrap_or(&text).trim()
            )));
        }
        Ok(v)
    }

    /// KIS 응답 공통 체크: rt_cd != "0" 이면 msg1을 에러로 반환
    pub fn check_rt(v: &Value) -> AppResult<()> {
        if v["rt_cd"].as_str() == Some("0") {
            Ok(())
        } else {
            let msg = v["msg1"].as_str().unwrap_or("알 수 없는 KIS 오류").trim().to_string();
            Err(AppError::Kis(msg))
        }
    }
}

/// KIS 응답의 문자열 숫자 필드 파싱 ("12345", "-1.23" 등)
pub fn num_f64(v: &Value) -> f64 {
    v.as_str().and_then(|s| s.trim().parse().ok()).unwrap_or(0.0)
}

pub fn num_u64(v: &Value) -> u64 {
    v.as_str()
        .and_then(|s| s.trim().parse::<f64>().ok())
        .map(|f| f.max(0.0) as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rate_limit_error_detection() {
        assert!(is_rate_limit_error("HTTP 500 Internal Server Error: 초당 거래건수를 초과하였습니다."));
        assert!(is_rate_limit_error("EGW00201"));
        assert!(!is_rate_limit_error("주문가능금액이 부족합니다"));
        assert!(!is_rate_limit_error("모의투자 장종료"));
    }

    #[tokio::test]
    async fn limiter_enforces_min_gap() {
        let limiter = RateLimiter::new(Duration::from_millis(50));
        let start = Instant::now();
        limiter.acquire().await; // 첫 호출은 즉시
        limiter.acquire().await; // +50ms
        limiter.acquire().await; // +100ms
        assert!(start.elapsed() >= Duration::from_millis(100));
    }
}
