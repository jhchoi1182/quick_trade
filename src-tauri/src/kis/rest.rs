use std::time::{Duration, Instant};

use serde_json::Value;
use tokio::sync::Mutex;

use crate::config;
use crate::error::{AppError, AppResult, OrderRejection};
use crate::kis::auth::TokenManager;
use crate::types::Settings;

pub const REAL_BASE: &str = "https://openapi.koreainvestment.com:9443";
pub const REAL_WS: &str = "ws://ops.koreainvestment.com:21000";

/// 실전 REST 호출 사이 최소 간격. 120ms면 어떤 1초 구간에도 최대 9건만
/// 시작하므로 다른 KIS 클라이언트나 서버 집계 오차가 있어도 여유를 둔다.
const REAL_MIN_GAP: Duration = Duration::from_millis(120);
/// POST는 자동 재시도할 수 없지만 유량 초과를 받은 뒤 이어지는 조회도 같은
/// 게이트웨이를 다시 압박하지 않도록 전체 REST 호출을 잠시 멈춘다.
const POST_RATE_LIMIT_COOLDOWN: Duration = Duration::from_secs(1);

pub struct RestPage {
    pub body: Value,
    pub tr_cont: String,
}

/// 균일 간격 리미터. KIS 게이트웨이는 고정 초 단위로 건수를 집계하므로
/// 슬라이딩 윈도우 방식은 초 경계에서 버스트가 생겨 EGW00201(초당 거래건수 초과)을 유발한다.
/// 호출 사이 최소 간격을 강제해 버스트를 원천 차단한다.
struct RateLimiter {
    schedule: Mutex<RateLimitSchedule>,
    min_gap: Duration,
}

struct RateLimitSchedule {
    /// 다음 호출이 허용되는 시각. 실제 허가 시점에만 갱신해, 런타임 지연 뒤
    /// 여러 대기자가 동시에 깨어나도 호출 시작 시각이 겹치지 않게 한다.
    next_slot: Instant,
    /// 한 요청이 유량 초과를 받으면 모든 대기 요청이 이 시각까지 기다린다.
    /// 호출별 sleep만 쓰면 그 사이 다른 GET/POST가 나간다.
    blocked_until: Instant,
}

impl RateLimiter {
    fn new(min_gap: Duration) -> Self {
        let now = Instant::now();
        Self {
            schedule: Mutex::new(RateLimitSchedule {
                next_slot: now,
                blocked_until: now,
            }),
            min_gap,
        }
    }

    async fn acquire(&self) {
        loop {
            let wait_until = {
                let mut schedule = self.schedule.lock().await;
                let now = Instant::now();
                let due = schedule.next_slot.max(schedule.blocked_until).max(now);
                if due <= now {
                    // 예약 시각이 아니라 실제 반환 시각을 기준으로 다음 슬롯을 잡는다.
                    schedule.next_slot = now + self.min_gap;
                    return;
                }
                due
            };
            // 여러 대기자가 같은 시각에 깨어나도 위 잠금에서 한 요청만 허가되고
            // 나머지는 갱신된 다음 슬롯을 다시 확인한다.
            tokio::time::sleep_until(wait_until.into()).await;
        }
    }

    async fn defer_for(&self, duration: Duration) {
        let mut schedule = self.schedule.lock().await;
        schedule.blocked_until = schedule.blocked_until.max(Instant::now() + duration);
    }
}

/// KIS 유량 초과 응답 판정 (HTTP 500 + msg_cd EGW00201)
pub fn is_rate_limit_error(msg: &str) -> bool {
    msg.contains("EGW00201") || msg.contains("초당 거래건수")
}

/// KIS 접근토큰 만료 응답 판정.
///
/// 운영 응답은 보통 HTTP 500 본문의 `msg_cd=EGW00123`,
/// `msg1=기간이 만료된 token 입니다.` 형태다.
pub fn is_token_expired_error(msg: &str) -> bool {
    let normalized = msg.to_ascii_lowercase();
    normalized.contains("egw00123")
        || normalized.contains("만료된 token")
        || normalized.contains("token이 만료")
        || normalized.contains("token 이 만료")
        || normalized.contains("token has expired")
        || normalized.contains("expired token")
}

pub struct KisRest {
    http: reqwest::Client,
    pub base: String,
    app_key: String,
    app_secret: String,
    pub token: TokenManager,
    limiter: RateLimiter,
}

impl KisRest {
    pub fn new(settings: &Settings) -> AppResult<Self> {
        if !settings.real_trading_confirmed {
            return Err(AppError::Config("실전 연결 확인이 필요합니다".into()));
        }
        if settings.app_key.is_empty() || settings.app_secret.is_empty() {
            return Err(AppError::Config(
                "APP KEY/SECRET이 설정되지 않았습니다".into(),
            ));
        }
        // 실전 한도에 안전 마진을 둔 균일 간격.
        let base = REAL_BASE;
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
            app_key: settings.app_key.clone(),
            app_secret: settings.app_secret.clone(),
            token,
            limiter: RateLimiter::new(REAL_MIN_GAP),
        })
    }

    pub fn ws_url(&self) -> &'static str {
        REAL_WS
    }

    /// 조회 요청. 유량 초과(EGW00201) 시 자동 재시도한다 (읽기 전용이라 안전).
    pub async fn get(
        &self,
        path: &str,
        tr_id: &str,
        params: &[(&str, String)],
    ) -> AppResult<Value> {
        Ok(self.get_page(path, tr_id, params, None).await?.body)
    }

    /// 연속조회가 필요한 주문·체결 조회용. 응답의 tr_cont 헤더를 보존한다.
    pub async fn get_page(
        &self,
        path: &str,
        tr_id: &str,
        params: &[(&str, String)],
        tr_cont: Option<&str>,
    ) -> AppResult<RestPage> {
        let mut rate_limit_attempt = 0u32;
        let mut token_refresh_attempted = false;
        loop {
            self.limiter.acquire().await;
            let bearer = self.token.bearer().await?;
            let mut request = self
                .http
                .get(format!("{}{}", self.base, path))
                .query(params)
                .header("authorization", format!("Bearer {bearer}"))
                .header("appkey", &self.app_key)
                .header("appsecret", &self.app_secret)
                .header("tr_id", tr_id)
                .header("custtype", "P");
            if let Some(cont) = tr_cont.filter(|v| !v.is_empty()) {
                request = request.header("tr_cont", cont);
            }
            let resp = request.send().await?;
            let response_cont = resp
                .headers()
                .get("tr_cont")
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default()
                .to_string();
            let result = Self::into_json(resp).await.map(|body| RestPage {
                body,
                tr_cont: response_cont,
            });
            match &result {
                Err(AppError::Kis(msg))
                    if is_token_expired_error(msg) && !token_refresh_attempted =>
                {
                    token_refresh_attempted = true;
                    self.token.invalidate_rejected(&bearer).await;
                    tracing::warn!("KIS 접근토큰 만료({tr_id}) — 캐시 폐기 후 조회를 한 번 재시도");
                }
                Err(AppError::Kis(msg)) if is_rate_limit_error(msg) && rate_limit_attempt < 4 => {
                    // 지수 백오프: 0.5s → 1s → 2s → 4s (읽기 전용이라 재시도 안전).
                    // 개별 태스크가 아니라 공유 리미터를 미뤄 다른 POST도 보호한다.
                    let delay = Duration::from_millis(500u64 << rate_limit_attempt);
                    rate_limit_attempt += 1;
                    self.limiter.defer_for(delay).await;
                    tracing::warn!(
                        "KIS 유량 초과({tr_id}) — {rate_limit_attempt}번째 재시도 ({delay:?} 대기)"
                    );
                }
                _ => return result,
            }
        }
    }

    pub async fn post(&self, path: &str, tr_id: &str, body: &Value) -> AppResult<Value> {
        let mut token_refresh_attempted = false;
        let result = loop {
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
            let result = Self::into_json(resp).await;
            if matches!(
                &result,
                Err(AppError::Kis(message))
                    if is_token_expired_error(message) && !token_refresh_attempted
            ) {
                // 인증 단계에서 명시적으로 거부된 요청은 KIS 주문 시스템에 접수되지
                // 않았으므로 같은 POST를 새 토큰으로 한 번만 다시 보내도 이중 주문이 없다.
                token_refresh_attempted = true;
                self.token.invalidate_rejected(&bearer).await;
                tracing::warn!("KIS 접근토큰 만료({tr_id}) — 캐시 폐기 후 POST를 한 번 재시도");
                continue;
            }
            break result;
        };
        if matches!(&result, Err(AppError::Kis(message)) if is_rate_limit_error(message)) {
            // 주문 POST는 이중 주문 위험 때문에 다시 보내지 않는다. 다만 뒤따르는
            // 체결·미체결 조정까지 연달아 유량 초과가 나지 않게 전역 쿨다운한다.
            self.limiter.defer_for(POST_RATE_LIMIT_COOLDOWN).await;
            tracing::warn!(
                "KIS 주문 유량 초과({tr_id}) — POST는 재시도하지 않고 전체 REST를 {POST_RATE_LIMIT_COOLDOWN:?} 대기"
            );
        }
        Self::classify_post_result(result)
    }

    /// HTTP 응답을 실제로 받은 EGW00201은 게이트웨이가 요청을 제한한 명시적
    /// 미접수다. 전송 타임아웃·응답 파싱 실패와 달리 주문번호 없는 Unknown으로
    /// 남기지 않되, POST 자체를 이 함수 안에서 다시 보내지는 않는다.
    fn classify_post_result(result: AppResult<Value>) -> AppResult<Value> {
        match result {
            Err(AppError::Kis(message)) if is_rate_limit_error(&message) => {
                Err(AppError::Order(OrderRejection::kis(
                    Some("EGW00201".into()),
                    format!("KIS 게이트웨이 유량 제한으로 요청이 접수되지 않았습니다: {message}"),
                )))
            }
            result => result,
        }
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
            let msg = v["msg1"]
                .as_str()
                .unwrap_or("알 수 없는 KIS 오류")
                .trim()
                .to_string();
            Err(AppError::Kis(msg))
        }
    }
}

/// KIS 응답의 문자열 숫자 필드 파싱 ("12345", "-1.23" 등)
pub fn num_f64(v: &Value) -> f64 {
    v.as_str()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0.0)
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
        assert!(is_rate_limit_error(
            "HTTP 500 Internal Server Error: 초당 거래건수를 초과하였습니다."
        ));
        assert!(is_rate_limit_error("EGW00201"));
        assert!(!is_rate_limit_error("주문가능금액이 부족합니다"));
        assert!(!is_rate_limit_error("모의투자 장종료"));
    }

    #[test]
    fn token_expired_error_detection() {
        assert!(is_token_expired_error(
            "HTTP 500 Internal Server Error: 기간이 만료된 token 입니다."
        ));
        assert!(is_token_expired_error("EGW00123"));
        assert!(is_token_expired_error("expired token"));
        assert!(!is_token_expired_error("접근토큰 발급 실패"));
        assert!(!is_token_expired_error("APP KEY가 유효하지 않습니다"));
    }

    #[test]
    fn post_rate_limit_is_classified_as_definite_rejection() {
        let rate_limited = KisRest::classify_post_result(Err(AppError::Kis(
            "HTTP 500 Internal Server Error: 초당 거래건수를 초과하였습니다.".into(),
        )))
        .unwrap_err();
        let rejection = rate_limited
            .order_rejection()
            .expect("유량 제한은 확정 미접수여야 한다");
        assert_eq!(rejection.code(), Some("EGW00201"));
        assert!(!rejection.is_buying_power_shortfall());

        let unknown = KisRest::classify_post_result(Err(AppError::Kis(
            "KIS 성공 응답에 주문번호가 없습니다".into(),
        )))
        .unwrap_err();
        assert!(matches!(unknown, AppError::Kis(_)));
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

    #[tokio::test(flavor = "current_thread")]
    async fn limiter_does_not_burst_after_runtime_delay() {
        let limiter = std::sync::Arc::new(RateLimiter::new(Duration::from_millis(30)));
        limiter.acquire().await;
        let mut waiting = Vec::new();
        for _ in 0..3 {
            let limiter = std::sync::Arc::clone(&limiter);
            waiting.push(tokio::spawn(async move {
                limiter.acquire().await;
                Instant::now()
            }));
        }

        // 대기 태스크가 잠든 뒤 런타임을 막아 예약 시각을 모두 지나게 한다.
        // 실제 허가 시각으로 재계산하지 않으면 세 호출이 한꺼번에 반환한다.
        tokio::task::yield_now().await;
        std::thread::sleep(Duration::from_millis(110));

        let mut granted_at = Vec::new();
        for task in waiting {
            granted_at.push(task.await.unwrap());
        }
        granted_at.sort_unstable();
        assert!(granted_at
            .windows(2)
            .all(|pair| pair[1].duration_since(pair[0]) >= Duration::from_millis(25)));
    }

    #[tokio::test]
    async fn limiter_global_cooldown_delays_waiting_call() {
        let limiter = std::sync::Arc::new(RateLimiter::new(Duration::from_millis(40)));
        limiter.acquire().await;

        // 두 번째 호출이 +40ms 슬롯을 기다리는 동안 더 긴 전역 쿨다운을 건다.
        let waiting = {
            let limiter = std::sync::Arc::clone(&limiter);
            tokio::spawn(async move { limiter.acquire().await })
        };
        tokio::task::yield_now().await;
        let cooldown_started = Instant::now();
        limiter.defer_for(Duration::from_millis(120)).await;

        waiting.await.unwrap();
        assert!(cooldown_started.elapsed() >= Duration::from_millis(110));
    }
}
