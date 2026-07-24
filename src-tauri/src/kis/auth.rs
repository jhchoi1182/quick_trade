use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::error::{AppError, AppResult};

/// 접근토큰은 24시간 유효 + 발급이 분당 1회로 제한되므로 반드시 디스크에 캐시해 재사용한다.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CachedToken {
    access_token: String,
    /// 실제 UTC epoch 초
    expires_at: i64,
    #[serde(default)]
    base: String,
    #[serde(default)]
    app_key_fingerprint: String,
}

pub struct TokenManager {
    http: reqwest::Client,
    base: String,
    app_key: String,
    app_secret: String,
    path: PathBuf,
    cached: Mutex<Option<CachedToken>>,
}

impl TokenManager {
    pub fn new(
        http: reqwest::Client,
        base: String,
        app_key: String,
        app_secret: String,
        path: PathBuf,
    ) -> Self {
        Self {
            http,
            base,
            app_key,
            app_secret,
            path,
            cached: Mutex::new(None),
        }
    }

    pub async fn bearer(&self) -> AppResult<String> {
        let mut guard = self.cached.lock().await;
        let now = chrono::Utc::now().timestamp();
        let expected_fingerprint = app_key_fingerprint(&self.app_key);
        let still_valid = |t: &CachedToken| {
            t.expires_at - 300 > now
                && t.base == self.base
                && t.app_key_fingerprint == expected_fingerprint
        };

        if let Some(t) = guard.as_ref() {
            if still_valid(t) {
                return Ok(t.access_token.clone());
            }
        }
        if guard.is_none() {
            if let Some(t) = self.load_from_disk() {
                if still_valid(&t) {
                    let token = t.access_token.clone();
                    *guard = Some(t);
                    return Ok(token);
                }
            }
        }

        let body = serde_json::json!({
            "grant_type": "client_credentials",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
        });
        let resp = self
            .http
            .post(format!("{}/oauth2/tokenP", self.base))
            .json(&body)
            .send()
            .await?;
        let v: serde_json::Value = resp.json().await?;
        let token = v["access_token"]
            .as_str()
            .ok_or_else(|| AppError::Kis(format!("토큰 발급 실패: {v}")))?
            .to_string();
        let expires_in = v["expires_in"].as_i64().unwrap_or(86_400);
        let t = CachedToken {
            access_token: token.clone(),
            expires_at: now + expires_in,
            base: self.base.clone(),
            app_key_fingerprint: expected_fingerprint,
        };
        self.save_to_disk(&t);
        *guard = Some(t);
        tracing::info!("KIS 접근토큰 신규 발급");
        Ok(token)
    }

    /// KIS가 만료됐다고 거부한 토큰만 메모리·디스크 캐시에서 폐기한다.
    ///
    /// 동시 요청 하나가 이미 새 토큰을 발급한 뒤 늦게 도착한 옛 응답이 새 토큰을
    /// 지우지 않도록, 실제 요청에 사용한 bearer와 현재 캐시가 일치할 때만 지운다.
    pub async fn invalidate_rejected(&self, rejected_bearer: &str) -> bool {
        let mut guard = self.cached.lock().await;
        if guard
            .as_ref()
            .is_some_and(|token| token.access_token != rejected_bearer)
        {
            return false;
        }

        let memory_matched = guard
            .as_ref()
            .is_some_and(|token| token.access_token == rejected_bearer);
        if memory_matched {
            *guard = None;
        }

        let disk_matched = self
            .load_from_disk()
            .is_some_and(|token| token.access_token == rejected_bearer);
        if disk_matched {
            if let Err(error) = std::fs::remove_file(&self.path) {
                tracing::warn!("만료된 KIS 토큰 캐시 삭제 실패: {error}");
            }
        }
        memory_matched || disk_matched
    }

    fn load_from_disk(&self) -> Option<CachedToken> {
        let raw = std::fs::read_to_string(&self.path).ok()?;
        serde_json::from_str(&raw).ok()
    }

    fn save_to_disk(&self, t: &CachedToken) {
        if let Some(dir) = self.path.parent() {
            let _ = std::fs::create_dir_all(dir);
        }
        if let Ok(raw) = serde_json::to_string(t) {
            let _ = std::fs::write(&self.path, raw);
        }
    }

    /// 웹소켓 접속키 발급자 생성. 재접속 태스크가 소유할 수 있게 자격을 복제해 담는다.
    pub fn approval_issuer(&self) -> ApprovalKeyIssuer {
        ApprovalKeyIssuer {
            http: self.http.clone(),
            base: self.base.clone(),
            app_key: self.app_key.clone(),
            app_secret: self.app_secret.clone(),
        }
    }
}

/// 웹소켓 접속키 발급자. 재접속 시도마다 새 키를 발급해,
/// 무효화된 키로 전 구독이 거절되는 영구 재접속 루프를 막는다.
#[derive(Clone)]
pub struct ApprovalKeyIssuer {
    http: reqwest::Client,
    base: String,
    app_key: String,
    app_secret: String,
}

impl ApprovalKeyIssuer {
    pub async fn issue(&self) -> AppResult<String> {
        let body = serde_json::json!({
            "grant_type": "client_credentials",
            "appkey": self.app_key,
            "secretkey": self.app_secret,
        });
        let resp = self
            .http
            .post(format!("{}/oauth2/Approval", self.base))
            .json(&body)
            .send()
            .await?;
        let v: serde_json::Value = resp.json().await?;
        v["approval_key"]
            .as_str()
            .map(str::to_string)
            .ok_or_else(|| AppError::Kis(format!("approval_key 발급 실패: {v}")))
    }
}

/// 키 원문을 토큰 파일에 복제하지 않고 환경 혼용만 판별하기 위한 안정적인 fingerprint.
fn app_key_fingerprint(value: &str) -> String {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fingerprint_is_stable_and_key_specific() {
        assert_eq!(app_key_fingerprint("abc"), app_key_fingerprint("abc"));
        assert_ne!(app_key_fingerprint("abc"), app_key_fingerprint("abd"));
    }

    fn 테스트_토큰_관리자(path: PathBuf) -> TokenManager {
        TokenManager::new(
            reqwest::Client::new(),
            "https://example.test".into(),
            "app-key".into(),
            "app-secret".into(),
            path,
        )
    }

    fn 테스트_토큰(value: &str) -> CachedToken {
        CachedToken {
            access_token: value.into(),
            expires_at: chrono::Utc::now().timestamp() + 86_400,
            base: "https://example.test".into(),
            app_key_fingerprint: app_key_fingerprint("app-key"),
        }
    }

    #[tokio::test]
    async fn 서버가_거부한_토큰만_메모리와_디스크에서_폐기한다() {
        let path = std::env::temp_dir().join(format!(
            "easy-scalping-token-invalidate-{}.json",
            std::process::id()
        ));
        let manager = 테스트_토큰_관리자(path.clone());
        let expired = 테스트_토큰("expired-token");
        manager.save_to_disk(&expired);
        *manager.cached.lock().await = Some(expired);

        assert!(manager.invalidate_rejected("expired-token").await);
        assert!(manager.cached.lock().await.is_none());
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn 늦게_도착한_옛_만료_응답은_새_토큰을_폐기하지_않는다() {
        let path = std::env::temp_dir().join(format!(
            "easy-scalping-token-stale-response-{}.json",
            std::process::id()
        ));
        let manager = 테스트_토큰_관리자(path.clone());
        let current = 테스트_토큰("new-token");
        manager.save_to_disk(&current);
        *manager.cached.lock().await = Some(current);

        assert!(!manager.invalidate_rejected("expired-token").await);
        assert_eq!(
            manager
                .cached
                .lock()
                .await
                .as_ref()
                .map(|token| token.access_token.as_str()),
            Some("new-token")
        );
        assert_eq!(
            manager
                .load_from_disk()
                .as_ref()
                .map(|token| token.access_token.as_str()),
            Some("new-token")
        );

        let _ = std::fs::remove_file(path);
    }
}
