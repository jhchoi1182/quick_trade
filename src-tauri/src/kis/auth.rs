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

    /// 웹소켓 접속키 발급
    pub async fn ws_approval_key(&self) -> AppResult<String> {
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
}
