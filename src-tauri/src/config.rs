use std::fs;
use std::path::PathBuf;

use crate::error::{AppError, AppResult};
use crate::types::{AutoSymbols, Settings};

pub fn config_dir() -> PathBuf {
    dirs::config_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("easy-scalping")
}

/// 구 quick-trade 폴더를 easy-scalping으로 1회 이관한다.
/// 실전 KIS 키(config.json)·토큰(token.json)·거래 장부(trading.db)가 그대로 넘어간다.
/// 새 폴더가 이미 있으면(=이관 완료 또는 신규 설치) 아무것도 하지 않는다.
pub fn migrate_legacy_dir() {
    let base = dirs::config_dir().unwrap_or_else(|| PathBuf::from("."));
    let legacy = base.join("quick-trade");
    let current = config_dir();
    if legacy.exists() && !current.exists() {
        match fs::rename(&legacy, &current) {
            Ok(()) => tracing::info!(
                "설정 폴더 이관: {} → {}",
                legacy.display(),
                current.display()
            ),
            Err(error) => {
                tracing::warn!("설정 폴더 이관 실패(기존 경로 유지): {error}")
            }
        }
    }
}

pub fn config_path() -> PathBuf {
    config_dir().join("config.json")
}

pub fn token_path() -> PathBuf {
    config_dir().join("token.json")
}

pub fn trading_db_path() -> PathBuf {
    config_dir().join("trading.db")
}

/// 설정 로드. 구 real/paper/demo 설정은 실전 전용 설정으로 1회 마이그레이션한다.
pub fn load() -> Settings {
    let path = config_path();
    match fs::read_to_string(&path) {
        Ok(raw) => match migrate(&raw) {
            Ok(settings) => {
                let old_shape = serde_json::from_str::<serde_json::Value>(&raw)
                    .ok()
                    .is_some_and(|value| value.get("configVersion").is_none());
                if old_shape {
                    if let Err(error) = save(&settings) {
                        tracing::warn!("마이그레이션 설정 저장 실패: {error}");
                    }
                }
                settings
            }
            Err(e) => {
                tracing::warn!("설정 파일 파싱 실패, 기본값 사용: {e}");
                Settings::default()
            }
        },
        Err(_) => Settings::default(),
    }
}

fn migrate(raw: &str) -> Result<Settings, serde_json::Error> {
    let original: serde_json::Value = serde_json::from_str(raw)?;
    let mut settings: Settings = serde_json::from_value(original.clone())?;
    let old_shape = original.get("configVersion").is_none();
    if old_shape {
        // 구 real만 연결 의사가 확인된 것으로 본다. demo/paper는 키를 보존하되
        // 사용자가 설정 화면에서 실전 연결을 명시하기 전까지 엔진을 시작하지 않는다.
        settings.real_trading_confirmed = original
            .get("mode")
            .and_then(|v| v.as_str())
            .is_some_and(|mode| mode == "real");
        settings.config_version = 2;
        tracing::info!(
            "구 설정 마이그레이션: mode={} → 수동/실전확인={}",
            original
                .get("mode")
                .and_then(|v| v.as_str())
                .unwrap_or("없음"),
            settings.real_trading_confirmed
        );
    }
    let defaults = Settings::default();
    if settings.trade_symbols.is_empty() {
        settings.trade_symbols = defaults.trade_symbols;
    }
    if settings.chart_symbols.is_empty() {
        settings.chart_symbols = defaults.chart_symbols;
    }
    // 자동 전략은 SK하이닉스 전용이다. 설정 파일을 직접 편집해도 다른 종목으로
    // 주문 대상이 바뀌지 않도록 코드 세 개를 버전 상수와 같은 기본값으로 고정한다.
    let fixed_auto_symbols = AutoSymbols::default();
    if settings.auto_symbols != fixed_auto_symbols {
        tracing::warn!("자동매매 종목 설정을 SK하이닉스 고정값으로 복원합니다");
        settings.auto_symbols = fixed_auto_symbols;
    }
    Ok(settings)
}

pub fn save(settings: &Settings) -> AppResult<()> {
    let dir = config_dir();
    fs::create_dir_all(&dir)?;
    let raw =
        serde_json::to_string_pretty(settings).map_err(|e| AppError::Config(e.to_string()))?;
    fs::write(config_path(), raw)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn old_real_is_confirmed() {
        let raw = r#"{"appKey":"k","appSecret":"s","cano":"1","acntPrdtCd":"01","htsId":"","mode":"real","tradeSymbols":[],"chartSymbols":[],"chartInterval":10,"opacity":1}"#;
        let settings = migrate(raw).unwrap();
        assert!(settings.real_trading_confirmed);
        assert!(!settings.trade_symbols.is_empty());
    }

    #[test]
    fn old_paper_requires_real_confirmation() {
        let raw = r#"{"appKey":"k","appSecret":"s","cano":"1","acntPrdtCd":"01","htsId":"","mode":"paper","tradeSymbols":[],"chartSymbols":[],"chartInterval":10,"opacity":1}"#;
        let settings = migrate(raw).unwrap();
        assert!(!settings.real_trading_confirmed);
        assert_eq!(settings.app_key, "k");
    }

    #[test]
    fn auto_symbols_are_always_fixed_to_sk_hynix() {
        let raw = r#"{"configVersion":2,"autoSymbols":{"underlying":"005930","leverage":"111111","inverse":"222222"}}"#;
        let settings = migrate(raw).unwrap();
        assert_eq!(settings.auto_symbols, AutoSymbols::default());
    }
}
