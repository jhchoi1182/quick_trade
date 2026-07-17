use std::fs;
use std::path::PathBuf;

use crate::error::{AppError, AppResult};
use crate::types::Settings;

pub fn config_dir() -> PathBuf {
    dirs::config_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("quick-trade")
}

pub fn config_path() -> PathBuf {
    config_dir().join("config.json")
}

pub fn token_path() -> PathBuf {
    config_dir().join("token.json")
}

/// 설정 로드. 파일이 없거나 깨졌으면 기본값 (기본 = 데모 모드)
pub fn load() -> Settings {
    let path = config_path();
    match fs::read_to_string(&path) {
        Ok(raw) => serde_json::from_str(&raw).unwrap_or_else(|e| {
            tracing::warn!("설정 파일 파싱 실패, 기본값 사용: {e}");
            Settings::default()
        }),
        Err(_) => Settings::default(),
    }
}

pub fn save(settings: &Settings) -> AppResult<()> {
    let dir = config_dir();
    fs::create_dir_all(&dir)?;
    let raw = serde_json::to_string_pretty(settings)
        .map_err(|e| AppError::Config(e.to_string()))?;
    fs::write(config_path(), raw)?;
    Ok(())
}
