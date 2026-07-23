use std::sync::{Arc, RwLock};

use crate::config;
use crate::engine::EngineHandle;
use crate::ledger::Ledger;
use crate::types::Settings;

pub struct AppState {
    /// 현재 설정 (엔진 시작 실패와 무관하게 항상 유효)
    pub settings: RwLock<Settings>,
    /// 실행 중인 엔진 (설정 저장 시 교체)
    pub engine: tokio::sync::Mutex<Option<EngineHandle>>,
    /// 엔진 재시작과 무관하게 유지되는 단일 SQLite 거래 장부.
    pub ledger: Arc<Ledger>,
}

impl AppState {
    pub fn new(settings: Settings) -> Result<Self, String> {
        let ledger = Ledger::open(config::config_dir()).map_err(|e| e.to_string())?;
        Ok(Self {
            settings: RwLock::new(settings),
            engine: tokio::sync::Mutex::new(None),
            ledger: Arc::new(ledger),
        })
    }
}
