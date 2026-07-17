use std::sync::RwLock;

use crate::engine::EngineHandle;
use crate::types::Settings;

pub struct AppState {
    /// 현재 설정 (엔진 시작 실패와 무관하게 항상 유효)
    pub settings: RwLock<Settings>,
    /// 실행 중인 엔진 (설정 저장 시 교체)
    pub engine: tokio::sync::Mutex<Option<EngineHandle>>,
}

impl AppState {
    pub fn new(settings: Settings) -> Self {
        Self {
            settings: RwLock::new(settings),
            engine: tokio::sync::Mutex::new(None),
        }
    }
}
