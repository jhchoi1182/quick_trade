use std::sync::{Arc, RwLock};

use crate::config;
use crate::engine::EngineHandle;
use crate::ledger::Ledger;
use crate::types::Settings;

pub struct AppState {
    /// 현재 설정 (엔진 시작 실패와 무관하게 항상 유효)
    pub settings: RwLock<Settings>,
    /// 일반 엔진 명령과 엔진 교체 경계를 직렬화한다.
    ///
    /// 일반 명령은 전체 실행 동안 읽기 잠금을, 엔진 재시작·초기화는 쓰기 잠금을
    /// 유지해 폐기된 엔진의 응답이 새 엔진 상태와 장부를 덮지 못하게 한다.
    pub engine_lifecycle: tokio::sync::RwLock<()>,
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
            engine_lifecycle: tokio::sync::RwLock::new(()),
            engine: tokio::sync::Mutex::new(None),
            ledger: Arc::new(ledger),
        })
    }
}
