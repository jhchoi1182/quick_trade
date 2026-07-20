use std::sync::Arc;

use tauri::{AppHandle, State};

use crate::config;
use crate::engine::{self, Engine};
use crate::state::AppState;
use crate::types::{AccountSnapshot, Candle, OrderResult, Settings, SymbolConfig};

/// 브로커 연결·구독에 영향을 주는 필드가 바뀐 경우에만 엔진을 재시작한다.
/// UI 전용 필드(opacity/chartInterval/theme, 라벨)는 저장만 하고 유지 —
/// 매 저장마다 재시작하면 웹소켓 재접속 + 백필 버스트로 유량 초과가 발생한다.
fn needs_engine_restart(old: &Settings, new: &Settings) -> bool {
    fn codes(list: &[SymbolConfig]) -> Vec<&str> {
        list.iter().map(|s| s.code.as_str()).collect()
    }
    old.app_key != new.app_key
        || old.app_secret != new.app_secret
        || old.cano != new.cano
        || old.acnt_prdt_cd != new.acnt_prdt_cd
        || old.hts_id != new.hts_id
        || old.mode != new.mode
        || old.exchange != new.exchange
        || codes(&old.trade_symbols) != codes(&new.trade_symbols)
        || codes(&old.chart_symbols) != codes(&new.chart_symbols)
}

async fn engine_of(state: &State<'_, AppState>) -> Result<Arc<Engine>, String> {
    let guard = state.engine.lock().await;
    guard
        .as_ref()
        .map(|h| Arc::clone(&h.engine))
        .ok_or_else(|| "엔진이 시작되지 않았습니다 — 설정을 확인하세요".to_string())
}

#[tauri::command]
pub fn get_settings(state: State<'_, AppState>) -> Settings {
    state.settings.read().unwrap().clone()
}

#[tauri::command]
pub async fn save_settings(
    app: AppHandle,
    state: State<'_, AppState>,
    settings: Settings,
) -> Result<(), String> {
    config::save(&settings).map_err(|e| e.to_string())?;
    let old = {
        let mut s = state.settings.write().unwrap();
        std::mem::replace(&mut *s, settings.clone())
    };

    let mut guard = state.engine.lock().await;
    let restart = guard.is_none() || needs_engine_restart(&old, &settings);
    if restart {
        // 브로커 관련 변경 → 기존 엔진 중단 후 재시작
        guard.take();
        match engine::start(app, settings).await {
            Ok(handle) => {
                *guard = Some(handle);
                Ok(())
            }
            Err(e) => Err(format!("엔진 시작 실패: {e}")),
        }
    } else {
        // UI 필드만 변경 → 실행 중인 엔진에 반영만
        if let Some(handle) = guard.as_ref() {
            handle.engine.update_settings(settings);
        }
        Ok(())
    }
}

#[tauri::command]
pub async fn get_candles(state: State<'_, AppState>, code: String) -> Result<Vec<Candle>, String> {
    let engine = engine_of(&state).await?;
    engine.candles(&code).await.map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn get_account(state: State<'_, AppState>) -> Result<AccountSnapshot, String> {
    let engine = engine_of(&state).await?;
    Ok(engine.account_snapshot())
}

#[tauri::command]
pub async fn buy_max(state: State<'_, AppState>, code: String) -> Result<OrderResult, String> {
    let engine = engine_of(&state).await?;
    Ok(engine.buy_max(&code).await)
}

#[tauri::command]
pub async fn sell_all(state: State<'_, AppState>, code: String) -> Result<OrderResult, String> {
    let engine = engine_of(&state).await?;
    Ok(engine.sell_all(&code).await)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ui_only_changes_do_not_restart() {
        let old = Settings::default();
        let mut new = old.clone();
        new.opacity = 0.5;
        new.chart_interval = 15;
        new.theme = "mono".into();
        new.trade_symbols[0].label = "라벨만 변경".into();
        assert!(!needs_engine_restart(&old, &new));
    }

    #[test]
    fn broker_changes_do_restart() {
        let old = Settings::default();

        let mut keys = old.clone();
        keys.app_key = "NEW".into();
        assert!(needs_engine_restart(&old, &keys));

        let mut exch = old.clone();
        exch.exchange = "SOR".into();
        assert!(needs_engine_restart(&old, &exch));

        let mut sym = old.clone();
        sym.trade_symbols[0].code = "999999".into();
        assert!(needs_engine_restart(&old, &sym));

        let mut mode = old.clone();
        mode.mode = crate::types::TradeMode::Paper;
        assert!(needs_engine_restart(&old, &mode));
    }
}
