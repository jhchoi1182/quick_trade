use std::sync::Arc;
use std::time::Duration;

use tauri::{AppHandle, Emitter, State};

use crate::config;
use crate::engine::{self, Engine};
use crate::ledger::{
    CursorPage, DecisionQuery, DecisionRecord, LedgerExecutionKind, TradeQuery, TradeRecord,
};
use crate::state::AppState;
use crate::types::{
    AccountSnapshot, AutoSymbols, AutomationSnapshot, Candle, ControlMode, OrderResult,
    ReservationInfo, RuntimeResyncResult, Settings, SymbolConfig,
};

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
        || old.real_trading_confirmed != new.real_trading_confirmed
        || old.exchange != new.exchange
        || old.auto_symbols != new.auto_symbols
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
    if settings.auto_symbols != AutoSymbols::default() {
        return Err("자동매매 종목은 SK하이닉스 000660 / 0193T0 / 0197X0으로 고정됩니다".into());
    }
    let old = state.settings.read().unwrap().clone();
    let mut guard = state.engine.lock().await;
    let restart = guard.is_none() || needs_engine_restart(&old, &settings);
    if restart {
        if let Some(handle) = guard.as_ref() {
            handle.engine.ensure_broker_restart_safe().await?;
        }
    }

    // 안전 조건을 모두 확인한 뒤에만 디스크와 공유 설정을 바꾼다. 거부된 설정이
    // 다음 시작에 적용되어 기존 계좌 보호를 우회하는 일을 막는다.
    config::save(&settings).map_err(|e| e.to_string())?;
    *state.settings.write().unwrap() = settings.clone();
    if restart {
        // 브로커 관련 변경 → 기존 엔진 중단 후 재시작
        guard.take();
        match engine::start(app, settings, Arc::clone(&state.ledger)).await {
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

/// 예약 매도 설정: 평단 기준 target_pct(%) 이상 첫 호가에 보유 전량 지정가 매도를 건다
#[tauri::command]
pub async fn place_reserved_sell(
    state: State<'_, AppState>,
    code: String,
    target_pct: f64,
) -> Result<OrderResult, String> {
    let engine = engine_of(&state).await?;
    Ok(engine.place_reserved_sell(&code, target_pct).await)
}

#[tauri::command]
pub async fn cancel_reserved_sell(
    state: State<'_, AppState>,
    code: String,
) -> Result<OrderResult, String> {
    let engine = engine_of(&state).await?;
    Ok(engine.cancel_reserved_sell(&code).await)
}

#[tauri::command]
pub async fn get_reservations(state: State<'_, AppState>) -> Result<Vec<ReservationInfo>, String> {
    let engine = engine_of(&state).await?;
    Ok(engine.get_reservations())
}

#[tauri::command]
pub async fn get_automation_status(
    state: State<'_, AppState>,
) -> Result<AutomationSnapshot, String> {
    let engine = engine_of(&state).await?;
    Ok(engine.automation_snapshot())
}

#[tauri::command]
pub async fn set_control_mode(
    state: State<'_, AppState>,
    mode: ControlMode,
) -> Result<AutomationSnapshot, String> {
    let engine = engine_of(&state).await?;
    engine.set_control_mode(mode).await
}

/// 과거 런타임 소유권을 폐기하고 KIS 현재 계좌를 원장으로 새 수동 엔진을 만든다.
///
/// 설정·토큰·거래/주문 기록은 보존한다. 실제 미체결이 있으면 엔진 유무와 관계없이
/// 초기화 전에 거부하므로, 주문을 고아로 만든 채 수동 모드가 되는 경로는 없다.
#[tauri::command]
pub async fn reset_runtime_and_resync(
    app: AppHandle,
    state: State<'_, AppState>,
) -> Result<RuntimeResyncResult, String> {
    let settings = state.settings.read().unwrap().clone();
    let mut guard = state.engine.lock().await;
    if let Some(handle) = guard.as_ref() {
        handle.engine.reset_runtime_for_resync().await?;
    } else {
        // 저장된 Auto 인계 복구 실패로 엔진 시작 자체가 안 된 경우에도 설정에서
        // 읽기 전용 KIS 검증을 거쳐 복구할 수 있어야 한다.
        engine::reset_runtime_without_engine(&settings, &state.ledger).await?;
    }

    // 기존 피드·스케줄러를 먼저 중단한다. 인스턴스별 REST 리미터가 교체되는
    // 경계에서도 KIS 호출 간격이 붙지 않도록 한 칸 여유를 둔다.
    guard.take();
    tokio::time::sleep(Duration::from_millis(200)).await;

    match engine::start(app.clone(), settings, Arc::clone(&state.ledger)).await {
        Ok(handle) => {
            let result = RuntimeResyncResult {
                account: handle.engine.account_snapshot(),
                automation: handle.engine.automation_snapshot(),
            };
            *guard = Some(handle);
            if let Err(error) = app.emit("runtime-reset", &result) {
                tracing::warn!("런타임 초기화 이벤트 전송 실패: {error}");
            }
            Ok(result)
        }
        Err(error) => Err(format!(
            "런타임 상태는 수동으로 초기화했지만 엔진 재시작에 실패했습니다. 설정을 확인한 뒤 다시 시도하거나 앱을 재시작하세요: {error}"
        )),
    }
}

#[tauri::command]
pub fn list_trade_records(
    state: State<'_, AppState>,
    kind: String,
    cursor: Option<i64>,
    limit: usize,
) -> Result<CursorPage<TradeRecord>, String> {
    let execution_kind = match kind.as_str() {
        "real" => LedgerExecutionKind::Real,
        "shadow" => LedgerExecutionKind::Shadow,
        _ => return Err("기록 종류는 real 또는 shadow여야 합니다".into()),
    };
    state
        .ledger
        .list_trades(
            &TradeQuery {
                execution_kind: Some(execution_kind),
                ..TradeQuery::default()
            },
            cursor,
            limit,
        )
        .map_err(|error| error.to_string())
}

#[tauri::command]
pub fn list_llm_decisions(
    state: State<'_, AppState>,
    cursor: Option<i64>,
    limit: usize,
) -> Result<CursorPage<DecisionRecord>, String> {
    state
        .ledger
        .list_decisions(&DecisionQuery::default(), cursor, limit)
        .map_err(|error| error.to_string())
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

        let mut confirmation = old.clone();
        confirmation.real_trading_confirmed = true;
        assert!(needs_engine_restart(&old, &confirmation));

        let mut auto_symbol = old.clone();
        auto_symbol.auto_symbols.leverage = "999999".into();
        assert!(needs_engine_restart(&old, &auto_symbol));
    }
}
