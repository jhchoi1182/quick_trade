mod automation;
pub mod broker;
mod chart_image;
mod commands;
mod config;
mod engine;
pub mod error;
mod kis;
mod ledger;
mod market_history;
#[cfg(test)]
mod mock;
mod state;
pub mod types;
pub mod util;

use std::sync::Arc;
use tauri::{Emitter, Manager};

use crate::state::AppState;

/// 로그 폴더에서 보존 기한이 지난 파일을 지운다. 실패해도 앱 동작에는 영향 없다.
fn prune_old_logs(dir: &std::path::Path, keep_days: u64) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    let cutoff = std::time::SystemTime::now()
        - std::time::Duration::from_secs(keep_days.saturating_mul(86_400));
    for entry in entries.flatten() {
        let too_old = entry
            .metadata()
            .and_then(|meta| meta.modified())
            .map(|modified| modified < cutoff)
            .unwrap_or(false);
        if too_old {
            let _ = std::fs::remove_file(entry.path());
        }
    }
}

pub fn run() {
    // 설치형 앱은 stdout이 유실되므로 파일에도 남긴다 — 장중 웹소켓 구독 거절·
    // 재구독 경고를 사후 진단할 수 있어야 한다. logs/easy-scalping.log.YYYY-MM-DD,
    // 7일 보존.
    let log_dir = config::config_dir().join("logs");
    prune_old_logs(&log_dir, 7);
    let file_appender = tracing_appender::rolling::daily(&log_dir, "easy-scalping.log");
    let (file_writer, _file_log_guard) = tracing_appender::non_blocking(file_appender);
    {
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::util::SubscriberInitExt;
        tracing_subscriber::registry()
            .with(tracing_subscriber::filter::LevelFilter::INFO)
            .with(tracing_subscriber::fmt::layer())
            .with(
                tracing_subscriber::fmt::layer()
                    .with_ansi(false)
                    .with_writer(file_writer),
            )
            .init();
    }

    // config::load() 전에 구 폴더명(quick-trade)을 새 폴더명으로 이관한다.
    config::migrate_legacy_dir();

    tauri::Builder::default()
        .plugin(tauri_plugin_single_instance::init(|app, _args, _cwd| {
            // 이중 실행 = 이중 주문 위험 → 기존 창 포커스만
            if let Some(win) = app.get_webview_window("main") {
                let _ = win.set_focus();
            }
        }))
        .manage(AppState::new(config::load()).expect("거래 장부 초기화 실패"))
        .setup(|app| {
            let handle = app.handle().clone();
            tauri::async_runtime::spawn(async move {
                let state = handle.state::<AppState>();
                let _lifecycle = state.engine_lifecycle.write().await;
                let settings = state.settings.read().unwrap().clone();
                match engine::start(handle.clone(), settings, Arc::clone(&state.ledger)).await {
                    Ok(engine_handle) => {
                        *state.engine.lock().await = Some(engine_handle);
                        tracing::info!("엔진 시작 완료");
                    }
                    Err(e) => {
                        tracing::error!("엔진 시작 실패: {e}");
                        let _ = handle.emit("engine-error", format!("엔진 시작 실패: {e}"));
                    }
                }
            });
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            commands::get_settings,
            commands::save_settings,
            commands::get_candles,
            commands::get_account,
            commands::buy_max,
            commands::sell_all,
            commands::place_reserved_sell,
            commands::cancel_reserved_sell,
            commands::get_reservations,
            commands::get_automation_status,
            commands::set_control_mode,
            commands::reset_runtime_and_resync,
            commands::list_trade_records,
            commands::list_llm_decisions,
        ])
        .run(tauri::generate_context!())
        .expect("Tauri 실행 실패");
}
