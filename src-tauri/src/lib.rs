pub mod broker;
pub mod candle_cache;
mod commands;
mod config;
mod engine;
pub mod error;
mod kis;
pub mod mock;
mod state;
pub mod types;
pub mod util;

use tauri::{Emitter, Manager};

use crate::state::AppState;

pub fn run() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    tauri::Builder::default()
        .plugin(tauri_plugin_single_instance::init(|app, _args, _cwd| {
            // 이중 실행 = 이중 주문 위험 → 기존 창 포커스만
            if let Some(win) = app.get_webview_window("main") {
                let _ = win.set_focus();
            }
        }))
        .manage(AppState::new(config::load()))
        .setup(|app| {
            let handle = app.handle().clone();
            tauri::async_runtime::spawn(async move {
                let state = handle.state::<AppState>();
                let settings = state.settings.read().unwrap().clone();
                match engine::start(handle.clone(), settings).await {
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
        ])
        .run(tauri::generate_context!())
        .expect("Tauri 실행 실패");
}
