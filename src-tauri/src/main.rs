// Windows에서 릴리스 빌드 시 콘솔 창 숨김
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

fn main() {
    easy_scalping_lib::run()
}
