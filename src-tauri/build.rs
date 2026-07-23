fn main() {
    tauri_build::build();

    // Windows(MSVC): 테스트 실행 파일에도 comctl32 v6 매니페스트를 임베드한다.
    // tauri-build는 bin 타겟에만 매니페스트를 넣어주므로, tauri 심볼을 링크하는
    // 단위 테스트 exe가 매니페스트 없이 빌드되면 STATUS_ENTRYPOINT_NOT_FOUND로 죽는다.
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    let target_env = std::env::var("CARGO_CFG_TARGET_ENV").unwrap_or_default();
    if target_os == "windows" && target_env == "msvc" {
        let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests.manifest");
        println!("cargo:rerun-if-changed=tests.manifest");
        println!("cargo:rustc-link-arg-tests=/MANIFEST:EMBED");
        println!(
            "cargo:rustc-link-arg-tests=/MANIFESTINPUT:{}",
            manifest.display()
        );
    }
}
