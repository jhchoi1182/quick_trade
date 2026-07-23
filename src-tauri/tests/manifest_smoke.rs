// Cargo 1.97+는 `cargo:rustc-link-arg-tests`를 사용할 때 명시적인 통합 테스트
// 타깃이 있어야 한다. 이 파일은 Windows 테스트 실행 파일에 `tests.manifest`의
// comctl32 v6 설정을 임베드할 타깃을 제공하므로 비워 두거나 삭제하지 않는다.
#[test]
fn 윈도우_테스트_매니페스트_타깃을_유지한다() {}
