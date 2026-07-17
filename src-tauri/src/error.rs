use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("네트워크 오류: {0}")]
    Http(#[from] reqwest::Error),
    #[error("KIS API 오류: {0}")]
    Kis(String),
    #[error("설정 오류: {0}")]
    Config(String),
    #[error("주문 불가: {0}")]
    Order(String),
    #[error("입출력 오류: {0}")]
    Io(#[from] std::io::Error),
    #[error("파싱 오류: {0}")]
    Parse(String),
}

pub type AppResult<T> = Result<T, AppError>;

impl From<serde_json::Error> for AppError {
    fn from(e: serde_json::Error) -> Self {
        AppError::Parse(e.to_string())
    }
}
