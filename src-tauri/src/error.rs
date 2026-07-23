use std::fmt;

use thiserror::Error;

/// KIS가 응답 본문으로 확정한 주문 업무거부.
///
/// `msg_cd`를 버리지 않아 운영 로그와 거부 사유 분류에 사용할 수 있게 하고,
/// 테스트 더블처럼 코드가 없는 거부도 같은 타입으로 다룬다.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderRejection {
    code: Option<String>,
    message: String,
}

impl OrderRejection {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            code: None,
            message: message.into(),
        }
    }

    pub fn kis(code: Option<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.filter(|value| !value.trim().is_empty()),
            message: message.into(),
        }
    }

    pub fn code(&self) -> Option<&str> {
        self.code.as_deref()
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    /// 매수가능수량 재조회·재주문을 허용하는 자금 부족 계열 거부만 판별한다.
    ///
    /// KIS 문구의 조사·공백 차이는 허용하되, 단순히 확정 거부이거나 가능수량이
    /// 줄었다는 이유만으로 매매정지·가격 오류를 재주문하지 않는다.
    pub fn is_buying_power_shortfall(&self) -> bool {
        let normalized: String = self
            .message
            .chars()
            .filter(|ch| !ch.is_whitespace())
            .collect();
        normalized.contains("주문가능금액")
            && (normalized.contains("부족") || normalized.contains("초과"))
    }
}

impl fmt::Display for OrderRejection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(code) = &self.code {
            write!(f, "[{code}] {}", self.message)
        } else {
            f.write_str(&self.message)
        }
    }
}

impl From<String> for OrderRejection {
    fn from(message: String) -> Self {
        Self::new(message)
    }
}

impl From<&str> for OrderRejection {
    fn from(message: &str) -> Self {
        Self::new(message)
    }
}

#[derive(Debug, Error)]
pub enum AppError {
    #[error("네트워크 오류: {0}")]
    Http(#[from] reqwest::Error),
    #[error("KIS API 오류: {0}")]
    Kis(String),
    #[error("설정 오류: {0}")]
    Config(String),
    #[error("주문 불가: {0}")]
    Order(OrderRejection),
    #[error("입출력 오류: {0}")]
    Io(#[from] std::io::Error),
    #[error("파싱 오류: {0}")]
    Parse(String),
}

impl AppError {
    pub fn order_rejection(&self) -> Option<&OrderRejection> {
        match self {
            Self::Order(rejection) => Some(rejection),
            _ => None,
        }
    }

    pub fn is_confirmed_order_rejection(&self) -> bool {
        self.order_rejection().is_some()
    }
}

pub type AppResult<T> = Result<T, AppError>;

impl From<serde_json::Error> for AppError {
    fn from(e: serde_json::Error) -> Self {
        AppError::Parse(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn 주문가능금액_부족과_초과만_재주문_대상으로_분류한다() {
        for message in ["주문가능금액이 부족합니다", "주문 가능 금액을 초과했습니다"]
        {
            assert!(OrderRejection::new(message).is_buying_power_shortfall());
        }

        for message in [
            "매매정지 종목입니다",
            "가격제한폭을 벗어났습니다",
            "EGW00201",
        ] {
            assert!(!OrderRejection::new(message).is_buying_power_shortfall());
        }
    }

    #[test]
    fn kis_거부는_메시지코드를_표시에_보존한다() {
        let rejection = OrderRejection::kis(Some("APBK1234".into()), "주문 불가");
        assert_eq!(rejection.code(), Some("APBK1234"));
        assert_eq!(rejection.message(), "주문 불가");
        assert_eq!(rejection.to_string(), "[APBK1234] 주문 불가");
    }
}
