//! KIS 국내휴장일조회로 KRX 개장일 여부를 확인한다.

use crate::broker::BrokerMarketDay;
use crate::error::{AppError, AppResult};
use crate::kis::rest::KisRest;

const PATH_HOLIDAY: &str = "/uapi/domestic-stock/v1/quotations/chk-holiday";
const TR_HOLIDAY: &str = "CTCA0903R";

fn validate_basis_date(basis_date: &str) -> AppResult<()> {
    if basis_date.len() != 8 || !basis_date.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(AppError::Parse(format!(
            "휴장일 기준일자는 YYYYMMDD 형식이어야 합니다: {basis_date}"
        )));
    }
    Ok(())
}

fn parse_market_days(response: &serde_json::Value) -> AppResult<Vec<BrokerMarketDay>> {
    let output = &response["output"];
    let rows = match output {
        serde_json::Value::Array(rows) => rows.iter().collect::<Vec<_>>(),
        serde_json::Value::Object(_) => vec![output],
        _ => {
            return Err(AppError::Parse(
                "국내휴장일조회 output 형식을 해석할 수 없습니다".into(),
            ));
        }
    };
    let days = rows
        .iter()
        .filter_map(|row| {
            let date = row["bass_dt"].as_str()?.trim();
            let open = row["opnd_yn"].as_str()?.trim();
            if date.len() != 8 || !matches!(open, "Y" | "N") {
                return None;
            }
            Some(BrokerMarketDay {
                date: date.to_string(),
                is_open: open == "Y",
            })
        })
        .collect::<Vec<_>>();
    if days.is_empty() {
        return Err(AppError::Parse(
            "국내휴장일조회에서 유효한 개장일 데이터를 찾지 못했습니다".into(),
        ));
    }
    Ok(days)
}

pub async fn market_days(rest: &KisRest, basis_date: &str) -> AppResult<Vec<BrokerMarketDay>> {
    validate_basis_date(basis_date)?;
    let params = vec![
        ("BASS_DT", basis_date.to_string()),
        ("CTX_AREA_FK", String::new()),
        ("CTX_AREA_NK", String::new()),
    ];
    let response = rest.get(PATH_HOLIDAY, TR_HOLIDAY, &params).await?;
    KisRest::check_rt(&response)?;
    parse_market_days(&response)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn 기준일자_형식을_검증한다() {
        assert!(validate_basis_date("20260723").is_ok());
        assert!(validate_basis_date("2026-07-23").is_err());
    }

    #[test]
    fn 개장일여부를_파싱한다() {
        let response = serde_json::json!({
            "output": [
                { "bass_dt": "20260723", "opnd_yn": "Y" },
                { "bass_dt": "20260724", "opnd_yn": "N" }
            ]
        });
        let days = parse_market_days(&response).unwrap();
        assert_eq!(days[0].date, "20260723");
        assert!(days[0].is_open);
        assert!(!days[1].is_open);
    }

    #[test]
    fn 단일객체_응답도_파싱한다() {
        let response = serde_json::json!({
            "output": { "bass_dt": "20260723", "opnd_yn": "Y" }
        });
        let days = parse_market_days(&response).unwrap();
        assert_eq!(days.len(), 1);
        assert_eq!(days[0].date, "20260723");
        assert!(days[0].is_open);
    }
}
