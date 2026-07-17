use crate::error::AppResult;
use crate::kis::rest::{num_f64, num_u64, KisRest};
use crate::types::{Position, TradeMode};

const PATH_BALANCE: &str = "/uapi/domestic-stock/v1/trading/inquire-balance";
const PATH_PSBL: &str = "/uapi/domestic-stock/v1/trading/inquire-psbl-order";

fn tr(mode: TradeMode, real: &'static str, paper: &'static str) -> &'static str {
    match mode {
        TradeMode::Paper => paper,
        _ => real,
    }
}

/// 잔고조회 → 보유 종목 목록
pub async fn positions(rest: &KisRest, cano: &str, prdt: &str) -> AppResult<Vec<Position>> {
    let params = vec![
        ("CANO", cano.to_string()),
        ("ACNT_PRDT_CD", prdt.to_string()),
        ("AFHR_FLPR_YN", "N".to_string()),
        ("OFL_YN", String::new()),
        ("INQR_DVSN", "02".to_string()),
        ("UNPR_DVSN", "01".to_string()),
        ("FUND_STTL_ICLD_YN", "N".to_string()),
        ("FNCG_AMT_AUTO_RDPT_YN", "N".to_string()),
        ("PRCS_DVSN", "00".to_string()),
        ("CTX_AREA_FK100", String::new()),
        ("CTX_AREA_NK100", String::new()),
    ];
    let v = rest
        .get(PATH_BALANCE, tr(rest.mode, "TTTC8434R", "VTTC8434R"), &params)
        .await?;
    KisRest::check_rt(&v)?;
    let list = v["output1"].as_array().cloned().unwrap_or_default();
    Ok(list
        .iter()
        .filter_map(|row| {
            let qty = num_u64(&row["hldg_qty"]);
            if qty == 0 {
                return None;
            }
            Some(Position {
                code: row["pdno"].as_str()?.to_string(),
                qty,
                avg_price: num_f64(&row["pchs_avg_pric"]),
                eval_pnl: num_f64(&row["evlu_pfls_amt"]),
                pnl_rate: num_f64(&row["evlu_pfls_rt"]),
            })
        })
        .collect())
}

/// 매수가능조회 → 주문가능현금 (가격과 무관한 ord_psbl_cash 사용)
pub async fn orderable_cash(rest: &KisRest, cano: &str, prdt: &str, any_code: &str) -> AppResult<u64> {
    let params = vec![
        ("CANO", cano.to_string()),
        ("ACNT_PRDT_CD", prdt.to_string()),
        ("PDNO", any_code.to_string()),
        ("ORD_UNPR", "0".to_string()),
        ("ORD_DVSN", "01".to_string()),
        ("CMA_EVLU_AMT_ICLD_YN", "N".to_string()),
        ("OVRS_ICLD_YN", "N".to_string()),
    ];
    let v = rest
        .get(PATH_PSBL, tr(rest.mode, "TTTC8908R", "VTTC8908R"), &params)
        .await?;
    KisRest::check_rt(&v)?;
    Ok(num_u64(&v["output"]["ord_psbl_cash"]))
}
