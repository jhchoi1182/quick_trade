use crate::error::AppResult;
use crate::kis::rest::{num_f64, num_u64, KisRest};
use crate::types::Position;

const PATH_BALANCE: &str = "/uapi/domestic-stock/v1/trading/inquire-balance";
const PATH_PSBL: &str = "/uapi/domestic-stock/v1/trading/inquire-psbl-order";

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
    let v = rest.get(PATH_BALANCE, "TTTC8434R", &params).await?;
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

/// 매수가능조회 → 주문가능금액.
/// KIS가 주문 검증에 쓰는 것과 같은 기준인 미수없는매수금액(nrcvb_buy_amt)을 우선 사용한다.
/// 장중 매도를 반복하면 미정산 매도대금 탓에 ord_psbl_cash가 실제 허용액과 어긋나
/// "주문가능금액 초과" 거부가 난다. 응답에 필드가 없으면 ord_psbl_cash로 폴백.
pub async fn orderable_cash(rest: &KisRest, cano: &str, prdt: &str, code: &str) -> AppResult<u64> {
    let v = psbl_order(rest, cano, prdt, code, 0, "01").await?;
    let out = &v["output"];
    let has_nrcvb = out["nrcvb_buy_amt"]
        .as_str()
        .map(str::trim)
        .is_some_and(|s| !s.is_empty());
    if has_nrcvb {
        Ok(num_u64(&out["nrcvb_buy_amt"]))
    } else {
        Ok(num_u64(&out["ord_psbl_cash"]))
    }
}

/// 매수가능조회 → 지정가 price 기준 미수 없는 최대 매수 수량.
/// 수수료·미정산 매도대금까지 KIS가 반영해 계산하므로 이 수량은 주문 검증을 통과한다.
pub async fn max_buy_qty(
    rest: &KisRest,
    cano: &str,
    prdt: &str,
    code: &str,
    price: u64,
) -> AppResult<u64> {
    // 실제 재주문도 IOC 지정가(11)이므로 매수가능조회에도 같은 주문구분을
    // 전달해야 종목 증거금률과 주문 규칙이 동일하게 반영된다.
    let v = psbl_order(rest, cano, prdt, code, price, "11").await?;
    Ok(num_u64(&v["output"]["nrcvb_buy_qty"]))
}

/// 실제 주문과 같은 주문구분으로 매수가능금액·수량을 조회한다.
async fn psbl_order(
    rest: &KisRest,
    cano: &str,
    prdt: &str,
    code: &str,
    price: u64,
    order_division: &str,
) -> AppResult<serde_json::Value> {
    let params = vec![
        ("CANO", cano.to_string()),
        ("ACNT_PRDT_CD", prdt.to_string()),
        ("PDNO", code.to_string()),
        ("ORD_UNPR", price.to_string()),
        ("ORD_DVSN", order_division.to_string()),
        ("CMA_EVLU_AMT_ICLD_YN", "N".to_string()),
        ("OVRS_ICLD_YN", "N".to_string()),
    ];
    let v = rest.get(PATH_PSBL, "TTTC8908R", &params).await?;
    KisRest::check_rt(&v)?;
    Ok(v)
}
